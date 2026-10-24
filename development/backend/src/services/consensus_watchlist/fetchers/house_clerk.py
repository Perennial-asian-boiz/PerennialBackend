"""
PURPOSE:
    Collects House members' stock trades straight from the official source,
    the Clerk of the House's Periodic Transaction Reports (PTRs), and turns
    them into the same trade records fmp.py writes.

    FMP's free tier only returns the 20 latest House trades (page 0, limit
    20); deeper pages are HTTP 402. Its House rows link to these same PDFs,
    so this is the source FMP repackages. fmp.py combines the two: House
    trades from here, Senate trades still from FMP.

SOURCE:
    Clerk of the House — no key, no account, no robots.txt restrictions
    Index:  https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip
            (one XML row per filing, refreshed daily; FilingType "P" = PTR)
    Report: https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{DocID}.pdf

    About 88% of 2026 PTRs are e-filed PDFs with a text layer. The rest are
    scanned paper filings with no text; they are counted in the coverage as
    `scanned`, never guessed at.

    Use is restricted by law: 5 U.S.C. § 13107(c) bars obtaining or using
    these reports for commercial purposes (news media excepted). This
    applies equally to the same data bought from FMP or any other reseller.

CACHE:
    A filed PTR never changes (an amendment is a new filing with a new
    DocID), so each report is downloaded and parsed once. Results live in
    database/local_data/house_clerk_cache.json, keyed by DocID and tagged
    with PARSER_VERSION — bump it when the parser changes and every report
    is parsed again.

ROWS KEPT:
    Only stock rows, asset code [ST] (common stock and ADRs), that carry a
    ticker in parentheses — the same population as FMP's "Stock" rows.
    Bonds, funds, options, private placements and crypto are skipped.
"""

import io
import json
import logging
import os
import re

# ── shared path resolution (see paths.py) ──
import sys as _sys
import time
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from pathlib import Path as _Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

_sys.path.insert(
    0,
    str(
        next(
            p
            for p in _Path(__file__).resolve().parents
            if p.name == "consensus_watchlist"
        )
    ),
)
# Support direct script execution as well as package imports.
_sys.path.insert(
    0, str(next(p for p in _Path(__file__).resolve().parents if p.name == "backend"))
)
from paths import LOCAL_DATA_DIR  # noqa: E402
from src.utils.logger import configure_logging  # noqa: E402

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────

INDEX_URL = (
    "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip"
)
PTR_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"
SOURCE_NAME = "Clerk of the House (Periodic Transaction Reports)"
HEADERS = {"User-Agent": "Perennial research pipeline (Congress trade signals)"}

LOOKBACK_DAYS = (
    365  # PTRs filed in the last year; fmp.py then keeps its 2-year trade window
)
REQUEST_DELAY = 0.25  # seconds between report downloads
REQUEST_TIMEOUT = 30  # seconds
MAX_INDEX_BYTES = 20 * 1024 * 1024
MAX_PDF_BYTES = 10 * 1024 * 1024
PARSER_VERSION = 4

# A run that could not answer at least this share of its reports (downloaded,
# then parsed or recognized as scanned) is failed, and fmp.py falls back to
# FMP's House rows rather than publishing a thin House picture.
MIN_USABLE_SUCCESS_RATE = 0.9

# Report outcomes. Cached outcomes are final; a failed download is retried
# on the next run.
STATUS_PARSED = "parsed"  # text layer read; every stock row accounted for
STATUS_PARTIAL = "partial"  # text read, but some [ST] rows did not parse
STATUS_SCANNED = "scanned"  # no text layer — a scanned paper filing
STATUS_FAILED = "failed"  # download or PDF read failed — unknown, not cached

TRADE_TYPES = {
    "P": "Purchase",
    "S": "Sale",
    "S (PARTIAL)": "Sale (Partial)",
    "E": "Exchange",
}

# STOCK Act value ranges, keyed by lower bound, so an amount whose upper
# bound wrapped to another line is still labeled exactly.
AMOUNT_RANGES = {
    "1,001": "$1,001 - $15,000",
    "15,001": "$15,001 - $50,000",
    "50,001": "$50,001 - $100,000",
    "100,001": "$100,001 - $250,000",
    "250,001": "$250,001 - $500,000",
    "500,001": "$500,001 - $1,000,000",
    "1,000,001": "$1,000,001 - $5,000,000",
    "5,000,001": "$5,000,001 - $25,000,000",
    "25,000,001": "$25,000,001 - $50,000,000",
}

# The table header repeats on every page of a PTR.
_HEADER_LINES = {
    "ID Owner Asset Transaction",
    "Type",
    "Date Notification",
    "Date",
    "Amount Cap.",
    "Gains >",
    "$200?",
}
_TABLE_START = "ID Owner Asset Transaction"
_TABLE_END = "* For the complete list of asset type abbreviations"

# Sub-rows under each transaction ("Filing Status: New", "Subholding Of:
# ...", "Description: ..."). The label font has no text mapping for most
# letters, which extract as NUL characters: "F\x00\x00 S\x00: New". Every
# transaction has at least the Filing Status sub-row, so these lines split
# the table into one block per transaction.
_LABEL_LINE = re.compile(r"^[A-Z](?:\s+[A-Z])*\s*:")
_BOUNDARY = "\x1e"

_DATE = r"\d{2}/\d{2}/\d{4}"
# The asset column ends in "(TICKER) [CODE]". The type, dates and amount sit
# beside it: after it, before it when the asset name wraps to a second line,
# or between the ticker and the code when only "[CODE]" wraps. Preferred
# shares carry a "$" ticker such as "T$A".
_ASSET = re.compile(
    r"\((?P<ticker>[A-Z][A-Z0-9.\-$]{0,9})\)"
    r"(?:(?!\[[A-Z]{2}\])[^()])*?"
    r"\[(?P<code>[A-Z]{2})\]",
    re.DOTALL,
)
# Tickers the database schema accepts (src/ingestion/schemas.py _TICKER).
# Anything else, e.g. a preferred share's "T$A", is skipped and counted.
_SUPPORTED_TICKER = re.compile(r"^[A-Z0-9][A-Z0-9./-]{0,19}$")
_DEAL = re.compile(
    r"(?<!\S)(?P<type>S\s*\(partial\)|P|S|E)\s+"
    rf"(?P<tdate>{_DATE})\s*(?P<ndate>{_DATE})\s*"
    r"(?P<amount>Over\s+\$[\d,]+|\$[\d,]+(?:\s*-\s*\$[\d,]+)?)",
    re.IGNORECASE,
)
_STOCK_CODE = re.compile(r"\[ST\]")
_OWNER_PREFIX = re.compile(r"^(?:SP|JT|DC)\s+")


# ─────────────────────────────────────────────────────────
# INDEX — which PTRs were filed
# ─────────────────────────────────────────────────────────


def _iso(us_date: str) -> Optional[str]:
    """'09/01/2026' → '2026-09-01'; None if it does not parse."""
    try:
        return datetime.strptime(us_date.strip(), "%m/%d/%Y").date().isoformat()
    except (ValueError, AttributeError):
        return None


def parse_index(zip_bytes: bytes) -> List[Dict[str, str]]:
    """
    Reads the yearly {year}FD.zip and returns its PTR filings:
    [{doc_id, year, name, district, filing_date}, ...]. Raises ValueError
    when the archive or its XML is not the expected shape.
    """
    try:
        archive = zipfile.ZipFile(io.BytesIO(zip_bytes))
        name = next(n for n in archive.namelist() if n.lower().endswith(".xml"))
        root = ET.fromstring(archive.read(name))
    except (zipfile.BadZipFile, StopIteration, ET.ParseError, KeyError) as e:
        raise ValueError(f"index is not a readable FD zip ({type(e).__name__})")

    filings = []
    for member in root:
        row = {child.tag: (child.text or "").strip() for child in member}
        if row.get("FilingType") != "P":
            continue
        filing_date = _iso(row.get("FilingDate", ""))
        doc_id = row.get("DocID", "")
        if not filing_date or not doc_id.isdigit():
            raise ValueError("index row has no valid FilingDate or DocID")
        filings.append(
            {
                "doc_id": doc_id,
                "year": row.get("Year") or filing_date[:4],
                "name": " ".join(
                    p
                    for p in (row.get("First"), row.get("Last"), row.get("Suffix"))
                    if p
                ),
                "district": row.get("StateDst", ""),
                "filing_date": filing_date,
            }
        )
    return filings


def _get_capped(session: requests.Session, url: str, max_bytes: int) -> bytes:
    resp = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
    try:
        if resp.status_code != 200:
            raise requests.HTTPError(f"HTTP {resp.status_code}")
        body = bytearray()
        for chunk in resp.iter_content(chunk_size=65536):
            body.extend(chunk)
            if len(body) > max_bytes:
                raise ValueError(f"response over {max_bytes} bytes")
        return bytes(body)
    finally:
        resp.close()


# ─────────────────────────────────────────────────────────
# PARSE — one PTR's text into trade records
# ─────────────────────────────────────────────────────────


def extract_text(pdf_bytes: bytes) -> str:
    """The PDF's text layer ('' for a scanned filing). Raises on an unreadable PDF."""
    from pypdf import PdfReader  # imported here so other fetchers never need it

    reader = PdfReader(io.BytesIO(pdf_bytes))
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def _table_text(text: str) -> str:
    """
    The transaction table only: header lines dropped, and each sub-row line
    replaced by a boundary marker, so the table splits into one block per
    transaction.
    """
    text = text.replace("\x00", " ")
    start = text.find(_TABLE_START)
    if start < 0:
        return ""
    end = text.find(_TABLE_END, start)
    body = text[start : end if end >= 0 else len(text)]

    kept = []
    for line in body.splitlines():
        stripped = line.strip()
        if (
            not stripped
            or stripped in _HEADER_LINES
            or stripped.startswith("Filing ID #")
        ):
            continue
        kept.append(_BOUNDARY if _LABEL_LINE.match(stripped) else stripped)
    return "\n".join(kept)


def _amount_label(raw: str) -> str:
    """The standard STOCK Act range for an amount, even if its upper bound wrapped away."""
    raw = " ".join(raw.split())
    if raw.lower().startswith("over"):
        return "Over" + raw[4:]
    lower = raw.split("-")[0].strip().lstrip("$")
    return AMOUNT_RANGES.get(lower, raw)


def _asset_description(
    block: str, asset: "re.Match[str]", deal: "Optional[re.Match[str]]"
) -> str:
    """
    The asset name: the line holding '(TICKER)' and the line above it, with
    any type/date/amount text and the owner code removed. Earlier lines in a
    block are usually the previous sub-row's wrapped note, so they are left out.
    """
    if deal is not None and deal.start() < asset.start():
        before = block[: deal.start()] + " " + block[deal.end() : asset.start()]
    else:
        before = block[: asset.start()]
    lines = [line for line in before.split("\n") if line.strip()][-2:]
    name = " ".join(" ".join(lines).split())
    name = _OWNER_PREFIX.sub("", name)
    return name[-200:].strip(" -")


def parse_ptr_text(
    text: str, filing: Dict[str, str]
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Returns (trades, stats) for one PTR. stats counts the [ST] rows the
    report lists and how many parsed, so a layout the parser does not
    understand shows up as unparsed rows instead of silently missing trades.

    The table is read one transaction block at a time. A block normally holds
    one "(TICKER) [CODE]" and one type/dates/amount group; when it holds
    several of each they are paired in order, and a block whose counts differ
    is left unparsed.
    """
    table = _table_text(text)
    stock_rows = len(_STOCK_CODE.findall(table))
    skipped = 0  # recognized stock rows whose ticker the schema cannot hold
    trades = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for block in table.split(_BOUNDARY):
        assets = list(_ASSET.finditer(block))
        deals = list(_DEAL.finditer(block))
        if not assets or len(assets) != len(deals):
            continue
        for asset, deal in zip(assets, deals):
            if asset.group("code").upper() != "ST":
                continue
            if not _SUPPORTED_TICKER.match(asset.group("ticker").upper()):
                skipped += 1
                continue
            tdate = _iso(deal.group("tdate"))
            if not tdate:
                continue
            disclosure = filing["filing_date"]
            days = (
                datetime.fromisoformat(disclosure) - datetime.fromisoformat(tdate)
            ).days
            raw_type = " ".join(deal.group("type").upper().split()).replace("S(", "S (")
            trades.append(
                {
                    "politician_name": filing["name"],
                    "chamber": "House",
                    "ticker": asset.group("ticker").upper(),
                    "asset_description": _asset_description(
                        block, asset, deal if len(assets) == 1 else None
                    ),
                    "asset_type": "Stock",
                    "trade_type": TRADE_TYPES[raw_type],
                    "amount_range": _amount_label(deal.group("amount")),
                    "transaction_date": tdate,
                    "disclosure_date": disclosure,
                    "days_to_disclose": days,
                    "district": filing["district"],
                    "source_link": PTR_URL.format(
                        year=filing["year"], doc_id=filing["doc_id"]
                    ),
                    "data_source": "house_clerk",
                    "fetched_at": fetched_at,
                }
            )

    return trades, {
        "stock_rows": stock_rows,
        "parsed_rows": len(trades),
        "skipped_rows": skipped,
    }


def parse_report(pdf_bytes: bytes, filing: Dict[str, str]) -> Dict[str, Any]:
    """One report's cache entry: status, trades and row counts."""
    try:
        text = extract_text(pdf_bytes)
    except Exception as e:
        return {
            "status": STATUS_FAILED,
            "trades": [],
            "error": f"unreadable PDF ({type(e).__name__})",
        }

    if not text.strip():
        return {"status": STATUS_SCANNED, "trades": []}

    trades, stats = parse_ptr_text(text, filing)
    accounted = stats["parsed_rows"] + stats["skipped_rows"]
    status = STATUS_PARSED if accounted >= stats["stock_rows"] else STATUS_PARTIAL
    return {"status": status, "trades": trades, **stats}


# ─────────────────────────────────────────────────────────
# CACHE
# ─────────────────────────────────────────────────────────


def _cache_path() -> Path:
    return LOCAL_DATA_DIR / "house_clerk_cache.json"


def load_cache(path: Optional[str | Path] = None) -> Dict[str, Any]:
    """Cached report outcomes for this PARSER_VERSION; {} when absent or stale."""
    path = Path(path or _cache_path())
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return {}
    if data.get("parser_version") != PARSER_VERSION:
        return {}
    reports = data.get("reports")
    return reports if isinstance(reports, dict) else {}


def save_cache(reports: Dict[str, Any], path: Optional[str | Path] = None) -> None:
    """Atomic write — a crash mid-save must not leave a truncated cache."""
    path = Path(path or _cache_path())
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(f"{path}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"parser_version": PARSER_VERSION, "reports": reports}, f, indent=1)
    os.replace(tmp, path)


# ─────────────────────────────────────────────────────────
# COLLECT
# ─────────────────────────────────────────────────────────


def collect(
    lookback_days: int = LOOKBACK_DAYS,
    session: Optional[requests.Session] = None,
    cache_path: Optional[str | Path] = None,
    sleep: Callable[[float], None] = time.sleep,
) -> Dict[str, Any]:
    """
    House trades from every PTR filed in the last `lookback_days`.

    Returns {"status", "trades", "coverage"}:
        success — every report answered (parsed or recognized as scanned)
        partial — usable, but some reports failed or had unparsed stock rows
        failed  — an index was unusable, or fewer than
                  MIN_USABLE_SUCCESS_RATE of the reports answered;
                  fmp.py then keeps FMP's House rows instead
    """
    session = session or requests.Session()
    today = datetime.now(timezone.utc).date()
    cutoff = (today - timedelta(days=lookback_days)).isoformat()
    years = sorted({today.year, int(cutoff[:4])})

    filings = []
    for year in years:
        try:
            filings += parse_index(
                _get_capped(session, INDEX_URL.format(year=year), MAX_INDEX_BYTES)
            )
        except (requests.RequestException, ValueError) as e:
            logger.error(
                "house_clerk_index_unusable year=%d error=%s", year, type(e).__name__
            )
            return {
                "status": STATUS_FAILED,
                "trades": [],
                "coverage": {"error": f"index_unusable:{year}"},
            }
    # One entry per report, even if two yearly indexes ever list the same DocID.
    filings = list(
        {f["doc_id"]: f for f in filings if f["filing_date"] >= cutoff}.values()
    )

    cache = load_cache(cache_path)
    outcomes = {}
    downloaded = 0
    for filing in filings:
        doc_id = filing["doc_id"]
        cached = cache.get(doc_id)
        if cached and cached.get("status") != STATUS_FAILED:
            outcomes[doc_id] = cached
            continue
        try:
            pdf = _get_capped(
                session,
                PTR_URL.format(year=filing["year"], doc_id=doc_id),
                MAX_PDF_BYTES,
            )
            outcomes[doc_id] = parse_report(pdf, filing)
        except (requests.RequestException, ValueError) as e:
            outcomes[doc_id] = {
                "status": STATUS_FAILED,
                "trades": [],
                "error": f"{type(e).__name__}: {e}"[:200],
            }
        downloaded += 1
        if downloaded % 50 == 0:
            logger.info("house_clerk_progress downloaded=%d", downloaded)
        sleep(REQUEST_DELAY)

    # Keep final outcomes only; failures are retried next run.
    save_cache(
        {
            d: o
            for d, o in {**cache, **outcomes}.items()
            if o.get("status") != STATUS_FAILED
        },
        cache_path,
    )

    statuses = Counter(o["status"] for o in outcomes.values())
    answered = len(filings) - statuses[STATUS_FAILED]
    rate = answered / len(filings) if filings else 0.0
    trades = [t for o in outcomes.values() for t in o.get("trades", [])]
    coverage = {
        "lookback_days": lookback_days,
        "filed_since": cutoff,
        "reports": len(filings),
        "parsed": statuses[STATUS_PARSED],
        "partial": statuses[STATUS_PARTIAL],
        "scanned": statuses[STATUS_SCANNED],
        "failed": statuses[STATUS_FAILED],
        "downloaded_this_run": downloaded,
        "unparsed_stock_rows": sum(
            max(
                0,
                o.get("stock_rows", 0)
                - o.get("parsed_rows", 0)
                - o.get("skipped_rows", 0),
            )
            for o in outcomes.values()
        ),
        "skipped_stock_rows": sum(o.get("skipped_rows", 0) for o in outcomes.values()),
        "failed_reports": sorted(
            d for d, o in outcomes.items() if o["status"] == STATUS_FAILED
        )[:25],
        "scanned_reports": sorted(
            d for d, o in outcomes.items() if o["status"] == STATUS_SCANNED
        )[:25],
    }
    if not filings or rate < MIN_USABLE_SUCCESS_RATE:
        status = STATUS_FAILED
    elif statuses[STATUS_FAILED] or statuses[STATUS_PARTIAL]:
        status = "partial"
    else:
        status = "success"
    return {"status": status, "trades": trades, "coverage": coverage}


# ─────────────────────────────────────────────────────────
# MAIN — standalone check; fmp.py is what writes trades_congress.json
# ─────────────────────────────────────────────────────────


def run() -> Dict[str, Any]:
    logger.info("house_clerk_collect_start")
    result = collect()
    cov = result["coverage"]
    trades = result["trades"]
    purchases = [t for t in trades if t["trade_type"] == "Purchase"]
    logger.info(
        "house_clerk_summary status=%s reports=%d parsed=%d partial=%d scanned=%d "
        "failed=%d downloaded=%d unparsed_stock_rows=%d skipped_stock_rows=%d "
        "trades=%d purchases=%d",
        result["status"],
        *(
            cov.get(k, 0)
            for k in (
                "reports",
                "parsed",
                "partial",
                "scanned",
                "failed",
                "downloaded_this_run",
                "unparsed_stock_rows",
                "skipped_stock_rows",
            )
        ),
        len(trades),
        len(purchases),
    )
    for ticker, count in Counter(t["ticker"] for t in purchases).most_common(10):
        logger.info("house_clerk_top_purchase ticker=%s purchases=%d", ticker, count)
    return result


if __name__ == "__main__":
    configure_logging()
    run()
