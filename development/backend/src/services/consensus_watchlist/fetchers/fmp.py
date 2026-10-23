"""
SOURCE:
    Financial Modeling Prep (FMP) stable endpoints
    Senate: https://financialmodelingprep.com/stable/senate-latest
    House:  https://financialmodelingprep.com/stable/house-latest
    Free tier: 250 requests/day

SCHEDULE:
    Runs daily at 9:00am via scheduler/cron.py
"""

import json
import logging

# ── shared path resolution (see paths.py) ──
import sys as _sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path as _Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

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
from paths import LOCAL_DATA_DIR, local_data_dir  # noqa: F401 -- legacy module exports
from src.utils.logger import configure_logging, log_exception

if TYPE_CHECKING:
    from pydantic import SecretStr
    from src.settings import ProviderSettings


logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────

# Historical JSON callers and synthetic contract tests may set this explicitly.
# Importing the fetcher never reads a key or a dotenv file.
FMP_API_KEY: str | None = None
BASE_URL = "https://financialmodelingprep.com/stable"
MAX_PAGES = 50  # Safety cap per chamber; full final page is not a complete result.


# ─────────────────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────────────────


def legacy_fetch_chamber_trades(
    chamber: str, *, api_key: str | None = None, max_pages: int | None = None
) -> List[Dict[str, Any]]:
    """
    Fetches trades for one chamber.
    chamber = "senate" or "house"
    """
    endpoint = f"{BASE_URL}/{chamber}-latest"
    key = api_key if api_key is not None else FMP_API_KEY
    if not key:
        raise ValueError("FMP_API_KEY is required")
    page_limit = MAX_PAGES if max_pages is None else max_pages
    if page_limit < 1:
        raise ValueError("FMP page limit must be positive")
    all_trades: List[Dict[str, Any]] = []

    for page in range(page_limit):
        try:
            params: Dict[str, str | int] = {"page": page, "limit": 20, "apikey": key}
            resp = requests.get(endpoint, params=params, timeout=15)

            if resp.status_code == 401:
                logger.warning("congress_credential_rejected")
                break
            if resp.status_code == 402:
                logger.warning("congress_plan_limit page=%d", page)
                break
            if resp.status_code == 403:
                logger.warning("congress_access_denied")
                break

            resp.raise_for_status()
            data = resp.json()

            for item in data:
                item["_chamber"] = chamber.capitalize()

            all_trades.extend(data)
            if data:
                logger.info("congress_page page=%d records=%d", page + 1, len(data))

        except Exception as e:
            log_exception(logger, "fmp_fetch_chamber_trades_failed", e)
            break

        if len(data) < 20:
            break
        if page == page_limit - 1:
            raise RuntimeError("FMP pagination exhausted")

    return all_trades


# ─────────────────────────────────────────────────────────
# PARSE
# ─────────────────────────────────────────────────────────


def _provider_record_id(item: Dict[str, Any]) -> Optional[str]:
    """A transaction identifier, never a filing URL (one filing can contain many trades)."""
    for key in ("transactionId", "transaction_id", "id"):
        value = item.get(key)
        if value is None or value == "":
            continue
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise ValueError("malformed provider transaction identity")
        value = str(value).strip()
        if not value or len(value) > 200:
            raise ValueError("malformed provider transaction identity")
        return value
    return None


def _text(item: Dict[str, Any], key: str) -> str:
    """An optional FMP string: absent or null both become "" (stripped otherwise).

    FMP marks missing values with "" or null; the collector accepts both. ""
    keeps the legacy JSON string-typed, and the importer normalizes "" and None
    to the same missing value, so the stored record and its hash do not depend
    on which form FMP sent. A required field left "" fails record validation.
    """
    value = item.get(key)
    return value.strip() if isinstance(value, str) else ""


def parse_trades(
    raw_data: List[Dict[str, Any]], *, preserve_provenance: bool = True
) -> List[Dict[str, Any]]:
    """
    Cleans raw FMP response into structured dicts.
    Skips entries with no ticker symbol.
    """
    trades = []

    for item in raw_data:
        ticker = (item.get("symbol") or "").strip().upper()
        if not ticker or ticker == "N/A":
            continue

        trade_date_str = item.get("transactionDate") or ""
        disclosure_date_str = item.get("disclosureDate") or ""

        days_to_disclose = None
        try:
            if trade_date_str and disclosure_date_str:
                t = datetime.strptime(trade_date_str, "%Y-%m-%d").date()
                d = datetime.strptime(disclosure_date_str, "%Y-%m-%d").date()
                days_to_disclose = (d - t).days
        except ValueError:
            pass

        first = _text(item, "firstName")
        last = _text(item, "lastName")

        trades.append(
            {
                "politician_name": f"{first} {last}".strip(),
                "chamber": item.get("_chamber", ""),
                "ticker": ticker,
                "asset_description": _text(item, "assetDescription"),
                "asset_type": _text(item, "assetType"),
                "trade_type": _text(item, "type"),
                "amount_range": _text(item, "amount"),
                "transaction_date": trade_date_str,
                "disclosure_date": disclosure_date_str,
                "days_to_disclose": days_to_disclose,
                "district": _text(item, "district"),
                "source_link": _text(item, "link"),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        if preserve_provenance:
            trades[-1].update(
                data_source="fmp", provider_record_id=_provider_record_id(item)
            )

    return trades


def legacy_deduplicate(trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Removes repeated trades from the legacy JSON output.
    Key: politician + ticker + transaction_date + trade_type
    """
    seen = {}
    result = []

    for trade in trades:
        key = (
            trade["politician_name"].lower(),
            trade["ticker"],
            trade["transaction_date"],
            trade["trade_type"].lower(),
        )
        if key not in seen:
            seen[key] = True
            result.append(trade)

    removed = len(trades) - len(result)
    logger.info("congress_deduplicated removed=%d remaining=%d", removed, len(result))
    return result


def deduplicate_proven_repeats(trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove proven repeats only: same provider transaction ID and same content.

    Amount, disclosure date, filing, chamber and asset fields remain identity
    dimensions. A filing URL alone is not a transaction ID. Rows without an
    explicit provider transaction ID retain their multiplicity, and providers
    never merge: a politician/ticker/date/type match cannot prove two reported
    transactions are the same. This deliberately favors preservation over
    speculative cross-provider deduplication.
    """
    seen = set()
    result = []

    for trade in trades:
        identity = trade.get("provider_record_id")
        provider = trade.get("data_source")
        if identity and provider:
            # Restamping a fetch must not change identity; all other fields
            # participate so conflicting reports of one ID remain visible.
            key = json.dumps(
                {k: v for k, v in trade.items() if k != "fetched_at"}, sort_keys=True
            )
            if key in seen:
                continue
            seen.add(key)
        result.append(trade)

    removed = len(trades) - len(result)
    logger.info("congress_deduplicated removed=%d remaining=%d", removed, len(result))
    return result


def filter_recent(trades: List[Dict[str, Any]], years: int = 2) -> List[Dict[str, Any]]:
    """
    Filters trades to only include records within the last N years.
    Keeps signals fresh and relevant for Perennial users.
    Default: 2 years — enough volume, recent enough to be actionable.
    """
    cutoff = datetime.now(timezone.utc).date()
    cutoff = cutoff.replace(year=cutoff.year - years)

    recent = []
    skipped = 0

    for t in trades:
        try:
            trade_date = datetime.strptime(t["transaction_date"], "%Y-%m-%d").date()
            if trade_date >= cutoff:
                recent.append(t)
            else:
                skipped += 1
        except ValueError:
            skipped += 1

    logger.info("congress_date_filter years=%d retained=%d", years, len(recent))
    return recent


# ─────────────────────────────────────────────────────────
# ANALYZE
# ─────────────────────────────────────────────────────────


def legacy_analyze_trades(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Counts purchase mentions per ticker.
    This is the only signal Perennial needs from Congress data.
    """
    purchases = [t for t in trades if "purchase" in t["trade_type"].lower()]
    sales = [t for t in trades if "sale" in t["trade_type"].lower()]

    purchase_tickers = [t["ticker"] for t in purchases]

    return {
        "total_trades": len(trades),
        "total_purchases": len(purchases),
        "total_sales": len(sales),
        "top_tickers": Counter(purchase_tickers).most_common(20),
        "unique_tickers": len(set(purchase_tickers)),
    }


# ─────────────────────────────────────────────────────────
# SAVE — placeholder until DB is ready
# ─────────────────────────────────────────────────────────


def legacy_save_to_json(
    trades: List[Dict[str, Any]], filename: Optional[str | _Path] = None
) -> None:
    if filename is None:
        output_dir = LOCAL_DATA_DIR
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = output_dir / "trades_congress.json"

    output = {
        "source": "Financial Modeling Prep (FMP)",
        "endpoints": [f"{BASE_URL}/senate-latest", f"{BASE_URL}/house-latest"],
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "total_trades": len(trades),
        "trades": trades,
    }
    with open(filename, "w") as f:
        json.dump(output, f, indent=2)
    logger.info("congress_saved records=%d", len(trades))


# ─────────────────────────────────────────────────────────
# PRINT
# ─────────────────────────────────────────────────────────


def legacy_print_summary(
    trades: List[Dict[str, Any]], analysis: Dict[str, Any]
) -> None:
    logger.info(
        "congress_summary trades=%d purchases=%d sales=%d tickers=%d",
        analysis["total_trades"],
        analysis["total_purchases"],
        analysis["total_sales"],
        analysis["unique_tickers"],
    )


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────


def legacy_run(
    provider_settings: "ProviderSettings | None" = None,
) -> Optional[List[Dict[str, Any]]]:
    # The compatibility branch is only for existing JSON callers that set the
    # module symbol themselves. Normal runs resolve settings at invocation.
    legacy_key = provider_settings is None and FMP_API_KEY is not None
    if legacy_key:
        key = FMP_API_KEY
    else:
        if provider_settings is None:
            from src.settings import load_provider_settings

            provider_settings = load_provider_settings()
        provider_settings.require_provider_keys("fmp")
        # require_provider_keys has just rejected a missing key.
        key = cast("SecretStr", provider_settings.fmp_api_key).get_secret_value()
    page_limit = getattr(provider_settings, "fmp_max_pages", MAX_PAGES)

    logger.info("Starting Congress trades fetch...")

    # Fetch both chambers
    if legacy_key:
        fmp_senate = legacy_fetch_chamber_trades("senate")
        fmp_house = legacy_fetch_chamber_trades("house")
    else:
        fmp_senate = legacy_fetch_chamber_trades(
            "senate", api_key=key, max_pages=page_limit
        )
        fmp_house = legacy_fetch_chamber_trades(
            "house", api_key=key, max_pages=page_limit
        )

    all_raw = fmp_senate + fmp_house
    logger.info("Total raw records: %s", len(all_raw))

    # Legacy file output keeps fab7188 fields; database collection keeps the
    # provider transaction identity and provenance via the default parsers.
    fmp_trades = parse_trades(all_raw, preserve_provenance=False)

    trades = legacy_deduplicate(fmp_trades)

    # Filter to recent 2 years only
    trades = filter_recent(trades, years=2)

    if not trades:
        logger.info("No valid trades. Exiting.")
        return None

    # Analyze + print + save
    analysis = legacy_analyze_trades(trades)
    legacy_print_summary(trades, analysis)
    legacy_save_to_json(trades)

    logger.info("Done.")
    return trades


# scheduler/cron.py calls run() and cannot change; delete with cron.py (M7).
run = legacy_run


if __name__ == "__main__":
    configure_logging()
    legacy_run()
