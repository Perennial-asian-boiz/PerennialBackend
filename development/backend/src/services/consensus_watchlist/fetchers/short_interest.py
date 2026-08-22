"""
Perennial — Short Interest Fetcher
File: services/consensus_watchlist/fetchers/short_interest.py
Team: Asian Boiz | CSULB Senior Project 2026

PURPOSE:
    Fetches short interest data for tickers present in
    trades_congress.json (fmp.py) and ark_holdings.json (ark.py).

    Short interest is an ATTENTION/RISK signal — high short interest
    means the market is actively betting against a stock. This can
    mean either bearish sentiment OR short-squeeze potential.
    It's a scorer signal in consensus.py, not a gate.

SOURCE:
    Nasdaq public quote API — free, no key, no OAuth needed
    Endpoint: https://api.nasdaq.com/api/quote/{symbol}/short-interest?assetClass=stocks
    Covers all NASDAQ + NYSE listed stocks
    Updated bi-monthly (mid-month + end-of-month settlement dates)
    Requires a browser User-Agent header to avoid being blocked

    NOTE: This replaces the FINRA consolidated API which requires
    OAuth 2.0 organization credentials. The Nasdaq public endpoint
    provides the same data (sourced from FINRA) without auth.

SCHEDULE:
    Runs bi-weekly (1st and 15th) via scheduler/cron.py
    after fmp.py and ark.py, before consensus.py

OUTPUT:
    database/local_data/short_interest.json
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Any
import requests

# ─────────────────────────────────────────────────────────
# CONFIG & LOGGING
# ─────────────────────────────────────────────────────────

logger = logging.getLogger("short_interest")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[short_interest] %(levelname)s: %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

NASDAQ_URL = "https://api.nasdaq.com/api/quote/{symbol}/short-interest?assetClass=stocks"

# Nasdaq blocks requests without a browser User-Agent
HEADERS = {
    "accept": "application/json",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
}

REQUEST_TIMEOUT = 15
MAX_RETRIES     = 3
REQUEST_DELAY   = 0.5   # be respectful to the public endpoint


# ─────────────────────────────────────────────────────────
# PATH HELPERS
# ─────────────────────────────────────────────────────────

def resolve_data_dir() -> Path:
    """
    Returns the database/local_data directory, creating it if needed.
    """
    curr = Path(__file__).resolve()
    project_root = None
    for p in [curr] + list(curr.parents):
        if (p / "development" / "backend").exists():
            project_root = p
            break
    if not project_root:
        project_root = curr.parents[6]

    data_dir = project_root / "development" / "database" / "local_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def get_input_file(filename: str) -> Optional[Path]:
    """Finds an input JSON file in the data directory."""
    path = resolve_data_dir() / filename
    return path if path.exists() else None


# ─────────────────────────────────────────────────────────
# CANDIDATE UNIVERSE
# ─────────────────────────────────────────────────────────

def normalize_ticker(ticker: str) -> Optional[str]:
    """Normalizes a ticker symbol. Rejects invalid symbols."""
    if not ticker or not isinstance(ticker, str):
        return None
    cleaned = ticker.strip().upper()
    if not cleaned or cleaned in ("N/A", "NONE", "NULL", "--", "NAN"):
        return None
    return cleaned


def get_candidate_tickers() -> List[str]:
    """
    Builds deduplicated union of tickers from:
      - trades_congress.json
      - ark_holdings.json
    Only tickers in these files are checked — no market-wide scan.
    """
    candidates: Set[str] = set()

    # Congress tickers
    c_path = get_input_file("trades_congress.json")
    if c_path:
        try:
            with open(c_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for t in data.get("trades", []):
                norm = normalize_ticker(t.get("ticker"))
                if norm:
                    candidates.add(norm)
            logger.info("Loaded candidate tickers from Congress trades")
        except Exception as e:
            logger.warning(f"Error loading Congress trades: {e}")

    # ARK tickers
    a_path = get_input_file("ark_holdings.json")
    if a_path:
        try:
            with open(a_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for h in data.get("holdings", []):
                norm = normalize_ticker(h.get("ticker"))
                if norm:
                    candidates.add(norm)
            logger.info("Loaded candidate tickers from ARK holdings")
        except Exception as e:
            logger.warning(f"Error loading ARK holdings: {e}")

    sorted_candidates = sorted(candidates)
    logger.info(f"Total deduplicated candidate universe: {len(sorted_candidates)} tickers")
    return sorted_candidates


# ─────────────────────────────────────────────────────────
# PARSE — Convert Nasdaq strings to numbers
# ─────────────────────────────────────────────────────────

def _to_int(value: Any) -> Optional[int]:
    """Converts '292,667,375' → 292667375. Returns None if invalid."""
    if value is None:
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _to_float(value: Any) -> Optional[float]:
    """Converts a days-to-cover value to float. Returns None if invalid."""
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _normalize_date(date_str: str) -> str:
    """Converts MM/DD/YYYY → YYYY-MM-DD. Leaves other formats as-is."""
    if not date_str:
        return ""
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return date_str.strip()


def parse_short_interest(ticker: str, raw: dict) -> Optional[Dict[str, Any]]:
    """
    Parses Nasdaq short interest response into canonical schema.
    Takes the MOST RECENT settlement row (rows[0]) as current value,
    and keeps the full history for trend analysis later.
    """
    try:
        table = raw.get("data", {}).get("shortInterestTable")
        if not table:
            return None

        rows = table.get("rows", [])
        if not rows:
            return None

        # rows[0] is the most recent settlement date
        latest = rows[0]

        # Build compact history (date + short interest) for trend use later
        history = []
        for r in rows:
            history.append({
                "settlement_date":       _normalize_date(r.get("settlementDate", "")),
                "short_interest":        _to_int(r.get("interest")),
                "avg_daily_volume":      _to_int(r.get("avgDailyShareVolume")),
                "days_to_cover":         _to_float(r.get("daysToCover")),
            })

        return {
            "ticker":            ticker,
            "short_interest":    _to_int(latest.get("interest")),
            "avg_daily_volume":  _to_int(latest.get("avgDailyShareVolume")),
            "days_to_cover":     _to_float(latest.get("daysToCover")),
            "settlement_date":   _normalize_date(latest.get("settlementDate", "")),
            "history":           history,
            "data_source":       "nasdaq",
            "fetched_at":        datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        logger.warning(f"Parse error for {ticker}: {e}")
        return None


# ─────────────────────────────────────────────────────────
# FETCH — One ticker
# ─────────────────────────────────────────────────────────

def fetch_short_interest(ticker: str, session: requests.Session) -> Optional[Dict[str, Any]]:
    """
    Fetches short interest for one ticker from Nasdaq public API.
    Returns parsed record or None.
    """
    url = NASDAQ_URL.format(symbol=ticker)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 429:
                logger.warning(f"Rate limited on {ticker} — waiting (attempt {attempt})")
                time.sleep(2 ** attempt)
                continue

            if resp.status_code != 200:
                return None

            data = resp.json()

            # Nasdaq returns data:null for tickers with no short interest record
            if not data.get("data"):
                return None

            return parse_short_interest(ticker, data)

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on {ticker} (attempt {attempt}/{MAX_RETRIES})")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error on {ticker}: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error on {ticker}: {e}")
            return None

        if attempt < MAX_RETRIES:
            time.sleep(attempt)

    return None


# ─────────────────────────────────────────────────────────
# SAVE
# ─────────────────────────────────────────────────────────

def save_to_json(records: List[Dict[str, Any]], errors: List[str]) -> None:
    """Saves short interest records to database/local_data/short_interest.json"""
    output_dir = resolve_data_dir()
    output_file = output_dir / "short_interest.json"

    payload = {
        "schema_version": "1.0",
        "source":         "nasdaq",
        "fetched_at":     datetime.now(timezone.utc).isoformat(),
        "total_records":  len(records),
        "note": (
            "Short interest is an attention/risk signal for consensus.py. "
            "High days_to_cover can mean bearish sentiment OR squeeze potential."
        ),
        "records":        records,
        "errors":         errors,
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved {len(records)} records → {output_file}")


# ─────────────────────────────────────────────────────────
# PRINT SUMMARY
# ─────────────────────────────────────────────────────────

def print_summary(records: List[Dict[str, Any]]) -> None:
    print("\n" + "="*60)
    print("  PERENNIAL — Short Interest Summary")
    print("="*60)
    print(f"  Total tickers with data : {len(records)}")

    if records:
        # Sort by days to cover — highest = most short pressure
        ranked = sorted(
            [r for r in records if r.get("days_to_cover") is not None],
            key=lambda x: x["days_to_cover"],
            reverse=True
        )

        print("\n  📊 Highest days-to-cover (most short pressure):")
        print("  " + "-"*40)
        for r in ranked[:15]:
            dtc = r["days_to_cover"]
            si  = r["short_interest"] or 0
            print(f"  {r['ticker']:<8} {dtc:>6.2f} days to cover | "
                  f"{si:>14,} shares short | {r['settlement_date']}")
    print()


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────

def run() -> Dict[str, Any]:
    """
    Main entry point. Called by scheduler/cron.py bi-weekly.
    Can also be run manually: py short_interest.py
    """
    logger.info("Starting Nasdaq Short Interest fetcher...")

    tickers = get_candidate_tickers()
    if not tickers:
        logger.warning("No candidate tickers found — run fmp.py and ark.py first")
        save_to_json([], [])
        return {"records": [], "errors": []}

    records: List[Dict[str, Any]] = []
    errors:  List[str] = []

    session = requests.Session()

    logger.info(f"Fetching short interest for {len(tickers)} tickers...")
    logger.info(f"Est. time: ~{len(tickers) * REQUEST_DELAY:.0f} seconds")

    for i, ticker in enumerate(tickers, 1):
        record = fetch_short_interest(ticker, session)
        if record:
            records.append(record)
            dtc = record.get("days_to_cover")
            dtc_str = f"{dtc:.2f}" if dtc is not None else "N/A"
            print(f"  ✅ {ticker:<8} {dtc_str} days to cover [{i}/{len(tickers)}]")
        else:
            # Not an error — many tickers just have no short interest record
            pass

        time.sleep(REQUEST_DELAY)

    records.sort(key=lambda r: r["ticker"])

    print_summary(records)
    save_to_json(records, errors)

    logger.info(f"Short interest complete. {len(records)} records, {len(errors)} errors.")
    return {"records": records, "errors": errors}


if __name__ == "__main__":
    run()