"""
SOURCE:
    Financial Modeling Prep (FMP) stable endpoints
    Senate: https://financialmodelingprep.com/stable/senate-latest
    House:  https://financialmodelingprep.com/stable/house-latest
    Free tier: 250 requests/day

SCHEDULE:
    Runs daily at 9:00am via scheduler/cron.py
"""

import requests
import json
from datetime import datetime, timezone
from collections import Counter
from typing import TYPE_CHECKING
# ── shared path resolution (see paths.py) ──
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(next(
    p for p in _Path(__file__).resolve().parents if p.name == "consensus_watchlist"
)))
from paths import LOCAL_DATA_DIR, local_data_dir

if TYPE_CHECKING:
    from src.settings import ProviderSettings

from pathlib import Path

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────

# Historical JSON callers and synthetic contract tests may set this explicitly.
# Importing the fetcher never reads a key or a dotenv file.
FMP_API_KEY: str | None = None
BASE_URL = "https://financialmodelingprep.com/stable"
MAX_PAGES = 50  # Safety cap per chamber; full final page is not a complete result.

# Senate Stock Watcher — its newest trade is 2020-12-02, so the 2-year filter
# drops every record. run() no longer downloads it. The URL and
# parse_watcher_trade stay because src/ingestion/collectors.py still uses them.
SENATE_WATCHER_URL = (
    "https://raw.githubusercontent.com/timothycarambat/"
    "senate-stock-watcher-data/master/aggregate/all_transactions.json")


# ─────────────────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────────────────

def fetch_chamber_trades(
    chamber: str, *, api_key: str | None = None, max_pages: int | None = None
):
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
    all_trades = []

    for page in range(page_limit):
        try:
            resp = requests.get(
                endpoint,
                params={"page": page, "limit": 20, "apikey": key},
                timeout=15
            )

            if resp.status_code == 401:
                print(f"[fmp] ❌ Unauthorized — check FMP_API_KEY")
                break
            if resp.status_code == 402:
                print(f"[fmp] ⚠️  Free tier limit on {chamber} page {page} — stopping")
                break
            if resp.status_code == 403:
                print(f"[fmp] ❌ 403 on {chamber} — may need paid tier")
                break

            resp.raise_for_status()
            data = resp.json()

            for item in data:
                item["_chamber"] = chamber.capitalize()

            all_trades.extend(data)
            if data:
                print(f"[fmp] {chamber.capitalize()} page {page + 1} — {len(data)} trades")

        except Exception as e:
            print(f"[fmp] ❌ {chamber} page {page} error: {e}")
            break

        if len(data) < 20:
            break
        if page == page_limit - 1:
            raise RuntimeError("FMP pagination exhausted")

    return all_trades

# ─────────────────────────────────────────────────────────
# PARSE
# ─────────────────────────────────────────────────────────

def _provider_record_id(item):
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


def parse_trades(raw_data: list, *, preserve_provenance: bool = True):
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

        first = item.get("firstName", "").strip()
        last = item.get("lastName", "").strip()

        trades.append({
            "politician_name": f"{first} {last}".strip(),
            "chamber": item.get("_chamber", ""),
            "ticker": ticker,
            "asset_description": item.get("assetDescription", "").strip(),
            "asset_type": item.get("assetType", "").strip(),
            "trade_type": item.get("type", "").strip(),
            "amount_range": item.get("amount", "").strip(),
            "transaction_date": trade_date_str,
            "disclosure_date": disclosure_date_str,
            "days_to_disclose": days_to_disclose,
            "district": item.get("district", "").strip(),
            "source_link": item.get("link", "").strip(),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        })
        if preserve_provenance:
            trades[-1].update(data_source="fmp", provider_record_id=_provider_record_id(item))

    return trades

def parse_watcher_trade(item: dict, *, preserve_provenance: bool = True):
    """
    Parses Senate Stock Watcher format into standard Perennial format.
    Different from FMP — uses MM/DD/YYYY dates and different field names.
    """
    ticker = (item.get("ticker") or "").strip().upper()

    # Watcher uses "--" for unknown tickers
    if not ticker or ticker == "--":
        return None

    # Convert MM/DD/YYYY to YYYY-MM-DD
    trade_date_str = ""
    raw_date = item.get("transaction_date") or ""
    try:
        parsed = datetime.strptime(raw_date, "%m/%d/%Y")
        trade_date_str = parsed.strftime("%Y-%m-%d")
    except ValueError:
        trade_date_str = raw_date

    trade = {
        "politician_name":   item.get("senator", "").strip(),
        "chamber":           "Senate",
        "ticker":            ticker,
        "asset_description": item.get("asset_description", "").strip(),
        "asset_type":        item.get("asset_type", "").strip(),
        "trade_type":        item.get("type", "").strip(),
        "amount_range":      item.get("amount", "").strip(),
        "transaction_date":  trade_date_str,
        "disclosure_date":   "",
        "days_to_disclose":  None,
        "district":          "",
        "source_link":       item.get("ptr_link", "").strip(),
        "data_source":       "senate_watcher",
        "fetched_at":        datetime.now(timezone.utc).isoformat(),
    }
    if preserve_provenance:
        trade["provider_record_id"] = _provider_record_id(item)
    return trade

def deduplicate(trades: list):
    """
    Removes repeated trades from the legacy JSON output.
    Key: politician + ticker + transaction_date + trade_type
    """
    seen   = {}
    result = []

    for trade in trades:
        key = (
            trade["politician_name"].lower(),
            trade["ticker"],
            trade["transaction_date"],
            trade["trade_type"].lower()
        )
        if key not in seen:
            seen[key] = True
            result.append(trade)

    removed = len(trades) - len(result)
    print(f"[fmp] Deduplication: removed {removed} duplicates → {len(result)} unique")
    return result


def deduplicate_proven_repeats(trades: list):
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
            key = json.dumps({k: v for k, v in trade.items() if k != "fetched_at"}, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
        result.append(trade)

    removed = len(trades) - len(result)
    print(f"[fmp] Deduplication: removed {removed} duplicates → {len(result)} unique")
    return result

def filter_recent(trades: list, years: int = 2):
    """
    Filters trades to only include records within the last N years.
    Keeps signals fresh and relevant for Perennial users.
    Default: 2 years — enough volume, recent enough to be actionable.
    """
    cutoff = datetime.now(timezone.utc).date()
    cutoff = cutoff.replace(year=cutoff.year - years)

    recent  = []
    skipped = 0

    for t in trades:
        try:
            trade_date = datetime.strptime(
                t["transaction_date"], "%Y-%m-%d"
            ).date()
            if trade_date >= cutoff:
                recent.append(t)
            else:
                skipped += 1
        except ValueError:
            skipped += 1

    print(f"[fmp] Date filter ({years}yr window): "
          f"kept {len(recent)}, removed {skipped} older trades")
    return recent

# ─────────────────────────────────────────────────────────
# ANALYZE
# ─────────────────────────────────────────────────────────

def analyze_trades(trades: list):
    """
    Counts purchase mentions per ticker.
    This is the only signal Perennial needs from Congress data.
    """
    purchases = [t for t in trades if "purchase" in t["trade_type"].lower()]
    sales     = [t for t in trades if "sale" in t["trade_type"].lower()]

    purchase_tickers = [t["ticker"] for t in purchases]

    return {
        "total_trades":    len(trades),
        "total_purchases": len(purchases),
        "total_sales":     len(sales),
        "top_tickers":     Counter(purchase_tickers).most_common(20),
        "unique_tickers":  len(set(purchase_tickers)),
    }


# ─────────────────────────────────────────────────────────
# SAVE — placeholder until DB is ready
# ─────────────────────────────────────────────────────────

def save_to_json(trades: list, filename=None):
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
    print(f"[fmp] 💾 Saved {len(trades)} trades → {filename}")


# ─────────────────────────────────────────────────────────
# PRINT
# ─────────────────────────────────────────────────────────

def print_summary(trades: list, analysis: dict):
    print("\n" + "="*60)
    print("  PERENNIAL — Congress Trades Summary")
    print("="*60)
    print(f"  Total trades   : {analysis['total_trades']}")
    print(f"  Purchases      : {analysis['total_purchases']}")
    print(f"  Sales          : {analysis['total_sales']}")
    print(f"  Unique tickers : {analysis['unique_tickers']}")

    print("\n  📊 Top tickers — Congress PURCHASES:")
    print("  " + "-"*40)
    for ticker, count in analysis["top_tickers"]:
        bar = "█" * count
        print(f"  {ticker:<8} {bar:<15} {count} member{'s' if count > 1 else ''}")

    print("\n  📋 5 most recent trades:")
    print("  " + "-" * 40)
    for t in trades[:5]:
        print(f"  {t['politician_name']:<25} [{t['chamber']:<6}] "
              f"{t['ticker']:<8} {t['trade_type']:<12} {t['transaction_date']}")
    print()


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────

def run(provider_settings: "ProviderSettings | None" = None):
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
        key = provider_settings.fmp_api_key.get_secret_value()
    page_limit = getattr(provider_settings, "fmp_max_pages", MAX_PAGES)

    print("[fmp] Starting Congress trades fetch...")

    # Fetch both chambers
    if legacy_key:
        fmp_senate = fetch_chamber_trades("senate")
        fmp_house  = fetch_chamber_trades("house")
    else:
        fmp_senate = fetch_chamber_trades("senate", api_key=key, max_pages=page_limit)
        fmp_house  = fetch_chamber_trades("house", api_key=key, max_pages=page_limit)

    all_raw = fmp_senate + fmp_house
    print(f"\n[fmp] Total raw records: {len(all_raw)}")

    # Legacy file output keeps fab7188 fields; database collection keeps the
    # provider transaction identity and provenance via the default parsers.
    fmp_trades = parse_trades(all_raw, preserve_provenance=False)

    trades = deduplicate(fmp_trades)

    # Filter to recent 2 years only
    trades = filter_recent(trades, years=2)

    if not trades:
        print("[fmp] No valid trades. Exiting.")
        return

    # Analyze + print + save
    analysis = analyze_trades(trades)
    print_summary(trades, analysis)
    save_to_json(trades)

    print("[fmp] ✅ Done.")
    return trades

if __name__ == "__main__":
    run()
