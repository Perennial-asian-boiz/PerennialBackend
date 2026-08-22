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
import os
from datetime import datetime, timezone
from collections import Counter
from dotenv import load_dotenv
from pathlib import Path

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────

dotenv_path = Path(__file__).resolve().parents[4] / ".env"
load_dotenv(dotenv_path=dotenv_path)

FMP_API_KEY = os.getenv("FMP_API_KEY", "YOUR_FMP_KEY_HERE")
BASE_URL = "https://financialmodelingprep.com/stable"
MAX_PAGES = 3  # 20 trades per page = ~60 trades per chamber

# Senate Stock Watcher — free, no key, 8,350+ records back to 2019
SENATE_WATCHER_URL = (
    "https://raw.githubusercontent.com/timothycarambat/"
    "senate-stock-watcher-data/master/aggregate/all_transactions.json")


# ─────────────────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────────────────

def fetch_chamber_trades(chamber: str):
    """
    Fetches trades for one chamber.
    chamber = "senate" or "house"
    """
    endpoint = f"{BASE_URL}/{chamber}-latest"
    all_trades = []

    for page in range(MAX_PAGES):
        try:
            resp = requests.get(
                endpoint,
                params={"page": page, "limit": 20, "apikey": FMP_API_KEY},
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

            if not data:
                break

            for item in data:
                item["_chamber"] = chamber.capitalize()

            all_trades.extend(data)
            print(f"[fmp] {chamber.capitalize()} page {page + 1} — {len(data)} trades")

        except Exception as e:
            print(f"[fmp] ❌ {chamber} page {page} error: {e}")
            break

    return all_trades

def fetch_senate_watcher():
    """
    Fetches full Senate trade history from Senate Stock Watcher.
    Free, no key needed. 8,350+ records going back to 2019.
    """
    try:
        print(f"[fmp] Fetching Senate Stock Watcher (full history)...")
        resp = requests.get(SENATE_WATCHER_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for item in data:
            item["_chamber"] = "Senate"
            item["_source"]  = "senate_watcher"

        print(f"[fmp] Senate Stock Watcher — {len(data)} records")
        return data

    except Exception as e:
        print(f"[fmp] ❌ Senate Stock Watcher failed: {e}")
        return []


# ─────────────────────────────────────────────────────────
# PARSE
# ─────────────────────────────────────────────────────────

def parse_trades(raw_data: list):
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

    return trades

def parse_watcher_trade(item: dict):
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

    return {
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

def deduplicate(trades: list):
    """
    Removes duplicate trades that appear in both FMP and Senate Watcher.
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
        base = Path(__file__).resolve().parents[5]  # goes up to development/
        output_dir = base / "database" / "local_data"
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

def run():
    if FMP_API_KEY == "YOUR_FMP_KEY_HERE":
        print("[fmp] ⚠️  No FMP_API_KEY set in .env")
        return

    print("[fmp] Starting Congress trades fetch...")

    # Fetch all sources
    fmp_senate = fetch_chamber_trades("senate")
    fmp_house  = fetch_chamber_trades("house")
    sw_senate  = fetch_senate_watcher()

    all_raw = fmp_senate + fmp_house + sw_senate
    print(f"\n[fmp] Total raw records: {len(all_raw)}")

    # Parse — FMP records already parsed by existing parse_trades()
    fmp_trades = parse_trades(fmp_senate + fmp_house)

    # Parse Senate Watcher records separately
    sw_trades = []
    skipped = 0
    for item in sw_senate:
        parsed = parse_watcher_trade(item)
        if parsed:
            sw_trades.append(parsed)
        else:
            skipped += 1
    print(f"[fmp] Senate Watcher: {len(sw_trades)} valid, {skipped} skipped (no ticker)")

    # Combine and deduplicate
    all_trades = fmp_trades + sw_trades
    trades = deduplicate(all_trades)

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