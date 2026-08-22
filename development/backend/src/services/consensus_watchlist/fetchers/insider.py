"""
PURPOSE:
    Reads ticker lists from fmp.py and ark.py outputs,
    then checks each ticker for SEC Form 4 insider buying activity.

    BUCKET 1 — "Popular & Stable"
        Tickers from trades_congress.json (fmp.py output)
        Checks if insiders confirm what Congress is buying

    BUCKET 2 — "Affordable & Growing"
        Tickers from ark_holdings.json (ark.py output)
        Checks if insiders are buying ARK's growth picks

    NOTE on current market:
        If no insider buys are found, that is real market data —
        insiders may be selling after a run-up or receiving grants.
        ARK tickers still feed consensus.py as the Affordable &
        Growing candidate list regardless of insider activity.
        Insider buying just adds an extra confidence boost.

SOURCE:
    SecuritiesDB — Free, no key needed
    https://securitiesdb.com/api/v1/stocks/{ticker}/insider-activity
    Returns SEC Form 4 filings from EDGAR, refreshed daily

SCHEDULE:
    Runs daily at 9:05am (after fmp.py and ark.py)
    via scheduler/cron.py

OUTPUT:
    database/local_data/trades_insider.json
"""

import requests
import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import Counter, defaultdict
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────

dotenv_path = Path(__file__).resolve().parents[4] / ".env"
load_dotenv(dotenv_path=dotenv_path)

SECURITIES_URL      = "https://securitiesdb.com/api/v1/stocks"
LOOKBACK_DAYS       = 180    # 6 months
MIN_BUY_VALUE       = 10_000 # $10K minimum conviction filter
MAX_CONGRESS_TICKERS = 50
REQUEST_DELAY       = 0.5    # seconds between API calls


# ─────────────────────────────────────────────────────────
# LOAD — Bucket 1: Congress tickers from fmp.py
# ─────────────────────────────────────────────────────────

def load_congress_tickers():
    """
    Reads trades_congress.json produced by fmp.py.
    Returns top purchased tickers for Bucket 1.
    """
    base       = Path(__file__).resolve().parents[5]
    input_file = base / "database" / "local_data" / "trades_congress.json"

    if not input_file.exists():
        print("[insider] ❌ trades_congress.json not found — run fmp.py first")
        return []

    try:
        with open(input_file, "r") as f:
            data = json.load(f)

        trades    = data.get("trades", [])
        purchases = [
            t["ticker"] for t in trades
            if "purchase" in t.get("trade_type", "").lower()
            and t.get("ticker")
        ]

        ticker_counts = Counter(purchases).most_common(MAX_CONGRESS_TICKERS)
        tickers       = [t for t, _ in ticker_counts]
        print(f"[insider] Loaded {len(tickers)} Congress tickers (Bucket 1)")
        return tickers

    except Exception as e:
        print(f"[insider] ❌ Failed to load Congress tickers: {e}")
        return []


# ─────────────────────────────────────────────────────────
# LOAD — Bucket 2: ARK tickers from ark.py
# ─────────────────────────────────────────────────────────

def load_ark_tickers():
    """
    Reads ark_holdings.json produced by ark.py.
    Returns list of tickers for Bucket 2 (Affordable & Growing).
    Prioritizes multi-fund tickers (appear in 2+ ARK funds)
    as they represent stronger ARK conviction.
    """
    base       = Path(__file__).resolve().parents[5]
    input_file = base / "database" / "local_data" / "ark_holdings.json"

    if not input_file.exists():
        print("[insider] ❌ ark_holdings.json not found — run ark.py first")
        return []

    try:
        with open(input_file, "r") as f:
            data = json.load(f)

        holdings = data.get("holdings", [])

        # Sort: multi-fund first, then single fund
        multi  = [h["ticker"] for h in holdings if h["fund_count"] > 1]
        single = [h["ticker"] for h in holdings if h["fund_count"] == 1]
        tickers = multi + single

        print(f"[insider] Loaded {len(tickers)} ARK tickers "
              f"({len(multi)} multi-fund, {len(single)} single-fund) "
              f"(Bucket 2)")
        return tickers

    except Exception as e:
        print(f"[insider] ❌ Failed to load ARK tickers: {e}")
        return []


# ─────────────────────────────────────────────────────────
# FETCH — Insider activity for one ticker
# ─────────────────────────────────────────────────────────

def fetch_insider_activity(ticker: str):
    """
    Fetches insider transaction data from SecuritiesDB.
    Returns raw response dict or None on failure.
    """
    try:
        resp = requests.get(
            f"{SECURITIES_URL}/{ticker}/insider-activity",
            timeout=15
        )

        if resp.status_code == 404:
            return None
        if resp.status_code == 429:
            print(f"[insider] ⚠️  Rate limited — waiting 5s")
            time.sleep(5)
            return None
        if resp.status_code != 200:
            return None

        return resp.json()

    except Exception as e:
        print(f"[insider] ❌ {ticker} error: {e}")
        return None


# ─────────────────────────────────────────────────────────
# PARSE — Extract meaningful insider buys
# ─────────────────────────────────────────────────────────

def parse_insider_buys(ticker: str, raw: dict, bucket: str):
    """
    Extracts open market PURCHASE transactions only.

    SecuritiesDB transaction types:
        "Purchase" → open market buy with real money ✅ KEEP
        "Sale"     → insider selling                 ❌ SKIP
        "Grant"    → compensation grant, value=0     ❌ SKIP
        "Other"    → misc, usually value=0           ❌ SKIP

    Additional filters:
        value >= MIN_BUY_VALUE  → conviction buy only
        date >= cutoff          → within lookback window
    """
    buys = []

    try:
        insider_data = raw.get("data", {})
        transactions = insider_data.get(
            "insider_transactions", {}
        ).get("recent", [])

        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
        ).date()

        for txn in transactions:
            # Only open market purchases
            if txn.get("type") != "Purchase":
                continue

            # Date within lookback window
            date_str = txn.get("date", "")
            try:
                txn_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                if txn_date < cutoff:
                    continue
            except ValueError:
                continue

            # Must be real money — filters grants and option exercises
            value = txn.get("value", 0) or 0
            if value < MIN_BUY_VALUE:
                continue

            buys.append({
                "ticker":           ticker,
                "insider_name":     txn.get("insider", "").strip(),
                "transaction_type": "Purchase",
                "value":            value,
                "shares":           txn.get("shares", 0),
                "transaction_date": date_str,
                "bucket":           bucket,
                "data_source":      "securitiesdb",
                "fetched_at":       datetime.now(timezone.utc).isoformat(),
            })

    except Exception as e:
        print(f"[insider] ❌ Parse error {ticker}: {e}")

    return buys


# ─────────────────────────────────────────────────────────
# SCAN — Check a list of tickers for insider buys
# ─────────────────────────────────────────────────────────

def scan_tickers(tickers: list, bucket: str, label: str):
    """
    Loops through tickers checking each for insider buying.
    Returns flat list of all insider buy dicts found.
    """
    all_buys  = []
    found     = 0
    not_found = 0

    print(f"\n[insider] {label}")
    print(f"[insider] Checking {len(tickers)} tickers...")
    print(f"[insider] Est. time: ~{len(tickers) * REQUEST_DELAY:.0f}s\n")

    for i, ticker in enumerate(tickers, 1):
        raw = fetch_insider_activity(ticker)

        if raw:
            buys = parse_insider_buys(ticker, raw, bucket)
            if buys:
                all_buys.extend(buys)
                found += 1
                total = sum(b["value"] for b in buys)
                print(f"  ✅ {ticker:<8} {len(buys)} buy(s) | "
                      f"${total:,.0f} total [{i}/{len(tickers)}]")
            else:
                not_found += 1
        else:
            not_found += 1

        time.sleep(REQUEST_DELAY)

    print(f"\n[insider] Done — "
          f"{found} with buys, {not_found} with none")
    return all_buys


# ─────────────────────────────────────────────────────────
# ANALYZE
# ─────────────────────────────────────────────────────────

def analyze_insider_buys(all_buys: list):
    """
    Groups buys by ticker and calculates signal metrics.
    Multiple insiders buying same stock = cluster buy signal.
    """
    ticker_summary = defaultdict(lambda: {
        "ticker":            "",
        "bucket":            "",
        "insider_buy_count": 0,
        "total_buy_value":   0,
        "most_recent_buy":   "",
        "insiders":          [],
    })

    for buy in all_buys:
        ticker = buy["ticker"]
        s      = ticker_summary[ticker]
        s["ticker"]            = ticker
        s["bucket"]            = buy["bucket"]
        s["insider_buy_count"] += 1
        s["total_buy_value"]   += buy["value"]
        s["insiders"].append(buy["insider_name"])

        if not s["most_recent_buy"] or \
           buy["transaction_date"] > s["most_recent_buy"]:
            s["most_recent_buy"] = buy["transaction_date"]

    return sorted(
        ticker_summary.values(),
        key=lambda x: x["insider_buy_count"],
        reverse=True
    )


# ─────────────────────────────────────────────────────────
# SAVE
# ─────────────────────────────────────────────────────────

def save_to_json(all_buys: list, summary: list, filename=None):
    if filename is None:
        base       = Path(__file__).resolve().parents[5]
        output_dir = base / "database" / "local_data"
        output_dir.mkdir(parents=True, exist_ok=True)
        filename   = output_dir / "trades_insider.json"

    popular = [s for s in summary if s["bucket"] == "popular_stable"]
    growth  = [s for s in summary if s["bucket"] == "affordable_growing"]

    output = {
        "source":            "SecuritiesDB (SEC Form 4)",
        "fetched_at":        datetime.now(timezone.utc).isoformat(),
        "lookback_days":     LOOKBACK_DAYS,
        "min_buy_value":     MIN_BUY_VALUE,
        "total_buys":        len(all_buys),
        "tickers_with_buys": len(summary),
        "note": (
            "insider.py is a SCORER not a gate. "
            "Zero buys is valid — tickers still enter consensus.py "
            "from fmp.py and ark.py. Insider buying only adds "
            "bonus points to the confidence score in consensus.py."
        ),

        "popular_stable": {
            "count":   len(popular),
            "tickers": popular,
        },
        "affordable_growing": {
            "count":   len(growth),
            "tickers": growth,
        },
        "transactions": all_buys,
    }

    with open(filename, "w") as f:
        json.dump(output, f, indent=2)

    print(f"[insider] 💾 Saved {len(all_buys)} insider buys → {filename}")


# ─────────────────────────────────────────────────────────
# PRINT
# ─────────────────────────────────────────────────────────

def print_summary(summary: list, all_buys: list):
    popular = [s for s in summary if s["bucket"] == "popular_stable"]
    growth  = [s for s in summary if s["bucket"] == "affordable_growing"]

    print("\n" + "="*60)
    print("  PERENNIAL — Insider Buys Summary")
    print("="*60)
    print(f"  Total insider buys     : {len(all_buys)}")
    print(f"  Popular & Stable hits  : {len(popular)}")
    print(f"  Affordable & Growing   : {len(growth)}")
    print(f"  Lookback window        : {LOOKBACK_DAYS} days")
    print(f"  Min buy value          : ${MIN_BUY_VALUE:,}")

    if popular:
        print("\n  📊 Popular & Stable — Congress + Insider overlap:")
        print("  " + "-"*40)
        for s in popular[:10]:
            bar = "█" * min(s["insider_buy_count"], 10)
            print(f"  {s['ticker']:<8} {bar:<12} "
                  f"{s['insider_buy_count']} insider(s) | "
                  f"${s['total_buy_value']:>12,.0f} | "
                  f"last: {s['most_recent_buy']}")

    if growth:
        print("\n  🚀 Affordable & Growing — ARK + Insider signal:")
        print("  " + "-"*40)
        for s in growth[:10]:
            bar = "█" * min(s["insider_buy_count"], 10)
            print(f"  {s['ticker']:<8} {bar:<12} "
                  f"{s['insider_buy_count']} insider(s) | "
                  f"${s['total_buy_value']:>12,.0f} | "
                  f"last: {s['most_recent_buy']}")

    if not popular and not growth:
        print("\n  ℹ️  No insider buys found — this is fine.")
        print("     insider.py is a scorer, not a gate.")
        print("     Congress + ARK tickers still enter consensus.py.")
        print("     Insider buying adds +bonus to confidence score")
        print("     when present. Zero buys = no bonus, not no output.")


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────

def run():
    print("[insider] Starting insider trades fetch...")
    print("[insider] Reads from: trades_congress.json + ark_holdings.json")
    print("[insider] Checks via: SecuritiesDB (SEC Form 4)\n")

    all_buys = []

    # ── BUCKET 1: Congress tickers ───────────────────────
    congress_tickers = load_congress_tickers()
    if congress_tickers:
        stable_buys = scan_tickers(
            congress_tickers,
            bucket="popular_stable",
            label="BUCKET 1 — Popular & Stable"
        )
        all_buys.extend(stable_buys)

    # ── BUCKET 2: ARK tickers ────────────────────────────
    ark_tickers = load_ark_tickers()
    if ark_tickers:
        congress_set  = set(congress_tickers)
        unique_growth = [t for t in ark_tickers if t not in congress_set]
        print(f"[insider] {len(unique_growth)} unique ARK tickers "
              f"(excluding {len(ark_tickers) - len(unique_growth)} "
              f"already in Bucket 1)")

        growth_buys = scan_tickers(
            unique_growth,
            bucket="affordable_growing",
            label="BUCKET 2 — Affordable & Growing"
        )
        all_buys.extend(growth_buys)

    # ── Analyze + Print + Save ───────────────────────────
    summary = analyze_insider_buys(all_buys)
    print_summary(summary, all_buys)
    save_to_json(all_buys, summary)

    print("[insider] ✅ Done.")
    return all_buys


if __name__ == "__main__":
    run()