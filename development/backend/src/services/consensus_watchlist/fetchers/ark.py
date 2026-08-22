"""
PURPOSE:
    Fetches current holdings from ARK Invest ETFs and saves them
    as the "Affordable & Growing" candidate list for Perennial.

    ARK specifically invests in disruptive/early-stage companies —
    exactly the profile Perennial's "Affordable & Growing" persona
    is looking for. These tickers feed into:
        - insider.py  (checks insider buying on these tickers)
        - consensus.py (routes to Affordable & Growing bucket)
        - Cohen's sentiment pipeline (checks social/news sentiment)

SOURCE:
    arkfunds.io — Free third party ARK holdings API
    No key needed, updated daily
    Endpoint: https://arkfunds.io/api/v2/etf/holdings?symbol={FUND}

FUNDS TRACKED:
    ARKK — ARK Innovation ETF (broad disruptive tech)
    ARKQ — ARK Autonomous Tech & Robotics ETF
    ARKG — ARK Genomic Revolution ETF
    ARKW — ARK Next Generation Internet ETF

SCHEDULE:
    Runs daily at 9:02am via scheduler/cron.py
    (after fmp.py, before insider.py)

OUTPUT:
    database/local_data/ark_holdings.json

UPGRADE PATH:
    When school funding is available, supplement with
    FMP paid screener for broader small/mid cap coverage:
    GET https://financialmodelingprep.com/stable/company-screener
    params: marketCapLowerThan=5B, priceLowerThan=50
"""
import requests
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────

ARK_API_URL = "https://arkfunds.io/api/v2/etf/holdings"

# Funds to pull — each targets a different growth sector
ARK_FUNDS = {
    "ARKK": "ARK Innovation ETF",
    "ARKQ": "ARK Autonomous Tech & Robotics ETF",
    "ARKG": "ARK Genomic Revolution ETF",
    "ARKW": "ARK Next Generation Internet ETF",
}

REQUEST_DELAY = 0.5  # seconds between API calls


# ─────────────────────────────────────────────────────────
# FETCH — Holdings for one ARK fund
# ─────────────────────────────────────────────────────────

def fetch_fund_holdings(symbol: str):
    """
    Fetches current holdings for one ARK ETF fund.
    Returns list of holding dicts or empty list on failure.
    """
    try:
        resp = requests.get(
            ARK_API_URL,
            params={"symbol": symbol},
            timeout=15
        )

        if resp.status_code == 429:
            print(f"[ark] ⚠️  Rate limited on {symbol} — waiting 5s")
            time.sleep(5)
            return []
        if resp.status_code != 200:
            print(f"[ark] ⚠️  {symbol} returned {resp.status_code}")
            return []

        data = resp.json()
        holdings = data.get("holdings", [])
        print(f"[ark] {symbol:<6} ({ARK_FUNDS[symbol]}) "
              f"— {len(holdings)} holdings")
        return holdings

    except Exception as e:
        print(f"[ark] ❌ {symbol} fetch failed: {e}")
        return []


# ─────────────────────────────────────────────────────────
# PARSE — Combine holdings across all funds
# ─────────────────────────────────────────────────────────

def parse_holdings(all_raw: dict):
    """
    Combines holdings from all ARK funds.
    If a ticker appears in multiple funds, merges them.
    Tags each ticker with which fund(s) hold it —
    appearing in multiple funds = stronger ARK conviction.

    Returns list of clean holding dicts.
    """
    ticker_map = defaultdict(lambda: {
        "ticker": "",
        "company": "",
        "funds": [],
        "fund_count": 0,
        "share_price": 0.0,
        "total_weight": 0.0,
        "bucket": "affordable_growing",
        "data_source": "ark_etf",
    })

    for symbol, holdings in all_raw.items():
        for h in holdings:
            ticker = (h.get("ticker") or "").strip().upper()
            if not ticker:
                continue

            entry = ticker_map[ticker]
            entry["ticker"] = ticker
            entry["company"] = h.get("company", "").strip()
            entry["share_price"] = h.get("share_price", 0.0)
            entry["total_weight"] += h.get("weight", 0.0)

            if symbol not in entry["funds"]:
                entry["funds"].append(symbol)
                entry["fund_count"] += 1

    # Sort by fund count (tickers in multiple funds = stronger signal)
    # then by total weight
    holdings_list = sorted(
        ticker_map.values(),
        key=lambda x: (x["fund_count"], x["total_weight"]),
        reverse=True
    )

    return holdings_list


# ─────────────────────────────────────────────────────────
# SAVE
# ─────────────────────────────────────────────────────────

def save_to_json(holdings: list, filename=None):
    """
    Saves ARK holdings to local JSON file.
    Replace with write_to_db() once DB is set up.
    """
    if filename is None:
        base = Path(__file__).resolve().parents[5]
        output_dir = base / "database" / "local_data"
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = output_dir / "ark_holdings.json"

    # Separate multi-fund vs single-fund tickers
    multi_fund = [h for h in holdings if h["fund_count"] > 1]
    single_fund = [h for h in holdings if h["fund_count"] == 1]

    output = {
        "source": "ARK Invest ETF Holdings (arkfunds.io)",
        "funds_tracked": list(ARK_FUNDS.keys()),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "total_tickers": len(holdings),
        "multi_fund_tickers": len(multi_fund),
        "single_fund_tickers": len(single_fund),
        "note": (
            "multi_fund_tickers appear in 2+ ARK funds = "
            "stronger conviction signal"
        ),
        "holdings": holdings,
    }

    with open(filename, "w") as f:
        json.dump(output, f, indent=2)

    print(f"[ark] 💾 Saved {len(holdings)} tickers → {filename}")


# ─────────────────────────────────────────────────────────
# PRINT
# ─────────────────────────────────────────────────────────

def print_summary(holdings: list):
    multi_fund = [h for h in holdings if h["fund_count"] > 1]
    single_fund = [h for h in holdings if h["fund_count"] == 1]

    print("\n" + "=" * 60)
    print("  PERENNIAL — ARK ETF Holdings Summary")
    print("=" * 60)
    print(f"  Total unique tickers  : {len(holdings)}")
    print(f"  In multiple ARK funds : {len(multi_fund)} "
          f"← stronger conviction")
    print(f"  In single ARK fund    : {len(single_fund)}")

    if multi_fund:
        print("\n  🚀 Multi-fund tickers (highest ARK conviction):")
        print("  " + "-" * 40)
        for h in multi_fund[:15]:
            funds_str = ", ".join(h["funds"])
            price_str = f"${h['share_price']:.2f}" \
                if h["share_price"] else "N/A"
            print(f"  {h['ticker']:<8} {price_str:<10} "
                  f"[{funds_str}]  {h['company'][:30]}")

    print("\n  📋 All tickers (first 20):")
    print("  " + "-" * 40)
    for h in holdings[:20]:
        price_str = f"${h['share_price']:.2f}" \
            if h["share_price"] else "N/A"
        print(f"  {h['ticker']:<8} {price_str:<10} "
              f"{len(h['funds'])} fund(s)  {h['company'][:30]}")
    print()


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────

def run():
    """
    Main function called by scheduler/cron.py daily at 9:02am.
    Can also be run manually: py ark.py
    Runs BEFORE insider.py which reads this output.
    """
    print("[ark] Starting ARK ETF holdings fetch...")
    print(f"[ark] Funds: {', '.join(ARK_FUNDS.keys())}\n")

    # Fetch all funds
    all_raw = {}
    for symbol in ARK_FUNDS:
        holdings = fetch_fund_holdings(symbol)
        if holdings:
            all_raw[symbol] = holdings
        time.sleep(REQUEST_DELAY)

    if not all_raw:
        print("[ark] ❌ No data returned from any ARK fund")
        return []

    print(f"\n[ark] Parsing and merging holdings...")

    # Parse and merge
    holdings = parse_holdings(all_raw)
    print(f"[ark] {len(holdings)} unique tickers across all funds")

    # Print summary
    print_summary(holdings)

    # Save
    save_to_json(holdings)

    print("[ark] ✅ Done. ark_holdings.json ready for insider.py")
    return holdings


if __name__ == "__main__":
    run()
