"""
Show one database collection on its own: Congress House trades from the
Clerk of the House, through collectors.collect_congress — the same function
the database pipeline calls. Prints the outcome, the unit diagnostics, and the
most-bought tickers. Needs no API key and writes nothing to the database.

USAGE (from development/backend):
    python scripts/show_congress_collection.py
"""

import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # development/backend

from src.ingestion.collectors import collect_congress  # noqa: E402


def main():
    print("Collecting House trades from the Clerk of the House...\n")
    outcome = collect_congress(house_source="house_clerk", include_fmp_senate=False)

    print(f"Succeeded : {outcome.succeeded}")
    print(f"Error     : {outcome.error_code}")
    print(f"Coverage  : {outcome.coverage}")
    print("\nDiagnostics (one unit = one index or one report):")
    shown = {k: v for k, v in (outcome.diagnostics or {}).items() if k != "failures"}
    print(json.dumps(shown, indent=2))
    if outcome.diagnostics and outcome.diagnostics.get("failures"):
        print("Failures  :", outcome.diagnostics["failures"])
    if not outcome.succeeded:
        return 1

    records = outcome.records or []
    buys = [r for r in records if "purchase" in r["trade_type"].lower()]
    print(f"\nRecords   : {len(records):,} stock trades, {len(buys):,} purchases")
    print("Envelope  :", json.dumps(outcome.envelope, indent=2))

    buyers = Counter()
    for ticker in {r["ticker"] for r in buys}:
        buyers[ticker] = len({r["politician_name"] for r in buys if r["ticker"] == ticker})
    print("\nTop 10 House purchase tickers by distinct members:")
    for ticker, count in buyers.most_common(10):
        print(f"  {ticker:<8} {count} member(s)")

    sample = max(buys, key=lambda r: r["disclosure_date"] or "")
    print("\nNewest purchase (check it against the filing):")
    for key in ("politician_name", "district", "ticker", "trade_type", "amount_range",
                "transaction_date", "disclosure_date", "source_link"):
        print(f"  {key:<17}: {sample.get(key)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
