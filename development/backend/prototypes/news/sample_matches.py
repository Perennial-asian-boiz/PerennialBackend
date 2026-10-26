"""S1-03 AC 3: draw 20 article-to-ticker matches for a hand check. Prints and writes a CSV.

Runs the production collector and match-v1 live against Finnhub for the prototype
tickers, without a database. Company names come from SEC's company list (the
production job uses the security and company-profile names instead).

    cd development/backend/prototypes/news
    ../../../../.venv/bin/python sample_matches.py

Needs FINNHUB_API_KEY and SEC_USER_AGENT in development/backend/.env. Fill the
"correct" column with y or n; the criterion is at least 18 of 20 correct.
"""

import csv
import json
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import dotenv_values

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))  # development/backend
from src.services.news.finnhub import FinnhubNewsClient  # noqa: E402
from src.services.news.job import collect_news  # noqa: E402

SEED, SAMPLE = 20261026, 20


def main() -> None:
    env = dotenv_values(HERE.parents[1] / ".env")
    tickers = sorted(json.loads((HERE.parent / "fundamentals" / "tickers.json").read_text()))
    sec = requests.get("https://www.sec.gov/files/company_tickers.json",
                       headers={"User-Agent": env["SEC_USER_AGENT"]}, timeout=30).json()
    titles = {r["ticker"].upper(): r["title"] for r in sec.values()}
    names = {t: [titles[t.replace(".", "-")]] for t in tickers if t.replace(".", "-") in titles}
    client = FinnhubNewsClient(requests.Session(), env["FINNHUB_API_KEY"])
    got = collect_news(client, tickers, names, {}, datetime.now(timezone.utc).date())
    records = got.outcome.records
    statuses = {c.symbol: c.status for c in got.coverage}
    print(f"{len(tickers)} tickers: {list(statuses.values()).count('covered')} covered, "
          f"{list(statuses.values()).count('no_coverage')} no coverage, "
          f"{list(statuses.values()).count('error')} errors; {len(records)} matched pairs")
    print(f"match rules: ticker {sum(r['match_rule'] == 'ticker' for r in records)}, "
          f"name {sum(r['match_rule'] == 'name' for r in records)}")
    sample = random.Random(SEED).sample(records, min(SAMPLE, len(records)))
    out = HERE / "match_sample.csv"
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ticker", "company", "match_rule", "headline", "outlet", "published_at", "url", "correct"])
        for r in sample:
            w.writerow([r["ticker"], (names.get(r["ticker"]) or [""])[0], r["match_rule"], r["headline"],
                        r["outlet"], r["published_at"].isoformat(), r["url"], ""])
            print(f"{r['ticker']:6} {r['match_rule']:6} {r['headline'][:95]}")
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
