"""Prototype: rank watchlist companies by how unusual today's GDELT news coverage is.

Prototype, not production code: it is not scheduled, not tested in CI, and never
touches the database. Run it off the campus network; GDELT answers the campus IP
with HTTP 429.

    cd development/backend/prototypes
    ../../../.venv/bin/python gdelt_rank.py --no-tone             # all companies, ~50+ min
    ../../../.venv/bin/python gdelt_rank.py --no-tone --limit 10  # quick check
    ../../../.venv/bin/python gdelt_rank.py --show                # reprint from saved results

For each company it asks the GDELT DOC API for 3 months of daily article counts
(and tone) matching the company's names in stock-market articles, then compares the
most recent complete UTC day with the median of the same weekday over the previous 8 weeks.
Results are saved to gdelt_rank_results.json next to this file, so a run blocked by a
429 can be restarted and continues where it stopped. Commit that file after a run to
share the data with the team.
"""
import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median

import time

import requests

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))  # development/backend, so `src` imports resolve
from src.services.gdelt.client import DocApiClient, GdeltError, Pacer, parse_tone, parse_volume  # noqa: E402

RESULTS = HERE / "gdelt_rank_results.json"
THEME = "ECON_STOCKMARKET"

# ticker: (DOC API search terms, stand-in large cap?)  Quoted terms are exact phrases.
# Non-stand-ins are today's ARK holdings; stand-ins replace the Congress half until FMP data is available.
COMPANIES = {
    'TSLA': (['tesla'], False),
    'SPCX': (['spacex'], False),
    'TEM': (['"tempus ai"'], False),
    'CRCL': (['"Circle Internet"'], False),
    'COIN': (['coinbase', '"coinbase global"'], False),
    'CRSP': (['"CRISPR Therapeutics"'], False),
    'SHOP': (['shopify'], False),
    'TWST': (['"twist bioscience"'], False),
    'HOOD': (['robinhood', '"robinhood markets"'], False),
    'NVDA': (['nvidia'], False),
    'PLTR': (['palantir'], False),
    'TXG': (['"10x genomics"'], False),
    'CRWV': (['coreweave'], False),
    'AMZN': (['amazon'], False),
    'XYZ': (['"Block Inc"'], False),
    'BEAM': (['"beam therapeutics"'], False),
    'META': (['"Meta Platforms"'], False),
    'CBRS': (['Cerebras'], False),
    'AVGO': (['broadcom'], False),
    'NTLA': (['Intellia'], False),
    'AMD': (['"Advanced Micro Devices"', 'AMD'], False),
    'BLSH': (['"bullish exchange"'], False),
    'BMNR': (['"Bitmine"'], False),
    'GOOG': (['Alphabet', 'Google'], False),
    'RXRX': (['"Recursion Pharmaceuticals"'], False),
    'NET': (['cloudflare'], False),
    'RKLB': (['"rocket lab"'], False),
    'NTRA': (['natera'], False),
    'KTOS': (['"Kratos Defense"'], False),
    'BWXT': (['"bwx technologies"', 'bwxt'], False),
    'VCYT': (['veracyte'], False),
    'TSM': (['"Taiwan Semiconductor"', 'TSMC'], False),
    'ACHR': (['"archer aviation"'], False),
    'WGS': (['genedx'], False),
    'ILMN': (['illumina'], False),
    'TER': (['teradyne'], False),
    'XE': (['"x energy"'], False),
    'LLY': (['"Eli Lilly"'], False),
    'SOFI': (['sofi'], False),
    'PACB': (['"Pacific Biosciences"', 'PacBio'], False),
    'DE': (['"John Deere"', 'Deere'], False),
    'LHX': (['l3harris', '"l3harris technologies"'], False),
    'AVAV': (['aerovironment'], False),
    'TRMB': (['trimble'], False),
    'KMTUY': (['komatsu'], False),
    'SYM': (['symbotic'], False),
    'LUNR': (['"intuitive machines"'], False),
    'AUR': (['"aurora innovation"'], False),
    'ISRG': (['"intuitive surgical"'], False),
    'DASH': (['doordash'], False),
    'CCJ': (['cameco'], False),
    'CAT': (['caterpillar'], False),
    'ESLT': (['"Elbit Systems"'], False),
    'SNPS': (['synopsys'], False),
    'JOBY': (['"joby aviation"', 'joby'], False),
    'WRD': (['weride'], False),
    'BIDU': (['baidu'], False),
    'PONY': (['"pony ai"'], False),
    'BYDDY': (['BYD'], False),
    'OKLO': (['oklo'], False),
    'KDK': (['"kodiak ai"', '"kodiak robotics"'], False),
    'GH': (['"Guardant Health"'], False),
    'CDNA': (['caredx'], False),
    'SDGR': (['schrodinger'], False),
    'CMPS': (['"compass pathways"'], False),
    'NRIX': (['"Nurix Therapeutics"'], False),
    'ADPT': (['"adaptive biotechnologies"'], False),
    'PSNL': (['personalis'], False),
    'BFLY': (['"Butterfly Network"'], False),
    'IONS': (['"Ionis Pharmaceuticals"'], False),
    'ARCT': (['"Arcturus Therapeutics"'], False),
    'ALMR': (['"Alamar Biosciences"'], False),
    'PRME': (['"prime medicine"'], False),
    'MASS': (['"908 devices"'], False),
    'GENB': (['"generate biomedicines"'], False),
    'SCTX': (['"scribe therapeutics"'], False),
    'FRNM': (['freenome'], False),
    'KYMR': (['"Kymera Therapeutics"'], False),
    'QSI': (['"Quantum-Si"'], False),
    'DDOG': (['datadog'], False),
    'CRWD': (['crowdstrike'], False),
    'RBRK': (['rubrik'], False),
    'RBLX': (['roblox'], False),
    'GTLB': (['gitlab'], False),
    'TOST': (['"Toast Inc"'], False),
    'MELI': (['mercadolibre', '"mercado libre"'], False),
    'SPOT': (['spotify'], False),
    'SNOW': (['"Snowflake Inc"'], False),
    'NFLX': (['netflix'], False),
    'FIG': (['figma'], False),
    'ABNB': (['airbnb'], False),
    'GENI': (['"genius sports"'], False),
    'DKNG': (['draftkings'], False),
    'AAPL': (['"Apple Inc"', 'iPhone'], True),
    'MSFT': (['microsoft'], True),
    'JPM': (['JPMorgan'], True),
    'GS': (['"Goldman Sachs"'], True),
    'MS': (['"Morgan Stanley"'], True),
    'BLK': (['blackrock'], True),
    'XOM': (['Exxon', 'ExxonMobil'], True),
    'WMT': (['walmart'], True),
    'DIS': (['Disney'], True),
    'WBD': (['"Warner Bros"'], True),
    'BA': (['boeing'], True),
    'INTC': (['Intel'], True),
    'ORCL': (['Oracle'], True),
    'BAC': (['"Bank of America"'], True),
    'UNH': (['UnitedHealth'], True),
}


def build_query(terms, use_theme):
    words = terms[0] if len(terms) == 1 else "(" + " OR ".join(terms) + ")"
    theme = f" theme:{THEME}" if use_theme else ""
    return f"{words}{theme} sourcelang:english"


def fetch(client, query, with_tone):
    base = {"query": query, "timespan": "3m"}
    volume = parse_volume(client._get({**base, "mode": "timelinevolraw"}))
    tone = parse_tone(client._get({**base, "mode": "timelinetone"})) if with_tone else {}
    return {"volume": {str(d): v.article_count for d, v in volume.items()},
            "tone": {str(d): round(t, 3) for d, t in tone.items()}}


def trend(counts, day, weeks=8, min_weeks=4):
    """Compare a day with the same weekday over the previous `weeks` weeks (weekends vs weekends)."""
    window = [str(day - timedelta(weeks=n)) for n in range(1, weeks + 1)]
    seen = [counts[d] for d in window if d in counts]  # missing days are unknown, not zero
    if str(day) not in counts or len(seen) < min_weeks:
        return None
    base = median(seen)
    today = counts[str(day)]
    return today, base, (today / base if base else None)


def show(results, day, min_articles, use_theme=True):
    rows, thin = [], []
    for ticker, r in results.items():
        t = trend(r["volume"], day)
        if t is None or t[0] < min_articles:
            thin.append(ticker)
            continue
        today, base, ratio = t
        rows.append((ratio if ratio is not None else float("inf"), ticker, today, base, r["tone"].get(str(day))))
    rows.sort(reverse=True)
    stand_in = {t for t, (_, s) in COMPANIES.items() if s}
    scope = "stock-market articles" if use_theme else "all articles"
    print(f"\nGDELT coverage trend for {day:%A} {day} (UTC), {scope}. * = stand-in large cap")
    print(f"Usual = median of the previous 8 {day:%A}s.")
    print(f"{'rank':>4}  {'ticker':7} {'today':>6} {'usual':>6} {'x usual':>8} {'tone':>6}")
    for i, (ratio, ticker, today, base, tone) in enumerate(rows, 1):
        mark = "*" if ticker in stand_in else " "
        x = "new" if ratio == float("inf") else f"{ratio:.1f}"
        t = "" if tone is None else f"{tone:+.2f}"
        print(f"{i:>4}  {ticker:6}{mark} {today:>6} {base:>6.1f} {x:>8} {t:>6}")
    if thin:
        print(f"\nNot ranked (under {min_articles} articles today, or under 4 of the last 8 same weekdays): {', '.join(sorted(thin))}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="only the first N companies")
    ap.add_argument("--tickers", help="comma-separated tickers to fetch")
    ap.add_argument("--no-theme", action="store_true", help=f"do not require theme:{THEME}")
    ap.add_argument("--no-tone", action="store_true", help="skip tone calls (halves the run time)")
    ap.add_argument("--min-articles", type=int, default=5)
    ap.add_argument("--show", action="store_true", help="reprint from saved results without fetching")
    ap.add_argument("--interval", type=float, default=30, help="seconds between requests; GDELT punishes bursts")
    ap.add_argument("--retries", type=int, default=4, help="times to wait and retry a company after HTTP 429")
    ap.add_argument("--backoff", type=float, default=60, help="seconds to wait after a 429, doubled each retry")
    args = ap.parse_args()

    day = datetime.now(timezone.utc).date() - timedelta(days=1)
    key = f"{datetime.now(timezone.utc).date()}|theme={not args.no_theme}"
    saved = json.loads(RESULTS.read_text()) if RESULTS.exists() else {}
    results = saved.get(key, {})

    if not args.show:
        tickers = args.tickers.split(",") if args.tickers else list(COMPANIES)
        tickers = [t for t in tickers if t in COMPANIES][: args.limit]
        todo = [t for t in tickers if t not in results]
        calls = 1 if args.no_tone else 2
        print(f"{len(tickers) - len(todo)} already fetched today, {len(todo)} to go "
              f"(~{round(len(todo) * calls * args.interval / 60)} min without throttling)")
        client = DocApiClient(requests.Session(), Pacer(args.interval))
        stopped = False
        for n, ticker in enumerate(todo, 1):
            query = build_query(COMPANIES[ticker][0], not args.no_theme)
            for attempt in range(args.retries + 1):
                try:
                    results[ticker] = fetch(client, query, not args.no_tone)
                    break
                except GdeltError as exc:
                    if str(exc) != "http_429" or attempt == args.retries:
                        print(f"stopped at {ticker}: {exc}. Rerun later to continue; progress is saved.")
                        stopped = True
                        break
                    wait = args.backoff * 2 ** attempt
                    print(f"  {ticker}: GDELT throttled (429), waiting {wait:.0f}s before retry {attempt + 1}/{args.retries}")
                    time.sleep(wait)
            if stopped:
                break
            saved[key] = results
            RESULTS.write_text(json.dumps(saved))
            print(f"  [{n}/{len(todo)}] {ticker}: {query}")
    show(results, day, args.min_articles, not args.no_theme)


if __name__ == "__main__":
    main()
