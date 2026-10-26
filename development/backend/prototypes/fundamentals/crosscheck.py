"""Cross-check margins and 3-year revenue growth computed from EDGAR against Finnhub /stock/metric. Prints only.

Needs FINNHUB_API_KEY and SEC_USER_AGENT in development/backend/.env.
"""
import time
from datetime import date

import requests

from edgar_probe import ENV, UA, series

KEY = ENV.get("FINNHUB_API_KEY")
if not KEY:
    raise SystemExit("Set FINNHUB_API_KEY in development/backend/.env.")
SAMPLE = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "PLTR", "RKLB", "CRWV", "SHOP", "TSM", "COIN",
          "HOOD", "TWST", "PACB", "ILMN", "NFLX", "WMT", "CAT", "ABSI", "DDOG", "SOFI", "JPM"]


def near(points, end, days=10):
    target = date.fromisoformat(end)
    for p in points or []:
        if abs((date.fromisoformat(p["period"]) - target).days) <= days:
            return p["v"]
    return None


def pct(x):
    return "   -  " if x is None else f"{x * 100:6.1f}"


def main():
    sec = requests.get("https://www.sec.gov/files/company_tickers.json", headers=UA, timeout=30).json()
    cik = {r["ticker"].upper(): r["cik_str"] for r in sec.values()}
    print(f"{'ticker':6} {'FY end':10} | {'gross % E/F':>13} | {'oper % E/F':>13} | {'net % E/F':>13} | {'rev 3y CAGR E/F':>15}")
    diffs = []
    for t in SAMPLE:
        time.sleep(0.15)
        facts = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik[t]:010d}.json",
                             headers=UA, timeout=60).json()["facts"]
        rev = series(facts, "revenue")[0]
        if len(rev) < 2:
            print(f"{t:6} not enough EDGAR revenue")
            continue
        end = max(rev)
        r = rev[end]
        gp, cost = series(facts, "gross_profit")[0], series(facts, "cost_of_revenue")[0]
        gross = gp[end] / r if end in gp else ((r - cost[end]) / r if end in cost else None)
        op = series(facts, "operating_income")[0].get(end)
        ni = series(facts, "net_income")[0].get(end)
        ends = sorted(rev)
        three = [e for e in ends if abs((date.fromisoformat(end) - date.fromisoformat(e)).days - 3 * 365) <= 20]
        cagr = (r / rev[three[0]]) ** (1 / 3) - 1 if three and rev[three[0]] > 0 else None

        time.sleep(1.1)  # Finnhub free plan: 60 calls a minute
        fh = requests.get("https://finnhub.io/api/v1/stock/metric",
                          params={"symbol": t, "metric": "all", "token": KEY}, timeout=30).json()
        annual = fh.get("series", {}).get("annual", {})
        f_gross, f_op, f_net = (near(annual.get(k), end) for k in ("grossMargin", "operatingMargin", "netMargin"))
        f_cagr = fh.get("metric", {}).get("revenueGrowth3Y")
        f_cagr = f_cagr / 100 if f_cagr is not None else None

        e_vals = [gross, op / r if op is not None else None, ni / r if ni is not None else None, cagr]
        f_vals = [f_gross, f_op, f_net, f_cagr]
        cells = [f"{pct(e)}/{pct(f)}" for e, f in zip(e_vals, f_vals)]
        print(f"{t:6} {end:10} | {cells[0]:>13} | {cells[1]:>13} | {cells[2]:>13} | {cells[3]:>15}")
        for name, e, f in zip(("gross", "oper", "net", "cagr3"), e_vals, f_vals):
            if e is not None and f is not None:
                diffs.append((name, t, abs(e - f) * 100))
    print("\nE = from EDGAR, F = Finnhub. Values in %.")
    for name in ("gross", "oper", "net", "cagr3"):
        d = [x for x in diffs if x[0] == name]
        if d:
            close = sum(1 for x in d if x[2] <= 1.0)
            worst = max(d, key=lambda x: x[2])
            print(f"{name:6}: {close}/{len(d)} within 1 point; worst {worst[1]} off by {worst[2]:.1f} points")


if __name__ == "__main__":
    main()
