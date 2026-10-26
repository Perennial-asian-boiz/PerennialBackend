"""Probe: what SEC EDGAR companyfacts covers for the prototype tickers. Prints only, stores nothing.

SEC requires a contact in the User-Agent. Set SEC_USER_AGENT in development/backend/.env,
for example: SEC_USER_AGENT=Perennial research you@example.com

    cd development/backend/prototypes/fundamentals
    ../../../../.venv/bin/python edgar_probe.py
"""
import json
import time
from datetime import date
from pathlib import Path

import requests
from dotenv import dotenv_values

HERE = Path(__file__).parent
ENV = dotenv_values(HERE.parents[1] / ".env")
if not ENV.get("SEC_USER_AGENT"):
    raise SystemExit("Set SEC_USER_AGENT in development/backend/.env (SEC requires a contact email).")
UA = {"User-Agent": ENV["SEC_USER_AGENT"]}
ANNUAL_FORMS = {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}

CONCEPTS = {  # metric -> (us-gaap names, ifrs-full names), first match per fiscal year wins
    "revenue": (["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
                 "RevenueFromContractWithCustomerIncludingAssessedTax", "SalesRevenueNet", "SalesRevenueGoodsNet",
                 "RevenuesNetOfInterestExpense"], ["Revenue", "RevenueFromContractsWithCustomers"]),
    "gross_profit": (["GrossProfit"], ["GrossProfit"]),
    "cost_of_revenue": (["CostOfRevenue", "CostOfGoodsAndServicesSold", "CostOfGoodsSold"], ["CostOfSales"]),
    "operating_income": (["OperatingIncomeLoss"], ["ProfitLossFromOperatingActivities"]),
    "pretax_income": (["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
                       "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"],
                      ["ProfitLossBeforeTax"]),
    "net_income": (["NetIncomeLoss", "ProfitLoss"], ["ProfitLossAttributableToOwnersOfParent", "ProfitLoss"]),
    "operating_cash_flow": (["NetCashProvidedByUsedInOperatingActivities"], ["CashFlowsFromUsedInOperatingActivities"]),
    "assets": (["Assets"], ["Assets"]),
    "current_assets": (["AssetsCurrent"], ["CurrentAssets"]),
    "current_liabilities": (["LiabilitiesCurrent"], ["CurrentLiabilities"]),
    "long_term_debt": (["LongTermDebtNoncurrent", "LongTermDebt", "LongTermDebtAndCapitalLeaseObligations"],
                       ["NoncurrentPortionOfNoncurrentBorrowings", "LongtermBorrowings"]),
    "diluted_shares": (["WeightedAverageNumberOfDilutedSharesOutstanding"], ["AdjustedWeightedAverageShares"]),
    "liabilities": (["Liabilities"], ["Liabilities"]),
    "equity": (["StockholdersEquity"], ["EquityAttributableToOwnersOfParent", "Equity"]),
    "noninterest_expense": (["NoninterestExpense"], []),
    "revenue_net_of_interest": (["RevenuesNetOfInterestExpense", "Revenues"], []),
}
FLOW = {"revenue", "gross_profit", "cost_of_revenue", "operating_income", "pretax_income", "net_income", "operating_cash_flow",
        "diluted_shares", "noninterest_expense", "revenue_net_of_interest"}
PIOTROSKI = ["net_income", "operating_cash_flow", "assets", "current_assets", "current_liabilities",
             "long_term_debt", "diluted_shares", "revenue"]


def annual(entries, flow):
    out = {}
    for e in entries:
        if e.get("form") not in ANNUAL_FORMS or e.get("fp") != "FY":
            continue
        if flow:
            if "start" not in e:
                continue
            days = (date.fromisoformat(e["end"]) - date.fromisoformat(e["start"])).days
            if not 350 <= days <= 380:
                continue
        prev = out.get(e["end"])
        if prev is None or e["filed"] > prev[1]:
            out[e["end"]] = (e["val"], e["filed"])
    return {end: v for end, (v, _) in out.items()}


def series(facts, metric):
    gaap, ifrs = CONCEPTS[metric]
    merged, taxonomy = {}, None
    for tax, names in (("us-gaap", gaap), ("ifrs-full", ifrs)):
        for name in names:
            units = facts.get(tax, {}).get(name, {}).get("units", {})
            if not units:
                continue
            unit = "shares" if metric == "diluted_shares" else ("USD" if "USD" in units else next(iter(units)))
            for end, v in annual(units.get(unit, []), metric in FLOW).items():
                merged.setdefault(end, v)
            taxonomy = taxonomy or tax
    return merged, taxonomy


def currency(facts, metric):
    """The unit `series` reads for a money metric: USD when reported, otherwise the filing currency."""
    gaap, ifrs = CONCEPTS[metric]
    for tax, names in (("us-gaap", gaap), ("ifrs-full", ifrs)):
        for name in names:
            units = facts.get(tax, {}).get(name, {}).get("units", {})
            if units:
                return "USD" if "USD" in units else next(iter(units))
    return None


def main():
    tickers = json.loads((HERE / "tickers.json").read_text())
    sec = requests.get("https://www.sec.gov/files/company_tickers.json", headers=UA, timeout=30).json()
    cik = {r["ticker"].upper(): r["cik_str"] for r in sec.values()}
    rows, missing_cik = [], []
    for t in tickers:
        lookup = {"XYZ": "XYZ", "SPCX": "SPCX"}.get(t, t)
        if lookup not in cik:
            missing_cik.append(t)
            continue
        time.sleep(0.15)  # SEC allows 10 requests a second
        r = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik[lookup]:010d}.json", headers=UA, timeout=60)
        if r.status_code != 200:
            rows.append((t, f"http_{r.status_code}", 0, None, {}, ""))
            continue
        facts = r.json().get("facts", {})
        data, tax = {}, None
        for m in CONCEPTS:
            data[m], mt = series(facts, m)
            tax = tax or mt
        rev = data["revenue"]
        years = sorted(rev)
        last2 = years[-2:]
        has = {m: all(y in data[m] for y in last2) and len(last2) == 2 for m in PIOTROSKI}
        gross = len(last2) == 2 and all(y in data["gross_profit"] or y in data["cost_of_revenue"] for y in last2)
        has["gross_margin_inputs"] = gross
        has["operating_income"] = len(last2) == 2 and all(y in data["operating_income"] for y in last2)
        rows.append((t, tax or "none", len(years), years[-1] if years else None, has, ""))

    print(f"{'ticker':7} {'taxonomy':9} {'rev yrs':>7} {'latest FY':>10}  missing for last 2 fiscal years")
    for t, tax, n, latest, has, _ in rows:
        miss = [k for k, v in has.items() if not v]
        print(f"{t:7} {tax:9} {n:>7} {str(latest):>10}  {', '.join(miss) or '-'}")
    ok = [r for r in rows if r[4]]
    def count(pred):
        return sum(1 for r in ok if pred(r))
    print(f"\n{len(tickers)} tickers: {len(missing_cik)} not in SEC's ticker list ({', '.join(missing_cik)})")
    print(f"{len(ok)} with EDGAR data")
    print(f"  5+ years of annual revenue: {count(lambda r: r[2] >= 5)}")
    print(f"  revenue for last 2 FYs: {count(lambda r: r[4].get('revenue'))}")
    print(f"  gross margin inputs: {count(lambda r: r[4].get('gross_margin_inputs'))}")
    print(f"  operating income: {count(lambda r: r[4].get('operating_income'))}")
    print(f"  all Piotroski inputs: {count(lambda r: all(r[4].get(m) for m in PIOTROSKI))}")
    for m in PIOTROSKI:
        print(f"    {m}: {count(lambda r, m=m: r[4].get(m))}")


if __name__ == "__main__":
    main()
