"""Probe: which bank figures EDGAR reports, and each company's SEC industry (SIC) code. Prints only.

Used to choose the bank model's inputs: return on equity and the efficiency ratio.
Needs SEC_USER_AGENT in development/backend/.env.
"""
import time

import requests

from edgar_probe import UA, annual

TICKERS = ["JPM", "BAC", "GS", "MS", "SOFI", "BLK", "COIN", "HOOD"]
CONCEPTS = {
    "net_income": ["NetIncomeLoss"],
    "equity": ["StockholdersEquity"],
    "noninterest_expense": ["NoninterestExpense"],
    "revenue_net_of_interest": ["RevenuesNetOfInterestExpense", "Revenues"],
    "net_interest_income": ["InterestIncomeExpenseNet"],
    "noninterest_income": ["NoninterestIncome"],
    "provision_credit_losses": ["ProvisionForLoanLeaseAndOtherLosses", "ProvisionForLoanAndLeaseLosses",
                                "ProvisionForCreditLosses"],
    "tier1_ratio": ["CommonEquityTier1CapitalRatio", "TierOneRiskBasedCapitalToRiskWeightedAssets"],
}
INSTANT = {"equity", "tier1_ratio"}


def main():
    sec = requests.get("https://www.sec.gov/files/company_tickers.json", headers=UA, timeout=30).json()
    cik = {r["ticker"]: r["cik_str"] for r in sec.values()}
    print("annual years reported per concept")
    for t in TICKERS:
        time.sleep(0.2)
        sub = requests.get(f"https://data.sec.gov/submissions/CIK{cik[t]:010d}.json", headers=UA, timeout=30).json()
        time.sleep(0.2)
        facts = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik[t]:010d}.json",
                             headers=UA, timeout=60).json()["facts"].get("us-gaap", {})
        counts = []
        for metric, names in CONCEPTS.items():
            years = set()
            for name in names:
                for entries in facts.get(name, {}).get("units", {}).values():
                    years |= set(annual(entries, metric not in INSTANT))
            counts.append(f"{metric}={len(years)}")
        print(f"{t:5} SIC {sub.get('sic')} {sub.get('sicDescription', '')[:38]:38} | {' '.join(counts)}")


if __name__ == "__main__":
    main()
