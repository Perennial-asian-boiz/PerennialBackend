# Pattern evaluation: design note

**Branch:** `Pattern-Evaluation` · **Author:** @Zura16 · **Co-owner:** @haohnguyen94-droid · **Status:** proposal, prototypes only, no migration · **Implements:** DECISIONS.md, "Confidence Score composition" (fundamentals, 60%)

## Summary

The fundamentals component scores each company from 0 to 100 using its filed annual reports. Two models are proposed:

- **General model:** 3-year revenue growth (50%), operating margin (30%) and operating margin trend (20%).
- **Bank model:** used for companies that report bank figures. 3-year growth in revenue net of interest (50%), return on equity (30%) and efficiency ratio (20%).

Data comes from **SEC EDGAR**, not FMP as the roadmap assumed. EDGAR is free, needs no key, is public data, covers the small caps that FMP's free plan blocks, and includes every input the Piotroski validation needs. Probes and their output are in `development/backend/prototypes/fundamentals/`.

## 1. Data source

| | SEC EDGAR `companyfacts` | Finnhub free `/stock/metric` | FMP free |
|---|---|---|---|
| Key | None. A contact email is required in the User-Agent. | Required | Required |
| Small caps | Every US filer | Mostly covered | RKLB returns 402 (premium only) |
| Annual statements | As filed (10-K, 20-F) | Precomputed ratios only | Yes |
| Piotroski inputs | All | Not all | Yes |
| Terms | Public data | "Personal, non-commercial" | Limited |

**Decision proposed:** EDGAR is the source. Finnhub is used only to cross-check our calculations during development.

**Coverage**, for the 110 tickers in the GDELT prototype universe (today's ARK holdings plus 15 large caps), from `edgar_probe_output.txt`:

| Result | Count |
|---|---|
| Not SEC filers (BYDDY, KMTUY) | 2 |
| No annual report yet (SPCX, CBRS, XE, ALMR, GENB, SCTX, FRNM) | 7 |
| No revenue (OKLO, CMPS) | 2 |
| Ticker resolves to the wrong entity (XOM, a new holding company with no history) | 1 |
| Two or more years of revenue | 98 |
| Of those 98: five or more years of revenue | 86 |
| Of those 98: operating income for the last two years | 89 |
| Of those 98: gross-margin inputs | 71 |

**Accuracy**, from `crosscheck_output.txt`. Our calculations from EDGAR were compared with Finnhub for 23 companies:

| Measure | Within 1 percentage point |
|---|---|
| Net margin | 21 of 21 |
| Operating margin | 19 of 20 |
| 3-year revenue growth | 19 of 22 |
| Gross margin | 16 of 18 |

Where the two disagree, the cause is known:

- Banks, where the general revenue concept doesn't apply (SoFi, JPM).
- Company-specific definitions of cost of revenue (NVDA, PACB, CAT).
- One-off charges (TWST operating margin).
- Currency: TSM reports in TWD and we compute in reporting currency.

## 2. General model

| Measure | Weight | 0 points at | 100 points at |
|---|---|---|---|
| 3-year revenue growth, annualized (CAGR) | 50% | 0% or below | 40% or above |
| Operating margin, latest fiscal year | 30% | -20% or below | 30% or above |
| Operating margin trend: latest minus prior year, in percentage points | 20% | -10 points or worse | +10 points or better |

- **Scaling.** Each measure maps to 0–100 with a straight line between its two limits, and values beyond the limits are capped. The caps keep extreme values, such as ABSI's -4,297% operating margin, from distorting the score. The limits are version 1 starting values. They are stored with every result (rule 6) and will be tuned against the Piotroski check.
- **Why fixed limits instead of percentiles.** A percentile score changes whenever the candidate list changes. DECISIONS.md requires a stable score that doesn't move without a visible reason. A fixed limit can be explained to a user: "growing 25% a year".
- **Why 3-year growth only.** This was a decision. One year of growth is noisy for small caps, and three years shows a pattern.
- **Gross margin** is reported in the explanation text but not scored. Only 71 of 98 companies have it, and definitions vary.

## 3. Bank model

**When it applies.** A company gets the bank model when its filings include both `NoninterestExpense` and `RevenuesNetOfInterestExpense`. SIC codes are too broad for this: Coinbase (6199) and Robinhood (6211) are classed as financial, but they don't report bank figures, and the general model works for them. From `bank_probe_output.txt`, JPM, BAC, GS, MS and SOFI qualify.

| Measure | Weight | 0 points at | 100 points at |
|---|---|---|---|
| 3-year growth of revenue net of interest expense | 50% | 0% or below | 25% or above |
| Return on equity: net income / average shareholders' equity | 30% | 0% or below | 20% or above |
| Efficiency ratio: non-interest expense / revenue net of interest (lower is better) | 20% | 80% or above | 50% or below |

## 4. Missing data

This follows DECISIONS.md. A missing measure is dropped and its weight is spread over the remaining measures. A missing measure never counts as neutral. If every measure is missing, the component is missing, and the Confidence Score renormalizes. Every missing result carries a fixed reason code that the app can show:

| Code | Shown to the user | Example |
|---|---|---|
| `not_sec_filer` | "Financial reports for this company aren't available yet." | BYDDY |
| `no_annual_report` | "No annual report yet (recently listed)." | CBRS |
| `no_revenue` | "No revenue yet." | OKLO |
| `insufficient_history` | "Less than 3 years of reports, so growth isn't scored yet." | CRWV, for growth only |
| `identity_unresolved` | Internal only. Blocked until M4 identity work lands. | XOM |

## 5. Choices

- **Period:** annual reports (10-K, 20-F, 40-F) only. Trailing-twelve-month figures from quarterly reports can come later.
- **Currency:** all measures are ratios, computed in the filing currency. No exchange-rate conversion is needed.
- **Concept mapping:** revenue uses the first available of `Revenues`, `RevenueFromContractWithCustomerExcludingAssessedTax`, `RevenueFromContractWithCustomerIncludingAssessedTax` and `SalesRevenueNet`, chosen per fiscal year, because companies change concepts over time. IFRS filers use `Revenue`. The full list is in `edgar_probe.py`.

## 6. Explanations

Every result carries short Key Reasons generated from the measures, for the "Why This Company" screen:

- "Revenue grew 33% a year over the last 3 years."
- "Keeps 32 cents of every sales dollar as operating profit."
- "Profitability improved 4 points from last year."
- For banks: "Earns 17% a year on shareholders' money."

The 5-year revenue history is included for the chart.

## 7. Validation

The score is checked by rank correlation (Spearman) against the Piotroski F-Score across the candidate list, as DECISIONS.md proposes.

- Piotroski inputs: 44 of 98 companies have every input. The main gap is long-term debt, which debt-free filers don't report. **Proposed rule:** if long-term debt is absent but total liabilities are reported, treat it as 0.
- Banks are excluded from the Piotroski check, because the F-Score is not defined for banks.
- The correlation and the companies with the largest disagreement will be recorded in this note.

## 8. Fit with the roadmap

- **Prototype first.** A JSON prototype (`fundamentals_scores.json`) produces scores, the parts, the weights and limits used, reason codes, Key Reasons and revenue history. It needs no migration, so the migration freeze doesn't block it.
- **Production** follows rules 1 and 2. EDGAR becomes the `fundamentals` source in `0007_fundamentals`, with one row per `(batch, security, fiscal_period, line_item)`. The scores become a derivation run that records the input batch and `score_version`.
- **Identity.** EDGAR keys companies by CIK, which M4's `security_identifiers` (scheme `cik`) already plans for. The XOM case is a regression test for M4.

## Questions for review

1. Is EDGAR approved as the fundamentals source in place of FMP?
2. Are the starting limits acceptable as version 1, given they will be tuned against Piotroski?
3. Is the data-based bank rule (`NoninterestExpense` plus `RevenuesNetOfInterestExpense`) acceptable, rather than SIC codes?
4. Is treating absent long-term debt as 0 acceptable for Piotroski?
5. SEC fair-access rules: the production collector needs a team contact address in the User-Agent. Whose address should it use?
