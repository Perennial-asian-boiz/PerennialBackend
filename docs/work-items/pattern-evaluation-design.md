# Pattern evaluation: design note

**Branch:** `Pattern-Evaluation` · **Author:** @Zura16 · **Co-owner:** @haohnguyen94-droid · **Status:** implemented (`src/services/fundamentals`, migration `0016`, rules `fundamentals-v1`); awaiting review · **Implements:** DECISIONS.md, "Confidence Score composition" (fundamentals, 60%)

## Summary

The fundamentals component scores each company from 0 to 100 using its filed annual reports. Two models are proposed:

- **General model:** 3-year revenue growth (50%), operating margin (30%) and operating margin trend (20%).
- **Bank model:** used for companies that report non-interest expense. 3-year growth in revenue net of interest (50%), return on equity (30%) and efficiency ratio (20%).

Data comes from **SEC EDGAR**, not FMP as the roadmap assumed. EDGAR is free, needs no key, is public data, covers the small caps that FMP's free plan blocks, and includes every input the Piotroski validation needs.

The production implementation is in `src/services/fundamentals/`: two ingestion-contract sources, `fundamentals` (live) and `fundamental_scores` (derived), with migration `0016` and a scheduler job (section 8). The probes and the JSON prototype that led to these rules are in `development/backend/prototypes/fundamentals/`.

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
- **Pre-tax fallback.** When a company doesn't report operating income for its latest year (Eli Lilly, Robinhood, Deere), the margin and trend use pre-tax income from continuing operations. One source is used for all years, so the trend compares like with like, and the explanation text says "pre-tax profit".
- **Margins beyond ±100%.** The trend is dropped when either year's margin is beyond ±100%, because revenue near zero makes it noise (Joby's trend computed to +437,443 points). A margin above +100% is dropped, because it means non-operating gains were booked as income (BitMine: 7,288%). Large negative margins are real and stay, and they score 0.

## 3. Bank model

**When it applies.** A company gets the bank model when its filings report `NoninterestExpense` for its latest revenue year. Bank revenue is `RevenuesNetOfInterestExpense` when reported, otherwise `Revenues`: Bank of America reports only `Revenues`, which for a bank is already net of interest. SIC codes are too broad for this. Coinbase (6199) and Robinhood (6211) are classed as financial, but they don't report bank figures, and the general model works for them. JPM, BAC, GS, MS and SOFI qualify.

| Measure | Weight | 0 points at | 100 points at |
|---|---|---|---|
| 3-year growth of revenue net of interest expense | 50% | 0% or below | 25% or above |
| Return on equity: net income / average shareholders' equity | 30% | 0% or below | 20% or above |
| Efficiency ratio: non-interest expense / revenue net of interest (lower is better) | 20% | 80% or above | 50% or below |

## 4. Missing data

This follows DECISIONS.md. A missing measure is dropped and its weight is spread over the remaining measures. A missing measure never counts as neutral.

A company needs **at least 2 of its model's 3 measures** to be scored. Without that rule, one measure becomes the whole score: Robinhood scored 100 on growth alone, and Deere scored 0. With fewer than 2, the component is missing and the Confidence Score renormalizes.

**Growth from a tiny revenue base is not scored.** When the operating margin is below −100%, meaning costs are more than twice revenue, growth is dropped. Without this rule, CRSP scored 62.5 on 43% growth of small, irregular revenue while its margin was −18,934%. The rule works in any currency and matches the ±100% margin rule. With the 2-of-3 rule, such companies end as `not_enough_data`.

A stored `fundamental_scores` row carries fixed reason codes the app can show:

| Code | Shown to the user | Example |
|---|---|---|
| `no_revenue` | "No revenue yet." | OKLO |
| `insufficient_history` | "Less than 3 years of reports, so growth isn't scored yet." | CRWV, for growth only |
| `not_enough_data` | "Not enough reported figures to score yet." | JOBY, CRSP |

A company with **no score row** has no SEC annual data: it isn't an SEC filer (BYDDY), it hasn't filed an annual report yet (CBRS), or its ticker maps to a new entity without history (XOM). The app shows "Financial reports for this company aren't available yet." The `fundamentals` batch envelope counts these companies (`no_cik_count`, `no_annual_data_count`) and lists the first 20. Scores are derived only from the `fundamentals` batch, so a company that batch doesn't mention can't get a row, which keeps the derivation replayable.

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
- **Result (rules v1.1):** Spearman ρ = **0.40** across 61 general-model companies with all 9 signals. That is a moderate positive agreement. Piotroski measures financial health (debt, liquidity, cash flow, dilution), while this score rewards growth and operating profitability, so only partial agreement is expected. The value didn't change between v1 (0.40, 63 companies) and v1.1.

## 7a. Prototype results

From `score_output.txt` (rules v1.1, 110 tickers):

| Result | Count |
|---|---|
| Scored | 90 (85 general, 5 bank: JPM, SOFI, MS, GS, BAC) |
| `no_annual_report` | 6 |
| `no_revenue` | 3 |
| `not_enough_data` | 8 |
| `not_sec_filer` | 2 |
| `identity_unresolved` | 1 |

The highest scores are HOOD 100, PLTR 91, LLY 90 and NVDA 88. Large, slower-growing companies sit mid-table (MSFT 61, AAPL 43), because the weights favor growth, as designed.

Growth from a tiny revenue base, the weakness found here, is now handled by the rule in section 4.

## 7b. Production results (`fundamentals-v1`)

The production collector and scorer ran live against EDGAR for the same 110 tickers on 2026-10-25, without a database:

| Result | Count |
|---|---|
| Companies with annual data | 100 (7,044 line-item rows) |
| No CIK in SEC's ticker map | 3 (BYDDY, KMTUY, and WBD, whose ticker left SEC's map) |
| No annual data for the mapped CIK | 7 (ALMR, CBRS, GENB, SCTX, SPCX, XE, XOM) |
| Scored | 78 (73 general, 5 bank: BAC, GS, JPM, MS, SOFI) |
| `not_enough_data` | 19, of which 11 come from the tiny-revenue rule (ABSI, AUR, BEAM, CRSP, KYMR, NRIX, NTLA, PACB, PONY, PSNL, RXRX) |
| `no_revenue` | 3 (CMPS, FRNM, OKLO) |

Every company scored by both the production code and the v1.1 prototype has the same score.

**Validation:** Spearman ρ against Piotroski is **0.27** over 56 general-model companies with all 9 signals, down from 0.40 in v1.1. The tiny-revenue rule removed weak companies that ranked low on both measures. That narrows the range and lowers the correlation without changing how the remaining companies agree. The correlation stays positive. D10's validation is recorded, and tuning the limits is the next step if the team wants closer agreement.

## 8. Production implementation

This follows design rule 1 (one ingestion contract) and rule 2 (derived data is a run).

| Piece | Where |
|---|---|
| Sources `fundamentals` and `fundamental_scores` | `models.FUNDAMENTALS_SOURCES`, lock keys 170018 and 170019, `CANONICAL_ONLY_SOURCES`, `IMMUTABLE` grants |
| Tables and the vocabulary CHECK | Migration `0016` (expand only, `lock_timeout`, a downgrade that refuses while fundamentals lineage exists) |
| Record schemas | `FundamentalLine`, `FundamentalScore` in `src/ingestion/schemas.py` (signed decimals; a scored row needs a model, an unscored row needs a code) |
| EDGAR client and line-item extraction | `src/services/fundamentals/edgar.py` |
| Score rules, `breakdown()`, `key_reasons()` | `src/services/fundamentals/scoring.py` (pure functions, strict mypy) |
| Collection and derivation runs | `src/services/fundamentals/job.py` |
| Scheduler | `python -m src.pipeline.scheduler --once --job fundamentals` |
| Setting | `SEC_USER_AGENT`, read as `Settings.sec_user_agent` |
| Tests | `tests/test_fundamentals.py`: EDGAR parsing, every rule, both runs with lineage, batch reuse, failure fallback |

**The universe** is every ticker in the current production watchlist publication, and the run's dependencies name the Congress and ARK batches that publication pinned. The candidate list therefore matches the watchlist exactly, whatever ranking version produced it.

**Long format, as the roadmap specified:** one `fundamentals` row per `(batch, security, fiscal_year_end, line_item)`, with the XBRL concept, form and filing date of each value. Restatements arrive as new batches.

**Not stored:** points and Key Reasons. Both are recomputed from the stored measures and `ScoreRule`, whose limits are in the derived batch's envelope.

**Left for M6 scores:** combining fundamentals with sentiment and events into `company_scores`, and the `GET /v1/securities/{symbol}/score` breakdown endpoint, which can call `breakdown()` and `key_reasons()`.

## Questions for review

1. Is EDGAR approved as the fundamentals source in place of FMP?
2. Are the starting limits acceptable as version 1, given they will be tuned against Piotroski?
3. Is the data-based bank rule (non-interest expense reported for the latest revenue year) acceptable, rather than SIC codes?
4. Is treating absent long-term debt as 0 acceptable for Piotroski?
5. SEC fair-access rules: the production collector needs a team contact address in the User-Agent. Whose address should it use?
6. Should XOM's predecessor CIK be seeded in `company_profiles` now, or as part of M4?
