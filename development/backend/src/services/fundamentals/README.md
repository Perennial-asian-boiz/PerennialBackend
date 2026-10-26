# Fundamentals (pattern evaluation)

This module computes the fundamentals part of the Confidence Score: 60% of it, per `DECISIONS.md`. It scores each company from 0 to 100 using the annual reports it files with the SEC. The rules, the evidence behind them and the results are in [`docs/work-items/pattern-evaluation-design.md`](../../../../../docs/work-items/pattern-evaluation-design.md).

## What a run does

A run is two ordinary runs through the ingestion contract. Each one writes an immutable batch.

| Source | Mode | Rows | Lineage |
|---|---|---|---|
| `fundamentals` | live | One row per security, fiscal year and line item, from SEC EDGAR `companyfacts`, for the latest 6 fiscal years | The Congress and ARK batches pinned by the current production watchlist publication |
| `fundamental_scores` | derived | One row per security: score, measures, missing-data codes, Piotroski signals | The `fundamentals` batch it was computed from |

1. **Universe.** Every ticker in the current production watchlist publication (`popular_stable`, `affordable_growing` and `unresolved`).
2. **CIK.** A seeded `company_profiles.cik` is used when present. Otherwise the CIK comes from SEC's ticker map. Seeding is how a ticker that SEC now maps to a new entity, such as XOM, gets its history back.
3. **Collection.** One `companyfacts` request per company, spaced 0.12 s apart (SEC allows 10 a second). Each request carries `Settings.sec_user_agent`.
   - A company with no CIK, or no annual data, has no rows. The envelope counts it.
   - A transport failure for any company fails the whole batch (`partial_collection`), and readers keep the last good batch.
4. **Scoring.** `scoring.py` applies `ScoreRule` (`fundamentals-v1`) to the latest good `fundamentals` batch. The limits and rule parameters are stored in the derived batch's envelope.

Identical inputs produce identical batches. A rerun reuses its batch instead of writing a new one.

## Score rules (`fundamentals-v1`)

| Model | Measure | Weight | 0 points at | 100 points at |
|---|---|---|---|---|
| General | 3-year revenue growth (CAGR) | 50% | 0% | 40% |
| | Operating margin | 30% | −20% | 30% |
| | Operating margin trend | 20% | −10 points | +10 points |
| Bank | 3-year growth of revenue net of interest | 50% | 0% | 25% |
| | Return on equity | 30% | 0% | 20% |
| | Efficiency ratio (lower is better) | 20% | 80% | 50% |

- A company gets the **bank model** when it reports non-interest expense for its latest revenue year.
- A company needs **at least 2 of the 3 measures** to get a score. Otherwise its row records `not_enough_data`.
- Margins beyond ±100% drop the margin trend. A margin above +100% is dropped.
- When the operating margin is below −100%, growth is not scored, because growth from a tiny revenue base flatters the company.
- When operating income isn't reported, **pre-tax income** stands in for it, and `profit_source` records which was used.
- `breakdown()` returns the points and weights for the score breakdown screen. `key_reasons()` returns the plain-language reasons for "Why This Company". Neither is stored: both are recomputed from the stored measures and the rule.

## Run

```bash
cd development/backend
PERENNIAL_ENV=local python -m src.pipeline.scheduler --once --job fundamentals
python -m pytest tests/test_fundamentals.py
```

The job exits 0 only when both runs succeed. It needs `SEC_USER_AGENT` (a contact email; SEC requires one) and `DATABASE_URL`. It does not need an API key.

## Known limits

- **Annual reports only.** Figures can be more than a year old. Trailing-twelve-month figures from quarterly reports are a planned improvement.
- **Coverage.** Companies that don't file with the SEC (BYD and Komatsu ADRs) and new listings without an annual report have no score.
- **Identity.** XOM needs its predecessor CIK seeded in `company_profiles`. WBD dropped out of SEC's ticker map on 2026-10-25.
