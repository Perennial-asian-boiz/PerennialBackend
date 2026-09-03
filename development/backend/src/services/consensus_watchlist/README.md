# Phase B — Consensus Watchlist Pipeline

Aggregates Congressional stock trades, ARK ETF holdings, corporate insider
transactions, and short interest into two beginner-friendly candidate lists:
`popular_stable` and `affordable_growing`.

Five modules, run in order. Each fetcher writes one JSON file; `consensus.py`
reads all four back and ranks them.

---

## Data Sources & Contracts

| Signal | Source | Script | Output File | Key needed |
|---|---|---|---|---|
| **Congress trades** | Financial Modeling Prep / Senate Stock Watcher | `fetchers/fmp.py` | `trades_congress.json` | `FMP_API_KEY` |
| **ARK holdings** | arkfunds.io API | `fetchers/ark.py` | `ark_holdings.json` | — |
| **Insider trades** | SecuritiesDB (SEC Form 4) | `fetchers/insider.py` | `trades_insider.json` | `FINNHUB_API_KEY` |
| **Short interest** | Nasdaq public quote API | `fetchers/short_interest.py` | `short_interest.json` | — |
| **Aggregation** | Internal | `consensus.py` | `consensus_watchlist.json` | `FMP_API_KEY` |

All outputs land in `development/database/local_data/`, resolved through
`paths.py` — see *Paths* below.

### On the short-interest source

This pipeline reads the **Nasdaq public quote endpoint**
(`api.nasdaq.com/api/quote/{symbol}/short-interest`), which serves the same
FINRA-sourced consolidated open short interest without OAuth. It needs only a
browser `User-Agent` header.

An earlier draft targeted `api.finra.org` directly, which requires
organisation OAuth credentials (`FINRA_CLIENT_ID` / `FINRA_CLIENT_SECRET`) the
team does not hold. Those variables are gone; do not reintroduce them.

Separately: FINRA *daily short-sale volume* (`regsho-daily-download.aspx`) is
**not** the same thing as open short interest. Don't substitute it.

---

## Paths

`paths.py` is the single source of truth for where the pipeline reads and
writes. It finds the repo root by searching upward for a `.git` directory or a
`development/backend/` folder, so nothing depends on how deep a file sits.

| Constant | Resolves to |
|---|---|
| `REPO_ROOT` | the repo root |
| `ENV_PATH` | `development/backend/.env` |
| `LOCAL_DATA_DIR` | `development/database/local_data/` |

Do not reintroduce hardcoded `parents[N]` counts or match on the repo's
directory name — both broke when this code moved out of `project-perennial`.

---

## Bucket Assignment & Ranking

- **`popular_stable`** — discovered from Congress *purchase* trades with market
  cap > $10B. Ranked by Congressional buyer count, buy count, recency, insider
  support, then ticker alphabetically.
- **`affordable_growing`** — discovered from ARK holdings with market cap ≤ $10B.
  Ranked by ARK fund count, ETF weighting, insider support, then ticker.
- **`unresolved`** — tickers whose market cap is missing or failed to look up,
  flagged `["market_cap_unavailable"]`. Never silently defaulted into a bucket.

Ranking is deterministic: same inputs, same order, every run.

---

## Setup

```bash
pip install -r requirements.txt
cp development/backend/.env.example development/backend/.env
# then fill in FMP_API_KEY and FINNHUB_API_KEY
```

## Running

Whole pipeline once, in order:

```bash
python3 development/backend/src/services/consensus_watchlist/scheduler/cron.py --test
```

Leave the scheduler running (9:00am daily; short interest at 9:10am on the 1st
and 15th, when settlement data lands):

```bash
python3 development/backend/src/services/consensus_watchlist/scheduler/cron.py
```

Any single stage on its own:

```bash
python3 development/backend/src/services/consensus_watchlist/fetchers/fmp.py
python3 development/backend/src/services/consensus_watchlist/consensus.py
```

Order matters — `insider.py` reads `trades_congress.json` and
`ark_holdings.json`, and `consensus.py` reads all four.

---

## Known Limitations

- **No tests yet in this repo.** A 319-line `test_consensus.py` exists on
  `aalind-working` in the old repo and should port over roughly as-is. The
  companion `test_short_interest.py` was written against the FINRA response
  shape and needs rewriting before it means anything.
- **Flat imports.** `cron.py` puts `fetchers/` on `sys.path` so the modules can
  import each other by bare name, which is what keeps direct `python3 …/ark.py`
  execution working. If the backend adopts a real package layout, this and the
  `paths.py` bootstrap in each module are the things to convert.
- **Downstream.** Financial pattern evaluation and LLM sentiment scoring will
  consume `consensus_watchlist.json`; neither is wired up here.
