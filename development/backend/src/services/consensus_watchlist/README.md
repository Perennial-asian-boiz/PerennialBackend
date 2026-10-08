# Phase B — Consensus Watchlist Pipeline

Aggregates Congressional stock trades, ARK ETF holdings, corporate insider
transactions, and short interest into two beginner-friendly candidate lists:
`popular_stable` and `affordable_growing`.

This page describes the retained legacy JSON workflow. The database production
workflow has a separate scheduler (`python -m src.pipeline.scheduler` from
`development/backend`), publication/replay and health commands. See
[database setup](../../../../database/README.md) before switching consumers.
The database scheduler does not write the JSON output described below.

Five legacy modules, run in order. Each fetcher writes one JSON file; `consensus.py`
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

The legacy JSON path keeps the `fab7188` Congress dedup key:
`(politician_name.lower(), ticker, transaction_date, trade_type.lower())`,
keeping the first row. `fmp.deduplicate` serves that legacy writer; the database
collector uses `deduplicate_proven_repeats`, which retains cross-provider
observations and unidentified repeated rows. The legacy short-interest parser
keeps a ticker when a number is malformed and sets that field to null. The
database collector uses `parse_short_interest_strict` and fails the collection
on malformed numbers.

**Intentional output change:** `consensus.py` now reads the short-interest
field aliases correctly. These signals were always null at `fab7188`; valid
short-interest values now populate the JSON watchlist.

Database rankings use `watchlist-v2`. Before counting Congress purchases, they
group by `(politician_name.strip().casefold(), symbol, transaction_date,
trade_type.strip().casefold())`, keeping the provider group with the most rows.
A null `data_source` is its own provider; ties use the provider whose lowest
canonical `row_number` comes first. A cross-provider pair counts once, while two
rows from one provider count twice. Signal dates and amounts come from those
kept rows. Raw database snapshots retain every observation, and historical
`watchlist-v1` publications replay with their original counting rules.

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

- **Legacy failure behavior.** This JSON scheduler may continue after a stage
  fails. Use the database pipeline for fail-closed publication. Synthetic
  collector, scheduler and PostgreSQL integration tests are in `backend/tests`.
- **Flat imports.** `cron.py` puts `fetchers/` on `sys.path` so the modules can
  import each other by bare name, which is what keeps direct `python3 …/ark.py`
  execution working. If the backend adopts a real package layout, this and the
  `paths.py` bootstrap in each module are the things to convert.
- **Downstream.** Financial pattern evaluation and LLM sentiment scoring will
  consume `consensus_watchlist.json`; neither is wired up here.
