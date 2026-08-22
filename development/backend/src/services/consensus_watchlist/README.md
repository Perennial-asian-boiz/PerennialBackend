# Phase B — Consensus Watchlist Pipeline

The Phase B consensus watchlist pipeline aggregates signals from Congressional stock trades, ARK ETF holdings, Corporate Insider transactions, and FINRA Short Interest into beginner-friendly candidate lists (`popular_stable` and `affordable_growing`).

---

## Data Sources & Contracts

| Signal | Source | Script | Output File |
|---|---|---|---|
| **Congress Trades** | Financial Modeling Prep / Senate Stock Watcher | `fetchers/fmp.py` | `database/local_data/trades_congress.json` |
| **ARK Holdings** | arkfunds.io API | `fetchers/ark.py` | `database/local_data/ark_holdings.json` |
| **Insider Trades** | SecuritiesDB (SEC Form 4) | `fetchers/insider.py` | `database/local_data/trades_insider.json` |
| **Short Interest** | FINRA Consolidated Equity Short Interest | `fetchers/short_interest.py` | `database/local_data/short_interest.json` |
| **Consensus Aggregation** | Internal pipeline | `consensus.py` | `database/local_data/consensus_watchlist.json` |

> ⚠️ **Important Data Source Note for Short Interest:**
> FINRA daily short-sale volume (`regsho-daily-download.aspx`) represents daily short sale volume and is **NOT** equivalent to open short interest. This pipeline uses the official FINRA Consolidated Equity Short Interest dataset/API (`https://api.finra.org/data/group/equity/service/shortInterest`).

---

## Bucket Assignment & Ranking Rules

- **`popular_stable`**: Primary discovery from Congress purchase trades with Market Cap > $10B ($10,000,000,000). Ranked deterministically by Congressional buyer count, buy count, recency, insider support, and alphabetical ticker.
- **`affordable_growing`**: Primary discovery from ARK holdings with Market Cap <= $10B. Ranked deterministically by ARK fund count, ETF weighting, insider support, and alphabetical ticker.
- **`unresolved`**: Tickers with missing, invalid, or failed market cap lookups are placed in `unresolved` with warning `["market_cap_unavailable"]`. Tickers are never defaulted to `affordable_growing`.

---

## Environment Variables

Configure in `development/backend/.env`:

```env
FMP_API_KEY=your_fmp_api_key
FINRA_API_URL=https://api.finra.org/data/group/equity/service/shortInterest
FINRA_CLIENT_ID=your_finra_client_id
FINRA_CLIENT_SECRET=your_finra_client_secret
FINRA_API_KEY=your_finra_api_key
```

---

## How to Run

### Run Fetchers & Aggregator
```bash
python3 development/backend/src/services/consensus_watchlist/fetchers/short_interest.py
python3 development/backend/src/services/consensus_watchlist/consensus.py
```

### Run Unit Tests
```bash
python3 testing/unit/test_short_interest.py
python3 testing/unit/test_consensus.py
```

---

## Current Limitations & Future Work

- **Scheduler:** Automated cron scheduling (daily execution order: `fmp.py` -> `ark.py` -> `insider.py` -> `short_interest.py` -> `consensus.py`) will be wired in Phase C.
- **Scoring & LLM:** Financial pattern evaluation and LLM sentiment scoring will consume `consensus_watchlist.json` downstream.
