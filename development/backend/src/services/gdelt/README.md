# GDELT macro events

This service watches GDELT themes such as oil prices, interest rates and trade disputes, and records when coverage of a theme spikes. It feeds the Insights page, which shows how an event flows to sectors and then to companies.

GDELT gives us global event coverage. It doesn't score company sentiment: that is FinBERT's job, on the `FinBERT` branch. The tone numbers below are GDELT's own word-list score, not FinBERT output.

## What we built

All work is on the `GDELT` branch. Nothing reaches `main` until the feature is finished.

| Step | Commit | What changed |
|---|---|---|
| 1. Sector for each ticker | `e5ced09` | `collect_market_caps` in `src/pipeline/runner.py` already fetched the FMP profile and threw away `sector` and `industry`. It now keeps both, `publication.py` validates them, and migration `0004` stores them on `market_cap_observations`. |
| 2. Macro-event pipeline | `1821eb7` | Everything in this folder, migration `0005`, and the three tables below. |
| 3. CI formatting fix | `06a6841` | `ruff format` on the two shared files from steps 1 and 2. |

### Decisions and why

- **The DOC API, not raw GKG files.** The DOC API needs no key and returns three months of daily counts in one call, so no backfill is needed. The cost: GDELT rate-limits by IP, and the campus network gets HTTP 429.
- **Sector is stored on `market_cap_observations`, not `securities`.** Each publication points at the observations it used. If FMP reclassifies a company later, old publications keep the label they were built with.
- **GDELT is separate from the watchlist.** The GDELT tables are not in `SOURCES` and have no tie to publications, so a GDELT outage cannot block the watchlist from publishing.
- **Spike checks are upserts, never deletes.** The importer database role has no DELETE grant. Instead of deleting a withdrawn spike, a rerun flips `is_spike` in place.
- **Shared files get one-line hooks.** The logic lives here. Shared files only get the scheduler job, two lines in `provision_roles.py`, the new tables appended to the end of `models.py`, and one test expectation. This keeps merge conflicts with teammates' branches small.

## Tables

All tables are in the `perennial` schema and created by migration `0005`.

### `gdelt_theme_days`

One row per theme per UTC day, holding three months of history. A rerun updates the row in place.

| Column | Meaning |
|---|---|
| `theme` | GDELT theme code, for example `ECON_OILPRICE` |
| `day` | UTC date |
| `article_count` | English articles GDELT saw that day with this theme. The spike rule uses this. |
| `total_articles` | All English articles GDELT monitored that day. Use it to tell a real jump from a busy news day. |
| `avg_tone` | GDELT's average tone for the theme that day. Below 0 is gloomy wording, above 0 is upbeat. |
| `retrieved_at` | When we fetched it |

### `gdelt_articles`

Up to 5 example articles per theme per day, taken only from `evidence_domains` in `theme_sector_map.json`. Rows are never updated or deleted.

| Column | Meaning |
|---|---|
| `theme`, `day` | The theme and day the article belongs to |
| `url`, `title`, `domain` | The article |
| `seen_at` | When GDELT first saw it |

### `gdelt_spike_checks`

One row per theme per target day, for days with at least 14 days of history. Readers filter on `is_spike`.

| Column | Meaning |
|---|---|
| `theme`, `day` | The theme and the day checked |
| `article_count` | That day's count |
| `baseline_median` | Median count over the previous 28 days |
| `ratio` | `article_count / baseline_median`. Empty when the median is 0. |
| `is_spike` | True when the count is at least 2 times the median and at least `MIN_ARTICLES` |
| `checked_at` | When the check ran |

## How a run works

Each theme in `theme_sector_map.json` gets three [DOC 2.0 API](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/) calls:

| Call | Mode | Stored in |
|---|---|---|
| Daily article count, last 3 months | `timelinevolraw` | `gdelt_theme_days.article_count`, `total_articles` |
| Daily average tone, last 3 months | `timelinetone` | `gdelt_theme_days.avg_tone` |
| Up to 75 articles for the target day | `artlist` | Up to 5 from `evidence_domains`, in `gdelt_articles` |

1. The target day is the most recent complete UTC day, which is yesterday. Today's partial count is never stored.
2. `Pacer` waits 5.5 seconds between calls, because GDELT allows one request every 5 seconds. Three themes take about 45 seconds.
3. If a theme fails, the run logs it and moves on to the next theme.
4. `spikes.py` checks the target day and the job upserts the result.

| File | Role |
|---|---|
| `client.py` | API calls, pacing, and parsing GDELT's responses |
| `spikes.py` | The spike rule. A pure function with no database or network access. |
| `themes.py` | Loads and validates `theme_sector_map.json` |
| `job.py` | The daily run. Also the scheduler entry point `run_scheduled`. |

## Run

```bash
cd development/backend
source ../../.venv/bin/activate
python -m pytest -v tests/test_gdelt.py   # 21 tests. Set TEST_DATABASE_URL to include the database test.
python -m src.services.gdelt.job          # one live run, writes to DATABASE_URL
```

The scheduler (`src/pipeline/scheduler.py`) runs the job daily at 9:30 America/Los_Angeles, 30 minutes after the watchlist pipeline.

To print live data without writing to the database:

```bash
python -c "
import requests
from src.services.gdelt.client import DocApiClient
c = DocApiClient(requests.Session())
vol = c.volume('ECON_OILPRICE')
for d in sorted(vol)[-7:]: print(d, vol[d])
"
```

## Known limits

- **Campus network.** The campus network (CENIC) shares one public IP, and GDELT often answers it with HTTP 429. Run live checks from a hotspot or home network.
- **Gaps in GDELT's data.** A live tone pull on 2026-10-14 had no data for 3 to 5, 7 and 9 October or after 10 October. `spikes.py` counts a missing day in the baseline as 0 articles, which can produce false spikes if the gaps are GDELT outages. We will confirm against the volume series before changing the rule.
- **`MIN_ARTICLES = 20` is a placeholder.** Set it after reviewing a few weeks of real daily counts.
- **The theme map is a starter.** `theme_sector_map.json` lists three themes with empty `sectors`. Filling in sectors, directions and reasons is a separate task.
- **The parsers are partly unverified.** The tone parser has worked on a live response. The volume and article parsers have only seen test data.

## Planned: company attention ranking

This will rank companies by how much news coverage they get, and how positive or negative that coverage is. It ranks attention, not quality: a company in a scandal ranks high. Rankings by size or financials come from FMP, not GDELT.

The plan is still open:

- **Source.** The DOC API can only count a list of names we give it, at about 5 seconds per name. Raw GKG files list every organization in every article, at about 430 MB of downloads a day. BigQuery can rank any date range in one SQL query, but a careless query can use up the monthly free quota.
- **Names to tickers.** GDELT returns names like "Apple Inc", not tickers. Variants have to be merged and mapped to one ticker. Names with other meanings, such as Target, Gap or Visa, need special handling.
- **Universe.** Either the watchlist tickers only, or every company GDELT mentions.
- **Output.** A new table for daily rank per company, with mention count and average tone.
