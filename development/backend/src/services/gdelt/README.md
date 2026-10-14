# GDELT macro events

Watches GDELT themes (oil prices, interest rates, trade disputes, and so on) and records when coverage of a theme spikes. This feeds the Insights page's event-to-sector-to-company flow.

## What a run does

The run uses the [GDELT DOC 2.0 API](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/), which needs no key. It makes three calls per theme in `theme_sector_map.json`:

| Call | Mode | Stored in |
|---|---|---|
| Daily article count, last 3 months | `timelinevolraw` | `gdelt_theme_days.article_count`, `total_articles` |
| Daily average tone, last 3 months | `timelinetone` | `gdelt_theme_days.avg_tone` |
| Up to 75 articles for the target day | `artlist` | Up to 5 from `evidence_domains`, in `gdelt_articles` |

The target day is the most recent complete UTC day, which is yesterday. A day counts as a spike when its article count is at least 2 times the median of the previous 28 days and at least `MIN_ARTICLES`. These rules are in `spikes.py`. Every theme-day that has enough history gets a row in `gdelt_spike_checks`, and readers filter on `is_spike`.

GDELT allows one request every 5 seconds. `Pacer` waits 5.5 seconds between calls, so three themes take about 45 seconds. A theme that fails is logged and skipped, and the other themes still run. Every write is an insert or an upsert, so rerunning a day is safe.

These tables are separate from the watchlist sources and publications, so a GDELT outage cannot block the watchlist from publishing.

## Run

```bash
cd development/backend
python -m src.services.gdelt.job        # one run against DATABASE_URL
python -m pytest tests/test_gdelt.py    # tests (set TEST_DATABASE_URL for the database test)
```

The scheduler (`src/pipeline/scheduler.py`) runs it daily at 9:30 America/Los_Angeles, 30 minutes after the watchlist pipeline.

## Known limits

- The campus network (CENIC) shares one public IP, and GDELT answers it with HTTP 429 for every request. Run live checks from another network.
- `MIN_ARTICLES = 20` is a placeholder. Set it after reviewing a few weeks of real daily counts.
- `theme_sector_map.json` is a starter list with empty `sectors`. Filling in sectors, directions and reasons is a separate task.
