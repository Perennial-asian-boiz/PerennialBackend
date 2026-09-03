# PerennialBackend

Backend for Project Perennial — CSULB Senior Project 2026, team Asian Boiz.

This repo replaces `haohnguyen94-droid/project-perennial`, where five branches
ran for three months off a single README-only `main` and never merged. Work is
being ported branch by branch onto a trunk that is settled first.

## Status

| Landed | Component | From |
|---|---|---|
| ✅ | Consensus watchlist pipeline (Phase B) | `hong-working` |
| ⬜ | Postgres schema, Alembic migrations, ADRs | `bryan-working` |
| ⬜ | YouTube / web-search ingestion | `cohen-working` |
| ⬜ | Cloudflare worker | `cohen-cloudflare` |

## Layout

```
development/
  backend/
    .env.example          # copy to .env — the only credentials file read
    src/services/consensus_watchlist/
      paths.py            # repo-root, .env and data-dir resolution
      consensus.py        # aggregates + ranks the four signals
      fetchers/           # fmp, ark, insider, short_interest
      scheduler/cron.py   # pipeline entry point (--test runs everything now)
  database/local_data/    # fetcher JSON output, gitignored
requirements.txt
```

## Quick start

```bash
pip install -r requirements.txt
cp development/backend/.env.example development/backend/.env   # add your keys
python3 development/backend/src/services/consensus_watchlist/scheduler/cron.py --test
```

See [`consensus_watchlist/README.md`](development/backend/src/services/consensus_watchlist/README.md)
for data sources, bucket rules, and per-stage commands.

## Conventions for the branches still to land

- **One `.gitignore`, one `requirements.txt`, one `.env.example`** — at the
  paths above. Three competing copies of each is what made the old repo
  unmergeable.
- **Never hardcode a path depth.** Import `paths.py` rather than counting
  `parents[N]` or matching on the repo's directory name.
- **Credentials live in `development/backend/.env`.** Nothing else is loaded.
