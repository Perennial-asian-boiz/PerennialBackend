# PerennialBackend

Backend for Project Perennial — CSULB Senior Project 2026, team Asian Boiz.

This repo replaces `haohnguyen94-droid/project-perennial`, where five branches
ran for three months off a single README-only `main` and never merged. Work is
being ported branch by branch onto a trunk that is settled first.

## Status

| Landed | Component | From |
|---|---|---|
| ✅ | Consensus watchlist pipeline (Phase B) | `hong-working` |
| ✅ | PostgreSQL snapshots, migrations, publication pipeline and tests (deployment pending) | `database-ingestion-pipeline` |
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
      scheduler/cron.py   # legacy JSON pipeline (--test makes live calls)
  database/local_data/    # fetcher JSON output, gitignored
  database/               # PostgreSQL Compose, Alembic migrations — see database/README.md
  backend/src/db/, backend/src/ingestion/  # schema, importer, live collectors, CLI
  backend/src/pipeline/   # database orchestration, replay, health, production scheduler
requirements.txt
```

## Quick start

```bash
pip install -r requirements.txt
cp development/backend/.env.example development/backend/.env   # add your keys
python3 development/backend/src/services/consensus_watchlist/scheduler/cron.py --test
```

See [`consensus_watchlist/README.md`](development/backend/src/services/consensus_watchlist/README.md)
for legacy JSON commands. For the database workflow, follow
[`database/README.md`](development/database/README.md) to migrate PostgreSQL,
then run `python -m src.ingestion.cli run-pipeline` from `development/backend`.
It publishes a verified watchlist without source JSON files. Read the
[operations runbook](docs/technical-specs/OPERATIONS.md) before deployment.

## Conventions for the branches still to land

- **One `.gitignore`, one `requirements.txt`, one `.env.example`** — at the
  paths above. Three competing copies of each is what made the old repo
  unmergeable.
- **Never hardcode a path depth.** Import `paths.py` rather than counting
  `parents[N]` or matching on the repo's directory name.
- **Credentials live in `development/backend/.env`.** Nothing else is loaded.

### Backend quality checks

Use Python 3.12 (`.python-version`) and PostgreSQL 17. Install
`requirements-dev.txt` into `.venv`, then run `python -m ruff check`,
`python -m ruff format --check`, and `python -m mypy` from the repository root.
For the mandatory synthetic PostgreSQL suite, migration round-trip/drift check,
dependency audit, and protection activation, see
[Phase 0 quality gates](docs/technical-specs/OPERATIONS.md#phase-0-quality-gates).
Historical independent reviews are preserved in [docs/reviews](docs/reviews/README.md).
