# Perennial database

PostgreSQL storage for the four watchlist sources (Congress trades, ARK
holdings, insider trades, short interest). Imports are validated, idempotent
snapshots: rerunning identical data adds no rows, changed data adds a new
snapshot, and a failed import leaves the previous good snapshot current.

| Piece | Location |
|---|---|
| Local PostgreSQL (Docker Compose) | `development/database/docker-compose.yml` |
| Migrations (Alembic) | `development/database/alembic.ini`, `migrations/` |
| Tables, engine, config, read queries | `development/backend/src/db/` |
| Validation, hashing, importer, collectors, CLI | `development/backend/src/ingestion/` |
| Tests and synthetic fixtures | `development/backend/tests/` |

## Setup

Requires Python 3.10+ and Docker.

```bash
pip install -r requirements.txt
cp development/backend/.env.example development/backend/.env   # if you have no .env yet
docker compose -f development/database/docker-compose.yml up -d
cd development/database && alembic upgrade head
```

`DATABASE_URL` comes from the environment first, then `development/backend/.env`.
The example value targets the Compose database on `localhost:5433`. Its
`perennial`/`perennial` credentials are development-only; never reuse them for a
shared database, and keep any real URL only in your untracked `.env`. A missing
or malformed URL gives a fixed error message that never echoes the URL.

Compose publishes PostgreSQL on `127.0.0.1` only. The local `perennial` role
owns the schema. For a shared database, run migrations with an owner role and
use the explicit table and column grants in `ops/provision_roles.py`. Source
snapshots, publication inputs and publication outputs are immutable for the
runtime role; only run state, the current pointer and security metadata may be
updated. Readers only need `SELECT`. See [operations](../../docs/technical-specs/OPERATIONS.md).

## Importing

Run from `development/backend`. Exit code 0 means the import succeeded, 1
means a failed run was recorded, and 2 means the command could not complete (configuration, connection, or pipeline
failure). Earlier stages may have recorded runs before a pipeline failure.

```bash
# Live collection through the explicit-outcome wrapper (all units must succeed)
python -m src.ingestion.cli fetch ark_holdings

# Synthetic/sanitized fixture (recorded as collection_mode = fixture)
python -m src.ingestion.cli import-file ark_holdings tests/fixtures/ark_holdings.json --fixture

# Existing fetcher output: requires an explicit completeness attestation
python -m src.ingestion.cli import-file ark_holdings ../database/local_data/ark_holdings.json --attest-complete

# Inspect the current snapshot
python -m src.ingestion.cli latest ark_holdings --limit 5
```

Sources are `congress_trades`, `ark_holdings`, `insider_trades` and
`short_interest`. The legacy fetcher scripts and `scheduler/cron.py` retain
the JSON workflow and its schedule. The database workflow is a separate entry
point; production deployments should use it:

```bash
# From development/backend; needs DATABASE_URL and provider credentials
python -m src.ingestion.cli run-pipeline
python -m src.ingestion.cli current
python -m src.ingestion.cli replay 1   # use a saved publication ID
python -m src.ingestion.cli health    # exit 1 when unhealthy
python -m src.pipeline.scheduler      # startup catch-up, then daily 09:00 PT
```

The database scheduler refreshes all four sources daily, including short interest
for the current candidate universe. Settlement dates remain provider dates.
It runs under a process supervisor; advisory locks prevent concurrent workflows
and source workers. The legacy JSON schedule remains daily with an additional
short-interest job at 09:10 on the 1st/15th. Do not start both paths for the same
production workload. `--test` on either scheduler performs LIVE provider calls.
The database path does not refresh `local_data/*.json`; readers use `current`
or the `current_publication` join. The legacy file consumers require an explicit
migration before cutover.

### Collection outcome rules

The existing fetchers swallow errors and save or return partial or empty lists,
so neither an empty list nor an empty `errors[]` proves success.

- **Live (`fetch`)** calls each provider unit itself (ARK fund, FMP chamber
  page, Senate Watcher, insider ticker, short-interest ticker) and validates
  the response structure before the fetcher's parse function runs. Any failed
  or malformed unit fails the whole collection. That includes FMP HTTP 402
  page limits: truncation is never accepted. Completing every unit with zero
  records is a successful empty snapshot. Congress trades whose parsed
  transaction date is not ISO fail the collection instead of being dropped
  by the fetcher's two-year filter. Database insider and short-interest ticker plans
  come from pinned Congress/ARK batches, recorded in `run_dependencies`.
  Supplied database rows never fall back to files. Standalone collector usage
  may read legacy upstream files with bounded, validated input. A missing or
  malformed upstream plan fails with `missing_upstream`. A plan
  of more than 2,000 tickers fails with `plan_too_large` instead of being
  truncated. Congress `source_link` values must be plain http(s) URLs: no
  credentials, no credential-like query keys, and no fragment.
- **Fixture files** (`--fixture`) attest themselves. Use fixtures only against
  a disposable development database: `latest` includes successful runs of
  every mode, so a fixture import becomes the operational latest snapshot. It cannot
  change the published production watchlist.
- **Fetcher output files** fail with `collection_unattested` unless you pass
  `--attest-complete`, meaning you know the run that wrote the file finished
  without errors. An empty file always needs `--attest-complete`. A
  short-interest file with a non-empty `errors` list is always rejected.

A failed collection records a failed run and writes no batch.
Collectors stop at the first failed provider unit. Diagnostics record planned,
attempted, completed, failed and not-attempted counts; remaining units are not
requested after a collection is doomed.

## Publication and replay

A successful import is separate from production publication. `run-pipeline`
records each attempt before network I/O, pins downstream dependencies, then
promotes one publication only after all stages pass. Failure retains the previous
publication. Runs have heartbeats; `recover-stale --minutes 10` abandons expired
workers, and abandoned workers cannot commit a late batch.
Heartbeat connectivity errors retry with interruptible, capped backoff; a
guarded update matching no running row marks the lease lost. Importing after
lease loss returns `failed/worker_abandoned` and leaves the finalized run intact.
The database scheduler emits fixed diagnostics and passes a fixed-message
exception to APScheduler, with no raw exception context attached to error events.

Publication requires successful live runs with complete coverage and exact
upstream lineage. Default collection age limits are 3 days for Congress, ARK
and insiders, 45 days for short interest; ARK observation dates must be within
7 days and short-interest settlement dates within 45 days. These are initial
engineering defaults, not an agreed service-level objective. Market caps are
pinned observations with retrieval time, provider and USD currency; missing
values stay unresolved, and an entirely missing market-cap universe is rejected.
Fixture/file imports and unverified legacy runs cannot publish. A publication
cannot regress collection time or known source observation dates.

`replay` reads pinned batches, market-cap observations, ranking version and
configuration; it needs no provider calls or current clock. Keep released ranking
implementations available when introducing a new version. Batches and published
inputs are retained indefinitely until an explicit reference-aware retention
policy is implemented; only failed-run diagnostics currently expire.

## Data model (schema `perennial`)

| Table | Contents |
|---|---|
| `securities` | One row per normalized symbol; optional exchange/currency. Reused across sources. |
| `source_batches` | One distinct snapshot per source. Unique `(source, content_hash)`. `payload` JSONB holds the hashed records (including short-interest history) plus a sanitized envelope. |
| `ingestion_runs` | Every attempt: status, mode, start/finish time (UTC), input/accepted counts, batch used, whether it was reused, error code/summary, bounded diagnostics. |
| `congress_trades`, `insider_trades` | Rows keyed by `(batch_id, row_number)`, so repeated identical trades are kept. |
| `ark_holdings`, `short_interest` | Rows keyed by `(batch_id, security_id)`. |
| `run_dependencies` | Exact upstream batches used by each downstream run. |
| `publications`, `publication_sources` | Saved ranking/output and its four source runs/batches. |
| `market_cap_observations`, `publication_market_caps` | Immutable cap values and the observations used by each publication. |
| `current_publication` | One atomic production pointer; imports alone never move it. |

Rules worth knowing:

- **Batch identity (hash `v2`, `src/ingestion/canonical.py`).** The hash covers
  the validated business fields only: never `fetched_at`, totals, notes or
  unknown keys. Records are sorted by their canonical JSON, so fetcher order
  does not matter but duplicates still count. The hash also includes the
  provider transaction identity when supplied (Congress), canonical
  short-interest history fields, and provider `source_as_of` date when there is one (ARK supplies it; the others
  leave it null). `row_number` follows the canonical order.
  Upgrading from hash v1 to v2 creates one equal-content snapshot per source;
  both versions coexist harmlessly, with publications pinning their exact batches.
- **Identical import:** reuses the batch and its rows, and records a new
  succeeded run with `reused_batch = true`. **Changed snapshot:** creates a new
  batch and keeps the old one.
- **Atomicity:** the batch, its securities, its rows and the succeeded run
  commit together. On any error everything rolls back, then the failed run is
  recorded separately. Concurrent identical imports produce one batch.
- **Types:** timestamps are `timestamptz` in UTC; date-only fields are `date`
  (ISO `YYYY-MM-DD` only, blank means null, other text fails the batch).
  Money, weights and ratios are `numeric`, converted from floats via `str()`
  and rounded half-even to the column scale before hashing. ARK `share_price`
  0.0 is stored as null because `ark.parse_holdings` uses 0.0 to mean
  "missing". `total_weight` is a sum across funds, so it has no 0–100 cap.
- **Short interest** maps the fetcher's `short_interest` and `avg_daily_volume`
  to `short_interest_shares` and `average_daily_volume`. Returned history is
  kept in the batch payload only.
  The database collector uses `parse_short_interest_strict`; malformed numbers
  fail collection. The separate legacy parser keeps the ticker and sets a
  malformed field to null. `consensus.py` intentionally fixes its old alias
  mismatch: valid short-interest signals now populate the JSON watchlist where
  they were always null at `fab7188`.
- **Congress adapters and versioned ranking.** The legacy JSON writer uses
  `fmp.deduplicate`, retaining the first row for the `fab7188` key
  `(politician_name.lower(), ticker, transaction_date, trade_type.lower())`.
  The database collector uses `deduplicate_proven_repeats`, preserving
  cross-provider observations and unidentified repeats without changing hash v2.
  New publications use `watchlist-v2`: purchases are grouped in Python by
  `(politician_name.strip().casefold(), symbol, transaction_date,
  trade_type.strip().casefold())`. Each key counts the largest row count from
  one `data_source` (null is its own provider). The provider with the lowest
  canonical `row_number` wins equal-count ties; kept rows supply amounts and
  dates. Thus an FMP/Watcher pair counts once, but two same-provider rows still
  count twice. Raw snapshot rows are untouched. Replay dispatches by the saved
  ranking version, preserving the original `watchlist-v1` output.
- **Securities and the supported universe.** This sprint covers US equities
  only. Symbols are upper-cased and limited to `A-Z 0-9 . / -`. Placeholders
  (`N/A`, `--`, `NONE`, `NULL`, `NAN`) and any other form fail validation.
  Class shares keep their punctuation (`BRK.B` and `BRK-B` stay distinct).
  ARK reports some US listings with a Bloomberg composite exchange suffix
  (observed in the 2026-10-06 live ARK collection: `RKLB UQ`, `ARCT UQ`,
  `SYM UQ`, `DKNG UW`).
  Only these suffixes are recognized: `UQ`, `UW` and `UR` map to `NASDAQ`, `UN`
  to `NYSE`, `UA` to `NYSEAMERICAN` and `UP` to `NYSEARCA`. A recognized suffix
  becomes the base symbol plus that exchange. Foreign or other suffixes
  (`HK`, `LN`, ...) fail validation. ARK rows without a ticker (cash and
  similar) are skipped by the fetcher. Suffix reference: Bloomberg Global
  Equity Indices Methodology, Appendix III,
  <https://data.bloomberglp.com/professional/sites/10/Bloomberg-Global-Equity-Indices-Methodology.pdf>.
  An import that would give an existing symbol a different exchange or
  currency fails with `security_conflict`.

### Failed-input diagnostics policy

Failed inputs are never archived. A run stores only an error code from a fixed
list, a summary rendered from that code's template, and diagnostics in a
closed shape (`src/ingestion/diagnostics.py`): counts, record indexes, known
field paths, pydantic error types, ticker-shaped unit names and fixed failure
codes. Input values, payload text, URLs, query strings and exception messages
are never stored or printed. Each diagnostics list holds at most 20 entries,
and each run's diagnostics stay under 8 KB. After every failed run, the
diagnostics of that source's older failed runs are cleared once they fall
outside the newest 20 or are older than 30 days. Run rows themselves are kept.

The batch payload envelope keeps only allowlisted top-level fetcher fields
(`src/ingestion/envelope.py`). Strings are redacted and capped, notes and
per-ticker summaries are dropped, and short-interest `errors` text is reduced
to a count.

## Querying

Operational latest data for a source is the batch referenced by its latest successful
run, even when that run reused an older batch. Never sum rows across batches:
each batch is a full snapshot.

```python
from src.db.session import make_engine
from src.db.queries import latest_rows, latest_successful_run

engine = make_engine()
with engine.connect() as conn:
    run = latest_successful_run(conn, "ark_holdings")   # batch_id, finished_at, source_as_of, ...
    rows = latest_rows(conn, "ark_holdings")            # list of dicts with `symbol`
```

```sql
-- Latest successful batch per source
SELECT DISTINCT ON (source) source, batch_id, finished_at, reused_batch, collection_mode
FROM perennial.ingestion_runs
WHERE status = 'succeeded'
ORDER BY source, finished_at DESC, id DESC;

-- Operational latest ARK snapshot; production readers use publication_sources
WITH cur AS (
  SELECT batch_id FROM perennial.ingestion_runs
  WHERE source = 'ark_holdings' AND status = 'succeeded'
  ORDER BY finished_at DESC, id DESC LIMIT 1
)
SELECT s.symbol AS ticker, h.company, h.funds, h.fund_count, h.total_weight, h.share_price
FROM perennial.ark_holdings h
JOIN cur ON h.batch_id = cur.batch_id
JOIN perennial.securities s ON s.id = h.security_id
ORDER BY h.fund_count DESC, h.total_weight DESC;

-- Recent failures
SELECT id, source, collection_mode, finished_at, error_code, error_summary
FROM perennial.ingestion_runs WHERE status = 'failed' ORDER BY id DESC LIMIT 10;
```

## Tests

Compose creates only the `perennial` database. Create the test admin database
once, then run the suite:

```bash
docker compose -f development/database/docker-compose.yml exec postgres \
  createdb -U perennial perennial_test        # use your PERENNIAL_DB_USER if you overrode it
cd development/backend
TEST_DATABASE_URL=postgresql+psycopg://perennial:perennial@localhost:5433/perennial_test python -m pytest
```

Without `TEST_DATABASE_URL`, local database tests are skipped. CI sets
`REQUIRE_DATABASE_TESTS=1`, which fails immediately if the test URL is missing. The test URL must point at a loopback host, include `test` in the
database name, and carry no query parameters other than `sslmode`. The test
process removes every `PG*` environment variable and pins `hostaddr` to
loopback, so libpq defaults such as `PGHOSTADDR` or `PGSERVICE` cannot redirect
it. The role needs `CREATEDB`: each session creates `perennial_test_<random>`,
migrates it, and drops exactly that database. Tests use synthetic fixtures and
fake HTTP sessions only, and never load the real `.env`.

## Migration history

`0001` introduces snapshots; `0002` introduces lifecycle and publications;
`0003` renames the doubled CHECK constraint names from `0001` in place and adds
lineage/health indexes. Historical migrations remain unchanged. Upgrade/downgrade,
actual CHECK names, constraint enforcement and preservation of existing rows
are tested against PostgreSQL. Run migrations as the owner before application
startup; the runtime account does not have DDL privileges.

Hash v2 deliberately creates a new identity after the Congress identity and
short-interest history normalization change. Existing v1 batches remain
readable and replayable; they are never rehashed or updated.
