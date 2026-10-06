# Perennial Database Implementation Sprint

**Goal:** Get the existing watchlist data into PostgreSQL through repeatable, inspectable imports.  
**Proposed duration:** Two weeks / 10 working days, with one primary data owner.  
**Status:** Sprint proposal; tasks and commands below are not implemented yet.  
**Repository baseline:** `main` at `fab7188` · **Updated:** September 23, 2026

## 1. Sprint outcome

By the end of this sprint, a teammate can start the database, apply migrations, import existing fetcher output, and query the latest successfully imported data. Repeating an import must not duplicate the same batch, and a failed import must leave previously valid data usable.

**First milestone: one source flowing into PostgreSQL by Day 4.** Do not wait until every source is integrated to demonstrate database ingestion.

The repository already has collectors for Congress trades, ARK holdings, insider trades, and short interest. Reuse their output contracts. The database directory currently contains only a placeholder; inspect the unmerged database work mentioned in the root README before starting a competing schema.

## 2. Scope and stack

| Include this sprint | Defer |
|---|---|
| PostgreSQL connection and local setup | New orchestration platform or cloud scheduler |
| Alembic migrations and essential tables | Full temporal/revision architecture |
| Imports for the four existing watchlist outputs | Fundamentals, news, sentiment, and composite scores |
| Run status, basic validation, safe reruns | Serving APIs and frontend integration |
| One verified live source-to-database flow | Object storage, data warehouse, streaming, dashboards |
| Query examples and teammate setup instructions | Rewriting all fetchers or the repository package layout |

| Tool | Decision and reason |
|---|---|
| PostgreSQL | Use locally through Docker Compose; connect to the team's Supabase PostgreSQL instance for shared development if available. Same schema in both. |
| SQLAlchemy 2 + psycopg 3 | Shared connection handling, parameterized writes, and transactions. |
| Alembic | One migration history checked into the repository. Reuse existing branch work where sound. |
| Pydantic 2 | Validate each source's normalized input before committing a batch. |
| pytest + PostgreSQL | Verify imports, constraints, reruns, and rollback against the actual database engine. |
| Existing root `requirements.txt` | Add the needed dependencies here. Dependency-manager migration is outside this sprint. |

Keep local credentials in the existing `development/backend/.env`. Use `DATABASE_URL`; never commit real credentials. The database importer consumes existing Python/JSON output, so no HTTP-library change is needed.

## 3. Minimal ingestion design

```text
Existing fetcher -> returned records / existing JSON output
                -> validate and identify import batch
                -> PostgreSQL transaction
                -> source tables + successful ingestion record
```

Start with a command that imports existing JSON files. Then connect one existing fetcher directly to the same importer. Both entry points must call the same validation and persistence functions.

### Tables to implement

Use one application schema for this sprint. Choose the schema name after inspecting existing migrations.

| Table | Minimum fields | Purpose |
|---|---|---|
| `securities` | ID, normalized symbol, optional exchange/currency | Stable internal references for the small initial universe. Resolve only unambiguous symbols; reject ambiguous mappings. |
| `ingestion_runs` | ID, source, status, started/finished times, input/accepted counts, nullable batch ID, error summary | Shows whether an import succeeded, failed, or reused an existing batch. |
| `source_batches` | ID, source, content hash, imported time, nullable source-as-of date, input payload JSONB | Preserves one distinct imported source snapshot. Unique `(source, content_hash)`. |
| `congress_trades` | Batch/security IDs, row number, politician, chamber, type, transaction/disclosure dates, amount label, source link | Stores the fields already produced by `fmp.py`. |
| `ark_holdings` | Batch/security IDs, funds, fund count, total weight, nullable share price | Matches the current aggregated ARK output. Individual per-fund positions are a later enhancement. |
| `insider_trades` | Batch/security IDs, row number, insider name, type, transaction date, shares, value | Stores the transactions already produced by `insider.py`. |
| `short_interest` | Batch/security IDs, settlement date, short-interest shares, average daily volume, nullable days to cover | Stores the current observations produced by `short_interest.py`. Keep returned history in the batch payload for now. |

**This sprint stores source snapshots.** Repeated trades across different snapshots are expected history. Queries must select one successful batch per source before counting activity. A complete, reconciled transaction history is deferred.

The payload is the existing fetcher's output, not necessarily the original provider response. Do not label it a complete raw-provider archive.

### Required rules

- Use foreign keys from source rows to batches and securities.
- Enforce a unique normalized symbol within the explicitly limited initial US-equity universe; reject conflicting exchange mappings. Reuse security IDs on import. A broader exchange/provider alias model is deferred.
- Use UTC timestamps, dates for date-only fields, and `NUMERIC`/`Decimal` for monetary values. Unknown values stay null.
- For Congress and insider rows, use unique `(batch_id, row_number)` to preserve distinct same-day trades. Do not collapse them using politician/name + ticker + date.
- For ARK and short-interest rows, require unique `(batch_id, security_id)` for the current output shapes.
- Hash canonical business content for batch identity: deterministic field/record ordering, preserve repeated rows, exclude retrieval/run timestamps, and retain meaningful dates. Version this hashing rule in code.
- An identical batch reuses existing rows but creates a new ingestion-run result recording the successful check. A changed snapshot creates a new batch.
- Commit a new batch, all its source rows, and successful run state atomically. Roll back all batch writes on failure, then record the failed run separately.
- Expose a small query/helper selecting the batch referenced by the latest successful ingestion run for each source. Do not sum across all historical batches.
- Index successful-run lookup by source and finish time, and source-table reads by batch ID.
- Preserve source-as-of dates separately from import time. If the collector does not provide a source date, leave it unknown.

ARK `total_weight` is the current sum of weights across funds; it is not a single portfolio allocation. Do not add a misleading 0–100 allocation constraint.

## 4. Sprint backlog

| Ticket | Target | Work | Acceptance criteria |
|---|---|---|---|
| **DB-01 — Confirm starting point** | Day 1 | Inspect existing database branch/migrations, confirm database access, choose initial source and small symbol sample, obtain sanitized fixtures for all four outputs. | One agreed schema/migration starting point; provider access gaps documented; fixtures available. |
| **DB-02 — Database setup** | Days 1–2 | Add local PostgreSQL Compose setup, shared connection/session module, environment validation, and required dependencies. | A fresh checkout connects using documented steps; invalid/missing configuration produces a useful error without exposing credentials. |
| **DB-03 — First migration** | Days 2–3 | Create the seven tables, foreign keys, batch uniqueness, source-row uniqueness, and essential query indexes. | `alembic upgrade head` succeeds on an empty PostgreSQL database; invalid foreign keys and duplicate batch identities are rejected. |
| **DB-04 — First source end to end** | Days 3–4 | Build shared batch importer and one source mapping; start with ARK if access works, otherwise an accessible existing source. Add JSON-file and direct-fetch entry points. | Fixture and live data reach the source table; rerunning identical data does not add another batch or duplicate its rows. |
| **DB-05 — Remaining source mappings** | Days 5–7 | Map Congress, insider, ARK, and short-interest outputs through the shared importer; use fixtures wherever live access is unavailable. | All four fixture types import correctly; dates, nulls, numbers, and row counts match the inputs. Access-blocked integrations remain explicitly marked. |
| **DB-06 — Failure handling and handoff** | Days 8–10 | Verify rollback, run status, latest-successful-batch queries, and repeat imports. Add setup/import/query examples and demonstrate the flow to a teammate. | A failed import leaves previous valid data queryable; a teammate runs the documented workflow; at least one live source succeeds. |

**Ownership:** You own migrations, database access, import contracts, and verification. Fetcher contributors provide representative payloads and make only the small return/status changes needed to distinguish successful collection from failure. The whole team does not need to pause while database setup proceeds.

## 5. Integration with the current repository

| Existing component | Change this sprint |
|---|---|
| `fetchers/fmp.py` | Feed its `trades` output to the Congress importer. Preserve source links and amount labels. |
| `fetchers/ark.py` | Feed aggregated `holdings` to the ARK importer. Preserve `funds`, `fund_count`, and `total_weight`. |
| `fetchers/insider.py` | Feed `transactions` to the insider importer. Record failed checks distinctly from successful empty results. |
| `fetchers/short_interest.py` | Map `short_interest` to `short_interest_shares` and `avg_daily_volume` to `average_daily_volume` explicitly. This addresses the existing naming mismatch at the database boundary. |
| `consensus.py` | Keep its existing JSON path working. Supply an example database query/export for its future integration; migrating ranking is not a sprint dependency. |
| `scheduler/cron.py` | Keep existing scheduling unchanged. Database imports must work manually before scheduling is expanded. |
| `development/database/` | Add local setup and migrations, unless existing branch work establishes a better location. |
| `development/backend/src/` | Add a focused `db/` module for sessions/models and an ingestion module for validators/importers. Avoid restructuring unrelated services. |

Do not assume an empty JSON array proves collection succeeded: current fetchers sometimes swallow errors. The import interface needs an explicit collection outcome. During the sprint, fixture imports identify themselves as fixtures, and a live wrapper must pass success/failure accurately. A failed source must not replace the latest successful batch with an empty batch.

For initial implementation, fail the entire import on malformed required records and retain the input/error for diagnosis. Per-record quarantine and partial-batch policy can follow later. This keeps the first release's behavior predictable.

## 6. Definition of done

- [ ] Database can be started and migrated from a clean checkout.
- [ ] Credentials are external to source control.
- [ ] All four existing source formats import from sanitized fixtures.
- [ ] At least one existing fetcher writes a successful live collection into PostgreSQL through the shared importer.
- [ ] Importing an identical batch twice reuses the batch and its rows.
- [ ] Changed input creates a new snapshot, preserving the previous snapshot.
- [ ] A malformed batch produces a failed run and no partial source rows.
- [ ] Failed collection is distinguishable from a successful empty result.
- [ ] Queries return one latest successful batch per source without counting older snapshots again.
- [ ] Setup, migration, import, and inspection instructions have been followed by another teammate.

The handoff demo is simple: ingest one source, query its rows, rerun it, show unchanged batch/row counts, then force a failure and show that the previous valid data remains available.

## 7. Scope protection and dependencies

**Dependencies to resolve on Day 1:** access to the existing database work, a PostgreSQL instance, sample payloads, and live access to at least one source. Live access for all four sources is desirable but is not required to prove their database mappings.

**If the sprint slips:** finish the shared importer, first live source, migrations, and failure/rerun checks. Move remaining source mappings to the next sprint rather than dropping transaction safety or claiming fixture-only work is live integration.

**Next sprint candidates:** switch consensus to database reads, add persistent market-cap collection, improve transaction reconciliation and source identity, then consider scheduling and financial-statement ingestion. These are not acceptance conditions for this sprint.

## References

- [Current repository overview](../../README.md)
- [Existing watchlist pipeline contracts](../../development/backend/src/services/consensus_watchlist/README.md)
- [Existing scoring decisions — downstream context](DECISIONS.md)
- [SQLAlchemy transaction/session documentation](https://docs.sqlalchemy.org/en/20/orm/session_basics.html)
- [Alembic migration tutorial](https://alembic.sqlalchemy.org/en/latest/tutorial.html)
