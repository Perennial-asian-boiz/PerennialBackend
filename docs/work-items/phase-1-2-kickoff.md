# Phase 1 + Phase 2 kickoff

**For:** Codex primary (perennial-pipeline root), which assigns the `sources` seat.
**From:** the Claude coordinator, with Bryan's approval to start.
**Date:** 2026-10-09. **Base:** `origin/main` at `6915fb0` (PR #6 merged; CI `Backend quality` green; `main` protected).
**Roadmap:** `production-roadmap.md` §3 Phase 1 and Phase 2, §4 findings 1, 2, 3, 6, 7, 8, 12.

## Ground rules

- **One local branch per packet**, cut from `origin/main`, named as in the table below. Rebase on `main` before handing off for review.
- **Commit on local branches. Pushing and opening PRs need Bryan's go-ahead per packet.** Ask in your handoff note; don't assume it carries over from PR #6.
- No deploys, live provider calls or real credentials. The only interpreter is the repository's `.venv/bin/python` (3.12). Tests use your own isolated PostgreSQL 17 on port 55471.
- **Leave these unchanged:** `scheduler/cron.py` (retired in Phase 4), and `tests/test_pipeline_contract.py`, which the database rig owns. If you think a fixture there needs to change, ask the coordinator.
- `fetchers/insider.py` belongs to haohnguyen94-droid. You can edit it, but every PR that touches it needs their review. Keep those edits small and mechanical.
- **Migrations:** Alembic head is `0003`. Only P1-B may need a migration (see below). It has to follow roadmap §2.4: expand-only, a tested downgrade and `lock_timeout`, with a one-page design note sent to the coordinator *before* you write it.
- Definition of done (§2.3) applies to every packet. In particular, every behavior change needs a test that fails on `6915fb0`, and you record the evidence that it failed.
- Status and evidence go in issue #5's successor issues (see "Tracking" below) and in `/private/tmp/perennial-pipeline-rig/<seat>-status.md`, which seats use to wake each other.

## Packets and owners

The file ownership rule exists to avoid merge conflicts: **`sources` owns every edit under `fetchers/**` and `src/ingestion/collectors.py`**, and primary owns everything else. When a primary packet needs a fetcher change, send it to `sources` as a queue item instead of making the edit yourself.

| ID | Packet | Owner | Branch | Blocked by | Review |
|---|---|---|---|---|---|
| P2-B | Typed settings module | primary (module) → sources (fetcher adoption) | `feat/settings` | — | checker + auditor |
| P1-A | FMP pagination | sources | `feat/fmp-pagination` | — | checker |
| P2-D | Database session settings | primary | `feat/db-session-settings` | — | checker |
| P1-B | Data quality checks before publishing | primary | `feat/publication-quality-checks` | design note if it needs a migration | checker |
| P2-A | Structured logging, remove `print()` | primary (infra) → sources (fetchers) | `feat/structured-logging` | P2-B merged | checker + auditor |
| P2-C | Lock file and split dependencies | primary | `ops/lockfile` | — | auditor |
| P1-C | Provider contract-test harness | sources | `test/provider-contracts` | P1-A merged | checker + auditor (scrubbing) |
| P2-E | Rename legacy functions to `legacy_*`, plus `Protocol` types | sources | `refactor/legacy-names` | P1-A, P2-A merged | checker |
| G0 | Gate 0 cleanup | primary | `ops/gate0-cleanup` | — | Bryan |

**Order.** Primary: P2-B module → P2-D → P1-B → P2-A infra → P2-C. Sources: P1-A → P2-B adoption → P2-A fetchers → P1-C → P2-E. The two queues run in parallel.

---

### P1-A: FMP pagination (sources)

**Problem.** Both pagination loops stop at `fmp.MAX_PAGES = 3`, which is 60 trades per chamber: one in `fetchers/fmp.py:56` (legacy) and one in `src/ingestion/collectors.py:523` (database). The database path fails with `pagination_exhausted` and the legacy path truncates silently.

**Change.**
- Replace the fixed limit with "fetch until a short or empty page", plus a configurable safety cap (default 50 pages, coming from P2-B settings once that lands; use a module constant until then).
- Database path: reaching the cap is still `pagination_exhausted`, a failure and never a silent truncation. Record pages fetched per chamber in the outcome diagnostics.
- Legacy path: the same loop rule. Output for any set of 59 trades or fewer must stay byte-identical to today's.
- HTTP 402, "plan limit", ends pagination as an **explicit failure code** on the database path, not as success. If the plan tier really caps pages, write it up as an ADR in `docs/adr/` for decision D4.

**Tests.** Synthetic sessions with 0, 1, 19, 20, 21, 59, 60 and 61 trades, cap−1, cap and cap+1 pages, and a 402 on page N. The 60- and 61-trade cases must fail on `6915fb0`.

### P1-B: Data quality checks before publishing (primary)

**Change.** Add per-source checks in `src/pipeline/publication.py` before promotion, compared against the previous successful batch for that source:
- row count within the configured limits. Default: block on a drop of more than 50% or a rise of more than 300%. Thresholds are per source, in settings.
- the share of null required fields doesn't rise by more than a configured margin.
- `source_as_of` is strictly later than the previous batch's (or equal, if the source is flagged as not updating every day).
- the first batch for a source has no baseline: it passes, and the run records `no_baseline`.

A failed check blocks publication and returns a fixed error code, added to the vocabulary in `diagnostics.py`. That file belongs to `sources`, so ask them for the codes through the queue.

**Override.** `cli.py publish --override "<reason>"` publishes anyway and records who, why and which checks were overridden. Storing that durably probably needs an expand-only migration `0004`, either a new table or a nullable column. **Send the design note to the coordinator before writing the migration.** Replay must stay byte-identical for existing publications.

**Tests.** Each check passes and fails at its boundary; an override is recorded and audited; there's no baseline on the first run; v1 and v2 publications replay unchanged.

### P1-C: Provider contract-test harness (sources)

**Now:** build the harness. That means `tests/fixtures/providers/<provider>/<endpoint>.json`, a shape extractor (keys, types, nullability, list or object), and a comparison that reports drift as a structured diff. Seed it with **synthetic** fixtures derived from the shapes the collectors already validate (`_FMP_KEYS`, `_WATCHER_KEYS` and so on).
**Blocked on D3 (staging keys):** capturing real scrubbed responses, and the weekly CI job that calls the live APIs and opens an issue when the shape drifts. Write the workflow file disabled (`workflow_dispatch` only) so the auditor can review it, but don't schedule it.

### P2-A: Structured logging (primary infra, sources for fetchers)

**Problem.** There are 164 `print()` calls in `src/`: insider.py 59, fmp.py 32, cron.py 25, ark.py 24, cli.py 14, short_interest.py 9, search_engine.py 1.

**Change.**
- `src/utils/logger.py` gets a JSON formatter, configured once at process start. `run_id`, `source` and `publication_id` are attached through `contextvars`, so callers don't pass them around.
- Replace the prints in fetchers, `ark.py` and `search_engine.py` with leveled log calls. Error text goes through the existing `describe_exception`/redaction path. **Never log raw exception messages or request params, because FMP keys travel in `params`.**
- `cli.py`: output meant for the user stays on stdout through one small `emit()` helper. Diagnostics move to logging.
- Turn on Ruff `T20` for `src/`, exempting only `scheduler/cron.py`, which stays byte-identical.

**Tests.** A sentinel test: run each collector with a failing fake session whose error text contains a fake key and a URL, capture root logging, and assert that neither appears. Also check the JSON shape and that context propagates.

### P2-B: Typed settings (primary module, sources adoption)

- Add `src/settings.py` on `pydantic-settings` with typed fields, `SecretStr` for provider keys and the database URL, the pagination cap and the quality-check thresholds. It's validated **once at process start** by the CLI and scheduler entry points. A missing required setting exits non-zero with a message naming the setting, never its value.
- Remove the module-level `load_dotenv` calls and the `"YOUR_FMP_KEY_HERE"` default from `fmp.py`, `insider.py` and `consensus.py`. Settings are passed into adapters explicitly. `.env` is read only in local dev.
- Pin the new dependency exactly.

**Tests.** Importing any fetcher has no side effects and reads no environment variables. A missing key fails at startup. `SecretStr` never shows up in `repr` or logs.

### P2-C: Lock file (primary)

Split runtime and dev dependency groups in `pyproject.toml` and generate a lock file with hashes (`uv lock` or `pip-tools --generate-hashes`; your choice, recorded in an ADR). CI installs from the lock and keeps `pip-audit`. Add a Dependabot config for weekly updates.

### P2-D: Database session settings (primary)

In `src/db/session.py`, add a `workload` parameter (`pipeline`, `api`, `migrate`, `cli`) that sets:
- `application_name=perennial-<workload>`
- `statement_timeout`: 60s for the pipeline, 5s for the API
- `lock_timeout=5s` and `idle_in_transaction_session_timeout=60s`
- per-workload pool sizes; the pipeline job gets `pool_size=2`

Migrations must not inherit the 60s statement timeout. **Check that the advisory lock and its heartbeat still work under these timeouts.**

**Tests.** Read the settings back from `pg_settings`/`pg_stat_activity`, and run a lock-contention test.

### P2-E: Legacy names and interfaces (sources)

Rename the legacy-only fetcher functions to `legacy_*`, for example `deduplicate` → `legacy_deduplicate` and the lenient parsers, and update the legacy call sites. `cron.py` can't change, so if it imports them directly, keep thin aliases and note them for deletion in Phase 4. Add `Protocol` types for the collector and adapter interfaces, and bring the touched modules under strict mypy.

### G0: Gate 0 cleanup (primary)

- `.github/branch-protection.json` already asks for reviews, but the live `main` protection only has the status check. Before Bryan applies it, fix the deadlock: every PR is authored by `bryan-tieu`, who is the only code owner on most paths, and GitHub doesn't let authors approve their own PRs. Revise the payload and CODEOWNERS to whatever Bryan decides on reviewers. **Bryan applies it.**
- Copy `/private/tmp/perennial-pipeline-rig/production-roadmap.md` into `docs/roadmap/production-roadmap.md`, unchanged apart from a provenance line.
- Open one tracking issue per packet above (labels `phase:1` or `phase:2`, `owner:codex`) and link them from issue #5. The coordinator closes #5 once the retroactive review of PR #6 is in.

## Not in this round

- The live staging run (Phase 1 step 4) and real fixture capture both wait on **D3**.
- Concurrency work (Phase 1 step 5) waits for live measurements.
- All of Phase 3 and later.

## Handoff for each packet

When a packet is ready, write to `primary-status.md` and the packet's issue: the branch and head commit, the files changed, the tests added and their failure on `6915fb0`, the full suite counts (0 skipped, 0 failed, with `REQUIRE_DATABASE_TESTS=1`), `ruff`, `mypy`, the migration round-trip if any, and `git diff --check`. Then tell Bryan the packet is ready for review. The coordinator routes the checker and auditor; don't contact the database-rig seats directly.
