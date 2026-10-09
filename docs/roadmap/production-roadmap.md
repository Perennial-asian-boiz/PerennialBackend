<!-- Provenance: copied 2026-10-09 from /private/tmp/perennial-pipeline-rig/production-roadmap.md; source SHA-256 8ddd703fa1425c8d764135209e43455db2f22a416f1ad33faade493ad957863c -->
# Perennial database: production readiness roadmap

**Owner:** Bryan (product owner and final approver)
**Teams:** Codex pipeline rig (builds) and Claude database rig (reviews and designs), working together.
**Written:** 2026-10-08, starting from PR #4 (`38300d4`, CI failing when merged with `main`).
**Goal:** the Perennial app serves its watchlist from a monitored, backed-up, managed PostgreSQL database that refreshes from live providers every day without anyone watching it.

Today's honest status: the core database code is about 85% done and has passed review. Measured against "running in production and serving the app", the project is about 50% done. This roadmap covers the remaining 50%.

---

## 1. Team and roles

| Who | Role | Owns | Never does |
|---|---|---|---|
| **Bryan** | Product owner, approver, on-call | Decisions in §9, merges to `main`, production deploys, credentials, spending, provider accounts | Nothing is off-limits; Bryan is the final gate |
| **Codex primary** (pipeline rig root) | Tech lead and integrator | Architecture, schema, migrations, pipeline, serving layer, integration of seat work, review packets | Self-approve, merge, deploy, touch production credentials |
| **Codex `sources` seat** | Builder | Provider adapters, collectors, provider contract tests | Edit outside its assigned files |
| **Codex `ops` seat** | Builder (platform) | CI, packaging, container image, infrastructure as code, runbooks, backup and restore tooling | Apply infrastructure changes without approval |
| **Claude coordinator** (database rig parent) | Review lead and planner | Roadmaps, routing reviews, verdicts, release readiness checklists | Edit implementation files |
| **Claude `db-checker`** | Independent reviewer | Correctness, data integrity, migration safety, test quality | Edit reviewed code |
| **Codex `db-security-auditor`** (GPT-6-Astra only) | Independent security reviewer | Secrets, privileges, logging leaks, dependency and supply-chain risk | Sign off on any model other than Astra |
| **Claude `db-implementer`** | Designer (when assigned) | Design docs and architecture decision records (ADRs) for the larger Phase 4 data model work | Write production code unless assigned |
| **haohnguyen94-droid** | Teammate, code owner | `fetchers/insider.py` and their work on `main` | — |

**The two-key rule.** The author never approves their own work. Every change needs one build-side key (Codex) and one review-side key (the database rig). Bryan is the third key for anything irreversible: merges, migrations against shared databases, deploys and spending.

---

## 2. How we work (production processes)

These replace the ad-hoc process from the last two days. Phase 0 sets them up.

### 2.1 Source control and pull requests
- **Short-lived branches off `main`.** One branch per work packet, for example `feat/fmp-pagination`. **Rebase on `main` before asking for review.** PR #4 failed because `main` had moved and nobody rebased.
- **Branch protection on `main`:** the CI check must pass, one approving review is required, no force pushes, and the branch must be up to date before merging.
- **`CODEOWNERS`:**
  - `development/database/**` → database rig reviewers and Bryan
  - `fetchers/**` → `sources` seat and the code owner of the file (for example haohnguyen94 for `insider.py`)
  - `.github/**` and `ops/**` → `ops` seat and Bryan
- **The PR head commit is the review fingerprint.** It replaces the `/private/tmp` fingerprint script. Reviewers record their verdict as a GitHub review on that exact commit, and any new push needs a fresh review.
- **Squash-merge** with a descriptive title. Conventional prefixes (`feat:`, `fix:`, `db:`, `ops:`) make release notes automatic.

### 2.2 Where work and decisions live
- **Move coordination out of `/private/tmp`.** macOS clears it on reboot, and it currently holds the only copy of the review packets, fingerprints and findings.
  - Work items become GitHub issues with labels (`phase:N`, `owner:codex`, `review:needed`).
  - Durable decisions become ADRs in `docs/adr/NNNN-title.md`.
  - Review reports stay in the rigs' own folders, with a link from the PR.
  - OpenRig queue items remain the way seats are woken, but each one points to an issue or PR.
- **Archive now (Phase 0):** copy the `f9c73aaa` and `b3ef39d8` review packets and findings into `docs/reviews/` so the history survives.

### 2.3 Definition of done (applies to every PR)
1. Tests are added or updated, and new behavior has a test that fails without the change.
2. CI is green: lint, type check, unit tests, PostgreSQL tests, migration checks.
3. Docs and runbook are updated if behavior or operations change.
4. Migrations follow §2.4.
5. Reviews: one approval from the review side. Add the security auditor if the PR touches secrets, privileges, logging, auth, dependencies or network boundaries.
6. No new `print()`, no secrets in code or logs, no unpinned dependencies.

### 2.4 Database change management
- **Expand, then contract.** Add new columns and tables in one release, move readers and writers over in the next, and remove the old ones in a third. Never rename or drop in the same release that changes the code using them.
- **Every migration has a tested downgrade**, or a written reason it can't have one, which needs Bryan's approval.
- **Migration review checklist:** lock impact on large tables (`CREATE INDEX CONCURRENTLY`, no table rewrites during peak hours); a `lock_timeout` set in the migration; backfills done in batches, outside the migration transaction; grants updated in `provision_roles.py`.
- **Migrations run as their own deploy step** under the migration owner role. Production must be backed up, and the migration must already have run on staging, before it runs on production.

### 2.5 Release process
`main` → automatic deploy to **staging** → nightly staging pipeline run against real providers → manual promotion to **production** by Bryan, using a tagged release (`vYYYY.MM.DD`). Every release notes migrations, changed behavior and rollback steps.

### 2.6 Incident response
- **SEV1:** production is serving wrong data, or there's a security exposure. Roll back the publication pointer right away (§Phase 3), then investigate.
- **SEV2:** no new publication for over 36 hours. The app keeps serving the last good publication.
- **SEV3:** one source failing, or the pipeline degraded.
- **On-call:** Bryan, with alerts routed per Phase 3.
- **Postmortem within 48 hours for SEV1 and SEV2:** timeline, root cause, action items as issues. The goal is to learn, not to blame.
- **Runbooks:** each alert links to its runbook section in `OPERATIONS.md`.

---

## 3. Phases

Each phase ends at a **gate**: both review keys plus Bryan's sign-off. Phases 1 and 2 can run in parallel once Phase 0 is done, because they touch different files.

### Phase 0: Unblock and set up the foundations
**Lead:** Codex primary, with `ops` · **Review:** checker · **Size:** about 1 day

1. **Fix PR #4 so it passes against `main`.** Rebase onto `origin/main`, which includes haohnguyen94's `a9fed58` insider rewrite.
   - Adapt `collectors.collect_insider` and `_insider_plan` to the new `parse_insider_buys` and coverage contract. Their per-ticker `success / not_found / failed` statuses match our unit tracking, so map them onto `_Tracker` rather than parsing around them.
   - Make the 4 failing tests pass: three in `test_collectors.py` and one in `test_pipeline_contract.py`. **The database rig owns `test_pipeline_contract.py`**: if its fixtures need to change, the rig makes that edit.
   - Ask haohnguyen94 to review the insider integration as code owner.
2. **Make CI match production.** Pin Python and PostgreSQL to the production versions (decision D2; my recommendation is Python 3.12 and PostgreSQL 17 everywhere, plus a `.python-version` file and a 3.12 local `.venv`). CI currently runs PostgreSQL 16 and Python 3.12, while local runs PostgreSQL 17 and Python 3.10.
3. **Add quality gates to CI:**
   - `ruff check` and `ruff format --check`
   - `mypy` in strict mode on `src/db`, `src/ingestion` and `src/pipeline`, relaxed on legacy code
   - `alembic upgrade head → downgrade base → upgrade head` on an empty database
   - an autogenerate drift check to confirm the models match the migrations
   - `pip-audit`
4. **Branch protection, `CODEOWNERS` and a PR template** (with the definition-of-done checklist).
5. **Move coordination to GitHub issues and archive the review packets** (§2.2).
6. **Merge PR #4** after the checker reviews the rebase and Bryan approves.

**Gate 0:** PR #4 merged with CI green, `main` protected, quality gates required.

### Phase 1: Live correctness (does it work with real data?)
**Lead:** `sources` seat · **Review:** checker, plus the auditor for the credential steps · **Size:** about 3–5 days, depending on provider access

1. **FMP pagination.** Replace the fixed `MAX_PAGES = 3` with pagination that runs until a short or empty page, with a configurable safety cap (for example 50 pages) and a page-count diagnostic. If the plan tier limits pages, record that in an ADR and choose either a date-window query or a paid tier (decision D4). Test with 0, 1, 59, 60, 61 and cap-boundary trades.
2. **Provider contract tests with recorded responses.**
   - Capture one real, scrubbed response per provider endpoint into `tests/fixtures/providers/`.
   - Add a weekly CI job (not on every PR) that calls each provider's real API with a staging key and compares the response shape against those recorded files.
   - When the shape changes, open an issue automatically. The pipeline doesn't break silently; we find out within a week.
3. **Data quality checks before publishing.** Today a provider that returns 3 rows instead of 300 publishes without complaint. Add per-source checks in `publication.py`:
   - row count within limits relative to the previous successful batch (for example, block on a drop of more than 50% or a rise of more than 300%, configurable per source)
   - no single-day rise in the share of null required fields
   - the `source_as_of` date moves forward
   A check that fails blocks publication with a clear error code, and an operator can override it with a recorded reason through `cli.py publish --override <reason>`.
4. **First live run on staging** (needs D3, staging keys): run the full pipeline against real providers into a staging database. Record row counts, run time per source, the share of requests that failed or were retried, and the size of the stored data. These numbers set the SLOs and capacity plan.
5. **Measure before optimizing.** If insider or short-interest collection takes longer than its window (it does up to 2,000 sequential requests), add bounded concurrency with a per-provider rate limiter. Don't do this until step 4 shows it's needed.

**Gate 1:** three consecutive successful staging runs against live providers, the data quality checks on, and the numbers recorded in `OPERATIONS.md`.

### Phase 2: Raise code quality to production standard
**Lead:** Codex primary and `sources` · **Review:** checker, plus the auditor for config and logging · **Size:** about 3–4 days · Can run in parallel with Phase 1.

The remediation list in §4 has the details. In summary: structured logging, one typed settings module, packaging with a lock file, database session settings, and a plan for retiring the legacy JSON path.

**Gate 2:** zero `print()` in `src/` outside the legacy code being retired; settings validated at startup; lock file in place; mypy strict passes on the core packages.

### Phase 3: Infrastructure, deployment and operations
**Lead:** `ops` seat · **Review:** security auditor (required) and checker · **Approval:** Bryan for every apply · **Size:** about 1 week

1. **Hosting** (decision D1). My recommendation is a managed PostgreSQL 17 service with point-in-time recovery, such as Neon, Supabase, AWS RDS or Crunchy Bridge. Pick one that supports **direct or session-mode connections**, because our advisory locks break under transaction pooling.
2. **Environments:** `dev` (local Docker), `staging` and `prod`. Each has its own database, credentials and provider keys. Production data is never copied to dev.
3. **Infrastructure as code:** Terraform or the provider's IaC for the database, roles, the scheduled job and alerts. Plan output goes into the PR, and apply needs Bryan's approval.
4. **Container image:** a multi-stage Dockerfile, non-root user, pinned base image digest, lock-file install, image scanning in CI, and an SBOM.
5. **Run the pipeline as a scheduled one-shot job.** Replace the long-running APScheduler daemon with the platform's scheduler (Cloud Run Jobs, a Fly or Render cron, a Kubernetes CronJob, or similar) running `python -m src.ingestion.cli run-pipeline`. Exit code 0 or 1 is the success signal. The advisory lock already stops overlapping runs, and the platform handles retries and history. This removes a daemon that has to be supervised and an exception-handling boundary that already caused SEC-P1.
6. **Secrets** come from the platform's secret manager and are passed in as environment variables. No `.env` in deployed images. Provider keys and database credentials are rotated every 90 days and right away when someone's access is removed.
7. **Roles:** apply `provision_roles.py` in staging, then in production. Each workload gets a login role: the migrator owns the schema, the pipeline job has importer rights, and the app has read-only rights. Record the result of the privilege audit.
8. **Backups and recovery:**
   - Turn on point-in-time recovery with at least 7 days of retention, plus a separate daily logical backup to another location.
   - **Run a restore drill on staging every quarter**, timing it to measure real RPO and RTO. Proposed targets: RPO ≤ 15 minutes, RTO ≤ 1 hour (decision D6).
9. **Monitoring and alerts:**
   - A **missed-run monitor**: the job pings it on success, and it alerts if no ping arrives by 11:00 PT.
   - `cli.py health` run on a schedule, alerting on any issue.
   - Database metrics: connections, disk use, replication lag, slow queries. Use the provider's dashboards plus `pg_stat_statements`.
   - Alerts go to Bryan by phone or email, each linking to its runbook.
10. **Publication rollback command:** `cli.py rollback --to <publication_id>` moves `current_publication` back to an earlier publication, inside a transaction with an audit record. This is the main fix for a SEV1 incident.

**Gate 3 (production readiness review):** the coordinator runs the checklist in §5 on staging. Any item left unchecked blocks production.

### Phase 4: Serve the app from the database
**Lead:** Codex primary · **Review:** checker and auditor · **Size:** about 1 week, depending on the app's architecture

1. **Read API.** Today nothing reads `current_publication`. Add a read-only endpoint, for example `GET /v1/watchlist`, that serves the current publication:
   - an `ETag` equal to the publication ID, and cache headers
   - the read-only database role
   - the publication's `ranking_version` and `generated_at` in the response
   - a versioned response schema documented in `docs/technical-specs/`
2. **Shadow mode for at least 2 weeks.** Keep the legacy JSON pipeline and the database pipeline both running. Each day an automated job compares their watchlists, and every difference has to be explained (for example, intentional v2 dedup). Differences are logged as issues.
3. **Cutover.** Switch the app to the API behind a config flag, so turning the flag off reverts. Watch for one week.
4. **Retire the legacy path** after cutover:
   - delete the JSON writers, `scheduler/cron.py` and the legacy `deduplicate` and lenient parser functions
   - fold the fetchers into one provider-adapter layer
   This also removes the legacy raw-exception logging hazard that SEC-P1 was scoped to leave alone.

**Gate 4:** the app is served from production, shadow differences are resolved, and the legacy code is deleted.

### Phase 5: Resilience and data model maturity (after launch)
**Lead:** `db-implementer` writes the design, then Codex primary builds it · **Review:** both reviewers; the ADR needs Bryan's approval before any migration · **Size:** 2–3 weeks, ordered by what live data shows

1. **Degraded publication.** Allow publishing when one non-critical source fails, by reusing its last good batch within a freshness limit. Mark the output `degraded` with the reused source listed. Decision D7 sets which sources may be reused and for how long.
2. **Market caps as a tracked source,** with its own runs and batches, so failures have a durable history.
3. **Trade events and security identifiers** (from `design-3-data-model.md`): reconcile events across providers, and track ticker renames and CUSIP/FIGI identifiers over time. This replaces the ranking-time dedup with stored event identities.
4. **Retention:**
   - A scheduled job deletes `source_batches.payload` older than N days (decision D5) when no publication references it.
   - If storage grows past budget, move raw payloads to object storage and keep only a content-hash pointer.
5. **Capacity and load testing:** simulate a year of daily batches, then measure the time to publish, to replay and to back up.

---

## 4. Not yet production grade: findings and recommended fixes

From an audit of `38300d4`. The phase that fixes each one is in brackets.

| # | Where | Current state | Production-grade alternative |
|---|---|---|---|
| 1 | `fetchers/*.py`, `cron.py`, `cli.py`, `search_engine.py`: **144 `print()` calls** | Unstructured output with no level and no run context; lost in a scheduled job | `logging` with a JSON formatter configured once at startup (`src/utils/logger.py`). Every record carries `run_id`, `source` and `publication_id` through a `LoggerAdapter` or `contextvars`. Ruff rule `T20` bans `print` in `src/`. [P2] |
| 2 | `fmp.py:32-34`, `insider.py:54`, `consensus.py:49`: **`load_dotenv` when the module is imported**; `FMP_API_KEY` defaults to `"YOUR_FMP_KEY_HERE"` | Importing a module has side effects; a missing key surfaces deep in a run as an HTTP 401 | One `src/settings.py` built on `pydantic-settings`: typed fields and `SecretStr` for keys, validated **once at process start** so a missing setting stops the process immediately. Pass settings into adapters explicitly; no module-level state. `.env` only in local dev. [P2] |
| 3 | `requirements.txt`: runtime and test dependencies mixed, `apscheduler>=3.10,<4` a version range, no hashes | Builds can't be reproduced; test tools ship in production; open to supply-chain risk | `pyproject.toml` with runtime and `dev` dependency groups, plus a lock file with hashes (`uv lock` or `pip-tools --generate-hashes`). Dependabot opens weekly update PRs, and CI runs `pip-audit`. [P0/P2] |
| 4 | `.github/workflows/tests.yml`: PostgreSQL 16 and Python 3.12, while local runs 17 and 3.10 | Bugs that depend on the version slip through | CI pinned to production versions, with lint, type check, migration round-trip and drift check (Phase 0 item 3). [P0] |
| 5 | `src/pipeline/scheduler.py`: a long-running APScheduler `BlockingScheduler` | Needs a supervisor; the process can hang; history lives only in logs | A one-shot CLI job triggered by the platform scheduler (Phase 3 item 5). Keep APScheduler only if the hosting platform has no scheduler. [P3] |
| 6 | `src/db/session.py`: no `application_name` or timeouts; default pool | A stuck query or lock can block the pipeline indefinitely; connections can't be told apart in `pg_stat_activity` | Set `application_name=perennial-<workload>` and server options `statement_timeout` (60s for the pipeline, 5s for the API), `lock_timeout=5s` and `idle_in_transaction_session_timeout=60s`. Size the pool for each workload (job: `pool_size=2`; API sized to its worker count). Keep `pool_pre_ping`. [P2] |
| 7 | `publication.py`: no checks on data volume or shape | A partial or empty provider response that passes validation gets published | Data quality checks before publishing, with an audited override (Phase 1 item 3). [P1] |
| 8 | Legacy JSON path and database path share fetcher modules (`deduplicate` vs `deduplicate_proven_repeats`, lenient vs strict parsers) | Two behaviors in one file; a fix to one path is easily applied to the wrong function | Short term: name the legacy functions explicitly (`legacy_*`). After cutover, delete the legacy path and keep one adapter per provider behind a typed interface (Phase 4 item 4). [P2→P4] |
| 9 | `models.py:88`: `source_batches.payload` JSONB kept forever | Storage and backups grow without limit | Retention job, then object storage if needed (Phase 5 item 4). [P5] |
| 10 | `health.py`: only reachable from the CLI | Nobody is told when something breaks | Scheduled health check and missed-run monitor wired to alerts (Phase 3 item 9). [P3] |
| 11 | Collectors: sequential synchronous HTTP, up to 2,000 requests | Run time may exceed the window at real volumes | Measure in Phase 1. If needed, add bounded concurrency with a per-provider token-bucket limiter. [P1, only if measured] |
| 12 | Type hints partial; no type checker | Interface changes break at runtime, as PR #4 did with `a9fed58` | mypy strict on the core packages in CI. `Protocol` types for collector and adapter interfaces, so a contract change fails the type check, not production. [P0/P2] |
| 13 | Review fingerprints and packets kept in `/private/tmp` | Lost on reboot; nobody can audit them later | Move to GitHub PR reviews on the head commit, issues and `docs/reviews/` (§2.2). [P0] |
| 14 | `cron.py` (legacy) logs raw exceptions | Can leak secrets in error text | Retired with the legacy path. Until then, don't run it with production credentials. [P4] |
| 15 | No API or read path | The database has no consumer | Read API with a read-only role, then shadow mode and cutover (Phase 4). [P4] |

**Already production grade, keep as is:**
- immutable content-hashed snapshots and full lineage
- atomic publication with a pointer, and deterministic replay
- versioned ranking
- restricted roles that block changes to stored history
- advisory locking with heartbeats
- error diagnostics restricted to a fixed vocabulary
- isolated, disposable test databases
- tests that prove each fix fails on the old code

---

## 5. Production readiness checklist (Gate 3)

The coordinator checks these on staging. The checker and the auditor each sign their sections.

**Data correctness:** three or more consecutive successful live staging runs · data quality checks on · replay of a staging publication is byte-identical · shadow comparison set up.
**Reliability:** missed-run and health alerts fire in a test · publication rollback tested · a failed source leaves the previous publication served.
**Security** (auditor): secrets only in the secret manager · least-privilege login roles applied and audited · no secrets in logs (sentinel test on the deployed job) · image scan clean of high/critical findings · `pip-audit` clean · TLS required on database connections (`sslmode=verify-full`).
**Recoverability:** point-in-time recovery on · restore drill completed with RPO and RTO measured · migration downgrade tested on a staging copy.
**Operability:** every alert links to its runbook · on-call contact set · deploy and rollback steps written and rehearsed once.
**Compliance:** provider terms of service reviewed for storing and redistributing data (decision D8).

---

## 6. Service level objectives (proposed; Bryan approves in D6)

| SLO | Target |
|---|---|
| Freshness | A new publication by 10:30 PT on 95% of trading days |
| Availability | The watchlist API returns the current publication 99.5% of the time each month |
| Correctness | Zero publications rolled back for wrong data per quarter |
| Recovery | RPO ≤ 15 min, RTO ≤ 1 h, measured in each quarterly drill |

---

## 7. Cross-team operating loop (each work packet)

1. **Plan:** the coordinator writes a work packet as a GitHub issue: scope, files, owner, tests, definition of done. Bryan approves the plan for each phase.
2. **Build:** Codex primary assigns seats through OpenRig queue items that link the issue. Seats work on a branch and report evidence in the issue.
3. **Integrate:** Codex primary rebases on `main`, runs the full CI locally, and opens the PR with the evidence and the list of findings addressed.
4. **Review:** the coordinator routes the checker, plus the auditor if needed, to the PR head commit. Reviewers post GitHub reviews, and blocking findings become `changes requested`.
5. **Fix and re-review** only the findings, on the new head commit.
6. **Approve and merge:** Bryan merges. `main` deploys to staging automatically.
7. **Promote:** Bryan promotes the release to production using the release checklist.

Daily: one sync note in the tracking issue from each side (done, next, blocked). Every week Bryan gets a status summary with the phase, gate status and risks.

---

## 8. Sequencing summary

```
Phase 0  ──►  Phase 1 (live correctness)  ──┐
   │                                         ├──► Phase 3 (infra/ops) ──► Gate 3 ──► Phase 4 (serve + cutover) ──► Phase 5
   └────►  Phase 2 (code quality)  ─────────┘
```
Rough total to the app serving from production: **3–4 weeks**, mostly limited by decisions D1, D3 and D4 and by the 2-week shadow period.

---

## 9. Decisions Bryan needs to make

| ID | Decision | Needed by | Recommendation |
|---|---|---|---|
| D1 | Hosting provider and monthly budget | Phase 3 | Managed PostgreSQL 17 with point-in-time recovery and session-mode connections |
| D2 | Standard Python and PostgreSQL versions | Phase 0 | Python 3.12, PostgreSQL 17 |
| D3 | Staging provider keys (FMP, Nasdaq, ARK, SecuritiesDB) | Phase 1 | Separate keys for staging and production |
| D4 | FMP plan tier and pagination approach | Phase 1 | Decide from the first live run's numbers |
| D5 | How long to keep raw data | Phase 5 | 180 days for payloads not used by any publication |
| D6 | SLO targets and on-call routing | Phase 3 | The §6 targets; alerts to phone |
| D7 | Which sources can be reused when degraded, and for how long | Phase 5 | Short interest up to 15 days (it is reported twice a month); the others not reused |
| D8 | Terms-of-service review for storing and redistributing provider data | Gate 3 | Required before launch |
| D9 | Coordinating with haohnguyen94 on `insider.py` | Phase 0 | Make them code owner for fetchers they wrote |
