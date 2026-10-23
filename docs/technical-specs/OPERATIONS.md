# Production pipeline operations

This runbook describes deployment requirements and the repository tooling. It
does not record a completed cloud deployment or a successful recovery drill.
Bryan approved D6 targets and D14 free-tier operation on 2026-10-21; the M3
preparation below describes work to configure and prove, not deployed controls.
Each environment needs an accountable deployment owner.

## Environment isolation and credentials

Use separate managed PostgreSQL databases, role memberships, provider accounts,
and secrets for production, staging, and disposable tests. Never load production
provider keys or a production `.env` into CI. Inject application `DATABASE_URL`
and provider credentials through the deployment secret store; restrict secret
access to the worker identity. Give the query service a reader login. Rotation,
revocation, access audit, certificate trust and secret-store integration belong
to the deployment. Do not put passwords in command arguments or logs.

Tests use only `TEST_DATABASE_URL`, pin connections to loopback, remove libpq
`PG*` routing defaults, disable dotenv loading, and create/drop their own random
`perennial_test_*` database. The test administrator needs `CREATEDB` on an
isolated synthetic PostgreSQL server. The production worker must not have it.
CI sets `REQUIRE_DATABASE_TESTS=1`; a missing test URL fails the job instead of
silently skipping database coverage.

From the repository root, run the full synthetic suite with the environment's
test URL already injected:

```bash
REQUIRE_DATABASE_TESTS=1 .venv/bin/python -m pytest -q development/backend/tests
```

## Database ownership and grants

`development/database/ops/provision_roles.py` uses only `OPS_DATABASE_URL` from
the process environment, never `.env`. It runs each phase in one transaction and
quotes role/table identifiers through psycopg. Use a dedicated database and
dedicated group names; the script changes PUBLIC privileges in that database.
It creates three NOLOGIN groups by default:

| Group | Access |
| --- | --- |
| `perennial_owner` | Owns application schema/tables, their identity sequences, and Alembic version table; migration-only membership |
| `perennial_importer` | SELECT/INSERT on explicit application tables; UPDATE only ingestion runs and current publication; column UPDATE only securities exchange/currency; USAGE/SELECT on existing identity sequences |
| `perennial_reader` | SELECT on explicit application tables; no writes or sequence access |

Source batches, source rows, run dependencies, publications, publication sources,
market-cap observations and publication market-cap links receive no importer
UPDATE, DELETE or TRUNCATE. New owner-created tables/sequences receive no
automatic application grants. After adding a table, update the explicit policy
and reapply grants after migrating. Existing table and column grants are cleared
before regranting. Unsafe existing groups (login, administrative attributes, or
parent role membership) are rejected. Deployments must also audit login group
membership and privileges from other roles: this tool does not rewrite external
login accounts or repair arbitrary legacy authorization.

Run bootstrap using the administrative secret, migrate using the owner identity,
then reapply grants using the administrative secret:

```bash
.venv/bin/python development/database/ops/provision_roles.py --phase bootstrap
# Use a migration login that SET ROLEs to perennial_owner for the whole migration.
.venv/bin/alembic -c development/database/alembic.ini upgrade head
.venv/bin/python development/database/ops/provision_roles.py --phase grants
```

Bootstrap reserves `perennial` for the owner and grants owner CREATE in `public`
for Alembic's version table. Grant phase transfers the explicitly listed tables
and their attached identity sequences to the owner, and fails if the migrated
schema does not match the policy. Custom groups are available through `--owner`,
`--importer`, and `--reader`. An administrator separately creates authenticated
logins and grants exactly the corresponding group. Run migrations as owner
(e.g. by the migration login's session role configuration), then run workers
with importer membership and query consumers with reader membership.

Verify effective privileges with those login identities, including denied
historical-row UPDATE/DELETE/TRUNCATE and a successful complete pipeline and
publication replay. Repository ops tests exercise these paths via SET ROLE on
an isolated server. The provisioning tool deliberately creates no passwords.

## Managed PostgreSQL deployment checklist

- Choose supported PostgreSQL and matching backup client versions; test the
  migration path at the deployed commit (use `upgrade head`; do not pin an old revision).
- Configure private network access, TLS with certificate verification, encryption
  at rest, maintenance/failover policy, storage and connection capacity.
- Use a direct PostgreSQL connection or a session-pooling endpoint for workers.
  The pipeline uses session advisory locks; transaction pooling is unsupported,
  including when prepared statements are disabled.
- Provision the owner/importer/reader groups and independent login identities;
  restrict migration credentials to release operations.
- Set the application's `DATABASE_URL` to the importer identity and inject the
  required provider secrets. Run migrations and reapply the explicit grants.
- Under D14, configure scheduled logical backups and off-site retention as
  described below. PITR is deferred until launch; do not enable a paid add-on.
  Record backup storage separation, access controls and restore steps.
- Configure supervisor, monitoring, alert delivery, on-call ownership and the
  recovery objectives below; exercise them before launch.

## Worker and observability

Run the production worker from `development/backend`:

```bash
../../.venv/bin/python -m src.pipeline.scheduler
```

The worker performs a recovery/catch-up run on supervisor start, then schedules
daily at 09:00 America/Los_Angeles. `--test` executes one **live** complete run;
it is not an offline verification option. Use pytest with synthetic collectors
for offline checks. `src/services/consensus_watchlist/scheduler/cron.py` remains
the legacy JSON workflow and is not the database production scheduler.

Supervise the process and send logs to the deployment's log sink. Monitor worker
availability, publication age, source freshness, repeated failed/abandoned runs,
coverage/lineage failures, database availability/storage, backup snapshot age, and backup completion failures. Configure paging destinations, severity and thresholds in the deployment;
the repository does not install alert integrations. A failed candidate keeps the
previous current publication. Inspect persisted run status/error codes and the
current publication before deciding whether to retry or escalate. Avoid bypassing
coverage, freshness or lineage rules to make a publication appear current.

## M3 database observability plan (Supabase PostgreSQL 17)

**Not configured by this document.** Bryan owns deployment and live validation.
Use the existing free-tier project and approved notification destination; this
plan creates no resources or paid monitoring service. Record the actual database
quota, disk capacity, usable direct-connection limit, pooler client limit, and
reserved service headroom from the project dashboard before setting alerts.
Do not treat `max_connections` as the number available to the worker.

Verify `pg_stat_statements` is enabled in Database → Extensions. If absent,
Bryan enables it through the dashboard, then verifies statistics accumulate.
Collect counters with a dedicated monitoring identity whose read permissions
are reviewed; never give the worker elevated privileges for monitoring.
Use query IDs, call counts and execution durations in reports, not SQL text,
parameters, URLs or payloads. Query text can contain literals even when many
statements are normalized. Do not enable blanket statement/parameter logging.
See [Supabase pg_stat_statements documentation](https://supabase.com/docs/guides/database/extensions/pg_stat_statements).

The following thresholds are initial operational proposals, not measurements.
Evaluate once a minute on the existing approved runner; persist only safe numeric
samples. Compare counter deltas over five minutes; if the server restarts or
statistics reset, discard that comparison and begin a fresh window. Zero calls
means no latency sample, not zero latency. Missing samples for five minutes
raise a monitoring-gap notification rather than a healthy result.

| Signal | Proposed alert | Response and proof |
| --- | --- | --- |
| Slow queries | SEV3: five-minute mean execution time >1 s with ≥5 calls for one query ID, in two consecutive windows; SEV2: timeout failures preventing publication | Identify query ID and blocking state; review its plan with synthetic inputs. Do not run EXPLAIN ANALYZE on an unreviewed write. Trigger the rule with synthetic slow-query samples. |
| Direct connections | SEV3: >70% of usable limit for 5 min; SEV2: >85% for 5 min or connection refusal | Check worker/pool counts, transactions and blocked sessions. Drain duplicate workers, reduce pool concurrency, and preserve advisory-lock ownership; do not mass-terminate sessions. Inject numeric samples for threshold tests. |
| Pooler clients | Same percentages, using the separate dashboard pooler limit | Check pooler client metrics separately: pg_stat_activity counts server sessions, not all pooled clients. Verify both alert paths. |
| Database and disk | SEV3: either >70% of its quota or projected exhaustion <7 days; SEV2: >85% or projected exhaustion <48 h | Track database size separately from total disk/WAL. Inspect growth by table; never purge pinned batches to quiet an alert. Estimate linear growth from daily samples; re-evaluate after large imports. Test with synthetic samples. |
| Backups | SEV3: newest verified off-site snapshot >10 min old; SEV2: >15 min or any failed backup job | Check schedule, dump completion, checksum and upload; preserve last good archive. Snapshot time, not upload time, governs age. Exercise failed-upload and missing-schedule paths. |
| Publication | SEV2: no successful publication for today's trading session at 10:30 America/Los_Angeles | Check fixed run error codes and source freshness; retain prior publication while repairing. Test calendar/DST and missed-run cases synthetically. |

Supabase's [reports](https://supabase.com/docs/guides/observability/reports) and
[performance guide](https://supabase.com/docs/guides/platform/performance) describe
connection and disk inspection. Dashboard visibility alone is not alert delivery.
Implement the small numeric checks on an approved existing runner and send them
to Bryan's selected destination; if no runner or destination is available, record
that as an M3 deployment prerequisite. No new paid service is assumed. Alert
messages contain environment, fixed rule name, numeric value/limit, UTC time and
this runbook link. Deduplicate until recovery; a recovery notification closes the
incident. Never include connection strings, SQL, request headers or secret values.

For acceptance, Bryan records each synthetic threshold crossing, received
notification, recovery notification and delivery delay, then confirms one live
numeric sample per metric. Agents prepare/test synthetic fixtures only. Keep the
30-trading-day on-time-publication count to assess the agreed 95% target; a daily
alarm is not itself proof of the SLO.

## Recovery targets and scheduled logical backups (D6, D14)

**Agreed targets:** publish by 10:30 PT on 95% of trading days; RPO ≤15 minutes;
RTO ≤1 hour. These remain targets until measured. **Free tiers for now: no PITR
until launch.** M3 uses scheduled logical backups, one timed pre-launch restore,
then a yearly drill. Supabase recommends off-site exports for free projects;
see [Database Backups](https://supabase.com/docs/guides/platform/backups).

Proposed schedule: start a consistent logical dump every five minutes, with only
one backup running at a time. Include upload/checksum verification in the job.
A skipped overlapping job or delayed scheduler must alert; do not call it success.
The recovery point is the dump's snapshot time. With five-minute spacing, dump
plus off-site verification must finish within ten minutes to keep the worst-case
recoverable snapshot age within 15 minutes. Monitor the actual age continuously.
A daily dump, or a dump whose upload has not finished, cannot establish D6.
If free-tier runtime/storage/egress limits cannot sustain this, report the measured
gap to Bryan; do not silently relax RPO or enable a paid service.

Use PostgreSQL 17 backup clients and a direct or supported session connection,
never transaction pooling. Bryan injects connection settings through an approved
secret mechanism (for libpq, a protected service/passfile), not command arguments
or shell history. Disable shell tracing. Use TLS verification. See
[Supabase connection guidance](https://supabase.com/docs/guides/database/connecting-to-postgres).

The backup manifest must identify schema revision, snapshot time, archive checksum,
completion time and storage receipt, without credentials or data rows. Store
archives encrypted outside the source project's failure boundary, with restricted
access and a tested decryption recovery path. Proposed initial retention is all
five-minute snapshots for 24 hours and one daily for seven days; Bryan must confirm
that storage fits D14 before configuring it. Never delete the last verified archive
because a newer attempt started. Do not place real dumps in Git, CI artifacts or
agent-readable scratch directories.

Back up the application schema/data and separately retain reviewed role/grant
configuration without password hashes. As app/auth tables land, expand and test
coverage explicitly: a pipeline-only dump is not a complete account recovery.
Supabase-managed schemas, Auth configuration and any Storage objects need their
own documented restore coverage; database dumps do not include Storage file bytes.
Follow the [Supabase backup/restore procedure](https://supabase.com/docs/guides/platform/migrating-within-supabase/backup-restore)
for managed-schema restrictions rather than overwriting service-owned roles.

### Timed logical restore drill (Bryan-operated)

1. Record incident-start time, newest verified archive's snapshot time, archive
   checksum, expected schema revision and safe reference counts. Block provider
   egress and disable scheduled workers on the isolated restore target. Preserve
   the source database and existing backups. Choose an already approved isolated
   target; if none exists, the drill is blocked, not permission to create one.
2. Retrieve/decrypt the archive and restore schema/data to the isolated target.
   Recreate reviewed login/group memberships and grants separately with fresh
   credentials. Do not use a production endpoint or destructive `--clean` option.
3. Verify migration revision, constraints, table counts, batch/run lineage,
   publication pointer and byte-identical publication replay. Validate denied
   importer updates to immutable rows and reader-only access; after M5 include
   cross-user RLS checks. Exercise the worker with synthetic inputs, no providers.
4. Measure RPO as incident time minus the restored snapshot time, and RTO from
   incident declaration through restore and usable-reader verification (including
   archive retrieval, configuration and checks). Record safe counts, fixed status
   codes, times and deviations. A successful import alone does not establish RTO.
5. Keep production unchanged. A real cutover needs Bryan's explicit authorization,
   writer drain, endpoint change, reader verification and rollback plan. Retire
   only resources explicitly created for the drill, under their owner's control.

PITR and its measured recovery window become a separate launch decision. Logical
archives cannot recover arbitrary commits between snapshots. The local synthetic
drill below remains useful but does not prove the hosted restore time or D6.

## Secret rotation runbook (Bryan-operated; agents use synthetic fixtures)

D3 requires only **FMP** and **Finnhub** keys. Nasdaq short interest and ARK are
keyless in the current collectors: no secret to create, store or rotate. Revisit
this inventory if their contracts change. Database passwords belong to independent
login identities; the NOLOGIN owner/importer/reader groups have no passwords.

Before rotating, inventory consumers, current secret-version identifiers, and
rollback versions without reading values into reports. Rehearse with synthetic
credentials in staging; Bryan alone performs live steps. Keep the current
publication available throughout. Schedule changes between collections, drain the
worker, and restart it with the new environment because Settings is read at
process startup. Readers remain running; do not disturb database advisory locks
held by an active collection. Record only timestamps, version IDs, status codes
and counts. No secret screenshots, verbose HTTP traces, parameter dumps or URLs.

| Secret | Replacement and verification | Revoke/rollback |
| --- | --- | --- |
| FMP_API_KEY | Bryan obtains a replacement in the provider account, checking whether the old key remains valid. Store a new version, restart the drained worker, and run the bounded Congress probe when its separately owned script has landed (command below). Also verify the market-cap consumer with a bounded owner-operated check. | Revoke old key only after both consumers succeed if overlap is supported. Before revocation, rollback uses the old secret version plus restart. If regeneration invalidates immediately, pause collection first; rollback requires provider recovery/new key, not the revoked value. |
| FINNHUB_API_KEY | Same staged replacement. Verify one known synthetic/test-universe symbol through the existing collector, honoring rate limits and X-Finnhub-Token transport. Bryan prepares a bounded safe-output invocation before performing this live check; no ready probe script is claimed here. | Revoke old key after verification where overlap exists. With immediate invalidation, keep reads on the last publication while repairing; no guaranteed uninterrupted ingestion. |
| Database importer login | Create a successor login through the approved admin path, with exactly importer membership, session settings and limits; no ownership or administrative rights. Store its new connection secret, verify positive inserts and forbidden updates on staging, then drain and restart the worker. | Keep old login valid until new connections and a safe pipeline check succeed. Roll back the secret version before disabling old login. Then NOLOGIN the old identity and inspect its remaining sessions; drain only known application sessions. |
| Database reader/API login | Equivalent successor login with only the required reader or API/RLS permissions. Roll readers gradually, health-check fresh connections and user isolation when applicable. | Keep old login during rolling replacement, then disable new authentication and drain its identified connections. Never terminate unrelated Supabase service sessions. |
| Migration/backup login | Rotate separately from worker/readers. Preserve only the documented group memberships and backup permissions; verify a staging migration or backup/restore before retiring the old identity. | Retain an authorized recovery login throughout. Do not grant superuser/BYPASSRLS merely to pass a check. |

PostgreSQL stores a single password per role, so seamless database rotation uses
successor login identities rather than pretending one role accepts two passwords.
Use the protected interactive password mechanism, never a literal password in SQL
history. See [PostgreSQL ALTER ROLE](https://www.postgresql.org/docs/17/sql-alterrole.html).
Supabase's managed project/admin password is a separate dashboard-controlled
credential: inventory all consumers, use the supported reset flow, then replace
their secrets and reconnect. If overlapping identities are unavailable, Bryan
must plan that maintenance interval; this document cannot promise zero downtime.

For Bryan, once the D4-B probe script is reviewed and landed, with FMP_API_KEY
already injected, the bounded Congress check is one command from the repo root:

```bash
.venv/bin/python development/database/ops/probe_fmp_congress.py
```

Its contract is status codes, counts and date ranges only, nonzero on 402. This
command is pending that packet, not a tool installed by M3 docs. Never run the
scheduler's `--test` as a small rotation probe: it executes the full live pipeline.
Before a Finnhub or market-cap live check, the coordinator must supply an equally
bounded command with safe output. Agents do not run any of these live checks.

Rotation acceptance requires fresh connections/requests using the replacement,
confirmation that the old credential is revoked, continued reader availability,
and a tested rollback before revocation. Provider overlap support is unverified;
if unsupported, preserve read availability but record the ingestion interruption.
No claim of rotation readiness is made until Bryan's rehearsal succeeds.

## Local synthetic dump/restore drill

With only synthetic data on a loopback test server, matching `pg_dump` and
`pg_restore` in PATH, and `TEST_DATABASE_URL` injected:

```bash
.venv/bin/python development/database/ops/restore_drill.py \
  --synthetic-only --output /private/tmp/perennial-synthetic-drill-unique
```

The output directory must be new. The tool rejects non-loopback URLs, database
names without `test`, and routing query parameters; it pins the effective host
address and clears ambient libpq defaults. `--synthetic-only` explicitly attests
the source has no real data; URL naming alone cannot establish that fact.

It exports a repeatable-read snapshot, makes a custom-format dump from that same
snapshot, creates a random `perennial_test_restore_*` database, and restores there
in one transaction. It compares SHA-256 content hashes and row counts for every
application table and checks that constraints are validated. Only the newly
created target is dropped, including after a restore failure; the source and
existing databases are preserved. The dump and `report.json` remain in the private
output directory for review. A passing report contains `verified: true` and
`target_dropped: true`. If cleanup fails, identify only the reported generated
target before taking action. Do not retry with a broader target or `--clean`.

This drill proves snapshot dump/restore and retained row content for a disposable
database. It excludes ownership/ACL restoration and does not exercise continuous
WAL, recovery to a timestamp, service failover, cloud permissions, production data
loss, or production RTO. The M3 logical exercise above must separately establish hosted recovery
evidence; neither exercise establishes PITR. Synthetic archives may be deleted after their evidence is recorded;
production backup retention is separately owned by the deployment.

## Phase 0 quality gates

The baseline is Python **3.12** (`.python-version` pins 3.12.13) and PostgreSQL
**17** for local development and CI. The production version decision (roadmap
D2) remains part of Bryan's Gate 0 review; no deployed runtime has been changed.
Use a 3.12 interpreter to create `.venv`, then install `requirements-dev.txt`.
Runtime and tool top-level requirements are exact-pinned; a complete transitive
lock with hashes remains Phase 2 work.

From the repository root:

```sh
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m ruff check
.venv/bin/python -m ruff format --check
.venv/bin/python -m mypy
REQUIRE_DATABASE_TESTS=1 .venv/bin/python -m pytest development/backend/tests -q
.venv/bin/python development/database/ops/quality_gate.py
.venv/bin/python -m pip_audit -r requirements-dev.txt --progress-spinner off
```

Export `TEST_DATABASE_URL` pointing to a disposable PostgreSQL 17 server on
loopback with `test` in its database name before the last three checks. Never
use `DATABASE_URL` or provider credentials. Tests and the migration gate create
and drop their own randomly named databases; the URL's named database is not
migrated or cleared by the gate. The test role needs database-creation rights on
this isolated server. Put the PostgreSQL 17 `pg_dump` and `pg_restore` clients
on PATH for restore-drill tests. The workflow installs them from the
[official PostgreSQL Ubuntu repository](https://www.postgresql.org/download/linux/ubuntu/).

Ruff and strict mypy cover the complete `src/db`, `src/ingestion`, and
`src/pipeline` trees. Ruff also covers the migration-gate script. Legacy services
are outside this initial static-check scope; existing tests still exercise
legacy compatibility. The database rig retains `test_pipeline_contract.py`.
No new blanket mypy suppressions are applied to the core packages. APScheduler
has a narrow missing-stubs exemption; legacy service imports are skipped.

On macOS, uv's standalone Python may fail when pip-audit creates a **copied**
temporary venv (`libpython3.12.dylib` cannot be found). In an environment freshly
installed from `requirements-dev.txt`, `python -m pip check` followed by
`python -m pip_audit --progress-spinner off` audits the installed dependency set
without that temporary venv. This does not suppress vulnerability findings.
Linux CI keeps the requirements-based audit.

### Review and protection activation

The workflow's required check name is exactly **Backend quality**. The proposed
GitHub protection payload is `.github/branch-protection.json`: up-to-date branch,
one approval, stale-review dismissal, code-owner approval, last-push approval,
resolved conversations, no force pushes/deletions, and enforcement for admins.
It is staged for Gate 0 review, not automatically applied by CI. Once the
follow-up branch is authorized for publication and its workflow is green, Bryan
can apply the reviewed payload:

```sh
gh api --method PUT repos/Perennial-asian-boiz/PerennialBackend/branches/main/protection \
  --input .github/branch-protection.json
```

Verify the live response and repository rulesets after applying. GitHub usernames
in CODEOWNERS are human accounts, not rig seats. The database coordinator routes
checker review (plus the Astra security auditor for dependency/network changes)
and records the verdict against the actual PR head commit. Request
`haohnguyen94-droid` on the insider integration follow-up, even when the changed
file is the collector rather than `insider.py`. The author must not self-approve.
Bryan owns final merge and production decisions. Until publication and review,
local test results do not mean GitHub CI or Gate 0 has passed.

Historical review packets and findings are archived under `docs/reviews/`, with
per-file hashes and original authorship. Phase 0 work is tracked in GitHub and
linked from its Gate 0 handoff; `/private/tmp` remains only a local seat handoff
and scratch-evidence location.
