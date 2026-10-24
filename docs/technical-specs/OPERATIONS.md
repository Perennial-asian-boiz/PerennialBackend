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

Run bootstrap using the administrative secret before every upgrade, not only the
first, then migrate using the owner identity, then reapply grants using the
administrative secret:

```bash
.venv/bin/python development/database/ops/provision_roles.py --phase bootstrap
# Use a migration login that SET ROLEs to perennial_owner for the whole migration.
.venv/bin/alembic -c development/database/alembic.ini upgrade head
.venv/bin/python development/database/ops/provision_roles.py --phase grants
```

A third phase, `--phase roles`, is only for restores: it runs bootstrap's role
checks and creates the group roles, and nothing else (see "Backup and restore
logins with row-level security" below).

Bootstrap is idempotent. It creates schema `perennial` owned by the owner, and the
owner deliberately holds no `CREATE` on the database, so it cannot create schemas,
publications or extensions. PostgreSQL checks that right before `IF NOT EXISTS`, so
a migration creates a schema only when `to_regnamespace()` finds none: that happens
only where nothing ran bootstrap (the tests, dev Compose and the quality gate, which
migrate as a superuser). A migration that adds a schema must also add it to bootstrap;
without that, the owner's upgrade fails with `permission denied for database` and
rolls back.

The administrative login may be a superuser or, as on Supabase, a non-superuser
`CREATEROLE` login that owns the database. Such a login holds only `ADMIN OPTION` on
the roles it creates, so bootstrap grants it `SET` on the owner (never `INHERIT`): it
can then create the owner's schema and re-own objects to the owner, and both phases
make the grants on the owner's objects as the owner. This needs PostgreSQL 16 or later.

Bootstrap also grants owner CREATE in `public` for Alembic's version table. Grant phase transfers the explicitly listed tables
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
configuration without password hashes. The dump and its manifest cover schemas
`perennial` and `app` (the account tables); a pipeline-only dump is not a
complete account recovery.
Supabase-managed schemas, Auth configuration and any Storage objects need their
own documented restore coverage; database dumps do not include Storage file bytes.
Follow the [Supabase backup/restore procedure](https://supabase.com/docs/guides/platform/migrating-within-supabase/backup-restore)
for managed-schema restrictions rather than overwriting service-owned roles.

### Backup and restore logins with row-level security (decision 34)

The `app` tables have forced row-level security, and their policies name only
the API role. A login that RLS filters cannot take or check a complete backup:
`pg_dump` runs with `row_security` off, so as the owner it fails at the first
`app` table rather than dumping zero rows, and `COPY` into those tables is
refused. So the former single migration/backup login is split three ways:

| Login | Attributes and access | Never |
| --- | --- | --- |
| Migration login | Owner-class (session role `perennial_owner`), `NOBYPASSRLS`; runs Alembic | Superuser or `BYPASSRLS`: the owner must stay subject to `FORCE ROW LEVEL SECURITY` |
| Backup login | `BYPASSRLS` plus membership in `pg_read_all_data`; can only read | DDL rights, ownership, write privileges or any other membership |
| Restore: the administrative login | Superuser, or `BYPASSRLS` with the right to create the database and its objects. On Supabase, the platform's restore path or `postgres` | A standing application login |

`BYPASSRLS` on the backup login is what a complete backup requires, not a way to
pass a check. The backup login reads every user's data, so hold its secret as
Bryan holds the administrative one (D3). It can never double as the API login
(the startup check refuses `BYPASSRLS` and `pg_*` membership) or as the importer
(no write privilege). It cannot restore either: it has no right to create the
database or its objects.

`backup.py` refuses before `pg_dump` starts, with `error_code`
`rls_bypass_required` and nothing stored, when any table has row security
enabled (forced or not) and its login is neither superuser nor `BYPASSRLS`.
`restore_drill.py` makes the same check before it dumps or restores.

**Restore order.** `pg_dump --no-acl` still writes `CREATE POLICY ... TO` the API
role, and restores run with `--exit-on-error`, so the roles must exist first. As
the administrative login, against the new target database:

1. **Roles only:** `provision_roles.py --phase roles`. It runs bootstrap's role
   checks and refusals and creates any missing group role, the API role
   included, then stops before any database, schema or default-privilege
   statement. Roles are cluster-wide, so this is all the restore needs. Do not
   use `--phase bootstrap` here: it creates schema `perennial`, and the restore
   then fails with `schema "perennial" already exists`.
2. **Restore:** `pg_restore --exit-on-error --single-transaction --no-owner
   --no-acl` of the archive. Every restored object is now owned by the restorer.
3. **Migrate:** `alembic upgrade head`, also as the administrative login
   (`DATABASE_URL` set to it for this one command). A backup can be older than
   the code: its `schema_revision` is in the manifest. At the current revision
   this does nothing, so always run it. It runs as the administrative login, not
   the migration login as in a deployment, because the restorer owns every
   restored object until step 4. Skipping it fails closed: a backup at `0009`
   makes `--phase grants` refuse ("table policy differs"), and one at `0010`
   (any backup from before the account tables) passes grants, then
   `provision_api_role.py` fails on the missing `app.email_is_registered`.
4. **Provision again:** `provision_roles.py --phase grants`, then
   `provision_api_role.py`. They re-own schemas `perennial` and `app` with their
   tables and sequences to the owner role, re-grant both schemas, and re-create
   the `app` policies for the API role. `app.email_is_registered` stays owned by
   the administrative role. The restored database is not ready for the API or the
   pipeline until this step has run.

```bash
.venv/bin/python development/database/ops/provision_roles.py --phase roles
# pg_restore into the new target as the administrative login (step 2)
# DATABASE_URL: the administrative login on the target, for this command only (step 3)
.venv/bin/alembic -c development/database/alembic.ini upgrade head
.venv/bin/python development/database/ops/provision_roles.py --phase grants
.venv/bin/python development/database/ops/provision_api_role.py
```

**On Supabase (Bryan confirms on the project):** whether `postgres` has
`BYPASSRLS` (`SELECT rolbypassrls FROM pg_roles WHERE rolname = 'postgres'`), and
whether a dedicated backup login can be given it (since PostgreSQL 16, only a
role that has the attribute can grant it). Recommended: a dedicated `BYPASSRLS`
backup login if the platform allows one; otherwise the scheduled backup runs as
`postgres`, with its credential held by Bryan alone. Never `service_role` through
PostgREST.

### Timed logical restore drill (Bryan-operated)

1. Record incident-start time, newest verified archive's snapshot time, archive
   checksum, expected schema revision and safe reference counts. Block provider
   egress and disable scheduled workers on the isolated restore target. Preserve
   the source database and existing backups. Choose an already approved isolated
   target; if none exists, the drill is blocked, not permission to create one.
2. Retrieve/decrypt the archive and restore schema/data to the isolated target
   as the administrative login, in the restore order above: `--phase roles`,
   `pg_restore`, `alembic upgrade head`, then `--phase grants` and
   `provision_api_role.py`. Recreate
   reviewed login memberships separately with fresh credentials. Do not use a
   production endpoint or destructive `--clean` option.
3. Verify migration revision, constraints, table counts (the `app` tables too, as
   the administrative login: as the owner they show zero rows), batch/run lineage,
   publication pointer and byte-identical publication replay. Validate denied
   importer updates to immutable rows, reader-only access and cross-user RLS
   isolation through the API login. Exercise the worker with synthetic inputs,
   no providers.
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
| Migration login | Rotate separately from worker/readers. Preserve only the owner session role; verify a staging migration before retiring the old identity. | Retain an authorized recovery login throughout. Never superuser or `BYPASSRLS`: the owner must stay subject to forced RLS. |
| Backup login | Successor with exactly `BYPASSRLS` and `pg_read_all_data` (decision 34): no DDL, ownership, writes or other membership. Verify a staging backup and a restore drill before retiring the old identity. | Keep the old login until a new backup verifies, then disable it promptly: it reads every user's data. Restores use the administrative login, not this one. |

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
table in `perennial` and `app` (reported as `app.<table>`) and checks that
constraints are validated. It reads them with `row_security` off, so a login
that RLS would filter fails instead of verifying fewer rows. Run it as the
administrative login. With `--roles OWNER IMPORTER READER API` it follows the
restore order above around its restore, except that it does not migrate (step
3): it drills a backup at the code's revision, and an older one fails closed at
step 4. Without `--roles`, those roles must already exist on the server, as they
do when the drill server also hosts the source. Only the newly
created target is dropped, including after a restore failure; the source and
existing databases are preserved. The dump and `report.json` remain in the private
output directory for review. A passing report contains `verified: true` and
`target_dropped: true`. If cleanup fails, identify only the reported generated
target before taking action. Do not retry with a broader target or `--clean`.

This drill proves snapshot dump/restore and retained row content for a disposable
database. Without `--roles` it excludes ownership/ACL restoration. It does not exercise continuous
WAL, recovery to a timestamp, service failover, cloud permissions, production data
loss, or production RTO. The M3 logical exercise above must separately establish hosted recovery
evidence; neither exercise establishes PITR. Synthetic archives may be deleted after their evidence is recorded;
production backup retention is separately owned by the deployment.

### Local scheduled backup job and timed archive restore

`development/database/ops/backup.py` is one run of the scheduled job above,
local-first: the same loopback/`test`-name guards and `--synthetic-only`
attestation as the drill, `pg_dump`/`pg_restore` 17 and `openssl` (3.x) on PATH.

```bash
.venv/bin/python development/database/ops/backup.py --synthetic-only \
  --recipient synthetic-cert.pem --spool /private/tmp/perennial-backup-spool \
  --store /private/tmp/perennial-backup-store
```

It takes a repeatable-read snapshot under advisory lock 170097 (one backup at a
time) and streams a custom-format `pg_dump` of that snapshot straight into
OpenSSL CMS public-key encryption (AES-256-GCM). No plaintext dump is written,
and the backup host holds only the recipient certificate; the private key is
needed only to restore. The archive is uploaded, its stored SHA-256 is checked
against the local one, and only then is its manifest written: name, snapshot
time, completion time, schema revision, archive checksum and size, encryption,
recipient certificate fingerprint, storage receipt, and per-table row counts and
content hashes. No connection details or rows. A manifest exists only for a
verified archive. Retention then keeps every snapshot from the last 24 hours, the
newest of each of the last seven UTC days, and always the newest verified backup.
It deletes a manifest before its archive, so an interruption leaves an orphan
archive, never a manifest without one. Orphan archives (a failed manifest upload,
or an interruption) are swept once their name dates them more than an hour old.

A store is untrusted input once it is remote, so nothing read back from it
chooses a path:
- Every stored name is `perennial-<YYYYMMDDTHHMMSSZ>-<6 hex>` plus `.dump.cms` or
  `.manifest.json`.
- A manifest is used only if it is at most 1 MiB and well formed, and its `name`
  and `archive` agree with its own filename. Malformed manifests are never acted
  on (not deleted, not restored); they are listed in `invalid_manifests` for a
  human.
- `LocalStore` accepts only plain basenames confined to its root, lists and
  touches only regular files (never symlinks), and tightens an existing spool or
  store directory to 0700. It refuses a symlinked root.

Manifests and archives are **not signed**: anyone holding the public certificate
can create an archive that decrypts. Checksums and GCM prove integrity, not
origin, so the store itself must stay trusted. Compare archive fingerprints with
the custody inventory below.

The report is JSON on stdout with a fixed `status`/`error_code`. Exit 0 is `ok`;
1 is a failure (`unsafe_source`, `unsafe_directory`, `openssl_unsupported`,
`recipient_unreadable`, `dump_failed`, `encrypt_failed`, `upload_failed`,
`upload_verify_failed`, `retention_failed`, `backup_failed`); 3 is
`skipped_overlap`, which must alert like a failure. A `pg_dump` that dies
mid-stream fails the run even though encryption of the truncated stream succeeds.
A stored-object checksum that differs from the local one fails as
`upload_verify_failed`, and the unverified archive is removed. OpenSSL 3 is
required: macOS's `/usr/bin/openssl` is LibreSSL, which cannot do CMS AES-GCM and
is refused up front, so put Homebrew's `openssl@3` first on PATH. `snapshot_age_seconds` is completion minus snapshot
time: it must stay under ten minutes for the five-minute schedule to meet RPO.

`--store` is a stub: a local directory standing in for off-site storage until
the Supabase project and an encrypted off-site target exist. A real uploader
keeps `LocalStore`'s `put`/`get`/`names`/`delete` and returns the provider's
checksum of the stored object. Production use also needs a connection mode
without the loopback guard (a protected libpq service file, TLS verification),
a scheduler, alerting on the exit status and newest manifest age, and Bryan's
key custody. None of that exists yet.

To restore the newest verified archive into a disposable database and time it:

```bash
.venv/bin/python development/database/ops/restore_drill.py --synthetic-only \
  --from-store /private/tmp/perennial-backup-store --key synthetic-key.pem \
  --output /private/tmp/perennial-archive-drill-unique
```

It selects only a validated manifest, copies that archive to a fixed name in the
new 0700 output directory, and checks it against the manifest checksum. It then
streams `openssl cms -decrypt` straight into `pg_restore`, which restores in one
transaction into a new random `perennial_test_restore_*` database. **The decrypted
dump is never written to disk.** OpenSSL holds the whole archive in memory and
checks the GCM tag before writing any output, so an archive that fails
authentication (including one with a forged checksum) passes no bytes to
`pg_restore`. That memory cost is proportional to the archive. If the key is
encrypted, set `BACKUP_KEY_PASSPHRASE`: only the decryption process receives it,
never `pg_restore`.

The drill compares every table's count and hash with the manifest, the `app`
tables included, and drops only the database it created. As above, run it as the
administrative login, and add `--roles` to rehearse the full restore order. On every path (success, failure, interruption), it
kills its child processes and removes the encrypted copy, so **only
`report.json` remains**. Unlinking is cleanup, not secure erasure, which is one
more reason the plaintext never reaches disk. The report records
`retrieve_seconds`, `decrypt_restore_and_verify_seconds`, `rto_seconds` from
incident start (default: drill start) against `rto_target_seconds` 3600, and
`rpo_seconds` against 900. A local synthetic timing is not the hosted RTO, which
the Bryan-operated drill above must measure.

### Backup key custody (Bryan-owned; draft options)

Archives are encrypted to a recipient **certificate**: the public half, which
the backup host holds. Only the matching **private key** decrypts them. Losing
the key makes every archive unreadable; leaking it, together with storage access,
exposes every archive. Agents never create, hold or see the real key. The
commands below are for Bryan, on his own machine.

**Key and certificate.** Generate an RSA-3072 key that is encrypted at rest from
the start, plus a self-signed certificate for it:

```bash
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -aes-256-cbc -out perennial-backup-key.pem
openssl req -x509 -new -key perennial-backup-key.pem -days 400 \
  -subj "/CN=perennial-backup-YYYY" -out perennial-backup-cert.pem
openssl x509 -noout -fingerprint -sha256 -in perennial-backup-cert.pem
```

`genpkey` prompts for the passphrase, so it never enters argv or shell history.
Record the fingerprint: every manifest stores it as
`recipient_sha256_fingerprint`, which is how you tell which key opens an archive.
Encryption does **not** check the certificate's validity dates, so an expired
certificate keeps working. Rotation is a schedule, not an expiry event. Only the
certificate goes to the backup host and its config; the key never does.

**Where the private key lives.** Options, all within D14's free tiers:

| Option | Strength | Weakness |
|---|---|---|
| A. Encrypted key file as an attachment in Bryan's password manager | Available anywhere Bryan is; synced copies | One account compromise exposes the key file (it is still passphrase-protected) |
| B. Encrypted key file on two offline media, kept in separate places | Outside every online failure boundary | Slow to reach during an incident; the media themselves need checking |
| C. Cloud secret manager in the hosting provider | Scriptable | Shares a failure and compromise boundary with the backups; may cost money; unnecessary for one operator |
| D. Hardware token (key generated on device) | Key cannot be copied | OpenSSL needs a PKCS#11 provider; not tested here; losing the token loses the archives unless a second recipient exists |

**Recommendation: A plus one copy of B.** Keep the passphrase-encrypted key file
in the password manager and one encrypted copy offline. Keep the passphrase in
the password manager as a separate item. Note the limit: a separate item is
organizational separation, not a separate compromise boundary. Anyone who
controls the password manager account holds both the file and the passphrase.

For a paper copy of the passphrase (so the key survives losing the password
manager), Bryan chooses between:
- **Store it apart from the offline key media (recommended).** Taking one place
  yields only one factor. Recovery needs both places, so it is slower.
- **Seal it with the offline media.** Recovery is simpler, but whoever gets that
  bundle holds both factors, just as with the password manager.

Never put the key or passphrase on the backup host, in CI secrets, the
repository, Supabase, chat, or any agent-readable directory. These options are
proposals, not verified product features or plan entitlements.

**Passphrase handling during a drill.** `restore_drill.py --from-store` passes
the passphrase to the decryption process only, as
`-passin env:BACKUP_KEY_PASSPHRASE`, so it never appears in argv or reports. The
key file's *path* is in argv; its contents are not. Enter it without echo or history, and clear it
afterwards:

```bash
read -rs BACKUP_KEY_PASSPHRASE && export BACKUP_KEY_PASSPHRASE
# ... run the drill ...
unset BACKUP_KEY_PASSPHRASE
```

Other processes of the same OS user can read a process's environment. Run the
drill on Bryan's own trusted machine, in a fresh shell that is closed afterwards.

**Decrypting during a drill** (fits step 2 of the timed drill above):

1. Copy the encrypted key file from custody into a new private directory
   (`mkdir -m 700`). The file is still passphrase-encrypted on disk.
2. Before restoring, check where the archive came from and that the key matches
   it. The manifest's `recipient_sha256_fingerprint` only says which key opens
   the archive, not who made it, because manifests are unsigned. Compare it with
   the fingerprint in Bryan's custody inventory, and restore only from the
   trusted store. The key must also match that certificate (the two hashes must
   be equal):
   ```bash
   openssl x509 -noout -fingerprint -sha256 -in perennial-backup-cert.pem
   openssl pkey -in perennial-backup-key.pem -pubout | openssl sha256
   openssl x509 -in perennial-backup-cert.pem -noout -pubkey | openssl sha256
   ```
3. Run the drill with `--key` pointing at that file. The drill streams the
   decrypted dump into `pg_restore`, so no plaintext file is created. An archive
   that does not decrypt and authenticate restores nothing.
4. Delete the key directory, then `unset` the passphrase and close the shell.
   The deleted key file was ciphertext. The drill's output directory holds only
   `report.json` (the encrypted copy is removed automatically); keep it as
   evidence.

**Rotation.** Rotate yearly with the restore drill, immediately on suspected
compromise, and when custody changes hands.

1. Generate a new key and certificate as above, and store the key per the
   chosen option.
2. Put the new certificate on the backup host. The next run's manifest shows the
   new fingerprint.
3. Run a drill on a new-key archive with the new key before retiring anything.
4. Keep the old key until no stored archive uses its fingerprint. Check this in
   the actual store inventory (every manifest's `recipient_sha256_fingerprint`,
   plus any orphan or invalid entries), never by elapsed time alone. Under the
   retention above, expect it about seven days after the first new-key backup.
   Then destroy every copy and record the date and fingerprint.
5. On compromise, also decide whether to delete old-key archives once new
   verified backups exist. Anyone holding the old key and storage access can read
   them. Re-encryption is not supported.

Changing only the passphrase (`openssl pkey -in old.pem -aes-256-cbc -out
new.pem`, with prompts) leaves the key and every archive unchanged. A
compromised passphrase alone, with the file still safe, can be handled this way.

**Not built:** encrypting to a second escrow recipient. OpenSSL CMS supports
several recipient certificates, but `backup.py --recipient` takes one. Add it if
Bryan wants a second custodian or a hardware-token primary.

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
