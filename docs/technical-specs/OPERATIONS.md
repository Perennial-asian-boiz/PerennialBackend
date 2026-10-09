# Production pipeline operations

This runbook describes deployment requirements and the repository tooling. It
does not record a completed cloud deployment, configured PITR, or approved
recovery objectives. Each environment needs an accountable deployment owner.

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
  migration path (currently revisions through `0003`, use `upgrade head`).
- Configure private network access, TLS with certificate verification, encryption
  at rest, maintenance/failover policy, storage and connection capacity.
- Use a direct PostgreSQL connection or a session-pooling endpoint for workers.
  The pipeline uses session advisory locks; transaction pooling is unsupported,
  including when prepared statements are disabled.
- Provision the owner/importer/reader groups and independent login identities;
  restrict migration credentials to release operations.
- Set the application's `DATABASE_URL` to the importer identity and inject the
  required provider secrets. Run migrations and reapply the explicit grants.
- Enable the service's backups and continuous WAL/PITR retention. Record actual
  retention, backup region/account separation, access controls and restore steps.
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
coverage/lineage failures, database availability/storage, backup age, and PITR
window. Configure paging destinations, severity and thresholds in the deployment;
the repository does not install alert integrations. A failed candidate keeps the
previous current publication. Inspect persisted run status/error codes and the
current publication before deciding whether to retry or escalate. Avoid bypassing
coverage, freshness or lineage rules to make a publication appear current.

## Recovery objectives and production PITR exercise

The deployment owner must agree and record the real RPO (maximum acceptable
committed-data loss) and RTO (maximum acceptable recovery outage). **Configurable
examples only:** RPO 15 minutes, RTO 2 hours, PITR retention 14 days, and a monthly
restore exercise. These values are not an established Perennial agreement.
Record chosen targets, alert thresholds, responsible operator, exercise cadence
and measured results in the deployment record.

1. Confirm the managed backup/WAL history covers the chosen recovery timestamp
   and record source identity, target time, expected publication and retained
   source batches. Preserve evidence outside the source database.
2. Restore through the managed service to a new isolated instance/database. Keep
   original production intact, block provider network access, and disable workers
   so restored credentials cannot trigger live collection.
3. Measure restore/startup elapsed time and compare committed records immediately
   before/after the chosen timestamp to measure actual data loss. Verify schema
   revision, constraints, source row/batch counts, lineage, publication pointer,
   retained market-cap observations and deterministic publication replay. Restore
   and inspect effective role grants and authenticated reader/importer access.
4. Validate a reader smoke check and a synthetic worker path on the recovered
   instance. Record success/failure, measured RPO/RTO, retention gaps and actions.
5. Any production cutover requires the deployment owner's authorization and a
   rollback plan. Isolate/pause writers before changing endpoints; rotate restored
   secrets as required. Retire only the exercise resources created for this drill.

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
loss, or production RTO. Only the managed-service exercise above establishes PITR
evidence. Synthetic archives may be deleted after their evidence is recorded;
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
