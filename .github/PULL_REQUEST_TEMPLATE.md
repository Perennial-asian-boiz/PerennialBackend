## What changed

Describe the user-visible or operational behavior and the reason for the change.

## Definition of done

- [ ] Relevant tests pass against synthetic data, including PostgreSQL tests when database behavior changes.
- [ ] Ruff lint and format checks pass for the owned core packages.
- [ ] Strict mypy passes for `src/db`, `src/ingestion`, and `src/pipeline`.
- [ ] Alembic upgrade → downgrade → upgrade and drift checks pass for schema changes.
- [ ] Dependency audit result is reviewed; any finding has a tracked disposition.
- [ ] Documentation and operations notes reflect changed behavior and risks.
- [ ] No credentials, provider responses, or production data appear in the PR or logs.

## Evidence and rollout

Record exact commands/results, migration or deployment steps, and any remaining limits.

## Independent review

- [ ] Branch is rebased onto current `main`; all required CI checks are green.
- [ ] Database rig checker reviewed this exact PR head commit; verdict is linked.
- [ ] Astra security review is linked for secrets, privileges, logs, dependencies, or network changes.
- [ ] Affected code owners reviewed the change (including insider integration).
- [ ] No new `print()` in `src/`, secrets, or unpinned dependencies.
- [ ] Migrations use expand/contract, have tested downgrades, and document lock impact/timeouts and grants.
- [ ] Bryan approved any merge, shared-database migration, deployment, or spending.
