# Phase 0: production pipeline foundations

PR #4 was already merged at `c16831e` when Phase 0 began. The local branch was
rebased onto fetched `origin/main`, then `fix/phase-0-foundations` was created
for the follow-up. This packet links the insider integration, CI foundation,
and review archive work. Existing CI issue: #1. Original integration PR: #4.

Implemented locally for independent review:

- Adapt the insider collector to `a9fed58`'s parser tuple; preserve answered-empty,
  not-found, failed, and unattempted semantics with synthetic regression coverage.
- Keep `insider.py` and the database-owned `test_pipeline_contract.py` unchanged.
- Standardize local and CI Python 3.12/PostgreSQL 17; add strict core mypy, Ruff
  lint/format, PostgreSQL tests, migration round-trip/drift, and dependency audit.
- Pin tool dependencies; update urllib3 and pytest to audit-reported fixed versions.
- Add CODEOWNERS, PR checklist, and a reviewed-before-application branch-protection payload.
- Archive f9c73aaa and b3ef39d8 packets/manifests/findings and independent reports
  under `docs/reviews/`, with verified hashes and attribution.

Verification on 2026-10-08:

- Python 3.12.13, PostgreSQL 17.7: **286 synthetic tests passed**, zero skipped.
- Strict mypy: 22 core source files passed. Ruff lint/format: 23 files passed.
- Upgrade head → downgrade base → upgrade head and Alembic drift check passed
  on a fresh isolated database. Injected drift fails and drops only its own DB.
- Installed requirements pass `pip check`; installed dependency audit reports no
  known vulnerabilities. macOS standalone Python cannot support pip-audit's
  temporary copied venv; Linux CI retains the requirements-based audit.
- Workflow YAML parses, protection check name matches `Backend quality`, archive
  hashes verify, and both protected source/test files match `origin/main`.

Gate 0 acceptance remains open:

- [ ] Bryan confirms D2 (Python 3.12 / PostgreSQL 17).
- [ ] Database coordinator routes checker and Astra security review of the follow-up.
- [ ] Insider code owner `haohnguyen94-droid` reviews the integration.
- [ ] Bryan authorizes commit/publication of the follow-up; actual PR head becomes
      the review identity. Current local branch has no new commits or pushes.
- [ ] GitHub Actions reports green for that exact head.
- [ ] Apply and verify `.github/branch-protection.json` after review; `main` was
      unprotected when read via GitHub API on 2026-10-08.
- [ ] Bryan approves and merges the follow-up. Existing PR #4 is already merged.

No production providers, credentials, deployment, global permission changes, or
new agents were used. The previously selected review keys remain mandatory;
local tests do not self-approve this work or certify a production deployment.

Tracking issue: https://github.com/Perennial-asian-boiz/PerennialBackend/issues/5
Detailed handoff: [Phase 0 Gate 0 review](../reviews/phase-0-gate-0.md).
