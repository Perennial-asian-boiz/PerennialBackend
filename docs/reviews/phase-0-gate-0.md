# Phase 0 handoff for Gate 0 review

Work item: [GitHub issue #5](https://github.com/Perennial-asian-boiz/PerennialBackend/issues/5).
Related CI issue: [#1](https://github.com/Perennial-asian-boiz/PerennialBackend/issues/1).

**Implementation is ready for review; Gate 0 is not closed.** PR #4 was already
merged at `c16831e` (2026-10-07 22:11:50 Pacific) when this work started. After
fetching, `git rebase origin/main` advanced the clean local branch to that merge.
The follow-up work is uncommitted on `fix/phase-0-foundations`, based on `c16831e`.
The remote follow-up branch also points at `c16831e` (verified after it appeared during this session); it contains none of these edits, and there is no follow-up PR yet. Reviewers must use the eventual published commit SHA
for approval; neither this base SHA nor an old review fingerprint approves the
current worktree.

## Changes to review

The insider parser's `(buys, error_code, error_detail)` return value is now
unpacked. Parser failures discard partial buys, fail the unit, and stop further
requests. Answered-empty and HTTP 404 count as completed units; transport/parser
failure leaves later planned units unattempted. Error detail is not persisted.
The existing bounded HTTP/retry layer and pinned database inputs are retained.
Tests were adapted to the loaders' `(tickers, error)` tuples. `insider.py` and
`test_pipeline_contract.py` remain byte-identical to `origin/main`; no database
rig fixture change was necessary.

Strict core typing required explicit engine/connection, collection, callable,
and return types. Pydantic dynamic type factories became `Annotated` fields
using the same validators. JSON/database boundaries retain explicit dynamic
value types. SQLAlchemy row-pair reads use `.tuples()` for their dictionary
conversion. Core formatting is normalized to the new Ruff configuration.

The workflow uses Python 3.12.13 and PostgreSQL 17, mandatory synthetic database
tests, lint, formatting, strict core mypy, migration round-trip/drift and audit.
The new migration gate validates its target, strips ambient libpq defaults,
pins loopback, creates a fresh random database and drops only that database.
A regression test introduces real schema drift and confirms detection and cleanup.

The dependency audit required urllib3 2.8.0 and pytest 9.0.3; APScheduler is now
exact-pinned at 3.11.3. New quality tools are exact-pinned. Full transitive locking
remains Phase 2. Review dependency/network changes with the Astra security auditor.

CODEOWNERS uses verified human repository accounts. The PR template includes the
independent-review checklist. `.github/branch-protection.json` is the proposed
payload for the exact check name `Backend quality`; it has **not been applied**.
Archived f9c73aaa and b3ef39d8 evidence is indexed in [README.md](README.md).

## Verification

All commands ran using repository `.venv/bin/python` (Python 3.12.13). The original
Python 3.10 environment is preserved under `.local-tools/venv-python310-pre-phase0`.
The selected `.venv` points to the prepared project-local 3.12 environment.

- `TEST_DATABASE_URL=postgresql+psycopg://perennial_test@127.0.0.1:55471/perennial_test REQUIRE_DATABASE_TESTS=1 .venv/bin/python -m pytest development/backend/tests -q --tb=short`
  — **286 passed in 3.83s, zero skipped**, PostgreSQL 17.7, synthetic data only.
- `.venv/bin/python -m mypy` — **22 source files passed**, strict core config.
- `.venv/bin/python -m ruff check` and `ruff format --check` — **passed**, 23 files.
- `TEST_DATABASE_URL=... .venv/bin/python development/database/ops/quality_gate.py`
  — head/base/head passed; **no new upgrade operations detected**.
- `.venv/bin/python -m pip check` — no broken requirements.
- `.venv/bin/python -m pip_audit --progress-spinner off` — **no known vulnerabilities**
  in the installed final dependency set. Requirements-based audit on this macOS
  standalone Python hit a reproduced copied-venv shared-library failure; the
  operational workaround is documented. Linux CI retains `-r requirements-dev.txt`.
- Workflow YAML parsed; required check name matches the protection payload.
- All nine archived source artifacts match recorded SHA-256 values. Protected
  insider source and database-owned contract test match fetched `origin/main`.
- `git diff --check` passed.

Before the fix, the collector suite had **3 failures / 41 passes** and the
PostgreSQL contract suite had **1 failure / 3 passes**. After the integration,
the unmodified end-to-end contract publishes and replays successfully. These
failures are the behavioral baseline; the typing/configuration changes are not
claimed as a separate test-first implementation.

## Gate 0 remaining decisions and actions

1. Bryan confirms D2, reviews this local implementation, and authorizes commit
   and publication of the follow-up under the repository's no-commit/no-push boundary.
2. The database coordinator routes checker and Astra security reviews against the
   published head; request `haohnguyen94-droid` for insider integration review.
3. GitHub-hosted `Backend quality` must pass. Local execution does not establish
   that the hosted Ubuntu package-install step or GitHub CI has run successfully.
4. Apply and verify the reviewed branch-protection payload and merge the follow-up
   only with Bryan's approval. GitHub reported `main` unprotected during this work.

No live provider calls, production credentials/data, deployment, new agents, or
global permission changes were made. Existing source and ops seats were used;
primary integrated the source work and completed ops after a recorded handoff.
Phase 1 has not started.
