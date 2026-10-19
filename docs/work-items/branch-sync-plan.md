# Branch sync plan

**Owner:** Bryan · **Written:** 2026-10-14 from `origin/main` at `022152f`
**Status: complete (2026-10-18).** Outcome:
- **Our branches:** all merged. #9 and #15 merged directly. #10–#13 and #16 had been merged into their stack bases by mistake; they landed on `main` through #19 with an identical tree.
- **Guardrails:** the CI migration guard (#15) is required, merge commits are the style, and head branches are deleted automatically on merge.
- **Branches deleted:** 12 merged or superseded branches.
- **Contributor branches, tracked in the roadmap rather than here:**
  - #17 GDELT: Stage C review
  - #18 Finnhub insider collector
  - Jaden's `codex/supabase-auth-resend`: no PR yet
  - CohenK's `public-sentiment`: no PR yet

The rest of this document is the original plan, kept for the record.
**Goal:** every active branch is merged or rebased onto one `main`, with exactly one migration history and green CI, and with guardrails so this doesn't happen again. Database building (roadmap M0 onward) resumes when this plan's exit criteria are met.

---

## 1. Inventory

| Branch | Owner | PR | Behind `main` | State | Touches |
|---|---|---|---|---|---|
| `feat/settings` | Bryan (rig) | #9 draft | 0 | CI green | settings, `cli.py`, `scheduler.py`, `collectors.py`, `fmp.py`, `requirements.txt` |
| `feat/db-session-settings` | Bryan (rig) | #10 draft | 6 | CI green | `session.py`, `cli.py`, `scheduler.py`, migrations `env.py` |
| `feat/publication-quality-checks` | Bryan (rig) | #11 draft | 6 | CI green; **design decision D12 open** | `publication.py`, `runner.py`, `cli.py`, `scheduler.py`, **plus a duplicate copy of the settings commit** |
| `feat/fmp-pagination` | Bryan (rig) | #12 draft | 6 | CI green | `collectors.py`, `fmp.py`, `diagnostics.py` |
| `ops/lockfile` | Bryan (rig) | #13 draft | 6 | **CI red**: `greenlet` missing from the lockfile | `pyproject.toml`, lockfiles, CI workflow, **plus a duplicate copy of the settings commit** |
| `ops/gate0-cleanup` | Bryan (rig) | #14 draft | 6 | CI green | docs only |
| `integration-preview` | Bryan (rig) | none (backup) | 6 | holds already-resolved versions of the conflicts above | reference only |
| `GDELT` | AalindCS | **none** | 6 | migrations **`0004` and `0005`** | `models.py`, `runner.py`, `publication.py`, `scheduler.py`, `provision_roles.py`, `services/gdelt/` |
| `feature/backend-auth-api` | Jaden Le | **none** | 6 | migration **`0004`** | `models.py`, `provision_roles.py`, `requirements.txt`, `src/api/` |
| `public-sentiment` | CohenK | **none** | 19 | conflicts with `main` on `.gitignore` | `services/sentiment/`, `utils/fetch_tickers.py` |

**Fully merged and safe to delete:** `FinBERT`, `daily-news-fetcher`, `database-ingestion-pipeline`, `fix/phase-0-foundations`, `insider-finnhub-provider`, `remove-senate-stock-watcher` and `search-engine-sentiment`.

### Where conflicts happen

| File | Branches editing it | Why |
|---|---|---|
| `src/pipeline/scheduler.py` | settings, session settings, quality checks, lockfile (via the duplicate commit), GDELT | Every branch hooks into the scheduler |
| `src/ingestion/cli.py`, `tests/test_scheduler.py` | settings, session settings, quality checks, lockfile | Same cause |
| `requirements.txt` | settings, lockfile, auth | New dependencies; the lockfile turns this file into a stub |
| `ops/provision_roles.py`, `src/db/models.py` | GDELT, auth | Both add tables and grants |
| **Alembic revisions** | GDELT (`0004`, `0005`), auth (`0004`), plus the roadmap's planned `0004` | Sequential numbers chosen on parallel branches |

---

## 2. The problems and their fixes

| # | Problem | Severity | Fix |
|---|---|---|---|
| P1 | **Two migrations claim revision `0004`.** Alembic only *warns* about a duplicate revision id; one file silently replaces the other, so the schema you get depends on which file is loaded. | Critical | §3 Stage A: a CI guard and a migration-numbering rule. §3 Stage D: renumber at merge time. |
| P2 | **The auth and GDELT schemas go against the database roadmap.** Auth builds custom login tables, while issue #3 specifies Supabase Auth. Its user tables are in the pipeline schema, and the pipeline role gets `DELETE` on them. GDELT puts `sector` and `industry` on `market_cap_observations`, a table the roadmap retires. | High: migrations become hard to undo once they run on a shared database | §3 Stage C: schema review before merge |
| P3 | #13 fails CI because the lockfile was compiled on macOS, which leaves out `greenlet`, a Linux-only SQLAlchemy dependency | High: blocks the lockfile | Recompile the lockfile on Linux (Stage B5) |
| P4 | Five draft PRs are behind `main`, and two carry a duplicate copy of the settings commit | Medium | Merge train with explicit `--onto` rebases (Stage B) |
| P5 | Three contributor branches have no PR, so nobody saw them coming (which is how P1 happened) | Medium | Open draft PRs now (Stage A) |
| P6 | Branch protection has no reviewer rule; `main` requires only CI | Medium | Decision in §5 |
| P7 | Seven merged branches remain on GitHub | Low | Stage E |
| P8 | The roadmap and kickoff docs are uncommitted | Low | Stage B1 |

---

## 3. The plan

The order is deliberate. **Infrastructure branches merge first** because they touch the shared files (`scheduler.py`, `cli.py`, `requirements.txt`) and we can rebase them ourselves. Contributors then rebase **once** onto a `main` that has stopped moving, instead of chasing four successive changes.

### Stage A: Freeze and guardrails (day 1)

1. **Announce a short migration freeze:** nothing under `development/database/migrations/` merges until Stage D. Feature work continues on branches as normal.
2. **Ask each contributor to open a draft PR** from their branch (AalindCS, Jaden, CohenK). Drafts cost nothing, make the work visible, and run CI.
3. **Ask AalindCS and Jaden whether their migrations have run on any shared database** (for example a Supabase dev project). If not, renumbering is free. If so, that database needs an `alembic stamp` after renumbering, and the plan records which one.
4. **Merge a migration-guard PR** (`ops/migration-guard`, small, owned by the rig) before any migration PR:
   - a test that fails if the revision ids in `migrations/versions/` are not unique
   - a test that fails if `alembic heads` returns more than one head
   - CODEOWNERS: `development/database/migrations/**` and `development/database/ops/**` require a database reviewer

   This turns P1 from a silent schema bug into a red CI check on whichever PR comes second.

### Stage B: Infrastructure merge train (days 1–3, rig-owned)

Each step: rebase onto the latest `main`, wait for green CI, mark ready, get review, merge, then move to the next. Use **merge commits** to match #4–#8. The `--onto` rebases below work whichever merge style you pick.

| Step | PR | How to rebase | Expected conflicts |
|---|---|---|---|
| B1 | #14 docs | Add `docs/roadmap/database-roadmap.md`, `docs/work-items/phase-1-2-kickoff.md` and this plan, then merge | none |
| B2 | #9 settings | Already current | none |
| B3 | #10 session settings | `git rebase origin/main` | `cli.py`, `scheduler.py`, `test_scheduler.py`. Resolve to match `integration-preview` commit `9bc74b5`, which already combines the two |
| B4 | #12 FMP pagination | `git rebase origin/main` | `collectors.py`. Resolve to match `integration-preview` commit `3863971` |
| B5 | #13 lockfile | `git rebase --onto origin/main df1e74f ops/lockfile` drops the duplicate settings commit | `requirements.txt`. Then **recompile both lockfiles on Linux** with the ADR 0001 commands inside `docker run --platform linux/amd64 python:3.12`, so `greenlet` and every other Linux-only package is included |
| B6 | #11 quality checks | `git rebase --onto origin/main 07a99d9 feat/publication-quality-checks` drops the duplicate | `cli.py`, `scheduler.py`, `settings.py`. **Stays draft until D12 is decided.** If the decision is a `publication_events` table, its migration takes the next number in Stage D order |

After B5, every new dependency goes into `pyproject.toml` with a lockfile regeneration. Contributors adding `fastapi`, `uvicorn`, `yfinance` and others follow the same two commands.

When B3–B6 have merged, delete `integration-preview`; its contents will all be on `main`.

### Stage C: Schema review (days 1–3, runs alongside Stage B)

Review the two contributor schemas **before** they merge, because a migration that has run on a shared database is expensive to take back.

**`feature/backend-auth-api` (Jaden). Decide first:** Supabase Auth (issue #3) or the custom `users`, `email_verification_codes` and `auth_sessions` tables (this branch).
- If **Supabase Auth:** drop the custom identity tables. Keep `user_preferences` and `user_watchlist`, moved into an `app` schema with row-level security as in roadmap M5.
- If **custom auth:** record it in an ADR and close issue #3 as superseded. Either way:
  - user tables move out of the `perennial` schema
  - **the pipeline role loses its write and `DELETE` grants on user tables**; only the API role writes them
  - password, token and verification-code storage gets a security-auditor review

**`GDELT` (AalindCS):**
- Move `sector` and `industry` out of `market_cap_observations` and into a company-profile table. That can be a small early version of roadmap M4's `company_profiles`, which this branch now justifies building.
- Check that `gdelt_macro_events` follows the ingestion contract (runs, batches, lineage) rather than a separate write path.
- Agree on how its hook enters `scheduler.py` once settings and session settings have merged.

**Outcome:** each branch either gets a short list of required changes or an explicit "merge as is" decision written down in its PR.

### Stage D: Contributor merge train (days 3–5)

The order follows review readiness. **Migration numbers are assigned at merge time, not when written:** whoever merges next takes the next free number and renumbers in the rebase. Provisional order:

| Step | Branch | Rebase work | Migration ids |
|---|---|---|---|
| D1 | `public-sentiment` (CohenK) | Merge `main` into it or rebase (19 behind); fix the `.gitignore` conflict; add its dependencies through the lockfile | none |
| D2 | `GDELT` | Rebase; resolve `scheduler.py` against the new settings and session code; apply the Stage C changes | `0004`, `0005` (written first) |
| D3 | `feature/backend-auth-api` | Rebase; resolve `provision_roles.py` against GDELT's grants; move `fastapi` and `uvicorn` into the lockfile; apply the Stage C changes | renumber to `0006` (`down_revision = "0005"`) |
| D4 | #11 quality checks, if D12 calls for a table | Rebase | `0007` |

The Stage A guard fails any PR that gets this wrong. Each migration PR also runs the existing upgrade → downgrade → upgrade round-trip.

Contributors rebase their own branches. The rig doesn't push to someone else's branch without that person's agreement.

### Stage E: Cleanup and lock-in (day 5)

1. Delete the seven merged remote branches and `integration-preview`, after a thumbs-up from each owner.
2. Apply branch protection with the reviewer rule decided in §5, keeping "require up to date" and the `Backend quality` check.
3. Update the database roadmap to reflect what landed: GDELT is partly built (M6), and app and auth tables exist in some form (M5). Re-baseline the migration plan table from the real head.

---

## 4. Rules from now on

1. **Open a draft PR on the first push.** A branch with no PR is invisible, and invisible branches collide.
2. **Branch from the latest `main` and rebase at least every two days** while a branch is open.
3. **Migrations:** take the next number only when merging, never edit a merged migration, and keep the CI guard required. Migration PRs need a database reviewer through CODEOWNERS.
4. **Dependencies:** add them to `pyproject.toml` and regenerate the lockfiles on Linux. Never hand-edit `requirements*.txt`.
5. **Shared files** (`scheduler.py`, `cli.py`, `models.py`, `provision_roles.py`): keep changes small and tell the other open branches in the PR description.
6. **Worktrees and anything else you need to keep go outside `/private/tmp`**, which macOS clears on reboot.
7. **One merge style:** merge commits, which is what the repo already uses.

---

## 5. Decisions needed from Bryan

| # | Decision | Blocks | Recommendation |
|---|---|---|---|
| S1 | Merge style | Stage B | Merge commits (current practice) |
| S2 | Migration order: who gets `0004` | Stage D | GDELT first (written first, smaller), then auth renumbered to `0006` |
| S3 | Supabase Auth or custom auth (D1/D11 from the roadmap, now urgent) | merging the auth branch | Supabase Auth, as issue #3 specifies. Custom auth is a large security surface for a small team |
| S4 | PR reviewers for branch protection | Stage E | One approval from anyone other than the author, plus CODEOWNERS on migrations, ops and `.github` |
| S5 | Where P1-B audits live (D12) | B6 / D4 | A `publication_events` table |

---

## 6. Done when

- [ ] Every branch in §1 is merged into `main` or closed with a recorded reason. No open draft is more than 2 days behind.
- [ ] `alembic heads` returns one head, the revision ids are unique, and the round-trip passes from base to the real head.
- [ ] CI is green on `main`, installing from Linux-compiled lockfiles.
- [ ] The migration guard and the reviewer rule are required checks.
- [ ] Only active branches remain on GitHub.
- [ ] The roadmap is updated to the real schema.

After that, database work resumes at roadmap M0 (the Finnhub collector port, which is still the only thing stopping publications).
