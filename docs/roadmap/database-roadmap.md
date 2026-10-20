# Perennial database: roadmap to completion

**Owner:** Bryan (product owner, final approver)
**Written:** 2026-10-13 · **Updated:** 2026-10-18, from `origin/main` at `12e4b1b` · **Rescoped:** 2026-10-20 for a pre-launch team (see "Sizing" below)
**Replaces:** the 2026-10-08 production roadmap (archived at `docs/roadmap/production-roadmap.md`). The process rules in §2 of that document still apply and are not repeated here.
**Purpose:** take the database from "a working watchlist pipeline that is currently stalled" to "finished": every product data domain modeled, in production, serving the app, and operating unattended. When the last milestone in this document closes, the database is done. Anything after that is a feature, not completion work.

---

## 1. Where we are

### What exists and is sound

The core pipeline merged in PR #4 and was hardened in PR #6. Keep these properties; every later milestone builds on them.

- Immutable, content-hashed source batches with run lineage (`source_batches`, `ingestion_runs`, `run_dependencies`).
- Atomic publications behind a single pointer (`publications`, `publication_sources`, `current_publication`), with byte-identical replay and versioned ranking.
- A fixed error vocabulary, advisory locking with heartbeats, restricted roles (`ops/provision_roles.py`), and a restore drill script.
- CI gates: strict mypy on the core, Ruff, PostgreSQL 17 tests, a migration round-trip and drift check, and `pip-audit`.

Since then, Phase 1/2 work has merged: typed settings (P2-B), database session timeouts and pools (P2-D), FMP pagination (P1-A), hashed lockfiles (P2-C), publication quality checks (P1-B), and Senate Stock Watcher removal. CI also fails on duplicate migration ids or a second Alembic head (#15).

Alembic head is `0005` on `main`. GDELT (#17) merged on 2026-10-20 with `0004` (`sector`/`industry` columns on `market_cap_observations`) and `0005` (`gdelt_theme_days`, `gdelt_articles`, `gdelt_spike_checks`).

### What is wrong right now

| # | Problem | Consequence |
|---|---|---|
| 1 | **`run-pipeline` can't succeed.** Since #9, the runner calls each collector without passing `settings`, and `fmp.FMP_API_KEY` is now `None`. | Congress collection always ends with `missing_credential`, so no publication is possible, even with valid keys. |
| 2 | **The free FMP plan returns HTTP 402 on Senate page 1** (found in #18's live test). | Congress collection ends as `partial_collection`, which is rejected by design. Insider collection then has no upstream input. Blocked on decision D4. |
| 3 | **The database insider collector still calls SecuritiesDB,** which has been down since 2026-10-07. | Fixed in #18 (Finnhub collector), which is open and needs a merge. |
| 4 | All-or-nothing publishing | One provider outage freezes the whole watchlist (M2). |
| 5 | The schema covers the four watchlist sources plus GDELT, which merged outside the ingestion contract. | User data and Confidence Score data are not modeled yet. Supabase Auth is in PR #22. GDELT needs follow-up rework (M6). |
| 6 | **CI fails on every branch, `main` included,** since PYSEC-2026-4320 was published against `certifi` 2026.6.17. | No PR can go green, including #18. Fix: `certifi` 2026.10.19 on `fix/certifi-cve` (locks recompiled on Linux per ADR 0001). |

### What "done" means

The database is finished when all of these hold:

1. **Complete model.** Every product data domain has a reviewed schema, migrations with tested downgrades, and constraint tests: watchlist sources, market caps, security identity, fundamentals, news and sentiment, events, Confidence Scores, and user data.
2. **In production.** It runs on a managed PostgreSQL 17 with point-in-time recovery, least-privilege roles, row-level security on user data, and secrets in a secret manager.
3. **The only source of truth.** The app reads everything through the API from the database. The legacy JSON path and `cron.py` are deleted.
4. **Unattended.** Daily scheduled runs, alerting (pipeline *and* database health), degraded publishing and rollback all work without anyone watching.
5. **Proven.** One restore drill has succeeded and been timed, a storage estimate for a year of data is recorded, user accounts can be deleted and exported, the security audit is signed, and the ERD, runbooks and ADRs are current.

**Out of scope, deliberately:** a data warehouse, streaming, an analytics or BI stack, a feature store for model training, multi-region, and real-time quotes. Each would be a new project with its own justification.

### Sizing: what this roadmap is built for

Perennial is pre-launch: a small team, one daily batch of a few thousand rows, few or no users yet. The 2026-10-20 rescope keeps everything that protects users and data, and cuts the parts sized for a company with SLAs and an on-call rotation:

| Kept as written | Made leaner | Added (production gaps) |
|---|---|---|
| Managed Postgres with point-in-time recovery; migrations with tested downgrades; least-privilege roles; `app` schema with RLS; API-only access | **M7 shadow:** a diff script over 3–5 trading days, not a fixed 2 weeks | **Account deletion and data export** (M5) |
| Quality checks and audited overrides (M1) | **M8 capacity:** a written estimate, not a simulated year; measure only if a source grows 10× | **Database observability:** slow queries, connections, disk growth, with alerts (M3) |
| Degraded publishing (M2) | **M8 recovery:** one timed restore drill before launch, then yearly, not quarterly | **Secret rotation runbook** for provider keys and the database password (M3) |
| Byte-identical replay for publications and scores | **M4 identity:** a plain alias table; no time-ranged identifiers; `figi` stays optional | **A cost ceiling** (D14) that anchors D1, D4 and D8 |
| Missed-run monitor and alerts (M3) | **Review:** the checker reviews every change; the auditor only migrations, roles, auth, secrets and logging | |

Re-evaluate the "leaner" column at launch, or as soon as there are paying users.

---

## 2. Design rules for everything that follows

These are the rules that make a nine-domain schema stay maintainable. Reviewers enforce them, and breaking one needs an ADR.

1. **One ingestion contract for every external source.** Fundamentals, news and GDELT plug into the same contract as Congress trades: a collector produces a validated outcome, the importer writes an immutable batch, and the run records lineage. No new source gets its own storage or retry machinery. Adding a source means a migration that extends the `source` CHECK vocabulary and adds the source's table, plus a collector and a validation schema. That is deliberate friction, because sources are a closed set.
2. **Derived data is a run too.** FinBERT sentiment and Confidence Scores are produced by derivation runs. Each records its inputs (`run_dependencies`) and its model or weights version. A derived row with no lineage is a bug.
3. **The app reads only published data.** API reads go through `current_publication`, or through serving views over pinned publications. The API never reads raw batch tables or "the latest" anything.
4. **Two ownership zones, one database.**
   - Schema `perennial` holds pipeline data. It is append-only and written by the pipeline role, with no per-user data.
   - Schema `app` holds user data. Row-level security is enabled on every table, and the API writes it on the user's behalf.
   - The pipeline never writes `app`. User data only references pipeline data by stable identifiers (`security_id`), never by batch.
5. **Changes only expand, then contract** (2026-10-08 roadmap §2.4). Every migration sets `lock_timeout`, has a tested downgrade, and updates grants in `provision_roles.py` in the same PR.
6. **Policy lives in code and is stored with its output.** Freshness limits, reuse limits, quality thresholds and score weights are typed settings. Each publication records the exact policy it was built under, so replay never depends on today's settings.
7. **Error text never crosses the database boundary.** Only fixed codes and safe counts are persisted, as today.
8. **Use relational tables for anything queried by key, and JSONB for what is served whole.** The watchlist output stays JSONB because the API serves it as one document. Scores and user data are relational, because they are queried per company and per user, with history.

---

## 3. Milestones

Each milestone ends at a gate that needs the checker's verification and Bryan's sign-off. The security auditor also signs gates that touch migrations, roles, auth, secrets or logging. Sizes are working days for the current rig at its current parallelism.

```
M0 Restore ──► M1 Correctness ──► M2 Resilient publishing ──► M3 Platform ──┬─► M5 App data + API ──► M7 Shadow and cutover ──► M8 Closeout
                                                                            └─► M4 Identity ──► M6 Score data ──────────────┘
```

**Critical path:** M0 → M1 → M2 → M3 → M5 → M7 → M8. M4 and M6 run beside it. If M6 isn't ready when M7 cuts over the watchlist, the Confidence Score cuts over later with its own short diff run.

### M0: Restore publishing (now, 2–3 days)

**Goal:** a full `run-pipeline` publishes again.

| Task | Owner | State |
|---|---|---|
| Merge PR #7 (legacy Senate Watcher) and #8 (legacy Finnhub) | — | **Done** 2026-10-14 |
| Remove Senate Stock Watcher from `collect_congress` and `fmp.py` (feature dropped) | — | **Done** (#16, landed via #19) |
| Branch cleanup: stranded stack landed (#19), merged branches deleted, auto-delete of head branches turned on | — | **Done** 2026-10-18 |
| **Merge #18: move `collect_insider` to Finnhub.** Includes the ticker mapping (`RKLB UQ` → `RKLB`, `BRK.B` → `BRK-B`, `GOOG` → `GOOGL`), skipping listings with no Form 4 filings, honoring Finnhub's rate-limit reset, and deleting the SecuritiesDB code | haohnguyen94-droid; Bryan reviews | Open, updated with `main` 2026-10-20, merges cleanly. CI is red only on the `certifi` audit (problem 6); goes green once `fix/certifi-cve` lands and #18 is updated again. Needs Bryan's review |
| **Fix the runner's missing settings.** `run_pipeline` and `collect_source` pass the startup `Settings` into every collector, so Congress gets its FMP key and insider its Finnhub key. Add a test that runs the pipeline through the CLI with keys present only in `Settings` | db-implementer | Assigned 2026-10-20 (`fix/runner-settings`) |
| **D4: FMP plan tier.** Free plan returns 402 on page 1 | Bryan | Decision needed |
| Record the provider switch: the first Finnhub batch will differ sharply from the last SecuritiesDB batch, so expect one recorded quality override when it first publishes | primary | When it goes live |

**Gate M0:** with synthetic fixtures, `run-pipeline` started through the CLI publishes with all four sources, using keys supplied only through `Settings`. No code path references SecuritiesDB.

### M1: Correctness and code quality (about 1 week)

The Phase 1/2 packets from `docs/work-items/phase-1-2-kickoff.md` that are **merged:** P2-B settings, P2-D session settings, P1-A FMP pagination, P1-B quality checks, P2-C lockfile.

**Still open from those packets:** P2-A structured logging (no `print()` in `src/`; dev-ops, in progress), P1-C provider contract-test harness (dev-sources; **built, in review** on `test/provider-contracts`), P2-E `legacy_*` renames and `Protocol` interfaces.

**Follow-ups to merged code. These must land before the first real publication.** The first four are **built and in review** as packet M1-A (`feat/m1-publication-events`, commit `7ed26ee`, 390 tests passing; migration id `publication_events_v1` is a placeholder, renumbered at merge per §4):

- **Move the quality audit out of `ranking_config`** (decision D12). P1-B merged storing its audit and override inside `publications.ranking_config`, which is a replay input. Add a `publication_audit` migration (the next free number at merge): one append-only `publication_events` table (`publication_id`, `kind` ∈ {`quality_passed`, `quality_override`, `degraded`, `rollback`}, `actor`, `reason`, `checks` JSONB, `created_at`). It also stores degraded markers (M2) and rollbacks (M3). `ranking_config` goes back to holding ranking inputs only. This is free until the first real publication and permanent after it.
- **Narrow overrides.** An override skips only the named quality checks, not the observation-date regression guard. Going backwards in time needs a rollback.
- **Fix the empty-baseline lockout.** When the previous batch had zero rows, the row-count check records `no_baseline` instead of failing every later batch.
- **Make publishing fail fast.** The session-wide `lock_timeout=5s` from P2-D also applies to the blocking `pg_advisory_xact_lock(170099)` in `publish()`. Switch to `pg_try_advisory_xact_lock` with a fixed `publication_in_progress` code, plus a test with two concurrent publishers.
- **Congress history window** (new decision D13). Both Congress paths drop trades older than 2 years before storage. If Hong's "keep all the data" is confirmed, store the full history FMP serves and apply the 2-year window in ranking, so the watchlist doesn't change.

**Gate M1** (old Gates 1 and 2 combined):
- three consecutive successful staging runs against live providers, which needs D3
- quality checks on, with the measured row counts, run times and retry rates recorded in `OPERATIONS.md`
- zero `print()` calls in `src/` outside `cron.py`
- the four follow-ups above merged before any real publication

### M2: Resilient publishing (about 1 week; moved up from old Phase 5)

**Goal:** one provider outage makes one source stale, not the whole watchlist.

- **Per-source policy in settings:** `required`, `max_reuse_age` and `max_age`. The recommended defaults are in decision D7.
- **The runner selects instead of aborting.** For each source, it uses the current run's batch if the collection succeeded. Otherwise it reuses the latest good batch, if that batch is within `max_reuse_age`. A required source with nothing usable still aborts the run.
- **Downstream runs use the upstream batches that were selected.** If Congress fails and yesterday's batch is reused, the insider collection is planned from that same reused batch, and the existing lineage check (`publication.py:102-110`) passes unchanged. That check doesn't need to be relaxed.
- **Degraded publications are marked.** A publication that reuses any batch gets a `degraded` event in `publication_events`, listing each reused source and its age. The ranking config records each source's as-of date, which it already does. The API (M5) exposes `degraded: true` and the per-source dates.
- **Health reports the new states.** `cli.py health` reports a degraded publication as SEV3 and a missed publication as SEV2.

**Gate M2:**
- a fault-injection test per source: fail it, confirm the publication is degraded and correct
- a test that a reused batch past its limit aborts the run
- replay of a degraded publication is byte-identical

### M3: Platform (about 1 week; old Phase 3)

Unchanged from old Phase 3 except for these points:

- **D1 hosting: effectively Supabase Postgres 17,** since auth is being built on Supabase Auth (Jaden's `codex/supabase-auth-resend`). One database holds both user and pipeline data, which avoids a cross-system join and a second backup story. Two things still need confirming before production:
  - the pipeline can use a direct or session-mode connection, because transaction pooling breaks advisory locks
  - point-in-time recovery is available on the chosen plan
- **Environments:** local (Docker PostgreSQL 17 for pipeline tests, plus the Supabase local stack for `app` and RLS tests), staging and prod.
- **Roles** extend `provision_roles.py`: `perennial_migrator`, `perennial_pipeline`, `perennial_api` (read-only on `perennial`, row-level-security-bound on `app`) and `perennial_readonly` for ad-hoc operator queries.
- **`cli.py rollback --to <publication_id>`** records a `rollback` event in `publication_events`. No new migration is needed.
- **Scheduling:** a one-shot scheduled job replaces the APScheduler daemon, with a missed-run monitor, scheduled health checks and alerts linked to runbooks.
- **Database observability** (added in the rescope): `pg_stat_statements` on, and alerts for slow queries, connection count near the plan limit and disk growth. On Supabase these are mostly dashboard settings plus a few alert rules; write them down in `OPERATIONS.md`.
- **Secret rotation runbook** (added): the steps to rotate each provider key and the database passwords without downtime, rehearsed once on staging.
- **Restore drill:** one timed point-in-time recovery plus logical-backup restore on staging. This is the pre-launch drill M8 relies on.

**Gate M3:** the old Gate 3 checklist for the pipeline, plus the observability alerts firing in a test and one successful, timed restore drill. Shadow comparison moves to M7.

### M4: Security identity and reference data (1–1.5 weeks; old Phase 5 item 3, moved forward)

**Why now:** the Confidence Score and GDELT both join on company identity, and the providers already disagree on tickers:
- `GOOG` vs `GOOGL`, and `BRK.B` vs `BRK-B`
- ARK's suffixed tickers such as `RKLB UQ`
- ADRs: TSM's Form 4 rows are priced per ordinary share, and one ADR is 5 shares

#18 handles the ticker cases with a mapping function inside `insider.py`. That's the right fix for now, but it's per-provider code, not data. M4 replaces it with stored identifiers.

GDELT's `0004` put `sector` and `industry` on `market_cap_observations`, which the final contract migration drops. M4 moves them, expand then contract: `company_profiles` gets the columns, a backfill copies them, GDELT reads from `company_profiles`, and the old columns go with `market_cap_observations` in the contract step.

Migration (security identity; numbered at merge):

| Table | Purpose |
|---|---|
| `securities` (existing) | Keeps `symbol` as the display ticker. Add `name`, `security_type` (common, ADR, ETF), `country`, `figi` (unique, nullable) and `is_active`. |
| `security_identifiers` | `(security_id, scheme, value)`, unique on `(scheme, value)`, with scheme ∈ {`ticker`, `figi`, `cusip`, `cik`}. Holds share-class and provider aliases. **Rescope:** no `valid_from`/`valid_to` yet. Add time ranges with an expand migration the first time a ticker rename actually matters. |
| `company_profiles` | One row per issuer: legal name, sector, industry and alias names. These feed the GDELT name-to-ticker lookup. |
| `market_caps` | Market caps become a tracked source with its own runs and batches (old Phase 5 item 2), replacing the side-table observations. Migrate `market_cap_observations` with expand then contract. |

- **Resolution:** importers resolve provider tickers through `security_identifiers`. Unresolved symbols fail with a fixed `unresolved_security` code instead of silently creating a new security. ARK suffixes and share-class aliases are seeded once and covered by tests.
- **Backfill:** fill `figi` and `cik` where OpenFIGI returns them, in batches outside the migration transaction. **Rescope:** `figi` stays optional; there is no mandatory-FIGI contract migration.

**Gate M4:** every symbol in the current publication resolves to exactly one security. The #18 alias cases (`RKLB UQ`, `BRK.B`, `GOOG`) are regression tests, and the mapping code in `insider.py` is replaced by stored identifiers.

### M5: App data model and read API (1–1.5 weeks; issues #2 and #3; runs in parallel with M4 after M3)

**Status:** Jaden is building on **Supabase Auth** (`codex/supabase-auth-resend`, now **PR #22**). That branch includes account, notification and OTP tables and four migrations. Reviewing it against this section is the M5 work. The requirements that matter most:
- user tables live in schema `app` with row-level security, not in `perennial`
- the pipeline role gets **no** write or `DELETE` grants on user tables; only the API role writes them
- migrations use the repo's numbering convention (see §4), not named revision ids such as `auth_supabase_v1`

Target shape, in schema `app`, with row-level security on every table:

| Table | Key columns and rules |
|---|---|
| `profiles` | `user_id` (PK, FK `auth.users`), `display_name`, `created_at`. Created by a trigger on `auth.users` insert, which this migration defines. |
| `user_preferences` | `user_id` (PK), `risk_profile`, `buckets` (enum array), `notifications` JSONB with a schema validated in the API. |
| `user_watchlists` | `id`, `user_id`, `name`, unique `(user_id, name)` |
| `user_watchlist_items` | `(watchlist_id, security_id)` PK, `added_at`. References `perennial.securities` by id. |

- **RLS policy:** every policy is `user_id = auth.uid()`. The API connects as `perennial_api` and, inside each request's transaction, sets the verified JWT claims (`set_config('request.jwt.claims', …, true)`) so `auth.uid()` resolves. The API also filters by user explicitly, so RLS is a second defense, not the only one. This answers decision D11.
- **Read API (FastAPI):**
  - `GET /v1/watchlist`: the current publication, with `ETag` set to the publication id, `ranking_version`, `degraded` and per-source as-of dates
  - `GET /v1/securities/{symbol}`
  - `/me`, preferences, and user-watchlist CRUD
  - The response schemas are versioned and documented in `docs/technical-specs/`.
- **Account deletion and export** (added in the rescope):
  - `DELETE /v1/me` removes the user's `app` rows and their Supabase Auth user in one flow. Foreign keys from `app` tables to the user are `ON DELETE CASCADE`. Pipeline data is never touched, because it holds nothing per user (design rule 4).
  - `GET /v1/me/export` returns the user's profile, preferences and watchlists as JSON.
  - Both are audited: who and when, never the exported content.
- **Seeds:** two users and three companies, per issue #2.
- **Tests:**
  - a cross-user isolation test for every table, run against the Supabase local stack
  - migration round-trip including the RLS policies
  - a test that `perennial_api` cannot write to `perennial`
  - a test that deleting a user leaves no `app` rows behind and changes no pipeline rows

**Gate M5:** the acceptance scenarios in issues #2 and #3 pass, and so do account deletion and export. The auditor signs off on RLS and the JWT trust boundary.

### M6: Confidence Score data (2–3 weeks; needs M4 and decision D10)

These are the new sources and derivations from `DECISIONS.md`. Each source follows design rule 1, and each derivation follows rule 2.

**GDELT is merged** (AalindCS, #17, 2026-10-20) as built: daily theme volume, tone and evidence articles, plus spike detection, in tables that don't go through the ingestion contract. Its design note (`docs/work-items/gdelt-design-note.md`) proposes the rework, which is now **follow-up work on `main`**, not a condition for merging:
- move `sector`/`industry` to M4's `company_profiles` (see M4)
- send GDELT through the ingestion contract, with an expand migration that adds batch lineage to the GDELT tables
- turn spike checks into a derivation run

Recommended answers to the note's four questions:
1. **Option A.** Extend the ingestion contract to sources that have no security, as its own PR before the GDELT tables.
2. **Wait for M4** rather than building a partial `company_profiles` on the GDELT branch.
3. **Mark GDELT non-required with a flag in the source vocabulary,** not a hard-coded list, so M2's per-source policy covers it.
4. **Missing GDELT days are gaps in the collector's coverage,** and the spike rule treats them as unknown, never as zero.

| Migration | Domain | Shape |
|---|---|---|
| fundamentals | Financial statements (FMP) | A new source, `fundamentals`: one row per `(batch, security, fiscal_period, statement, line_item)`, with values as `Numeric` and a currency. Restated periods appear as new batches, never as updates. |
| news and sentiment | Articles (Yahoo, Bing or news fetcher) and FinBERT output | Source `news_articles` stores metadata only: URL, canonical URL hash, outlet, published time, title and a content hash. **Full article text is not stored** (D8 terms of service, plus storage cost); it is fetched, scored and discarded. A derivation stores `article_sentiment` per `(run, article, security)` with `model_version`, label and score in [-1, 1]. |
| GDELT (from #17, reworked) | GDELT | Theme-day and article sources through the ingestion contract. Spike checks become a derivation. Company-level `event_company_impacts`, with match confidence and the resolution method, use M4's alias names. |
| scores | Confidence Score | A new publication kind. `publications` gets a `kind` column (`watchlist` or `score`, default `watchlist`), and `current_publication` gets `(kind, channel)` through expand then contract. A relational `company_scores` table holds `publication_id`, `security_id`, `score`, `score_version`, the three component values, the weights applied and the components that were missing and reweighted (`DECISIONS.md` rules 4–6). |

The rules are the same as for the watchlist:
- **Pinned and replayable:** score publications pin their batches and derivation runs, and replay byte-identically.
- **Smoothing is stored, not recomputed:** the 7-day sentiment smoothing is computed inside the derivation from pinned article batches.
- **Missing inputs are explicit:** a missing component is reweighted (rule 4), never silently treated as neutral.

**Gate M6:**
- a score publication replays byte-identically
- the breakdown endpoint `GET /v1/securities/{symbol}/score` returns the components, weights and version
- the score validation from D10 (for example, rank correlation with the Piotroski F-Score) has run and is recorded

### M7: Shadow, cutover and legacy retirement (about 1.5 weeks; old Phase 4, rescoped)

- **Shadow:** run both paths daily for **3–5 trading days**. A diff script compares the legacy JSON watchlist against the API, and every difference is fixed or explained, such as an intentional v2 deduplication. Extend the run only if differences keep appearing.
- **Cutover:** switch the app to the API behind a config flag, then watch for a few days. The flag stays until retirement, so rolling back is a config change.
- **Retirement:** delete the JSON writers, `scheduler/cron.py`, `legacy_*` functions, `market_cap_observations` (the contract step after M4), and the `paths.py` local-data plumbing. Fold the fetchers into one adapter per provider behind the `Protocol` interfaces from P2-E.

**Gate M7:** the app is served from production, the shadow differences are resolved, and the legacy code is gone. A search for `json.dump` in `src/services` returns nothing.

### M8: Hardening and closeout (about 3–4 days; the "done" gate)

- **Retention (D5):** the recommendation is now to **keep all data** (Hong). Raw payloads stay. If storage grows past budget, unpinned payloads move to object storage behind a content-hash pointer (`payload_location`, a migration at that point), and a replay test proves pinned publications are untouched.
- **Capacity (rescoped):** a written estimate per source (rows per day × 365 × row and index size), checked against real table sizes after the first month in production. Run a load simulation only if a source grows 10× or the estimate nears the plan limit. Partition nothing unless measurements require it.
- **Recovery (rescoped):** the M3 drill is the pre-launch proof. After launch, repeat it yearly and after any hosting change, against RPO ≤ 15 min and RTO ≤ 1 h (D6).
- **Documentation:** a final ERD in `docs/technical-specs/`, every alert linked to a runbook, an ADR for each decision in §5, and the README rewritten for the finished system.
- **Final audit:** checker and auditor review the whole schema and its roles, not just the diff.

**Gate M8:** every item in §1's definition of done is checked off. The database is complete.

---

## 4. Migration plan

**Numbers are assigned when a migration merges, not when it's written.** GDELT merged first and took `0004` and `0005`. `publication_events` (placeholder `publication_events_v1`) and auth (#22) both still need renumbering. Whoever merges next takes the next free number, starting at `0006`, and rebases onto it. CI (`test_migration_guard.py`) fails any PR that would create a duplicate id or a second head. All revision ids are sequential four-digit numbers; named ids such as `auth_supabase_v1` get renumbered at merge. Each migration still needs its design note first (§2.4 of the 2026-10-08 roadmap).

Expected order, by readiness:

| Order | Milestone | Kind | Contents |
|---|---|---|---|
| next | M1 | expand | `publication_events` audit table |
| then | M5 | expand | Supabase Auth account tables from Jaden's branch, after review: `app` schema, RLS, no pipeline grants |
| then | M4 | expand | security identity, `company_profiles` (takes GDELT's sector and industry), `market_caps` source |
| done | M6 | expand | `0004` GDELT `sector`/`industry` columns, `0005` GDELT tables (#17, merged 2026-10-20) |
| then | M6 | expand | GDELT follow-up: batch lineage through the ingestion contract |
| later | M6 | expand | fundamentals, news and sentiment, `publications.kind` and `company_scores` |
| later | M8 | expand | payload storage pointer, only if storage needs it |
| last | M7/M8 | **contract** | Drop `market_cap_observations` and `publication_market_caps`, and switch the `current_publication` PK to `(kind, channel)`. (`figi` stays nullable after the rescope.) |

Only the last one removes anything, and it runs only after the code that used those objects has been gone for a full release.

---

## 5. Decisions

**Open:**

| ID | Decision | Needed by | Recommendation |
|---|---|---|---|
| D3 | Staging provider keys: FMP, Finnhub, Nasdaq, ARK | M1 gate | Separate staging and prod keys |
| **D4** | **FMP plan tier** | **M0, now** | The free plan returns 402 on Senate page 1, so Congress can't complete. Choose a paid tier, or a different Congress endpoint or date-window query, recorded in an ADR |
| D6 | SLOs and on-call | M3 | As proposed: publish by 10:30 PT on 95% of trading days; RPO 15 min, RTO 1 h |
| D7 | Reuse limits for degraded publishing | M2 | Short interest 15 days; insider, ARK and Congress 3 days; market caps 3 days. Nothing may be reused past its `max_age` |
| D8 | Terms-of-service review | before M7 cutover | Finnhub's free tier (check redistribution), FMP, Nasdaq, Yahoo, GDELT |
| D10 | Confidence Score weights and validation | M6 | Accept 60/25/15 as `score-v1` with the Piotroski check. **Blocks M6** |
| D12 | Quality-check audit storage | before the first real publication | The `publication_events` table, not JSONB in `ranking_config` |
| D13 | Congress history window | M1 | Store everything FMP serves, and apply the 2-year window in ranking |
| **D14** | **Monthly cost ceiling** for hosting and data providers | M3 (before D1 is final) | Set one number. It decides the Supabase plan (point-in-time recovery is a paid add-on), the FMP tier (D4) and whether Finnhub's free terms (D8) are enough |

**Settled:**

| ID | Decision | Outcome |
|---|---|---|
| D1 | Hosting | Supabase Postgres 17, since auth is on Supabase. Still confirm session-mode connections and point-in-time recovery on the plan |
| D2 | Versions | Python 3.12, PostgreSQL 17 |
| D5 | Retention | Keep all data (Hong). Move unpinned payloads to object storage only if storage requires it |
| D11 | API data path | FastAPI is the only path to user data, with Supabase Auth JWTs and RLS as a second defense |
| — | Merge style | Merge commits; head branches are deleted automatically on merge |
| — | Migration numbering | Assigned at merge, enforced by CI |

---

## 6. Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Another free provider dies, as SecuritiesDB did | High | M2 degraded publishing limits the damage; the P1-C contract tests detect shape changes within a week; adapters sit behind `Protocol` interfaces, so swapping a provider is a contained change |
| Finnhub free-tier terms forbid redistribution | Medium | D8 before cutover; the adapter boundary keeps a paid swap contained |
| Supabase pooling breaks advisory locks | Medium | The pipeline uses a direct or session connection, as decided in D1; a test fails if the lock is taken through a transaction pooler |
| Entity resolution for GDELT is harder than estimated | High | M4 before M6; unresolved matches are stored with low confidence and excluded from scores rather than guessed |
| The score spec changes mid-build | Medium | D10 before M6 starts; versioned weights |
| Many contributors (sentiment, events, auth, fetchers) | Medium | CODEOWNERS per domain; every new source goes through rule 1 and the importer, so reviewers check one contract |
| Coordination state in `/private/tmp` lost again | Mitigated | Durable state goes in GitHub issues and `docs/`; worktrees go outside `/private/tmp` |
| Stacked PRs merged into a stack branch instead of `main` | Mitigated (happened 2026-10-14) | Head branches are deleted automatically on merge, so GitHub moves stacked PRs to `main` |
| Parallel branches claim the same migration number | Mitigated | CI migration guard; numbers assigned at merge |
| The free FMP tier blocks Congress collection | Happening now | D4 |

---

## 7. Timeline

Re-baselined on 2026-10-18.

| Weeks from now | Milestones |
|---|---|
| 0–1 | M0: merge #18, runner settings fix, D4. M1 follow-ups start |
| 1–2 | M1 follow-ups and logging; M2 |
| 2–3 | M3; review Jaden's auth PR #22 against M5; plan the GDELT follow-up rework |
| 3–5 | M4 and M5 in parallel; GDELT rework lands after M4 |
| 5–6.5 | Rest of M6; M7's short shadow run starts as soon as M5 is serving |
| 6.5–7 | M7 cutover; M8 |

About **7 weeks to a finished database** after the rescope, down from 9. The time saved comes mainly from M7's shorter shadow run and M8's lighter capacity and recovery work; the added items (account deletion, observability, secret rotation) cost a few days. The things most likely to stretch it:
- D4, which blocks a live publication now
- D3, D10 and D14
- entity-resolution quality in M4 and M6
