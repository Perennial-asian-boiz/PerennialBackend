# Perennial database: roadmap to completion

**Owner:** Bryan (product owner, final approver)
**Written:** 2026-10-13 · **Updated:** 2026-10-18, from `origin/main` at `12e4b1b`
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

Alembic head is still `0003`.

### What is wrong right now

| # | Problem | Consequence |
|---|---|---|
| 1 | **`run-pipeline` can't succeed.** Since #9, the runner calls each collector without passing `settings`, and `fmp.FMP_API_KEY` is now `None`. | Congress collection always ends with `missing_credential`, so no publication is possible, even with valid keys. |
| 2 | **The free FMP plan returns HTTP 402 on Senate page 1** (found in #18's live test). | Congress collection ends as `partial_collection`, which is rejected by design. Insider collection then has no upstream input. Blocked on decision D4. |
| 3 | **The database insider collector still calls SecuritiesDB,** which has been down since 2026-10-07. | Fixed in #18 (Finnhub collector), which is open and needs a merge. |
| 4 | All-or-nothing publishing | One provider outage freezes the whole watchlist (M2). |
| 5 | The schema covers only the four watchlist sources. | User data and Confidence Score data are not modeled yet. Supabase Auth (Jaden) and GDELT (#17) are in progress on contributor branches. |

### What "done" means

The database is finished when all of these hold:

1. **Complete model.** Every product data domain has a reviewed schema, migrations with tested downgrades, and constraint tests: watchlist sources, market caps, security identity, fundamentals, news and sentiment, events, Confidence Scores, and user data.
2. **In production.** It runs on a managed PostgreSQL 17 with point-in-time recovery, least-privilege roles, row-level security on user data, and secrets in a secret manager.
3. **The only source of truth.** The app reads everything through the API from the database. The legacy JSON path and `cron.py` are deleted.
4. **Unattended.** Daily scheduled runs, alerting, degraded publishing, rollback and retention all work without anyone watching.
5. **Proven.** A restore drill has measured RPO and RTO, a capacity test with one simulated year of data passes, the security audit is signed, and the ERD, runbooks and ADRs are current.

**Out of scope, deliberately:** a data warehouse, streaming, an analytics or BI stack, a feature store for model training, multi-region, and real-time quotes. Each would be a new project with its own justification.

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

Each milestone ends at a gate that needs one build-side approval, one review-side approval and Bryan's sign-off, as before. Sizes are working days for the current rig at its current parallelism.

```
M0 Restore ──► M1 Correctness ──► M2 Resilient publishing ──► M3 Platform ──┬─► M5 App data + API ──► M7 Shadow and cutover ──► M8 Closeout
                                                                            └─► M4 Identity ──► M6 Score data ──────────────┘
```

**Critical path:** M0 → M1 → M2 → M3 → M5 → M7 (including the fixed 2-week shadow period) → M8. M4 and M6 run beside it, and M6 has to land before M7's shadow period ends, or the Confidence Score cuts over in a second, smaller shadow.

### M0: Restore publishing (now, 2–3 days)

**Goal:** a full `run-pipeline` publishes again.

| Task | Owner | State |
|---|---|---|
| Merge PR #7 (legacy Senate Watcher) and #8 (legacy Finnhub) | — | **Done** 2026-10-14 |
| Remove Senate Stock Watcher from `collect_congress` and `fmp.py` (feature dropped) | — | **Done** (#16, landed via #19) |
| Branch cleanup: stranded stack landed (#19), merged branches deleted, auto-delete of head branches turned on | — | **Done** 2026-10-18 |
| **Merge #18: move `collect_insider` to Finnhub.** Includes the ticker mapping (`RKLB UQ` → `RKLB`, `BRK.B` → `BRK-B`, `GOOG` → `GOOGL`), skipping listings with no Form 4 filings, honoring Finnhub's rate-limit reset, and deleting the SecuritiesDB code | haohnguyen94-droid; Bryan reviews | Open; needs "Update branch" after #19. Tested combined with the stack: 402 passed |
| **Fix the runner's missing settings.** `run_pipeline` and `collect_source` pass the startup `Settings` into every collector, so Congress gets its FMP key and insider its Finnhub key. Add a test that runs the pipeline through the CLI with keys present only in `Settings` | primary | Not started; first implementation task |
| **D4: FMP plan tier.** Free plan returns 402 on page 1 | Bryan | Decision needed |
| Record the provider switch: the first Finnhub batch will differ sharply from the last SecuritiesDB batch, so expect one recorded quality override when it first publishes | primary | When it goes live |

**Gate M0:** with synthetic fixtures, `run-pipeline` started through the CLI publishes with all four sources, using keys supplied only through `Settings`. No code path references SecuritiesDB.

### M1: Correctness and code quality (about 1 week)

The Phase 1/2 packets from `docs/work-items/phase-1-2-kickoff.md` that are **merged:** P2-B settings, P2-D session settings, P1-A FMP pagination, P1-B quality checks, P2-C lockfile.

**Still open from those packets:** P2-A structured logging (no `print()` in `src/`), P1-C provider contract-test harness, P2-E `legacy_*` renames and `Protocol` interfaces.

**Follow-ups to merged code. These must land before the first real publication:**

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
- **Scheduling:** a one-shot scheduled job replaces the APScheduler daemon, with a missed-run monitor, scheduled health checks, alerts linked to runbooks, and a point-in-time recovery plus logical-backup restore drill.

**Gate M3:** the old Gate 3 checklist in full, for the pipeline. Shadow comparison is the one exception; it moves to M7.

### M4: Security identity and reference data (1–1.5 weeks; old Phase 5 item 3, moved forward)

**Why now:** the Confidence Score and GDELT both join on company identity, and the providers already disagree on tickers:
- `GOOG` vs `GOOGL`, and `BRK.B` vs `BRK-B`
- ARK's suffixed tickers such as `RKLB UQ`
- ADRs: TSM's Form 4 rows are priced per ordinary share, and one ADR is 5 shares

#18 handles the ticker cases with a mapping function inside `insider.py`. That's the right fix for now, but it's per-provider code, not data. M4 replaces it with stored identifiers. GDELT (#17) also needs `company_profiles` for its sector data, which is another reason to do M4 early.

Migration (security identity; numbered at merge):

| Table | Purpose |
|---|---|
| `securities` (existing) | Keeps `symbol` as the display ticker. Add `name`, `security_type` (common, ADR, ETF), `country`, `figi` (unique, nullable at first) and `is_active`. |
| `security_identifiers` | `(security_id, scheme, value, valid_from, valid_to)` with scheme ∈ {`ticker`, `figi`, `cusip`, `cik`}. Records ticker renames and share-class aliases over time. |
| `company_profiles` | One row per issuer: legal name, sector, industry and alias names. These feed the GDELT name-to-ticker lookup. |
| `market_caps` | Market caps become a tracked source with its own runs and batches (old Phase 5 item 2), replacing the side-table observations. Migrate `market_cap_observations` with expand then contract. |

- **Resolution:** importers resolve provider tickers through `security_identifiers`. Unresolved symbols fail with a fixed `unresolved_security` code instead of silently creating a new security. ARK suffixes and share-class aliases are seeded once and covered by tests.
- **Backfill:** fill `figi` and `cik` in batches, outside the migration transaction. Making `figi` mandatory is a separate contract migration after the backfill is verified.

**Gate M4:** every symbol in the current publication resolves to exactly one security with a FIGI. The Finnhub alias cases are regression tests.

### M5: App data model and read API (1–1.5 weeks; issues #2 and #3; runs in parallel with M4 after M3)

**Status:** Jaden is building on **Supabase Auth** (`codex/supabase-auth-resend`, no PR yet). That branch includes account, notification and OTP tables and four migrations. Reviewing it against this section is the M5 work. The requirements that matter most:
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
- **Seeds:** two users and three companies, per issue #2.
- **Tests:**
  - a cross-user isolation test for every table, run against the Supabase local stack
  - migration round-trip including the RLS policies
  - a test that `perennial_api` cannot write to `perennial`

**Gate M5:** the acceptance scenarios in issues #2 and #3 pass. The auditor signs off on RLS and the JWT trust boundary.

### M6: Confidence Score data (2–3 weeks; needs M4 and decision D10)

These are the new sources and derivations from `DECISIONS.md`. Each source follows design rule 1, and each derivation follows rule 2.

**GDELT is already largely built** (AalindCS, draft #17): daily theme volume, tone and evidence articles, plus spike detection. Its design note (`docs/work-items/gdelt-design-note.md` on the branch) proposes the rework this roadmap needs:
- drop its `sector`/`industry` migration and fold that into M4's `company_profiles`
- send GDELT through the ingestion contract
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

### M7: Shadow, cutover and legacy retirement (2 weeks shadow, plus 1 week watch; old Phase 4)

- **Shadow:** run both paths daily for at least 2 weeks. An automated job compares the legacy JSON watchlist against the API, and every difference becomes an issue that is either fixed or explained, such as an intentional v2 deduplication.
- **Cutover:** switch the app to the API behind a config flag, then watch for one week.
- **Retirement:** delete the JSON writers, `scheduler/cron.py`, `legacy_*` functions, `market_cap_observations` (the contract step after M4), and the `paths.py` local-data plumbing. Fold the fetchers into one adapter per provider behind the `Protocol` interfaces from P2-E.

**Gate M7:** the app is served from production, the shadow differences are resolved, and the legacy code is gone. A search for `json.dump` in `src/services` returns nothing.

### M8: Hardening and closeout (about 1 week; the "done" gate)

- **Retention (D5):** the recommendation is now to **keep all data** (Hong). Raw payloads stay. If storage grows past budget, unpinned payloads move to object storage behind a content-hash pointer (`payload_location`, a migration at that point), and a replay test proves pinned publications are untouched.
- **Capacity:** simulate 1 year of daily batches for every source. Measure publish time, replay time, backup and restore time, and table and index sizes. Partition a table only if the measurements require it; at current volumes none should.
- **Recovery:** a quarterly restore drill on staging, with measured RPO ≤ 15 min and RTO ≤ 1 h (D6).
- **Documentation:** a final ERD in `docs/technical-specs/`, every alert linked to a runbook, an ADR for each decision in §5, and the README rewritten for the finished system.
- **Final audit:** checker and auditor review the whole schema and its roles, not just the diff.

**Gate M8:** every item in §1's definition of done is checked off. The database is complete.

---

## 4. Migration plan

**Numbers are assigned when a migration merges, not when it's written.** Three branches currently declare `0004`: ours, GDELT's and auth's. Whoever merges next takes the next free number and renumbers in their rebase. CI (`test_migration_guard.py`) fails any PR that would create a duplicate id or a second head. All revision ids are sequential four-digit numbers; named ids such as `auth_supabase_v1` get renumbered at merge. Each migration still needs its design note first (§2.4 of the 2026-10-08 roadmap).

Expected order, by readiness:

| Order | Milestone | Kind | Contents |
|---|---|---|---|
| next | M1 | expand | `publication_events` audit table |
| then | M5 | expand | Supabase Auth account tables from Jaden's branch, after review: `app` schema, RLS, no pipeline grants |
| then | M4 | expand | security identity, `company_profiles` (takes GDELT's sector and industry), `market_caps` source |
| then | M6 | expand | GDELT tables through the ingestion contract (#17, reworked) |
| later | M6 | expand | fundamentals, news and sentiment, `publications.kind` and `company_scores` |
| later | M8 | expand | payload storage pointer, only if storage needs it |
| last | M7/M8 | **contract** | Make `securities.figi` NOT NULL, drop `market_cap_observations` and `publication_market_caps`, and switch the `current_publication` PK to `(kind, channel)` |

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
| 2–3 | M3; review Jaden's auth branch and GDELT #17 against M5 and M6 |
| 3–5 | M4 and M5 in parallel; GDELT rework lands after M4 |
| 5–7 | Rest of M6; M7's shadow period starts as soon as M5 is serving |
| 7–9 | M7 cutover and watch; M8 |

About **9 weeks to a finished database.** The things most likely to stretch it:
- D4, which blocks a live publication now
- D3 and D10
- the fixed 2-week shadow period
- entity-resolution quality in M4 and M6
