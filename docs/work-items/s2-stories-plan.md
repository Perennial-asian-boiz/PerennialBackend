# Sprint 2 stories: plan (S2-01, S2-02, S2-03)

**Status:** roadmap and task list. Docs only; no code in this PR.
**Date:** 2026-10-28. **Branch:** `docs/s2-stories-plan` from `origin/main` at `70dbafe`.
**Sprint window:** Oct 21 – Nov 9 2026, demo Mon Nov 9 (Sprint 2 Demo Verification Matrix).
**Builders:** Bryan and Hong. Every task below is **un-assigned**; the two of them split the list,
and Bryan takes the tasks marked **DBF** (database functions).

**Inputs**
- `output/pdf/final-Perennial_Sprint2_Demo_Verification_Matrix.pdf`: the three stories and their acceptance criteria, verbatim below
- `docs/technical-specs/DECISIONS.md`, "Confidence Score composition" (score-v1: fundamentals 60%, sentiment 25%, event exposure 15%)
- `docs/roadmap/database-roadmap.md` (M6 scores, `company_scores` sketch), `docs/work-items/pattern-evaluation-design.md`
- main at `70dbafe`: fundamentals (#66), news ingestion (#69), sentiment scripts (#68), read API (#50), summaries (#59–#63, #74)

**Decisions taken for this plan (Bryan, 2026-10-28)**
1. **Second score component.** S2-01 needs fundamentals **plus one other component**. Sentiment comes from the Sprint 1
   carryover (Cohen's S1-04 article scoring, Aalind's S1-05 credibility-weighted roll-up) through the contract in §4.
   If no producer writes that table by **Mon Nov 2**, Bryan and Hong build the minimal fallback (JOB-7). Event exposure
   stays a **missing** component in score-v1 (its weight is redistributed); no event rule is designed in Sprint 2.
2. **Explanations** are deterministic templates filled from stored components. No model, no paid API.
3. **Interim pointer table** `current_scores` instead of the roadmap's `publications.kind` expand/contract; folded in later.
4. **Separate route** `GET /v1/securities/{symbol}/score`, so `/v1/securities/{symbol}` and its OpenAPI snapshot stay untouched.

---

## 1. The stories (verbatim from the matrix)

**S2-01 View Company Confidence Score** (7 pts). *As a User, I want to view a company confidence score so that I can compare
investment opportunities.* Rules follow DECISIONS.md "Confidence Score composition" (score-v1). Event exposure has no 0–100
conversion yet, so it is treated as a missing component until one is defined.
- AC 1 (Happy Path): Given a company with sufficient data (a fundamentals score plus at least one other component), when its page opens, then a Confidence Score from 0 to 100, its as-of date and its score version are shown.
- AC 2 (Calculation): Given the same stored inputs and score-v1 weights, when the score is calculated twice, then the stored score is identical.
- AC 3 (Missing Inputs): Given a company with no event coverage, when the score is calculated, then the event weight is redistributed across the remaining components and the score is stored with that component listed as missing.
- AC 4 (Empty State): Given a company without sufficient data, when its page opens, then it shows "Score unavailable" rather than an invented score.

**S2-02 View Confidence Score Breakdown** (7 pts). *As a User, I want to view the confidence score breakdown so that I can
understand how each component contributes.* Depends on S2-01's stored components and weights.
- AC 1 (Happy Path): Given a company score, when its breakdown opens, then the fundamentals, sentiment and event components are each shown with their 0–100 value, the weight applied and their contribution.
- AC 2 (Consistency): Given a breakdown, when its contributions are added up, then they equal the displayed score within ±0.5 points.
- AC 3 (Traceability): Given a component, when its details are viewed, then its input as-of date and score version are shown.
- AC 4 (Empty State): Given a missing component, when the breakdown opens, then it is labeled "Unavailable" and states that its weight was redistributed under S2-01's missing-input rule.

**S2-03 View Why Perennial Recommends This Company** (7 pts). *As a User, I want to see why Perennial recommends a company so
that I can understand the supporting signals.* A "recommended company" is one on the current consensus list with a Confidence
Score of at least 60.
- AC 1 (Happy Path): Given a recommended company, when its explanation opens, then it shows up to 3 main supporting signals in plain language.
- AC 2 (Traceability): Given an explanation, when its supporting details are viewed, then each factual claim links to stored source data and an as-of date.
- AC 3 (Consistency): Given an explanation, when it is compared with the score breakdown, then every claim cites a breakdown component or stored record, and no claim is made about a component marked unavailable.
- AC 4 (Reproducibility): Given an unchanged score and inputs, when the explanation is requested again, then the stored explanation is returned instead of being regenerated.
- AC 5 (Empty State): Given insufficient supporting data, when an explanation is requested, then the page states that an explanation is unavailable.

---

## 2. What exists on main today

Paths: `$BE` = `development/backend`, `$DB` = `development/database`.

- **Fundamentals** (#66, `fundamentals-v1`): `$BE/src/services/fundamentals/{edgar.py,scoring.py,job.py}`. SEC EDGAR companyfacts
  (`SEC_USER_AGENT`). `fundamental_scores.score` is already 0–100. `scoring.combine()` redistributes a missing measure's weight;
  `breakdown()` and `key_reasons()` exist but nothing stores or serves them. Runs only via
  `python -m src.pipeline.scheduler --once --job fundamentals`; not in the cron in `scheduler.main()`.
- **News** (#69, migration `0017`): `news_articles` (headline, outlet, published_at, no body text, no sentiment), `news_coverage`;
  `--job news`, last 7 days per consensus member.
- **Sentiment** (#68): scripts under `$BE/sentiment/`, outside `src/`: FinTwitBERT over social posts into JSON files on a 1–10
  scale, equal-weighted. **No database table, no credibility weights, no scheduler hook.** This is the S1-04/S1-05 carryover.
- **GDELT**: theme-level volume, tone and spikes only; `theme_sector_map.json` has empty sector lists. No ticker link, so event
  exposure cannot be scored yet.
- **Scores**: nothing. No `company_scores`, no blend code, no score route. `dashboard.py`'s `confidence_score` is always null live.
- **Read API** `$BE/src/api/read.py`: `/v1/watchlist`, `/v1/securities/{symbol}`, `/v1/market-status`. `_security()` selects explicit
  columns because `perennial_api`'s grants are column-level (`$DB/ops/provision_roles.py`: `API_TABLES`, `API_COLUMNS`; the
  fundamentals and news tables are `IMMUTABLE` pipeline tables the API cannot read). OpenAPI snapshot
  `$BE/tests/fixtures/api/openapi_v1.json` (regenerate with `UPDATE_API_SNAPSHOT=1`). All `/v1` reads use the `read` rate bucket.
- **Migrations**: head `0017`; `YYYYMMDD_NNNN_slug.py`, ids assigned at merge, single head enforced by CI; each extends the `source`
  CHECK through `_vocabulary()`, sets `lock_timeout`, and its downgrade refuses while lineage exists. Release path:
  `provision_roles.py --phase bootstrap` → `alembic upgrade head` as `perennial_owner` → `--phase grants`.
- **Frontend** (`PerennialFrontEnd`): `src/pages/Company.tsx` at `/company/:symbol` renders `/v1/securities/{symbol}` (name, sector,
  summary) through `src/api/company.ts`.

---

## 3. Architecture

### 3.1 Data (migration `0018`)

| Table | Purpose | Writer |
|---|---|---|
| `perennial.ticker_sentiment` | The **sentiment contract** (§4). One row per `(batch, security)`. | Cohen/Aalind's roll-up, or the fallback job |
| `perennial.company_scores` | One row per `(batch, security)`: `score`, `score_version`, `status`, `unavailable_reason`, `missing[]`, `recommended`, `watchlist_publication_id`; per component: value, nominal weight, effective weight, contribution, input as-of, input version, record reference. | `--job scores` |
| `perennial.score_explanations` | One row per `(batch, security)`: `status`, `template_version`, `claims` JSONB, `input_digest`. | `--job scores` |
| `perennial.current_scores` | `(channel PK, batch_id, promoted_at)`, mirroring `current_publication`. **The API reads only through this pointer.** | `--job scores` |

### 3.2 score-v1 (pure functions, `$BE/src/services/scores/scoring.py`, `Decimal` only)

- Frozen `ScoreWeights(fundamentals=0.60, sentiment=0.25, event_exposure=0.15, version="score-v1")` with `envelope()` stored in the
  run envelope, like `ScoreRule.envelope()`. Component order is fixed: fundamentals, sentiment, event_exposure.
- Inputs: fundamentals = `fundamental_scores.score`; sentiment = `(value + 1) × 50` quantized to 0.1 (DECISIONS rule 1); event
  exposure = always missing in v1.
- **Sufficiency** (S2-01 AC1/AC4): fundamentals present **and** at least one other component. Otherwise `status = 'unavailable'`,
  `unavailable_reason ∈ {no_fundamentals, only_fundamentals}`, `score NULL`.
- **Redistribution** (AC3): `eff_w_i = w_i / Σ w_present` quantized 0.0001; `contribution_i = value_i × eff_w_i` quantized 0.01;
  `score = Σ contributions` quantized 0.1. With event missing: 0.7059 / 0.2941; `missing = ["event_exposure"]`.
- **Determinism** (AC2): Decimal arithmetic, sorted securities, inputs pinned through `run_dependencies`. If the dependency ids
  equal the last successful scores run's, the run is skipped and the pointer stays (the fundamentals job's existing pattern).
- **±15 sentiment cap** (DECISIONS rule 2) holds by construction in v1: the largest swing is 0.2941 × 50 = 14.7, and fundamentals
  is mandatory. Documented, no cap code. Revisit when event exposure arrives.
- `recommended = status == 'ok' AND score ≥ 60 AND security ∈ current production watchlist publication`
  (store the `watchlist_publication_id` it was judged against).

### 3.3 Explanations (`explain.py`, extends `key_reasons()`)

A claim is `{order, text, component, measure, record: {table, id, batch_id}, as_of, value}`. Only components not in `missing[]`
produce claims; candidates are ordered fundamentals measures by points descending, then sentiment; the first three are kept;
`template_version = "explain-v1"`. Generated only when `recommended`; otherwise `status = 'not_recommended'`.
`input_digest` = SHA-256 of the canonical JSON of the score row plus the referenced record values; an equal digest carries the
stored row forward (AC4). Zero candidate claims → `status = 'unavailable'`, `reason = 'insufficient_supporting_data'` (AC5).

### 3.4 API: `GET /v1/securities/{symbol}/score`

`read` bucket, `Cache-Control: no-cache`, ETag as `/v1/watchlist`. `404 unknown_security` as today; **200 with
`score.status: "unavailable"`** when no scored row exists. One request serves all three stories.

Full score (the only score-v1 "full" shape, since event exposure is always missing):
```json
{"api_version":"v1","symbol":"MSFT",
 "score":{"status":"ok","value":74.3,"score_version":"score-v1","as_of":"2026-11-05T17:02:11Z","recommended":true,
   "components":[
     {"name":"fundamentals","status":"ok","value":78.0,"nominal_weight":0.60,"weight":0.7059,"contribution":55.06,
      "input_as_of":"2025-06-30","input_version":"fundamentals-v1","record":{"table":"fundamental_scores","id":812}},
     {"name":"sentiment","status":"ok","value":65.5,"nominal_weight":0.25,"weight":0.2941,"contribution":19.26,
      "input_as_of":"2026-11-04","input_version":"sentiment-rollup-v1","record":{"table":"ticker_sentiment","id":77}},
     {"name":"event_exposure","status":"unavailable","value":null,"nominal_weight":0.15,"weight":0.0,"contribution":0.0,
      "input_as_of":null,"input_version":null,"record":null,"note":"weight redistributed to the remaining components"}],
   "missing":["event_exposure"]},
 "explanation":{"status":"ok","template_version":"explain-v1","as_of":"2026-11-05T17:02:11Z",
   "claims":[
     {"order":1,"text":"Revenue grew 18% a year over the last 3 years.","component":"fundamentals",
      "record":{"table":"fundamental_scores","id":812},"as_of":"2025-06-30"},
     {"order":2,"text":"Keeps 42 cents of every sales dollar as operating profit.","component":"fundamentals",
      "record":{"table":"fundamental_scores","id":812},"as_of":"2025-06-30"},
     {"order":3,"text":"News coverage over the last 7 days leaned positive (12 articles).","component":"sentiment",
      "record":{"table":"ticker_sentiment","id":77},"as_of":"2026-11-04"}]}}
```

Score unavailable:
```json
{"api_version":"v1","symbol":"XYZ",
 "score":{"status":"unavailable","reason":"only_fundamentals","value":null,"score_version":"score-v1",
          "as_of":"2026-11-05T17:02:11Z","recommended":false,"components":[],"missing":["sentiment","event_exposure"]},
 "explanation":{"status":"unavailable","reason":"score_unavailable","claims":[]}}
```

Explanation states with a score present: `{"status":"not_recommended","claims":[]}` or
`{"status":"unavailable","reason":"insufficient_supporting_data","claims":[]}`.

---

## 4. Sentiment dependency

**Contract.** Migration `0018` creates `perennial.ticker_sentiment`; the producer's own `article_sentiment` migration can be `0019`.

| Column | Type / rule |
|---|---|
| `id`, `batch_id`, `security_id`, `row_number` | as every ingestion-contract table; source `ticker_sentiment`, a derived run with `run_dependencies` on the `news_articles` and `article_sentiment` batches |
| `value` | `Numeric(5,4)`, CHECK −1..1 |
| `article_count` | `Integer`, CHECK ≥ 1 |
| `window_start`, `window_end` | `Date`; `window_end` is the component's as-of |
| `model_version` | e.g. `ProsusAI/finbert@<rev>` |
| `source_weights_version` | version of the committed source-weight table |
| `method` | `credibility_weighted_mean` |
| `rollup_version` | `sentiment-rollup-v1` |
| | `UNIQUE(batch_id, security_id)`; **no row = no coverage** |

Freeze this with Cohen and Aalind by **Oct 29**, so S2-01 reads it unchanged whoever produces it.

**Cutoff, Mon Nov 2.** Is a PR on `main` (or review-ready) writing `ticker_sentiment` through `import_collection`, with DB
tests passing? If not, start JOB-7 that day.

**Fallback (JOB-7).** ProsusAI/finbert (news-trained, the model DECISIONS.md names) rather than the repo's tweet-trained
FinTwitBERT. About 440 MB; CPU inference on roughly 1k headlines a day takes under a minute. `s = P(pos) − P(neg)` over
`news_articles.headline` for the last 7 days (body text is never stored). Roll-up `value = Σ w(outlet)·s / Σ w(outlet)` from a
committed, versioned `source_weights.json` whose `default` is the lowest listed weight (for unknown outlets). torch and
transformers go in an optional dependency extra and are imported lazily inside the job only; tests use a fake scorer so CI
downloads nothing. The model runs at ingestion only, never per request.

---

## 5. Task list (un-assigned; **DBF** = database function)

### (a) Database

| ID | Task | ACs | Depends | Size | DBF |
|---|---|---|---|---|---|
| DB-1 | Migration `$DB/migrations/versions/2026110X_0018_company_scores.py`: extend the `source` CHECK via `_vocabulary()` (`ticker_sentiment`, `company_scores`, `score_explanations`); create the 4 tables with CHECKs (score 0–100, sentiment value −1..1, weights 0..1, `status IN (...)`, `missing <@ ARRAY[...]`), `UNIQUE(batch_id, security_id)`, security indexes; `SET LOCAL lock_timeout='5s'`; downgrade refuses while lineage exists; single head | all | — | L | yes |
| DB-2 | Models in `$BE/src/db/models.py`: 4 `Table`s, `SCORE_COMPONENTS`, `SCORE_STATUSES`, `EXPLANATION_STATUSES`, `SOURCE_TABLES.update(...)`, `claims` as JSONB | all | DB-1 | M | yes |
| DB-3 | Schemas in `$BE/src/ingestion/schemas.py` for the 3 new sources (pattern: `FundamentalScore`) so `import_collection` validates rows | 01-AC2 | DB-2 | M | yes |
| DB-4 | Grants in `$DB/ops/provision_roles.py`: `IMMUTABLE += (ticker_sentiment, company_scores, score_explanations)`, `MUTABLE += (current_scores,)`, `API_TABLES += (current_scores,)`, `API_COLUMNS["company_scores"]` and `["score_explanations"]` listing every served column | all | DB-1 | S | yes |
| DB-5 | Grant and owner tests: extend `test_owner_migrations.py`; `test_read_api_column_grants.py` (API reads the listed columns only; cannot read `fundamental_scores` or `ticker_sentiment`; cannot write) | — | DB-4 | M | yes |
| DB-6 | Staging deploy checklist entry: `--phase bootstrap` → `alembic upgrade head` as `perennial_owner` → `--phase grants`; `\dp perennial.company_scores` check. Order matters (decision 85: grants before upgrade pass silently) | — | DB-1..4 | S | yes |

### (b) Pipeline and jobs

| ID | Task | ACs | Depends | Size | DBF |
|---|---|---|---|---|---|
| JOB-1 | `$BE/src/services/scores/scoring.py`: `ScoreWeights`, `normalize_sentiment()`, `blend()` → `ScoreResult` (sufficiency, redistribution, `missing[]`), `breakdown(row)` reproducing contributions from a stored row. Pure, no I/O | 01-AC1..4, 02-AC1/2/4 | — | M | no |
| JOB-2 | `$BE/src/services/scores/job.py::derive_scores(engine)`, shaped like `fundamentals/job.py::derive_scores`: `source_lock`, latest successful `fundamental_scores` and `ticker_sentiment` runs, current watchlist publication id, `begin_run(dependencies=…)`, skip when the dependency triple is unchanged, `import_collection` of `company_scores` rows with envelope = weights + input batch ids, promote `current_scores` | 01-AC1..4 | DB-2/3, JOB-1 | L | yes |
| JOB-3 | Sentiment contract reader `read_ticker_sentiment(conn, batch_id)` and the as-of rule (`window_end`); no batch → the component is missing for every security (→ `only_fundamentals`) | 01-AC1/4 | DB-2 | S | yes |
| JOB-4 | `$BE/src/services/scores/explain.py`: `claims_for(score_row, fundamentals_row, sentiment_row)`, templates generalised from `key_reasons()`, fixed order, max 3, skip `missing[]`, `input_digest()`. Pure | 03-AC1/2/3/5 | JOB-1 | M | no |
| JOB-5 | `derive_explanations(engine)`: a second derived run depending on the scores batch; digest carry-forward (03-AC4); writes `score_explanations` | 03-AC4/5 | JOB-2, JOB-4 | M | yes |
| JOB-6 | Scheduler/CLI: `--job scores` in `$BE/src/pipeline/scheduler.py` (= derive_scores → derive_explanations); add `fundamentals`, `news`, `sentiment`, `scores` to the cron in `main()` **or** document the manual order for the demo | all | JOB-2/5 | S | no |
| JOB-7 | **Fallback only** (§4): `$BE/src/services/sentiment/{model.py,rollup.py,job.py}`, `--job sentiment`, `source_weights.json`, optional dependency extra | 01-AC1 | DB-1 | L | partly |
| JOB-8 | Coverage probe (read-only SQL) on staging: consensus members with a `fundamental_scores.score`, and with a `ticker_sentiment` row; paste the counts here; pick demo tickers from the covered set | — | staging | S | yes |

### (c) API

| ID | Task | ACs | Depends | Size |
|---|---|---|---|---|
| API-1 | Pydantic schemas in `read.py` beside `Security`: `ScoreComponent`, `Score`, `Claim`, `Explanation`, `ScoreResponse` | all | — | S |
| API-2 | `_score(conn, security_id)`: `current_scores` → `company_scores` (explicit columns) → `score_explanations` for that batch; `get_score()` at `/securities/{symbol}/score`, `_security()` for the 404, `CanonicalJSONResponse`, ETag; contributions via `scoring.breakdown()` | all | API-1, JOB-1, DB-4 | M |
| API-3 | Confirm the route is in the `read` bucket (`rate_limit.py`); add it to the route-list tests | — | API-2 | S |
| API-4 | OpenAPI snapshot regeneration; `docs/technical-specs/API.md` section after `/v1/securities` with the JSON examples above. **Same PR as API-2**, or CI fails | — | API-2 | S |
| API-5 | `test_read_api.py` additions: 200 full, 200 unavailable, 404, ETag, as the `perennial_api` login | 01-AC4 | API-2, DB-5 | M |

### (d) Frontend (`PerennialFrontEnd`)

| ID | Task | ACs | Depends | Size |
|---|---|---|---|---|
| FE-1 | `src/api/score.ts` typed client and types mirroring `ScoreResponse` | all | API-4 (contract frozen) | S |
| FE-2 | `ScoreCard` in `Company.tsx`: value, as-of, version, "Recommended" chip; "Score unavailable" state | 01-AC1/4 | FE-1 | M |
| FE-3 | `ScoreBreakdown` table: component, value, effective weight (nominal in a tooltip), contribution, input as-of, version; a missing row reads "Unavailable — weight redistributed to the other components"; footer sum against the score | 02-AC1..4 | FE-1 | M |
| FE-4 | `WhyPanel`: up to 3 claims, each with "source: <table> · as of <date>"; states `not_recommended`, `unavailable`, `score_unavailable` | 03-AC1/2/5 | FE-1 | M |
| FE-5 | Loading and error handling (404 vs 200-unavailable vs network); no layout shift with the summary section | 01-AC4 | FE-2..4 | S |
| FE-6 | `tests/score.test.mjs` (`node --test`): contribution sum within ±0.5, unavailable mapping, claim guards; `pages.yml` stays green | 02-AC2 | FE-1 | S |

### (e) Backend tests (written first, team practice)

| ID | File | Key cases | ACs |
|---|---|---|---|
| TEST-1 | `$BE/tests/test_scores_scoring.py` | hand-calculated blend (78, 65.5 → 74.3); event missing → 0.7059/0.2941 and `missing`; fundamentals-only → `only_fundamentals`; no fundamentals → `no_fundamentals`; `normalize_sentiment(−1/0/1)` → 0/50/100; Σ contributions within ±0.5 over a sweep; envelope stable; largest sentiment swing < 15 | 01-AC2/3/4, 02-AC2 |
| TEST-2 | `$BE/tests/test_scores_job.py` (DB) | twice on the same batches → skipped, pointer unchanged, identical rows; a new fundamentals batch → new scores batch, pointer moves; `run_dependencies` carry the 3 ids; `missing_upstream` without fundamentals | 01-AC2/3 |
| TEST-3 | `$BE/tests/test_scores_explain.py` | ≤ 3 claims; fixed order; no claim on a `missing` component; every claim has `record` and `as_of`; equal digest → carry-forward; not recommended → `not_recommended`; zero candidates → `insufficient_supporting_data` | 03-AC1..5 |
| TEST-4 | `test_read_api.py`, `test_read_api_column_grants.py` | 200 full, 200 unavailable, 404, ETag, snapshot; the API role cannot read `fundamental_scores` | 01-AC1/4 |
| TEST-5 | `test_owner_migrations.py`, `test_migration_guard.py` | 0018 up and down on a fresh DB; downgrade refuses with lineage | — |
| TEST-6 | `$BE/tests/test_sentiment_rollup.py` (fallback only) | weighted mean with the default weight; window bounds; fake scorer fixture | 01-AC1 |

### (f) Docs and ops

| ID | Task | Size |
|---|---|---|
| DOC-1 | `DECISIONS.md`: score-v1 Proposed → Accepted (sufficiency rule, event missing in v1, cap by construction, interim pointer table); a new entry for the ticker-sentiment contract | S |
| DOC-2 | This plan, plus GitHub issues for S2-01, S2-02 and S2-03 | M |
| DOC-3 | `OPERATIONS.md`: run order `fundamentals → news → sentiment → scores`, environment (`SEC_USER_AGENT`, Finnhub key), rollback = re-point `current_scores` | S |
| DOC-4 | Staging deploy and demo script (§7) | S |
| DOC-5 | Point `pattern-evaluation-design.md` ("left for M6") and `database-roadmap.md` (scores row) at the interim pointer | S |

---

## 6. Sequence, Oct 28 → Nov 9

| Date | Milestone | Lane A (DB functions) | Lane B |
|---|---|---|---|
| Oct 28–29 | **Contracts frozen** (sentiment table, JSON shape) with Cohen, Aalind and the frontend | DB-1 draft | JOB-1 + TEST-1 (pure, no DB) |
| Oct 30 – Nov 1 | Migration PR open | DB-2..5, TEST-5 | JOB-4 + TEST-3, API-1 |
| Nov 2 | **Migration merged; sentiment cutoff check** | DB-6 on staging, JOB-8 probe | decide and, if needed, start JOB-7 |
| Nov 2–4 | Jobs | JOB-2/3/5, TEST-2 | JOB-7 if needed, else API-2/3, TEST-4 |
| Nov 4–5 | **Jobs on staging**, coverage recorded | JOB-6 | API-4/5, FE-1 |
| Nov 5–7 | **API live on staging; frontend** | DOC-3/4, review the API PR | FE-2..6 |
| Nov 8 | **Demo rehearsal** against staging | both | both |
| Nov 9 | Demo | | |

Parallel-safe: DB-1..5 with JOB-1/JOB-4; JOB-2/5 with API-1..3 (API tests insert fixture rows directly); FE with DOC.
Chokepoints: the DB-1 merge gates JOB-2 and API-5; API-4 (the snapshot) gates FE-1.

---

## 7. Verification and demo checklist

| AC | Check |
|---|---|
| 01-AC1 | `GET /v1/securities/MSFT/score` → `status: ok`, `value`, `as_of`, `score_version`; the card shows all three |
| 01-AC2 | Run `--job scores` twice; `current_scores` unchanged; rows identical (TEST-2) |
| 01-AC3 | `SELECT missing, weights FROM company_scores` → `{event_exposure}`, 0.7059/0.2941; the API's `missing` matches |
| 01-AC4 | A ticker without a `ticker_sentiment` row → `status: unavailable, reason: only_fundamentals`; the page shows "Score unavailable" |
| 02-AC1 | The breakdown table shows value, weight and contribution per component |
| 02-AC2 | `abs(Σ contribution − value) ≤ 0.5` (TEST-1, FE-6); visible footer |
| 02-AC3 | Each row shows `input_as_of` and `input_version` |
| 02-AC4 | The `event_exposure` row reads "Unavailable — weight redistributed" |
| 03-AC1 | A recommended ticker → up to 3 plain-language claims |
| 03-AC2/3 | Each claim has `record.table/id` and `as_of`; no claim's component is in `missing[]` (TEST-3) |
| 03-AC4 | Re-run with unchanged inputs → the same `score_explanations` row is served |
| 03-AC5 | A fixture with zero candidate claims → `explanation.status: unavailable`; the page says so |

---

## 8. Risks and open decisions

- **Sentiment carryover slips** past Nov 2: JOB-7 costs about two days and adds torch to the dependency set. The Oct 29 contract
  freeze means either producer fits without changing S2-01.
- **Fundamentals coverage is unknown.** Banks, IFRS filers and new listings may lack two of the three measures. Run JOB-8 right
  after DB-6 and pick demo tickers from the covered set.
- **Decision 85.** Grants run before the upgrade pass silently; deploy strictly bootstrap → upgrade → grants.
- **Cron.** `fundamentals` and `news` are not in `scheduler.main()`; for the demo, run the jobs by hand in order. Adding them to
  cron (JOB-6) needs `SEC_USER_AGENT` and the Finnhub key on the worker.
- **FMP terms (decision 3).** Nothing served by these stories comes from FMP (EDGAR, Finnhub headlines, model output). Keep it so.
- **Headline-only sentiment** is weaker than body text; the component carries its version so later readers can tell.
- **Open:** pointer table vs `publications.kind` (pointer for now); whether `recommended` should also require bucket membership;
  minimum `article_count` (1 in v1).

---

## Appendix: Sprint 1 carry-over housekeeping (Bryan's stories)

All three (S1-01, S1-11, S1-12) were counted complete at the Oct 26 review. Small follow-ups remain:

- **S1-01 AC2:** the hosted two-account check (deploy checklist §7) has no recorded run; run it once and record the status codes.
- **S1-01 AC4:** the frontend CI has no post-build provider-host denylist step; add one to `pages.yml` (spec: S1-AC4 in the S1 plan).
- **S1-01 AC1/AC2 wording** (decisions 59, 60) and ADR 0005 live only on the planner's local `docs/s1-stories-plan` branch; land them.
- **S1-11:** close frontend PR #8 (superseded by #9).
- **S1-12 AC2:** the limit is grade 13 (decision 88, #74); record the scope change where the AC text lives (the matrix still says 8).
- **S1-12:** re-run `summaries` on staging after #74 and record coverage at grade 13; verify `securities.name` on staging.
- **S1-12 (post-demo, decision 3):** FMP's terms forbid redistribution; descriptions move to SEC 10-K Item 1 later.
