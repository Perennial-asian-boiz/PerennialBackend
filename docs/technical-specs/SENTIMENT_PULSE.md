# SPEC: S3-02 Sentiment Pulse, piece 1 (News/Social breakdown bars)

| | |
|---|---|
| Story | S3-02 View Company Sentiment Pulse (Sprint 3, Module: Company, assigned Cohen K.) |
| This piece | Tab switch (News / Social) and the three breakdown bars (Positive / Neutral / Negative) for the ticker's current status, plus AC 5 "Not enough coverage" |
| Repos | `perennialbackend` (pipeline, migration, API) and `perennialfrontend` (Company page card) |
| Base commits | backend `main` @ `a85125e`, frontend `main` @ `58bac80` |
| Status | Approved 2026-11-18; backend implemented |

## 1. Goal

A signed-in user who opens `/company/:symbol` sees, in the **Sentiment Pulse** card, how the
company's current sentiment splits into positive, neutral and negative, built from the same data
stored in Supabase. They can switch between **News** and **Social** without waiting.

## 2. Scope

**In scope (piece 1)**
- Pipeline: label every social post (positive / neutral / negative) and save, for every tally,
  the exact list of articles and posts it counted.
- API: `GET /v1/securities/{symbol}/sentiment`, returning both tabs' breakdowns in one response.
- Frontend: replace the placeholder Sentiment Pulse card with a live one.
- AC 5 per tab ("Not enough coverage" under 3 items).

**Out of scope (later pieces of S3-02)**
- The headline reading (label + value) of AC 1, the 30-day trend (AC 2), the top 5 contributing
  articles (AC 3) and showing the stored value / weights version (AC 4). Piece 1's item table is
  designed so AC 3 can read from it later.
- Scheduling the pipeline. Running social collection inside the scheduled job is being done in a
  separate thread ("Add social collection to main pipeline"); see §10.

## 3. Decisions (from the interview)

| # | Decision |
|---|---|
| D1 | S3-02 is built in pieces; this spec is piece 1 only. |
| D2 | **News bars = weighted share** of counted articles per label. **Social bars = plain share** of counted posts per label. |
| D3 | The tally job **saves every counted item** in a new table `ticker_sentiment_items`. Bars are computed from it. No read-time recomputation of weights. |
| D4 | A news item's weight is its **full final weight**, the same number the job used for `ticker_sentiment.value`: tier weight × age decay × relevance share. |
| D5 | Social posts are labeled with the **same cutoffs as articles**: score ≤ −0.2 negative, score ≥ +0.2 positive, otherwise neutral. |
| D6 | Labeling happens **in the pipeline** and is stored in a new required column `social_posts.label`. The frontend and API never compute labels. |
| D7 | The migration **backfills** `label` for existing `social_posts` rows from their score. (Production `social_posts` is empty today, so this is a no-op there.) |
| D8 | "Current" = the **newest successful `ticker_sentiment` batch**, not the batch the Confidence Score uses. The response includes `tallied_on`. |
| D9 | AC 5 applies **per tab**: fewer than 3 counted news articles → News says "Not enough coverage"; fewer than 3 counted posts → Social does. The minimum comes from the API (`min_items`). |
| D10 | Each bar shows **percent and count** ("Positive · 8 articles"). A source line sits at the bottom of the card ("Based on 13 articles, Nov 10–17 · tallied Nov 17"). News adds "Weighted by source credibility and age". |
| D11 | **One request** returns both tabs. Switching tabs makes no request. |
| D12 | The page **always opens on News**; the choice resets on every company page. Labels stay "News" and "Social". |
| D13 | States: loading placeholder; error + Retry; "No sentiment data for this company yet." for both no reading and a reading with no saved items. |
| D14 | Layout follows the Figma "Sentiment Pulse" frame (percent inside the bar, colored label below). **"Mix" is renamed "Neutral"**. Count goes after the label; source line at the card bottom in small grey text. |
| D15 | An **automated gate** must pass before code is pushed (§9). Claude runs it in the cloud workspace before delivering each patch; manual testing happens afterward on the branch. |
| D16 | SPEC.md lives in the project folder **and** is committed to the backend at `docs/technical-specs/SENTIMENT_PULSE.md`, with a `DECISIONS.md` entry. Two patches, backend first. |

## 4. Data model (backend, migration `0026`)

File: `development/database/migrations/versions/20261118_0026_sentiment_breakdown.py`, `down_revision = '0025'`.

### 4.1 `social_posts.label` (new column)
- `label varchar(8) NOT NULL`, CHECK `label IN ('negative','neutral','positive')` (constraint name `ck_social_posts_label`).
- Upgrade: add nullable, backfill
  `UPDATE perennial.social_posts SET label = CASE WHEN score <= -0.2 THEN 'negative' WHEN score >= 0.2 THEN 'positive' ELSE 'neutral' END`,
  then `SET NOT NULL` and add the CHECK. Also add CHECK `ck_social_posts_label_matches_score` tying label to score with the same expression, so a label can never disagree with its score.
- Downgrade: drop the checks and the column.

### 4.2 `ticker_sentiment_items` (new table)
One row per item a tally counted for one security.

| Column | Type | Notes |
|---|---|---|
| `id` | bigint identity PK | |
| `ticker_sentiment_id` | bigint NOT NULL FK → `ticker_sentiment.id` | the reading this item fed |
| `kind` | varchar(8) NOT NULL | `'news'` or `'social'` |
| `news_article_id` | bigint NULL FK → `news_articles.id` | set iff kind = news |
| `social_post_id` | bigint NULL FK → `social_posts.id` | set iff kind = social |
| `score` | numeric(5,4) NOT NULL | the score the tally used, −1..1 |
| `label` | varchar(8) NOT NULL | negative / neutral / positive (article_sentiment.label or social_posts.label) |
| `weight` | numeric(12,10) NOT NULL | final weight used for the average, ≥ 0 |
| `item_at` | timestamptz NOT NULL | news: `published_at`; social: end of the collection day (as `social_items` does today) |

Constraints: `kind IN ('news','social')`; `(kind = 'news') = (news_article_id IS NOT NULL)`;
`(kind = 'social') = (social_post_id IS NOT NULL)`; `score BETWEEN -1 AND 1`; `weight >= 0`;
label CHECK as above; UNIQUE `(ticker_sentiment_id, news_article_id)` and
`(ticker_sentiment_id, social_post_id)` (their leading column indexes `ticker_sentiment_id`).

Pre-0022 article rows may have `article_sentiment.label IS NULL`. The tally must label such a news
item from its score with the shared cutoffs (§5.1) so `label` is never NULL.

### 4.3 `models.py`
Add the column and table definitions above, mirroring 4.1 and 4.2 exactly
(`test_scores_schema`-style tests compare models to the migrated database).

### 4.4 Grants (`development/database/ops/provision_roles.py`)
- `IMMUTABLE += ("ticker_sentiment_items",)`: the importer may SELECT/INSERT only.
- `API_COLUMNS` additions (read API role, column-level SELECT):
  - `ticker_sentiment`: `id, batch_id, security_id, tallied_on`.
  - `ticker_sentiment_items`: `id, ticker_sentiment_id, kind, label, weight, item_at`.
  - Nothing from `ingestion_runs`, `news_articles`, `article_sentiment` or `social_posts`, and no item `score` (not needed for piece 1).

## 5. Pipeline changes (backend)

### 5.1 Shared label cutoffs
New module `src/services/credibility/labels.py` holding `NEGATIVE_AT_MOST = Decimal("-0.2")`,
`POSITIVE_AT_LEAST = Decimal("0.2")` and `label_for(score) -> str`. `article_scorer.py` imports
them from here (its behavior must not change; existing tests must still pass). The migration's
SQL repeats the same numbers; a test asserts they agree.

### 5.2 Social labeling (`src/services/credibility/social.py`, `src/ingestion/schemas.py`)
- `SocialPost` record gains `label` (in `DB_COLUMNS`), validated to the three values and to agree
  with `label_for(score)`.
- `run_social` sets `label = label_for(score)` on every record it builds.

### 5.3 Saving counted items (`src/services/credibility/job.py`, importer)
- `Item` gains `kind`, `news_article_id | None`, `social_post_id | None`, `label`, and keeps
  `score`, `weight`, `first_at`. `news_items` and `social_items` fill them (select the ids and
  labels they currently drop).
- `TickerSentiment` record gains `items: list[TickerSentimentItem]` (not in `DB_COLUMNS`). Because it
  is a record field it is part of `canonical()` and therefore the batch content hash, so an
  identical same-day rerun still reuses the batch and its items.
- The importer, after inserting a `ticker_sentiment` batch, inserts each record's items into
  `ticker_sentiment_items` with the new row's id, **in the same transaction**. Add this as a
  generic "child rows" hook keyed by source (only `ticker_sentiment` uses it) rather than a special
  case inline.
- Items are written for every counted item, including tickers with fewer than 3 items. A ticker
  with nothing counted has no items.
- Invariants (tested): for every reading, `count(items where kind='news') = news_count`,
  `count(kind='social') = social_count`, and `sum(weight·score)/(sum(weight)+prior)` over its items
  reproduces `value` to 4 dp.

## 6. API contract (backend)

`GET /v1/securities/{symbol}/sentiment` in `src/api/read.py`, following `get_score`:
public (no sign-in), `read_transaction`, explicit columns only, ETag + `Cache-Control: no-cache`, 304 on `If-None-Match`.

| Case | Status | Body |
|---|---|---|
| Malformed symbol | 422 | `invalid_request` |
| Unknown ticker | 404 | `unknown_security` |
| No successful `ticker_sentiment` batch, or no row for this security in it, or the row has no items | 200 | `status: "unavailable"` |
| Otherwise | 200 | `status: "ok"` with both channels |

Resolution of "current": the newest `ticker_sentiment` batch (`max(batch_id)`). Only successful
imports store rows, so this is the newest successful tally, as the financials route does for
`fundamentals`, and the API needs no access to `ingestion_runs`.

### 6.1 Response (200)
```json
{
  "api_version": "v1",
  "symbol": "NVDA",
  "status": "ok",
  "tallied_on": "2026-11-17",
  "min_items": 3,
  "news": {
    "status": "ok",
    "weighted": true,
    "count": 13,
    "first_at": "2026-11-10T14:02:00Z",
    "last_at": "2026-11-17T09:30:00Z",
    "counts": { "positive": 8, "neutral": 3, "negative": 2 },
    "shares": { "positive": 0.6180, "neutral": 0.2510, "negative": 0.1310 }
  },
  "social": {
    "status": "not_enough_coverage",
    "weighted": false,
    "count": 1,
    "first_at": "2026-11-10T23:59:59Z",
    "last_at": "2026-11-17T23:59:59Z",
    "counts": { "positive": 1, "neutral": 0, "negative": 0 },
    "shares": null
  }
}
```
- `status: "unavailable"` bodies carry `api_version`, `symbol`, `status`, `reason`
  (`"no_reading"` or `"no_breakdown"`) and nothing else.
- Channel `status`: `"ok"` when `count >= min_items`, else `"not_enough_coverage"` with `shares: null`.
- News `shares[label] = sum(weight of that label) / sum(weight)`; social `shares[label] = count(label) / count`.
  Rounded to 4 dp. If total news weight is 0 with count ≥ 3, fall back to plain shares (documented edge case).
- `first_at` / `last_at` are the min / max `item_at` of that channel's items; null when count is 0.
- `min_items` is a module constant `MIN_COVERAGE_ITEMS = 3`.
- Add the endpoint to `docs/technical-specs/API.md`.

## 7. Frontend

### 7.1 Files
| File | Change |
|---|---|
| `src/api/sentiment.ts` (new) | Types for §6.1, `loadSentiment(symbol, signal)` (same structure and error class pattern as `api/score.ts`: `SentimentApiError`, shape guard `isSentimentResponse`), and `sentimentState(response, error)` returning a discriminated union. |
| `src/components/useSentiment.ts` (new) | Mirrors `useScore`: one request per symbol, abortable, stale-result guard, plus a `retry()` that re-runs the request. |
| `src/components/SentimentPulse.tsx` (new) | The card: heading, tabs, bars, captions, source line, states. |
| `src/lib/sentimentFormat.ts` (new) | Pure helpers: whole-percent rounding that sums to 100 (largest remainder), date-range text, caption text. |
| `src/pages/Company.tsx` | Remove the placeholder sentiment section and `SentimentBars`; render `<SentimentPulse symbol={security.symbol} />` in its place. Price Breakdown stays as is. Update the comment above `CompanyDetailSections`. |
| `src/index.css` | Reuse `.sentiment-*` classes; rename `.mixed` to `.neutral`; style the percent inside the bar and the grey caption/source text; skeleton bars. |
| `scripts/mock-api.mjs`, `scripts/mock-score-fixtures.json` | Serve `/v1/securities/{symbol}/sentiment` with fixtures for: ok both tabs, news ok + social not enough, unavailable. |
| `tests/sentiment.test.mjs`, `tests/sentiment-pulse.test.mjs` (new) | §9.2. Add both to the `npm test` script in `package.json`. |
| `README.md` | Screens table and a short "Sentiment Pulse" section describing the endpoint. |

No new npm packages.

### 7.2 Behavior
1. The card mounts with the page's resolved `security.symbol` and immediately starts the single request (in parallel with score and financials).
2. Tabs: `News` selected on mount; buttons keep `aria-pressed`; clicking a tab changes only local state. Tabs are clickable in every state. Opening another company remounts the card on News.
3. Rendering a channel with `status: "ok"`:
   - Three rows in order Positive, Neutral, Negative. Bar fill width = whole percent; percent text inside the bar; label in the label color below with ` · {n} article(s)` or ` · {n} post(s)`.
   - Percents are whole numbers that add up to 100 (largest remainder over `shares`).
   - Source line: `Based on {count} articles, {first}–{last} · tallied {tallied_on}`; Social uses "posts" and, when first and last fall on the same day, `collected {day}`. Dates use the user's locale (`Nov 10`); `tallied_on` is formatted as a UTC date.
   - News only: a note `Weighted by source credibility and age`.
4. Channel `not_enough_coverage`: no bars; text `Not enough coverage` and `{count} of {min_items} {articles|posts} needed`.
5. `status: "unavailable"` (either reason): both tabs show `No sentiment data for this company yet.`
6. Loading: three grey skeleton bars using the existing `loading-bar` shimmer.
7. Error (network, non-2xx other than handled, invalid body): `Sentiment couldn't be loaded.` and a `Retry` button that calls `retry()`. A 404 here is treated as an error (the page already handles unknown companies before the card mounts).
8. Accessibility: each bar row has an accessible name like `Positive: 62 percent, 8 articles`; the tab group keeps `role="group"` and `aria-label="Sentiment source"`.
9. Never invent numbers: nothing is shown that the API did not return.

## 8. Session flow

```
user opens /company/NVDA
  └─ Company.tsx: loadSecurityDetails ─┐ (existing)
     useScore / loadFinancials ────────┤ (existing)
     <SentimentPulse symbol="NVDA">    │
        └─ useSentiment ─► GET /v1/securities/NVDA/sentiment
              ├─ loading  ─► skeleton bars (tabs live)
              ├─ error    ─► message + Retry ─► same GET again
              └─ 200      ─► state per channel
user clicks Social ─► local state only, bars swap, no request
user opens /company/AMD ─► card remounts on News, new GET for AMD
```

Pipeline order that produces the data (unchanged except the new writes):
`run_social` (labels posts) → `run_sentiment`: news → article scores → publishers → `derive_sentiment` (writes `ticker_sentiment` **and** `ticker_sentiment_items`).

## 9. Automated gate (must pass before push)

Run in the cloud workspace before each patch is delivered (Postgres 16 there; CI uses 17). Gate output is attached to the delivery.

**Backend**: `python -m ruff check` · `python -m ruff format --check` · `python -m mypy` ·
`TEST_DATABASE_URL=... REQUIRE_DATABASE_TESTS=1 python -m pytest development/backend/tests -q` ·
`python development/database/ops/quality_gate.py`

**Frontend**: `npm test` · `npm run lint` · `npm run build`

### 9.1 Backend tests (new files `tests/test_sentiment_breakdown.py`, `tests/test_read_api_sentiment.py`; updates to `test_read_api_column_grants.py`, `test_scores_schema.py` where they enumerate tables/columns)
Migration
- 0026 upgrade adds `social_posts.label`, backfills −0.2 → negative, −0.1999 → neutral, 0.1999 → neutral, 0.2 → positive, and makes it NOT NULL; a mismatched label is refused by the CHECK.
- 0026 creates `ticker_sentiment_items` with its checks (kind/id pairing, ranges, uniqueness); downgrade drops both cleanly; round-trip passes.
- `labels.py` cutoffs equal the migration's numbers.

Pipeline
- `run_social` stores the right label for boundary scores; a record whose label disagrees with its score is rejected.
- `article_scorer` behavior unchanged (existing suite green).
- `derive_sentiment` writes one item per counted article and post; counts equal `news_count` / `social_count`; weights and scores reproduce `value` (4 dp); duplicates, off-topic and summary-only articles behave as today (summary-only items carry the half weight); a pre-0022 article with NULL label gets a label from its score.
- An identical same-day rerun reuses the batch and writes no new items; a ticker with nothing counted has no items.
- A failure while writing items rolls back the whole batch (no reading without its items).

API
- ok: correct weighted news shares and plain social shares, each summing to 1 ± 0.0001; counts per label; `first_at`/`last_at`; `tallied_on`; `min_items`.
- Per-tab coverage: news 3 / social 2 → news ok, social `not_enough_coverage` with `shares: null`.
- Picks the newest successful batch, ignoring a newer failed run.
- `unavailable` with `no_reading` (no batch or no row) and `no_breakdown` (row without items).
- 404 unknown ticker, 422 malformed, ETag/304.
- The API role can read exactly the granted columns and nothing from `news_articles`, `article_sentiment`, `social_posts`.

### 9.2 Frontend tests (node:test with `tests/render-tsx.mjs` stubs)
`tests/sentiment.test.mjs`
- `loadSentiment` builds the URL, aborts, raises `SentimentApiError` with status/code, rejects non-JSON and malformed bodies.
- `sentimentState` maps ok / not_enough_coverage / unavailable / error.
- Largest-remainder percents always sum to 100 (e.g. 0.3333/0.3333/0.3334 → 33/33/34) and handle 0 / 1.
- Date-range and caption text (singular/plural, same-day social).

`tests/sentiment-pulse.test.mjs`
- Renders News by default with three bars, percents, counts, source line and the weighting note.
- Clicking Social swaps to social bars with no second request; Social has no weighting note.
- Not enough coverage on one tab while the other shows bars.
- Unavailable → "No sentiment data for this company yet." on both tabs.
- Loading skeleton; error message and Retry triggers exactly one new request.
- "Neutral" appears, "Mix" does not.

## 10. Dependencies and risks
- **Thread "Add social collection to main pipeline"** is adding social collection to the scheduled job before the tally. It must leave `social_posts.label` and `ticker_sentiment_items` to this work; whichever merges second rebases `social.py` / `job.py`. If it lands first, labels are still written by `run_social`, so no change is needed here.
- `social_posts` is empty in production today (separate thread investigating). Until posts arrive, Social shows "Not enough coverage".
- Existing readings have no items, so every company shows "No sentiment data for this company yet." until the pipeline runs once after deploy. Run the pipeline immediately after deploying, and re-run `provision_roles.py` after the migration so the API grants apply.
- `news_articles.url` (Finnhub redirect) and per-item sources are not exposed in piece 1; AC 3 will add them.

## 11. Delivery steps (agent)
1. Backend: migration 0026 + models → labels module + social labeling → items in job + importer hook → endpoint + grants → tests → `docs/technical-specs/SENTIMENT_PULSE.md` (this spec) + `DECISIONS.md` entry ("Sentiment Pulse breakdown: stored items, social labels") + `API.md`.
2. Run the backend gate; fix until green.
3. Frontend: api client + state → format helpers → hook → card → Company.tsx wiring → CSS → mock API → tests → README.
4. Run the frontend gate; fix until green.
5. Deliver per project rule: for each repo a `.patch`, `APPLY_PATCH.md` (PowerShell, repo under `C:\Users\cohen\OneDrive\Desktop\VSCode\`), `PR_DESCRIPTION.md`, and the gate output, in `/mnt/project-files/sentiment_pulse_piece1/`. Backend merges first.

## 12. Acceptance for piece 1
- [ ] Opening a company with ≥ 3 counted news articles shows three News bars whose percents equal the weighted shares computed from `ticker_sentiment_items` for the newest successful tally.
- [ ] Social bars equal the plain share of counted posts by `social_posts.label`.
- [ ] Switching tabs is instant and makes no request.
- [ ] A tab with fewer than 3 items says "Not enough coverage".
- [ ] A company with no reading or no saved items says "No sentiment data for this company yet."
- [ ] Both gates pass.

## 13. AC 2: 30-day trend (approved 2026-11-19)

**Design (approved from a rendered mock):** a full-width "30-day trend" section at the bottom of
the Sentiment Pulse card, under a divider. A grey horizontal zero line; one bar per day with a small
gap between bars; green bars above the line for positive values, red bars below for negative ones,
height proportional to the value on a fixed −1 to +1 scale. A day without a reading has no bar, only
a faint dot on the line. Week ticks along the bottom, −1 / 0 / +1 on the left, a legend, and a
tooltip per bar ("Nov 7: −0.34"). The chart is the company's combined reading, so it is the same on
both tabs.

**API:** `GET /v1/securities/{symbol}/sentiment/trend` (`src/api/sentiment.py` `trend`, route in
`read.py`). 30 points, oldest first, ending today (UTC): each the `value` of that day's
`ticker_sentiment` row (`tallied_on`), the newest batch when a day was tallied twice; `null` for a
day with no row or a row with `value` NULL. 404 unknown ticker, 422 invalid, 200 with 30 nulls when
nothing is tallied, ETag/304, `Cache-Control: no-cache`. Grant: `perennial_api` may read
`ticker_sentiment.value`. Decision 116.

**Frontend:** `src/api/sentimentTrend.ts` (client, shape guard, state), `useSentimentTrend`,
`SentimentTrend.tsx` (SVG chart), rendered inside `SentimentPulse`. Its own states: skeleton while
loading, "The 30-day trend couldn't be loaded." with Retry, and "No readings in the last 30 days."
when every point is null. Tests: `tests/sentiment-trend.test.mjs`.

**Tests (backend, `tests/test_read_api_sentiment_trend.py`):** window and order, gaps for missing
days and NULL values (and a stored 0 stays 0), newest batch per day, out-of-window and other
securities ignored, 4-place values, nothing tallied, 404/422, ETag/304 (new tally, new day),
canonical JSON, OpenAPI, snapshot, API.md.

## 14. AC 3: top 5 contributing articles (approved 2026-11-21)

**Design (approved from a rendered mock):** on the News tab only, a "Top contributing articles"
section between the bars and the 30-day trend, always visible. Rows ranked 1–5: the headline as a
link opening the original article in a new tab (↗), "Outlet · Nov 17", a label pill in the bar
colors, and the weight as "21% of weight" with a small meter (the article's share of the reading's
total weight). A note: "Weight = source credibility × how recent × how much the article is about
the company. N articles counted in all." Hidden when News is not_enough_coverage or the reading is
unavailable.

**API:** `GET /v1/securities/{symbol}/sentiment/articles` (`sentiment.top_articles`). The current
reading's news items by final weight, heaviest first, ties to the newer article, at most 5, joined to
`news_articles` for headline, outlet, url and published_at; `share` = weight / total news weight.
Same unavailable states as `/sentiment`. Grants: `news_articles(id, headline, outlet, url,
published_at)` and `ticker_sentiment_items.news_article_id`. Decision 117.

**Frontend:** `src/api/sentimentArticles.ts`, `useSentimentArticles`, `TopArticles.tsx` inside
`SentimentPulse`; only http(s) links are rendered as links. Its own error with Retry.

**Tests:** backend `tests/test_read_api_sentiment_articles.py`; frontend
`tests/sentiment-articles.test.mjs`.
