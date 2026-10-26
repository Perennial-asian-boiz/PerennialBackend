# S1-03 Sentiment discovery A: news and article ingestion

**Branch:** `S1-03-news-ingestion` · **Author:** Aalind K. (@Zura16) · **Story:** As a database administrator, I want to schedule collection of news and articles for each ticker so that sentiment can be calculated from them.

## Design

| Decision | Choice | Why |
|---|---|---|
| Source | Finnhub `company-news` | An official API, tagged by ticker, with small caps covered (RKLB 73 articles a week, ABSI 1). The team already has the key. Yahoo (PR #68) is unofficial and unmerged. EDGAR has company filings, not news. |
| Universe | Every ticker in the current production watchlist publication (the consensus list) | The same plan as fundamentals. Lineage names the Congress and ARK batches the publication pinned. |
| Window and schedule | The last 7 days, daily: `python -m src.pipeline.scheduler --once --job news` | AC 1 |
| Content | Headline and metadata only. `content_retrieved` records what Finnhub returned: `headline_and_summary` or `headline_only` | AC 2. The roadmap stores no article text (D8 terms of service, plus storage cost). Sentiment scores headlines. |
| URL | Finnhub's article link, deduplicated by Finnhub article ID | Finnhub returns its own redirect link. Resolving every link to the publisher would add thousands of requests a day. |
| Matching | `match-v1` in `src/services/news/matching.py` | AC 3, see below |
| Duplicates | A run's batch holds only the (security, article) pairs not stored before. `UNIQUE (security_id, finnhub_id)` on `news_articles` rejects any duplicate | AC 4 holds in the database, not only in code |
| Empty and errors | One `news_coverage` row per (run, ticker): `covered` with counts, `no_coverage`, or `error` with a fixed code such as `http_429`. A failing ticker never stops the run. The batch is then marked incomplete | AC 5 |

The key is sent in the `X-Finnhub-Token` header, never in the URL. Requests are spaced 1.1 seconds apart, inside the free plan's 60 calls a minute. Retries on 429 and 5xx responses come from the shared `_get_json`. If every ticker fails, the batch fails as `collection_failed`, and the coverage rows still record each error.

## Matching rule (`match-v1`)

Finnhub tags each article with the ticker it was requested for. The tag alone isn't trusted. An article is kept for a ticker only when its headline or summary names the company:

1. **Ticker:** `$NVDA` anywhere, or `NVDA` as an uppercase whole word. Tickers of one or two letters (F, MS, DE) need the `$` form or an exchange form such as `(NYSE: MS)`.
2. **Name:** a variant of the company's name as a whole phrase, case-insensitive. Variants come from the security name, legal name and alias names. Legal suffixes are removed (Inc, Corp, Holdings), and then generic trailing words (Technologies, Therapeutics). A single word that is ordinary in financial news (Block, Target, Square) never matches by itself.

Anything else is dropped. `match_rule` records which rule matched.

## Tables (migration `0017`)

- **`news_articles`** (source `news_articles`, through the ingestion contract): `finnhub_id`, `url`, `headline`, `outlet`, `published_at`, `content_retrieved`, `match_rule`, `content_hash` (SHA-256 of the headline and summary), plus `security_id` and `batch_id`.
- **`news_coverage`**: `run_id`, `security_id`, `status`, `articles_found`, `articles_new`, `error_code`. CHECK constraints make `error_code` present exactly when `status = 'error'`, and `no_coverage` mean zero articles.

## Verification

| AC | Evidence |
|---|---|
| 1 | `tests/test_news.py`: stored records carry URL, headline, outlet, publish date and ticker. The live sample covered 110 tickers: 98 covered, 12 with no coverage, 0 errors, 2,665 matched pairs. |
| 2 | `content_retrieved` on every row. Tested for both values. |
| 3 | `match-v1` unit tests, plus `prototypes/news/match_sample.csv`: 20 random live matches (seed 20261026), hand-checked at 19 of 20 correct (target 18). |
| 4 | Reruns store only new pairs (tested over three runs). A direct duplicate insert is rejected by the database (tested). |
| 5 | A rate-limited ticker gets an `error` row and the run continues. A ticker with no articles gets `no_coverage` (tested). |

## Hand check (AC 3)

**Result: 19 of 20 correct, so AC 3 passes (target 18).** The labels are in the CSV's `correct` column.

- 15 are mainly about the matched company.
- 4 name the company substantively without it being the main subject: an ETF adding Broadcom (AVGO), Meta in an article about Corning (META), a list of aerospace stocks (LUNR), and PHP bugs mentioning Cloudflare (NET). These count as correct, because the article does say something about the company.
- 1 is wrong: SPCX matched an article about Tesla's Cybercab.

The rule counts a substantive mention as a match. If sentiment later needs the company to be the main subject, a `match-v2` could require the match in the headline, and the sample would be redrawn.
