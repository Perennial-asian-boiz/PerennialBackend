# S1-05 Sentiment discovery C: source credibility weighting

**Branch:** `Source-Credibility-Weighting` · **Author:** Aalind K. (@Zura16), taken over from Hong N. · **Story:** As a database administrator, I want to weight each article's sentiment by the credibility of its source so that reliable outlets count more than anonymous posts.

## Design

| Decision | Choice | Why |
|---|---|---|
| Inputs | Scored news articles (S1-04's `article_sentiment`) and social posts (the sentiment pipeline's JSON files) | The story compares outlets with anonymous posts, so both are in one average |
| What a source is | The publisher's base domain for news, the platform for social posts | Finnhub's outlet field says "Yahoo" for 18 of 20 sampled articles. Each Finnhub link redirects once to the publisher, so the job reads that redirect |
| Weight table | `src/services/credibility/source_weights.json`, versioned, four tiers, plus the half-life and the relevance rule | AC 1. Any change to the file bumps the version, and every result stores it |
| Relevance | An article counts only if its headline names the company | S1-04 scores the headline alone. A headline that doesn't name the company is about someone else |
| Recency | Each weight halves every 3 days of age | A week-old headline shouldn't count as much as today's |
| Unknown source | The lowest listed weight (0.2) | AC 2 |
| Window | Every scored item from the last 7 days | Matches news ingestion. The value doesn't swing on one night's articles |
| Result | One `ticker_sentiment` row per security per run: weighted value, plain average, counts, date range, weights version | AC 3 |
| Schedule | `python -m src.pipeline.scheduler --once --job sentiment` | AC 4 |

### Weight tiers (`weights-v2`; v1 had the same tiers, no decay and no relevance rule)

| Tier | Weight | Sources |
|---|---|---|
| Wire services and major outlets | 1.0 | reuters.com, bloomberg.com, wsj.com, cnbc.com, ft.com, apnews.com, nytimes.com, washingtonpost.com, economist.com |
| Established financial media | 0.7 | barrons.com, marketwatch.com, benzinga.com, fool.com, 247wallst.com, zacks.com, seekingalpha.com, yahoo.com, investors.com, morningstar.com, forbes.com, businessinsider.com, thestreet.com, investopedia.com, kiplinger.com |
| Aggregators, press releases, small blogs | 0.4 | globenewswire.com, prnewswire.com, businesswire.com, accesswire.com, stocktwits.com, cryptoprowl.com, insidermonkey.com, gurufocus.com, tipranks.com, investing.com |
| Social platforms | 0.2 | reddit, bluesky, mastodon, youtube |

Press releases are low because they are the company describing itself. A host matches its nearest listed parent domain, so finance.yahoo.com takes yahoo.com's weight.

## The calculation

For each security, over the last 7 days:

- Each scored news article whose headline names the company contributes S1-04's latest score for it, in [-1, 1], with its publisher's weight.
- Each social post contributes its 1 to 10 score mapped to [-1, 1] by (score − 5.5) / 4.5, with its platform's weight.
- Each weight is multiplied by 0.5^(age in days / 3), with age measured at the time of the run. A news article's age runs from its publication, a social post's from the end of its file's day. An item dated after the run keeps its full weight.

`value` = Σ(weight × score) / Σ(weight), and `plain_average` is the unweighted mean of the same items. Both are stored, so the effect of weighting is visible per ticker. The counts and dates describe the counted items only.

### Relevance (`headline-match-v1`)

S1-03 keeps an article for a ticker when its headline or summary names the company (match-v1). C reruns the same rule on the stored headline alone, with the names S1-03 uses, and leaves out articles that matched only through the summary. The run records how many it left out as `news_off_topic`.

A week of Finnhub news for 10 tickers (1,445 articles, 2026-10-30) showed why. Of the articles S1-03 keeps, 5 to 11% per ticker name the company only in the summary, and most are about another company ("Alphabet's October wipeout hits $766 billion" under NVDA).

Known limit: a headline that names two companies ("Apple Set to Dethrone Nvidia") counts at full weight for both, with one score, though the news is good for one and bad for the other. Fixing that needs a score per company within an article, from S1-04.

### Repeated stories (not built)

We measured whether the same story is counted more than once. The same week's sample had no two articles with the same headline under one ticker. The few true repeats (two pieces on a Tesla–SpaceX merger) had different headlines and the same summary, about 0 to 2 per ticker a week. Grouping by summary would be wrong: one site reuses a single boilerplate summary across 15 different AMD stories. Each article is stored once per ticker (`UNIQUE (security_id, finnhub_id)`), so no article repeats across runs.

## The job

`--job sentiment` runs these steps in order. One step failing never stops the next, and the log records how long each step took.

1. **News ingestion (S1-03, A).** Finnhub company news for the published watchlist.
2. **Article scoring (S1-04, B).** Cohen's step plugs in at `score_articles` in `src/services/credibility/job.py`.
3. **Publisher lookup.** Each article in the window without a publisher gets one request to its Finnhub link, reading only the redirect's `Location` header. Requests are spaced 1.1 seconds apart, at most 1,500 per run (about 28 minutes). A failed lookup isn't stored, so the next run retries it.
4. **Social import.** Reads `development/backend/sentiment/<platform>/<platform>_sentiments.json` without changing Hong's scripts. A file older than 7 days is skipped and listed as stale. Tickers that aren't stored securities are skipped and counted.
5. **Weighting (C).** A derived run, with lineage to the news, article score and social batches it read.

Hong's social scrape is not part of the timed job. It drives a browser and needs cookies for YouTube and Reddit. Its files are picked up when they're fresh.

## Handoff with S1-04 (Cohen)

S1-04 writes the `article_sentiment` source through the ingestion contract. Migration `0019` creates the table. S1-04 passes the ticker symbol, which it gets by joining `news_articles` to `securities`; the importer stores the `security_id`.

| Column | Meaning |
|---|---|
| `ticker` (record) / `security_id` | The article's security, the same as its `news_articles` row |
| `news_article_id` | `news_articles.id` |
| `score` | Sentiment in [-1, 1], four decimals |
| `model` | The model name, for example `ProsusAI/finbert` |

A batch holds only articles not yet scored by that model; `UNIQUE (news_article_id, model)` rejects a repeat. C uses only the current model's scores (the model of the newest article score), each article's from its latest batch, and stores that model as `ticker_sentiment.model_version`. `tests/test_credibility.py::score_articles` shows a minimal writer.

## Tables (migrations `0018` and `0019`)

- **`social_posts`** (source): `platform`, `post_id`, `score` (1 to 10).
- **`article_sentiment`** (source, written by S1-04): see above.
- **`news_publishers`**: `finnhub_id`, `domain`, `resolved_at`. Insert only.
- **`ticker_sentiment`** (derived source, created by Bryan's `0018` as the shared contract, decision 108): `value`, `plain_average`, `article_count`, `news_count`, `social_count`, `first_at`, `last_at`, `weights_version`, `model_version`. CHECK constraints keep the counts consistent, make `value` and the dates present exactly when something was counted, and make `model_version` present exactly when news was counted. The Sprint 2 score reads it.

`0019` builds on `0018`. Our lock keys are 170024 (`social_posts`) and 170025 (`article_sentiment`).

## Verification

| AC | Evidence |
|---|---|
| 1 | `tests/test_credibility.py`: the stored value equals the hand-calculated weighted, decayed average (RKLB 0.4245 from Reuters 0.8, an unknown blog −0.5, Reddit 10 and YouTube 1). Separate tests check the decay (a 4-day-old article counts half as much as a 1-day-old one) and that an article named only in its summary is left out. |
| 2 | An unknown domain and a missing publisher both get 0.2, the lowest listed weight (tested). |
| 3 | Every security has a row with its value, counts, date range and weights version. A security with nothing scored has a count of 0 and no value (tested). |
| 4 | The whole job runs end to end with fakes, and a failing step doesn't stop the weighting (tested). A live timing needs S1-04. Estimate for 110 tickers: news about 2 minutes, publisher lookups about 7 minutes for a day's roughly 400 new articles (28 minutes at most), social import and weighting under a minute. Scoring time depends on S1-04. |

## Live check of publishers

The 20 articles from the S1-03 hand-check sample all resolved, in about 22 seconds. They split into 17 at weight 0.7, 2 at 0.4 and 1 unknown at 0.2. The outlet field alone would have put 18 under "Yahoo".

**Open question:** 9 of the 20 resolve to finance.yahoo.com, which republishes stories from other outlets, including Reuters and Motley Fool. Those articles all get Yahoo's 0.7. Finding the original outlet would mean downloading each Yahoo page and reading its provider tag. That's a later change if the team wants it.
