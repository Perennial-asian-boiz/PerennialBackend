# Technical Decisions

Running record of backend decisions and the reasoning behind them. Add new
entries at the top. Anything here is reversible — the point is that a future
reader can see *why*, not that the decision is final.

> Note on numbering: `.env.example` references ADR-005, but the ADR set lives
> on the `bryan-working` branch and has not landed yet. Reconcile the numbering
> in this file with those ADRs when that branch is ported.

---

## Explanation template explain-v2

**Status:** Accepted
**Date:** 2026-10-31
**Code:** `src/services/scores/explain.py` (`TEMPLATE_VERSION = "explain-v2"`)

### Decision

An explanation ("why Perennial recommends this company") lists only
**supporting** signals, at most three, each citing the stored record and value
it came from (decision 103 (c)):

- A fundamentals measure is a claim only if it is worth **at least 50 points**
  (the midpoint of its 0–100 range). Supporting measures go by points,
  highest first.
- Sentiment is a claim only if it **leans positive** (value above 0). When it
  does, it always keeps the **last** of the three places, so a company with
  three strong fundamentals measures still shows its news.
- A component the score lists as missing produces no claim.
- Claims are generated only for a recommended score. A recommended score with
  no supporting signal is `unavailable` with reason
  `insufficient_supporting_data` (S2-03 AC5).

The sentiment sentence names what was counted: "News coverage…",
"Social posts…" or "News and social posts… (a articles, b posts)", over the
batch's `window_days` (see the ticker_sentiment entry below).

### Why the version moved

Explain-v1 allowed any measure, including weak ones, so "Revenue shrank…" could
appear as a reason to recommend a company. Decision 103 (c) changed which
claims are made, but the template version first stayed `explain-v1` (#93).
A stored explanation is carried forward when its input digest **and** template
version are unchanged (S2-03 AC4), and the digest covers inputs only, so
explanations stored under the old rule would have been served unchanged.
Bumping to `explain-v2` (#97) makes every v1 explanation regenerate, while
v2 explanations still carry forward. A change to which claims are made, or to
their wording, needs a new template version.

---

## The ticker_sentiment contract

**Status:** Accepted
**Date:** 2026-10-30 (weights-v2 and weights-v3: 2026-11-01, with #85)
**Code:** migration 0018 (`perennial.ticker_sentiment`), written by
`src/services/credibility/job.py` (S1-05, #85) and read by
`src/services/scores/job.py`

### Decision

`ticker_sentiment` is the one table between the sentiment pipeline and the
Confidence Score. It is #85's table plus `model_version` (decision 108 (b)):
one row per security in the current production watchlist publication, per
derived run.

| Column | Meaning |
|---|---|
| `value` | The credibility-weighted sentiment, −1 to +1. **NULL when nothing was counted** in the window |
| `plain_average` | The unweighted mean of the same items, for comparison (S1-05 AC 1) |
| `article_count` | `news_count + social_count` |
| `news_count`, `social_count` | News articles and social posts counted |
| `first_at`, `last_at` | The oldest and newest counted item. NULL together with `value` |
| `weights_version` | The `source_weights.json` version the value was computed with |
| `model_version` | The news model. NULL exactly when `news_count` is 0 |

Rules for every reader:

- **No coverage** is a row with a NULL `value`, or no row at all. Both mean
  sentiment is missing; neither is treated as neutral (Confidence Score rule 4).
  Social posts alone do count as coverage.
- **As-of** is `last_at`'s UTC date, whatever the database session's time zone.
- **Version** is `weights_version`.
- **Window:** the batch envelope carries `window_days` (7 today). It must be an
  int of at least 1. A reader that needs the window and finds anything else
  fails `invalid_upstream` and writes nothing.

### Weight versions

Changing anything in `source_weights.json` changes its version, which every
row stores.

- **weights-v1:** each item weighted by its source's tier: wire and major
  outlets 1.0, financial media 0.7, aggregators, press releases and blogs 0.4,
  social platforms 0.2. An unlisted source gets the lowest weight.
- **weights-v2:** only on-topic articles count (`headline-match-v1`: the
  headline must name the company), and each item's weight halves every 3 days
  of age (`half_life_days`).
- **weights-v3** (current): the weighted average starts with a neutral 0 of
  weight 1 (`prior_weight`), so a value built on little data stays near zero.
  One fresh Reuters article at +0.8 gives +0.4; ten give +0.73.

### Why

Two branches defined the same table independently (our 0018 contract and #85).
Decision 108 (b) kept #85's shape, which stores a row for every security, so
coverage is visible, and added `model_version` so a score's breakdown can name
the model behind it. The as-of, version and window rules exist so the score and
its explanation say exactly which data they used.

---

## Confidence Score composition

**Status:** Accepted, as built in score-v1 (proposed 2026-09-02; the original
proposal is kept below, followed by what was built)
**Date:** 2026-11-01
**Owners:** `@haohnguyen94-droid` `@Zura16` (evaluation), `@CohenK` (sentiment)

### Decision

The Confidence Score is a **single 0–100 number** blended from three
components, rather than several separate scores shown side by side.

| Component | Source | Weight | Native range |
|---|---|---|---|
| Fundamentals | Pattern evaluation — revenue, growth, margins | 60% | 0–100 |
| Sentiment | FinBERT over article text, credibility-weighted | 25% | −1 to +1 |
| Event exposure | GDELT | 15% | 0–100 |

### Why one number rather than three

A single score is the product's core promise: a beginner should not have to
reconcile three competing numbers to answer "is this company doing well?"
Splitting the score pushes interpretation work back onto exactly the user we
built this for.

The tradeoff is that a blended number can become opaque. The mitigation is the
M4 Score Break Down screen: the score is one number to the user and a
transparent weighted sum underneath, with each component, its weight, and its
value visible on tap.

### Why fundamentals dominate

Fundamentals move on quarters. Headlines move on hours. A sentiment-heavy
weighting produces a score that visibly jumps day to day, and users stop
trusting a number that changes without a reason they can see.

### Rules that keep the score stable

1. **Normalize before weighting.** FinBERT returns −1..+1. Map to 0..100 via
   `(s + 1) * 50` before it enters the blend. Mixing scales is the most common
   way a composite score quietly goes wrong.
2. **Cap sentiment swing.** Sentiment moves the final number by at most ±15
   points, so a single bad news cycle cannot sink a fundamentally strong
   company.
3. **Smooth over time.** Score sentiment over a 7-day rolling window with decay
   on older articles, rather than off whatever published today.
4. **Renormalize when a component is missing.** Small caps often have zero
   GDELT coverage. Treating absence as neutral (50) silently penalizes quiet
   companies — and quiet small caps are much of the Affordable & Growing
   bucket. Drop the missing component and redistribute its weight across the
   remaining ones.
5. **Store components, never just the total.** Every score row carries the
   three sub-scores, the weights applied, and a timestamp. This powers the
   breakdown screen and is the only way to debug a score after the fact.
6. **Version the weights.** A `score_version` column keeps today's 78
   interpretable after the weights are retuned.

### As built: score-v1

Code: `src/services/scores/scoring.py` (the blend) and
`src/services/scores/job.py` (the run).

- **Weights:** fundamentals 0.60, sentiment 0.25, event exposure 0.15,
  stored with every scores batch as `score_version` `score-v1`.
- **Event exposure is always missing in v1.** Its weight is redistributed
  over the present components (rule 4), never filled with a neutral 50.
- **Sufficiency:** a score needs fundamentals plus at least one other
  component. Without fundamentals it is `unavailable` (`no_fundamentals`);
  with fundamentals alone, `unavailable` (`only_fundamentals`).
- **Normalization (rule 1):** sentiment becomes `(value + 1) × 50`, rounded
  to 0.1.
- **Rounding:** half-even at each step: the effective weight
  (nominal / sum of present nominals) to 0.0001, each contribution
  (value × effective weight) to 0.01, and the score, the sum of the rounded
  contributions, to 0.1. So the contributions shown always add up to the score
  (S2-02 AC2).
- **The ±15 sentiment cap (rule 2) holds by construction.** Fundamentals are
  mandatory, so sentiment's largest effective weight is 0.25 / 0.85 = 0.2941,
  a swing of at most 14.7 points from neutral. There is no cap code. Revisit
  when event exposure arrives.
- **Smoothing (rule 3)** happens in the sentiment input, not in the blend:
  a 7-day window with each item's weight halving every 3 days (see the
  ticker_sentiment entry).
- **Stored components (rule 5):** every `company_scores` row stores each
  component's value, nominal weight, effective weight and contribution, plus
  the as-of and version of the record it used. The API recomputes the
  breakdown from the values and refuses a row that doesn't match.
- **Recommended:** status `ok`, score of at least 60, and the company is listed
  in the current production watchlist publication in **any** bucket,
  `unresolved` included (plan §8). Every row stores the publication it was
  judged against. With no production publication, nothing is recommended.
- **Serving:** the API reads scores only through the `current_scores` pointer
  table (one row per channel, like `current_publication`). This is an interim
  choice instead of the roadmap's `publications.kind`; it can be folded in
  later. Unchanged inputs reuse the stored batch, so the pointer and its
  `promoted_at` don't move (S2-01 AC2).
- **Related decisions:** D1 (a new scores batch regenerates its
  explanations), 103 (c) (see explain-v2 above), 108 (the ticker_sentiment
  contract above), plan §8 (recommended in any bucket).

### Open questions for the team

- Are 60 / 25 / 15 the right weights? They are a starting point, not a result.
- How is event exposure converted to 0–100? Article volume, GDELT tone,
  event-type severity, or some mix — undecided.
- What validates the score? Rank correlation against the Piotroski F-Score
  across the candidate list is the cheapest option: it is computable from data
  the fetchers already pull, it is publishable in the M5 writeup, and the
  divergences are more interesting than the agreements.

---

## GDELT as an event source, not a score validator

**Status:** Accepted
**Date:** 2026-09-02

### Decision

GDELT supplies global events and news coverage at the **front** of the
analysis, feeding both the event-exposure component and the article text that
FinBERT reads. It is not used to check or validate the fundamentals score.

### Why

An earlier design had GDELT cross-referencing the fundamentals score to
"increase accuracy." That does not work: GDELT reads text and has no concept of
revenue, so disagreement between the two says nothing about whether the
fundamentals score is correct. It only says news mood diverges from financial
performance — which is normal, and is itself information worth showing rather
than a signal to correct against.

Placed at the front instead, GDELT does the job it was built for and feeds the
Event Impact Flow directly.

### Implementation notes

- **DOC 2.0 API is free and needs no key:**
  `https://api.gdeltproject.org/api/v2/doc/doc?query=...&mode=artlist&format=json`
- **Entity resolution is the hard part.** GDELT returns organization names
  (`"Apple Inc"`), not tickers. A name→ticker lookup built from FMP's company
  list and cached in Supabase is required. Budget real time for this step.
- **Filter aggressively.** Restrict to candidate-list tickers and to a domain
  allowlist of reputable outlets. GDELT is very large.
- **The domain field feeds the credibility weighting** — the outlet arrives
  with every article, which is what makes source weighting possible at all.

---

## FinBERT for sentiment scoring

**Status:** Accepted
**Date:** 2026-09-02

### Decision

`ProsusAI/finbert` scores the sentiment of article text retrieved via GDELT.
Its output feeds the sentiment component of the Confidence Score.

### Implementation notes

- ~440MB, runs on CPU through `transformers`. Adequate for batches of headlines.
- **512-token limit.** Feed headline plus lead paragraph, not full articles.
- Trained on Financial PhraseBank, which is analyst-style prose. It handles
  wire copy well and **social media slang poorly** — worth measuring before
  pointing it at Reddit or X.
- Output is per-sentence, three-class softmax. Collapse to a continuous value
  with `P(positive) − P(negative)`, then aggregate per ticker using the source
  credibility weights.

### What FinBERT's confidence is not

A 0.91 positive score means "91% sure this sentence is positively worded." It
does not mean "91% chance this company performs well." The distinction matters
when labeling anything in the UI.
