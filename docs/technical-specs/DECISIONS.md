# Technical Decisions

Running record of backend decisions and the reasoning behind them. Add new
entries at the top. Anything here is reversible — the point is that a future
reader can see *why*, not that the decision is final.

> Note on numbering: `.env.example` references ADR-005, but the ADR set lives
> on the `bryan-working` branch and has not landed yet. Reconcile the numbering
> in this file with those ADRs when that branch is ported.

---

## Confidence Score composition

**Status:** Proposed — logic to be reviewed by the team before implementation
**Date:** 2026-09-02
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
