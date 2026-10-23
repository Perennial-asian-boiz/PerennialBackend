# M4 follow-up: market caps as a tracked source

Status: proposed design for Bryan and checker review. Docs only; no code or migration.
Date: 2026-10-22. Branch: `docs/m4-market-caps` from `origin/main` at `04b75f9`.
Revised 2026-10-22 for the checker's MC-1..MC-4 corrections.
Inputs: roadmap M4 (`market_caps` row), decisions D7 and D14 and M4 answer 5
(`decisions-20261021.md`), the M4 identity design note (`docs/m4-identity`),
the M4 implementation (`feat/m4-identity` @ `eac71ce`), and M2 as accepted
(`feat/m2-degraded-publishing`, PR #29).

## Outcome

Market caps get the same run, batch and lineage history as the four watchlist
sources. A failed or partial market-cap collection leaves a durable failed run
instead of a log line. Every publication pins the exact market-cap rows it was
ranked with, replays byte-identically, and never uses a value older than D7's
3 days. The legacy `market_cap_observations` path keeps replaying the
publications that already reference it until the contract step.

## How it works today

- `runner.collect_market_caps` (`src/pipeline/runner.py:136`) runs at publish
  time and makes **one FMP `/stable/profile` request per candidate symbol**,
  sequentially, with a 300 s deadline. It keeps `marketCap`, `sector` and
  `industry`. A missing profile is an explicit null. Any transport or parse
  failure raises `PublicationError`.
- Candidates (`ranking.candidate_symbols`) are every ARK holding plus every
  Congress *purchase* ticker in the selected batches.
- `publish()` (`publication.py:292`, `:494`) validates the values, then inserts
  `market_cap_observations` rows and `publication_market_caps` pins inside the
  publication transaction. Observations exist only for publications that
  succeeded, so a failed collection leaves no history.
- M2 adds `market_caps_or_fallback`: if collection raises, each symbol reuses
  its latest stored non-null observation retrieved within
  `market_cap_max_reuse_age` (3 days, D7). Otherwise the value is `None`, which
  ranking reports as `market_cap_unavailable`.
- `replay()` reads values through `publication_market_caps` →
  `market_cap_observations`.

## Two findings that shape the design

**1. The free plan's call quota doesn't cover one call per candidate.**
FMP's free plan allows **250 requests per day** (FMP FAQ and pricing page,
read 2026-10-22). The checker already flagged this as P-R3: the candidates are
"likely several hundred". Congress collection also spends FMP calls from the
same quota. Sending one profile request per candidate per day therefore
cannot run every day under D14. Any design that keeps `/stable/profile` has to
budget its calls.

**2. Decision 5 names an endpoint whose plan access is unverified.**
M4 answer 5 makes FMP **company-screener** authoritative for issuer-level market
caps. The code has never used it; it uses `/stable/profile`. The only repo
evidence about company-screener is the ARK fetcher's upgrade note, which calls
it the "FMP paid screener" (`fetchers/ark.py:33`). FMP's public endpoint page
doesn't state which plan includes it. That is an absence on the pages read, not
proof either way. If company-screener *is* available on the free plan, it changes
everything: one filtered call (US exchanges, high `limit`) could return market
caps for the whole listed universe, and the quota problem disappears.

So the design treats the provider endpoint as a swappable adapter behind one
storage shape, and asks Bryan to settle the endpoint with a probe (below).

## Proposed shape

### Source and storage (expand migration, number at merge)

- **Depends on GDELT PR 1** (`feat/ingestion-subject`, accepted @ `91fc19b`).
  On `04b75f9`, `SOURCES` is also the watchlist's required set: `_selected`
  needs a run for every source, M2's `select_sources` and policies iterate it,
  and health loops over it. Adding `market_caps` there would make it a
  required source pinned in `publication_sources`, which this design
  deliberately avoids (MC-1). PR 1 separates `WATCHLIST_SOURCES` (pinned and
  required) from the `SOURCES` vocabulary. `market_caps` joins the vocabulary
  as a third group, a security-scoped source that is not in the watchlist
  (e.g. `PINNED_ROW_SOURCES = ("market_caps",)`, with
  `SOURCES = WATCHLIST_SOURCES + SUBJECTLESS_SOURCES + PINNED_ROW_SOURCES`).
  PR 1 derives lock keys from each source's position, so entries are appended
  in merge order and never inserted before GDELT's.
- It extends the CHECKs on `source_batches` and `ingestion_runs`. It is a
  **derived** source in M4's sense: its plan comes from the selected Congress
  and ARK batches, which it records in `run_dependencies`. Its tickers resolve
  lookup-only through `security_identifiers`, and an unknown ticker is
  `unresolved_security`.
- New immutable table `market_caps`, one row per `(batch_id, security_id)`:

  | Column | Rule |
  |---|---|
  | `batch_id`, `security_id` | FKs, unique together |
  | `value` | `Numeric(24, 2)`, `> 0` or NULL (provider returned no value) |
  | `currency` | `USD` only for now, as today |
  | `issuer_level` | boolean. True for FMP's company-level `marketCap`. Under M4 answer 5, never sum it across a profile's share classes |
  | `observed_at` | provider timestamp if given, else NULL |
  | `sector`, `industry` | as observed in the same response, nullable. Kept so a publication's classification replays with the label it was built under (the GDELT README's stated rule) |

  `retrieved_at` is the run's `collected_at`, so the table doesn't repeat it.
- New pin table `publication_market_cap_rows (publication_id, security_id,
  market_cap_id)`, with PK `(publication_id, security_id)` and an FK to
  `market_caps.id`. Market caps are not pinned in `publication_sources`,
  because a publication may use rows from several market-cap batches (see the
  rolling refresh below); each row's batch and run give the lineage.
- Grants: `market_caps` and the pin table go into the immutable set
  (importer SELECT/INSERT; reader SELECT).

### Collection: rolling refresh under a call budget (profile adapter)

Each pipeline run plans requests only for securities that need them:

1. Candidates = `candidate_symbols` over the selected upstream batches.
2. Skip a candidate whose latest stored market-cap row was retrieved within
   `refresh_age` (setting, default 1 day).
3. Order the rest: candidates with no usable row first, then oldest row first,
   then by symbol (deterministic).
4. Request at most the **market-cap share of one shared FMP budget** (below).

**One FMP budget across Congress and market caps (MC-2).** Congress and
market caps use the same FMP key and the same 250/day quota, and Congress
collection's cost per run isn't settled yet: probe v2 measures how deep free
pages go, and the incremental Congress design follows from it. A market-cap-only
`call_budget` could starve Congress, so there is one setting:

- `fmp_daily_call_budget` (default 200, under the 250 plan limit to leave room
  for manual checks and retries), counted per UTC day from recorded runs.
- **Congress spends first.** It is a required root source; its run records the
  FMP calls it made (a count in its diagnostics).
- Market caps take what remains: `remaining = fmp_daily_call_budget − FMP calls
  already recorded today (Congress, earlier market-cap runs, retries)`. With
  nothing remaining, the market-cap run succeeds as an empty refresh with a fixed
  `budget_exhausted` count. Selection then reuses rows within D7, and the
  degraded counts show it.
- Every FMP-calling run records its call count, so the budget is computed from
  durable state rather than an in-process counter. That keeps it correct across
  retries and separate CLI invocations.

The run's batch holds exactly the securities it requested. Coverage is
`{"scope": "selected_universe", "complete": true}` when every planned request
returned a usable answer (a value or an explicit missing profile). Any
transport, HTTP or parse failure fails the whole run (`partial_collection` or
the existing codes), matching the ingestion contract. Nothing partial is imported,
and the previous rows stay usable within D7.

Capacity follows from this: every candidate stays within 3 days if
`candidates ≤ 3 × (fmp_daily_call_budget − daily Congress calls)`. With
Congress at a handful of calls that's roughly 580; probe v2 will show the real
Congress cost. Past that, the oldest
caps go stale and show up explicitly (below). That is a sizing signal, not a
silent truncation.

With company-screener on the free plan, the adapter becomes one or a few
universe-wide calls, the market-cap share of the budget becomes a few calls, and each batch is a
full snapshot (`scope: "snapshot"`). The storage, pinning and selection rules
don't change.

### Selection at publish (replaces `market_caps_or_fallback`)

For each candidate security, use its latest `market_caps` row whose run
succeeded and was retrieved within `max_age` (D7: 3 days; `observed_at` must
also be within 3 days when present). If none qualifies, the value is `None`
and ranking marks `market_cap_unavailable` as today.

**A NULL row is an observation, and the latest row wins (MC-3).** If today's
row is NULL (FMP returned no profile) and a row with a value from 2 days ago
exists, the security is unavailable. The older value is not used. The
provider's current answer is "nothing", and replacing it with an older number
would publish a value FMP no longer stands behind. A transient omission costs
one day's value for that security. It is counted in `unavailable`, and the next
refresh restores it. (The alternative, the latest non-null within age, is also
coherent. This note picks the first so selection never skips a newer
observation.) Keep today's guard that
refuses publication when *every* candidate is unavailable.

- **Degraded marking:** when any candidate's value is reused from an earlier
  run, or is unavailable, the publication's M2 `degraded` event records
  counts only: `market_caps: {"refreshed": n, "reused": n, "unavailable": n,
  "oldest_age_seconds": s}`. No per-symbol lists, which keeps the event small
  and value-free.
- **Policy values:** fold `market_cap_max_reuse_age` into M2's
  `source_policies` as `market_caps: SourcePolicy(max_reuse_age=3d,
  max_age=3d)`. Here ages apply per row, not per batch.
- **Where policy is recorded (MC-4):** not in `ranking_config`. M1-A narrowed
  that to ranking inputs only. Today, M2 records reuse limits only in
  `degraded` events, so a non-degraded publication carries no policy, and design
  rule 6 is only partly met. Rather than add a third home, this note recommends
  **one: the quality event** that M1-A already writes for every publication
  (exactly one `quality_passed` or `quality_override`, enforced by
  `uq_publication_events_quality`). Its `checks` gains a `policy` object: each
  source's `max_reuse_age`/`max_age` (including `market_caps`), `refresh_age`
  and `fmp_daily_call_budget`. No migration is needed (`checks` is JSONB).
  `degraded` events then reference the policy instead of repeating it. Moving
  M2's limits there touches `publication.py` (db-implementer), and that
  migration of M2 behavior should be its own small change.
- **Determinism:** selection reads only stored rows, so a Q7 retry
  (`publish_or_reuse_refused`) reuses the same selection without calling FMP,
  which M2's `cap_cache` does today.

### Replay

`replay()` reads values from `publication_market_cap_rows` when it has rows
for the publication, and from `publication_market_caps` otherwise. A
publication uses one path, never both; a CHECK-style test asserts that. Old
publications replay unchanged through the legacy tables.

### Expand, then contract

- **Expand (this follow-up):** new source, tables, pins, grants; the runner
  collects through the contract; `publish()` writes pins to the new table. Stop
  writing `market_cap_observations` in the same change. The legacy table then
  only serves old publications and M2's fallback, which the new selection
  replaces.
- **No legacy backfill into batches.** Old observations were not collected as
  runs; inventing runs for them would fabricate collection history. They stay
  where they are for replay.
- **Classification:** `company_profiles` keeps the reviewed, current
  sector/industry (M4). Running `backfill_profile_classification` from
  `market_caps` as well is a small follow-up after GDELT switches its reads.
- **Contract (M7/M8, not here):** drop `market_cap_observations`,
  `publication_market_caps` and their sector/industry only when no retained
  publication replays through them, either because they are beyond the retention
  Bryan sets or because a reviewed one-off copies their pins. Either needs its
  own decision.

## Bryan-run probe needed first

One command, prepared like probes v1/v2 (status codes and counts only, no key
in output, never run by agents):

1. `company-screener` with US exchanges and `limit=1000`: HTTP status, row
   count, and how many rows carry a positive `marketCap`.
2. `profile` for one symbol, as a control that the key works.

A 402 or 403 on call 1 settles it as paid; keep the profile adapter with a
budget. A 200 with broad coverage means switching the adapter to screener
snapshots. Writing this probe is a small packet once approved.

## Tests the implementation needs

- Migration round-trip, drift, and the source CHECK accepting `market_caps`.
- Budget planner: no-row-first then oldest-first ordering, the `refresh_age`
  skip, deterministic ties, and the remaining shared budget computed from
  recorded call counts: Congress first, an exhausted budget gives an empty
  refresh, and two invocations on one UTC day share the budget.
- NULL precedence: today's NULL beats an older value (unavailable); the next
  successful refresh restores the value.
- The quality event carries the full policy on every publication, degraded or
  not.
- A failed request fails the run and imports nothing, and previous rows still
  publish within 3 days. At 3 days + 1 second they become unavailable.
- A missing profile stores NULL with `complete: true`, not a failure.
- Selection across several batches; the degraded event carries counts only.
- Replay byte-identical for a new-path publication, a legacy-path publication,
  and after later market-cap runs.
- GOOG and GOOGL rows stay separate with `issuer_level = true`; nothing sums
  them.
- A Q7 retry makes no second provider call.
- Grants: the importer cannot UPDATE or DELETE `market_caps` or the pins.

## Ownership

The storage, collector, record model and migration fit dev-sources. The
source-group split builds on GDELT PR 1 (db-implementer), so this lands after it. Selection,
pinning and replay change `publication.py` and `runner.py`, which
db-implementer owns. Split the implementation into two packets, or have the
coordinator assign one owner for both.

## Open questions for Bryan

1. **Endpoint:** run the probe above. If company-screener is paid, is the
   profile adapter with a daily budget acceptable as the D14 answer, or should
   decision 5 change?
2. **Budget:** one shared `fmp_daily_call_budget = 200` with Congress first and
   `refresh_age = 1 day`, with candidates beyond the 3-day capacity going stale
   explicitly. OK, or should the candidate set shrink (for example, Congress
   purchases within a shorter window)? The final numbers depend on probe v2's
   Congress cost.
3. **NULL precedence:** the latest row wins, so a missing profile today means
   unavailable (MC-3). Confirm, or prefer the latest non-null within 3 days.
4. **Unavailable caps:** today a candidate without a cap still publishes under
   the `unresolved` bucket with a warning. Keep that, or require some minimum
   coverage before publishing?
5. **Legacy tables:** keep old publications replayable through the legacy
   tables until a retention decision (recommended), rather than backfilling?
6. **Licensing (outside M4):** FMP's pricing page says displaying or
   redistributing FMP data needs a separate Data Display and Licensing
   Agreement. That applies to serving market caps in the app (M5), not to
   storing them. Flagging it so it isn't discovered at launch.
