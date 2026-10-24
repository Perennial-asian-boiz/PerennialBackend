# Incremental Congress collection on the free FMP plan

Status: proposed design for Bryan and checker review. Docs only; no code or migration.
Date: 2026-10-22. Branch: `docs/congress-incremental` from `origin/main` at `2f5f9b8`.
Replaces D4 option B (bounded date windows), which the probes ruled out.

## What the probes established

| Probe | Call | Result |
|---|---|---|
| v1 (`7d82763`) | `{senate,house}-latest`, page 0, limit 20, `from`/`to` 60 days back | 200, 20 rows each, dates from the latest disclosures: **date filters are ignored** |
| v2 (`b61ee0d`) | page 0 limit 100; page 1 limit 20 | **402** for all four |

So the free plan serves exactly one page of the 20 most recent disclosures per
chamber. Bryan's v1 output shows Senate's 20 rows spanning about 3 weeks of
disclosures and House's about 5 days. There is no way to read further back for
free.

That makes today's collector fail on every run. `collect_congress`
(`collectors.py:531`) keeps paging while a page is full, page 0 is always full,
and page 1 returns 402, so the run fails with `http_402` (the roadmap's
problem 2). It also drops trades older than 2 years before storage
(`fmp.filter_recent`), which contradicts D13.

## Outcome

Each run reads the free page and accumulates every trade it has ever seen into
stored history. Ranking applies the 2-year window to that history, per D13. A
publication pins exactly the history it used, so replay stays byte-identical.
When the page has moved further than the last stored row, the system says so
explicitly instead of quietly losing trades.

## 1. Collection: one free page per chamber per run

- Two calls per run: `senate-latest` and `house-latest`, `page=0, limit=20`.
  Never page 1. Without a paid key, a 402 on page 0 stays an explicit failure
  (`http_402`), as now.
- Remove `filter_recent` from the database collector (D13). The legacy JSON
  path keeps it; `cron.py` can't change.
- The run's batch is unchanged in kind: an immutable, content-hashed
  **observation batch** of "the latest disclosures seen in this run", with
  `coverage = {"scope": "recent_window", "complete": true}`. `complete` stays
  true because the run read everything it asked for. Gaps get their own signal
  (§3). `publication.py:247` refuses incomplete coverage, so reusing
  `complete: false` for gaps would block every publication after one.
- Identical pages hash to the same batch and are recorded as a reuse
  (existing behavior). That keeps the twice-daily schedule (§4) cheap when
  nothing new was disclosed.
- The run records `fmp_calls: 2` in diagnostics, which is the shared FMP budget
  input proposed in the market-caps note (MC-2).

### Accumulating history

Overlapping observation batches are deduplicated into an append-only history
by a small **derivation run** (roadmap design rule 2) that follows each
successful Congress collection:

- New table `congress_trade_history`. Each row is one distinct trade, with
  the trade fields as stored today (`security_id`, chamber, politician, type,
  transaction and disclosure dates, amount label, asset fields, district, link),
  plus:
  - `trade_key`: a SHA-256 of those fields in canonical form (it excludes
    `fetched_at` and anything per-run).
  - `occurrence` (1..n), which keeps genuine multiplicity.
  - `first_seen_batch_id`.
  - `history_id`: a bigint identity used for pinning.

  Unique `(trade_key, occurrence)`. INSERT only for the pipeline role; no
  UPDATE or DELETE.
- **Why a content key:** FMP's `-latest` rows carry no transaction id that any
  probe or doc read has shown (the parser's `_provider_record_id` looks for
  `transactionId`/`id` and finds none in the documented field list). A
  politician/ticker/date match alone can't prove two reports are the same
  trade. The full content can.
- **Multiplicity rule:** if one observation batch has *k* identical rows, the
  history holds at least *k* occurrences of that key. Each new batch adds
  occurrences up to its own count and never sums across batches. So overlap
  between runs never double-counts, and two genuinely identical trades in one
  filing still count twice.
- The derivation records `run_dependencies` on the observation batch and runs
  under the pipeline's existing advisory lock (`source_lock("pipeline")`). That
  makes history ids commit in order, which pinning (§2) relies on.

**Residuals, stated plainly:**
- **Amendments:** a corrected filing changes the content, so it becomes a new
  key, and the old version stays. Without a provider id there's no safe
  supersede rule. Both rows count in ranking until a source with ids exists.
- **Page boundary:** if two identical trades straddle the page edge and the
  second never appears on a page with the first, history keeps one. This is
  rare and undercounts rather than overcounts.

## 2. What ranking reads, and how publications pin it

Two designs were considered:

| | A. Snapshot batch derived from all batches | B. Ranking reads the union of pinned batches |
|---|---|---|
| Shape | Each run writes a new cumulative batch (all history so far); the publication pins that one batch | The publication pins every observation batch so far; ranking dedups the union at publish and at replay |
| Publication and replay changes | none: `publication_sources` already pins one batch | a pin-set table, plus dedup logic duplicated in replay |
| Storage | the full history is copied whenever anything new appears | no copies, but pins grow with runs × publications (~1,400 pins per publication after 2 years at 2 runs/day) |
| Cost at our size | about 5 new trades/day (1 Senate + 4 House, from v1's spans) gives ~3,700 history rows after 2 years. Copying them ~730×/yr is **~1.3M rows/yr**, which risks the 500 MB Supabase free database (D14) on its own | small rows, but every publication's pin list grows without bound |

**Recommendation: C, an append-only history with a watermark pin.** It keeps
A's simple pinning without A's copying, and B's lack of copies without B's
growing pin sets.

- Ranking's Congress input becomes the `congress_trade_history` rows with
  `history_id ≤ watermark`, filtered to the 2-year window. A new
  `publication_history_watermarks (publication_id, source, through_history_id)`
  row pins it. History is append-only and appended in id order under the
  pipeline lock, so `id ≤ watermark` is a fixed set forever. Replay reads the
  same set.
- **The 2-year window moves into ranking (D13)** and is measured from the
  publication's `created_at`, which replay already passes to `calculate`. Old
  publications replay with their original window; history beyond 2 years stays
  stored.
- `publication_sources` keeps pinning the day's Congress observation batch.
  That batch still carries lineage, `source_as_of`, quality checks and M2
  reuse. The watermark carries the ranking input.
- **Downstream planning reads the same history (CI-1).** Insider and short
  interest are planned from history up to a watermark, within the 2-year
  window, not from the day's ~20-row page. Otherwise a Congress ticker that
  has scrolled off the page would rank without being requested downstream.
  Before planning, the runner folds the selected Congress batch into history
  and reads the watermark under the history lock. It records that watermark
  on the downstream run's Congress dependency
  (`run_dependencies.history_through_id`). Publish pins exactly that
  watermark. It refuses when the two downstream runs recorded different
  watermarks, or when their plan predates the selected Congress batch. Runs
  without a recorded watermark (all runs from before this change) rank the
  history visible at publish time.
- Rows with the same key and occurrence come out in `history_id` order, which
  replaces today's `row_number` order as the deterministic tiebreak in ranking.
- Publications made before this change have no watermark row and replay from
  their pinned batch exactly as today. Ranking version `watchlist-v3` marks the
  switch.

## 3. Gap detection

For each chamber, on a **full page** (20 rows), the run checks whether the page
reached back into stored history:

- **Possible gap** if *none* of today's rows already exist in that chamber's
  history, and history isn't empty. That is stricter than "the oldest row today
  is newer than the newest stored row" (the packet's rule), which misses a gap
  that ends on a shared date. The date rule then measures the gap:
  `gap_days = oldest_today − newest_stored` (0 or more).
- A page with fewer than 20 rows is the whole served list, so it has no gap.
  An empty history is a cold start (§5), not a gap.
- **Recording:** the collection still succeeds. The derivation writes a durable
  row to `congress_history_gaps (chamber, newest_stored_date,
  oldest_seen_date, detected_run_id)` and adds fixed counts to the run's
  diagnostics: `possible_gaps` and `gap_days`, values only, never trade data.
  `possible_gap` joins the diagnostics failure vocabulary for the chamber unit.
- **Signal:** a publication whose 2-year window contains a recorded gap gets the
  M2 `degraded` event with `congress_gaps: {"count": n, "max_days": d}`. Health
  reports **SEV3** on the run that detected it, and the API (M5) exposes a
  `congress_history_gaps` count. On the free plan a gap can't be backfilled,
  so the marker stays until the gap leaves the 2-year window or a paid backfill
  closes it (§6). Nothing fails silently, and nothing blocks publishing.

**With M2 reuse:** if Congress collection fails, M2 reuses the last good
observation batch for up to 3 days (D7), and history simply doesn't advance.
House's page spans about 5 days, so three days of failures usually still
overlap. Longer outages are what gap detection is for. The first successful run
afterwards detects and records the gap.

**With X1's `source_as_of`:** today it is the latest disclosure date in the
observation batch, and the publication guard refuses a regression. Tying the
publication's Congress as-of to **history** instead (the max disclosure date
with `history_id ≤ watermark`) makes it monotonic by construction. A provider
retracting its newest row then can't trip the unoverridable
"would regress observation date" guard. The observation batch keeps its own
`source_as_of` for health.

## 4. Run frequency

The coordinator recommends **twice-daily** collection. That costs 4 FMP calls a
day, and it halves the window in which a burst of filings can push trades past
the 20-row page before they're seen.

- **Schedule (PT):** a Congress collection job at **06:00**, followed by the
  daily pipeline that publishes, then a collection-only job at **18:00**,
  using the M3 one-shot scheduler (`--once --job congress`, like `--job gdelt`).
  The evening run updates history and gap detection but doesn't publish. The
  next morning's publication pins whatever history exists by then.
- **D6 (publish by 10:30 PT):** unaffected. The morning collection is two calls
  of a few seconds each. The evening run never touches the publication path.
- **More often is cheap:** every 6 hours would cost 8 calls a day, still small
  against the 250/day budget. If gap markers appear in practice, Bryan can raise
  the frequency without a design change (open question 3).

## 5. Cold start

History starts at the first successful run. Until then the window holds nothing,
and early on the 2-year window holds only days since the start, so `buy_count`
and `distinct_buyer_count` read low for a while.

- Each publication records, per chamber, `history_since` (the earliest
  `first_seen` disclosure date) in its policy record. The market-caps note
  proposes putting all policy in the quality event (MC-4). The API exposes it,
  and the UI can label Congress signals "since <date>".
- Health reports **INFO `congress:history_warming`** until `history_since` is 2
  years old. That is not a failure.
- **Seed (optional, Bryan decides):** legacy JSON from the paginated era (the
  `trades_congress.json` that `fmp.py` wrote under `local_data/`) holds genuine
  provider observations. It could be imported once as an attested file batch
  (`--attest-complete`) and fed through the same derivation, with its provenance
  recorded. Nothing is fabricated, and gaps between that file and the first live
  run are detected the same way.

## 6. Upgrade path

A paid FMP tier needs no schema change:

- The collector walks pages from 0 and stops at the first page that **fully
  overlaps** stored history, so it's still incremental and pays for only what
  it needs. A configured page cap keeps today's `pagination_exhausted`
  behavior.
- A one-off backfill walks back to the D13 horizon (the full history FMP
  serves) and feeds the same derivation. Dedup makes it idempotent with history
  already collected. Gap rows that the backfill covers get a later
  `resolved_by_run_id` (an expand column), and the degraded marker clears for
  new publications. Old publications replay unchanged through their watermarks.
- If a paid tier exposes transaction ids, `trade_key` can switch to the
  provider id going forward (a versioned key), which would also give a safe
  amendment rule.
- **Licensing (D8, B5):** FMP's pricing page says displaying or redistributing
  FMP data requires a separate Data Display and Licensing Agreement. That holds
  on any tier, including free. It affects showing Congress trades in the app
  (M5), not storing them. Bryan should settle it before launch.

## Migration (expand only, number at merge)

`congress_trade_history`, `congress_history_gaps`, and
`publication_history_watermarks`. Grants: INSERT and SELECT for the importer,
SELECT for the reader, no UPDATE or DELETE. If GDELT PR 2+ lands first with
`collection_mode = 'derived'`, the derivation reuses it.

## Tests the implementation needs

- The collector never requests page 1 and makes exactly 2 calls. A full page 0
  succeeds. A 402 on page 0 fails with `http_402`. No 2-year filter at storage.
- Dedup: overlapping runs add nothing; k identical rows in one batch give k
  occurrences; a later batch with fewer copies doesn't remove any; an amended
  row is a new key.
- Watermark: a publication replays byte-identically after more history is
  appended; the 2-year window comes from `created_at`; legacy publications
  replay from their batch.
- Gap: a full page with zero overlap records a gap row and counts, the
  publication still succeeds and is degraded, and health shows SEV3. A short
  page or an empty history means no gap.
- As-of comes from the history watermark: a retracted newest row doesn't regress
  it.
- The schedule: the evening job doesn't publish, and identical pages reuse a
  batch.

## Ownership

The collector, the history derivation, the migration and tests fit dev-sources.
Ranking v3, watermark pinning and replay touch `ranking.py`/`publication.py`
(db-implementer). That's the same split as the market-caps note, so the
coordinator should assign it as one packet or two.

## Open questions for Bryan

1. **History design C** (append-only history plus watermark) instead of a
   per-run cumulative snapshot (A) or union pinning (B)? Recommended: C.
2. **Gaps degrade and never block** a publication (SEV3 plus a degraded marker,
   kept while the gap is in the window)? Recommended: yes. Blocking on a gap
   that the free plan can never fill would stop publishing permanently.
3. **Frequency:** twice daily (06:00 / 18:00 PT) to start, raised to every 6
   hours if gaps appear? Recommended: yes; it costs 4 to 8 FMP calls a day.
4. **Seed history** from the legacy paginated-era JSON, if a good copy exists?
   Recommended: yes, as one attested file import with provenance, if Bryan has
   the file.
5. **Amendments:** accept double-counting of amended filings until a source
   with transaction ids exists? Recommended: yes, and documented in the API.
6. **Licensing:** settle the FMP Data Display and Licensing Agreement before M5
   serves Congress data.
