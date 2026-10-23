# M4 market caps: task packets (provider-neutral)

**Status:** task packets for the test maker and builders. Docs only.
**Date:** 2026-10-23. **Author:** db-planner. **Branch:** `docs/m4-market-caps-packet` from `origin/main` at `ac21a00` (after #42 merged).
**Design:** `docs/work-items/m4-market-caps-design-note.md`, accepted by the checker (MC-1 to MC-6).
**Authorized by:** Bryan, 2026-10-23: decision 5 (start market caps) and decision 1 (deferred: FMP may be removed as a provider, so build only the provider-neutral parts).

## Outcome

Market caps get the same run, batch and lineage history as the watchlist sources. Every publication on the new path pins the exact market-cap rows it was ranked with and replays byte-identically, and no value it uses is older than D7's 3 days. A real provider can be added later by writing one adapter and registering it. Nothing else changes.

## What this packet does not build

Decision 1 rules out anything that depends on FMP or on choosing a provider:

- **No FMP work:** no `fmp_daily_call_budget` or other budget planner (MC-2), no company-screener probe, no new FMP adapter, no MC-6 reset-boundary work.
- **Existing FMP code stays exactly as it is.** That covers `runner.collect_market_caps` (`src/pipeline/runner.py:305-376`), `market_caps_or_fallback` (`runner.py:438-494`) and `fetchers/fmp.py`.
- **No rolling refresh or call quotas.** The note's `refresh_age` and its ordering exist only to stay inside FMP's quota. Quotas belong to whichever provider is chosen, so they are adapter work for later. Every run here plans every candidate.
- **No production switch-over.** Production keeps the legacy path until a real adapter exists (see "No provider configured" below).
- **No removal of legacy tables.** No contract step, no backfill of `market_cap_observations` into batches, and no classification backfill from `market_caps` into `company_profiles`.
- **No change to M2's degraded-event body for watchlist sources.** M2 is final as built (decision 8).
- **No API exposure** (M5) and no D8 licensing work.

## Current behaviour (origin/main ac21a00)

- **Collection at publish time.** `_run_pipeline` builds the FMP profile collector from `settings.fmp_api_key` (`runner.py:541-544`). `publish_selection` calls it once per candidate set, with a cache so that a Q7 retry makes no second FMP call (`runner.py:584-603`).
- **Candidates** are every ARK holding plus every Congress purchase ticker in the rows being ranked (`src/pipeline/ranking.py:19-27`). Since #42, those Congress rows are the stored history up to the publication's watermark, limited to 2 years (`runner.py:589-597`).
- **Without a key,** `collect_market_caps` raises `PublicationError` (`runner.py:316-317`). The fallback then reuses each symbol's latest stored non-null value retrieved within 3 days (`runner.py:450-494`), and marks the publication degraded (`publication.py:540-551`).
- **Checks before publishing.** `_caps` requires `provider == "fmp"` (`publication.py:450`). It refuses the publication when every candidate is unavailable (`publication.py:485-486`).
- **Writes at publish time.** `publish()` inserts one `market_cap_observations` row and one `publication_market_caps` pin per candidate, inside the publication transaction (`publication.py:775-796`). So history exists only for publications that succeeded.
- **Replay** reads values through `publication_market_caps` → `market_cap_observations` (`publication.py:922-940`).
- **Unavailable caps.** A candidate with no value is ranked in the `unresolved` bucket with the `market_cap_unavailable` warning (`ranking.py:139-143`, `:165`).
- **Demo mode** runs the same FMP profile collector against recorded fixtures (`src/pipeline/demo.py:239-252`, `:403`).

## No provider configured: what a pipeline run does

**Production keeps the legacy path, unchanged, until a real adapter is registered.**

A new setting, `market_cap_adapter`, defaults to `None`. With `None`, `run_pipeline` does exactly what it does today:
- it calls the legacy FMP profile collector and the stored-value fallback
- it writes `market_cap_observations` and `publication_market_caps`
- it records no `market_caps` run

The new path runs only when an adapter is configured. For now that means the fixture adapter in tests and demo mode, or a test's own stub adapter.

Why not switch production to the new path now:
- **With no adapter, every candidate is unavailable.** Today's guard then refuses every publication (`publication.py:485-486`), so production would stop publishing. Relaxing the guard would instead publish every candidate as `unresolved`, which is also worse than today.
- **Wrapping today's FMP profile call as an adapter** is building on FMP, which decision 1 rules out.
- **The cost is a temporary dual path** in `runner.py` and `publication.py`. The change that registers the first real adapter should also remove the legacy branch from the runner. The legacy tables then only serve replay until the contract step.

**Not worse than today also means today's exposure stays.** This needs Bryan's attention, though it doesn't block this packet:
- The legacy path makes one FMP `/stable/profile` call per candidate, inside the free plan's 250 calls a day, which Congress also uses. The design note found the candidates are likely several hundred (finding 1).
- Since #42 merged, Congress candidates come from up to 2 years of stored history, so the candidate count grows as that history accumulates.
- Once the quota runs out, `collect_market_caps` raises. Stored values cover the gap for 3 days, and after that every publication is refused.
- This packet doesn't change that. Settling decision 1, then writing the chosen provider's adapter, does.

## What counts as done without a provider

1. **New path with the fixture adapter:**
   - In tests and in demo mode, a pipeline run records a `market_caps` run and batch with lineage, publishes with pins in `publication_market_cap_rows`, writes nothing to the legacy tables for that publication, and replays byte-identically.
2. **Failures and age limits:**
   - A failed market-cap collection leaves a failed run. The publication uses earlier rows up to 3 days old, with degraded counts, and treats older rows as unavailable.
3. **Production default unchanged:**
   - With no adapter configured, the existing suite passes without edits.
   - A test proves that no `market_caps` run is recorded and the legacy tables are written as today.
4. **Adding a provider means one adapter:**
   - A stub adapter defined only inside a test runs the whole path with no other code change.
   - A reusable adapter conformance test, which the fixture adapter passes, is what any future adapter must also pass.

## Bryan's product calls (open questions 3, 4 and 5 of the design note)

The test maker can write everything else now. Only the tests named here wait on Bryan's answers.

| # | Question | Recommendation | Tests that depend on it |
|---|---|---|---|
| Q3 | **NULL precedence:** if the newest eligible row has no value and an older row within 3 days does, which wins? | **The latest row wins, so the security is unavailable** (MC-3, as accepted). Never publish a value the provider no longer stands behind. A transient omission costs one day for that security, it is counted as unavailable, and the next successful run restores it. The alternative is the latest non-null value within 3 days. | MCB-3c and MCB-3d only |
| Q4 | **Unavailable caps:** what happens to candidates without a usable cap? | **Keep today's rule.** Those candidates publish in `unresolved` with `market_cap_unavailable`, the degraded counts show them, and publishing is refused only when every candidate is unavailable. Don't set a minimum-coverage threshold until a real provider's coverage has been measured. | MCB-7a and MCB-7b only |
| Q5 | **Legacy tables:** keep old publications replayable through them, or backfill? | **Keep them** for replay until Bryan sets a retention policy. Don't backfill, because inventing runs for old observations would fabricate collection history. | MCB-9b and MCB-4c only |

## Packets, owners and order

Two packets, one owner per branch:

| Order | Packet | Owner | Branch | Builds on |
|---|---|---|---|---|
| 1 | Tests for MC-A | test-maker | `test/m4-market-caps-source` | `origin/main` (`ac21a00` or later) |
| 2 | **MC-A:** source, storage, adapter, collector | sources | `feat/m4-market-caps-source` | the head of step 1 |
| 3 | Tests for MC-B (can start during step 2, from the interfaces named here) | test-maker | `test/m4-market-caps-publish` | the head of step 2 when it exists; until then, the head of step 1 |
| 4 | **MC-B:** selection, pinning, replay, demo, health | implementer | `feat/m4-market-caps-publish` | the heads of steps 2 and 3 |
| 5 | Review | checker for both; auditor for MC-A (migration, grants, the fixture-adapter mode guard) | — | — |

**Base.** #42 merged at 20:50Z on 2026-10-23, so every line reference here is against `ac21a00`. Wherever this packet says "the rows `publish()` ranks", it means the Congress history rows up to the publication's watermark, plus the selected ARK batch. The migration takes the next free number at merge, `0011` today, because `0010` is #42's.

Merge MC-A and MC-B together, or MC-A first. MC-A alone changes no behaviour, because nothing calls the new collector until MC-B.

---

## MC-A: source, storage, adapter and collector (sources)

**You own:**
- a new migration
- `src/db/models.py`
- `src/pipeline/lifecycle.py` (the lock key only)
- `src/ingestion/schemas.py`, `src/ingestion/importer.py` and `src/ingestion/identity.py` (the `market_caps` entries)
- new `src/ingestion/market_caps.py`
- `src/settings.py` (the new field only)
- `development/database/ops/provision_roles.py`

**You don't touch:** `runner.py`, `publication.py`, `demo.py`, `health.py`, the legacy FMP code or the test maker's tests.

### Interfaces to build

- **Vocabulary:** add `PINNED_ROW_SOURCES: tuple[str, ...] = ("market_caps",)`, and set `SOURCES = WATCHLIST_SOURCES + SUBJECTLESS_SOURCES + PINNED_ROW_SOURCES` (`models.py:41-55`). Add the `SOURCE_TABLES` entry.
- **Table `market_caps`:** one row per `(batch_id, security_id)`. The columns:
  - `value` `Numeric(24, 2)`, NULL allowed
  - `currency` `String(3)`
  - `provider` `String(32)`. It is in the row, not just the envelope, because the envelope isn't part of the batch hash (`src/ingestion/canonical.py:25`).
  - `issuer_level` `Boolean`, not null
  - `observed_at` `timestamptz`, NULL allowed
  - `sector` `String(64)` and `industry` `String(128)`, both NULL allowed

  It needs a unique key on `(id, batch_id, security_id)` so the pin table's foreign key can target it.
- **Table `publication_market_cap_rows`:**
  - **Columns:** `publication_id`, `security_id`, `source` (always `'market_caps'`, by a CHECK), `run_id`, `batch_id` and `market_cap_id`.
  - **Keys:** primary key `(publication_id, security_id)`. `run_id`, `batch_id` and `market_cap_id` are all NULL (the candidate was unavailable) or all set.
  - **Run foreign key:** `(run_id, batch_id, source)` points to `ingestion_runs(id, batch_id, source)`, the same composite key `publication_sources` uses (`models.py:517-537`).
  - **Row foreign key:** `(market_cap_id, batch_id, security_id)` points to `market_caps`.

  This refines the note. Every candidate gets a pin row, so a publication's pins cover exactly its candidate set. The pin also records which run's retrieval made the row eligible, because a reused batch can belong to several runs.
- **Lock key:** `SOURCE_LOCK_KEYS["market_caps"] = 170017` (`lifecycle.py:184-193`).
- **Adapter interface, in `src/ingestion/market_caps.py`:**
  - `MarketCapQuote`: a frozen dataclass with `value: Decimal | None`, `currency: str`, `issuer_level: bool`, `observed_at: datetime | None`, `sector: str | None` and `industry: str | None`.
  - `MarketCapAdapter`: a Protocol with `name: str` (the provider id stored in each row, `^[a-z0-9_]{1,32}$`), `collection_mode: Literal["live", "fixture"]`, and `fetch(symbols: Sequence[str], *, session: requests.Session | None = None, sleep: Callable[[float], None] = time.sleep) -> Mapping[str, MarketCapQuote | None]`.
  - `MarketCapAdapterError(code)`: what an adapter raises on any failure, with a fixed code (`collection_failed`, `rate_limited`, `auth_failed`, `malformed_response`, `deadline_exceeded` or `missing_credential`). Messages are never stored.
  - `ADAPTERS: dict[str, Callable[[Settings], MarketCapAdapter]] = {"fixture": ...}`. This registry is the only place a provider is named.
  - `FixtureMarketCapAdapter`: `name = "fixture"`, `collection_mode = "fixture"`. It reads a synthetic JSON fixture (the test maker owns it under `tests/fixtures/providers/`), returns `None` for symbols the fixture doesn't list, and can be set to raise a given code, for fault injection.
  - `collect(adapter, *, upstream, session=None, sleep=time.sleep) -> CollectionOutcome` for source `market_caps`. `upstream` is the `{trades, holdings}` row shape the downstream collectors already take (`collectors.py:351-392`).
- **Setting:** `market_cap_adapter: str | None = None`. A name not in `ADAPTERS` fails at startup with `SettingsError`.

### Acceptance criteria

**Migration and schema**
- **MCA-1a** Upgrade adds `market_caps` to the `source` CHECK of `source_batches` and `ingestion_runs`, and creates both tables. Upgrade, downgrade and upgrade again leave no drift, with a single head.
- **MCA-1b** Downgrade refuses while any `market_caps` run, batch or pin exists, following the pattern of `0009` and `0010`. On an empty database it restores the previous schema exactly.
- **MCA-1c** `market_caps` rejects these inserts:
  - a `value` of 0 or less
  - a `currency` that doesn't match `^[A-Z]{3}$`
  - a blank `provider`
  - a second row for the same `(batch_id, security_id)`
- **MCA-1d** `publication_market_cap_rows` rejects these inserts:
  - a pin whose row isn't in its run's batch
  - a pin to a run of another source
  - a pin to a row of another security
  - a pin with only some of `run_id`, `batch_id` and `market_cap_id` set

  It accepts a pin with all three NULL.
- **MCA-1e** The source positions and lock keys that already exist don't change, and `market_caps` is not in `WATCHLIST_SOURCES`.

**Grants**
- **MCA-2** Both tables join `IMMUTABLE` (`provision_roles.py:14-29`):
  - The importer can SELECT and INSERT, but not UPDATE, DELETE or TRUNCATE.
  - The reader can only SELECT.

  `provision_roles.py` is re-run idempotently.

**Collector (`collect`)**
- **MCA-3a** **Planning.** The plan is `candidate_symbols` over `upstream`: sorted, with no duplicates. An empty plan fails with `no_input_tickers`, and a plan of more than `MAX_PLANNED_REQUESTS` (2000, `collectors.py:87`) fails with `plan_too_large`. Neither ever truncates.
- **MCA-3b** **Success.** One record per planned symbol, coverage `{"scope": "selected_universe", "complete": true}`, an envelope naming `adapter.name`, and `outcome.mode == "live"`, as `collect_source` requires (`runner.py:246-251`).
- **MCA-3c** **A symbol with no value.** A `None` quote is stored as a row with a NULL value, sector and industry. The run still succeeds, with coverage complete.
- **MCA-3d** **Adapter errors.** An adapter that raises `MarketCapAdapterError(code)` gives a failed run with that code and imports nothing. Any other exception is mapped by `collect_source` as today (`runner.py:254-267`): `collection_failed` for a `requests` exception, otherwise `internal_error`. No exception text appears in the run, its diagnostics or the logs.
- **MCA-3e** **Response mismatch.** If the response's symbols differ from the plan's, by one missing or one extra, the run fails with `malformed_response`.
- **MCA-3f** **Value validation.** This matches `_caps` (`publication.py:457-469`):
  - Values are finite, more than 0 and less than 1e22, rounded to 0.01 half-even.
  - A `bool` value, or one that is NaN, infinite or not a number, fails with `malformed_response`.
  - A non-USD currency fails with `unsupported_currency`.
  - An `observed_at` in the future fails with `malformed_response`.
  - An invalid sector or industry becomes NULL and doesn't fail, as `_classification` does (`runner.py:297-302`).

**Import and identity**
- **MCA-4a** Records resolve lookup-only by canonical ticker. An unknown symbol fails with `unresolved_security` and an inactive one with `inactive_security`, and nothing is imported. `market_caps` is not added to `ROOT_SOURCES` (`importer.py:98`). The adapter handles its own provider spellings and returns results under the canonical symbols it was given.
- **MCA-4b** Collecting identical results again reuses the batch (`reused_batch = true`). Identical values from two adapters with different `name`s produce two separate batches.
- **MCA-4c** GOOG and GOOGL get separate rows with `issuer_level = true`. Nothing sums them.

**Adapters and settings**
- **MCA-5a** **Fixture adapter.** It returns the fixture's values, gives `None` for unlisted symbols, and raises the configured code when told to fail. It never touches the network.
- **MCA-5b** **Conformance test.** A reusable conformance test covers the response key set, quote types, error codes and determinism. Any adapter must pass it, and the fixture adapter does.
- **MCA-5c** **Settings validation.** `market_cap_adapter = "nope"` raises `SettingsError`. `None` is the default.

---

## MC-B: selection, pinning, replay, demo and health (implementer)

**You own:** `src/pipeline/runner.py`, `src/pipeline/publication.py`, `src/pipeline/demo.py` and `src/pipeline/health.py`.

**You don't touch:** MC-A's files beyond calling them, the legacy FMP collector and fallback (keep them working for the `None` default), and the test maker's tests.

### Interfaces to build

- **New pipeline parameter:** `run_pipeline(..., market_cap_adapter: MarketCapAdapter | None = None)`. An injected adapter takes precedence over `settings.market_cap_adapter`. Neither one set means the legacy path.
- **New `publish()` parameter:** `market_caps="legacy" | "pinned"`, defaulting to `"legacy"`. With `"pinned"`, `publish()` selects rows itself, and `observations` must be `None`.

### Acceptance criteria

**Default unchanged**
- **MCB-1a** With no adapter injected and `market_cap_adapter = None`:
  - `run_pipeline`, `publish` and `replay` behave exactly as on `main`, and no `market_caps` run is recorded.
  - The legacy collector is called, `market_cap_observations` and `publication_market_caps` are written, and `publication_market_cap_rows` stays empty.
- **MCB-1b** The existing suite passes without edits.

**Collection on the new path**
- **MCB-2a** **When it runs.** Each `run_pipeline` call makes exactly one `market_caps` collection, after `select_sources`. It is planned from the rows `publish()` will rank for that selection.
- **MCB-2b** **Failure doesn't stop the run.** If that collection fails, the run is still recorded as failed and the pipeline goes on to selection.
- **MCB-2c** **No lineage check against the cohort.** Market-cap rows are pinned one by one, so `publish()` doesn't require the market-cap run's dependencies to match the published cohort.
- **MCB-2d** **Lineage.** `collect_source` treats `market_caps` as a downstream source (`runner.py:200`), so its run's `run_dependencies` names the Congress and ARK batches the plan came from. It also records the history watermark (`history_through_id`), as the other downstream sources do.
- **MCB-2e** **Mode safety.** `run_pipeline(demo=False)` with the fixture adapter configured refuses before any collection, with `ValueError`.

**Selection at publish**
- **MCB-3a** For each candidate, the eligible rows are those in `market_caps` that:
  - belong to a batch of a succeeded `market_caps` run whose `collection_mode` is in the publication's modes (live publishing never selects fixture-mode rows)
  - have an age (`now - run.collected_at`) of at most `market_cap_max_reuse_age` (D7, 3 days, `settings.py:79`)
  - have an `observed_at`, if present, within the last 3 days and not in the future

  When later runs reused the batch, the age comes from the latest of them.
- **MCB-3b** The selected row is the eligible one with the latest `collected_at`, ties broken by the higher run id. At exactly 3 days a row is eligible; at 3 days and 1 second it isn't.
- **MCB-3c** *(depends on Q3)* With latest-wins: a newer NULL row beats an older row that has a value, so the security is unavailable. It still gets a pin, to the NULL row.
- **MCB-3d** *(depends on Q3)* A later successful run with a value makes the security available again.

**Pins**
- **MCB-4a** On the new path, every candidate gets exactly one pin row: the selected row and its run, or all NULL when nothing is eligible. Nothing is written to `market_cap_observations` or `publication_market_caps` for that publication.
- **MCB-4b** No publication has pins in both `publication_market_caps` and `publication_market_cap_rows`. The suite checks this after every publish.
- **MCB-4c** *(depends on Q5)* The legacy rows of older publications are untouched, and nothing is backfilled.

**Degraded event and policy**
- **MCB-5a** **When it's written.** A `degraded` event is written only when at least one candidate is reused or unavailable. Its body gets `market_caps: {"refreshed": n, "reused": n, "unavailable": n, "oldest_age_seconds": s}`:
  - `refreshed` counts rows from this call's run.
  - `reused` counts rows from earlier runs.
  - `oldest_age_seconds` is `null` when no row was selected.
- **MCB-5b** **What it contains.** Counts only: no symbols, values or provider names. It merges into the M2 event body when watchlist sources are also reused, so there is still at most one `degraded` event per publication (0007).
- **MCB-6** **Policy on every publication.** Every new-path publication's quality event records `checks.policy.market_caps = {"max_age_seconds": 259200, "adapter": "<name>"}`, whether or not it is degraded. Legacy-path publications don't change.

**Unavailable caps**
- **MCB-7a** *(depends on Q4)* If some candidates are unavailable, the publication goes ahead. Those candidates are in `unresolved` with `market_cap_unavailable`.
- **MCB-7b** *(depends on Q4)* If the candidate set isn't empty and every candidate is unavailable, `publish()` refuses and writes nothing.

**Retries**
- **MCB-8** A Q7 retry (`publish_or_reuse_refused`, `runner.py:379-435`) selects again from stored rows and makes no second adapter call. The test counts calls on a stub adapter. Candidates that only the retry's selection adds get rows from earlier runs, or are unavailable.

**Replay and rollback**
- **MCB-9a** **New path.** `replay()` reads `publication_market_cap_rows` for a new-path publication, where a NULL pin means unavailable. Replay is byte-identical:
  - right after publishing
  - after later `market_caps` runs
  - for a degraded publication
- **MCB-9b** *(depends on Q5)* **Legacy path.** Legacy publications still replay byte-identically through `publication_market_caps`.
- **MCB-9c** **Rollback.** `cli rollback --to` a new-path publication succeeds; its replay check passes (`publication.py:842`).

**Demo, health and extension**
- **MCB-10** **Demo.** `run_demo` and `walkthrough` use the fixture adapter, so the new path runs in demo mode. Every walkthrough step still passes, and a demo publication replays byte-identically.
- **MCB-11** **Health.** With an adapter configured, `cli health` adds the advisory `market_caps:stale` when the latest succeeded `market_caps` run is more than 3 days old. It is an advisory, never an issue, like GDELT (`health.py:191-199`). With no adapter, it reports nothing about `market_caps`.
- **MCB-12** **Extension.** A stub adapter defined only in a test, added to `ADAPTERS` in that test, runs the full path (collect, select, pin, replay) with no other code change.

---

## Gates

- **Static checks:** `ruff`, format, strict `mypy` (tests included) and `git diff --check`.
- **Tests:** the full suite with `REQUIRE_DATABASE_TESTS=1` on each seat's own `perennial_test_*` database. Pass the path: `development/backend/tests`.
- **Migration:** round trip, drift check and a single head.
- **Tests first:** the test maker's tests fail for the right reason before each build. Report the failing output.
- **Review:** the checker reviews tests and code together for both packets. The auditor reviews MC-A: the migration, grants, the downgrade refusal and the fixture-adapter mode guard.
- **Safety:** synthetic fixtures only. No provider keys, no live calls, no push.

## Coming back to the coordinator

Bring these back rather than deciding them:
- anything that needs a provider chosen (quota planning, refresh cadence, provider-specific identifiers, the production switch-over)
- any change to the legacy FMP path
- any finding that `market_caps` can't stay out of `WATCHLIST_SOURCES`
