# M2 degraded publishing: design note

> Built in #29 (merged 2026-10-22, migration `0007`); see `docs/roadmap/database-roadmap.md` for current state.

**Branch:** `docs/m2-design` · **Author:** db-implementer · **Status:** proposal, no code changed · **Plan of record:** `docs/roadmap/database-roadmap.md` §3 M2; decisions D6 and D7 (settled by Bryan 2026-10-21)

**Goal (roadmap M2):** one provider outage makes one source stale, not the whole watchlist.

**Builds on M1-A** (merged in #25, migration `0006`). M1-A added the `publication_events` table, which already allows `kind = 'degraded'` with a NULL actor and reason, and any number of degraded rows per publication. It also adds the fail-fast publish lock and the narrowed overrides. Nothing here needs a new migration unless Q5 says so; if it does, that migration is `0007`.

---

## 1. What happens today

| Step | Code (origin/main 04b75f9) | Behaviour on one failure |
|---|---|---|
| Collect | `runner._run_pipeline` | The first failed source raises `RuntimeError`. No publication, and later sources aren't even collected. |
| Downstream planning | `collect_source(upstream_batches=…)` | Insider and short interest are planned from this run's Congress and ARK batches. |
| Freshness at publish | `publication.MAX_AGE` (3/3/3/45 days), plus ARK observation ≤ 7 days and short-interest settlement ≤ 45 days in `_selected` | An old run is refused. |
| Lineage | `_selected`: each downstream run's `run_dependencies` must equal the selected Congress and ARK batches | Refuses mixed cohorts. |
| Market caps | `collect_market_caps` at publish time; `_caps` requires `retrieved_at` ≤ 1 day | Any FMP failure aborts the publication. |
| Health | `health.py` | Flat `issues` list. Any latest failed run makes it unhealthy. `publication:stale` after 3 days. No severities. |

---

## 2. Proposal

### 2.1 Per-source policy in settings

Add a typed `SourcePolicy` to `Settings`, keyed like `quality_thresholds`. The defaults are the **settled D7 limits** (short interest 15 days; insider, ARK and Congress 3 days; market caps 3 days):

| source | required | max_reuse_age | max_age (publish freshness) |
|---|---|---|---|
| congress_trades | true | 3 days | 3 days |
| ark_holdings | true | 3 days | 3 days |
| insider_trades | true | 3 days | 3 days |
| short_interest | true | 15 days | 45 days |
| market_caps (publish-time input, not a source table) | true | 3 days | — |

- **Validation at startup:** `0 < max_reuse_age ≤ max_age` for every source, so D7's "nothing may be reused past its `max_age`" can't be configured away. The current `MAX_AGE` constant becomes the default `max_age`.
- **`required`** keeps its roadmap meaning: when a required source has neither a fresh batch nor a reusable one, the run aborts. All four are `true` by default, because `calculate()` and `_selected` need all four. What `required = false` would mean is open question Q2.
- **Stored with the output (design rule 6):** the policy in force goes into the degraded event (§2.4). `ranking_config` keeps only the `freshness_seconds` it already has. M1-A narrowed `ranking_config` to ranking inputs, and the reuse policy isn't one.

### 2.2 The runner selects instead of aborting

For each source, in the existing order (Congress, ARK, then insider and short interest):

1. Collect as today.
2. If the collection succeeded, select this run.
3. Otherwise, select the **reuse candidate**: the run and batch that the current production publication pins for this source (`publication_sources`). It qualifies only if `now - run.collected_at ≤ max_reuse_age`, and it must still pass every `_selected` freshness rule.
4. If neither applies, abort the run with a new fixed code, `required_source_unavailable`. That code goes in `diagnostics.ERROR_SUMMARIES`, and the failed collection run is already recorded with its own code.

Reuse triggers on a **failed collection**. A collection that succeeds but is then refused by `publish()`, whether by quality or by the observation-date regression guard, still aborts the whole publication under this proposal. Whether that refusal should also fall back to reuse is open question Q7.

**Why the pinned batch and not just the latest successful run:** a pinned batch has passed the quality checks and been served. The latest successful run may have been refused by quality and never published. Reusing it would bypass the quality gate, and a reused source skips quality evaluation (§2.5). With no production publication yet, nothing can be reused, so a first run still needs every source.

`publish()` takes an explicit `reused: Mapping[source, run_id]` argument and re-validates it inside the publication lock:
- the run is pinned by the current production publication
- it is within `max_reuse_age`

A caller can't reuse an arbitrary old batch by passing its run id.

### 2.3 Downstream planning from the selected batches

- **An upstream source is reused** (Congress or ARK fails): insider and short interest are collected fresh, planned from the *selected* upstream batches through the existing `upstream_batches` path. The lineage check passes unchanged, as the roadmap says.
- **A downstream source is reused** (insider or short interest fails): `_selected` compares a downstream run's `run_dependencies` with the selected upstream **batch ids**, and the importer reuses a batch id when content is identical. So there are two cases:
  - **Today's upstream batch ids equal the reused run's dependencies** (Congress and ARK content unchanged): lineage passes as is. No cohort is needed, and Congress and ARK stay fresh.
  - **Today's upstream batch ids differ** (new trades or holdings arrived): `_selected` refuses the mix, as the checker confirmed with a probe on 04b75f9. **This is open question Q1.** Recommendation: **conditional cohort reuse.** Only in this case, reuse the **whole prior cohort**: that publication's pinned Congress, ARK and the other downstream source. Every downstream run pinned there already passed lineage against exactly those upstream batches, so lineage holds with **zero extra provider calls**. A freshly collected sibling batch from this run (for example, today's insider when only short interest failed) is kept as a successful run but not published, and the degraded event lists it under `cohort`. Re-planning the sibling instead would need a second Finnhub pass at 1.1 s per ticker before the 10:30 PT deadline (D6). If the pinned sibling itself is outside its reuse limits, the run aborts with `required_source_unavailable` rather than re-collecting.
  - Either way, the lineage invariant ("every pinned downstream batch was planned from the pinned upstream batches") stays absolute, with no change to `_selected`. The cost is limited to the changed-content case: one insider outage then leaves Congress and ARK at their previous publication's age, which is still within their reuse limits.
- **Two sources fail in one run:** the same rules compose. If Congress and insider both fail, Congress is reused, and the reused insider's dependencies match it if ARK's batch id is also unchanged; otherwise ARK joins the cohort. If ARK and short interest both fail, ARK is reused, and insider is planned fresh from the selected Congress and the reused ARK. Short interest is then reused only if its dependencies match those selected batches. Otherwise the whole prior cohort applies, including the pinned insider, and the fresh insider batch goes unpublished. The run order is Congress → ARK → insider → short interest, so a short-interest failure is the one case where the cohort is decided after insider was already collected fresh. Reusing the pinned insider avoids a second pass. Any required source left with nothing usable aborts the run.

### 2.4 Degraded events

A publication that reuses anything gets exactly one `publication_events` row:
- `kind = 'degraded'`, `actor = NULL`, `reason = NULL`, `created_at` equal to the publication's.
- It's written in the same transaction as the quality event.
- `checks`:

```json
{
  "reused": {
    "insider_trades": {
      "run_id": 41, "batch_id": 17, "from_publication_id": 9,
      "collected_at": "2026-10-19T16:02:11Z", "age_seconds": 86400,
      "failed_run_id": 52, "failure_code": "collection_failed"
    }
  },
  "cohort": ["congress_trades", "ark_holdings"],
  "policy": {"insider_trades": {"required": true, "max_reuse_age_seconds": 259200, "max_age_seconds": 259200}}
}
```

- Only fixed codes, ids, timestamps and settings values go in it (design rule 7). It is built entirely from DB values and `Settings`, never from collector text. That answers the auditor's note on M1-A that `checks` has no byte bound: this writer's output is bounded by the four sources.
- `cohort` lists upstream sources reused only because a reused downstream run depended on different upstream batch ids (Q1). It is empty when upstream content was unchanged.
- Proposed hardening: a partial unique index for one `degraded` row per publication, if Q5 agrees it should be enforced.
- **The served output is unchanged.** `calculate()` output and `ranking_version` don't change, so replay stays byte-identical. The API (M5) reads `degraded` and the per-source dates from the event and from `ranking_config.source_as_of`.

### 2.5 Quality checks for reused sources

The M1-A quality audit compares a candidate batch against "the previous live batch other than this one". For a reused batch that comparison is meaningless, and it can fail spuriously. A newer but unpublished batch would make `source_as_of` look like it went backwards. So reused sources are recorded in the quality event with `status: "reused"` and are not re-evaluated. Fresh sources are checked as today, and a fresh source's failure still needs a named override. Both regression guards keep working without change: a reused run's `collected_at` and `source_as_of` equal the prior publication's, so neither moves backwards.

### 2.6 Market caps

Market caps are collected at publish time, not stored as a source batch (that's M4). Proposal: if `collect_market_caps` fails as a whole, fall back to each candidate symbol's latest `market_cap_observations` row with `retrieved_at ≤ 3 days`. A symbol with no such row gets `value = None`, which the ranking already renders as `market_cap_unavailable`. The degraded event records `"market_caps": {"fallback": true, "oldest_retrieved_at": …}`. `_caps` widens its `retrieved_at` limit from 1 day to the market-cap `max_reuse_age` only for fallback observations, and the publication pins them through the existing `publication_market_caps` table. Whether this belongs in M2 is open question Q3.

### 2.7 Health severities

`health()` gains `severity` on each issue and an overall `severity` (`ok`, `SEV3` or `SEV2`); the `issues` strings stay for compatibility.

| Condition | Severity |
|---|---|
| Current publication has a `degraded` event | SEV3 (`publication:degraded`, with the reused sources listed) |
| Every source in the current publication was reused (the full-cohort case in §2.3, where only market caps are new) | SEV3, reported as `publication:degraded:all_sources_reused`. The degraded event's `checks` carries `"all_sources_reused": true`, and the M5 API should surface it in the same terms. This publication meets D6's 10:30 PT check, but it holds no new source data. |
| A source's latest run failed, but the current publication covers it within policy | SEV3. It was SEV-less "unhealthy" before. |
| **Missed publication** (D6: publish by 10:30 PT on trading days): no publication created since 10:30 PT on the most recent trading day, evaluated after that deadline | SEV2 |
| Current publication pins a run past `max_age` (as today's `publication:<source>:stale`) | SEV2 |
| `heartbeat_expired`, `never_run` | unchanged, SEV2 |

`healthy` stays `severity == "ok"`. The CLI exit code stays 1 for any issue, so the current monitor contract doesn't change. Whether SEV3 should exit 0 is open question Q4.

---

## 3. Test plan (Gate M2)

All synthetic, stubbed at `requests.Session` like `test_pipeline_contract.py`, through `run_pipeline` and through `cli run-pipeline`.

1. **Fault injection per source (×4):** publish a baseline, then fail one source.
   - Assert the publication succeeds and pins the prior run and batch for that source.
   - Assert exactly one `degraded` event with the right `reused` entry and `failure_code`.
   - Assert fresh sources are fresh, and the output equals `calculate()` on the pinned rows.
   - For insider and short interest, assert both branches of Q1. With unchanged upstream content, the reused run's lineage passes and `cohort` is empty. With changed upstream content, the cohort rule applies.
2. **Reuse limit:**
   - A reuse candidate older than `max_reuse_age` aborts with `required_source_unavailable`, the pointer is unchanged and there's no publication row.
   - The boundary is inclusive (exactly `max_reuse_age` passes).
   - Settings with `max_reuse_age > max_age` are refused at startup.
3. **No production publication yet:** any failure aborts; nothing is reusable.
4. **Replay:** a degraded publication replays byte-identically, and so do the publications on either side of it.
5. **`publish(reused=…)` hardening:** reusing a run not pinned by the current publication, or past its limit, is refused even when called directly.
6. **Quality interplay:** a reused source records `status: reused` and is not evaluated. A fresh source failing quality still blocks without a named override.
7. **Market caps (if Q3 is yes):** total FMP failure falls back to observations ≤ 3 days; older observations give `None`; the degraded event records the fallback.
8. **Health:** degraded is SEV3, missed is SEV2, and a failed latest run covered by reuse is SEV3. Severity strings come from a fixed vocabulary.
9. **Concurrency:** two concurrent degraded publishers; the second gets `publication_in_progress` (M1-A behaviour).
10. **Reused batch failing a non-age rule:** a pinned batch within `max_reuse_age` but failing another `_selected` rule (ARK observation over 7 days, short-interest settlement over 45 days, incomplete coverage) is not reusable, and the run aborts with `required_source_unavailable`.
11. **Cohort cases:**
    - **Two failures in one run:** Congress plus insider, and ARK plus short interest.
    - **Short interest failing alone with changed upstream content:** the whole prior cohort is selected, including the pinned insider. The fresh insider run is recorded but unpublished, and the stub provider shows no second insider pass.
    - In each case, assert the selected batches and the `cohort` list follow §2.3, and that the publication is consistent and replayable.
12. **`cli health` contract:** exit codes and JSON (`severity`, per-issue severity, the existing `issues` strings) for degraded (SEV3) and missed (SEV2) states, matching the Q4 decision.
13. **Pointer race:** the production pointer changes between the runner choosing a reuse candidate and `publish()` taking the lock. `publish()` must refuse the now-unpinned reuse with a fixed code, not publish a stale cohort. This covers the race itself, not only the direct-call refusal in test 5.
14. **Q7 (if accepted):** a fresh batch refused by quality or by the observation-date guard falls back to the pinned batch. The degraded event records the refusal code, and the guard itself is not weakened.
15. **Every test must fail on the pre-M2 base**, and that failure is recorded as evidence, as with M1-A.

---

## 4. Open questions for Bryan

| # | Question | Recommendation |
|---|---|---|
| Q1 | When insider or short interest fails *and* today's Congress/ARK batch ids differ from what the reused run was planned from, reuse that Congress/ARK cohort too, or relax the lineage check? | **Conditional cohort reuse of the whole prior publication's cohort** (Congress, ARK and the sibling downstream source; zero extra provider calls). Apply it only when upstream batch ids changed; with unchanged content, lineage already passes. It keeps the lineage invariant absolute, at the cost of up to one extra day of staleness for Congress and ARK in the changed-content case. Relaxing lineage means mixing today's candidates with insider data planned from yesterday's candidates. |
| Q2 | Should any source be truly optional (`required = false`, publish with that source empty)? | **Not in M2.** All four are required and reuse covers outages. An optional source changes the output's meaning and `_selected`'s all-four rule, so it deserves its own decision. |
| Q3 | Are market caps in M2 scope (§2.6), or do they wait for M4's `market_caps` source? | **In M2, minimal fallback.** One FMP outage blocks publishing today, which is exactly what M2 is meant to stop. D7 settles the 3-day limit, and the M4 decisions keep FMP's issuer-level company-screener as the authoritative cap under that same limit, so the fallback reuses the same freshness rule. |
| Q4 | Should SEV3 (degraded) make `cli health` exit non-zero? | **Keep exit 1 for now**, so the existing monitor still alerts. Add a `--fail-on SEV2` flag when M3 alerting routes severities separately. |
| Q5 | Enforce one `degraded` event per publication with a partial unique index (one small expand migration)? | **Yes**, as a tiny expand migration `0007` (M1-A's `0006` has merged). |
| Q6 | D6 defines the target as 10:30 PT on trading days. Which trading-day calendar should health use: weekdays only, or weekdays minus NYSE holidays? | **Weekdays minus a short NYSE holiday list in settings**, so holidays don't page as missed. The scheduler can keep running daily, and a weekend run is simply not required. |
| Q7 | Should a fresh batch that collects fine but is refused by quality or by the observation-date guard fall back to reusing that source's pinned batch, recorded as degraded with the refusal code, instead of aborting the whole publication? (Raised by the checker; it would also close X1-1's insider lockout without weakening the guard.) | **Yes, within the same reuse limits.** The refused batch is never published, the guard stays absolute, and one source's refusal stops freezing the watchlist. A named override remains the way to publish the refused batch itself. |

---

## 5. Out of scope

- Rollback (`cli rollback`, M3)
- API exposure of `degraded` (M5)
- `market_caps` as a tracked source with its own runs (M4)
- Partial per-symbol fallback inside a source; reuse is whole-batch only
