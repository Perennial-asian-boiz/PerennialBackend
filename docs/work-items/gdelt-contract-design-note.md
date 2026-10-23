# GDELT through the ingestion contract: design note

**Branch:** `docs/gdelt-contract` · **Author:** db-implementer · **Status:** proposal, no code changed · **Plan of record:** `docs/roadmap/database-roadmap.md` §3 M6 (GDELT follow-up), design rules 1, 2 and 6 · **Builds on:** `docs/work-items/gdelt-design-note.md` (AalindCS), Option A

The roadmap has already chosen the direction for the original note's questions:
1. Option A, with the contract change as its own PR first.
2. Wait for M4 for `company_profiles`.
3. GDELT is non-required through a flag in the source vocabulary.
4. Missing days are coverage gaps, treated as unknown and never as zero.

This note turns those answers into a concrete design and lists what's still open.

---

## 1. What exists today (origin/main 04b75f9)

| Piece | Today | Conflict |
|---|---|---|
| Tables (`0005`) | `gdelt_theme_days` (unique theme, day; **upserted**), `gdelt_articles` (unique theme, day, url; insert-or-ignore), `gdelt_spike_checks` (unique theme, day; **upserted**) | No batches, runs or lineage (rule 1). Rows change in place (append-only schema). `provision_roles` grants `MUTABLE` on two of them. |
| Job | `services/gdelt/job.py`: per theme, fetch volume, tone and articles, then upsert. A failed theme is logged and skipped. | Bypasses the importer, its failure vocabulary and its diagnostics. A partial run looks like success. |
| Spike rule | `spikes.py`: 2× the trailing 28-day median, ≥ 20 articles, ≥ 14 observed baseline days; **missing days inside the window count as zero** | Contradicts the settled answer 4. The rule parameters aren't stored with results (rule 6). |
| Readers | Only the job and `tests/test_gdelt.py` touch these tables | No reader migration is needed. |
| `SOURCES` | One tuple doing two jobs: the **source vocabulary** (CHECKs on `source_batches` and `ingestion_runs`, CLI choices, `lifecycle` lock keys, `queries`) and the **watchlist publication's required set** (`publication.py`, `runner.py`, `health.py`, `cli publish`) | Adding GDELT to the vocabulary today would make it a required publication source. |
| Contract | `SourceRecord` requires `ticker`; `_write_batch` always calls `_resolve_securities` | Theme-day rows aren't about a security. |

---

## 2. Proposal

### 2.1 Split the source vocabulary from the watchlist set (PR 1, contract only)

Replace the overloaded tuple with one registry in `db/models.py`:

```python
@dataclass(frozen=True)
class SourceSpec:
    table: Table
    record_model: type[BaseModel]
    subject: Literal["security", "none"]   # does each row resolve to a security?
    publication: Literal["watchlist"] | None  # which publication pins it, if any
    mode: Literal["collected", "derived"] = "collected"

SOURCE_SPECS: dict[str, SourceSpec] = {...}            # the closed vocabulary
SOURCES = tuple(SOURCE_SPECS)                          # unchanged name, now "all sources"
WATCHLIST_SOURCES = tuple(s for s, spec in SOURCE_SPECS.items()
                          if spec.publication == "watchlist")
```

- The publication, the runner's selection, the M2 per-source policy and the per-source part of `health` switch from `SOURCES` to `WATCHLIST_SOURCES`. This is the "flag in the source vocabulary" (answer 3). GDELT has `publication=None`, so it can never block `publish()`, and M2's `required` stays true for every watchlist source (Q2 is unchanged).
- The CHECKs on `source_batches.source` and `ingestion_runs.source` are generated from `SOURCES`, as today. CLI `import-file`/`fetch`/`latest` and `queries` keep using `SOURCES`.
- **Lock keys stay stable:** `lifecycle.source_lock` uses `170010 + SOURCES.index(source)`. New sources are appended, so existing keys don't move. A test pins the four current keys.
- **PR 1 changes no behaviour:** the registry holds the same four sources. It lands before any GDELT schema, as the roadmap asks (answer 1).

### 2.2 Records without a security (PR 1)

- **Record models:** a `GlobalRecord` base with no `ticker` (same strictness: `extra="forbid"`, bounded strings, no NaN), and one model per GDELT source.
- **Importer:** `_write_batch` skips `_resolve_securities` when `spec.subject == "none"`. Content hashing, `(source, content_hash)` de-duplication, `row_number`, concurrency and failure recording are unchanged.
- **Tables:** a subject-less source table simply omits `_security_fk()`.
- **Collection mode:** the `collection_mode` vocabulary gains `derived` (§2.4). Widening a CHECK is expand-only.

### 2.3 Two GDELT sources (PR 2)

| source | one row per | columns (besides `batch_id`, `row_number`) | constraints |
|---|---|---|---|
| `gdelt_theme_volume` | theme × UTC day in the window | `theme` varchar(64), `day` date, `article_count` int, `total_articles` bigint null, `avg_tone` numeric(8,4) null | `article_count ≥ 0`; theme matches `^[A-Z0-9_]{1,64}$`; unique (batch_id, theme, day) |
| `gdelt_theme_articles` | evidence article for the target day | `theme`, `day`, `url` text, `title` varchar(500) null, `domain` varchar(255) null, `seen_at` timestamptz null | `url ~ '^https?://'`, `length(url) ≤ 2048`; unique (batch_id, theme, url) |

- **Collector:** `collect_gdelt_volume()` and `collect_gdelt_articles()` return `CollectionOutcome`s through the shared contract, reusing today's `DocApiClient`, 5.5 s pacing, `_get_json` 429 handling, theme map and evidence-domain filter (≤ 5 per theme).
- **Coverage (answer 4):** `{"complete": <every theme fetched>, "scope": "window", "window_days": 90, "target_day": "…", "missing_days": {"ECON_OILPRICE": ["2026-10-09"]}}`. A day absent from GDELT's own timeline is a recorded gap. It's never stored as a zero row.
- **Partial runs:** the contract's rule today is that success means every unit succeeded; otherwise the run fails with `partial_collection` and readers keep the last good batch. This note keeps that rule for GDELT (open question G1). It matches the M3 follow-up, where `scheduler --once --job gdelt` already exits 1 on any failed theme.
- **`source_as_of`:** the target day (the most recent complete UTC day). It's monotonic, which is the X1 contract.

### 2.4 Spike checks become a derivation run (PR 2, rule 2)

A derivation is an ordinary `ingestion_runs` row with `source = 'gdelt_spike_checks'` and `collection_mode = 'derived'`:
- Its `run_dependencies` names the exact `gdelt_theme_volume` batch it read, through the same lineage table insider and short interest use.
- Its output goes through the importer as a batch of `gdelt_spike_results`:

| column | |
|---|---|
| `batch_id`, `row_number` | contract |
| `theme`, `day` | the target day per theme |
| `article_count` | that day's count |
| `baseline_median` numeric(12,2) null, `observed_baseline_days` smallint | computed only from observed days |
| `ratio` numeric(10,2) null, `is_spike` boolean null | null when the baseline is too thin to judge |
| `rule_version` varchar(16) | e.g. `spike-v2` |

- **Rule parameters (rule 6):** `{ratio: 2.0, baseline_days: 28, min_baseline_days: 14, min_articles: 20}` are stored with the run in `coverage.rule` (bounded JSON from settings, never collector text). Replaying a derivation doesn't depend on today's settings.
- **`spike-v2` (answer 4):** missing days inside the baseline window are **unknown, not zero**. The median uses observed days only, and fewer than `min_baseline_days` observed days gives `is_spike = null`. This is a deliberate change from today's rule (v1 treats a missing day as zero, which drags the median down and makes spikes easier). Open question G2.
- **Readers** take the latest succeeded `gdelt_spike_checks` run's batch, as the watchlist does for sources. Nothing is updated or deleted, and the `MUTABLE` grants go away with the old tables (§2.5).
- **One input batch per run.** A derivation reruns only when the volume batch changes. An identical result re-imports to the same content hash, so no new batch.

### 2.5 Migration (PR 2; number assigned at merge, likely after M4's 0008)

Expand only:
1. Set `lock_timeout`.
2. Widen the `source` CHECKs (`source_batches`, `ingestion_runs`) with the three GDELT sources, and the `collection_mode` CHECK with `derived`.
3. Create `gdelt_theme_volume`, `gdelt_theme_articles` and `gdelt_spike_results`, with the batch FK (`batch_id`, `source`) as for the other source tables.
4. Add the three tables to `provision_roles.IMMUTABLE`, in the same PR.
5. Write a tested downgrade that drops the new tables and restores the narrower CHECKs (refused if contract rows exist, so a downgrade can't orphan runs).

- **Old tables:** `gdelt_theme_days`, `gdelt_articles` and `gdelt_spike_checks` are **left untouched**. The job stops writing them in PR 2. A later contract migration (with `MUTABLE` grants removed) drops them after one release, under the expand-then-contract rule.
- **No backfill** (open question G3): nothing reads the old tables, and the first contract run re-collects GDELT's 90-day window.

### 2.6 Scheduling and health (PR 2)

- `scheduler --once --job gdelt` runs `collect_source` for `gdelt_theme_volume` and `gdelt_theme_articles`, then the derivation. Exit 0 only if all three succeed. The source locks prevent overlap; the watchlist's pipeline lock isn't involved.
- **Health:** GDELT gets its own freshness entry (`gdelt_theme_volume:stale` and so on, against a GDELT-only policy, default 2 days). Its severity is **SEV3**, because it never blocks or degrades the watchlist publication. GDELT failures never appear in the watchlist's `publication:*` issues.

### 2.7 Unchanged

The theme map (`theme_sector_map.json`), the DOC API client and its pacing, the evidence-domain rule, five articles per theme, and the 09:30 PT schedule slot.

---

## 3. PR sequence

| PR | Contents | Behaviour change |
|---|---|---|
| 1 | `SOURCE_SPECS` / `WATCHLIST_SOURCES` split; `GlobalRecord`; importer skips security resolution for subject-less specs; `derived` mode in code only (no migration yet) | None. Same four sources, and lock keys pinned by a test. |
| 2 | Migration (expand); GDELT collectors and record models; `gdelt_spike_checks` derivation with `spike-v2`; scheduler and health wiring; job stops writing the old tables | GDELT runs, batches and lineage; spike rule v2 |
| 3 (one release later) | Contract migration: drop the three old tables and their `MUTABLE` grants | None for readers |

---

## 4. Test plan

1. **PR 1 equivalence:**
   - The full suite is unchanged.
   - `WATCHLIST_SOURCES` equals the old `SOURCES`, and the four lock keys are pinned.
   - A synthetic subject-less source imports without touching `securities`.
2. **Contract for GDELT:**
   - Fixture volume and article payloads import as batches with runs, coverage and canonical hashes.
   - Identical re-collection reuses the batch.
   - Any failed theme gives `partial_collection`, and the last good batch is still read.
   - Missing timeline days appear in `coverage.missing_days`, never as zero rows.
3. **Derivation:**
   - It records `run_dependencies` to the exact volume batch and stores its rule parameters.
   - `spike-v2` treats missing days as unknown, with boundary tests at 13 and 14 observed days, at 2.0×, and at 20 articles.
   - Re-deriving unchanged input reuses the batch.
   - A derivation over an older volume batch replays identically.
4. **Isolation:**
   - A GDELT outage leaves `publish()`, the runner selection and the watchlist health untouched.
   - GDELT staleness is SEV3 only.
5. **Migration:**
   - Round trip and `alembic check`.
   - The downgrade refuses while contract rows exist.
   - The grants test shows INSERT/SELECT only on the new tables.
6. **Scheduler:** `--once --job gdelt` exits 1 on any failed theme or a derivation failure, and 0 only on full success. Logs carry codes and counts only.
7. **Pre-change evidence:** each behaviour test fails on the pre-PR base, as for M1–M3.

---

## 5. Open questions for Bryan

| # | Question | Recommendation |
|---|---|---|
| G1 | When one theme fails (often a 429), should the whole GDELT batch fail and keep yesterday's data, or should a partial batch with `complete: false` be stored and used for the themes that succeeded? | **Fail the whole batch** (the contract's rule), with the existing 429 retries. Partial batches would need per-theme coverage semantics in every reader. Revisit if live runs fail often. |
| G2 | Accept the `spike-v2` change that missing days are unknown, not zero? It makes spikes rarer on themes with patchy timelines. | **Yes.** It's the settled answer 4, and each result stores its `rule_version`, so v1 and v2 results are never mixed silently. |
| G3 | Backfill the old tables' history into batches, or start fresh from the first contract run? | **Start fresh.** Nothing reads the old tables, and GDELT re-serves 90 days. |
| G4 | Should derivations reuse `ingestion_runs` with `collection_mode = 'derived'` (proposed), or get a separate `derivation_runs` table? This sets the pattern for M6's FinBERT sentiment and Confidence Score inputs. | **Reuse `ingestion_runs`.** It gets lineage, locking, heartbeats and failure codes for free, and one run model is simpler to operate. |
| G5 | What GDELT freshness limit and severity should health use? | **2 days, SEV3.** GDELT is informational until M6 scores consume it. |
| G6 | Should PR 1 (contract split, no behaviour change) be scheduled ahead of M4's importer changes, given that both touch `importer.py`? | **Yes, PR 1 first.** It's small, and M4's identity resolution then plugs into `subject = "security"` cleanly. |

---

## 6. Implementation notes from review (db-checker, 2026-10-22)

- **G-a:** PR 1's `subject = "none"` skip must also bypass M4's new security resolution in the importer (`_resolve_securities` / `ROOT_SOURCES` on `feat/m4-identity`). Settle G6's ordering when M4 merges, and rebase whichever lands second onto the other.
- **G-b:** `health._severity` defaults to SEV2. GDELT staleness needs an explicit SEV3 branch, and the per-source health loop must iterate `WATCHLIST_SOURCES`, not `SOURCES`, or GDELT would page as a watchlist outage.
- **G-c:** `queries.latest_successful_run` (and any reader filtering on `collection_mode == 'live'`) must accept `derived` for the `gdelt_spike_checks` source.

## 7. Out of scope

- **Company-level `event_company_impacts`:** needs M4's alias names and match confidence (roadmap M6).
- **`company_profiles` and moving sector/industry:** M4.
- **D8 terms-of-service review of GDELT:** before cutover.
- **Any change to the DOC API client or the theme map.**
