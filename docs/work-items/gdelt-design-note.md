# GDELT branch: design note for Stage C review

**Branch:** `GDELT` · **Author:** AalindCS · **Status:** proposal, no code changed yet · **Responds to:** `branch-sync-plan.md` Stage C (GDELT)

The branch was written before the database roadmap landed. This note lists where it departs from the roadmap and proposes the rework. Nothing below gets built until this is agreed.

## What the branch has today

| Item | Current design | Conflicts with |
|---|---|---|
| `0004_profile_sector_industry` | `sector`, `industry` columns on `market_cap_observations`, filled by `collect_market_caps` | M4 retires `market_cap_observations`; classification belongs in `company_profiles` |
| `0005_gdelt_macro_events` | `gdelt_theme_days` and `gdelt_spike_checks` (upserted), `gdelt_articles` (insert-only), written by `src/services/gdelt/job.py` | Rule 1 (one ingestion contract), the append-only `perennial` schema, rule 2 (derived data is a run), rule 5 (`lock_timeout`) |
| Revision ids | `0004`, `0005` | Planned `0004_publication_audit`, `0005_security_identity`, and auth's `0004` |
| `provision_roles.py` | `gdelt_theme_days`, `gdelt_spike_checks` added to `MUTABLE` | Append-only schema |
| `scheduler.py` | One `add_job` at 9:30 PT calling `run_scheduled` | Needs to fit the settings and session-settings work |

As far as we know, migrations `0004` and `0005` have only run on disposable local test databases, so renumbering is free. AalindCS will confirm this in the PR.

## Proposal

### 1. Sector and industry: drop `0004`, fold into M4

- Delete `0004_profile_sector_industry` and revert the `runner.py`/`publication.py` changes from commit `e5ced09`.
- `sector` and `industry` go in `company_profiles`. Either M4 delivers it, or we build a minimal early slice: `security_id`, `name`, `sector`, `industry`, `aliases text[]`, plus run lineage.
- GDELT does not need sector until the theme-to-sector mapping is filled, so nothing here blocks on timing.

### 2. GDELT through the ingestion contract

There is a gap in the contract today. `SourceRecord` requires `ticker`, and `_write_batch` resolves every record to a `security_id`. GDELT theme rows such as "ECON_OILPRICE on 2026-10-13" are not about any one security. Two options:

- **A. Extend the contract with security-less sources (recommended).** Add a `SourceRecord` variant without `ticker` and a source table without `_security_fk()`. `_write_batch` skips `_resolve_securities` for those sources. GDELT then gets runs, immutable batches, lineage, coverage and the shared failure codes like every other source. Two new values go in the `source` vocabulary:
  - `gdelt_theme_volume`: one record per theme per UTC day for the 3-month window. Fields: `theme`, `day`, `article_count`, `total_articles`, `avg_tone`. Each run writes a new batch, so no upserts are needed.
  - `gdelt_theme_articles`: up to 5 evidence articles per theme for the target day. Fields: `theme`, `day`, `url`, `title`, `domain`, `seen_at`.
- **B. Keep GDELT outside the contract with an ADR.** This is less work, but it breaks rule 1, and the rule says to avoid that.

Two notes for either option:

- GDELT must **not** join the watchlist publication's required set. A GDELT outage should never block `publish()`. With option A, that means the publication eligibility check has to read a fixed list of required sources instead of all of `SOURCES`.
- The collector keeps today's behavior: 5.5 s pacing, the shared `_get_json` for 429 retries, the most recent complete UTC day as the target, and per-theme failures recorded as partial coverage.

### 3. Spike checks become a derivation run

Following rule 2, a derivation run reads one `gdelt_theme_volume` batch, recorded through `run_dependencies`, and appends `gdelt_spike_checks` rows: `run_id`, `theme`, `day`, `article_count`, `baseline_median`, `ratio`, `is_spike`, `rule_version`. Readers take the latest succeeded derivation. Nothing is updated or deleted, and the `MUTABLE` grants go away. The rule parameters are stored with each run, per rule 6: 2x the 28-day median, at least 20 articles, at least 14 baseline days.

### 4. Migration and scheduler

- One GDELT migration with the next free revision id at merge time (Stage D). It sets `lock_timeout`, has a tested downgrade, and updates grants in the same PR.
- The scheduler hook is rebased onto the settings and session-settings work in whatever shape those land in.

## Questions for review

1. Option A or B for security-less sources? If A, should the contract change land as its own PR before the GDELT tables?
2. Should a minimal early `company_profiles` slice be built on this branch, or wait for M4?
3. How should GDELT stay out of the publication's required sources: a fixed tuple, or a flag in the `source` vocabulary?
4. Known data issue: a live pull on 2026-10-14 had missing days in GDELT's own timeline. We plan to treat missing baseline days as unknown rather than zero. Does that belong in the collector's coverage, or in the derivation rule?
