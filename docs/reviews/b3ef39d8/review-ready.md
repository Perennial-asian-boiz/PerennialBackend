# October 7 candidate ready for focused independent recheck

Branch: database-ingestion-pipeline
Base HEAD: 9adfa48485c413ed574f42a6aedda0228337d79a
Candidate SHA-256: b3ef39d869e39beadce950118510db796a51b92e52a598a48d4265ce495df504
Review files: 73 (tracked/untracked source, tests, fixtures, config and docs).
Manifest: /private/tmp/perennial-pipeline-rig/candidate-manifest.json
Recompute: `.venv/bin/python /private/tmp/perennial-pipeline-rig/fingerprint_candidate.py`
The aggregate hashes sorted `sha256  relative-path\n` lines. No secrets, provider data,
local agent tooling, generated caches or coordination packets are included.

Roadmap: roadmap-20261007.md. Reviewed starting candidate: f9c73aaa. Prior packet
and manifest are preserved as review-ready-f9c73aaa.md and candidate-manifest-f9c73aaa.json.
Existing commits and uncommitted work remain intact. No commits/push/deployment,
live providers, credentials, permission-setting changes or new agents.

## Fresh verification

From repository root:

```sh
TEST_DATABASE_URL=postgresql+psycopg://perennial_test@127.0.0.1:55471/perennial_test REQUIRE_DATABASE_TESTS=1 .venv/bin/python -m pytest development/backend/tests -q --tb=short
```

**277 passed in 3.32s; zero skipped, zero failed.** PostgreSQL 17.7 on loopback
port 55471, isolated perennial_test server. Every pytest session creates and drops
its own random test database. HTTP is synthetic and dotenv is disabled in tests.
The database rig's port 55439 was untouched. Port 55471 was already accepting
connections when checked; version, directory and test identity were verified.
A duplicate Docker start failed to bind; only its empty never-started container was removed.

```sh
.venv/bin/python -m pytest development/backend/tests -m 'not db' -q
git diff --check
```

**228 passed, 49 deselected in 0.86s**; diff check exit 0. The candidate was
fingerprinted before and after verification: exactly unchanged, 73 files.
Seven source-seat handoff hashes independently match the current files.

Protected-file checks: test_pipeline_contract.py and all migrations match
f9c73aaa byte-for-byte; cron.py matches both f9c73aaa and fab7188 byte-for-byte.
Alembic ScriptDirectory reports the single head **0003**. No schema changes today.
The full suite also retains real-COLLECTORS publication/replay with forbidden file
fallback, immutable-role grants, synthetic restore, populated migration round-trip,
pinned lineage, publication freshness, locking, retries and Retry-After evidence.

## Findings addressed

Paths below are relative to development/backend/.

| Finding | Implementation files | Regression evidence |
|---|---|---|
| SEC-P1 | src/pipeline/scheduler.py | tests/test_scheduler.py uses real APScheduler run_job with chained, sentinel-bearing OperationalError from engine creation and pipeline execution, including simultaneous disposal failure. Logs, event message/traceback and context/cause are safe; disposal occurs. Also tests successful-run disposal errors. Fixed PipelineRunFailed is raised outside the handler; both context and cause are None. Baseline: 5 failures. |
| F3 | src/pipeline/lifecycle.py, src/ingestion/importer.py; source-owned diagnostics.py template | tests/test_lifecycle.py covers actual default-clock recovery after two transient errors, injectable/reset/capped backoff, interruptible shutdown, and false guarded-update lease loss. Real PG abandonment before import, before write and before failed-input recording returns failed/worker_abandoned, leaves finalized rows unchanged and writes no batch. Missing/source/mode mismatches still raise. tests/test_pipeline.py proves sanitized public workflow failure and preservation of the current publication. Baseline: 7 lifecycle + 2 pipeline + 1 diagnostic-template failures. |
| F2 | src/ingestion/collectors.py, diagnostics.py | tests/test_source_regressions.py fail_fast/planned_counts checks ten planned tickers, failure at ticker 3 or 1, exactly bounded retries and no later request/delay. Third-unit counts: planned=10, attempted=3, completed=2, failed=1, not_attempted=7. Malformed/parse failures also stop. ARK stops after a failed fund. Congress previously stopped its inner page loop only; now stops the entire collection. Successful pages dynamically extend the known plan. Baseline: 10 failures. |
| F1 legacy | fetchers/fmp.py, fetchers/short_interest.py, collectors.py | Actual legacy run/fetch/save files and consensus Congress signals match scratch-imported fab7188 golden output. Cross-provider pair buy_count=1; malformed Nasdaq field is null with the ticker kept. DB collectors cannot reach poisoned legacy functions. Conservative DB dedup and strict parsing remain separate. Baseline: 3 failures and 1 already-passing DB hash guard. |
| F1 database | src/pipeline/ranking.py | tests/test_ranking.py covers watchlist-v2 cross-provider count=1, same-provider count=2, largest-provider multiplicity, NULL provider, normalized key, deterministic tie/metadata selection, corrected popular_stable order, unchanged other signals, v1 golden canonical JSON bytes, historical v1 publication replay and new v2 publication/replay. Shared legacy fixture on real PG retains both raw ZZAA rows, preserves its pre-split hash and counts one buy. Baseline: 11 failures. |

Root changed the two READMEs to document the intentional consensus short-interest
alias fix, separate adapter functions, v2 key/per-provider counting and harmless
same-content snapshots when upgrading batch hash v1 to v2. Consensus.py itself
was unchanged today; its already-implemented short-interest key fix stays.

## Failing without the fixes

Original reviewed bytes were copied before editing to baseline-20261007/. Current
regression tests/fixtures were copied into regression-20261007/, whose runtime
code remains f9c73aaa. Both directories are under this packet's directory.
No stash, checkout or restoration over shared working files was used.

```sh
TEST_DATABASE_URL=postgresql+psycopg://perennial_test@127.0.0.1:55471/perennial_test REQUIRE_DATABASE_TESTS=1 .venv/bin/python -m pytest -c /private/tmp/perennial-pipeline-rig/regression-20261007/development/backend/pytest.ini /private/tmp/perennial-pipeline-rig/regression-20261007/development/backend/tests/test_scheduler.py /private/tmp/perennial-pipeline-rig/regression-20261007/development/backend/tests/test_lifecycle.py /private/tmp/perennial-pipeline-rig/regression-20261007/development/backend/tests/test_ranking.py /private/tmp/perennial-pipeline-rig/regression-20261007/development/backend/tests/test_pipeline.py -q --tb=short -p no:cacheprovider
```

**25 failed, 17 passed in 2.28s**: scheduler 5, lifecycle 7, ranking 11, pipeline 2.
Full output: root-baseline-final-20261007.txt.

```sh
.venv/bin/python -m pytest -c /private/tmp/perennial-pipeline-rig/regression-20261007/development/backend/pytest.ini /private/tmp/perennial-pipeline-rig/regression-20261007/development/backend/tests/test_source_regressions.py -k 'fail_fast or planned_counts or worker_abandoned_has_fixed or legacy_fmp_run or legacy_short_interest_run or database_congress_golden or database_collectors_cannot' -q --tb=short -p no:cacheprovider
```

**14 failed, 1 passed, 66 deselected in 0.39s**: F2 10, worker template 1, legacy
F1/boundary separation 3. DB hash guard passes on both baseline and candidate.
Full output: source-baseline-final-20261007.txt.

Initial development RED evidence: scheduler 4 failed/2 passed; heartbeat/importer
7 failed/3 passed; ranking unit 8 failed/2 passed; new publication 1 failed;
source F2 10 failed and legacy F1 3 failed/1 passed. Tests were developed
iteratively; not every final test was written before the corresponding fix.

## Golden fixtures and ranking version

The actual fab7188 FMP and short-interest run/save functions and consensus loader
produced tests/fixtures/legacy_json_golden_fab7188.json in scratch, with a frozen
UTC clock and synthetic fetches. Root independently verified its baseline source
hashes against git show fab7188. Generator: generate-legacy-golden-20261007.py
in this packet's directory. The same fixture drives legacy output, DB collector
and actual DB publication tests. Its Congress hash remains:
f853d51c0bb7dbfb061deec842bdcb1d4ed0e9707a90b9091fb831259c5692b6.

The preserved f9c73aaa ranking module generated tests/fixtures/ranking_v1_golden.json.
Unit and historical-publication tests compare replay's canonical JSON bytes with
that independent output, including after a new v2 publication is promoted.

Conservative dedup is unchanged apart from its deduplicate_proven_repeats name;
legacy deduplicate restores fab7188's first-row key. FMP parsers default to
preserved DB provenance; legacy run selects the original JSON fields explicitly.
The DB collector explicitly selects parse_short_interest_strict; the separate
legacy parser restores lenient fields/history without certifying DB collection.

v2 groups purchases by (politician_name.strip().casefold(), symbol,
transaction_date, trade_type.strip().casefold()). It keeps the largest provider
group per key; NULL is its own provider. Equal counts select the provider whose
lowest canonical row_number comes first. Kept rows supply dates and amounts.
Raw snapshots are untouched; saved watchlist-v1 publications retain v1 dispatch.

## Auditor probe

`.venv/bin/python /private/tmp/security-f9c73aaa-scheduler-probe.py`:

```text
sanitized_application_log= True
apscheduler_logs_raw_secret= False
exception_event_contains_raw_secret= False
engine_disposed= True
AssertionError: Candidate no longer reproduces; recheck fix
```

The old probe asserts that the vulnerability is present. Its assertion failure
and exit 1 are therefore expected after the fix. Real executor tests also inspect
the traceback and context/cause chain. Output: scheduler-probe-final-20261007.txt.

## Ownership and routing

Sources completed steps 3 and 4a under qitem-sources-20261007-findings and froze
its files: sources-status-20261007.md and sources-fingerprint-20261007.json.
The logical queue address could not nudge its terminal; root delivered the queue
reference to perennial-sources-work and verified acknowledgment. Only exact
one-time queue inspection/claim prompts were approved; no permission settings
or topology were changed. Root completed steps 1, 2, 4b, 4c and 5. Ops and every
database-rig seat received no work or messages during this implementation turn.

The database coordinator routes the **checker for F1–F3** and **security auditor
for SEC-P1**, against this fingerprint. Confirm the required GPT-6-Astra runtime
before security recheck; declared rig model alone does not clear the prior model
hold. Independent acceptance is pending. Root has not contacted those seats.

## Unchanged limits

This is a local review-ready candidate, not production deployment or completion
of the whole roadmap. Existing limits remain:

- No live-provider entitlement/access/completeness proof. FMP MAX_PAGES=3 still
  fails pagination_exhausted at 60 or more recent trades per chamber when page
  three is full. Provider/config work is the next production blocker.
- Every source must succeed before publication; previous output stays served on
  failure. Degraded fresh-run reuse is not implemented.
- Market caps are pinned to publications but failed market-cap collection has
  no independent durable run/batch history.
- Unidentified snapshot trades remain conservative. Ranking dedup is not event
  reconciliation. Identifier histories, fund-level histories and reference-aware
  retention deletion remain follow-up work. design-3-data-model.md is design
  input, not implemented schema.
- HTTP deadlines use bounded attempts/cooperative checks; synchronous requests
  cannot enforce an exact cutoff for an indefinitely trickling blocked stream.
- Session advisory locks require direct connections or session pooling;
  transaction pooling remains unsupported.
- Legacy cron.py retains its continue-on-error/raw-exception logging hazard by
  design. The DB scheduler now passes fixed-message, unchained failures to APScheduler.
- Runtime role can update run state/current pointer/security metadata, but not
  archived source/publication rows or symbol identities. Deployment must audit
  owner/admin privileges and authenticated login membership.
- No managed DB provisioning, deployed logins, supervisor/alert routing, cloud
  backups/PITR, measured production RPO/RTO, load/failover proof or automatic
  retention purge. Local dump/restore is not PITR evidence. GitHub-hosted CI has
  not run for this candidate. Remote push status was not independently checked.
