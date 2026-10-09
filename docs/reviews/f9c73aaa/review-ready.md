# Integration candidate ready for independent review

Branch: database-ingestion-pipeline
Base HEAD: 9adfa48485c413ed574f42a6aedda0228337d79a
Candidate SHA-256: f9c73aaaa202253f5932e2f15c2a4ca50022193d8c2ccef1de4359b36a9ce553
Review files: 69 (tracked and untracked source, tests, configuration and documentation)
Manifest: /private/tmp/perennial-pipeline-rig/candidate-manifest.json
Recompute: python3 /private/tmp/perennial-pipeline-rig/fingerprint_candidate.py
The aggregate hashes sorted `sha256  relative-path\n` lines; exact per-file hashes are in the manifest. No secrets, local provider data, agent tooling or generated caches are included.

## Verification

From repository root:
`TEST_DATABASE_URL=postgresql+psycopg://perennial_test@127.0.0.1:55471/perennial_test REQUIRE_DATABASE_TESTS=1 .venv/bin/python -m pytest development/backend/tests -q --tb=short`

Observed final result: **234 passed in 2.94s; zero skipped**, isolated PostgreSQL 17, synthetic provider responses only. `git diff --check` passed. Runtime code/test bytes were unchanged after this run; final edits were operations documentation only.

Proof includes real COLLECTORS end-to-end publication/replay with forbidden file fallback, importer-role publication/replay/second promotion and denied history mutations, local dump/restore content hashes, populated additive migration round-trip with actual CHECK name inspection, workflow overlap/release, stale published-input health, bounded retries and provider Retry-After handling.

Regression evidence: real contract baseline 2 passed/2 failed (missing coverage and wrong programming-error classification); schema baseline 3 failures after correcting test transaction setup; legacy scheduler regression failed before restoring it; health baseline 2 failures; market-cap deadline baseline 2 failures. Source seat recorded 19 malformed-numeric failures and 8 retry-policy failures before repairs. Do not describe every existing test as newly written test-first.

## Findings closed in this candidate

- D1: real downstream collectors accept upstream= supplied by runner; signature contract passes.
- D2: coverage complete=true only after all planned units succeed. ARK snapshot, Congress recent_window, downstream selected_universe. Real collector pipeline publishes.
- D3: restored legacy cron.py from fab7188, preserving original JSON jobs. Database scheduler has a separate opt-in entrypoint: python -m src.pipeline.scheduler. Documentation explicitly separates consumers and schedules; database path does not refresh JSON files.
- D4: downstream collector plans read pinned database rows; poisoned legacy files are never opened on the database path. Persisted dependencies match actual selected batches.
- D5: additive migration 0003 renames historical doubled checks, including PostgreSQL-truncated names, and reverses correctly. 0001/0002 history is unchanged. Missing lineage FK indexes and per-source started_at index added.

Additional integration: safe internal_error/class diagnostics; canonical hash v2 for new provider identity/history fields, preserving v1 batches; live-only health and published-input freshness; market-cap overall deadline; explicit immutable-table grants; mandatory DB CI; operations runbook and synthetic restore drill.

## Reviewer scope and important limits

Review the complete 0002 + 0003 and src/pipeline layer, not only the diff from HEAD: substantial root implementation was captured in the user's eight commits while the root session was offline. Include collectors/importer/roles and the new real-collector contract tests. Base HEAD alone is not the candidate; include untracked files via manifest.

This candidate is local and review-ready, not a completed production deployment or full roadmap. No commits/pushes/deployments by this resumed root or its seats. Local git has an origin tracking ref; remote push status was not independently checked.

- No live-provider entitlement/access/completeness proof; a full third FMP page now intentionally fails rather than publishing truncated data. Pagination limits may require provider/config work.
- Every source must succeed for a new publication; previous publication remains served after failure. Degraded fresh-run reuse is not implemented.
- Market-cap observations are pinned to publications, but market-cap failures do not have their own durable run/batch history yet.
- Source snapshots retain all unidentified trades conservatively; event reconciliation, time-versioned security identifiers, fund-level histories, and reference-aware retention deletion remain follow-up model work. Other rig's design-3-data-model.md was read as design input and not implemented.
- HTTP timeouts/deadline checks are bounded attempts and cooperative checks; synchronous requests cannot enforce an exact wall-clock cutoff for an indefinitely trickling blocked stream.
- Worker uses session advisory locks. Direct connection or session pooling required; transaction pooling unsupported.
- Legacy scheduler is restored compatibility code, with its pre-existing continue-on-error/raw exception logging behavior. Production database scheduler uses fixed exception-class diagnostics.
- Runtime role may update run state/current pointer/security metadata, but cannot change archived source/publication rows or symbol identities. Owner/admin privilege and login memberships must be audited at deployment.
- No managed DB provisioning, deployed logins, actual supervisor/alert routing, cloud backups/PITR, measured production RPO/RTO, load/failover test, or automatic retention purge. Local dump/restore is not PITR evidence. CI workflow was added but has not been run on GitHub by this team.

## Routing

User will route this packet and fingerprint to perennial-database checker and auditor on their separate daemon. Root has not contacted those seats. Sources and ops own their prior file boundaries; test_pipeline_contract.py remains the other team's file. Please return findings against this fingerprint. Preserve all existing commits and edits.
