# Independent review findings: candidate b3ef39d8 (from the perennial-database rig)

Candidate `b3ef39d869e39beadce950118510db796a51b92e52a598a48d4265ce495df504` (73 files, base `9adfa48`).
Full report: `.local-tools/rigs/database/checker-report.md` § "Candidate b3ef39d8 (perennial-pipeline)".

| Finding | Reviewer | Verdict |
|---|---|---|
| F1 legacy and database | Checker | **Resolved** |
| F2 | Checker | **Resolved** |
| F3 | Checker | **Resolved** |
| SEC-P1 | Security auditor (GPT-6-Astra high, checked at start and end) | **Resolved. Accepted.** |

The checker recomputed the fingerprint at the start and the end of its review: unchanged. On its own database (port 55439) the full suite gave 277 passed, 0 skipped. Its other checks:
- **Invariants:** Alembic has a single head (`0003`). `cron.py` is byte-identical to `fab7188`. `test_pipeline_contract.py` and the migrations are unchanged.
- **F1, legacy:** the checker wrote its own harness and ran the real `fab7188` code and the candidate on the fixture's inputs. Both match the golden output (buy_count 1, malformed field null, ticker kept).
- **F1, database:** the Congress content hash is unchanged (`f853d51c…`) and all 3 raw rows are kept. v1 output is byte-identical to f9c73aaa. v2 counts 1 for a cross-provider pair and 2 for two rows from the same provider.
- **F2:** checked on all four collectors.
- **F3:** heartbeat backoff waits were 15/30/60 seconds, resetting after a success. A run abandoned in real PostgreSQL returns `failed/worker_abandoned` and leaves the finalized row unchanged.
- **Baseline failures:** reproduced exactly (25 failed / 17 passed, and 14 failed / 1 passed).

**F1-b, needs no action:** the checker asked for confirmation that the `consensus.py` short-interest fix may change legacy output. The user already decided this on 2026-10-07 (item 3 of the F1 decision): keep the fix and document it. The candidate does both, so this is closed.

**SEC-P1:** full report in `.local-tools/rigs/database/security-report.md` § "Candidate b3ef39d8". The fingerprint was unchanged at the start and the end of the review. `test_scheduler.py` passed 7 of 7 using the real APScheduler `run_job`. The probe prints False for both leaks; its exit code 1 is expected, because the probe asserts that the old bug still exists. The legacy `cron.py` logging hazard remains, as designed, and is not marked fixed.

## Round closed

**b3ef39d8 is accepted by both independent reviewers for SEC-P1 and F1 to F3.** No changes are requested. Any further edit needs a new fingerprint and a new recheck. This is not a deployment or whole-roadmap certification. FMP `MAX_PAGES` is the next production blocker.

**Committed by the user (via the database coordinator), 2026-10-07:** `38300d4` on `database-ingestion-pipeline`, on top of `9adfa48`. It contains the 37 changed files of b3ef39d8, and the fingerprint over the committed tree is unchanged. Not pushed.
