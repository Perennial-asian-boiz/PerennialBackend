# Independent review findings: candidate f9c73aaa (from the perennial-database rig)

Candidate `f9c73aaaa202253f5932e2f15c2a4ca50022193d8c2ccef1de4359b36a9ce553` (69 files, base `9adfa48`).
Both reviewers recomputed it at start and end: unchanged. Full reports (evidence, repro commands):
- Checker: `.local-tools/rigs/database/checker-report.md` § "Candidate f9c73aaa (perennial-pipeline)"
- Security: `.local-tools/rigs/database/security-report.md` § same heading

## Verdicts

| Reviewer | Verdict |
|---|---|
| Checker (Claude Opus 5.5) | **Accept with findings.** No correctness blocker. 234 passed; 0003 round-trips on populated data with identical content hashes; new indexes are used; `cron.py` is byte-identical to fab7188; D1/D2/D5 regression tests fail on 9adfa48 and pass on the candidate. |
| Security (Codex) | **Not accepted.** One High finding (SEC-P1). Roles/grants, 0003, retries, restore drill and CLI suppression checked out clean (128 focused tests passed). The seat also declined to sign off because its Codex runtime fell back from GPT-6-Astra to GPT-Reserve mid-review; it rechecks on a new fingerprint once Astra is available. |

## Must fix

**SEC-P1 (High), `src/pipeline/scheduler.py:25-28`, job registered at line 37.** `run_full_pipeline`
logs a sanitized message and then does a bare `raise`. APScheduler's executor (`run_job`) catches
it, calls `logger.exception`, and attaches the original exception to its error event, so the
**full unsanitized traceback** (driver/provider error text, possibly URLs or keys) is logged on
every scheduled failure. Repro (prints booleans only): `.venv/bin/python /private/tmp/security-f9c73aaa-scheduler-probe.py`.
Fix: raise a fixed safe exception `from None` (or report failure through an adapter without the
original), move engine construction inside the protected boundary, and add a regression test
that runs the **real APScheduler executor** with a sentinel-bearing chained error and asserts that
neither the logs nor the error event contain the sentinel.

## Should fix

- **F2 (Low–Medium), `collectors.py:529-541` (insider), `:602-618` (short interest): no fail-fast.** After the first failed unit the collection is already doomed, but the loops keep requesting the rest of the plan (up to 3 attempts / ~60 s each, up to 2000 units) while holding the pipeline lock: hours of wasted work and ~3× request amplification against a degraded provider. Fix: stop at the first failed unit (record the rest as not attempted), or cap the total collection wall time.
- **F3 (Low), `pipeline/lifecycle.py:54-61`.** The heartbeat thread exits permanently on its first exception or failed update, so one transient DB error gets a healthy worker abandoned after 10 minutes and its collection discarded. Safe (no partial batch), but `import_collection` raises instead of returning a failed result. Fix: retry heartbeats with backoff until the lease is really lost, and turn an inactive run into an explicit failed result.

## F1: user decision made: restore old counting

**Decision (user, 2026-10-07):**
1. The legacy JSON path (`fmp.run()` → `trades_congress.json` → `consensus.py`) behaves exactly like fab7188: cross-provider dedup of the same Senate trade, and a malformed short-interest number leaves that field null instead of dropping the ticker.
2. Database snapshots keep every raw row (multiplicity preserved for audit), but `ranking.calculate` counts Congress buys over a **cross-provider-deduplicated view**, so `buy_count` and the `popular_stable` order aren't inflated. Document the dedup key used.
3. **Keep** the `consensus.py` short-interest key fix (b): it repairs a real bug (signals were always null at fab7188). Note it in the README as an intentional output change.
4. Add regression tests: old vs new legacy output on a synthetic FMP + Watcher duplicate (buy_count 1), and the DB ranking on the same input (buy_count 1, with both raw rows still stored).

Original finding:

- **F1 (Medium): output semantics changed although `cron.py` is restored byte-identical.**
  (a) `fmp.py:219 deduplicate` no longer collapses the same Senate trade reported by both FMP and Senate Watcher, so `buy_count` doubles for those trades in the legacy `consensus.py` output **and** in `ranking.calculate` (which sorts `popular_stable` by it).
  (b) `consensus.py` now reads `short_interest_shares`, so short-interest signals that were always null at fab7188 are now populated.
  (c) The legacy short-interest parser now drops a whole ticker on any malformed number, where it previously left fields null.
  (Decision recorded above.)

## Production blocker to note (already a known limit, raised again because of its impact)

`fmp.py MAX_PAGES = 3` with strict pagination means live Congress collection fails with
`pagination_exhausted` whenever a chamber has 60 or more recent trades, so with real volumes the
live pipeline will likely **never publish**. This needs provider/config work before
`src.pipeline.scheduler` is worth running for real.

## Packet corrections (no action)

The coordinator's packet said 0003 "adds the lineage FK". It adds CHECK renames plus 4 indexes;
the FKs already existed in 0002. Root's own wording was accurate. Hash v2 creates one
duplicate-content snapshot per source after upgrade, which is harmless for selection and replay
and worth one README line.

## Next round

Fix SEC-P1, F1 (per the decision above), F2 and F3, then publish a new fingerprint and `review-ready.md`
noting which findings each change addresses. The reviewers recheck only those findings.
