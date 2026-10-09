# Checker report — perennial-database

Seat: db.checker · Assignment: qitem-20261005183826-b028f9647829861b (claim blocked by
`claim_destination_mismatch`; coordinator said proceed regardless) · Baseline: `fab7188`

## Status

| Phase | State |
|---|---|
| 1. Contract analysis + acceptance cases | **Done** (this document) |
| 2. Independent candidate verification | **Done** — frozen candidate `707aca1f4576be1f`: scoped acceptance, no checker blockers (see Final verdict) |

Environment verified: `.venv` Python 3.10.6; SQLAlchemy 2.0.54, psycopg 3.3.6, Alembic 1.20.0,
Pydantic 2.13.5, pytest 8.4.2. Isolated test PostgreSQL on 127.0.0.1:55439 accepting connections
(URL supplied by coordinator; not repeated here).

---

## Phase 1 — Contract risks (from reading fetcher code, not assumptions)

Severity: **H** = will produce wrong persisted data or false success if ignored; **M** = likely bug;
**L** = note.

### C1 (H) Fetchers report partial/failed collection as success
Every fetcher swallows errors and returns a smaller or empty list:

| Fetcher | Silent failure path | Effect if trusted |
|---|---|---|
| `ark.py` `fetch_fund_holdings` | 429/non-200/exception → `[]` per fund; `run()` continues with remaining funds | A snapshot missing a whole fund (e.g. ARKG) imports as a "changed" successful batch; `fund_count`/`total_weight` silently drop |
| `ark.py` `run()` | all funds fail → `[]` | Indistinguishable from empty universe |
| `fmp.py` `fetch_chamber_trades` | 401/402/403/exception → `break` mid-pagination | One chamber partially or entirely missing, still "success" |
| `fmp.py` `fetch_senate_watcher` | exception → `[]` | Senate watcher half missing |
| `fmp.py` `run()` | returns **`None`** (not `[]`) for missing key and for zero trades | Wrapper must not treat `None`/`[]` as empty success |
| `insider.py` `fetch_insider_activity` | 404, 429, non-200, exception all → `None` | Rate-limited/failed tickers look identical to "no insider activity"; parse exceptions swallowed in `parse_insider_buys` |
| `insider.py` `load_*_tickers` | missing/bad upstream JSON → `[]` | Zero tickers scanned → empty "success" |
| `short_interest.py` `fetch_short_interest` | non-200/timeout/exception → `None`, same as "no record" | `errors` list is **never appended to** anywhere in `run()`; `errors: []` proves nothing |
| `short_interest.py` `run()` | no candidate tickers → saves and returns empty | Empty success |

**Required of candidate:** the live wrapper must compute an explicit outcome (succeeded / failed,
with reason) from observed per-request results — e.g. count of failed requests/funds/chambers — not
from list emptiness or `errors[]`. Policy for *partial* collection (some funds/tickers failed) must be
explicit; per spec §5 the safe default is **fail the run** (no new batch) so latest-successful stays
on the previous complete batch. Insider "zero buys after all tickers answered" is a legitimate
successful empty batch; "zero buys because requests failed" is not.

Checker will verify by monkeypatching `requests.get` / fetch functions to fail for one fund /
chamber / ticker and asserting a failed run and unchanged latest-successful batch.

### C2 (H) Per-record retrieval timestamps would break dedupe
`fetched_at` appears at top level in all four outputs **and per record** in Congress (both FMP and
watcher parsers), insider, and short-interest records. Other volatile/derived top-level fields:
`total_*`, `multi_fund_tickers`, `single_fund_tickers`, `tickers_with_buys`, `popular_stable`/
`affordable_growing` summaries, `note`, `endpoints`. Canonical hash must be computed from the
normalized business records only (plus a hash-version constant), excluding every `fetched_at`.
Test: same fixture with every `fetched_at` changed → same `content_hash`, same batch, new run.

### C3 (H) Record ordering and repeated rows
- Congress/insider: genuinely repeated identical rows can exist (same politician/ticker/date/type
  only collapsed by `fmp.deduplicate` for *cross-source* duplicates; insider not deduped at all).
  Hash must keep multiplicity (no set semantics). `(batch_id, row_number)` must keep both rows.
- ARK output order is `sorted(fund_count, total_weight)` with ties in dict order; short-interest is
  sorted by ticker; Congress order follows API page order. If hash is order-sensitive, a reordered
  but identical snapshot becomes a new batch. Recommend canonical sort of records for hashing **and**
  deriving `row_number` from that canonical order, so identical content → identical rows. Whatever
  is chosen must be stated and tested.

### C4 (H) Dates
- `short_interest._normalize_date` returns the **raw string** on parse failure and `""` when missing.
- Senate watcher `parse_watcher_trade` returns raw `transaction_date` on parse failure (later dropped
  by `filter_recent`, but fixtures/files may still contain it) and always `disclosure_date: ""`.
- FMP `disclosureDate` may be `""`.
- Insider `transaction_date` is validated `%Y-%m-%d` by the parser.

Expected: `""`/missing → `NULL`; non-ISO non-empty date in a required field → validation failure of
the whole batch (spec §5 "fail the entire import on malformed required records"); never stored as
text in a `DATE` column. `history[].settlement_date` stays in payload only.

### C5 (H) Numbers / Decimal / null
- ARK `share_price` defaults to `0.0` in `parse_holdings` even when the API omits it; `weight`
  missing → `0.0`. A stored price of 0 is a fabricated value. Expect `0.0` → still allowed only if
  candidate documents it; checker will flag storing 0 where source was absent if distinguishable
  (it isn't after `parse_holdings` — note as a known-lossy upstream contract, keep column nullable).
- `total_weight` is a float sum across funds; no 0–100 CHECK (spec §3).
- Insider `value` is coerced `or 0` upstream; `shares` default `0`.
- Short interest ints/floats may be `None` → NULL.
- Money/weights must go `float → Decimal(str(x))`, never `Decimal(x)` (binary artifacts), into
  `NUMERIC`. Hash must use the same canonical numeric text so `1.0` vs `1` vs `1.00` behave
  deterministically.

### C6 (H) Short-interest naming at the boundary
Fetcher emits `short_interest`, `avg_daily_volume`. DB must store `short_interest_shares`,
`average_daily_volume` (spec §5). **Pre-existing bug, out of scope to fix:** `consensus.py:286-287`
reads `short_position_shares` / `average_daily_volume`, which no fetcher emits, so those signals
are always `None` today. The importer must map from the fetcher's real keys, not from what
consensus expects. Do not "fix" consensus in this sprint without the coordinator's say-so.

### C7 (M) Securities identity
- No fetcher supplies exchange or currency → these will be NULL on all real imports. The
  conflicting-exchange rejection is only exercisable when an exchange is supplied (fixture or API).
- Normalization should match existing `normalize_ticker` (strip/upper; reject `N/A`, `--`, `NONE`,
  `NULL`, `NAN`, empty). Class-share punctuation (`BRK.B` vs `BRK/B` vs `BRK-B`) must not be silently
  merged; treat as distinct or reject — document which.
- Security upsert must be race-safe (`INSERT … ON CONFLICT (symbol) DO NOTHING` then select) since
  concurrent imports of different sources share symbols.

### C8 (M) Source-as-of date
None of the four outputs carries a provider snapshot date (ARK's API response date is discarded in
`parse_holdings`). `source_as_of` should be NULL for all four unless derived from a genuine source
field; using `fetched_at` would be wrong (spec §3 "leave it unknown").

### C9 (M) Secrets in diagnostics
`fmp.py` sends `apikey` as a query parameter. `requests` exception messages and `resp.url` include
the full URL **with the key**. Any failed-run `error_summary` or retained diagnostic built from
exception text must redact query strings / `apikey=`. Also `DATABASE_URL` in connection errors must
be masked. (Flagged for the security auditor as well.)

### C10 (L) Scheduler / consensus must stay unchanged
`cron.py` calls `fmp.run()`, `ark.run()`, `insider.run()`, `short_interest.run()`, `consensus.run()`
and wraps each in `except`. Any change to fetcher return types (e.g. returning an outcome object)
must not break these call sites or `consensus.py` file reads. Insider and short-interest read
`trades_congress.json` / `ark_holdings.json` from disk — wrappers must keep writing those files.

### C11 (L) Python 3.10
Avoid `datetime.UTC`, `enum.StrEnum`, `typing.Self`, `tomllib`, `except*` — all 3.11+.

---

## Acceptance cases checker will run against the candidate

Against the isolated test DB, with synthetic fixtures only.

**Migrations**
- A1 `alembic upgrade head` on empty DB; `downgrade base`; `upgrade head` again.
- A2 Seven tables exist; FKs source→batch, source→security; unique `(source, content_hash)`;
  `(batch_id,row_number)` for congress/insider; `(batch_id,security_id)` for ARK/short-interest;
  index on ingestion_runs `(source, finished_at)`; indexes on source tables' `batch_id`;
  `timestamptz`, `date`, `numeric` column types; no 0–100 weight check.
- A3 Raw inserts violating FK / duplicate batch identity are rejected.

**Import semantics** (each source unless noted)
- B1 Fixture import → row counts, dates, NULLs, Decimals equal inputs.
- B2 Identical re-import → 0 new batches, 0 new rows, +1 successful run referencing old batch.
- B3 Only `fetched_at` changed → same as B2.
- B4 One field changed → new batch; previous batch rows intact.
- B5 Congress/insider fixture with two identical rows → two rows stored.
- B6 Reordered records → behaviour matches documented C3 rule.
- B7 Hash version constant present and included in hash.

**Failure / rollback**
- F1 Malformed required field in record N>1 → failed run, zero source rows and no batch from that
  attempt, error summary present and credential-free.
- F2 DB error mid-insert (e.g. forced constraint) → full rollback, failed run recorded afterward.
- F3 After F1/F2, latest-successful query still returns previous batch and its rows.
- F4 Collection failure (simulated fetch errors per C1) → failed run, no empty batch, latest unchanged.
- F5 Successful empty collection (insider all-answered, zero buys) → succeeded run, empty batch
  distinct from F4.
- F6 Diagnostic retention bounded (size/count) and policy documented; no secrets in stored input.

**Latest-successful**
- L1 Picks batch from most recent succeeded run even when it reused an older batch.
- L2 Never aggregates across batches; failed runs ignored; deterministic tiebreak.

**Securities**
- S1 Same symbol across sources/batches → same `security_id`.
- S2 Conflicting exchange for existing symbol → import rejected (failed run).
- S3 Invalid tickers rejected per C7.

**Concurrency**
- K1 Two simultaneous identical imports (separate connections/threads) → exactly one batch, both
  runs succeed (or one documented reuse), no unhandled IntegrityError.
- K2 Two simultaneous different imports sharing a new symbol → one securities row.

**Entry points / regression**
- E1 File entry point and direct-fetch wrapper call the same validate/persist functions.
- E2 All four fixtures import.
- E3 `consensus.py` and `cron.py` still import and run against JSON files (no network: patched).
- E4 Live integration attempt reported honestly (fixtures ≠ live evidence).
- E5 Missing/invalid `DATABASE_URL` → clear error, no credential echo.
- E6 Real `.env` untouched; `.gitignore` pre-existing edits and spec file preserved.

---

## Phase 2 — Candidate verification

### Incremental review 1 (18:5xZ) — `src/db/{config,session,models}.py`, `src/ingestion/redaction.py`

Method: read source; offline probes with `.venv` (config parsing on 5 malformed URLs, DDL compile
against the postgresql dialect, `paths.py` import, `redact()` on synthetic FMP/DB error strings).
Did **not** touch the shared test DB (implementer may be using it).

Verified good: config errors credential-free for all 5 probes; `hide_parameters=True`; DDL for all 7
tables + indexes compiles; `status_consistency` CHECK ties success↔batch and failure↔no batch;
partial latest-success index `(source, finished_at DESC, id DESC) WHERE succeeded`; batch-leading
unique keys serve batch_id reads; `fund_count = cardinality(funds)`; no 0–100 weight CHECK;
redaction strips URL credentials, query strings and `apikey=…`.

| ID | Sev | Location | Finding | Status |
|---|---|---|---|---|
| R1 | M | `db/config.py:18` | `Path(__file__).resolve().parents[2]` hardcodes depth, against repo convention. Verified `from src.services.consensus_watchlist.paths import ENV_PATH` imports and resolves to the identical file. | open |
| R2 | H | `models.py:184` + `ark.py parse_holdings` | `CHECK share_price IS NULL OR share_price > 0`, but upstream defaults a missing price to `0.0`. One holding without a price fails the **entire** ARK batch. Map `0`/`0.0` → NULL in the ARK validator (document as lossy upstream contract) or relax to `>= 0`. | open |
| R3 | H | `models.py:119-120` | `error_summary ≤ 1000` chars and `octet_length(diagnostics::text) ≤ 16384` are enforced server-side. If the importer doesn't bound *before* insert (jsonb text rendering ≠ `json.dumps` sizing), the failed-run insert itself violates the CHECK and the failure record is lost — precisely for large malformed inputs. Bound with margin and fall back to `diagnostics = NULL` + truncation marker if the failed-run insert errors. Will test with oversized input. | open |
| R4 | M | `redaction.py:13-16` | Quoted key forms leak: `{"apikey": "X"}` → unredacted (confirmed). Same shape for Python dict repr `{'apikey': 'X'}` (FMP passes `params={...,"apikey":...}`). Allow optional quote between key and `[=:]` and quoted values. | open |
| R5 | M | `models.py` congress/insider | Fetchers emit `""` for missing values (FMP `disclosureDate`, watcher `disclosure_date`, `district`, `asset_type`, `source_link`; possibly `trade_type`, `politician_name`). NOT NULL columns accept `""`. Validators must turn `""` → NULL for optional fields and reject for required ones. | open (verify in importer) |
| R6 | L | `models.py` | Column limits (symbol 20, source_link 2048, asset_description 500, company 300) and NUMERIC scales (`total_weight` 8, `value` 2, `shares` 6) silently truncate/round or raise opaque DB errors. Validators should enforce the same limits; hash should be computed from validated values so DB contents and hash agree. | open (verify in importer) |
| R7 | L | `models.py:105` | `ingestion_runs.batch_id` can reference a batch of a different `source`. Optional: composite FK `(batch_id, source)` → `source_batches(id, source)`. | suggestion |
| R8 | L | `config.py:30` | `mask_url` renders `***` as `%2A%2A%2A`. Cosmetic. | suggestion |

**Recheck at review 2** (bytes: importer `a4c3a44542`, schemas `e08188a50d`, redaction `0c4a8b1002`,
config `e85fa932cd`): R1 **resolved** (uses `paths.ENV_PATH`). R2 **resolved** (`zero_is_unknown`
→ 0.0 price stored NULL, probe confirmed). R3 **resolved in code** (`safe_diagnostics` allowlist +
half-size cap; summary truncated to 1000) — DB-level confirmation pending. R4 **resolved** (5/5
quoted/URL/dict-repr probes redacted). R5 **resolved** (`""`/whitespace → NULL optional, rejected
required; 21 field×value probes pass). R6 **resolved in code** (validator lengths match columns;
decimals quantized to column scale before hashing).

Observed and already fixed by implementer between probes: optional text with `Field(max_length)`
raised raw `TypeError` on `None` (pydantic 2.13) — schemas.py rewritten 12:00:54 using
`_bounded(...)`; regression test recommended.

### Incremental review 2 (19:0xZ) — importer/canonical/envelope/queries/files/collectors

Method: source read; offline probes with a fake `requests` session and patched ticker loaders
(no real local_data files read, no network, no DB).

Verified good: canonical v1 hash ignores `fetched_at` and record order, keeps multiplicity
(3 rows incl. duplicate → 3 canonical rows; dropping a duplicate changes hash); `row_number` from
canonical order; `source_as_of` in identity; batch insert `ON CONFLICT DO NOTHING` + re-select is
correct under READ COMMITTED; securities inserted in sorted order then `FOR UPDATE` (consistent
lock order); 40001/40P01 retried; validation/canonicalize catch-all records a failed run;
`safe_diagnostics` allowlist (free text and URLs dropped, probe confirmed); file entry treats
empty file as failure unless `allow_empty`; collectors fail the whole collection on any failed
unit; Congress missing key → `missing_credential`; latest-success query orders
`finished_at DESC, id DESC` over succeeded runs and follows reused batches.

| ID | Sev | Location | Finding (probe-confirmed unless noted) | Status |
|---|---|---|---|---|
| I1 | H | `importer.py:434-474` | Write loop catches only `ImportFailure`/`DBAPIError`/`SQLAlchemyError`. Any other exception after a DB write (hook `RuntimeError`, bug in `db_values`) rolls back and **escapes with no failed run**. Add a generic `except Exception` → `_record_failed_run(..., "internal_error", describe_exception(exc))` (then re-raise or return). By inspection; DB confirmation after migrations. | open |
| K1 | H | `collectors.py:311-325,351` | Short interest: HTTP 200 `{}`, `{"data": {}}`, `{"data": {"foo": 1}}` all count as "no record" success. Only an explicit `"data": null` (Nasdaq's no-record reply) should mean no data; a missing `data` key or a data object without `shortInterestTable` is malformed → fail unit. | open |
| K2 | H | `collectors.py:351-355` + `short_interest.parse_short_interest` | Parser swallows exceptions and returns `None`; collector then calls `tracker.ok()`. Row `settlementDate: 123` → success, 0 records (fetcher logs a warning). When data has table rows and parse returns `None`, fail the unit (`parse_error`). | open |
| K3 | H | `collectors.py:295` + `insider.parse_insider_buys` | Parser's outer `try/except Exception` returns the buys collected so far. Probes: `[value:"500000", good]` → success with **0** buys (good one lost); `[good, date:None]` → success with 1. Pre-validate every `Purchase` txn (numeric value, string date) before calling the parser, or surface the exception. | open |
| K4 | H | same | `Purchase` with non-ISO date (`10/01/2026`) or missing date is silently skipped by the fetcher's `except ValueError: continue` → success, 0 buys. Malformed required field must fail the unit. | open |
| K5 | M | `collectors.py:250-258` | `{"data": {}}` and `{"data": {"insider_transactions": {}}}` → successful zero-buy result via `.get(..., {})`/`.get(..., [])` defaults. Require `insider_transactions` and `recent` to be present. | open |
| K6 | M | `collectors.py:269-275,332` | Input universe: `load_congress_tickers()`/`load_ark_tickers()`/`get_candidate_tickers()` swallow read errors. Congress file missing + ARK present → success over half the universe. Check both input files are present and parseable (explicit), else fail `partial_input`. | open |
| K7 | M | `collectors.py:54-86` | No retry for timeout/5xx (fetcher had `MAX_RETRIES`). One transient timeout among hundreds of tickers fails the whole short-interest/insider collection (probe: single timeout → failed). Bounded retry before failing a unit. | open |
| K8 | M (decision) | `collectors.py:206-208` | FMP 402 ("free tier limit … stopping" in fetcher) is treated as failure. On a free key live Congress may never succeed. Needs a decision: fail (current) vs. accept truncation after ≥1 page with diagnostics. Not testable without live access. | question → coordinator |
| K9 | M | `collectors.py:231-232` | `filter_recent` silently drops records whose `transaction_date` isn't ISO (watcher passes unparsed raw dates through). Count non-ISO dates before filtering and fail if any. | open |
| I3 | M (design) | `queries.py:16-34` | Latest-successful includes `collection_mode='fixture'`; importing a fixture into a dev DB makes synthetic data "current". Suggest excluding fixtures by default (flag to include) or document. | question |
| I4 | L | `importer.py:448` | `finished_at` from app clock inside the tx, not commit time / DB clock; ordering across hosts can skew. Consider `clock_timestamp()`. | suggestion |
| I5 | L | `importer.py:372` | `prune_failed_diagnostics` runs in the failed-run transaction; if pruning errors, the failed run is lost. Separate tx or try/except. | suggestion |

**Coordinator decisions (19:09Z):** K8 FMP 402 = failed collection (current behaviour correct →
closed). I3 latest-success includes fixture/file/live by design → needs a doc note that fixtures
go only into a disposable DB (verify in docs at freeze). K7 retry optional → closed as accepted.

### PostgreSQL run 1 (19:1xZ) — checker-owned database `perennial_checker`

Owned DB created on the isolated :55439 server (implementer notified; `perennial_test` untouched).
Bytes: importer `2fbe3e256a` (changed since review 2), migration `20261005_0001_initial_schema.py`.
Script: synthetic records only; refuses to run unless the URL ends in `/perennial_checker`.

Migration: `upgrade head` → `downgrade base` → `upgrade head` OK; 7 tables in `perennial`.

| Case | Result | Evidence |
|---|---|---|
| B2 identical reimport reuses batch | PASS | 1 batch, 2 rows, 2 succeeded runs, `reused_batch` true |
| B4 changed snapshot → new batch, old kept | PASS | 2 batches, 4 rows |
| L1 latest = newest run; reuse of an **older** batch becomes latest | PASS | |
| L2 latest_rows not summed across batches | PASS | 2 rows with 2 batches present |
| F2 SQL error inside tx after writes | PASS | `database_error`; batches/rows/securities unchanged; +1 failed run |
| F3 latest-good unchanged after failure | PASS | |
| **I1** RuntimeError after writes | **PASS → I1 resolved** | rolled back; failed run recorded; did not escape (`importer.py:398` generic catch) |
| F1 malformed record 2 → failed run, zero rows | PASS | `validation_failed`, diagnostics value-free |
| S2 conflicting exchange rejected | PASS | `security_conflict` |
| S1 same symbol across sources → one security | PASS | |
| K1 12 concurrent identical imports | PASS | 1 batch, 3 rows, 12 succeeded runs, exactly 1 creator, no exceptions |
| K2 12 concurrent distinct imports, overlapping new symbols, mixed sources and opposite orderings | PASS | 22/22 securities, 0 duplicates, all succeeded, no deadlock surfaced |

15/15 passed. Outstanding for the frozen candidate: recheck K1–K6/K9 collector rewrite, A2 schema
detail review against migration, E1–E6 (entry points, all four fixtures, consensus/cron regression,
live attempt honesty, config errors, `.env`/`.gitignore` preservation), implementer's own test suite.

### Recheck 3 (2026-10-06 08:2xZ) — collector fixes + implementer suite

Bytes: collectors `b5d31288ac`, importer `2fbe3e256a`, schemas `0931bbc2b5`, files `22a24070ea`,
diagnostics `5fd24f91e1`, config `2be3d7c0d0`, migration `c9c367267f`; tests: conftest
`21798a17fb`, test_collectors `ca8adfb560`, test_contracts `71c751b135`, test_database
`8059545a42`, test_entrypoints `d69dda50cd`.

**Implementer suite** — checker-owned admin DB `perennial_checker_test` (created by checker; the
suite creates and drops its own `perennial_test_<hex>` per session):

```
cd development/backend
TEST_DATABASE_URL=postgresql+psycopg://<test-role>@127.0.0.1:55439/perennial_checker_test \
  ../../.venv/bin/python -m pytest -q -p no:cacheprovider      # 120 passed in 2.76s
../../.venv/bin/python -m pytest -q -p no:cacheprovider -rs    # 100 passed, 20 skipped (DB tests)
```
→ the 20 DB tests genuinely ran with the URL set (they skip without it). No leftover
`perennial_test_<hex>` database afterwards (databases: postgres, perennial_test, perennial_checker,
perennial_checker_test). Split: contracts 64, collectors 32, database 19, entrypoints 5.
Suite safety reviewed: loopback + "test"-in-name guard, routing query params rejected, real
`load_dotenv` disabled before fetcher import.

**Collector probes** — `scratchpad/probe_coll2.py` (fake `requests` session; synthetic upstream
files in a temp dir via patched `LOCAL_DATA_DIR`; `load_dotenv` disabled; no network/DB):

| ID | Probe | Result | Status |
|---|---|---|---|
| K1 | SI 200 `{}` / `{"data":{}}` / `{"data":{"foo":1}}` | failed (`collection_failed`) ×3; explicit `data:null` and empty rows → success 0 | **resolved** |
| K2 | SI row `settlementDate:123`, `interest:{…}` | failed ×2; good row → 1 record | **resolved** |
| K3 | insider `[value:"500000", good]`, `[good, date:None]` | failed ×2 | **resolved** |
| K4 | insider Purchase date `10/01/2026` / missing | failed ×2 (non-Purchase junk date still ignored, correct) | **resolved** |
| K5 | insider `data:{}` / `insider_transactions:{}` | failed ×2; `recent:[]` → success 0 (legit empty) | **resolved** |
| K6 | congress upstream file missing / corrupt (insider + SI) | `missing_upstream` ×3 | **resolved** |
| K7 | SI single timeout | failed — accepted per coordinator | closed |
| K8 | FMP 402 | failed — matches coordinator decision | closed |
| K9 | watcher rows `[date "2026/09/01", good]` | **succeeded with 1 of 2 trades** — malformed date row silently dropped by `fmp.filter_recent` | **open (M)** |

K9 detail: `collectors.py` docstring now documents this as "known upstream behaviour". Spec §5
says malformed required records fail the import; here the malformed row never reaches the
validator. Cheap fix: after `parse_watcher_trade`, count trades whose `transaction_date` is not ISO
(`_is_iso_date` already exists) and `tracker.parse_failure` if > 0, before `filter_recent`.
Needs implementer fix or explicit coordinator acceptance.

Note for security auditor: the `perennial_test` role on the test container is a superuser with
CREATEDB (fine for a disposable test container; should not be the documented app role).

## Process notes
- Queue claim fails for all seats with `claim_destination_mismatch` (daemon appends
  `@host-5794f4c8`). Coordinator directed proceeding without claim; not debugging further.
- `rig send parent-coordinator` returns "session not found"; coordinator messages arrive via the
  operator.


---

## Final verdict — frozen candidate `707aca1f4576be1f` (2026-10-06 ~08:40Z)

Handoff: qitem-20261006083005-a15558864343cdda (implementer). Fingerprint recomputed with the
command in implementer-report.md → `707aca1f4576be1f` (match). All runs below on checker-owned
databases on the isolated :55439 server unless stated; synthetic data only.

**Verdict: scoped acceptance. No open checker blockers.**

### Evidence (exact commands, from `development/backend`, repo `.venv`)

| Check | Command / method | Result |
|---|---|---|
| Fingerprint | implementer-report.md recompute command | `707aca1f4576be1f` ✓ |
| Untouched files | `git diff -- development/backend/src/services` | 0 lines (fetchers, consensus, paths, cron unchanged) |
| `.gitignore` | `git diff -- .gitignore` | only pre-existing local-tooling entries ✓; spec file still untracked ✓ |
| Real `.env` | `ls development/backend/.env` | does not exist — not created/read/edited ✓ |
| Suite (DB) | `TEST_DATABASE_URL=…/perennial_checker_test python -m pytest -q -p no:cacheprovider` | **133 passed** |
| Suite (no DB) | `python -m pytest -q -p no:cacheprovider` | 113 passed, 20 skipped |
| Temp DB cleanup | `pg_database` after runs | no `perennial_test_<hex>` left |
| Migration | `alembic downgrade base && alembic upgrade head` on `perennial_checker` | OK |
| Checker PG cases | `scratchpad/db_checks.py` (B2,B4,L1×2,L2,F1×2,F2,F3,I1×2,S1,S2,K1,K2) | **15/15 PASS** |
| Collector contracts | `scratchpad/probe_coll2_v3.py` (fake HTTP; repo fixtures as upstream files in temp dir) | K1–K6, K9 behave as specified; K7/K8 match coordinator decisions |
| README workflow | `python -m src.ingestion.cli import-file <src> tests/fixtures/<f>.json --fixture` ×2 per source | all 4 sources: pass 1 new batch, pass 2 `reused=True`; rows 5/3/3/2 = fixture counts; rc 0 |
| | `import-file ark_holdings … ` (no flag) | `collection_unattested`, rc 1 ✓ |
| | `cli latest short_interest` | follows reused batch ✓ |
| | README SQL (latest per source, ARK export, recent failures) | one batch per source; ARK export ordered; ✓ |
| | data checks | duplicate Congress rows kept (rows 1–2); `""` dates/district → NULL; ARK 0.0 price → NULL; weight 101.25 accepted; timestamptz/date/numeric/bigint types ✓ |
| | unreachable DB / malformed URL | rc 2 / rc 2, fixed messages, no password echoed ✓ |
| Suffix policy | `validate_records` + imports | `RKLB UQ`→RKLB/NASDAQ, `dkng uw`→DKNG/NASDAQ, `BRK.B UN`→BRK.B/NYSE; `HK`, `US`, 3-token rejected; UQ+explicit NYSE rejected, UQ+explicit NASDAQ ok; base+suffixed or two suffixes in one batch rejected (`duplicate_security`); DB: `RKLB UQ` then `RKLB UN` → `security_conflict`; Congress plain `RKLB` reuses NASDAQ security ✓ |
| Consensus/cron regression | import fetchers, `consensus`, `scheduler/cron.py` | import OK; `consensus.run`, `cron.main`, `run_full_pipeline` present |
| Live evidence honesty | **read-only** `BEGIN READ ONLY; SELECT …` on `perennial_test` | runs 1–5 and batch 1 (98 rows, as-of 2026-10-05, hash `2b95b166`, 98 securities / 4 with exchange) exactly match implementer-report table ✓ |

### Finding status (checker)

| ID | Status |
|---|---|
| R1 paths convention | resolved |
| R2 ARK 0.0 price | resolved (DB-verified) |
| R3 bounded error/diagnostics | resolved (closed vocabulary, <8 KB, failed run always inserts in tests) |
| R4 quoted-key redaction | resolved |
| R5 blank strings | resolved (DB-verified) |
| R6 lengths/scales | resolved |
| R7 run↔batch source | resolved (`fk_ingestion_runs_batch_source`) |
| R8 mask_url cosmetic | moot (function removed; fixed messages) |
| I1 RuntimeError failed run | resolved (DB-verified) |
| I3 fixtures in latest | accepted by coordinator; documented in README ✓ |
| I4 DB clock | not adopted — suggestion only, accepted |
| I5 prune tx | resolved (separate tx) |
| K1–K6, K9 | resolved (probe-verified on frozen bytes) |
| K7, K8 | closed per coordinator |

Auditor findings (SEC-xx) are the security auditor's to recheck; incidentally observed working:
redaction, diagnostics allowlist, test URL loopback/routing guard, `PG*` scrub.

### Scope limits (not blockers; state them in any handoff)

- **Live evidence is ARK only.** Congress, insider, short interest are fixture-verified only.
- **Compose startup not exercised** by implementer or checker (config validated only); the README
  setup/test path was followed against the provided isolated server instead.
- Sequence gaps in `source_batches.id` from `ON CONFLICT DO NOTHING` on reuse (cosmetic).
- `perennial_test` role on the test container is superuser — fine for a disposable container; the
  README already documents a least-privilege importer role for shared DBs.

### Checker-owned artifacts to clean up when the rig closes
Databases `perennial_checker`, `perennial_checker_test` on the :55439 test server (safe to drop).
Probe scripts live in the checker's session scratchpad only.

## Delta recheck — `f6cf94f5acbe24aa` (2026-10-06 ~08:45Z; qitem-20261006083336-cde7178ac46d329a)

Fingerprint recomputed → `f6cf94f5acbe24aa` (match). Files newer than the final verdict: exactly
`collectors.py`, `diagnostics.py`, `schemas.py`, `tests/test_collectors.py`, `tests/test_contracts.py`,
`development/database/README.md` (= claimed SEC-12/13 delta).

| Check | Result |
|---|---|
| Suite with DB (`…/perennial_checker_test`) | **136 passed** |
| Suite without DB | 116 passed, 20 skipped |
| Checker PG cases `db_checks.py` on `perennial_checker` | 15/15 PASS |
| Collector probes `probe_coll2_v3.py` | 26/26 outputs unchanged from 707aca (K1–K9 as before) |
| SEC-13 links | `…pdf` accepted; `…pdf#`, `…pdf#page=2` rejected (`validation_failed`); Senate efdsearch link accepted; all 5 fixture Congress links valid |
| SEC-12 short interest | 2001-ticker universe → `plan_too_large`, 0 requests |
| SEC-12 insider | plan 1993 → proceeds; plan 2004 (via ARK holdings) → `plan_too_large`, 0 requests. Note: Congress side is capped by the fetcher's `MAX_CONGRESS_TICKERS`, so only ARK size can trip it (correct, matches fetcher selection). |

**Verdict unchanged: scoped acceptance of `f6cf94f5acbe24aa`; no checker blockers.** Same scope
limits (live = ARK only; Compose startup unexercised). Residual note: a provider link with any
`#fragment` now fails the whole Congress batch — strict but safe; no fragments seen in fixtures or
known FMP/Senate link shapes.

### Compose startup (update 2026-10-06 ~08:50Z)

Replaces the "Compose startup not exercised" limit. **Parent-attested** (coordinator, 08:37Z):
fresh venv from exact `requirements.txt` pins, isolated Compose service started healthy on
127.0.0.1:55440, `alembic upgrade head` on the empty database, `createdb` test-admin workflow, full
suite 136 passed with no skips. **Checker-verified independently (read-only):** `pg_isready
127.0.0.1:55440` accepting; listener is Docker bound to `127.0.0.1:55440` only. Checker did not log
in — the service uses non-default credentials and checker did not guess them.

Remaining scope limit: live integration evidence is ARK only (Congress, insider, short interest
fixture-verified only). Verdict for `f6cf94f5acbe24aa` unchanged: scoped acceptance, no checker
blockers.

---

## Phase 1 — pre-read (2026-10-06 ~23:05Z; NOT findings, nothing sent)

Read-only pre-read per coordinator heads-up (22:59Z) of the never-reviewed layer, on moving bytes
(implementer mid-Phase 1). Items below are candidates to verify against the Phase 1 fingerprint;
any already covered by roadmap D1–D5 / P1–P2 tickets are marked so.

**Migration 0002**
- P-M1 downgrade: `abandoned → failed` is lossy (error_code `worker_abandoned` kept); publications,
  market caps, lineage, `coverage`/`collected_at`/`heartbeat_at` dropped. Refuses while any run is
  `running` (good). Verify populated round trip at gate.
- P-M2 0002 hard-codes doubled check names (`ck_ingestion_runs_ck_ingestion_runs_*`) — D5/P2-1.
- P-M3 legacy runs get `heartbeat_at = finished_at`, `collected_at`/`coverage` NULL → can never be
  published (intended per docstring).
- P-M4 no FK indexes on new tables — P2-2.

**lifecycle.py**
- P-L1 `heartbeat_worker` thread exits permanently on any exception (one transient DB error) →
  run later abandoned after 10 min although the worker is alive; long collections (insider/SI,
  hundreds of tickers × delay) are exposed. Check what `import_collection(run_id=…)` does when
  the run was abandoned underneath it (expect: no batch, clean failure, no exception leak).
- P-L2 `abandon_stale_runs` only runs inside `run_pipeline`; CLI `fetch` never recovers stale runs
  (there is a `recover-stale` CLI — confirm).
- P-L3 session advisory lock held on a pooled connection for the whole collection; keys
  170010+i / 170098 (pipeline) / 170099 (publish xact). Unlock failure → `invalidate()` (good).
  Won't work behind a transaction pooler (Phase 4 note already).

**publication.py**
- P-P1 coverage gate — D2.
- P-P2 regression guards: equal `collected_at` allowed (re-publish same runs ok); `as_of` compared
  as ISO strings (ok); a `None` new as_of where old had one → rejected (good).
- P-P3 short-interest gate rejects the **whole publication** if **any** row's settlement date is
  > 45 days old. A single ticker with stale Nasdaq data blocks all publishing. Verify intent.
- P-P4 `replay` determinism: caps quantized before `calculate` in publish and read back from
  NUMERIC(24,2) in replay; `created_at` µs round-trip; `ranking_config` JSONB round-trip — DB-test.

**runner.py**
- P-R1 D1 (`upstream=` kwarg), D4 (file-based plans vs DB lineage) — P1-1. `_upstream` maps only
  `ticker`; DB rows use `amount_label` not `amount_range`, etc. — P1-1 says map explicitly.
- P-R2 catch-all hides programming errors — P1-5.
- P-R3 `collect_market_caps`: one FMP request per candidate symbol, sequential, 300 s deadline, no
  pacing. Candidates = all ARK holdings + all Congress purchase tickers in the 2-year window
  (likely several hundred). FMP free-tier daily quotas would make daily publication impossible;
  the deadline may also be hit. Not testable live (no key); flag as Phase 4 / design input.
- P-R4 `item.get("currency", "USD")` treats a missing currency as USD (permissive).

**ranking.py / health.py**
- P-K1 output Decimals → JSON floats (precision loss in `market_cap`, `total_value`; deterministic).
- P-H1 health orders by `started_at` (index P2-2); fine otherwise.

**consensus.py (tracked file changed vs fab7188 — not mentioned in roadmap)**
- P-C1 `load_short_interest_signals` now falls back to `short_interest`/`avg_daily_volume`, fixing
  the pre-existing key mismatch (my C6). This **changes legacy JSON output** (short-interest
  signals go from always-null to populated), which can change consensus ranks. The original
  scope said keep the JSON path working and I advised leaving this bug alone without a
  coordinator decision. Needs explicit acceptance or revert.

**scheduler/cron.py** — D3 (legacy job gone; also runs a pipeline immediately at worker start,
which `fab7188` did not). P1-4 restores; will diff the restored default against `fab7188` at gate.

Environment note: root `CLAUDE.md` (referenced by roadmap line 5) no longer exists; root
`AGENTS.md` now describes a different rig (`perennial-pipeline-rig`, status files in
`/private/tmp/perennial-pipeline-rig/`). `rig whoami` still returns `db.checker` @
`perennial-database`; I am following the roadmap + `.local-tools/rigs/database/` scope/culture.

**Revision note (23:10Z).** Roadmap REVISION 1: the `perennial-pipeline` rig (Codex; root/sources/ops)
owns the pipeline, schema, collectors, CI, consensus and scheduler files. Checker role = that team's
independent review, starting only when the coordinator routes a queue row naming their
integration-ready fingerprint. The pre-read above is review input for that candidate (P-C1
consensus change and P-M/P-L/P-P/P-R items to verify there). No messages to, or edits of, the
other team's files; coordination is via the parent coordinator only. The AGENTS.md/CLAUDE.md
mismatch noted above is explained by this split.

---

## Candidate f9c73aaa (perennial-pipeline)

Routed: qitem-20261007065305-484c95dba4a680c3 (parent coordinator, 06:53Z). Branch
`database-ingestion-pipeline` @ `9adfa48` + uncommitted edits. Fingerprint
`f9c73aaaa202253f5932e2f15c2a4ca50022193d8c2ccef1de4359b36a9ce553` (69 files) recomputed with
`python3 /private/tmp/perennial-pipeline-rig/fingerprint_candidate.py` **at start and at end:
identical**. Read-only: no project file edited; pre-fix tree exported with
`git archive 9adfa48 development` into the checker scratchpad. All DB work on checker-owned
databases (`perennial_checker`, admin `perennial_checker_test`) on the isolated :55439 server.
Synthetic data only.

**Verdict: ACCEPT WITH FINDINGS** (no blocker; F1 needs a decision before production use of
either watchlist).

### Evidence

| # | Check | Command / method | Result |
|---|---|---|---|
| 1 | Full suite | `TEST_DATABASE_URL=…/perennial_checker_test REQUIRE_DATABASE_TESTS=1 .venv/bin/python -m pytest development/backend/tests -q -p no:cacheprovider` | **234 passed**, 0 skipped; no leftover `perennial_test_<hex>` |
| 2 | 0003 populated round trip | fixtures imported via CLI on head; `alembic downgrade 0002`; `upgrade head` | head: 0 doubled CHECK names, 23 checks, 4 new indexes; 0002: 19 doubled names restored, indexes gone; head again: 0 doubled, 4 indexes. Row counts + `md5(content_hash‖payload)` identical at all three points |
| 3 | Index use | `SET enable_seqscan=off; EXPLAIN` health latest-run, latest-success, run_dependencies(batch,source), publication_sources(run,batch,source), publication_market_caps(obs,sec) | all use `ix_ingestion_runs_source_started`, `ix_ingestion_runs_latest_success`, `ix_run_dependencies_batch_source`, `ix_publication_sources_run_batch_source`, `ix_publication_market_caps_observation_security` |
| 4 | Pre-fix regression (D1/D2/D5) | candidate `test_pipeline_contract.py`, `test_schema_hardening.py`, conftest, fixtures run against the 9adfa48 export | 6 failed / 1 passed: signature contract, real-collector e2e (`collection failed for insider_trades`), programming-error classification, all 3 schema-hardening tests fail pre-fix → pass on candidate. (Author's "2 passed/2 failed" baseline was an intermediate state; the committed base is stricter.) |
| 5 | Real-collector contract test quality | read `tests/test_pipeline_contract.py` | production `COLLECTORS`; HTTP stubbed only at `requests.Session`; poisoned `LOCAL_DATA_DIR` + `open`/`os.open` spies; asserts `run_dependencies` = upstream batches used, scopes, plan universe, `replay == current` |
| 6 | Retry arithmetic | fake clock/session against `_get_json` | 503,503,200 → ok after sleeps [1.5, 2.5]; 503×3 → `http_503` (3 req); timeout×3 → `timeout`; 429 Retry-After 5 → ok after 5.0; Retry-After 120 → `deadline_exceeded` after 1 req, no sleep; 404 and SSLError → 1 request, no retry |
| 7 | Pagination/coverage | source read | full 20-row page on `MAX_PAGES-1` → `pagination_exhausted`; coverage only set via `_Tracker.outcome` when `failure_count == 0` |
| 8 | Legacy scheduler (D3) | `git diff fab7188 -- scheduler/cron.py` | 0 lines — byte-identical. DB scheduler moved to opt-in `src.pipeline.scheduler`; pipeline advisory lock → overlapping worker gets `RuntimeError`, logged as class name only |
| 9 | Hash v1/v2 coexistence | same Congress fixture imported v1 (9adfa48 code), v2, v2, v1 | batch 1 (v1) and batch 2 (v2), each reused by its own version; both 5 rows with identical row digest; latest = newest run's batch; no cross-batch summing |
| 10 | Run abandoned under live worker | `begin_run`; age heartbeat 11 min; `abandon_stale_runs`; `import_collection(run_id=…)` | importer raises `RuntimeError("run is not active…")`; no batch written (4→4); run stays `abandoned/worker_abandoned` — safe |

### Findings

| ID | Sev | Location | Finding | Repro / evidence | Suggested fix |
|---|---|---|---|---|---|
| **F1** | **M** | `fetchers/fmp.py:219` `deduplicate`; `consensus.py:286-287`; `fetchers/short_interest.py` `_number`; `pipeline/ranking.py:54,80` | **Legacy JSON watchlist output changes although `cron.py` is restored byte-identical**, and the DB ranking inherits the same effect. (a) `deduplicate` no longer removes the same Senate trade reported by both FMP and Senate Watcher (only same-provider-ID repeats), so consensus `buy_count` doubles for those trades; `ranking.calculate` also counts `buy_count = len(c)` and sorts `popular_stable` by it. (b) `consensus.py` now picks up `short_interest_shares`/`short_interest`, so short-interest signals that were always null at `fab7188` are now populated. (c) The legacy SI parser drops a whole ticker on any malformed number (previously null fields). The packet lists conservative trade retention as a known limit but presents the legacy path as restored and does not mention ranking/score effects. | Old vs candidate `fmp.py` on one synthetic Senate trade from FMP + Watcher → rows 1 vs 2; `consensus.load_congress_signals` `buy_count` 1 vs 2, `distinct_buyer_count` 1 vs 1 (checker scratchpad script; synthetic data) | Decide explicitly. Either keep `fab7188` dedup semantics on the legacy JSON path (dedup in `fmp.run()` only) while DB snapshots keep multiplicity, and make the ranking count purchases over a cross-provider-collapsed view; or accept the new semantics in writing and document the ranking change. Same decision for (b) (my earlier pre-read item P-C1) and (c). |
| F2 | L–M | `collectors.py:529-541` (insider), `:602-618` (short interest) | **No fail-fast.** After the first failed unit (the collection is already guaranteed to fail), the loops keep requesting the remaining plan, each unit up to 3 attempts / 60 s. With `MAX_PLANNED_REQUESTS = 2000`, a degraded provider can keep one doomed collection running for many hours (~2000 × ≈50–60 s) while holding the pipeline lock, with up to ≈3× request amplification. | source read (no `break` after `tracker.fail`); retry bounds from row 6 | Stop at the first failed unit (record remaining units as not attempted in diagnostics), or cap total collection wall time. |
| F3 | L | `pipeline/lifecycle.py:54-61` | Heartbeat thread exits permanently on the first exception or a `False` update. One transient DB error during a long collection gets a healthy worker's run abandoned after 10 min, and its work is then discarded (row 10: safe, no partial batch, but wasted collection; `import_collection` raises instead of returning a failed result). | row 10 | Retry the heartbeat with backoff until the lease is actually lost; have `import_collection` turn an inactive run into an explicit failed result rather than an exception. |

### Notes (not findings)

- Coordinator packet says 0003 "adds the lineage FK"; 0003 adds only the CHECK renames and 4
  indexes (the lineage FKs already exist in 0002). The author's packet wording ("lineage FK
  indexes") is accurate.
- Hash v2: one duplicate-content snapshot per source after upgrade; mixed v1/v2 workers would
  alternate "latest" between equal-content batches. Harmless for selection and `replay`; worth one
  README line.
- Known-limit items not re-raised: FMP `MAX_PAGES = 3` makes live Congress fail with
  `pagination_exhausted` whenever a chamber has ≥60 recent trades, so with real volumes the live
  pipeline will likely never publish until pagination is reworked. The packet lists this; I am
  repeating it because it blocks production publication, not just completeness.
- Pre-read items now closed by this candidate: P-R1 (D1/D4), P-R2 (classification), P-M2/P-M4
  (0003). Still open as design items, no change requested here: P-P3 (one stale SI row blocks all
  publication) and P-R3 (one FMP request per candidate for market caps vs free-tier quotas).

---

## Candidate b3ef39d8 (perennial-pipeline)

Routed: qitem-20261007234449-bfe4f1e79ebbc9c3 (parent coordinator, 23:44Z): focused recheck of
F1–F3. Fingerprint `b3ef39d869e39beadce950118510db796a51b92e52a598a48d4265ce495df504` (73 files),
`.venv/bin/python /private/tmp/perennial-pipeline-rig/fingerprint_candidate.py` **at START and END:
identical**. Read-only; `PYTHONDONTWRITEBYTECODE=1` for all runs touching the other rig's
directories. DB work only on the checker server 127.0.0.1:55439 (`perennial_checker`, admin
`perennial_checker_test`); 55471 not used. Synthetic data only; no providers or credentials.
Claim still fails (`claim_destination_mismatch`); progress recorded via row notes.

**Verdict: F1 Resolved, F2 Resolved, F3 Resolved. No new issues.** One decision item to
confirm (F1-b below).

### Invariants

| Check | Result |
|---|---|
| `alembic heads` (development/database) | `0003 (head)` — single head |
| `git diff fab7188 -- …/scheduler/cron.py` | 0 lines (byte-identical) |
| `test_pipeline_contract.py`, migrations 0001–0003 vs `candidate-manifest-f9c73aaa.json` | all identical |
| Full suite: `TEST_DATABASE_URL=…55439/perennial_checker_test REQUIRE_DATABASE_TESTS=1 .venv/bin/python -m pytest development/backend/tests -q -p no:cacheprovider -rs` | **277 passed, 0 skipped** |

### F1 — Resolved

**Legacy.** Independent harness `scratchpad/legacy_harness.py` (written by checker, not the
author's generator): runs each tree's real `fmp.run()` / `short_interest.run()` / save functions and
`consensus.load_congress_signals` on the golden fixture's inputs, stubbing HTTP one level lower
than the author (`requests.get`/`requests.Session`, so `fab7188`'s fetch functions also run),
frozen clock from the fixture. Trees: `git archive fab7188` export and the candidate.

| Tree | trades_congress.json | short_interest.json | congress_signals | ZZAA buy_count (FMP+Watcher pair) | malformed SI field |
|---|---|---|---|---|---|
| fab7188 | matches golden | matches golden | matches golden | 1 | null, ticker kept |
| candidate | matches golden | matches golden | matches golden | 1 | null, ticker kept |

The fixture's `baseline_source_sha256` (fmp.py, short_interest.py, consensus.py) equals
`git show fab7188:<path>` for all three. The DB collector selects the strict parser and the
conservative dedup (`deduplicate_proven_repeats`); suite covers poisoned legacy functions.

**Database.** `scratchpad/db_side.py` run against the preserved `f9c73aaa` runtime
(`/private/tmp/perennial-pipeline-rig/baseline-20261007`, verified 35/35 src files equal the
`f9c73aaa` manifest) and the candidate, same golden inputs:

| | f9c73aaa | candidate | golden |
|---|---|---|---|
| Congress snapshot hash_version / content_hash | v2 / `f853d51c0bb7…` | v2 / `f853d51c0bb7…` | v2 / `f853d51c0bb7…` |
| raw DB rows (both ZZAA rows kept) | 3 | 3 | 3 |
| `ranking.calculate(..., version="watchlist-v1")` JSON bytes on a synthetic cross-provider + same-provider input | — | **identical to f9c73aaa** | |
| watchlist-v2 buy_count: AAA (fmp + senate_watcher same key) / BBB (two fmp rows) | unsupported | **1 / 2** (as documented) | |
| `ranking.VERSION` (new publications) | watchlist-v1 | watchlist-v2 | |

Historical v1 publication replay on real PG is covered by the suite (`tests/test_ranking.py`,
passing in the 277 run); my byte comparison of the v1 code path supports it independently.

**F1-b (decision, not a defect):** the consensus short-interest alias fix (`consensus.py`
reading `short_interest_shares`/`short_interest`) stays and is now documented as intentional in
both READMEs. It still changes legacy consensus output versus `fab7188` whenever the fetcher
returns real short-interest numbers (the golden fixture's SI value is null, so the golden test does
not exercise it). Coordinator/user should confirm they accept that change.

### F2 — Resolved

Fake-HTTP probes with `upstream=` (10 ARK-derived tickers) and direct ARK/Congress calls; then
`safe_diagnostics` on each outcome:

| Collector | Failure injected | HTTP requests | Result | safe diagnostics |
|---|---|---|---|---|
| insider | 503 on 3rd ticker | 5 (2 ok + 3 bounded attempts) | `partial_collection` | planned 10, attempted 3, completed 2, failed 1, not_attempted 7 |
| short_interest | 503 on 3rd ticker | 5 | `partial_collection` | same |
| ARK | 503 on first fund | 3 (ARKK only) | `collection_failed` | planned 4, attempted 1, failed 1, not_attempted 3 |
| Congress | 503 on senate page 0 | 3 (no house, no watcher) | `collection_failed` | planned 3, attempted 1, failed 1, not_attempted 2 |

### F3 — Resolved

- Heartbeat (`lifecycle.heartbeat_worker`, fake clock/wait, `heartbeat` stubbed: error, error,
  ok, ok, lease-lost): calls continued through both errors; waits `[15, 15, 30, 15, 15]` (backoff
  doubles, resets on success, cap `max_backoff=60`); stopped only on the false guarded update with
  `lease.lost = True`.
- Real PG on `perennial_checker`: `begin_run` → heartbeat aged 11 min → `abandon_stale_runs` →
  `import_collection(run_id=…)` returns **`failed / worker_abandoned`** (no exception), batches
  2 → 2, finalized run row (status, error_code, finished_at, error_summary) byte-unchanged.
  Wrong-source `run_id` still raises `RunIdentityError` (programming error stays loud).

### Baseline evidence spot-check

Author's commands, re-pointed at the checker server (`…55439/perennial_checker_test`):
`regression-20261007` runtime is identical to `baseline-20261007` (diff -rq clean), which equals
`f9c73aaa`.

| Command | Author | Checker |
|---|---|---|
| scheduler + lifecycle + ranking + pipeline tests on f9c73aaa runtime | 25 failed, 17 passed | **25 failed, 17 passed** |
| source regressions `-k 'fail_fast or planned_counts or …'` | 14 failed, 1 passed | **14 failed, 1 passed** |

### Not re-reviewed

SEC-P1 (scheduler logging) belongs to the security auditor. Unchanged known limits
(FMP `MAX_PAGES = 3` pagination blocker for live Congress, all-or-nothing publication, etc.)
were not re-raised.
