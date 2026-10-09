# Independent database security audit

**FINAL SCOPED ACCEPTANCE — October 6, 08:37–08:40Z, GPT-6-Astra high independently confirmed in terminal before and after verification. Candidate f6cf94f5acbe24aa independently fingerprinted before and after verification. SEC-04/08/09/11/12/13 are resolved on fresh Astra checks; no remaining blocker in the reviewed database security scope.** This conclusion supersedes the provisional Sol review and model hold below. Exact evidence and remaining scope limitations appear in the final section; historical sections are retained as an audit trail.

Live identity verified with project-local rig whoami: db.security-auditor, Codex runtime. Coordinator reports exact terminal model GPT-6-Astra. The identity command does not expose the active model, so exact model is coordinator-attested, not independently verified. Tool execution recovered after coordinator restored the missing code-mode host.

Scope read: CULTURE.md, scope.md, docs/technical-specs/DATA_PLATFORM_SPEC.md. Baseline HEAD verified fab7188. At inspection no database implementation files or candidate diff were present. No project code, real environment files, credentials, provider payloads, or databases changed/read by this audit.

## Actionable baseline findings for candidate integration

### SEC-01 — Raw FMP exception logging can expose API credentials

Evidence: development/backend/src/services/consensus_watchlist/fetchers/fmp.py:60 places FMP_API_KEY in request query parameters; line 87 prints the full exception. Request failures can include the request URL. An isolated AST-extracted function test with a mocked request raising a synthetic URL-bearing exception confirmed the sentinel appeared in captured stdout and the failed collection returned an empty list. No network, actual key, or module-level dotenv load was used.

Required candidate behavior: database direct-fetch path must prevent secret-bearing exceptions from reaching stdout/stderr, error summaries, JSON diagnostics, chained tracebacks, or SQL exception parameter dumps. Prefer bounded allowlisted error codes/statuses over raw exception text. Verify using synthetic credential sentinels, including URL query strings and database connection errors. Existing scheduler behavior must remain compatible.

### SEC-02 — Existing collection return values cannot attest completeness

FMP returns accumulated/empty rows after HTTP failures (fmp.py:64-90); ARK accepts whichever funds returned rows and prints success (ark.py:243-267); insider maps HTTP failure to None (insider.py:147-165); short interest maps HTTP failure and no data to None (short_interest.py:243-272). These are baseline integration hazards, not findings against an unseen candidate. Direct wrappers must track attempted versus completed units and distinguish valid empty results, malformed responses, and failed/partial collection. Latest-good snapshots must survive failure.

## Candidate checks still required

- Parameterized SQL, fixed identifiers, and synthetic injection-looking field values stored as data.
- File byte limits before JSON parse, direct-input row/string/depth limits, bounded network/retry behavior, and safe error handling for oversized/malformed input.
- Failed diagnostics: allowlisted credential-free metadata, explicit per-record and total retention bounds/expiry, no raw failed payload archive; unknown payload keys and free-text links must not silently archive credentials.
- SQLAlchemy logging/exception parameter exposure, Pydantic validation input disclosure, CLI traceback chains, and provider logger/stdout behavior.
- Loopback-only Compose publication, development-only credentials, documented DB privilege boundaries, and no accidental production/shared database test target.
- Isolated PostgreSQL test ownership and cleanup; inspect target checks before executing tests. All probes use synthetic inputs.

Remaining blocker: implementer must provide candidate and isolated test instructions/evidence. No candidate tests or live integration have been run by this auditor. Findings require independent recheck after fixes.

## Incremental candidate review — config/session/models

Reviewed development/backend/src/db/config.py, session.py, models.py following coordinator assignment at 18:49Z. Review applies to these work-in-progress bytes, not a frozen final candidate. Coordinator identifies perennial-db-test-20261005 on localhost:55439 as isolated; auditor has not connected to or mutated it.

### SEC-03 — Medium: malformed configuration discloses input in exceptions (RESOLVED after independent recheck)

config.py:45-47 catches only SQLAlchemy ArgumentError. A URL with nonnumeric port raises ValueError whose text includes the original port. config.py:48-51 also embeds the untrusted URL scheme in DatabaseConfigError. This violates the documented credential-free error contract if malformed configuration has credential material in these components.

Independent offline probes using .venv/bin/python and explicit synthetic strings confirmed:

- Nonnumeric synthetic-secret port: ValueError; sentinel present in message and formatted traceback.
- Synthetic-secret unsupported scheme: DatabaseConfigError; sentinel present in message and formatted traceback.

Fix: catch malformed URL ValueError as well as ArgumentError, suppress exception chaining, and return fixed allowlisted error messages without interpolating URL-derived scheme/port text. Recheck CLI and migration boundary behavior so driver errors cannot bypass this protection. Add synthetic regression assertions over stderr/traceback as well as str(exception).

### Observed protections and limits

- Engine construction with an explicit synthetic URL (no connection) verified hide_parameters=True. This hides SQLAlchemy bound parameter dumps; it does not sanitize database-origin error detail or every driver exception. Final caller boundaries remain to be reviewed.
- Compiling a securities insert with a synthetic sentinel under the PostgreSQL dialect kept the sentinel outside SQL text and in bind parameters. No dynamic user-controlled SQL identifiers found in these three files. models._in interpolates fixed internal constants only.
- Seven tables are declared, with fixed schema and FK/unique/check constraints. Error summaries have a 1,000-character database bound; JSON diagnostics have a 16,384-byte database bound. Total retained history, pre-parse limits, and diagnostic content controls are not implemented in these reviewed files and remain importer/retention review items.
- mask_url removed password/query values in the synthetic query test. It is not a universal secret sanitizer: username, host, and database are still rendered. Avoid using the rendered URL in generic credential-free errors.

Verification used installed project .venv dependencies, explicit synthetic URLs, no network, no real .env read, and no DB connection. Default system Python lacked dotenv; its failed import performed no probe. No implementation files edited. SEC-01/02 remain pending until direct-fetch integration is available; SEC-03 needs a fix and independent recheck.

SEC-03 partial recheck at 18:50Z: implementer expanded make_url exception handling to ArgumentError/ValueError/TypeError with suppressed chaining. Independent rerun confirms malformed-port sentinel is absent from message and formatted traceback. Unsupported-scheme sentinel remains present in both because config.py still interpolates url.drivername. Finding remains OPEN for that part; no additional database or network access used.

SEC-03 final recheck after implementer notification at 18:51Z: both malformed-port and unsupported-scheme cases now raise DatabaseConfigError without synthetic sentinel in either message or formatted traceback. Supported postgres, postgresql, and postgresql+psycopg schemes continue to normalize successfully. All assertions passed under project .venv with explicit synthetic URLs and no connections. SEC-03 is RESOLVED for these config-parser disclosure paths. This does not constitute final importer/CLI/migration error-boundary sign-off; those remain pending full candidate handoff.

## Incremental importer and payload review — 18:57Z assignment

Inspected ingestion/importer.py, schemas.py, canonical.py, envelope.py and redaction.py. Probes ran with PYTHONPATH=development/backend .venv/bin/python, used only synthetic dictionaries/strings, and made no DB or network calls. Findings delivered directly to implementer with rig send. No implementation files changed.

### SEC-04 — High: arbitrary diagnostic values are persisted (OPEN)

importer.py:145 _bounded_diagnostics performs a JSON copy and size truncation, but no content allowlisting or sanitization. import_collection forwards outcome.diagnostics into both failed (:370) and successful (:404) run writes. Synthetic input containing authorization and errors[].input fields retained its sentinel unchanged through _bounded_diagnostics. Thus the public CollectionOutcome API bypasses the stated policy that input values and raw payloads are never stored. Size limits do not provide secrecy. Successful-run diagnostics also bypass the failure-only pruning policy.

Required fix: enforce a typed, closed diagnostic structure at the persistence boundary; permit only internally defined codes/field paths and bounded counts/indexes, discard raw inputs/unknown keys/free text, and bound before serialization. Apply to both success and failure paths. Map caller-supplied error_code/error_summary to safe codes/messages rather than relying on regex redaction to identify arbitrary secrets. Probe with sentinels in unknown nested keys, allowed-looking fields, and collection-failure summary/code.

### SEC-05 — Medium: extreme Decimal bypasses failed-run handling (OPEN)

schemas.py:130 calls Decimal.quantize before safely rejecting oversized values and does not translate decimal.InvalidOperation. A valid-shaped ARK record with total_weight='1e100' caused validate_records to raise InvalidOperation rather than ImportFailure. import_collection catches only ImportFailure around validation/canonicalization (:378), so this malformed batch bypasses the promised recorded failure. Required fix: bound magnitude/exponent before quantization and convert decimal arithmetic failures to fixed validation errors. Recheck invalid input records a failed run with stale-good data intact, not just that parsing raises.

### SEC-06 — Medium: history numeric canonicalization has unbounded expansion (OPEN)

ShortInterestHistory.days_to_cover is OptionalDecimal without range/precision/exponent constraints (schemas.py:312). _json_value formats Decimal in fixed notation. A record with one history entry days_to_cover='1e100000' passed validation; the eight-character numeric value expanded the canonical payload to 100,406 JSON characters. This is a bounded synthetic reproduction; larger exponents were deliberately not exercised. Up to 120 history entries per record and 50,000 records amplify the resource risk. Required fix: bound numeric exponent/magnitude/precision before fixed-point formatting, and enforce total canonical payload budget for direct as well as file entry points. File byte limits alone cannot prevent this expansion.

### SEC-07 — Medium: URL secret handling misses fragments and no-path queries (OPEN)

redaction._strip_url splits authority/path on '/', then strips query/fragment only from path. Consequently redact('https://example.invalid?credential=SYNTHETIC_RETENTION_SECRET') and redact('https://example.invalid#SYNTHETIC_RETENTION_SECRET') both retain the sentinel. envelope.py uses this function for persisted allowed metadata. Separately, schemas._optional_link checks query keys but not fragments: a valid Congress record with source_link='https://example.invalid/#access_token=SYNTHETIC_RETENTION_SECRET' validated and archived the sentinel in canonical payload.

Required fix: parse URL components correctly and strip query/fragment regardless of path presence in diagnostic/envelope redaction; explicitly reject or remove credential-bearing fragments in retained source links. Consider an allowlist of necessary query fields rather than a denylist with missing credential aliases. Recheck no-path URLs, userinfo, percent-encoded query names, and fragments using synthetic sentinels.

### Other observations requiring final verification

SQL writes use SQLAlchemy bind parameters; prune_failed_diagnostics uses bound source/cutoff/keep with a fixed table name. Runtime input sources/modes are allowlisted before writes. Unknown row fields are ignored and absent from canonical payload. Validation diagnostics exclude Pydantic input/context values. Row counts, business text lengths, fund/history counts, and database diagnostic byte size have explicit caps, but these do not resolve the issues above. Pruning occurs after failed imports; concurrent failures and a quiet source aging past 30 days need verification against the documented retention promise. File loader, CLI, live adapters, Compose privileges, and isolated integration tests remain pending.

## Boundary review and fix rechecks — 19:03Z assignment

Reviewed files.py, collectors.py, cli.py plus targeted changes to previously reported paths. Synthetic offline probes used mock HTTP sessions, AST-extracted existing parsers (avoiding module initialization/real .env), and a fake SQLAlchemy connection that captures INSERT parameters without a database.

- SEC-04 remains OPEN: unknown diagnostic keys are now dropped, but regex-accepted strings in field/type/unit/code still preserve arbitrary sentinels. More concretely, CollectionOutcome.failure with synthetic bare-secret error_summary persisted that exact string into captured ingestion_runs INSERT parameters. Code syntax validation and regex redaction do not implement a closed error vocabulary. Required: safe internal summaries/codes at public persistence boundary and known field/code enumeration; do not retain caller-provided secret-bearing prose.
- SEC-05: the extreme ARK Decimal now produces validation_failed and a captured failed-run INSERT rather than escaping. The specific bug is fixed; real PostgreSQL rollback/stale-good verification remains for final suite.
- SEC-06: history 1e100000 now produces validation_failed and a captured failed-run INSERT; specific exponent-amplification case fixed. Total canonical payload budget is still absent; evaluate remaining bounds with file/network fixes.
- SEC-07: no-path query redaction and access_token fragment rejection pass independent sentinel probes. Specific reported URL cases resolved. This is not a claim that arbitrary tokens embedded in business text can be inferred/sanitized.

### SEC-08 — Medium: network response limit is enforced after full download (OPEN)

collectors._get_json calls requests.Session.get without stream=True, then checks len(resp.content). Requests buffers the complete response first, so MAX_RESPONSE_BYTES=50 MiB does not bound transfer/memory/decompression. Mock session recorded stream absent. Required: stream, count decompressed bytes incrementally, stop/close once the cap is exceeded, parse only bounded bytes, and safely catch stream/JSON depth errors. Close responses on retries and early exits. Network response limits should hold before provider parsing.

files.read_fetcher_file similarly stats then uses unbounded json.load. Static inspection: a replaced/growing file or nonregular stream can bypass the stat size bound. Prefer a regular-file check plus a bounded binary read of MAX_FILE_BYTES+1 and reject actual oversize before JSON parsing. This is the same resource-boundary concern, not evidence of an observed oversized real file.

### SEC-09 — High: malformed provider records become successful empty snapshots (OPEN)

Independent mock-provider tests using the actual existing parser functions confirmed:

- Insider: Purchase row with today's valid date, string value='not-a-number', and valid insider name passes _insider_payload_ok; parse_insider_buys catches its comparison TypeError and returns []; collect_insider reports succeeded=True with zero rows. Parse logs still pass through the wrapper.
- ARK: HTTP 200 with holdings=[{}] passes list/dict checks; actual parse_holdings skips the missing ticker; collect_ark reports succeeded=True with zero rows.
- Short-interest: _short_interest_payload_ok({}) returns True although the expected data key is absent. Missing data must not be conflated with an explicit provider data:null outcome. Existing parser can also swallow failures after shallow row-dict checks.

These outcomes would let importer replace stale-good data with a successful empty batch. Required: validate required provider structure/field types before legacy lossy parsers, distinguish explicit supported absence from malformed data, and propagate every parse failure. Preserve intended business filtering, but do not interpret malformed inputs as filtered nonmatches. Add regression cases covering partial and all-invalid responses for each collector. Legacy parser logging needs suppression or removal on direct path so malformed-provider errors cannot bypass error sanitation.

### SEC-10 — Medium: CLI engine-construction errors escape safe boundary (OPEN)

cli.main wraps make_engine only in except DatabaseConfigError; general exception sanitation begins later. Concrete offline reproduction (no network): set process DATABASE_URL to a synthetic PostgreSQL URL with query port=synthetic_secret and invoke main(['latest','ark_holdings']). SQLAlchemy create_engine raises ArgumentError containing that query value, which escapes main and appears in formatted traceback. A multi-host query with an invalid port likewise raises ArgumentError. Required: include engine creation inside the sanitized exception boundary, suppress raw traces/messages, dispose only an actually created engine, and keep useful fixed configuration diagnostics. Recheck the real CLI entry point with synthetic URL query secrets.

Unverified file behavior: nonempty legacy files require --attest-complete; empty fixtures also require attestation; short-interest reported errors reject even attested imports. These explicit policies avoid trusting bare [] or existing errors alone. Additional fixture/probe verification remains pending. No project code, databases, provider sessions, real .env, or actual credentials were touched by these probes.

## Targeted independent recheck — October 6, 08:17Z assignment

Identity rederived with rig whoami: db.security-auditor, Codex runtime. Coordinator again attests GPT-6-Astra active; the identity command still does not expose exact model. No implementation edits or queue-claim debugging performed.

Executed from development/backend with TEST_DATABASE_URL and DATABASE_URL removed: ../../.venv/bin/python -m pytest tests/test_contracts.py tests/test_collectors.py tests/test_entrypoints.py -m 'not db' -q. Result: **100 passed, 1 deselected in 0.67s**, exit 0. This includes synthetic collector tests, URL query-routing rejection, unattested-file behavior, CLI/Alembic refused-connection checks, and JSON pipeline compatibility. Database tests were intentionally excluded. Entry-point tests only attempt synthetic loopback port 1 connections; no successful database connection or mutation occurred. Fetcher dotenv loading is disabled by test conftest.

### SEC-04 remains OPEN: malformed diagnostic types and exception-name channel

Closed error codes and fixed summaries now eliminate the previously reported arbitrary summary/code channel. But independent direct probes confirm safe_diagnostics({'unit_kind': []}) raises TypeError at set membership, and safe_diagnostics({'errors': [{'type': {}}]}) also raises TypeError. These calls occur on the failed-run persistence path, so malformed diagnostics can prevent the failed run from being recorded. Type-check before all membership tests and bound count magnitudes before JSON serialization. Arbitrarily large integers are currently accepted by _count and only discarded after conversion/size measurement.

safe_diagnostics({'exception': {'name': 'SYNTHETIC_SECRET'}}) retains the exact sentinel; render_summary includes this caller-supplied field. Regex-shaped names are not a closed class vocabulary. Use a fixed list/category for persisted exception classes or derive trusted internal diagnostics separately from caller diagnostics. The documented ticker-shaped-unit residual is distinct from this unintended exception-name channel.

### SEC-08 PARTIAL: HTTP fixed, file bounds remain open

HTTP now requests stream=True, counts streamed decompressed chunks, stops after the cap, closes responses in finally, and catches JSON ValueError/RecursionError. Independent execution of collector tests verifies oversized declared and streamed bodies fail. File entry point still uses stat then unbounded json.load, and _upstream_failure plus legacy upstream loaders also read JSON without byte limits. Required: bounded actual reads (not only stat), reject nonregular inputs, and reuse the validated bounded upstream payload instead of reopening it through error-swallowing loaders.

### SEC-09 PARTIAL: original provider cases fixed, upstream partial success remains

The original all-invalid ARK, malformed insider Purchase numeric field, and absent short-interest data cases now fail in the passing synthetic collector tests. New targeted reproduction used only TemporaryDirectory JSON files and actual legacy insider loader: trades_congress.json held one synthetic Purchase ticker; ark_holdings.json held {'holdings': [{}]}. _upstream_failure accepted both list envelopes, the ARK loader swallowed KeyError and returned [], a mocked 404 completed the Congress plan, and collect_insider returned succeeded=True. Thus malformed required upstream data still produces a partial successful collection. Validate each upstream record/plan before calling legacy loaders or replace those wrapper-side reads with explicit outcome handling. Also _insider_payload_ok accepts recent=[{}] as a non-Purchase; missing type should be distinguished from a valid excluded transaction type.

### SEC-10 RESOLVED for reported engine-construction leakage

Independent real entry-point probes used DATABASE_URL with query port=SYNTHETIC_QUERY_SECRET. CLI main returned 2 and Alembic subprocess returned nonzero, neither output contained the sentinel nor a traceback. No connection is made for this invalid-query case. Existing refused-connection tests also passed. Alembic migration execution errors after connection are outside its explicit safe connection boundary; no new secret-bearing migration failure was established here.

### SEC-11 — High: test guard misses environment-based routing (OPEN)

tests/conftest.py check_test_server_url correctly rejects nonloopback URL hosts and routing query keys, but does not account for libpq environment defaults. Offline reproduction: a URL with host 127.0.0.1, port 55439, database perennial_test passes the guard. With a synthetic process PGHOSTADDR=192.0.2.123, SQLAlchemy connect parameters omit explicit hostaddr, psycopg conninfo_attempts leaves it omitted because get_param already sees the environment value, and effective get_param(attempt, 'hostaddr') remains 192.0.2.123. This demonstrates a nonloopback effective target without opening a socket. The initial direct-dictionary check was false because the value stays in the environment; verifying effective get_param established the bypass.

Test setup issues CREATE DATABASE / later DROP DATABASE on the resulting target, so local URL appearance alone is insufficient isolation. Required: reject or remove routing PG environment values (especially PGHOSTADDR and PGSERVICE/service configuration) before engine creation, or construct an explicit verified effective connection configuration that cannot inherit routing. Test the effective driver target offline, not just URL fields. Keep random per-session database ownership and avoid connecting until this guard is fixed.

All concrete remaining blockers were attempted via rig send twice; both attempts were refused because implementer was at a selection prompt. No prompt was driven or approved by the auditor. Findings are retained here and sent by durable queue handoff. Live ARK symbol policy, final setup/privilege docs, Compose and final candidate remain for handoff review.

## Frozen candidate final targeted recheck — 08:30Z handoff

Candidate fingerprint command from implementer report recomputed to **707aca1f4576be1f**, matching the frozen candidate identifier. Baseline remains fab7188. Reviewed current bytes; no implementation edits. Tests were run against synthetic files and fake sessions only; no DB connection, network/provider request, Compose startup, real .env access, or database mutation was performed in this final review. Exact active model is not exposed by rig identity/runtime evidence; coordinator asserts GPT-6-Astra. No contrary lower-model signal observed.

### Resolved findings

- SEC-04: malformed shapes `{unit_kind: []}`, `{errors:[{type:{}}]}`, `{errors:[{field:[]}]}`, `{failures:[{unit:{},code:[]}]}`, and `{exception:{name:{}}}` no longer raise or retain values. Exception names are constrained to known library/builtin classes; count capped at 1e9. Independent helper probes passed; relevant contract tests cover the same cases.
- SEC-08: read_json_bounded rejects non-regular files and reads at most MAX_FILE_BYTES+1 from an O_NOFOLLOW descriptor. _read_upstream uses this same bounded reader. HTTP calls stream responses, count decoded chunks, stop at 50 MiB, and close responses. Relevant tests verify oversized body/file, symlink, FIFO, directory and malformed JSON rejection. Original resource-bounding issue resolved within specified entry points.
- SEC-09: malformed insider transaction type, short-interest missing data, malformed ARK provider records, and malformed upstream records now fail collection. Upstream planning consumes the bounded validated records rather than calling the legacy loaders. Original partial-success probes are covered by relevant tests.
- SEC-10: invalid synthetic database URL query causes CLI and Alembic to return sanitized nonzero errors without traceback or secret sentinel (previous independent recheck).
- SEC-11: tests scrub inherited PG* settings and pinned_engine supplies an explicit hostaddr from a finite loopback allowlist. The connection interception test verifies actual SQLAlchemy driver cparams with PGHOSTADDR set to a documentation-only nonloopback address; hostaddr remains loopback and no socket is opened. `TEST_DATABASE_URL` rejects nonloopback hosts, non-test names and routing query keys. The previously observed effective-target bypass is resolved.

Test evidence: from development/backend, `.venv/bin/python -m pytest tests/test_contracts.py tests/test_collectors.py -q` → **109 passed in 0.40s**. This is targeted, DB-free testing. Coordinator separately reports 133 passing tests including isolated PostgreSQL and ARK live evidence; that evidence is attributed to the coordinator/implementer and was not reproduced under this synthetic-only audit instruction.

Compose/docs review: PostgreSQL binds only `127.0.0.1:5433`; default credentials are clearly marked development-only. README describes owner migrations and a least-privilege importer role (SELECT/INSERT/UPDATE plus sequence USAGE) and reader SELECT. Migration itself creates schema/tables but does not create a shared runtime role; setup documentation describes grants without a checked-in role-provisioning script. This is a documentation limitation, not an observed exposure in the local-only Compose setup. Compose config was not started here.

### SEC-12 — Medium: bounded upstream inputs have no outbound request-count cap (OPEN)

The shared JSON byte cap does not constrain the number of distinct tickers in the accepted files. `_insider_plan` sends requests for every selected ARK holding; `_short_interest_plan` unions every valid Congress/ARK ticker. A synthetic temporary input containing 5,000 valid tickers per source file (well below the 25 MiB per-file cap) passed `_read_upstream` and yielded 5,000 requests in each provider plan. No requests were sent in the probe. These collectors sleep and perform one network call per plan entry, so an unexpectedly large but byte-valid upstream file can cause a long run and thousands of external requests. Add a documented, enforced maximum candidate count before any provider call and fail the whole collection if exceeded; consider deduplication and a shared hard ceiling for both sources. Test the cap with synthetic plans and assert zero session calls when exceeded.

### SEC-13 — Low/Medium: opaque URL fragments can persist token-like material (OPEN)

schemas._optional_link rejects fragments containing `=` or `&`, but accepts opaque fragments. Independent probe `_optional_link('https://example.invalid/#SYNTHETIC_OPAQUE_TOKEN')` succeeds. Congress `source_link` is retained in canonical batch JSON, so token-like fragment contents persist. URL fragments are not needed for the stored provider filing reference; reject any nonempty fragment or drop it before persistence. Existing fragment tests cover key/value forms only, not an opaque fragment.

All findings above are limited to credential leakage, SQL/error safety, input/resource limits, local test routing, and Compose/documented privileges. No live network/database test was run by this auditor as instructed. SEC-12/13 require implementer response and independent recheck before closing the scoped audit. Due to the model mismatch recorded at the top, these are provisional findings, not final Astra conclusions.

## Model mismatch hold — October 6, 08:33Z

User selected `/model gpt-6-astra`. A live `rig capture db-security-auditor@perennial-database` at 08:33Z displayed `GPT-6-Sol high`. A second capture roughly 10 seconds later again displayed GPT-6-Sol high. rig whoami reports runtime=codex but not the exact model. Coordinator said an Astra restore was sent, but the terminal has not reflected it. No additional audit probes were run after confirming Sol. Prior evidence and SEC-12/13 implementer handoff remain preserved. Resume final targeted review only after a fresh capture confirms GPT-6-Astra.

## Final Astra verification and scoped conclusion — October 6, 08:37–08:40Z

**Model and candidate evidence.** Following the user's model-switch confirmation, terminal capture showed both `Model changed to gpt-6-astra high` and `GPT-6-Astra high` in the footer. End-of-verification capture again showed GPT-6-Astra high. The current implementer report names f6cf94f5acbe24aa; its documented fingerprint command returned that exact value before and after the checks. The model hold is cleared. No final conclusion here relies solely on the provisional Sol result.

**Fresh execution under Astra.** From development/backend:

`env -u TEST_DATABASE_URL -u DATABASE_URL ../../.venv/bin/python -m pytest tests/test_contracts.py tests/test_collectors.py -q`

Result: **112 passed in 0.28s**, exit 0. Only synthetic contracts, fake provider sessions, and intercepted connection parameters were used. The tests disable fetcher dotenv loading. No database socket was opened, no provider request was sent, and no implementation file was edited during this final verification.

Additional independent probes under Astra, beyond running implementer tests:

| Finding | Fresh verification | Result |
|---|---|---|
| SEC-04 | Five malformed/secret-bearing diagnostic shapes passed through actual import_collection using a fake persistence connection. Captured failed-run INSERT parameters contain no sentinel; unhashable values and 10^100 counts are contained. Unknown code becomes the closed default; exception name becomes the allowed generic class. | Resolved |
| SEC-08 | Actual bounded reader rejects oversize bytes, symlink and FIFO. Source inspection confirms fstat regular-file check and MAX_FILE_BYTES+1 read; shared upstream reader uses it. Passing HTTP tests verify stream=True and oversized responses fail; finally closes the response. | Resolved |
| SEC-09 | Temporary synthetic Congress input plus malformed ARK holdings=[{}] makes both collectors fail missing_upstream before any HTTP call. Explicit probes reject missing insider type, missing short-interest data and ARK [{}]. Regression suite covers original partial/malformed provider cases. | Resolved |
| SEC-10 | Real CLI entry point given a synthetic invalid query port returns exit 2 without secret or traceback, before any connection. | Resolved for reported CLI engine-creation path |
| SEC-11 | With PGHOSTADDR set to 192.0.2.123 after harness import, intercept SQLAlchemy do_connect before any socket. Effective cparams retain hostaddr=127.0.0.1. Harness scrubs inherited PG* variables; both admin and migrated test engines use pinned_engine. URL routing query guard tests pass. | Resolved |
| SEC-12 | Use the actual default 2,000 cap and temporary input containing 2,001 distinct valid tickers. Both insider and short-interest return plan_too_large with zero HTTP calls; no truncation. README documents the ceiling and diagnostic vocabulary includes this code. | Resolved |
| SEC-13 | Independently test opaque sentinel fragment, access_token fragment and bare #. All are rejected before canonical persistence. Current validator and README require no fragment. | Resolved |

SEC-03/05/06/07 regression coverage also passed in the 112 tests (fixed config messages, numeric limits/quantization, history exponent bounds, URL redaction). SEC-01/02 are addressed for the new direct database collection path by explicit per-unit outcomes and avoiding legacy HTTP functions that log raw exceptions. This does **not** claim the unchanged standalone FMP fetcher/scheduler logging hazard has been removed; that baseline code still needs separate work if used with real credentials. The acceptance scope is the database addition and audited entry points.

**Configuration and evidence limits.** Re-read Compose: loopback-only 127.0.0.1:5433 publication, development-only defaults, no broad host bind. README distinguishes owner migrations, importer SELECT/INSERT/UPDATE plus sequence USAGE, and reader SELECT; it is guidance rather than a deployed shared-role policy. Test databases are generated with an internal random name and cleanup targets that exact name. No shared database, production grants, cloud service, or actual Compose runtime was touched by this audit. Coordinator independently attests clean dependency install, actual Compose/migration/admin-createdb verification and 136 tests on unique port 55440; coordinator/implementer attest ARK's 98 rows, reuse and stale-good behavior. Those are attributed external verification, not auditor live evidence.

Failed diagnostic pruning is triggered by failed imports; it is not a background expiry service. Pruning errors are suppressed after the failed-run transaction so diagnostic cleanup cannot lose the failure record. Retention is therefore operationally best effort if the database is failing. Documented diagnostic units may retain bounded ticker-shaped text. No universal secret detector for arbitrary business-field text is claimed.

**Conclusion:** accept candidate f6cf94f5acbe24aa within the targeted database security review. No unresolved actionable blocker remains from SEC-04/08/09/11/12/13 after fresh verification on confirmed GPT-6-Astra. This is not a whole-repository security certification, production privilege deployment approval, or independent live-provider/DB test claim. Stop further audit work unless candidate bytes change or a concrete new finding is assigned.

## Candidate f9c73aaa (perennial-pipeline)

Independent security review, October 7, 2026. Assignment: qitem-20261007065306-ea406c9ad29454af. Terminal capture confirms GPT-6-Astra high. Start fingerprint recomputed using the repository Python and `/private/tmp/perennial-pipeline-rig/fingerprint_candidate.py`: `f9c73aaaa202253f5932e2f15c2a4ca50022193d8c2ccef1de4359b36a9ce553`, 69 files, HEAD `9adfa48485c413ed574f42a6aedda0228337d79a`. This section supersedes neither the old database-only acceptance nor its scope limits.

### SEC-P1 — High: database scheduler re-raises unsanitized exceptions to APScheduler (open, blocks acceptance)

Location: `development/backend/src/pipeline/scheduler.py:25-28`, registered directly as the job at line 37. `run_full_pipeline` logs `describe_exception(exc)` but immediately uses bare `raise`. The installed APScheduler executor (`apscheduler.executors.base.run_job`) catches that original exception, calls `logger.exception`, and attaches the original exception to its error event. Thus the new database scheduler's normal scheduled execution emits a full unsanitized traceback even though its own log line is safe. This is distinct from the explicitly excluded legacy cron logging hazard.

Independent reproduction (no database, credentials or network): `.venv/bin/python /private/tmp/security-f9c73aaa-scheduler-probe.py`, from repo root. It runs the real scheduler function through the real APScheduler executor, injecting a SQLAlchemy OperationalError carrying a synthetic provider-URL secret in its original error text. Observed: `sanitized_application_log=True`, `apscheduler_logs_raw_secret=True`, `exception_event_contains_raw_secret=True`, `engine_disposed=True`. The probe captures logs privately and prints only booleans. This verifies the framework boundary, not an actual credential compromise or that ordinary handled HTTP errors leak.

Suggested fix: preserve scheduler failure semantics while replacing the escaping exception with a fixed safe exception/message and suppressed original context, or use an executor-facing adapter that reports failure without exporting the original exception/traceback. Put engine construction inside the protected boundary too. Add a regression using APScheduler's actual executor and a sentinel-bearing chained request/database error; assert the complete log output and error event contain no sentinel, original message, URL or bound values. A test that only inspects the application's own logger is insufficient. Independent recheck required after a new fingerprint.

### Verification recorded so far

Using the existing isolated `perennial-db-test-20261005` server, first confirmed Docker reports running and binding only `127.0.0.1:55439`. Passed its synthetic URL privately through the child environment; no credential printed. Ran repository `.venv/bin/python -m pytest` with `REQUIRE_DATABASE_TESTS=1` on `test_ops.py`, `test_collectors.py`, `test_source_regressions.py`, `test_market_caps.py`, `test_scheduler.py`, and `test_pipeline_contract.py`: **128 passed in 1.71s, zero skipped**. The harness disables dotenv, strips PG defaults, pins hostaddr, and creates/migrates/drops its own random test database; real provider access was not performed.

These tests exercised actual importer/reader role access, repeated grants, denied history UPDATE/DELETE/TRUNCATE and symbol mutation, successful publication/replay/second promotion, denied future-table grants, and a successful snapshot dump/restore content comparison with generated-target cleanup. The role script uses bound values and psycopg Identifier for identifiers, explicit table/column policy, NOLOGIN groups without admin attributes or inherited parent roles, and revokes table/column/default privileges before granting. Migration 0003 adds no grants and interpolates only static, dialect-quoted identifiers. Deployment login memberships and arbitrary pre-existing privilege chains remain explicitly outside the script's guarantee, as documented.

Status: review ongoing; remaining boundary checks and final fingerprint/verdict follow below.

### Final gate status

The final fingerprint recomputation remained exactly `f9c73aaaa202253f5932e2f15c2a4ca50022193d8c2ccef1de4359b36a9ce553` (69 files; base `9adfa48485c413ed574f42a6aedda0228337d79a`). Additional synthetic probes covered bounded retries and `Retry-After`, response closure, diagnostic/coverage allowlists, CLI exception suppression, restore subprocess argument handling, and restore target-collision cleanup. All passed. The focused repository suite remained 128 passed, zero skipped.

The terminal capture during the review showed `GPT-Reserve high`, while this assignment requires the user-requested GPT-6-Astra. Because the active runtime is no longer confirmed Astra, I am not issuing an acceptance or final security signoff. SEC-P1 remains open and is a high-severity blocker independently of the model gate. Resume only after the coordinator routes a new fingerprint and the terminal again confirms GPT-6-Astra.

### Wake recheck — October 7, 2026

The watchdog wake rechecked the obligation. The candidate remains byte-identical (`f9c73aaaa202253f5932e2f15c2a4ca50022193d8c2ccef1de4359b36a9ce553`, 69 files), and the terminal still reports `GPT-Reserve high`. No further implementation recheck or acceptance was performed; SEC-P1 and the Astra runtime gate remain open.

## Candidate b3ef39d8 (perennial-pipeline)

**Verdict: accept within the assigned SEC-P1-only recheck. SEC-P1 is resolved.** October 7, 2026, following parent-coordinator's 23:52Z reassignment of qitem-20261007065306-ea406c9ad29454af. This verdict does not assess the separately assigned F1–F3 changes or certify the whole pipeline for deployment.

Model hold cleared: fresh terminal captures at the start and end of this recheck both showed **GPT-6-Astra high**. The repository Python fingerprint command returned the same full SHA at both boundaries: `b3ef39d869e39beadce950118510db796a51b92e52a598a48d4265ce495df504`, **73 files**, HEAD `9adfa48485c413ed574f42a6aedda0228337d79a`.

Inspected `development/backend/src/pipeline/scheduler.py:21` and `:28`: engine construction is now inside the exception boundary; execution failures are logged by class and recorded with a flag; disposal failures are separately sanitized; the function raises fixed-message `PipelineRunFailed` outside the exception handler. Thus the original exception is not forwarded to APScheduler or attached as its cause/context. Successful execution still returns its result, and pipeline failure still produces a scheduler error event.

Fresh verification from repository root, with no implementation edits or live providers:

- `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest development/backend/tests/test_scheduler.py -q -p no:cacheprovider` → **7 passed in 0.15s**, exit 0, no skips. The real `apscheduler.executors.base.run_job` tests cover chained sentinel-bearing errors from engine construction and pipeline execution, simultaneous disposal failure, event message/traceback, empty exception cause/context, and disposal after successful execution. These are synthetic scheduler tests; no database was needed or accessed for this narrowly assigned recheck.
- `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python /private/tmp/security-f9c73aaa-scheduler-probe.py` → `sanitized_application_log=True`, `apscheduler_logs_raw_secret=False`, `exception_event_contains_raw_secret=False`, `engine_disposed=True`. It then exits 1 because its final assertion explicitly requires the old vulnerability to exist (`AssertionError: Candidate no longer reproduces; recheck fix`). This expected reproduction failure corroborates the seven passing regression tests; it is not described as a passing test command.

No remaining blocker was found within SEC-P1. The previously documented legacy cron raw-exception hazard remains outside this recheck and is not marked fixed. The author's full-suite 277-test result and other finding fixes were read as handoff context, not independently reproduced or accepted here. Queue closure transfers this scoped verdict to parent-coordinator; subsequent changes require their own routed fingerprint and recheck.
