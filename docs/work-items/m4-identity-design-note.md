# M4: lean security identity and reference data

Status: proposed design for Bryan and checker review; no implementation or migration.
Date: 2026-10-20. Branch: `docs/m4-identity`, rebased onto `origin/main` at `98381eb` after GDELT merged.

Authority: the 2026-10-20 rescope in `docs/roadmap/database-roadmap.md` on
`docs/roadmap-revamp` (working copy inspected after the main-moved notice;
branch HEAD `567bb94`), §1 Sizing, §2, M4 and §4.
The roadmap on this branch's main base predates that rescope. This note follows
the revised plan: a plain alias table, optional FIGI, no identifier time ranges,
no mandatory-FIGI migration and no new identity service.

## Outcome and existing seams

Every security used by the current publication must resolve to exactly one
stable `security_id`. Collector spelling differences must not create duplicate
securities. A failed lookup must not manufacture a new security or masquerade
as a successful empty collection. Existing publications must replay unchanged.

At `12e4b1b`, `src/db/models.py` defines securities with a unique symbol, exchange,
currency and created_at. `src/ingestion/importer.py::_resolve_securities` inserts
unknown symbols, then fills missing exchange/currency. M4 replaces that insertion
with lookup. `src/ingestion/schemas.py::split_exchange_suffix` already recognizes
US suffixes and checks exchange conflicts. Keep those validation semantics.

PR #18's locally available ref `origin/insider-finnhub-collector` at `f5a1890e`
defines `provider_symbol` in `fetchers/insider.py`: strip US suffixes, replace dots
with dashes, route GOOG to GOOGL, and exclude unsupported listings. Its parser
retains the requested ticker. This is evidence from the local ref, not a claim
about the current remote PR head. The collector change remains owned by PR #18.

## Proposed relational shape

All tables are in `perennial`; user tables and credentials are outside this work.

| Table | Proposed columns and constraints |
|---|---|
| `securities` additions | `name` text, `security_type` constrained to common/ADR/ETF, `country` two-letter code, `figi` nullable unique, `is_active` boolean. Preserve existing IDs, symbol, exchange and currency. Add nullable metadata first; backfill classification before requiring it for new seeds. Existing rows can default active. |
| `security_identifiers` | `security_id` FK, `scheme` in ticker/figi/cusip/cik, `value` nonblank text; unique `(scheme, value)` and index on `security_id`. No `valid_from` or `valid_to`. No cascading deletion of referenced securities. |
| `company_profiles` | `id` PK, `legal_name`, nullable `sector`/`industry`, `alias_names` text array. Add nullable `securities.company_profile_id` FK so several share classes can share one issuer. Names are search candidates, not globally unique security identifiers. ETFs may initially have no issuer profile. |
| `market_caps` | Immutable `(batch_id, security_id)` rows with value, currency, provider, observed_at and retrieved_at. Positive numeric value or an explicit unavailable/null observation, retaining existing validation semantics. Batch/run lineage uses the existing ingestion contract. |

The profile link is a proposed addition needed to make “one row per issuer”
concrete; do not use ticker or security_id as the profile's primary key. GDELT has already merged migration `0004`, which added sector and industry
to `market_cap_observations`, and `0005`, which added its tables. Expand by
adding the profile columns, then backfill from existing observations through
the reviewed security-to-issuer mapping. Use the latest non-null observation
by observed_at (falling back to retrieved_at); conflicting values across share
classes or equally recent observations require review rather than an arbitrary
winner. Record the chosen provenance in the backfill evidence. Switch GDELT
reads to company_profiles and verify equivalent classifications before retiring
the old columns with market_cap_observations in the later contract step.

`securities.figi` is the selected instrument FIGI. Insert its matching alias in
the same controlled transaction; reject conflicting ownership. Do not mix
instrument, composite and share-class FIGIs in this single column.

CIK identifies an issuer and can cover multiple securities. The requested unique
`(scheme, value)` means one CIK cannot be attached to both share classes. Proposed
resolution: keep an issuer CIK as nullable unique metadata on `company_profiles`;
only use a security-level cik alias where it identifies one supported security
unambiguously. Do not choose an arbitrary share class to satisfy uniqueness.
Bryan approved this minimal issuer-field clarification on 2026-10-21.

## Resolution and provider requests

Use the existing database connection and a small resolver in the ingestion path.
No network lookup occurs during import. Resolve all rows before writing a batch;
a missing or conflicting mapping fails the transaction with `unresolved_security`.
Keep malformed syntax as validation failure and existing exchange/currency
conflict checks. Inactive listings cannot enter new daily collections; historical
replay uses already pinned IDs and payloads.

For incoming ticker aliases, trim whitespace and uppercase. Store canonical
aliases such as `(ticker, RKLB)` and explicit provider-qualified aliases such as
`(ticker, ARK:RKLB UQ)` and `(ticker, FINNHUB:BRK-B)`. Provider prefixes are an
encoding convention within `value`, not extra schemes. Qualify lookups using a
fixed adapter provider identifier, never an arbitrary prefix supplied by a row.
Try an exact provider alias, then a canonical ticker only where the provider's
market scope is known and exchange/currency agree. Never strip punctuation or
unknown suffixes globally. Preserve raw provider spelling in the immutable input
and resolve it before existing normalized record validation discards that detail.

For outbound requests, seed exactly one request spelling per provider and security
in the provider-qualified ticker namespace. Validate that cardinality during
seed review. Extra incoming spellings can use a distinct fixed `PROVIDER-INPUT:`
prefix, avoiding multiple request candidates. Missing or ambiguous request
spelling is a failed resolution, not “no insider buys.” This encoding keeps the
three-column alias table; implementers should not add a generic routing framework.

| Source/case | Resolution and request behavior |
|---|---|
| ARK `RKLB UQ` | Seed `ARK:RKLB UQ` to the existing RKLB security; require the known NASDAQ suffix to agree with stored exchange. Finnhub uses that security's `FINNHUB:RKLB` alias. Unknown suffixes are rejected, not guessed. |
| FMP Congress | Resolve its symbol to a seeded security; preserve transaction identity, dates and amounts. FMP request/response spellings are explicit aliases. Pagination and the history-window policy remain unchanged. |
| `BRK.B` / `BRK-B` | After seed review, both spellings refer to one class-B security; retain its existing display symbol and ID. Seed Finnhub's dash spelling explicitly. Do not apply dot-to-dash conversion to every security. |
| `GOOG` / `GOOGL` | Keep distinct securities linked to one company profile. PR #18's GOOG→GOOGL request routing is not permission to merge securities. Proposed profile field `insider_reporting_security_id` selects the seeded GOOGL security for the issuer's Finnhub endpoint; its identifier supplies the request spelling. Preserve the initiating security ID and coverage attribution, with the request alias recorded as lineage metadata. This replaces the hard-coded function with data. |
| Nasdaq short interest | Request the selected security's Nasdaq alias; bind the response to the same security, with exchange/currency checks. Do not inherit Finnhub's share-class routing. |
| Market-cap provider | Resolve to the security that the observation actually describes; retain currency and whether the provider metric is issuer-level. Do not duplicate an issuer total across classes and sum it as separate capitalization. Scope policy needs confirmation below. |
| ADRs, including TSM | ADR and ordinary shares are distinct instruments, possibly sharing a profile. An alias is not a price or unit conversion. Preserve reported units and currency; never relabel an ordinary-share price as an ADR price. |

For the GOOG route, one response may cover more than one requested class. Deduplicate
provider requests by reporting target, retain coverage for each requested security,
and do not double-count the same filing at issuer aggregation. Whether an
issuer-level insider signal should influence both classes requires explicit signal attribution under Bryan's 2026-10-21 decision;
M4 must not silently duplicate the signal or merge the securities.

The roadmap reports one TSM ADR represents five ordinary shares. Treat that as
a case to verify against issuer evidence before enabling conversion, not a
universal ratio. If normalized ADR quantities are later required, ordinary
shares / ratio and price × ratio preserve total value in the same currency.
Record ratio, unit basis and provenance with the derived output. This design
does not add automatic conversion or infer it from a price difference. Until the
basis is established, mark that normalization unsupported, not zero activity.

## Seeds, failures and backfill

Create a reviewed, idempotent seed manifest covering all symbols referenced by
the current publication plus the supported upcoming collection universe. Seed
existing IDs first; never delete/recreate a security with historical foreign keys.
Resolve pre-existing alias duplicates before enforcing lookup-only import; keep
old rows and pinned payloads unchanged. Conflicting alias ownership must stop
seeding instead of being overwritten by an upsert.

Include synthetic tests for RKLB/RKLB UQ, BRK.B/BRK-B, distinct GOOG and GOOGL
with one profile, an ADR and its ordinary share, missing aliases, conflicting
exchange and an unsupported listing. Real seeds require reviewed evidence for
name, exchange, currency and instrument class; do not invent FIGIs or CIKs.

Add fixed `unresolved_security` to the diagnostics vocabulary. Persist safe counts
and approved record indexes only, not raw response/error text. No partially
resolved batch can become current. An explicitly unsupported provider listing
may retain PR #18's “not evaluated” coverage only after its security identity is
known; unknown identity must not be silently skipped. A reviewed seed addition
followed by an ordinary retry is the recovery path.

Run OpenFIGI enrichment in bounded batches outside the migration transaction,
using existing retry/settings machinery and approved staging credentials only.
Match ticker plus exchange/currency/type evidence; accept exactly one compatible
instrument, otherwise leave FIGI null for review. Make reruns idempotent and never
overwrite a conflicting FIGI automatically. FIGI absence does not block publishing.

The [OpenFIGI mapping documentation](https://www.openfigi.com/api/documentation)
returns instrument, composite and share-class FIGI fields, with potentially
multiple matches. Its documented response does not promise CIK; do not design
an automatic OpenFIGI-to-CIK backfill on that assumption. Obtain issuer CIK from
separately approved evidence or leave it null. No live mapping was called for
this note; only public documentation was consulted.

## Market-cap migration and replay

Register `market_caps` in the source vocabulary, collector/validator dispatch,
immutable source tables and run lineage; use the existing importer and retry
path. Coordinate its freshness/reuse policy with M2. Do not make old watchlist
publications retroactively require this new source.

Backfill eligible legacy observations into explicit historical batches, preserving
provider, observation/retrieval timestamps and null availability. Do not fabricate
collection dates or claim an incomplete legacy set is a complete daily batch.
New publications pin the selected market-cap batch through publication sources.
Switch serving queries only after comparison proves the same selected values and
ranking output for equivalent inputs. Old publication replay continues through
its pinned `publication_market_caps` observations; never rewrite old hashes or
ranking inputs using today's alias table. New identity resolution must be pinned
at ingestion so later alias edits cannot change replay.

Keep both legacy market-cap tables during expansion. The later M7/M8 contract
change may remove them only after a full release without consumers and a proven
replacement for every retained publication's replay dependencies. This note does
not authorize removal or rehashing historical data.

## Delivery and proof

1. Approve identity/routing and issuer-field decisions below; coordinate with PR
   #18, GDELT and schema owners. This packet itself remains documentation only.
2. Implement one expand migration with a placeholder revision while developing;
   main now has Alembic head `0005`. Assign the next sequential four-digit
   revision and actual predecessor at merge,
   per revised §4. Do not reserve 0004/0005 or keep a named revision at merge.
   Include lock_timeout, downgrade, grants and migration guard/drift tests.
3. Seed/backfill outside the migration transaction. Pipeline imports get lookup
   permission, not authority to create aliases. Controlled seed/enrichment writes
   require a reviewed operator path; API remains read-only on pipeline data.
4. Switch resolution and Finnhub routing together after all current publication
   symbols pass exact-one resolution. Preserve replay and provider coverage tests.
5. Expand market caps and verify publication pinning before retiring side tables.

Acceptance tests must show exact-one resolution for the publication universe;
all three PR #18 cases; distinct share classes/ADR instruments; duplicate aliases
rejected; unresolved imports leave no successful batch; idempotent seeds; nullable
FIGI with successful publication; ambiguous enrichment left unassigned; immutable
replay after alias additions; and market-cap lineage plus replay for old and new
publications. Checker reviews all implementation; auditor reviews migrations,
roles and any secrets/logging changes. Bryan signs the milestone gate.

## Bryan's decisions — 2026-10-21

Authority: coordinator packet `decisions-20261021.md`, recording Bryan's
answers in roadmap PR #26. These supersede this note's earlier open questions.

1. GOOG and GOOGL remain separate securities, with explicit signal attribution.
   An issuer-level Finnhub request route does not make the securities equivalent
   or authorize duplicating one filing across both signals.
2. The minimal link fields are approved: `company_profile_id`, issuer CIK and
   `insider_reporting_security_id`.
3. Provisional until Bryan changes it: the initial universe is the symbols in
   the current publication; Bryan approves seed additions. No global universe
   expansion or time-ranged identifiers is implied.
4. ADR data stays on its reported basis; no guessed conversion. The TSM ratio
   example above does not authorize converting provider prices or quantities.
5. Provisional until Bryan changes it: FMP company-screener is authoritative
   for issuer-level market caps, with a three-day freshness limit (D7). Keep
   share-class identity separate and do not sum repeated issuer totals.

D3: only FMP and Finnhub need keys. Agents never handle real keys; Bryan runs
all live probes and staging runs. Prepare live steps as one command with output
limited to status codes and counts. Do not acquire an OpenFIGI key as part of M4;
any optional enrichment step needs a Bryan-run path consistent with this rule.
D14: use free tiers for now; no paid services or PITR before launch. The M3
restore drill uses scheduled logical backups. D13 stores all Congress history
FMP serves, with the two-year window applied in ranking, not storage.

Implementation still needs checker review and the selected migration/role
security review. This document records approved design choices, not permission
to deploy or call live providers.
