# M5 read API: design note

**Status:** proposed design for checker review. Docs only; no code.
**Date:** 2026-10-23. **Author:** db-planner. **Branch:** `docs/m5-read-api` from `origin/main` at `4e6acac`. Line references are against `ac21a00`; `src/` is unchanged at `4e6acac`, since #43 was docs only.
**Authorized by:** Bryan, 2026-10-23, decision 5 (start the M5 read API). Decision 4 is one watchlist per user for now.
**Plan of record:** roadmap §3 M5, design rules 3, 4 and 7, and decision D11.
**Inputs:**
- issues #2 and #3
- Jaden's #22 at `83d62d5`, which conflicts with `main` and is waiting on Jaden
- Bryan's handoff list for #22: rebase, drop the two legacy perennial-schema auth migrations, key the watchlist by `security_id`, DELETE `/me` plus export, tests, and the RLS/JWT questions

## Outcome

The app can read the current watchlist publication, and a signed-in user's watchlist, from the database through a versioned API. Every response that carries publication data names the publication it came from. The body served for a given publication is byte-identical every time, and it says whether the publication was degraded and how fresh each source was.

## Two parts, split by what depends on #22

| Part | What | Needs #22? | Owner |
|---|---|---|---|
| **R1, build now** | Public reads of published data: `GET /v1/watchlist` and `GET /v1/securities/{symbol}` (plus `GET /v1/publications/{id}` only if Bryan keeps it, question E), the `perennial_api` role's grants on `perennial`, and the v1 response schema document | **No.** No user data, no auth | implementer |
| **R2, after #22 lands** | `GET /v1/me/watchlist`: the user's one watchlist, joined to the current publication | **Yes:** verified identity, the `app.watchlist` table, RLS and the API role's `app` grants | implementer |

R1 has no user data, so it needs no token. Everything per-user waits for #22. This note doesn't redesign #22. It names exactly what R2 needs from #22 (see "What R2 needs from #22").

## Current state (origin/main ac21a00, and #22 at 83d62d5)

- **No API on `main`.** There is no `src/api/` and no FastAPI dependency. #22 adds both: `src/api/app.py`, `accounts.py`, `dashboard.py` and `supabase_auth.py`, with `fastapi==0.143.1` and `uvicorn==0.34.0`, and `httpx2==2.13.1` for development, all in `pyproject.toml`.
- **The publication model is immutable and replayable.** The pieces:
  - `publications` stores `output` (JSONB), `ranking_version`, `ranking_config` and `created_at` (`src/db/models.py:466`).
  - `current_publication` points the `production` channel at one publication (`models.py:582`). `cli rollback --to` moves it and appends a `rollback` event (`src/pipeline/publication.py:808`).
  - `replay()` recomputes `output` byte-identically from pinned rows (`publication.py:887`).
- **The output document** is `{schema_version, ranking_version, generated_at, popular_stable, affordable_growing, unresolved, errors}` (`src/pipeline/ranking.py:196-206`). Each record has `ticker`, `bucket`, `rank`, `market_cap`, `signals`, `source_dates` and `warnings`. Records carry no `security_id`, and no personal names, only counts and amount ranges.
- **The markers the API needs** sit outside `output`:
  - **Degraded:** at most one `degraded` event per publication (index `uq_publication_events_degraded`). Its body has:
    - `reused`, keyed by source, with `collected_at` and `age_seconds` (`publication.py:526`)
    - `all_sources_reused`
    - optionally `market_caps`, with `unavailable` (`publication.py:540-551`). It is written **only on the stored-value fallback** (`publication.py:697-698`), so it doesn't count missing caps on a normal collection
    - optionally `congress_gaps`, with `count` (`publication.py:705`)
  - **As of:** `ranking_config.source_as_of`, per source (`publication.py:709`).
  - **Demo:** the quality event's `checks.demo` (`publication.py:742`).
  - **Operator fields:** `publication_events` also has `actor` and `reason`, for overrides and rollbacks. Those are operator text, and the API must not serve them.
- **Roles on `main`:** `perennial_owner`, `perennial_importer` and `perennial_reader` (`development/database/ops/provision_roles.py:39`). The reader gets SELECT on **every** table in `perennial` (`provision_roles.py:94`), including the raw `source_batches.payload` and run diagnostics. M3's four new roles aren't built. The `api` database workload already exists: pool 5 + 5 overflow, 5 s statement timeout (`src/db/session.py:10-18`).
- **#22, as it stands:**
  - **`app` tables:** `profiles`, `preferences`, `notifications`, `watchlist` (keyed `(user_id, ticker)`), `auth_challenges` and `verified_sessions`.
  - **RLS:** on each `app` table, with `owner = current_setting('request.jwt.claim.sub')`, which the API sets per transaction. The roadmap says `auth.uid()` and `request.jwt.claims` (roadmap M5). Which one wins is one of Bryan's RLS/JWT questions for Jaden.
  - **Token check:** tokens are validated by calling Supabase's `/user` endpoint.
  - **API role:** a NOLOGIN, NOBYPASSRLS `perennial_api` role, defined in its own script (`ops/provision_api_role.py`). It gets CRUD on those `app` tables and SELECT on only `perennial.publications` and `current_publication`.
  - **Dashboard endpoint:** `GET /api/watchlist` maps the publication into a frontend dashboard shape. That shape drops the publication id, the degraded marker and the as-of dates. It also derives display values the data doesn't contain: `sentiment` "Positive" or "Watch" from whether a record has signals or warnings, `sector` "Unclassified", and `price` from ARK's share price. Outside production it serves fixture data by default.

## Which role the API uses: a dedicated `perennial_api`, not `perennial_reader`

The API connects with a login role that inherits only `perennial_api`. It does not use `perennial_reader`, for three reasons:
- The reader can see raw provider payloads and run diagnostics, and an internet-facing role should hold the least.
- Design rule 3 says the app reads only published data.
- #22 and the roadmap (M3, M5) already name `perennial_api`. Defining it once avoids a second, competing definition.

**R1 defines `perennial_api` in `provision_roles.py`** with these grants:
- `perennial`, SELECT only:
  - `publications`, `current_publication`, `securities`, `security_identifiers` and `company_profiles`
  - on `publication_events`, the columns `id`, `publication_id`, `kind`, `checks` and `created_at`, **not** `actor` or `reason`
- Nothing else in `perennial`: no raw source tables, `source_batches`, `ingestion_runs`, `run_dependencies`, market-cap or Congress-history tables, and no writes anywhere.
- `perennial_api` joins the `ALTER DEFAULT PRIVILEGES … REVOKE` that `provision_roles.py` runs for its group roles (`provision_roles.py:72`). A table added to `perennial` later is then never readable by the API until a reviewed change grants it.
- The role is NOLOGIN and NOBYPASSRLS, and inherits no other role. Provisioning is idempotent and refuses an existing role with stronger attributes, as `provision_roles.py` already does for the other three.

**When #22 lands, its `app` grants join that same definition** (see "What R2 needs from #22"). This is narrower than the roadmap's "read-only on `perennial`". The narrowing is deliberate, so the checker should confirm it.

## Response shapes (v1)

These are the rules that keep responses stable for replay:

1. **`watchlist` is `publications.output`, verbatim.** The API never recomputes or reshapes it. A test proves it equals `replay(engine, id)`.
2. **The envelope comes only from data that never changes after publishing:** the `publications` row (including `output`), its one quality event and its at most one `degraded` event. Rollback events are about the pointer, not the publication, so they never change a publication's body.
3. **Fields are whitelisted.** The envelope copies named fields out of the event bodies. Any future key in an event never reaches the API without a v1 schema change.
4. **Serialization is canonical:** UTF-8 JSON with sorted keys and no insignificant whitespace. Postgres JSONB doesn't keep the original key order, so the API must not depend on it.

```json
{
  "api_version": "v1",
  "publication": {
    "id": 123,
    "created_at": "2026-10-23T17:05:00+00:00",
    "ranking_version": "watchlist-v3",
    "demo": false,
    "degraded": true,
    "stale_sources": [
      {"source": "insider_trades", "collected_at": "2026-10-22T17:01:00+00:00", "age_seconds": 86640}
    ],
    "market_caps_unavailable": 3,
    "congress_history_gaps": 0,
    "source_as_of": {"ark_holdings": "2026-10-22", "congress_trades": "2026-10-21", "insider_trades": "2026-10-20", "short_interest": "2026-10-15"}
  },
  "watchlist": { "...": "publications.output, verbatim" }
}
```

How each field is filled:
- **`degraded`** is true when a `degraded` event exists.
- **`stale_sources`** comes from that event's `reused` map, sorted by source. It is `[]` when the publication isn't degraded.
- **`market_caps_unavailable`** counts the records in `watchlist`, across all buckets, whose `warnings` contain `market_cap_unavailable` (`ranking.py:165`). Taking it from the immutable output makes it exact on every path:
  - a normal legacy collection, where missing FMP profiles are explicit `None` values (`runner.py:341`, `:365`) and no degraded event is written
  - the stored-value fallback
  - M4's pinned rows

  It doesn't by itself mean the publication is degraded. The degraded event's own `market_caps.unavailable` can't be the source, because it exists only on the fallback.
- **`congress_history_gaps`** is `degraded.congress_gaps.count`, or `0`.
- **`demo`** is true only when the quality event says so. A demo document can't pass for a real one.

### Endpoints in R1

| Endpoint | Returns | Caching |
|---|---|---|
| `GET /v1/watchlist` | The envelope for the current production publication. `404 no_current_publication` if there is none | `ETag: "v1-<id>"`. `If-None-Match` with a match returns `304` with no body. `Cache-Control: no-cache`, because the pointer moves |
| `GET /v1/publications/{id}` | **Only if Bryan keeps it (question E).** The same envelope for a publication by id. `404 unknown_publication`; `410 publication_withdrawn` for a withdrawn one | Same `ETag`. `Cache-Control: public, max-age=300` |
| `GET /v1/securities/{symbol}` | `{"api_version", "security": {id, symbol, name, security_type, exchange, currency, country, is_active, sector, industry}, "current": {"publication": <envelope.publication>, "entry": <the record from current output, or null>}}`. `404 unknown_security` | `Cache-Control: no-cache` |

- **Errors** use #22's shape, `{"detail": "...", "code": "..."}`, with fixed codes. A bad path parameter is `422 invalid_request`, and the response never echoes the input.
- **Symbol lookup.** The symbol is trimmed, uppercased and checked against `_TICKER` (`src/ingestion/schemas.py:36`). It resolves through `security_identifiers` (`scheme = 'ticker'`, values without a provider prefix), so `BRK-B` finds `BRK.B`. Inactive securities still resolve, with `is_active: false`. `sector` and `industry` come from `company_profiles`, the reviewed current classification.
- **Finding the entry.** `entry` is found by matching `securities.symbol` to the record's `ticker`, because records carry no `security_id`. Adding `security_id` to records would need a new ranking version, so that is left for later.

### R2 endpoint (after #22)

`GET /v1/me/watchlist` returns `{"api_version", "publication": <envelope.publication of the current publication>, "items": [{"security_id", "symbol", "name", "added_at", "entry": <record or null>}]}`. Items are sorted by `added_at`, then `security_id`. There is one watchlist per user (decision 4), so the endpoint takes no list name or id. Writes stay #22's endpoints.

## How R1 lands without making #22's rebase harder

- **Files.** R1 adds `src/api/__init__.py` with **exactly** #22's one-line docstring, so the two additions are identical and don't conflict. It also adds `src/api/read.py`, which exposes `router` and a `create_app()` used for tests and local runs. It doesn't add `src/api/app.py`.
- **Wiring after #22.** When #22 lands, its `app.py` adds `app.include_router(read.router)`, and `create_app()` stays as the test app.
- **Dependencies.** R1 adds the same pins #22 uses, so Jaden's rebase is a lock recompile, not a version negotiation. Locks follow ADR 0001.
- **Database access.** R1 uses `make_engine(workload="api")`, and each request runs in one `READ ONLY` transaction.

## What R2 needs from #22

R2 doesn't ask #22 for anything beyond Bryan's list. It names the exact interfaces R2 builds on:

1. **`app.watchlist` keyed `(user_id, security_id)`.** It has an FK to `perennial.securities(id)` and `ON DELETE CASCADE` from the user, as in Bryan's list ("watchlist by security_id").
2. **One RLS subject helper.** Whichever form Bryan settles with Jaden (`request.jwt.claim.sub` with #22's expression, or `auth.uid()` as in the roadmap), R2 uses the same one. The API also filters by user explicitly, so RLS is the second defense (D11).
3. **A FastAPI dependency that returns the verified user id.** It returns 401 with no private data for a missing, expired or malformed token, as in issue #3's scenario.
4. **One `perennial_api` definition, and #22 leaves schema `perennial` alone.** R1's provisioning owns every `perennial` grant for `perennial_api`, and #22's owns the `app` grants (in `ops/provision_api_role.py` or in `provision_roles.py`).
   - **Today's #22 script would break R1.** It revokes and re-grants on schema `perennial` itself (`provision_api_role.py:46`, `:51`). It then runs `REVOKE ALL ON ALL TABLES IN SCHEMA app, perennial` (`:56`) and grants back only `publications` and `current_publication` (`:75`).
   - **Why that matters.** Run after R1's provisioning, it silently strips R1's grants on `securities`, `security_identifiers`, `company_profiles` and the `publication_events` columns. That breaks `/v1/securities` and the envelope, and the result depends on which script ran last.
   - **What #22 must do.** Its provisioning issues no REVOKE or GRANT on schema `perennial` or any table in it. RU-6 checks both orders. The coordinator is taking this requirement to Jaden through Bryan.

## Acceptance criteria

### R1 (testable now, synthetic publications only)

**Watchlist and publications**
- **RA-1** **No publication.** `GET /v1/watchlist` with no current publication returns `404` with code `no_current_publication`.
- **RA-2** **The envelope.** With a current publication, the response is `200`:
  - `publication.id` is the pointer's publication.
  - `watchlist` is JSON-equal to `publications.output` **and** to `replay(engine, id)`.
  - `ranking_version` and `created_at` match the row.
- **RA-3** **Caching.** The `ETag` is `"v1-<id>"`. `If-None-Match: "v1-<id>"` returns `304` with no body, and a different tag returns `200`.
- **RA-4** **Degraded, from an M2 fault-injection publication.** It has `degraded: true`, `stale_sources` listing each reused source with `collected_at` and `age_seconds`, and the counts filled in.
- **RA-4b** **Not degraded.** A publication without a degraded event, and with every cap present, has `degraded: false`, `stale_sources: []` and both counts `0`.
- **RA-4d** **Missing caps on a normal collection.** A legacy publication whose collector returned `None` for some symbols, with no fallback and so no degraded event, has `degraded: false` and `market_caps_unavailable` equal to the number of records warning `market_cap_unavailable` (more than 0).
- **RA-4c** **Whitelisting.** A key added to a test's degraded-event body doesn't appear in the response.
- **RA-5** **As of.** `source_as_of` equals `ranking_config.source_as_of` for the four watchlist sources, with `null` allowed.
- **RA-6** **Byte-stable.** While publication P is current, `GET /v1/watchlist` returns byte-identical bodies in all of these cases:
  - across calls
  - after a pipeline run that fails or is refused, so the pointer doesn't move
  - after `cli rollback --to` another publication and then back to P

  The test compares raw response bytes.
- **RA-7** **Rollback.** After `cli rollback --to N`, `GET /v1/watchlist` returns publication `N` with `ETag: "v1-N"`.
- **RA-9** **Demo marker.** A demo publication (quality event `checks.demo`) has `demo: true`. Any other publication has `demo: false`.

**Only if Bryan keeps `GET /v1/publications/{id}` (question E, option 2).** The test maker can leave these until Bryan answers:
- **RA-8a** **Bad ids.** An id that doesn't exist returns `404 unknown_publication`. A non-integer, zero, or more than 2^63−1 returns `422 invalid_request`, without echoing the input.
- **RA-8b** **Withdrawn.** A publication that production was rolled back away from returns `410 publication_withdrawn` with no publication data. A publication is withdrawn when it is the `from_publication_id` of a rollback event and isn't the current publication. If a later rollback makes it current again, it is served again.
- **RA-8c** **Stable and short-cached.** A non-withdrawn publication's body is byte-identical across calls, later publications and rollbacks between other publications, and is served with `Cache-Control: public, max-age=300`.

**Securities**
- **RA-10** **Lookup.**
  - `GET /v1/securities/BRK-B` resolves to the security whose symbol is `BRK.B`.
  - An unknown symbol gives `404 unknown_security`.
  - An invalid symbol (lowercase is fine; spaces, more than 20 characters or bad characters are not) gives `422 invalid_request`.
  - An inactive security returns `200` with `is_active: false`.
- **RA-11** **The entry.** `current.entry` is that symbol's record from the current publication's output, or `null` if it isn't in it. `current.publication` is the same envelope as RA-2.

**Role and grants**
- **RA-12** **Allowed.** `perennial_api` can SELECT exactly the tables and columns listed above. SELECT on `publication_events.actor` or `.reason` fails.
- **RA-13** **Refused.** These fail:
  - SELECT on `source_batches`, `ingestion_runs`, `run_dependencies`, any raw source table, `market_cap_observations` or `congress_trade_history`
  - any INSERT, UPDATE or DELETE in `perennial`
- **RA-14** **Role shape.** The role is NOLOGIN and NOBYPASSRLS and inherits nothing. Running provisioning twice is a no-op, and leaves exactly the RA-12 grant set.
- **RA-14b** **Default privileges.** A table the owner creates in `perennial` after provisioning isn't readable by `perennial_api`.

**Startup, transactions and logs**
- **RA-15** **Startup refusal.** `create_app()` refuses to start if its connection role can INSERT, UPDATE or DELETE any table in `perennial`, or is a superuser, or has BYPASSRLS. This covers a `DATABASE_URL` that points at the owner.
- **RA-16** **Read-only transactions.** Each request runs in a READ ONLY transaction on the `api` workload, and a write attempted inside a handler fails.
- **RA-17** **Logs and errors.** Logs are JSON (`configure_logging`) and contain no query strings or database URLs. Error bodies have only `detail` and `code`.

**Schema document**
- **RA-18** `docs/technical-specs/API.md` documents the v1 schemas. A snapshot test of the `/v1` part of the OpenAPI document fails whenever a v1 shape changes, so every change to the shape is deliberate.

### R2 (after #22 lands; tests run against the Supabase local stack, as the roadmap plans for `app` and RLS tests)

- **RU-1** **Bad tokens.** `GET /v1/me/watchlist` with a missing, expired or malformed token returns `401` with no private data (issue #3).
- **RU-2** **Own items only.** With two users, each sees only their own items.
- **RU-2b** **RLS without the API's filter.** Querying as `perennial_api` with user A's subject set returns none of user B's rows, even with the API's explicit filter bypassed.
- **RU-3** **Item contents.** Each item has `security_id`, `symbol`, `name` and `added_at`. `entry` is the current publication's record or `null`. The response envelope is the same as RA-2.
- **RU-4** **No current publication.** The items still return, with `publication: null` and every `entry: null`.
- **RU-5** **No pipeline writes.** `perennial_api` still can't write `perennial` after the `app` grants are added (RA-13 still passes).
- **RU-6** **Both provisioning orders.** Run R1's provisioning and #22's in both orders on fresh databases. Each time, `perennial_api` ends with exactly R1's `perennial` grant set (RA-12, RA-13, RA-14b) and #22's `app` grants.

## Needs Bryan

| # | Question | Recommendation | Blocks |
|---|---|---|---|
| A | **Is the watchlist public, or only for signed-in users?** | **Public.** It holds no user data, and the M7 shadow diff can read it without tokens. Signing in doesn't change the licensing question (row C) | R1's deployment, not its build |
| B | **#22's `GET /api/watchlist` dashboard endpoint vs `GET /v1/watchlist`.** | The frontend moves to `/v1/watchlist`, and `/api/watchlist` is removed when it does. Its display mapping (`sentiment`, `reason`, `sector` "Unclassified", ARK share price as `price`) is presentation, and partly values the data doesn't contain. It belongs in the frontend, or in a documented view built on `/v1`. This is a follow-up after #22 lands, not a change to #22 | which endpoint the frontend calls |
| C | **Provider display licensing (D8, decision 3).** Serving the watchlist shows FMP (Congress trades, market caps), Finnhub (insider), Nasdaq (short interest, an unofficial endpoint) and ARK (an unofficial endpoint) data to users. | Build R1 now, but run it only locally and on staging until D8 is settled. Don't block the build on it | any public deployment (M7) |
| D | **One version prefix.** | Mount #22's routers under `/v1` when it lands (one `prefix="/v1"` at include time), before any client ships. Then R2 extends #22's `GET /v1/me/watchlist`, and there aren't two endpoints for the same list | R2's path |
| E | **Should past publications be served by id, including rolled-back ones?** A rollback usually means the newer publication's data was wrong. As first drafted, `GET /v1/publications/{id}` served every publication forever with `public, max-age=86400`, so withdrawn data stayed public and could sit in shared caches for a day. That also matters for D8 (public caching of provider data). | **Option 1 (recommended): leave it out of R1.** The outcome, the roadmap and the app need only the current publication. Clients revalidate it with the ETag, and the M7 diff can read the database. **Option 2:** keep it, with `410 publication_withdrawn` for a publication production was rolled back away from, and a 5-minute public cache (RA-8a–8c). **Option 3:** keep it as first drafted. | `GET /v1/publications/{id}` only |

Bryan's open RLS/JWT questions for Jaden (`auth.uid()` vs `request.jwt.claim.sub`, and the Supabase `/user` call vs verifying signing keys locally) decide R2's dependency 2 and 3. R2 adopts whatever #22 lands with. This note doesn't reopen them.

## Out of scope

- **Belongs to #22 (Bryan's list):** preferences, notifications, `DELETE /me` and export.
- **Later milestones:** the score endpoint `GET /v1/securities/{symbol}/score` (M6); price and change fields (no source yet).
- **Platform and frontend:** rate limiting and CORS (#22's app and the hosting edge); deployment and hosting (M3); the dashboard presentation layer.
- **Publication changes:** adding `security_id` to publication records (a ranking-version change).

## Gates

- **Review:** the checker reviews this note. The auditor reviews R1's role and grants (RA-12 to RA-15) and R2's RLS and JWT boundary (Gate M5).
- **Builds:** each part is test-maker first, then the implementer. CI as usual: ruff, format, strict mypy and the full suite with `REQUIRE_DATABASE_TESTS=1`.
- **Safety:** synthetic data only. No provider keys, no deployment, no push.
