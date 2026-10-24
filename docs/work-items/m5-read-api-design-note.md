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
  - `publications`, `current_publication`, `securities` and `security_identifiers`
  - on `company_profiles`, the columns `id`, `sector` and `industry`. The other columns are reviewed public reference data, but the endpoint serves only these, so the grant stays column-level.
  - on `publication_events`, the columns `id`, `publication_id`, `kind`, `checks` and `created_at`, **not** `actor` or `reason`
- **Explicit columns in code.** Because two grants are column-level, the implementer must select explicit columns. SQLAlchemy's `select(m.publication_events)` expands to every column, `actor` and `reason` included, and fails as `perennial_api` with 42501. Tests that run as the API role (RA-2, RA-4, RA-12) catch it, but it should be designed in, not discovered.
- Nothing else in `perennial`: no raw source tables, `source_batches`, `ingestion_runs`, `run_dependencies`, market-cap or Congress-history tables, and no writes anywhere. `USAGE` on schema `perennial` only, never `CREATE`.
- **`perennial_api` joins every safety check `provision_roles.py` already runs for its group roles:**
  - the role-count check (`provision_roles.py:36-38`), which becomes four roles
  - the attribute and membership refusals (`:43-51`): an existing role with any of `rolsuper`, `rolcreatedb`, `rolcreaterole`, `rolreplication`, `rolbypassrls` or `rolcanlogin`, or with any membership, is refused
  - the table and column `REVOKE` loop (`:83-90`)
  - the `ALTER DEFAULT PRIVILEGES … REVOKE` (`:72`), so a table a future migration adds is never readable by the API until a reviewed change grants it

  Column grants are re-applied after every migration, like the rest of the grants.
- **Provisioning also refuses** if `perennial_api` owns any relation or schema (as #22's `provision_api_role.py:23-27` already checks). It also refuses if any of Supabase's API roles holds a privilege on a table in `perennial`, a privilege on any of its columns, or `USAGE` on the schema (and the same for `app`, once R2 lands). Their names are a provisioning parameter, defaulting to `("anon", "authenticated", "service_role")`, so tests can pass their own. `REVOKE … FROM PUBLIC` doesn't cover named roles, so this is asserted, not assumed.
- Provisioning is idempotent. It runs as Supabase's dashboard `postgres` role, which isn't a superuser but already provisions the other three groups. The script creates `perennial_api`, so that role holds ADMIN on it for the login member's `IN ROLE` grant.

**The login role is Bryan's to create** (D3: agents never hold credentials). It must be:
- `LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS IN ROLE perennial_api`
- a member of nothing else
- given a `CONNECTION LIMIT` sized to the `api` pool per instance (5 + 5 today)

`INHERIT` is required, because with the pooler in transaction mode `SET ROLE` can't be relied on. The startup check (RA-15) refuses any other membership anyway.

**Supabase's exposed schemas.** `perennial` and `app` must never appear in Supabase's API "exposed schemas" setting. Otherwise PostgREST serves those tables to anyone holding the project's anon key, bypassing this role entirely. This is a deployment setting, confirmed on the real project before any deployment.

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
- **Database access.** R1 uses `make_engine(workload="api")`. Each request runs in one transaction marked read-only **per transaction**: `BEGIN READ ONLY`, which SQLAlchemy's psycopg dialect issues for `execution_options(postgresql_readonly=True)`. It is never a session-level `SET`, because Supabase's pooler runs in transaction mode and reuses backends. Read-only is a safety net; the grants are the boundary. Optionally, Bryan sets `default_transaction_read_only = on` on the **login** role. A setting on the NOLOGIN group doesn't apply to its members.

## What R2 needs from #22

R2 doesn't ask #22 for anything beyond Bryan's list. It names the exact interfaces R2 builds on:

1. **`app.watchlist` keyed `(user_id, security_id)`.** It has an FK to `perennial.securities(id)` and `ON DELETE CASCADE` from the user, as in Bryan's list ("watchlist by security_id").
2. **One RLS subject helper.** Whichever form Bryan settles with Jaden (`request.jwt.claim.sub` with #22's expression, or `auth.uid()` as in the roadmap), R2 uses the same one. The API also filters by user explicitly, so RLS is the second defense (D11).
3. **A FastAPI dependency that returns the verified user id.** It returns 401 with no private data for a missing, expired or malformed token, as in issue #3's scenario.
4. **One `perennial_api` definition, and #22 leaves schema `perennial` alone.** R1's provisioning owns every `perennial` grant for `perennial_api`, and #22's owns the `app` grants (in `ops/provision_api_role.py` or in `provision_roles.py`).
   - **Today's #22 script would break R1.** It revokes and re-grants on schema `perennial` itself (`provision_api_role.py:46`, `:51`). It then runs `REVOKE ALL ON ALL TABLES IN SCHEMA app, perennial` (`:56`) and grants back only `publications` and `current_publication` (`:75`).
   - **Why that matters.** Run after R1's provisioning, it silently strips R1's grants on `securities`, `security_identifiers`, `company_profiles` and the `publication_events` columns. That breaks `/v1/securities` and the envelope, and the result depends on which script ran last.
   - **What #22 must do.** Its provisioning issues no REVOKE or GRANT on schema `perennial` or any table in it. RU-6 checks both orders. The coordinator is taking this requirement to Jaden through Bryan.

### R2 boundary decisions, recorded now

These three hold under either answer to Bryan's open questions for Jaden (`auth.uid()` vs `request.jwt.claim.sub`, and the Supabase `/user` call vs local key verification). They don't reopen those questions.
1. **The subject is transaction-local only.** The API sets it with `set_config('<name>', $1, true)` inside the request's transaction, never with `SET` or `set_config(..., false)`. Transaction-mode pooling reuses backends across users, so a session-level setting would carry user A's subject into user B's next transaction.
   - The API validates the subject as a UUID before setting it.
   - Every policy expression denies when the setting is missing or empty: `nullif(current_setting('<name>', true), '')::uuid`, compared with equality. A missing subject then gives no rows, rather than an error or everything.
2. **Policies name the role.** Every `app` policy is written `TO perennial_api`. PostgREST sets `request.jwt.claim.*` and `request.jwt.claims` for its own roles, so naming the role means a claim set by any other path never satisfies a policy, whichever setting name wins.
3. **Ownership and FORCE.** `app` tables are owned by `perennial_owner` (or a dedicated app owner), never by `perennial_api`, and have both `ENABLE ROW LEVEL SECURITY` and `FORCE ROW LEVEL SECURITY`. Table owners bypass RLS unless it is forced. The startup check refuses ownership anyway, and FORCE keeps RU-2b meaningful even if provisioning drifts.

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
- **RA-4c** **Whitelisting.** A key added to a test's degraded-event body doesn't appear in the response.
- **RA-4d** **Missing caps on a normal collection.** A legacy publication whose collector returned `None` for some symbols, with no fallback and so no degraded event, has `degraded: false` and `market_caps_unavailable` equal to the number of records warning `market_cap_unavailable` (more than 0).
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
- **RA-12** **Allowed.** `perennial_api` can SELECT exactly the tables and columns listed above. SELECT on `publication_events.actor` or `.reason`, or on a `company_profiles` column other than `id`, `sector` and `industry`, fails with 42501.
- **RA-13** **Refused, computed rather than hand-listed.** The refused set is every table in `provision_roles.TABLES` minus the RA-12 allow list, plus `public.alembic_version`. For each one, SELECT fails with 42501. For every table in `perennial`, INSERT, UPDATE, DELETE and TRUNCATE fail. Computing the set means the next migration's table is covered without editing the test.
- **RA-14** **Role shape.** `perennial_api` has none of `rolsuper`, `rolcreatedb`, `rolcreaterole`, `rolreplication`, `rolbypassrls` or `rolcanlogin`. It is a member of no role and owns no relation or schema. Running provisioning twice is a no-op, and leaves exactly the RA-12 grant set.
- **RA-14b** **Default privileges.** A table the owner creates in `perennial` after provisioning isn't readable by `perennial_api`.
- **RA-14c** **Supabase API roles.** Provisioning refuses (fixed error, nothing changed) when `anon`, `authenticated` or `service_role` has any of these: a privilege on a `perennial` table (`has_table_privilege`), a privilege on any column of one (`has_any_column_privilege`), or `USAGE` on schema `perennial` (`has_schema_privilege`). A column or schema grant is still an exposure through PostgREST. The test covers one table grant, one column grant and one schema `USAGE` grant on a test database, and provisioning refuses each. It passes unique role names (for example `anon_<hex>`) through the parameter, as `test_ops` already does for owner, importer and reader. Roles belong to the whole server, not one database, so fixed names would collide between seats sharing the test server and survive a crash. It drops them in `finally`.

**Startup, transactions and logs**
- **RA-15** **Startup refusal.** `create_app()` runs one check on the `api` workload before serving. It fails closed on any clause, with a fixed error code and no role name, URL or SQL in the output, using bound parameters:
  1. **Attributes.** `session_user` and `current_user` are the same role, and that role has none of `rolsuper`, `rolcreatedb`, `rolcreaterole`, `rolreplication` or `rolbypassrls` (`pg_roles`). Attributes aren't inherited through membership, so they are checked on the session role itself.
  2. **Membership.** The roles the session role belongs to, directly or transitively, are exactly `{perennial_api}`. The set excludes only the role itself, and is computed from `pg_auth_members` or `pg_has_role(session_user, r.rolname, 'MEMBER')`. Predefined `pg_*` roles count too, because several carry real power: `pg_write_all_data` and `pg_read_all_data` reach every table; `pg_read_server_files`, `pg_write_server_files` and `pg_execute_server_program` reach the host; `pg_database_owner` owns the database. This closes the `SET ROLE` path to a non-inherited grant. For example, a NOINHERIT member of `perennial_owner` or `pg_write_all_data` would pass clauses 1, 3 and 4 but can `SET ROLE` into full writes.
  3. **No writes.** For every table in `pg_tables WHERE schemaname = 'perennial'` (and `'app'` once R2 lands, against its intended write set), `has_table_privilege(current_user, tbl, …)` is false for INSERT, UPDATE, DELETE and TRUNCATE. The privilege functions evaluate inheritance and ownership; `information_schema.role_table_grants` doesn't, so it is never used. An empty `perennial` table list fails the check, so an unmigrated or wrong database can't pass trivially.
  4. **No ownership.** The session role owns no relation or schema (`pg_class.relowner`, `pg_namespace.nspowner`), and has no `CREATE` on schema `perennial` (`has_schema_privilege`) or on the database (`has_database_privilege`). An owner can re-grant itself anything, so ownership is refused independently of clause 3.

  Tests cover each clause with its own refusing role: a superuser or BYPASSRLS login, a NOINHERIT member of `perennial_owner`, a NOINHERIT member of `pg_write_all_data`, a login that is a member of `perennial_api` and `perennial_reader`, a role that owns a table, and an empty database. Each fails with the same fixed code, and a correctly shaped login starts.

  **Test setup.**
  - Every role these tests create has a unique suffix and is dropped in `finally`, as in RA-14c.
  - The clause 1 cases (SUPERUSER, BYPASSRLS) need a test connection that is itself a superuser, because only superusers can create those roles. That holds today: `perennial_test` on the local test server is `rolsuper`, and CI's `POSTGRES_USER` is the image's superuser. The test can rely on it rather than skip.
- **RA-16** **Read-only transactions.** Each request's transaction is read-only per transaction (`BEGIN READ ONLY`, through `postgresql_readonly=True`), never a session `SET`. The test drives the request-transaction helper directly, with a test engine whose role *could* write, because such a role would fail RA-15 at startup. A write attempted inside it fails with 25006, and the next transaction on the same pooled connection is still read-only.
- **RA-16b** **Every route goes through the helper.** With the request-transaction helper patched to record its calls, a test hits every `/v1` route once under `create_app()`. It asserts exactly one helper call per route and no other use of the engine while serving requests. Read-only is then a property of the API, not just of the helper.
- **RA-17** **Logs and errors.** Logs are JSON (`configure_logging`) and contain no query strings or database URLs. Error bodies have only `detail` and `code`.

**Schema document**
- **RA-18** `docs/technical-specs/API.md` documents the v1 schemas. A snapshot test of the `/v1` part of the OpenAPI document fails whenever a v1 shape changes, so every change to the shape is deliberate.

### R2 (after #22 lands; tests run against the Supabase local stack, as the roadmap plans for `app` and RLS tests)

- **RU-1** **Bad tokens.** `GET /v1/me/watchlist` with a missing, expired or malformed token returns `401` with no private data (issue #3).
- **RU-2** **Own items only.** With two users, each sees only their own items.
- **RU-2b** **RLS without the API's filter.** Querying as `perennial_api` with user A's subject set returns none of user B's rows, even with the API's explicit filter bypassed.
- **RU-3** **Item contents.** Each item has `security_id`, `symbol`, `name` and `added_at`. `entry` is the current publication's record or `null`. The response envelope is the same as RA-2.
- **RU-4** **No current publication.** The items still return, with `publication: null` and every `entry: null`.
- **RU-5** **No pipeline writes.** With the `app` grants added, the computed RA-13 assertion and the RA-15 startup check still pass. RA-15's clause 3 now also covers `app`, against #22's intended write set.
- **RU-5b** **Boundary decisions hold.** These are the three decisions in "R2 boundary decisions, recorded now":
  - With no subject set, or an empty one, every `app` policy returns zero rows and no error.
  - A non-UUID subject is refused before `set_config`.
  - After a request for user A, the next transaction on the same pooled connection, for user B, sees no subject until it sets its own.
  - Every `app` policy is `TO perennial_api`.
  - Every `app` table is owned by the owner role, with ENABLE and FORCE ROW LEVEL SECURITY.
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

- **Review:** the checker reviews this note. The auditor reviews R1's role, grants and startup check (RA-12 to RA-16) and R2's RLS and JWT boundary (RU-5b, RU-6, Gate M5).
- **Builds:** each part is test-maker first, then the implementer. CI as usual: ruff, format, strict mypy and the full suite with `REQUIRE_DATABASE_TESTS=1`.
- **Safety:** synthetic data only. No provider keys, no deployment, no push.

## Addendum (2026-10-24): basic rate limiting for the public reads

**Why:** decision 14 makes `/v1/watchlist` public, with no login and basic rate limiting. R1 (`feat/m5-read-api` at `25f38f0`) has none. The API runs only locally and on staging until D8 is settled (decision 16), and the API host isn't chosen yet (decision 21 covers only the database).

**Scope:**
- R1's read routes, `GET /v1/watchlist` and `GET /v1/securities/{symbol}`.
- The two #22 routes that call `app.email_is_registered`, `POST /v1/auth/signup` and `POST /v1/auth/forgot-password`. That function is an account-enumeration oracle (auditor). They're installed in the #22 fix branch (its change 5 and RB-5d).
- Other `/v1/auth/*` routes rely on Supabase Auth's own limits and Jaden's per-email send limit (`check_email_limit`).

### Where it lives: both layers, and "basic" means the in-app one

- **In the app (this addendum).** A per-client token bucket, charged before any other work. It is always on, host-independent and testable now. Its job is to stop one client from monopolizing the `api` pool (5 + 5 connections, 5 s statement timeout, `src/db/session.py:12-18`) and from scraping the enumeration oracle.
- **At the edge or host (a deployment checklist item, not code).** Request-rate and connection limits, configured when the API host is chosen. Only this layer can absorb a flood, and it's required before any public deployment.
- **Delivery (decision 26).** Recommended: a small follow-up PR on top of R1 (`feat/m5-rate-limit`) before any public deployment, rather than reopening the reviewed R1 before its push. That's safe because the API isn't public until D8 (decision 16). The #22 fix branch then calls the same installer.

### One installer, used by both apps

`read.install(app, config)` installs everything, and both `create_app()` and #22's integrated app call it (checker RL-B):
- uvicorn's `ProxyHeadersMiddleware`, with `trusted_hosts` from the validated trusted-proxy setting. **`read.install` is the only place it is added.** Neither `create_app()` nor #22's `app.py` adds it any other way, and uvicorn's own proxy handling is off (see the run commands below). One middleware means one trust decision.
- the limiter state on `app.state`, so state is **per app instance**, never module-level. Each `create_app()` starts empty, and tests can't leak buckets into each other.
- the configuration and its startup validation (RL-11)

The limits are attached as dependencies:
- `read.rate_limit` on `read.router`
- `read.auth_rate_limit` on the two #22 routes, in the #22 branch

Each limit runs **before any other dependency or validation that touches the engine**. Routes added later must keep that order. A request reaching a limit dependency on an app where `install` never ran gets `503` with code `rate_limiter_missing`. It fails closed, never unlimited.

### How a client is keyed

- **The key comes from `request.client`**, as uvicorn's middleware leaves it. App code never parses `X-Forwarded-For` or `Forwarded` itself.
- **Normalization, in this order:**
  1. **No peer** (`request.client` is `None`, as with a unix-socket listener or a transport with no peer information): one shared `unknown` bucket. It is never exempt, and never a 500.
  2. **The host string.** Strip one pair of surrounding brackets and a trailing `:port`, so `[2001:db8::1]:443` becomes `2001:db8::1` and `198.51.100.1:5678` becomes `198.51.100.1`. Uvicorn passes a forwarded value through unvalidated, and a proxy that writes `ip:port` would otherwise give every source port its own bucket.
  3. **IPv4** is keyed by its full address.
  4. **IPv4-mapped IPv6** (`::ffff:a.b.c.d`, which is how a dual-stack `::` listener presents every IPv4 client) is keyed by its embedded IPv4 address, exactly as IPv4 is. Without this step, every IPv4 client would share one /64 bucket.
  5. **Other IPv6** is keyed by its /64, because one host commonly controls a whole /64. A client with a /48 still controls 65,536 /64 buckets, which is acceptable for "basic", since the edge limit is the global answer.
  6. **Anything still not an IP address** (TestClient's `testclient`, a misbehaving proxy) is keyed by the literal string, truncated to 64 characters. It is never exempt.
- **Trusted proxies: list every hop.**
  - `API_TRUSTED_PROXIES` lists **every** proxy hop the host guarantees in front of the API, by IP or CIDR, and is empty by default (trust no one).
  - Uvicorn keys by the rightmost address in `X-Forwarded-For` that isn't trusted, so a missing hop collapses every client onto that hop's address. For example, if the chain is a CDN then the host's proxy and only the host's proxy is listed, everyone is keyed by the CDN's egress address and throttled together.
  - The literal `*` is refused. Trusting every peer needs the separately named `API_TRUST_ALL_PROXIES=1`, which is for a host that guarantees all traffic passes through a proxy that overwrites client-supplied forwarded headers. A permissive value is then never a typo away.
- **The run commands** turn uvicorn's own proxy handling off, so only the installer's middleware runs and uvicorn's default trust of `127.0.0.1` never applies behind the operator's back:
  - `--no-proxy-headers` on the command line
  - `proxy_headers=False` in the #22 branch's `python -m src.api` entry
- **Spoofing.** A client that sends `X-Forwarded-For` from an untrusted address is keyed by its real peer address. Fake entries that a client prepends are skipped, because the key is the rightmost untrusted address.
- **Deployment checklist:**
  - The host's proxy must append the real peer to `X-Forwarded-For`. Uvicorn's middleware reads only `X-Forwarded-For`, not RFC 7239 `Forwarded`.
  - Every hop is listed in `API_TRUSTED_PROXIES`.
- **Shared addresses** (NAT, carrier-grade NAT) share one bucket. That's acceptable for "basic", and the read limits are generous for that reason.

### Limits and the response

- **Read bucket** (`read.rate_limit`): capacity 30, refilling at 60 a minute.
  - Configuration: `API_RATE_LIMIT_BURST=30`, `API_RATE_LIMIT_PER_MINUTE=60`.
  - The watchlist changes at most once a day, and a normal client revalidates with its ETag every few minutes. So 60 a minute is far above legitimate use and far below what 10 connections can serve.
- **Auth bucket** (`read.auth_rate_limit`, signup and forgot-password together): capacity 5, refilling at 30 an hour.
  - Configuration: `API_AUTH_RATE_LIMIT_BURST=5`, `API_AUTH_RATE_LIMIT_PER_HOUR=30`.
  - A person signs up or asks for recovery a few times at most. Each call answers "is this email registered?", so the bucket is deliberately small. It's a separate bucket per client key, so reads don't spend auth tokens.
- **Every request to a limited route costs one token, before any database work.** That includes `304` responses and `404` and `422` errors. A 304 still opens a read transaction to look up the pointer (`read.py`, `get_watchlist`), so not counting 304s would leave the database unprotected. A limited auth request calls neither `email_is_registered` nor Supabase.
- **Over the limit:**
  - `429`
  - `Retry-After: <whole seconds until the next token, rounded up, at least 1>`
  - body `{"detail": "too many requests; retry later", "code": "rate_limited"}`, in R1's canonical JSON
  - `Cache-Control: no-store`
  - no `ETag` and no publication data

  A limited request opens no transaction.
- **Browsers.** If the frontend should read `Retry-After`, #22's CORS configuration adds it to `expose_headers`. That's optional and not a security item.
- **No `RateLimit-*` headers in v1.**

### State with several workers

- **In memory, per app instance in each process.** That's acceptable for "basic". With N workers the effective limit is up to N times higher. This is documented, and the edge limit is the global one. A restart resets the buckets.
- **Thread-safe.** R1's routes are plain `def` functions, so FastAPI runs them in its threadpool. Bucket updates take a lock.
- **Bounded memory.** The limiter tracks at most `API_RATE_LIMIT_MAX_CLIENTS` buckets per bucket kind (10,000 by default). When the table is full, it evicts the least recently used bucket. That is by definition a quiet client, so a client that is hammering keeps its drained bucket, and rotating addresses can't grow memory. An evicted client starts again with a full bucket, which is the only safe reset.
- **A monotonic clock, injectable for tests.**
- **Shared state** (Redis and the like) is out of scope. Revisit it only if the API runs several instances and the edge can't limit.

### Effect on accepted criteria

- **RA-15 (startup check):** unchanged.
- **RA-16b (every route through the helper):** it still holds for every *served* request. Its test keeps each route under the limit by using a fresh client key, and a limited request makes no helper call (RL-7).
- **RA-3 and RA-6 (ETag, byte stability):** unchanged under the limit.
- **RA-17 (logs):** RL-13 below.
- **RA-18 (OpenAPI snapshot):** the snapshot gains the `429` response on both read routes. That is a deliberate snapshot update, and `docs/technical-specs/API.md` documents it.
- **The #22 packet:** RB-5d covers the integrated app.

### Acceptance criteria (RL)

**Limiting**
- **RL-1** **Under the limit.** Responses don't change. R1's test suite passes with the limiter on at its defaults, and each `create_app()` starts with empty state.
- **RL-2** **Over the limit.** In one instant, the request after `burst` gets a `429` with an integer `Retry-After` of at least 1, the body exactly `{"code", "detail"}` in canonical JSON, `Cache-Control: no-store`, and no `ETag`.
- **RL-3** **Refill.** With a fake clock, the next request succeeds after `Retry-After` seconds, and requests at the refill rate succeed indefinitely.
- **RL-4** **Independent clients and buckets.** Two client keys have independent buckets, and the read and auth buckets of one key are independent.

**Keying**
- **RL-5** **Proxies, spoofing and hops.**
  - With `API_TRUSTED_PROXIES` empty, a client's own `X-Forwarded-For` doesn't change its key.
  - With every hop of a two-hop test chain listed, requests are keyed by the original client.
  - With only the inner hop listed, every client is keyed by the outer hop's address. This test documents the collapse the setting must avoid.
  - Entries a client prepends are ignored, and a forwarded header from any other peer is ignored.
- **RL-6** **Address normalization.**
  - **IPv6 /64:** two addresses in one /64 share a bucket, and addresses in different /64s don't.
  - **IPv4-mapped:** `::ffff:198.51.100.1` and `198.51.100.1` share a bucket. `::ffff:198.51.100.1` and `::ffff:198.51.100.2` don't.
  - **Ports and brackets:** `198.51.100.1:5678` and `198.51.100.1:5679` share `198.51.100.1`'s bucket, and `[2001:db8::1]:443` shares `2001:db8::1`'s /64.
- **RL-6b** **Non-IP and missing peers.** A request with no `request.client` uses the shared `unknown` bucket. A non-IP host string uses its own literal-string bucket, truncated to 64 characters. Neither is exempt, and neither causes a 500.

**Cost and safety**
- **RL-7** **No database work when limited.** A `429` makes no `read_transaction` call and doesn't use the engine. The test patches the helper, as RA-16b does. A limited auth request calls neither `email_is_registered` nor the auth provider.
- **RL-8** **304s count.** Conditional requests consume tokens. Once the bucket is empty, a matching `If-None-Match` gets `429`, not `304`.
- **RL-9** **Thread safety.** With the clock frozen, concurrent requests from one client through the threadpool give exactly `burst` successes.
- **RL-10** **Bounded memory.** With `max_clients = K`, K + M distinct clients never leave more than K buckets of a kind, and an evicted client starts again with a full bucket.
- **RL-14** **Fails closed without the installer.** On an app where `read.install` didn't run, a request to a limited route returns `503 rate_limiter_missing` and does no database work.
- **RL-15** **Same behaviour in both apps.** The integrated #22 app limits and keys exactly as `create_app()` does, through `read.install`. This is the RL side of the #22 packet's RB-5d.
- **RL-15b** **One proxy middleware.** The middleware stacks of both `create_app()` and the integrated app contain exactly one `ProxyHeadersMiddleware`, the one `read.install` added. `python -m src.api` passes `proxy_headers=False` (patched `uvicorn.run`).

**Configuration, docs and logs**
- **RL-11** **The app's own validation.** Uvicorn silently accepts invalid trusted-proxy entries, so the app validates them itself. The limits come from configuration. These refuse at startup with a fixed error:
  - zero, negative or non-integer limits
  - a trusted-proxy entry that isn't an IP or CIDR
  - a literal `*`

  `API_TRUST_ALL_PROXIES=1` is the only way to trust every peer.
- **RL-12** **Docs and snapshot.** `API.md` documents:
  - the `429` and `Retry-After`
  - both buckets' defaults and the per-worker caveat
  - `API_TRUSTED_PROXIES` (every hop), `API_TRUST_ALL_PROXIES`, the run flags, and the `X-Forwarded-For` requirement

  The OpenAPI snapshot includes `429` on both read routes.
- **RL-13** **Logs.** At most one JSON `rate_limited` log line per key per `Retry-After` window, carrying a count of the rejections it covers. Never one line per 429, which would turn a flood into a log flood. The line has no client address, path or header values.

### For Bryan

- **Decision 26 (open):** recommended as a follow-up PR on top of R1, before any public deployment.
- **Edge limits:** a deployment checklist item once the API host is chosen.

There are no new decisions. The limits are configurable defaults, not product choices.

**Owner after review:** the implementer, on `feat/m5-rate-limit` from R1, test-maker first. The #22 branch installs it (its RB-5d). **Review:** the checker; the auditor for keying, trusted proxies, memory bounds and the auth bucket (RL-5, RL-6, RL-6b, RL-10, RL-11).
