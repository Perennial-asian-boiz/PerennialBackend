# Perennial read API, v1

**Status:** M5 R1, public reads of published data. **Design:** `docs/work-items/m5-read-api-design-note.md`. **Code:** `development/backend/src/api/read.py`. **Schema snapshot:** `development/backend/tests/fixtures/api/openapi_v1.json`. Any change to a v1 shape fails that snapshot test, so every change is deliberate.

R1 serves no user data and needs no token (Bryan's question A; public is the recommended answer). `GET /v1/publications/{id}` isn't in R1 (question E). `GET /v1/me/watchlist` is R2, after #22.

## Conventions

- **Version:** every path starts with `/v1`, and every body carries `api_version: "v1"`.
- **Encoding:** bodies are UTF-8 JSON, serialized canonically (sorted keys, no insignificant whitespace). A publication's body is byte-identical every time it's served.
- **Errors:** every error body is exactly `{"detail": "...", "code": "..."}`, with a fixed `code`. The detail never echoes request input.

| code | status | when |
|---|---|---|
| `no_current_publication` | 404 | `GET /v1/watchlist` when no publication is current |
| `unknown_security` | 404 | `GET /v1/securities/{symbol}` when no security has that ticker or alias |
| `invalid_request` | 422 | a path parameter is invalid (for example a symbol with spaces, bad characters or more than 20 characters) |

## `GET /v1/watchlist`

The current production publication.

- **`200`:** `{"api_version": "v1", "publication": <publication>, "watchlist": <output>}`.
- **Headers:** `ETag: "v1-<id>"` and `Cache-Control: no-cache`. The production pointer moves, so clients revalidate.
- **Conditional request:** `If-None-Match` with the current tag returns `304` with no body.
- **`404 no_current_publication`:** when nothing is current.

`watchlist` is `publications.output`, verbatim: `{schema_version, ranking_version, generated_at, popular_stable, affordable_growing, unresolved, errors}`. Each record has `ticker`, `bucket`, `rank`, `market_cap`, `signals`, `source_dates` and `warnings`. The API never recomputes or reshapes it; it equals `replay()` of the publication.

### The `publication` object

Every field comes from immutable data: the `publications` row, its quality event and its at most one `degraded` event. Fields are whitelisted, so a new key in an event never reaches the API without a v1 change.

| field | type | meaning |
|---|---|---|
| `id` | integer | publication id |
| `created_at` | string | ISO 8601 creation time, UTC |
| `ranking_version` | string | ranking algorithm version, e.g. `watchlist-v3` |
| `demo` | boolean | true only when the quality event marks a demo publication |
| `degraded` | boolean | true when the publication has a `degraded` event: a reused source, Congress history gaps or a market-cap fallback. `stale_sources` can be `[]` while this is true |
| `stale_sources` | array | the reused sources, sorted by `source`: `{source, collected_at, age_seconds}`. `[]` when not degraded |
| `market_caps_unavailable` | integer | records in `watchlist`, across all buckets, whose `warnings` contain `market_cap_unavailable`. This doesn't imply `degraded` |
| `congress_history_gaps` | integer | the degraded event's Congress gap count, else `0` |
| `source_as_of` | object | observation date per watchlist source (`congress_trades`, `ark_holdings`, `insider_trades`, `short_interest`), or `null` |

## `GET /v1/securities/{symbol}`

A security by ticker or alias, with its entry in the current publication.

- **Symbol handling:** `symbol` is trimmed and uppercased, must match `^[A-Z0-9][A-Z0-9./-]{0,19}$`, and resolves through `security_identifiers` (`scheme = 'ticker'`). So `BRK-B`, `brk-b` and `BRK.B` all find `BRK.B`.
- **Inactive securities:** still resolve, with `is_active: false`.
- **`200`:** `{"api_version": "v1", "security": <security>, "current": {"publication": <publication or null>, "entry": <record or null>}}`, with `Cache-Control: no-cache`.
- **`current`:** `publication` is the same object as in `/v1/watchlist`. `entry` is the record in the current output whose `ticker` equals the security's `symbol`, or `null`. Both are `null` when no publication is current.
- **Errors:** `404 unknown_security`, `422 invalid_request`.

### The `security` object

| field | type | meaning |
|---|---|---|
| `id` | integer | security id |
| `symbol` | string | display ticker |
| `name` | string or null | security name |
| `security_type` | string or null | e.g. `common`, `adr`, `etf` |
| `exchange` | string or null | listing exchange |
| `currency` | string or null | trading currency |
| `country` | string or null | ISO country code |
| `is_active` | boolean | false for a retired security |
| `sector` | string or null | from the reviewed company profile |
| `industry` | string or null | from the reviewed company profile |

The profile's `legal_name`, `cik` and other columns are never served.

## Database access

- **Role:** the API connects as a login that inherits only `perennial_api`, provisioned by `development/database/ops/provision_roles.py`. That role can SELECT `publications`, `current_publication`, `securities` and `security_identifiers`, plus the columns `company_profiles(id, sector, industry)` and `publication_events(id, publication_id, kind, checks, created_at)`. That means no operator `actor`/`reason`, no raw source tables or runs, and no writes. The login is Bryan's to create: `LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS IN ROLE perennial_api`, a member of nothing else, with a `CONNECTION LIMIT` sized to the `api` pool.
- **Startup check:** `create_app()` (or `check_startup` in #22's app) refuses to serve unless the login:
  - has no unsafe attributes
  - is a member of exactly `perennial_api`
  - can't write any `perennial` table, and the schema isn't empty
  - owns nothing and has no CREATE on the schema or database

  Every refusal is the same fixed error, `api_login_refused`.
- **Transactions:** each request runs in one read-only transaction (`BEGIN … READ ONLY`), never a session-level `SET`, so pooled backends aren't affected.
- **Supabase:** `perennial` must never be in Supabase's exposed schemas. Provisioning refuses if `anon`, `authenticated` or `service_role` can reach `perennial`.

## Mounting in #22's app

`router` already carries the `/v1` prefix: `app.include_router(read.router)`. The app must set `app.state.engine` (an engine for the `api` workload) and call `read.check_startup(engine, "perennial_api")` before serving. It also needs the error handlers `create_app()` installs, so that errors keep the `{"detail", "code"}` shape.

Serve with uvicorn's access log off (`log_config=None` or `access_log=False`): it prints raw request lines, query strings included, and bypasses the JSON log redaction.
