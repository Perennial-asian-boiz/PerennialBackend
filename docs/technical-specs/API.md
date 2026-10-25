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
| `rate_limited` | 429 | the client is over its rate limit (see "Rate limiting") |

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

## `GET /v1/market-status`

Whether the US stock market (the NYSE calendar, `XNYS`) is in regular hours, and its next open and close (S1-11). It's public, touches no database and takes no input: query parameters and headers don't change the answer. Unlike the other routes, its body has no `api_version`.

Open (a Monday, 10:00 a.m. ET):

```json
{
  "exchange": "XNYS",
  "is_open": true,
  "session": "regular",
  "label": "Market Open",
  "note": "Regular hours, 9:30 a.m.–4:00 p.m. ET. Some brokers allow extended-hours trading.",
  "as_of": "2026-10-26T14:00:00Z",
  "next_close": "2026-10-26T16:00:00-04:00",
  "next_open": "2026-10-27T09:30:00-04:00",
  "early_close": false
}
```

Closed (a Saturday):

```json
{
  "exchange": "XNYS",
  "is_open": false,
  "session": "closed",
  "label": "Market Closed",
  "note": "Regular hours, 9:30 a.m.–4:00 p.m. ET. Some brokers allow extended-hours trading.",
  "as_of": "2026-10-31T16:00:00Z",
  "next_open": "2026-11-02T09:30:00-05:00",
  "next_close": "2026-11-02T16:00:00-05:00",
  "early_close": false
}
```

- **Open** means regular hours only, from the session's open (inclusive) to its close (exclusive). `session` is `regular` or `closed`, and `label` is exactly `Market Open` or `Market Closed`.
- **`next_open` and `next_close` are never null.** While open, `next_close` is today's close and `next_open` is the next session's open. While closed, `next_open` is the next session's open and `next_close` is that same session's close.
- **`early_close`** describes the session `next_close` belongs to. When it's true, `note` reads "Regular hours, 9:30 a.m.–1:00 p.m. ET (early close). Some brokers allow extended-hours trading.", with the closing time from the calendar.
- **Times:** ISO 8601. `next_open` and `next_close` carry the America/New_York offset (`-04:00` or `-05:00`), and `as_of` is UTC with whole seconds. The process time zone has no effect.
- **Holidays and early closes** come from the `exchange_calendars` library, never from a hand-kept list. Its calendars reach about a year ahead, and unscheduled closures arrive only in library releases, so keep its pin current.
- **Caching:** `Cache-Control: public, max-age=N`, where N is the whole seconds until the next open or close, at most 15 and at least 1. A cached answer never spans a transition, so a client polling every 30 seconds sees a change within 45.
- **Errors:** only `429 rate_limited`, from the `read` bucket.

## Rate limiting

Every route above is limited per client, before any other work: a limited request opens no database transaction. Each request costs one token, including `304` responses and `404` and `422` errors.

- **Over the limit:** `429` with `{"detail": "too many requests; retry later", "code": "rate_limited"}`, `Retry-After: <whole seconds until the next token, at least 1>` and `Cache-Control: no-store`. There is no `ETag` and no publication data, and no `RateLimit-*` headers in v1.
- **Buckets:** token buckets per client key, set through the environment. Every limit must be a positive integer; an invalid setting refuses startup with the fixed error `rate_limit_config_invalid`.

| setting | default | meaning |
|---|---|---|
| `API_RATE_LIMIT_BURST` | `30` | read bucket capacity (the routes above) |
| `API_RATE_LIMIT_PER_MINUTE` | `60` | read bucket refill rate |
| `API_AUTH_RATE_LIMIT_BURST` | `5` | auth bucket capacity: #22's signup and forgot-password together |
| `API_AUTH_RATE_LIMIT_PER_HOUR` | `30` | auth bucket refill rate |
| `API_RATE_LIMIT_MAX_CLIENTS` | `10000` | buckets kept per kind; the least recently used is evicted, and an evicted client starts again full |
| `API_TRUSTED_PROXIES` | empty | comma-separated IPs or CIDRs of **every** proxy hop in front of the API |
| `API_TRUST_ALL_PROXIES` | unset | `1` trusts every peer's `X-Forwarded-For` |

- **Client keys:** the peer address, after the proxy handling below. IPv4 is keyed by address, an IPv4-mapped IPv6 address by its IPv4 address, other IPv6 by its /64. A port or brackets from a proxy are stripped. A request with no peer shares one `unknown` bucket, and a non-IP host is keyed by its first 64 characters. Clients behind one NAT share a bucket.
- **Proxies:** `X-Forwarded-For` is honoured only from a listed peer, and the key is its rightmost untrusted entry, so entries a client prepends are ignored. List every hop: with a hop missing, every client is keyed by that hop's address. `*` is refused. With `API_TRUST_ALL_PROXIES=1` the key is the **first** entry, which the client controls, so use it only behind a proxy that overwrites `X-Forwarded-For` with the real peer. The proxy must append the real peer to `X-Forwarded-For`; RFC 7239 `Forwarded` isn't read.
- **Run commands:** the app adds its own proxy handling (`read.install`), so turn uvicorn's off: `uvicorn --no-proxy-headers ...` (`proxy_headers=False` when calling `uvicorn.run`).
- **Workers:** state is in memory per app instance in each worker process. With N workers the effective limit is up to N times higher, and a restart resets it. The edge or host limit, required before any public deployment, is the global one.
- **Logs:** a client's first rejection writes one `rate_limited` line; its further rejections are counted until its `Retry-After` has passed and carried on its next line. At most 60 such lines a minute in all, then one `rate_limited_suppressed` line with the number held back. No line holds an address, path or header value.

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

`router` already carries the `/v1` prefix and the read limit: `app.include_router(read.router)`, then `read.install(app, read.RateLimitConfig.from_environ(os.environ))`, the only place the proxy middleware is added. Without `install`, a limited route answers `503 rate_limiter_missing` (it fails closed). Attach `Depends(read.auth_rate_limit)` to signup and forgot-password, ahead of any other dependency. The app must set `app.state.engine` (an engine for the `api` workload) and call `read.check_startup(engine, "perennial_api")` before serving. It also needs the error handlers `create_app()` installs, so that errors keep the `{"detail", "code"}` shape.

Serve with uvicorn's access log off (`log_config=None` or `access_log=False`): it prints raw request lines, query strings included, and bypasses the JSON log redaction.
