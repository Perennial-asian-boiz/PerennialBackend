"""Public read API (M5 R1): the current watchlist publication and security lookups.

Design: docs/work-items/m5-read-api-design-note.md. Schemas: docs/technical-specs/API.md.

- Published data only, read as a login that inherits only the ``perennial_api``
  group role; ``create_app`` refuses to start as any other kind of login (RA-15).
- Every request runs in exactly one read-only transaction (``read_transaction``),
  marked read-only per transaction, never with a session ``SET`` (RA-16).
- ``watchlist`` is ``publications.output`` verbatim. The envelope copies named
  fields from immutable data only, and bodies are canonical JSON (sorted keys,
  no insignificant whitespace), so a publication's body never changes (RA-6).

- Every route is rate limited per client before any database work
  (``rate_limit``, installed by ``install``; the addendum's RL criteria).

#22's application mounts ``router`` (it already carries ``/v1``), calls
``install``, sets ``app.state.engine`` and runs ``check_startup`` before serving.
"""

import json
import os
from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from datetime import timezone
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field
from sqlalchemy import select, text
from sqlalchemy.engine import Connection, Engine
from starlette.exceptions import HTTPException as StarletteHTTPException
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from src.api.rate_limit import (
    Kind,
    RateLimitConfig,
    RateLimitConfigError,
    RateLimiter,
    client_key,
)
from src.db import models as m
from src.ingestion.schemas import _TICKER

API_VERSION = "v1"
CHANNEL = "production"
QUALITY_KINDS = ("quality_passed", "quality_override")
BUCKETS = ("popular_stable", "affordable_growing", "unresolved")
UNAVAILABLE_WARNING = "market_cap_unavailable"


# ── responses ────────────────────────────────────────────


def canonical_json(content: Any) -> bytes:
    """UTF-8 JSON, sorted keys, no insignificant whitespace (design rule 4)."""
    return json.dumps(
        content, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


class CanonicalJSONResponse(Response):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return canonical_json(content)


class APIError(Exception):
    """A fixed-code client error; the detail never echoes request input."""

    def __init__(self, status: int, code: str, detail: str):
        super().__init__(detail)
        self.status, self.code, self.detail = status, code, detail


def _error(status: int, code: str, detail: str) -> CanonicalJSONResponse:
    return CanonicalJSONResponse({"detail": detail, "code": code}, status_code=status)


# ── schema (documentation and the OpenAPI snapshot) ──────


class StaleSource(BaseModel):
    source: str
    collected_at: str | None = Field(description="When the reused batch was collected")
    age_seconds: int | None = Field(
        description="Its age when this publication was made"
    )


class Publication(BaseModel):
    id: int
    created_at: str = Field(description="ISO 8601, UTC")
    ranking_version: str
    demo: bool = Field(description="True only for a demo publication")
    degraded: bool = Field(
        description="True when the publication has a degraded event: a reused source,"
        " Congress history gaps or a market-cap fallback; stale_sources may be empty"
    )
    stale_sources: list[StaleSource] = Field(description="Reused sources, by source")
    market_caps_unavailable: int = Field(
        description="Records warning market_cap_unavailable, across all buckets"
    )
    congress_history_gaps: int
    source_as_of: dict[str, str | None] = Field(
        description="Observation date per watchlist source"
    )


class WatchlistResponse(BaseModel):
    api_version: str = Field(examples=[API_VERSION])
    publication: Publication
    watchlist: dict[str, Any] = Field(description="publications.output, verbatim")


class Security(BaseModel):
    id: int
    symbol: str
    name: str | None
    security_type: str | None
    exchange: str | None
    currency: str | None
    country: str | None
    is_active: bool
    sector: str | None
    industry: str | None


class SecurityCurrent(BaseModel):
    publication: Publication | None
    entry: dict[str, Any] | None = Field(
        description="The security's record in the current publication, or null"
    )


class SecurityResponse(BaseModel):
    api_version: str = Field(examples=[API_VERSION])
    security: Security
    current: SecurityCurrent


class ErrorBody(BaseModel):
    detail: str
    code: str = Field(
        description="no_current_publication, unknown_security, invalid_request"
        " or rate_limited"
    )


# ── data access ──────────────────────────────────────────


@contextmanager
def read_transaction(engine: Engine) -> Iterator[Connection]:
    """One read-only transaction (BEGIN ... READ ONLY), never a session SET.

    The characteristic is reset when the connection returns to the pool, so a
    pooled backend is never left read-only for its next user (RA-16). Routes
    look this up as a module global at request time.
    """
    with engine.connect() as conn:
        conn.execution_options(postgresql_readonly=True)
        with conn.begin():
            yield conn


def _current_publication_id(conn: Connection) -> int | None:
    value = conn.execute(
        select(m.current_publication.c.publication_id).where(
            m.current_publication.c.channel == CHANNEL
        )
    ).scalar_one_or_none()
    return int(value) if value is not None else None


def _records(output: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
    for bucket in BUCKETS:
        records = output.get(bucket)
        if isinstance(records, list):
            yield from (r for r in records if isinstance(r, Mapping))


def _stale_sources(degraded: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    """Whitelisted fields of the degraded event's reused map, sorted by source."""
    reused = (degraded or {}).get("reused")
    if not isinstance(reused, Mapping):
        return []
    return [
        {
            "source": source,
            "collected_at": entry.get("collected_at"),
            "age_seconds": entry.get("age_seconds"),
        }
        for source, entry in sorted(reused.items())
        if isinstance(entry, Mapping)
    ]


def _envelope(conn: Connection, publication_id: int) -> tuple[dict[str, Any], Any]:
    """(publication envelope, output) from immutable data only (rule 2)."""
    pub = conn.execute(
        select(
            m.publications.c.id,
            m.publications.c.created_at,
            m.publications.c.ranking_version,
            m.publications.c.ranking_config,
            m.publications.c.output,
        ).where(m.publications.c.id == publication_id)
    ).one()
    # Explicit columns: perennial_api may not read publication_events.actor/reason.
    events = conn.execute(
        select(m.publication_events.c.kind, m.publication_events.c.checks).where(
            m.publication_events.c.publication_id == publication_id,
            m.publication_events.c.kind.in_((*QUALITY_KINDS, "degraded")),
        )
    ).all()
    quality = next((e.checks for e in events if e.kind in QUALITY_KINDS), None) or {}
    degraded = next((e.checks for e in events if e.kind == "degraded"), None)
    gaps = (degraded or {}).get("congress_gaps")
    gap_count = gaps.get("count") if isinstance(gaps, Mapping) else 0
    output = pub.output
    as_of = (pub.ranking_config or {}).get("source_as_of") or {}
    meta = {
        "id": int(pub.id),
        # MR1-1: UTC, so the body never depends on the session time zone.
        "created_at": pub.created_at.astimezone(timezone.utc).isoformat(),
        "ranking_version": pub.ranking_version,
        "demo": quality.get("demo") is True,
        "degraded": degraded is not None,
        "stale_sources": _stale_sources(degraded),
        # From the immutable output, so it is exact on every collection path.
        "market_caps_unavailable": sum(
            UNAVAILABLE_WARNING in (r.get("warnings") or []) for r in _records(output)
        ),
        "congress_history_gaps": gap_count if isinstance(gap_count, int) else 0,
        "source_as_of": {s: as_of.get(s) for s in m.WATCHLIST_SOURCES},
    }
    return meta, output


def _security(conn: Connection, symbol: str) -> Any:
    """Resolve a ticker through security_identifiers (BRK-B finds BRK.B)."""
    s, ids, p = m.securities, m.security_identifiers, m.company_profiles
    return conn.execute(
        select(
            s.c.id,
            s.c.symbol,
            s.c.name,
            s.c.security_type,
            s.c.exchange,
            s.c.currency,
            s.c.country,
            s.c.is_active,
            p.c.sector,
            p.c.industry,
        )
        .select_from(
            ids.join(s, s.c.id == ids.c.security_id).outerjoin(
                p, p.c.id == s.c.company_profile_id
            )
        )
        .where(ids.c.scheme == "ticker", ids.c.value == symbol)
    ).one_or_none()


# ── rate limiting (addendum: RL-*) ───────────────────────


class RateLimited(StarletteHTTPException):
    """Over the limit: 429 with Retry-After, rendered by ``install``'s handler."""

    code = "rate_limited"

    def __init__(self, retry_after: int):
        super().__init__(
            429,
            "too many requests; retry later",
            headers={"Retry-After": str(retry_after), "Cache-Control": "no-store"},
        )


class RateLimiterMissing(StarletteHTTPException):
    """A limited route on an app where ``install`` never ran: fail closed (RL-14)."""

    code = "rate_limiter_missing"

    def __init__(self) -> None:
        super().__init__(503, f"{self.code}: the rate limiter is not installed")


def _limit(request: Request, kind: Kind) -> None:
    limiter = getattr(request.app.state, "rate_limiter", None)
    if not isinstance(limiter, RateLimiter):
        raise RateLimiterMissing()
    client = request.client
    retry_after = limiter.check(kind, client_key(client.host if client else None))
    if retry_after is not None:
        raise RateLimited(retry_after)


async def rate_limit(request: Request) -> None:
    """The read bucket, on every ``router`` route before anything else runs."""
    _limit(request, "read")


async def auth_rate_limit(request: Request) -> None:
    """The auth bucket, for #22's signup and forgot-password (email lookups)."""
    _limit(request, "auth")


def install(app: FastAPI, config: RateLimitConfig) -> None:
    """Rate limiting for an app: the only place ProxyHeadersMiddleware is added.

    The proxy middleware rewrites request.client from X-Forwarded-For for
    trusted peers only; run uvicorn with --no-proxy-headers (proxy_headers=False)
    so this is the one trust decision. Limiter state is per app instance.
    """
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=config.trusted_hosts())
    app.state.rate_limiter = RateLimiter(config)

    @app.exception_handler(RateLimited)
    async def rate_limited(request: Request, exc: RateLimited) -> Response:
        response = _error(429, exc.code, exc.detail)
        response.headers.update(exc.headers or {})
        return response


RATE_LIMITED_429: dict[str, Any] = {
    "model": ErrorBody,
    "description": "rate_limited: too many requests from this client",
    "headers": {
        "Retry-After": {
            "description": "Whole seconds until the next request is allowed",
            "schema": {"type": "integer"},
        }
    },
}


# ── routes ───────────────────────────────────────────────


def _engine(request: Request) -> Engine:
    engine: Engine = request.app.state.engine
    return engine


def _etag_matches(header: str | None, etag: str) -> bool:
    if not header:
        return False
    tags = [t.strip() for t in header.split(",")]
    return "*" in tags or etag in tags or f"W/{etag}" in tags


ERRORS: dict[int | str, dict[str, Any]] = {
    404: {
        "model": ErrorBody,
        "description": "No current publication, or unknown security",
    },
    422: {"model": ErrorBody, "description": "invalid_request"},
}
WATCHLIST_404 = {"model": ErrorBody, "description": "No current publication"}

# The limit runs before every other dependency, validation and database use.
router = APIRouter(
    prefix="/v1",
    dependencies=[Depends(rate_limit)],
    responses={429: RATE_LIMITED_429},
)


@router.get(
    "/watchlist",
    response_model=WatchlistResponse,
    responses={
        304: {"description": "Not modified (If-None-Match matched)"},
        404: WATCHLIST_404,
    },
)
def get_watchlist(request: Request) -> Response:
    """The current production publication's watchlist and its envelope."""
    with read_transaction(_engine(request)) as conn:
        publication_id = _current_publication_id(conn)
        if publication_id is None:
            raise APIError(404, "no_current_publication", "no publication is current")
        etag = f'"v1-{publication_id}"'
        headers = {"ETag": etag, "Cache-Control": "no-cache"}
        if _etag_matches(request.headers.get("if-none-match"), etag):
            return Response(status_code=304, headers=headers)
        meta, output = _envelope(conn, publication_id)
    body = {"api_version": API_VERSION, "publication": meta, "watchlist": output}
    return CanonicalJSONResponse(body, headers=headers)


@router.get("/securities/{symbol}", response_model=SecurityResponse, responses=ERRORS)
def get_security(symbol: str, request: Request) -> Response:
    """A security by ticker or alias, with its entry in the current publication."""
    wanted = symbol.strip().upper()
    if not _TICKER.fullmatch(wanted):
        raise APIError(422, "invalid_request", "symbol is not a valid ticker")
    with read_transaction(_engine(request)) as conn:
        found = _security(conn, wanted)
        if found is None:
            raise APIError(404, "unknown_security", "no security has that ticker")
        current: dict[str, Any] = {"publication": None, "entry": None}
        publication_id = _current_publication_id(conn)
        if publication_id is not None:
            meta, output = _envelope(conn, publication_id)
            entry = next(
                (r for r in _records(output) if r.get("ticker") == found.symbol), None
            )
            current = {"publication": meta, "entry": entry}
    security = {
        "id": int(found.id),
        "symbol": found.symbol,
        "name": found.name,
        "security_type": found.security_type,
        "exchange": found.exchange,
        "currency": found.currency,
        "country": found.country,
        "is_active": bool(found.is_active),
        "sector": found.sector,
        "industry": found.industry,
    }
    body = {"api_version": API_VERSION, "security": security, "current": current}
    return CanonicalJSONResponse(body, headers={"Cache-Control": "no-cache"})


# ── startup refusal (RA-15) ──────────────────────────────


class StartupRefused(RuntimeError):
    """The database login is not a correctly shaped API login. Fixed text only."""

    code = "api_login_refused"

    def __init__(self) -> None:
        super().__init__("api_login_refused: the API login failed its startup check")


_ATTRIBUTES = text(
    "SELECT session_user = current_user, rolsuper OR rolcreatedb OR rolcreaterole"
    " OR rolreplication OR rolbypassrls FROM pg_roles WHERE rolname = session_user"
)
_MEMBERSHIP = text(
    "SELECT coalesce(array_agg(r.rolname ORDER BY r.rolname), '{}') FROM pg_roles r"
    " WHERE r.rolname <> session_user AND pg_has_role(session_user, r.oid, 'MEMBER')"
)
_WRITES = text(
    "SELECT count(*), count(*) FILTER (WHERE has_table_privilege("
    "current_user, c.oid, 'INSERT,UPDATE,DELETE,TRUNCATE')) FROM pg_class c"
    " JOIN pg_namespace n ON n.oid = c.relnamespace"
    " WHERE n.nspname = 'perennial' AND c.relkind IN ('r', 'p')"
)
# #22's schema app (fix packet, change 5): reads and writes only on its six
# tables, never TRUNCATE, no CREATE on app, and no USAGE on Supabase's auth
# schema (the login reaches auth.users only through app.email_is_registered).
APP_TABLES = (
    "profiles",
    "preferences",
    "notifications",
    "watchlist",
    "auth_challenges",
    "verified_sessions",
)
_APP = text(
    "SELECT (SELECT count(*) FROM pg_class c"
    "  JOIN pg_namespace n ON n.oid = c.relnamespace"
    "  WHERE n.nspname = 'app' AND c.relkind IN ('r', 'p') AND ("
    "   has_table_privilege(current_user, c.oid, 'TRUNCATE,REFERENCES,TRIGGER')"
    "   OR (c.relname <> ALL(CAST(:tables AS text[])) AND has_table_privilege("
    "    current_user, c.oid, 'INSERT,UPDATE,DELETE')))),"
    " coalesce((SELECT has_schema_privilege(current_user, oid, 'CREATE')"
    "  FROM pg_namespace WHERE nspname = 'app'), false),"
    " coalesce((SELECT has_schema_privilege(current_user, oid, 'USAGE')"
    "  FROM pg_namespace WHERE nspname = 'auth'), false)"
)
_OWNERSHIP = text(
    "SELECT (SELECT count(*) FROM pg_class WHERE relowner = r.oid)"
    " + (SELECT count(*) FROM pg_namespace WHERE nspowner = r.oid),"
    " has_schema_privilege(current_user, 'perennial', 'CREATE'),"
    " has_database_privilege(current_user, current_database(), 'CREATE')"
    " FROM pg_roles r WHERE r.rolname = session_user"
)


def check_startup(engine: Engine, api_role: str) -> None:
    """Fail closed unless the login inherits only ``api_role`` and can't write.

    Clauses (design note RA-15): 1 attributes of the session role itself;
    2 transitive membership is exactly {api_role}; 3 no write privilege on any
    perennial table, and at least one table exists; 4 owns nothing and has no
    CREATE on the schema or database. Where #22's schema app exists, clause 3
    also allows writes only on its six tables and clause 4 refuses CREATE on
    app; USAGE on schema auth (Supabase) is refused. Every failure, including an
    error while checking, raises the same StartupRefused with no role name, URL
    or SQL.
    """
    try:
        with engine.connect() as conn:
            same, unsafe = conn.execute(_ATTRIBUTES).one()
            members = list(conn.execute(_MEMBERSHIP).scalar_one())
            tables, writable = conn.execute(_WRITES).one()
            owned, schema_create, database_create = conn.execute(_OWNERSHIP).one()
            app_writes, app_create, auth_usage = conn.execute(
                _APP, {"tables": list(APP_TABLES)}
            ).one()
            conn.rollback()
        ok = (
            same is True
            and unsafe is False
            and members == [api_role]
            and tables > 0
            and writable == 0
            and owned == 0
            and not schema_create
            and not database_create
            and app_writes == 0
            and not app_create
            and not auth_usage
        )
    except Exception:
        ok = False
    if not ok:
        raise StartupRefused() from None


# ── application ──────────────────────────────────────────


def _install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(APIError)
    async def api_error(request: Request, exc: APIError) -> Response:
        return _error(exc.status, exc.code, exc.detail)

    @app.exception_handler(RequestValidationError)
    async def validation_error(
        request: Request, exc: RequestValidationError
    ) -> Response:
        return _error(422, "invalid_request", "the request is not valid")

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, exc: StarletteHTTPException) -> Response:
        if isinstance(exc, RateLimiterMissing):
            return _error(exc.status_code, exc.code, "request failed")
        code = {404: "not_found", 405: "method_not_allowed"}.get(
            exc.status_code, "error"
        )
        return _error(exc.status_code, code, "request failed")


def create_app(
    engine: Engine | None = None,
    *,
    api_role: str = "perennial_api",
    rate_limit: RateLimitConfig | None = None,
) -> FastAPI:
    """The R1 test and local app: ``router``, error handlers and the startup check.

    With no engine, one is made for the ``api`` workload from DATABASE_URL.
    ``api_role`` is the group role the login must inherit, and nothing else.
    With no ``rate_limit``, the limits come from the API_* environment
    variables; invalid settings raise RateLimitConfigError (fixed text).
    """
    if rate_limit is None:
        rate_limit = RateLimitConfig.from_environ(os.environ)
    if engine is None:
        from src.db.session import make_engine

        engine = make_engine(workload="api")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        check_startup(app.state.engine, app.state.api_role)
        yield

    app = FastAPI(title="Perennial read API", version=API_VERSION, lifespan=lifespan)
    app.state.engine = engine
    app.state.api_role = api_role
    app.include_router(router)
    _install_error_handlers(app)
    install(app, rate_limit)
    return app


__all__ = [
    "RateLimitConfig",
    "RateLimitConfigError",
    "auth_rate_limit",
    "check_startup",
    "create_app",
    "install",
    "rate_limit",
    "read_transaction",
    "router",
]
