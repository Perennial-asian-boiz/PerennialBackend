"""M5 R1: the public read API (GET /v1/watchlist, GET /v1/securities/{symbol}).

Spec: docs/work-items/m5-read-api-design-note.md (RA-1 to RA-18; RA-8a-8c are
left out while Bryan's question E is open). Synthetic publications only.

The API always runs as a correctly shaped login that inherits only the API
group role, never as the superuser test login. Roles belong to the whole
server, so every role here has a unique suffix and is dropped in `finally`.
src.api and fastapi are imported inside each test so the rest of the suite
still collects before R1 exists.
"""

import json
import logging
import re
import secrets
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import event, insert, select, text
from sqlalchemy.exc import DBAPIError
from src.db import models

OPS = Path(__file__).resolve().parents[2] / "database" / "ops"
REPO = Path(__file__).resolve().parents[3]
SNAPSHOT = Path(__file__).parent / "fixtures" / "api" / "openapi_v1.json"

WATCHLIST = models.WATCHLIST_SOURCES
FULL_TABLES = ("publications", "current_publication", "securities", "security_identifiers")
COLUMNS = {
    "company_profiles": ("id", "sector", "industry"),
    "publication_events": ("id", "publication_id", "kind", "checks", "created_at"),
}
PUBLICATION_KEYS = {
    "id",
    "created_at",
    "ranking_version",
    "demo",
    "degraded",
    "stale_sources",
    "market_caps_unavailable",
    "congress_history_gaps",
    "source_as_of",
}
SECURITY_KEYS = {
    "id",
    "symbol",
    "name",
    "security_type",
    "exchange",
    "currency",
    "country",
    "is_active",
    "sector",
    "industry",
}
WRITES = ("INSERT", "UPDATE", "DELETE", "TRUNCATE")


# ── roles ────────────────────────────────────────────────


def _provision(engine, roles, **kwargs):
    if str(OPS) not in sys.path:
        sys.path.insert(0, str(OPS))
    from provision_roles import provision

    with engine.begin() as conn:
        raw = conn.connection.driver_connection
        provision(raw, roles, phase="bootstrap", **kwargs)
        provision(raw, roles, phase="grants", **kwargs)


def _drop_roles(engine, names):
    with engine.begin() as conn:
        admin = conn.execute(text("SELECT current_user")).scalar()
        for name in names:
            if conn.execute(text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": name}).scalar():
                conn.execute(text(f'REASSIGN OWNED BY "{name}" TO "{admin}"'))
                conn.execute(text(f'DROP OWNED BY "{name}" CASCADE'))
                conn.execute(text(f'DROP ROLE "{name}"'))


@pytest.fixture
def groups(engine):
    """Provisioned owner, importer, reader and API group roles (unique names)."""
    suffix = secrets.token_hex(5)
    names = {r: f"ra_{r}_{suffix}" for r in ("owner", "importer", "reader", "api")}
    try:
        _provision(engine, (names["owner"], names["importer"], names["reader"], names["api"]))
        yield names
    finally:
        _drop_roles(engine, list(names.values()))


@contextmanager
def login(engine, groups, *, options="", members=("api",), database=None, connect=False):
    """A login role in the given groups; yields (pinned engine, role name, password).

    `connect` grants CONNECT directly: provisioning gives it only to the group
    roles, which a NOINHERIT login does not inherit.
    """
    from tests.conftest import pinned_engine

    name = f"ra_login_{secrets.token_hex(5)}"
    password = f"synthetic-{secrets.token_hex(8)}"  # hex only, so safe to inline in DDL
    in_role = ", ".join(f'"{groups[m]}"' for m in members if m in groups)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"CREATE ROLE \"{name}\" LOGIN {options} PASSWORD '{password}'"
                + (f" IN ROLE {in_role}" if in_role else "")
            )
        )
        if connect:
            db = database or engine.url.database
            conn.execute(text(f'GRANT CONNECT ON DATABASE "{db}" TO "{name}"'))
    url = engine.url.set(username=name, password=password)
    if database is not None:
        url = url.set(database=database)
    api_engine = pinned_engine(url, workload="api")
    try:
        yield api_engine, name, password
    finally:
        api_engine.dispose()
        _drop_roles(engine, [name])


@contextmanager
def client(api_engine, groups):
    from fastapi.testclient import TestClient
    from src.api.read import create_app

    with TestClient(create_app(api_engine, api_role=groups["api"])) as c:
        yield c


@pytest.fixture
def api(engine, groups):
    """A test client served by a correctly shaped API login."""
    with login(engine, groups) as (api_engine, _, _), client(api_engine, groups) as c:
        yield c


# ── publications ─────────────────────────────────────────


def feed(fixture_path):
    from test_degraded_publishing import Feed

    return Feed(fixture_path)


def publish_run(engine, f, **kwargs):
    from test_degraded_publishing import run

    return run(engine, f, **kwargs)


def row(engine, publication_id):
    with engine.connect() as conn:
        return conn.execute(
            select(models.publications).where(models.publications.c.id == publication_id)
        ).one()


def event_checks(engine, publication_id, kind):
    with engine.connect() as conn:
        return conn.execute(
            select(models.publication_events.c.checks).where(
                models.publication_events.c.publication_id == publication_id,
                models.publication_events.c.kind == kind,
            )
        ).scalar_one_or_none()


def records(output):
    return [
        r
        for bucket in ("popular_stable", "affordable_growing", "unresolved")
        for r in output.get(bucket, [])
    ]


def unavailable(output):
    return sum("market_cap_unavailable" in r.get("warnings", []) for r in records(output))


def canonical(body):
    """UTF-8 JSON, sorted keys, no insignificant whitespace."""
    return json.dumps(
        json.loads(body), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def assert_error(response, status, code, *echo):
    assert response.status_code == status
    body = response.json()
    assert set(body) == {"detail", "code"} and body["code"] == code
    for value in echo:
        assert value not in response.text


# ── RA-1 to RA-7, RA-9: the watchlist envelope ───────────


@pytest.mark.db
def test_no_current_publication_is_404(api):
    assert_error(api.get("/v1/watchlist"), 404, "no_current_publication")


@pytest.mark.db
def test_watchlist_envelope_is_the_current_publication_verbatim(engine, fixture_path, api):
    from src.pipeline.publication import replay

    pub = publish_run(engine, feed(fixture_path))
    response = api.get("/v1/watchlist")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert response.content == canonical(response.content)
    body = response.json()
    assert set(body) == {"api_version", "publication", "watchlist"}
    assert body["api_version"] == "v1"
    stored = row(engine, pub)
    meta = body["publication"]
    assert set(meta) == PUBLICATION_KEYS
    assert meta["id"] == pub
    assert meta["ranking_version"] == stored.ranking_version
    assert datetime.fromisoformat(meta["created_at"]) == stored.created_at
    assert body["watchlist"] == json.loads(json.dumps(stored.output))
    assert body["watchlist"] == json.loads(json.dumps(replay(engine, pub)))
    # RA-4b: not degraded, every cap present.
    assert meta["degraded"] is False and meta["stale_sources"] == []
    assert meta["market_caps_unavailable"] == 0 and meta["congress_history_gaps"] == 0
    assert meta["demo"] is False
    # RA-5
    as_of = stored.ranking_config["source_as_of"]
    assert meta["source_as_of"] == {s: as_of.get(s) for s in WATCHLIST}


@pytest.mark.db
def test_etag_and_conditional_requests(engine, fixture_path, api):
    pub = publish_run(engine, feed(fixture_path))
    first = api.get("/v1/watchlist")
    assert first.headers["etag"] == f'"v1-{pub}"'
    assert first.headers["cache-control"] == "no-cache"
    same = api.get("/v1/watchlist", headers={"If-None-Match": f'"v1-{pub}"'})
    assert same.status_code == 304 and same.content == b""
    other = api.get("/v1/watchlist", headers={"If-None-Match": f'"v1-{pub + 1}"'})
    assert other.status_code == 200 and other.content == first.content


@pytest.mark.db
def test_degraded_publication_lists_stale_sources_from_its_event(engine, fixture_path, api):
    f = feed(fixture_path)
    publish_run(engine, f)
    f.fail = {"insider_trades", "short_interest"}
    pub = publish_run(engine, f)
    checks = event_checks(engine, pub, "degraded")
    assert checks is not None
    meta = api.get("/v1/watchlist").json()["publication"]
    assert meta["id"] == pub and meta["degraded"] is True
    assert meta["stale_sources"] == [
        {
            "source": source,
            "collected_at": checks["reused"][source]["collected_at"],
            "age_seconds": checks["reused"][source]["age_seconds"],
        }
        for source in sorted(checks["reused"])
    ]
    assert [s["source"] for s in meta["stale_sources"]] == ["insider_trades", "short_interest"]
    assert meta["congress_history_gaps"] == checks.get("congress_gaps", {}).get("count", 0)
    assert meta["market_caps_unavailable"] == unavailable(row(engine, pub).output)


@pytest.mark.db
def test_event_keys_outside_the_whitelist_never_reach_the_response(engine, fixture_path, api):
    f = feed(fixture_path)
    publish_run(engine, f)
    f.fail = {"insider_trades"}
    pub = publish_run(engine, f)
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE perennial.publication_events SET checks = jsonb_set(jsonb_set("
                " checks || '{\"synthetic_extra\": \"leak-marker-1\"}',"
                " '{reused,insider_trades,synthetic_note}', '\"leak-marker-2\"'),"
                " '{policy,synthetic}', '\"leak-marker-3\"')"
                " WHERE publication_id = :p AND kind IN ('degraded', 'quality_passed')"
            ),
            {"p": pub},
        )
    response = api.get("/v1/watchlist")
    assert response.status_code == 200
    for marker in ("synthetic_extra", "synthetic_note", "leak-marker"):
        assert marker not in response.text
    assert set(response.json()["publication"]["stale_sources"][0]) == {
        "source",
        "collected_at",
        "age_seconds",
    }


@pytest.mark.db
def test_missing_caps_on_a_normal_collection_are_counted_without_degrading(
    engine, fixture_path, api
):
    from test_degraded_publishing import market_caps

    def some_missing(symbols):
        caps = market_caps(symbols)
        for symbol in sorted(symbols)[:2]:
            caps[symbol]["value"] = None  # the collector's explicit None
        return caps

    pub = publish_run(engine, feed(fixture_path), market_cap_collector=some_missing)
    assert event_checks(engine, pub, "degraded") is None
    expected = unavailable(row(engine, pub).output)
    assert expected > 0
    meta = api.get("/v1/watchlist").json()["publication"]
    assert meta["degraded"] is False and meta["stale_sources"] == []
    assert meta["market_caps_unavailable"] == expected


@pytest.mark.db
def test_body_is_byte_stable_while_the_publication_is_current(engine, fixture_path, api):
    from src.pipeline.publication import rollback

    f = feed(fixture_path)
    older = publish_run(engine, f)
    pub = publish_run(engine, f)
    first = api.get("/v1/watchlist").content
    assert api.get("/v1/watchlist").content == first

    def broken_caps(symbols):
        raise RuntimeError("synthetic collector bug")

    with pytest.raises(Exception):
        publish_run(engine, f, market_cap_collector=broken_caps)
    assert api.get("/v1/watchlist").content == first  # the pointer did not move

    rollback(engine, older, actor="synthetic-operator", reason="synthetic rollback check")
    assert api.get("/v1/watchlist").json()["publication"]["id"] == older
    rollback(engine, pub, actor="synthetic-operator", reason="synthetic rollback check")
    assert api.get("/v1/watchlist").content == first


@pytest.mark.db
def test_rollback_serves_the_target_publication(engine, fixture_path, api):
    from src.pipeline.publication import rollback

    f = feed(fixture_path)
    older = publish_run(engine, f)
    publish_run(engine, f)
    rollback(engine, older, actor="synthetic-operator", reason="synthetic rollback check")
    response = api.get("/v1/watchlist")
    assert response.json()["publication"]["id"] == older
    assert response.headers["etag"] == f'"v1-{older}"'


@pytest.mark.db
def test_demo_marker_comes_from_the_quality_event(engine, fixture_path, api):
    pub = publish_run(engine, feed(fixture_path))
    assert api.get("/v1/watchlist").json()["publication"]["demo"] is False
    with engine.begin() as conn:  # what publish(demo=True) records (publication.py:742)
        conn.execute(
            text(
                "UPDATE perennial.publication_events SET checks = checks || '{\"demo\": true}'"
                " WHERE publication_id = :p AND kind IN ('quality_passed', 'quality_override')"
            ),
            {"p": pub},
        )
    assert api.get("/v1/watchlist").json()["publication"]["demo"] is True


# ── RA-10, RA-11: securities ─────────────────────────────


def seed_security(engine, symbol, *aliases, active=True, profile=None, **fields):
    with engine.begin() as conn:
        profile_id = None
        if profile is not None:
            profile_id = conn.execute(
                insert(models.company_profiles)
                .values(**profile)
                .returning(models.company_profiles.c.id)
            ).scalar_one()
        sid = conn.execute(
            insert(models.securities)
            .values(symbol=symbol, is_active=active, company_profile_id=profile_id, **fields)
            .returning(models.securities.c.id)
        ).scalar_one()
        for value in (symbol, *aliases):
            conn.execute(
                insert(models.security_identifiers).values(
                    scheme="ticker", value=value, security_id=sid
                )
            )
        return sid


@pytest.mark.db
def test_security_lookup_resolves_aliases_and_returns_the_reviewed_fields(engine, api):
    sid = seed_security(
        engine,
        "BRK.B",
        "BRK-B",
        name="Synthetic Holdings Class B",
        security_type="common",
        exchange="NYSE",
        currency="USD",
        country="US",
        profile={
            "legal_name": "Synthetic Holdings Inc",
            "sector": "Financial Services",
            "industry": "Insurance - Diversified",
            "cik": "0000000001",
        },
    )
    for requested in ("BRK-B", "brk-b", "BRK.B", " BRK.B "):
        response = api.get(f"/v1/securities/{requested}")
        assert response.status_code == 200, requested
        assert response.headers["cache-control"] == "no-cache"
        assert response.content == canonical(response.content)
        body = response.json()
        assert set(body) == {"api_version", "security", "current"}
        assert body["api_version"] == "v1"
        assert body["security"] == {
            "id": sid,
            "symbol": "BRK.B",
            "name": "Synthetic Holdings Class B",
            "security_type": "common",
            "exchange": "NYSE",
            "currency": "USD",
            "country": "US",
            "is_active": True,
            "sector": "Financial Services",
            "industry": "Insurance - Diversified",
        }
    assert "Synthetic Holdings Inc" not in api.get("/v1/securities/BRK-B").text  # legal_name


@pytest.mark.db
def test_unknown_inactive_and_invalid_symbols(engine, api):
    seed_security(engine, "ZZOLD", active=False)
    inactive = api.get("/v1/securities/ZZOLD")
    assert inactive.status_code == 200
    assert inactive.json()["security"]["is_active"] is False
    assert set(inactive.json()["security"]) == SECURITY_KEYS
    assert_error(api.get("/v1/securities/ZZNONE"), 404, "unknown_security")
    for bad in ("BRK%20B", "A" * 21, "BR$K", "BRK.B%3BDROP", "-BRK", "BRK%2BB"):
        response = api.get(f"/v1/securities/{bad}")
        assert_error(response, 422, "invalid_request", "BRK B", "A" * 21, "BR$K", "DROP")


@pytest.mark.db
def test_security_entry_is_its_record_in_the_current_publication(engine, fixture_path, api):
    from decimal import Decimal

    from test_degraded_publishing import market_caps

    def spread(symbols):
        """One missing cap (unresolved), one under the threshold, the rest above."""
        caps = market_caps(symbols)
        first, second = sorted(symbols)[:2]
        caps[first]["value"] = None
        caps[second]["value"] = Decimal("5000000000")
        return caps

    pub = publish_run(engine, feed(fixture_path), market_cap_collector=spread)
    output = row(engine, pub).output
    envelope = api.get("/v1/watchlist").json()["publication"]
    buckets = {b for b in ("popular_stable", "affordable_growing", "unresolved") if output.get(b)}
    assert len(buckets) > 1  # entries are found in every bucket, not just the first
    for record in records(output):
        body = api.get(f"/v1/securities/{record['ticker']}").json()
        assert body["current"] == {"publication": envelope, "entry": record}, record["ticker"]
    seed_security(engine, "ZZOUT")
    outside = api.get("/v1/securities/ZZOUT").json()["current"]
    assert outside == {"publication": envelope, "entry": None}


# ── RA-12 to RA-14c: the API role and its grants ─────────


@contextmanager
def as_role(engine, role):
    with engine.connect() as conn:
        conn.execute(text(f'SET ROLE "{role}"'))
        try:
            yield conn
        finally:
            conn.rollback()
            conn.execute(text("RESET ROLE"))


def _denied(conn, statement):
    with pytest.raises(DBAPIError) as caught:
        with conn.begin_nested():
            conn.execute(text(statement))
    assert caught.value.orig.sqlstate == "42501", statement


def _readable(engine, role):
    """Every (table, column) in perennial and public.alembic_version the role can SELECT."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT c.table_schema, c.table_name, c.column_name FROM information_schema.columns c"
                " JOIN pg_tables t ON t.schemaname = c.table_schema AND t.tablename = c.table_name"
                " WHERE (c.table_schema = 'perennial')"
                " OR (c.table_schema = 'public' AND c.table_name = 'alembic_version')"
                " ORDER BY 1, 2, 3"
            )
        ).all()
        return {
            (f"{s}.{t}", c)
            for s, t, c in rows
            if conn.execute(
                text("SELECT has_column_privilege(:r, :t, :c, 'SELECT')"),
                {"r": role, "t": f"{s}.{t}", "c": c},
            ).scalar()
        }


def _expected_readable(engine):
    with engine.connect() as conn:
        columns = conn.execute(
            text(
                "SELECT table_name, column_name FROM information_schema.columns"
                " WHERE table_schema = 'perennial'"
            )
        ).all()
    return {
        (f"perennial.{t}", c)
        for t, c in columns
        if t in FULL_TABLES or c in COLUMNS.get(t, ())
    }


@pytest.mark.db
def test_api_role_reads_exactly_the_allowed_tables_and_columns(engine, groups):
    api_role = groups["api"]
    assert _readable(engine, api_role) == _expected_readable(engine)
    with as_role(engine, api_role) as conn:
        for table in FULL_TABLES:
            conn.execute(text(f"SELECT * FROM perennial.{table} LIMIT 0"))
        for table, cols in COLUMNS.items():
            conn.execute(text(f"SELECT {', '.join(cols)} FROM perennial.{table} LIMIT 0"))
        for statement in (
            "SELECT actor FROM perennial.publication_events",
            "SELECT reason FROM perennial.publication_events",
            "SELECT * FROM perennial.publication_events",
            "SELECT legal_name FROM perennial.company_profiles",
            "SELECT cik FROM perennial.company_profiles",
            "SELECT * FROM perennial.company_profiles",
        ):
            _denied(conn, statement)


@pytest.mark.db
def test_api_role_is_refused_everything_else(engine, groups):
    sys.path.insert(0, str(OPS))
    from provision_roles import TABLES

    api_role = groups["api"]
    refused = sorted(set(TABLES) - set(FULL_TABLES) - set(COLUMNS))
    assert refused  # computed: a new migration's table is covered automatically
    with engine.connect() as conn:
        perennial_tables = conn.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname = 'perennial'")
        ).scalars().all()
    with as_role(engine, api_role) as conn:
        for table in refused:
            _denied(conn, f"SELECT 1 FROM perennial.{table} LIMIT 1")
        _denied(conn, "SELECT 1 FROM public.alembic_version")
        _denied(conn, "DELETE FROM perennial.publications")
        _denied(conn, "UPDATE perennial.current_publication SET publication_id = publication_id")
        _denied(conn, "CREATE TABLE perennial.ra_never (id int)")
    with engine.connect() as conn:
        for table in perennial_tables:
            for privilege in WRITES:
                assert not conn.execute(
                    text("SELECT has_table_privilege(:r, :t, :p)"),
                    {"r": api_role, "t": f"perennial.{table}", "p": privilege},
                ).scalar(), (table, privilege)
        assert conn.execute(
            text("SELECT has_schema_privilege(:r, 'perennial', 'USAGE')"), {"r": api_role}
        ).scalar()
        assert not conn.execute(
            text("SELECT has_schema_privilege(:r, 'perennial', 'CREATE')"), {"r": api_role}
        ).scalar()


@pytest.mark.db
def test_api_role_shape_and_idempotent_provisioning(engine, groups):
    api_role = groups["api"]
    with engine.connect() as conn:
        attrs = conn.execute(
            text(
                "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls,"
                " rolcanlogin FROM pg_roles WHERE rolname = :r"
            ),
            {"r": api_role},
        ).one()
        assert not any(attrs)
        assert conn.execute(
            text(
                "SELECT count(*) FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member"
                " WHERE r.rolname = :r"
            ),
            {"r": api_role},
        ).scalar() == 0
        assert conn.execute(
            text(
                "SELECT (SELECT count(*) FROM pg_class WHERE relowner = r.oid)"
                " + (SELECT count(*) FROM pg_namespace WHERE nspowner = r.oid)"
                " FROM pg_roles r WHERE r.rolname = :r"
            ),
            {"r": api_role},
        ).scalar() == 0
    before = _readable(engine, api_role)
    _provision(engine, tuple(groups[r] for r in ("owner", "importer", "reader", "api")))
    assert _readable(engine, api_role) == before == _expected_readable(engine)


@pytest.mark.db
def test_tables_created_after_provisioning_are_not_readable(engine, groups):
    with engine.begin() as conn:
        conn.execute(text(f'SET LOCAL ROLE "{groups["owner"]}"'))
        conn.execute(text("CREATE TABLE perennial.ra_future_table (id bigint)"))
    try:
        with engine.connect() as conn:
            for privilege in ("SELECT", *WRITES):
                assert not conn.execute(
                    text("SELECT has_table_privilege(:r, 'perennial.ra_future_table', :p)"),
                    {"r": groups["api"], "p": privilege},
                ).scalar()
    finally:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE perennial.ra_future_table"))


@pytest.mark.db
@pytest.mark.parametrize(
    "grant",
    [
        "GRANT SELECT ON perennial.publications TO {role}",
        "GRANT SELECT (id) ON perennial.securities TO {role}",
        "GRANT USAGE ON SCHEMA perennial TO {role}",
    ],
)
def test_provisioning_refuses_when_a_supabase_api_role_can_reach_perennial(
    engine, groups, grant
):
    sys.path.insert(0, str(OPS))
    from provision_roles import provision

    suffix = secrets.token_hex(5)
    supabase = tuple(f"ra_{r}_{suffix}" for r in ("anon", "authenticated", "service_role"))
    roles = tuple(groups[r] for r in ("owner", "importer", "reader", "api"))
    try:
        with engine.begin() as conn:
            for name in supabase:
                conn.execute(text(f'CREATE ROLE "{name}" NOLOGIN'))
        with engine.begin() as conn:
            # The fresh roles hold nothing yet, so provisioning with them passes.
            provision(conn.connection.driver_connection, roles, phase="grants",
                      supabase_roles=supabase)
        with engine.connect() as conn:
            conn.execute(text(grant.format(role=f'"{supabase[1]}"')))
            # A canary: provisioning would re-grant this if it changed anything.
            conn.execute(text(f'REVOKE SELECT ON perennial.publications FROM "{roles[3]}"'))
            with pytest.raises(ValueError) as caught:
                provision(conn.connection.driver_connection, roles, phase="grants",
                          supabase_roles=supabase)
            for name in (*supabase, *roles):
                assert name not in str(caught.value)
            assert not conn.execute(
                text("SELECT has_table_privilege(:r, 'perennial.publications', 'SELECT')"),
                {"r": roles[3]},
            ).scalar()
            conn.rollback()
    finally:
        _drop_roles(engine, supabase)


ALEMBIC_PRIVILEGES = "SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN"


def _restored_alembic_version(conn, default_grantees=()):
    """Recreate public.alembic_version the way a --no-owner --no-acl restore does:
    created by the admin login, so the admin's default privileges for public apply
    (Supabase's: GRANT ALL ON TABLES TO anon, authenticated, service_role)."""
    version = conn.execute(text("SELECT version_num FROM public.alembic_version")).scalar()
    if default_grantees:
        conn.execute(text(
            "ALTER DEFAULT PRIVILEGES FOR ROLE CURRENT_USER IN SCHEMA public GRANT ALL ON TABLES TO "
            + ", ".join(f'"{r}"' for r in default_grantees)))
    conn.execute(text("DROP TABLE public.alembic_version"))
    conn.execute(text("CREATE TABLE public.alembic_version (version_num varchar(32) NOT NULL,"
                      " CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num))"))
    conn.execute(text("INSERT INTO public.alembic_version VALUES (:v)"), {"v": version})
    return version


def _alembic_holders(conn, names):
    """The given roles that hold any privilege on public.alembic_version, by the
    privilege functions and by its ACL entries directly."""
    by_function = [
        name for name in names
        if conn.execute(text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :r)"),
                        {"r": name}).scalar()
        and conn.execute(text(
            "SELECT has_table_privilege(:r, 'public.alembic_version', :p)"
            " OR has_any_column_privilege(:r, 'public.alembic_version', 'SELECT,INSERT,UPDATE,REFERENCES')"),
            {"r": name, "p": ALEMBIC_PRIVILEGES}).scalar()
    ]
    by_acl = conn.execute(text(
        "SELECT DISTINCT r.rolname FROM pg_class c, aclexplode(c.relacl) a"
        " JOIN pg_roles r ON r.oid = a.grantee"
        " WHERE c.oid = 'public.alembic_version'::regclass AND r.rolname = ANY(:n)"),
        {"n": list(names)}).scalars().all()
    return sorted(set(by_function) | set(by_acl))


@pytest.mark.db
@pytest.mark.parametrize("present", ["all_three", "only_anon"])
def test_grants_strip_supabase_default_privileges_from_a_restored_alembic_version(
    engine, groups, present
):
    """Ops's finding on the Supabase image: a restore as the admin recreates
    alembic_version under Supabase's default ACL, so anon, authenticated and
    service_role get arwdDxtm on it, and the Data API serves public. After grants,
    none of them may hold any privilege on it, whichever of them exist."""
    sys.path.insert(0, str(OPS))
    from provision_roles import provision

    suffix = secrets.token_hex(5)
    supabase = tuple(f"ra_{r}_{suffix}" for r in ("anon", "authenticated", "service_role"))
    existing = supabase if present == "all_three" else supabase[:1]
    roles = tuple(groups[r] for r in ("owner", "importer", "reader", "api"))
    try:
        with engine.connect() as conn:  # rolled back: the session's table is untouched
            for name in existing:
                conn.execute(text(f'CREATE ROLE "{name}" NOLOGIN'))
            version = _restored_alembic_version(conn, existing)
            assert _alembic_holders(conn, existing) == sorted(existing)  # the exposure is real
            provision(conn.connection.driver_connection, roles, phase="grants",
                      supabase_roles=supabase)
            assert _alembic_holders(conn, supabase) == []
            assert conn.execute(text(
                "SELECT pg_get_userbyid(relowner) FROM pg_class"
                " WHERE oid = 'public.alembic_version'::regclass")).scalar() == roles[0]
            assert conn.execute(text("SELECT version_num FROM public.alembic_version")).scalar() == version
            conn.rollback()
    finally:
        _drop_roles(engine, supabase)


@pytest.mark.db
@pytest.mark.parametrize("supabase_roles", ["default_names", "unique_absent_names"])
def test_grants_on_a_restored_alembic_version_succeed_without_supabase_roles(
    engine, groups, supabase_roles
):
    """A plain database has no Supabase API roles; grants still re-own the
    restored table and finish, and leave PUBLIC and our groups without access."""
    sys.path.insert(0, str(OPS))
    from provision_roles import SUPABASE_API_ROLES, provision

    names = (SUPABASE_API_ROLES if supabase_roles == "default_names"
             else tuple(f"ra_absent_{r}_{secrets.token_hex(5)}" for r in ("anon", "authenticated")))
    roles = tuple(groups[r] for r in ("owner", "importer", "reader", "api"))
    with engine.connect() as conn:
        assert not conn.execute(text("SELECT count(*) FROM pg_roles WHERE rolname = ANY(:n)"),
                                {"n": list(names)}).scalar(), "a stand-in name exists here"
        _restored_alembic_version(conn)
        conn.execute(text("GRANT SELECT ON public.alembic_version TO PUBLIC"))
        provision(conn.connection.driver_connection, roles, phase="grants", supabase_roles=names)
        assert conn.execute(text(
            "SELECT pg_get_userbyid(relowner) FROM pg_class"
            " WHERE oid = 'public.alembic_version'::regclass")).scalar() == roles[0]
        assert _alembic_holders(conn, roles[1:]) == []
        assert not conn.execute(text(
            "SELECT EXISTS (SELECT 1 FROM pg_class c, aclexplode(c.relacl) a"
            " WHERE c.oid = 'public.alembic_version'::regclass AND a.grantee = 0)")).scalar()
        conn.rollback()


# ── RA-15: startup refusal ───────────────────────────────


def _refusal(engine, groups, caplog, api_engine, secrets_to_hide):
    from fastapi.testclient import TestClient
    from src.api.read import create_app

    caplog.set_level(logging.DEBUG)
    with pytest.raises(Exception) as caught:
        with TestClient(create_app(api_engine, api_role=groups["api"])) as c:
            c.get("/v1/watchlist")
    shown = f"{type(caught.value).__name__}: {caught.value}\n{caplog.text}"
    for hidden in (*secrets_to_hide, groups["api"], "postgresql", "SELECT ", "pg_roles"):
        assert hidden not in shown, "startup refusal leaked a name, URL or SQL"
    return type(caught.value), str(caught.value)


@pytest.fixture
def scratch_database(engine):
    """A database with an empty perennial schema (no tables), dropped afterwards.

    The schema exists, so only the empty-table rule can refuse it, not an error
    from looking up a missing schema.
    """
    from tests.conftest import pinned_engine

    name = f"perennial_test_ra_{secrets.token_hex(4)}"
    admin = pinned_engine(engine.url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    scratch = pinned_engine(engine.url.set(database=name))
    with scratch.begin() as conn:
        conn.execute(text("CREATE SCHEMA perennial"))
        conn.execute(text("GRANT USAGE ON SCHEMA perennial TO PUBLIC"))
    scratch.dispose()
    try:
        yield name
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


REFUSING = {
    "superuser": dict(options="SUPERUSER"),
    "bypassrls": dict(options="BYPASSRLS"),
    "noinherit_owner": dict(options="NOINHERIT", members=("api", "owner"), connect=True),
    "noinherit_write_all": dict(
        options="NOINHERIT", members=("api",), extra="pg_write_all_data", connect=True
    ),
    "api_and_reader": dict(members=("api", "reader")),
    "owns_a_schema": dict(owns=True),  # outside perennial, so no write privilege hides it
    "empty_database": dict(empty=True),
}


@pytest.mark.db
def test_every_unsafe_login_is_refused_with_the_same_fixed_error(
    engine, groups, caplog, scratch_database
):
    outcomes = {}
    for case, spec in REFUSING.items():
        with login(
            engine,
            groups,
            options=spec.get("options", ""),
            members=spec.get("members", ("api",)),
            database=scratch_database if spec.get("empty") else None,
            connect=spec.get("connect", False),
        ) as (api_engine, name, password):
            owned = None
            with engine.begin() as conn:
                if "extra" in spec:
                    conn.execute(text(f'GRANT {spec["extra"]} TO "{name}"'))
                if spec.get("owns"):
                    owned = f"ra_owned_{secrets.token_hex(4)}"
                    conn.execute(text(f'CREATE SCHEMA {owned} AUTHORIZATION "{name}"'))
            try:
                outcomes[case] = _refusal(
                    engine, groups, caplog, api_engine,
                    (name, password, scratch_database, engine.url.database),
                )
            finally:
                if owned:
                    with engine.begin() as conn:
                        conn.execute(text(f"DROP SCHEMA IF EXISTS {owned} CASCADE"))
    assert len(set(outcomes.values())) == 1, outcomes  # one fixed error for every clause


@pytest.mark.db
def test_a_correctly_shaped_login_starts(engine, groups):
    with login(engine, groups) as (api_engine, _, _), client(api_engine, groups) as c:
        assert_error(c.get("/v1/watchlist"), 404, "no_current_publication")


# ── RA-16, RA-16b: read-only request transactions ────────


@pytest.mark.db
def test_request_transactions_are_read_only_per_transaction(engine):
    from src.api.read import read_transaction
    from tests.conftest import pinned_engine

    writer = pinned_engine(engine.url, workload="api", pool_size=1, max_overflow=0)
    try:
        with pytest.raises(DBAPIError) as caught:
            with read_transaction(writer) as conn:
                pid = conn.execute(text("SELECT pg_backend_pid()")).scalar()
                conn.execute(
                    text(
                        "INSERT INTO perennial.securities (symbol) VALUES ('ZZWRITE')"
                    )
                )
        assert caught.value.orig.sqlstate == "25006"
        with read_transaction(writer) as conn:
            assert conn.execute(text("SELECT pg_backend_pid()")).scalar() == pid
            assert conn.execute(text("SHOW transaction_read_only")).scalar() == "on"
        # Never a session SET: the same pooled connection writes outside the helper.
        with writer.begin() as conn:
            assert conn.execute(text("SELECT pg_backend_pid()")).scalar() == pid
            assert conn.execute(text("SHOW transaction_read_only")).scalar() == "off"
            assert conn.execute(text("SHOW default_transaction_read_only")).scalar() == "off"
    finally:
        writer.dispose()


@pytest.mark.db
def test_every_v1_route_uses_the_helper_exactly_once(engine, fixture_path, groups, monkeypatch):
    from fastapi.testclient import TestClient
    from src.api import read

    publish_run(engine, feed(fixture_path))
    real = read.read_transaction
    calls, outside, inside = [], [], [0]

    @contextmanager
    def recording(*args, **kwargs):
        calls.append(args)
        inside[0] += 1
        try:
            with real(*args, **kwargs) as conn:
                yield conn
        finally:
            inside[0] -= 1

    with login(engine, groups) as (api_engine, _, _):
        app = read.create_app(api_engine, api_role=groups["api"])
        with TestClient(app) as c:  # startup's own check may use the engine
            monkeypatch.setattr(read, "read_transaction", recording)

            @event.listens_for(api_engine, "before_cursor_execute")
            def watch(conn, cursor, statement, *rest):
                if not inside[0]:
                    outside.append(statement)

            routes = sorted(v1_routes(app))
            assert routes == ["/v1/securities/{symbol}", "/v1/watchlist"]
            for path in routes:
                calls.clear()
                response = c.get(path.replace("{symbol}", "ZZAA"))
                assert response.status_code == 200, path
                assert len(calls) == 1, path
            event.remove(api_engine, "before_cursor_execute", watch)
    assert outside == []


# ── RA-17: logs and errors ───────────────────────────────


@pytest.mark.db
def test_logs_are_json_without_urls_or_query_strings(engine, groups, capfd):
    from src.utils.logger import configure_logging

    configure_logging()
    with login(engine, groups) as (api_engine, name, password), client(api_engine, groups) as c:
        capfd.readouterr()
        c.get("/v1/watchlist?api_key=synthetic-query-secret")
        c.get("/v1/securities/ZZNONE?token=synthetic-query-secret")
        c.get("/v1/securities/BR$K")
        logged = capfd.readouterr().err
    for line in filter(None, logged.splitlines()):
        json.loads(line)
    for hidden in ("synthetic-query-secret", "postgresql", password, "SELECT "):
        assert hidden not in logged


# ── RA-18: schema document and OpenAPI snapshot ──────────


def _v1_openapi(app):
    """The /v1 paths and every component schema they reference, transitively."""
    document = app.openapi()
    paths = {p: v for p, v in document["paths"].items() if p.startswith("/v1")}
    schemas = document.get("components", {}).get("schemas", {})
    wanted, pending = {}, [paths]
    while pending:
        node = pending.pop()
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/components/schemas/"):
                name = ref.rsplit("/", 1)[1]
                if name not in wanted:
                    wanted[name] = schemas[name]
                    pending.append(schemas[name])
            pending.extend(node.values())
        elif isinstance(node, list):
            pending.extend(node)
    return {"paths": paths, "schemas": dict(sorted(wanted.items()))}


def _router_app():
    """The /v1 shape alone: the router on a bare app, no database or startup check."""
    from fastapi import FastAPI
    from src.api.read import router

    app = FastAPI()
    app.include_router(router)  # the router carries /v1, as #22's app.py will include it
    return app


def v1_routes(app):
    """/v1 path -> HTTP methods, from the OpenAPI document (stable across FastAPI versions)."""
    return {
        path: sorted(m.upper() for m in ops)
        for path, ops in app.openapi()["paths"].items()
        if path.startswith("/v1")
    }


def test_v1_routes_are_exactly_the_r1_reads():
    from src.api.read import create_app

    routes = v1_routes(_router_app())
    assert routes == {"/v1/watchlist": ["GET"], "/v1/securities/{symbol}": ["GET"]}
    assert callable(create_app)


def test_openapi_v1_snapshot():
    """Regenerate deliberately with UPDATE_API_SNAPSHOT=1, then review the diff."""
    import os

    current = _v1_openapi(_router_app())
    if os.environ.get("UPDATE_API_SNAPSHOT") == "1":
        SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
        SNAPSHOT.write_text(json.dumps(current, indent=2, sort_keys=True) + "\n")
    assert SNAPSHOT.exists(), "no v1 OpenAPI snapshot; generate it with UPDATE_API_SNAPSHOT=1"
    assert current == json.loads(SNAPSHOT.read_text())


def test_api_document_describes_the_v1_schemas():
    doc = REPO / "docs" / "technical-specs" / "API.md"
    assert doc.exists()
    content = doc.read_text()
    for name in (
        "/v1/watchlist",
        "/v1/securities/{symbol}",
        *sorted(PUBLICATION_KEYS),
        *sorted(SECURITY_KEYS),
        "api_version",
        "no_current_publication",
        "unknown_security",
        "invalid_request",
    ):
        assert re.search(re.escape(name), content), name


# ── Edge cases found after the build (04d5793) ───────────


@pytest.mark.db
def test_unknown_routes_and_methods_keep_the_error_shape(api):
    for response in (api.get("/v1/nothing-here"), api.post("/v1/watchlist"),
                     api.delete("/v1/securities/ZZAA")):
        assert response.status_code in (404, 405)
        body = response.json()
        assert set(body) == {"detail", "code"} and isinstance(body["code"], str)


@pytest.mark.db
def test_hostile_symbols_are_422_not_500_and_never_echoed(engine, api):
    for bad in ("%00", "ZZ%00AA", "A" * 1000, "%E2%80%8BZZAA", "ZZ%0AAA"):
        assert_error(api.get(f"/v1/securities/{bad}"), 422, "invalid_request", "A" * 1000)
    response = api.get("/v1/securities/ZZNOSUCH")
    assert_error(response, 404, "unknown_security", "ZZNOSUCH")


@pytest.mark.db
def test_security_lookup_without_a_current_publication(engine, api):
    seed_security(engine, "ZZSOLO")
    body = api.get("/v1/securities/ZZSOLO").json()
    assert body["current"] == {"publication": None, "entry": None}


@pytest.mark.db
def test_provider_prefixed_aliases_never_match_a_plain_symbol(engine, api):
    sid = seed_security(engine, "ZZCANON")
    with engine.begin() as conn:
        conn.execute(insert(models.security_identifiers).values(
            scheme="ticker", value="FINNHUB:ZZPROV", security_id=sid))
    assert_error(api.get("/v1/securities/ZZPROV"), 404, "unknown_security")


@pytest.mark.db
def test_a_degraded_event_without_reused_sources(engine, fixture_path, api):
    """Degraded by Congress gaps alone: degraded, no stale sources, the gap count."""
    pub = publish_run(engine, feed(fixture_path))
    with engine.begin() as conn:
        conn.execute(insert(models.publication_events).values(
            publication_id=pub, kind="degraded", created_at=datetime.now().astimezone(),
            checks={"reused": {}, "cohort": [], "all_sources_reused": False,
                    "congress_gaps": {"count": 2}}))
    meta = api.get("/v1/watchlist").json()["publication"]
    assert meta["degraded"] is True and meta["stale_sources"] == []
    assert meta["congress_history_gaps"] == 2


@pytest.mark.db
def test_an_overridden_publication_serves_its_envelope_and_demo_marker(
    engine, fixture_path, api
):
    pub = publish_run(engine, feed(fixture_path))
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE perennial.publication_events SET kind = 'quality_override',"
            " actor = 'synthetic-operator', reason = 'synthetic override',"
            " checks = checks || '{\"demo\": true}' WHERE publication_id = :p"
            " AND kind = 'quality_passed'"), {"p": pub})
    response = api.get("/v1/watchlist")
    assert response.status_code == 200
    assert response.json()["publication"]["demo"] is True
    for operator_text in ("synthetic-operator", "synthetic override"):
        assert operator_text not in response.text


@pytest.mark.db
def test_conditional_requests_follow_http_matching(engine, fixture_path, api):
    """RFC 9110: a list of tags matches if any does; If-None-Match compares weakly."""
    pub = publish_run(engine, feed(fixture_path))
    tag = f'"v1-{pub}"'
    for header in (f'"v1-0", {tag}', f"W/{tag}"):
        response = api.get("/v1/watchlist", headers={"If-None-Match": header})
        assert response.status_code == 304 and response.content == b"", header
        assert response.headers["etag"] == tag


REFUSING_MORE = {
    "createrole": dict(options="CREATEROLE"),
    "createdb": dict(options="CREATEDB"),
    "replication": dict(options="REPLICATION"),
    "indirect_member": dict(via_intermediate=True),
    "create_on_database": dict(grant="GRANT CREATE ON DATABASE \"{db}\" TO \"{name}\""),
    "create_on_schema": dict(grant="GRANT CREATE ON SCHEMA perennial TO \"{name}\""),
}


@pytest.mark.db
@pytest.mark.parametrize("case", sorted(REFUSING_MORE))
def test_more_unsafe_logins_are_refused_the_same_way(engine, groups, caplog, case):
    """The remaining attribute, membership and CREATE clauses of RA-15."""
    spec = REFUSING_MORE[case]
    intermediate = f"ra_mid_{secrets.token_hex(4)}"
    members = ("api",)
    if spec.get("via_intermediate"):
        with engine.begin() as conn:
            conn.execute(text(f'CREATE ROLE "{intermediate}" NOLOGIN IN ROLE "{groups["api"]}"'))
        groups = {**groups, "mid": intermediate}
        members = ("mid",)
    try:
        with login(engine, groups, options=spec.get("options", ""), members=members) as (
            api_engine, name, password
        ):
            if "grant" in spec:
                with engine.begin() as conn:
                    conn.execute(text(spec["grant"].format(db=engine.url.database, name=name)))
            outcome = _refusal(engine, groups, caplog, api_engine,
                               (name, password, engine.url.database, intermediate))
            with login(engine, groups, options="SUPERUSER") as (super_engine, n2, p2):
                reference = _refusal(engine, groups, caplog, super_engine, (n2, p2))
            assert outcome == reference  # the same fixed error as every other clause
    finally:
        _drop_roles(engine, [intermediate])


@pytest.mark.db
def test_bodies_do_not_depend_on_the_session_time_zone(engine, fixture_path, groups):
    """MR1-1: the same ETag must mean the same bytes; timestamps are served in UTC."""
    f = feed(fixture_path)
    publish_run(engine, f)
    f.fail = {"insider_trades"}  # degraded, so stale_sources timestamps are served too
    pub = publish_run(engine, f)
    bodies = {}
    for zone in ("UTC", "America/Los_Angeles", "Asia/Kolkata"):
        with login(engine, groups) as (api_engine, name, _):
            with engine.begin() as conn:
                conn.execute(text(f"ALTER ROLE \"{name}\" SET timezone = '{zone}'"))
            with client(api_engine, groups) as c:
                watchlist = c.get("/v1/watchlist")
                security = c.get("/v1/securities/ZZAA")
        bodies[zone] = (watchlist.headers["etag"], watchlist.content, security.content)
    assert len(set(bodies.values())) == 1, "a session time zone changed the served bytes"
    meta = json.loads(bodies["UTC"][1])["publication"]
    assert meta["id"] == pub
    assert meta["created_at"] == row(engine, pub).created_at.astimezone(timezone.utc).isoformat()
    assert meta["created_at"].endswith("+00:00")
    assert meta["stale_sources"] and all(
        s["collected_at"].endswith("+00:00") for s in meta["stale_sources"]
    )
