"""#22 fix branch: RB-2 to RB-10, and #22's halves of RL-4, RL-15 and RL-15b.

Spec: docs/work-items/m5-pr22-fix-packet.md (FINAL at 596a68f) and the M5 note's
rate-limit addendum. RB-13 (backup and restore) is in test_pr22_restore.py.

Test rule (above RB-8): every RLS assertion runs as the role that matters (the
API login, the owner or another named role), never as the superuser test login.
New interfaces are reached inside each test so the suite still collects.
"""

import json
import re
import secrets
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from pr22_support import (
    APP_TABLES,
    PRE_LOGIN_TABLES,
    USER_TABLES,
    account_client,
    drop_roles,
    integrated_app,
    pr22_roles,  # noqa: F401  (pytest fixture)
    provision_api,
    provision_roles,
    register,
    role_names,
    seed_securities,
)

pytestmark = pytest.mark.db

REPO = Path(__file__).resolve().parents[3]
APP_WRITES = ("SELECT", "INSERT", "UPDATE", "DELETE")


def _version(conn):
    return conn.execute(text("SELECT version_num FROM public.alembic_version")).scalar()


def _script():
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    from tests.conftest import ALEMBIC_INI

    return ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))


def _app_revision():
    """(revision, down_revision) of the migration that creates schema app."""
    for rev in _script().walk_revisions():
        # Either form: IF NOT EXISTS, or option 2's catalog-checked CREATE (decision 37).
        if re.search(r"CREATE SCHEMA (IF NOT EXISTS )?app\b", Path(rev.path).read_text()):
            return rev.revision, rev.down_revision
    raise AssertionError("no migration creates schema app")


@contextmanager
def as_role(engine, role, subject=None):
    """A transaction as `role` (SET LOCAL ROLE), optionally with a subject set."""
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text(f'SET LOCAL ROLE "{role}"'))
            if subject is not None:
                conn.execute(text("SELECT set_config('request.jwt.claim.sub', :s, true)"),
                             {"s": subject})
            yield conn


# ── RB-2: migrations ─────────────────────────────────────


def test_one_head_numbered_revisions_and_the_ruff_include(engine):
    """RB-2a (also a gate at #22's final merge)."""
    script = _script()
    assert len(script.get_heads()) == 1
    revisions = list(script.walk_revisions())
    assert all(re.fullmatch(r"\d{4}", r.revision) for r in revisions)
    app_rev, down = _app_revision()
    assert app_rev > "0010" and down is not None
    pyproject = (REPO / "pyproject.toml").read_text()
    for rev in revisions:
        if rev.revision > "0010":
            name = Path(rev.path).name
            assert name in pyproject, f"{name} is not format-checked"
            assert re.fullmatch(r"\d{8}_\d{4}_[a-z0-9_]+\.py", name)


def test_no_migration_creates_tables_in_perennial_beyond_the_models(engine):
    """RB-2b: the legacy user tables are gone."""
    from src.db import models

    with engine.connect() as conn:
        tables = set(conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'perennial'")).scalars())
    assert tables == {t.name for t in models.metadata.tables.values()}


def test_round_trip_has_no_drift_including_schema_app(engine):
    """RB-2c."""
    from alembic import command
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext
    from sqlalchemy import MetaData

    from src.api.account_models import metadata as account_metadata
    from src.db.models import metadata
    from tests.conftest import alembic_config

    merged = MetaData()
    for registry in (metadata, account_metadata):
        for table in registry.tables.values():
            table.to_metadata(merged)
    app_rev, down = _app_revision()
    with engine.connect() as conn:
        head = _version(conn)
        try:
            command.downgrade(alembic_config(conn), down)
            conn.commit()
            assert conn.execute(text("SELECT to_regnamespace('app')")).scalar() is None
        finally:
            command.upgrade(alembic_config(conn), "head")
            conn.commit()
        assert _version(conn) == head
        diff = compare_metadata(
            MigrationContext.configure(conn, opts={
                "include_schemas": True, "compare_type": True,
                "include_name": lambda n, t, p: t != "schema" or n in {"perennial", "app"}}),
            merged,
        )
    assert diff == []


def test_downgrade_as_the_owner_refuses_while_profiles_exist(engine, pr22_roles):  # noqa: F811
    """RB-2d: run as the NOBYPASSRLS owner, which sees 0 rows under FORCE RLS."""
    from alembic import command

    from tests.conftest import alembic_config

    owner = pr22_roles["owner"]
    app_rev, down = _app_revision()
    with engine.begin() as conn:  # superuser: populate past RLS
        conn.execute(text("INSERT INTO app.profiles (id, email, full_name)"
                          " VALUES (:i, 'one@example.com', 'One')"), {"i": str(uuid4())})
    with engine.connect() as conn:
        head = _version(conn)
        conn.execute(text(f'SET ROLE "{owner}"'))
        try:
            with pytest.raises((RuntimeError, DBAPIError)) as caught:
                command.downgrade(alembic_config(conn), down)
            if isinstance(caught.value, DBAPIError):  # a refusal raised in SQL, not a privilege error
                assert caught.value.orig.sqlstate == "P0001", caught.value.orig.sqlstate
        finally:
            conn.rollback()
            conn.execute(text("RESET ROLE"))
            if _version(conn) != head:
                command.upgrade(alembic_config(conn), "head")
                conn.commit()
        assert _version(conn) == head
        assert conn.execute(text("SELECT relforcerowsecurity FROM pg_class"
                                 " WHERE oid = 'app.profiles'::regclass")).scalar() is True
        assert conn.execute(text("SELECT count(*) FROM app.profiles")).scalar() == 1


def test_downgrade_as_the_owner_succeeds_on_an_empty_database(engine, pr22_roles):  # noqa: F811
    """RB-2d / RB-2c: an empty app downgrades as the owner."""
    from alembic import command

    from tests.conftest import alembic_config

    app_rev, down = _app_revision()
    with engine.connect() as conn:
        head = _version(conn)
        conn.execute(text(f'SET ROLE "{pr22_roles["owner"]}"'))
        try:
            command.downgrade(alembic_config(conn), down)
            conn.commit()
            assert _version(conn) == down
        finally:
            conn.rollback()
            conn.execute(text("RESET ROLE"))
            command.upgrade(alembic_config(conn), "head")
            conn.commit()
        assert _version(conn) == head


# ── RB-3: the watchlist by security_id ───────────────────


def test_watchlist_is_keyed_by_security(engine):
    """RB-3a."""
    with engine.connect() as conn:
        pk = conn.execute(text(
            "SELECT array_agg(a.attname ORDER BY a.attname) FROM pg_index i"
            " JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)"
            " WHERE i.indrelid = 'app.watchlist'::regclass AND i.indisprimary")).scalar()
        fk = conn.execute(text(
            "SELECT confrelid::regclass::text, confdeltype FROM pg_constraint"
            " WHERE conrelid = 'app.watchlist'::regclass AND contype = 'f'"
            " AND confrelid = 'perennial.securities'::regclass")).one_or_none()
    assert sorted(pk) == ["security_id", "user_id"]
    assert fk is not None and fk[1] == "r"  # ON DELETE RESTRICT


@pytest.fixture
def accounts_api(engine, pr22_roles, monkeypatch):  # noqa: F811
    with integrated_app(engine, pr22_roles, monkeypatch) as (app, _, _):
        with account_client(app, monkeypatch) as (client, provider, counting):
            yield client, provider, counting


def test_watchlist_resolves_aliases_and_orders_by_symbol(engine, accounts_api):
    """RB-3b, RB-3d."""
    client, provider, _ = accounts_api
    ids = seed_securities(engine, "BRK.B", "AAPL", aliases={"BRK.B": ("BRK-B",)})
    _, headers = register(client, provider)
    for ticker in ("BRK-B", "aapl", "BRK.B"):  # the last is a duplicate of the first
        assert client.post("/v1/me/watchlist", headers=headers,
                           json={"ticker": ticker}).status_code == 201, ticker
    assert client.get("/v1/me/watchlist", headers=headers).json() == [
        {"ticker": "AAPL"}, {"ticker": "BRK.B"}]
    with engine.connect() as conn:  # superuser, reading the stored keys only
        stored = sorted(conn.execute(text("SELECT security_id FROM app.watchlist")).scalars())
    assert stored == sorted([ids["BRK.B"], ids["AAPL"]])
    assert client.delete("/v1/me/watchlist/BRK-B", headers=headers).status_code == 204
    assert client.get("/v1/me/watchlist", headers=headers).json() == [{"ticker": "AAPL"}]


def test_unknown_and_malformed_tickers(engine, accounts_api):
    """RB-3c (decision 28, PK-5)."""
    client, provider, _ = accounts_api
    seed_securities(engine, "AAPL")
    _, headers = register(client, provider)
    response = client.post("/v1/me/watchlist", headers=headers, json={"ticker": "ZZNOPE"})
    assert response.status_code == 404 and response.json()["code"] == "unknown_security"
    assert client.delete("/v1/me/watchlist/ZZNOPE", headers=headers).status_code == 204
    assert client.post("/v1/me/watchlist", headers=headers,
                       json={"ticker": "BAD TICKER"}).status_code == 422
    assert client.get("/v1/me/watchlist", headers=headers).json() == []
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM app.watchlist")).scalar() == 0


# ── RB-4: routes under /v1 and no-store ──────────────────


def _account_routes():
    from fastapi.routing import APIRoute

    from src.api import accounts

    return [(r.path, m) for r in accounts.router.routes if isinstance(r, APIRoute)
            for m in r.methods]


def test_every_account_route_answers_under_v1_only(accounts_api):
    """RB-4a."""
    client, _, _ = accounts_api
    routes = _account_routes()
    assert routes
    for path, method in routes:
        concrete = path.replace("{ticker}", "ZZAA")
        old = client.request(method, concrete)
        new = client.request(method, "/v1" + concrete)
        assert old.status_code == 404, (method, path)
        assert new.status_code != 404, (method, path)
    assert client.get("/health").status_code == 200
    assert client.get("/api/watchlist").status_code == 200


def test_auth_and_me_responses_are_no_store(accounts_api):
    """RB-4b."""
    client, provider, _ = accounts_api
    _, headers = register(client, provider)
    for response in (
        client.get("/v1/me", headers=headers),
        client.get("/v1/me"),
        client.get("/v1/me/watchlist", headers=headers),
        client.post("/v1/auth/login", json={}),
        client.post("/v1/auth/refresh", json={"refresh_token": "x"}),
    ):
        assert response.headers.get("cache-control") == "no-store", response.request.url


# ── RB-5: one engine, R1 unchanged, read-only reads, the shared installer ─


def test_one_api_engine_from_api_database_url(engine, pr22_roles, monkeypatch):  # noqa: F811
    """RB-5a."""
    from src.api import accounts

    with integrated_app(engine, pr22_roles, monkeypatch) as (app, user, _):
        assert app.state.engine is accounts.get_engine()
        assert app.state.engine.url.username == user
        assert app.state.api_role == pr22_roles["api"]
        with app.state.engine.connect() as conn:
            assert conn.execute(text("SHOW application_name")).scalar() == "perennial-api"


def test_r1_reads_are_identical_in_both_apps(engine, fixture_path, pr22_roles, monkeypatch):  # noqa: F811
    """RB-5b: the integrated app serves R1's bytes exactly."""
    from fastapi.testclient import TestClient

    from src.api.read import create_app as read_app
    from test_read_api import feed, login, publish_run

    publish_run(engine, feed(fixture_path))
    paths = ("/v1/watchlist", "/v1/securities/ZZAA", "/v1/securities/ZZNONE",
             "/v1/securities/BR$K")
    with login(engine, pr22_roles) as (r1_engine, _, _), \
            TestClient(read_app(r1_engine, api_role=pr22_roles["api"])) as r1:
        expected = [(r.status_code, r.content) for r in map(r1.get, paths)]
    with integrated_app(engine, pr22_roles, monkeypatch) as (app, _, _), TestClient(app) as c:
        got = [(r.status_code, r.content) for r in map(c.get, paths)]
    assert got == expected


def test_read_routes_use_the_read_only_helper_once_in_the_integrated_app(
    engine, fixture_path, pr22_roles, monkeypatch  # noqa: F811
):
    """RB-5c."""
    from fastapi.testclient import TestClient

    from src.api import read
    from test_read_api import feed, publish_run, v1_routes

    publish_run(engine, feed(fixture_path))
    real, calls = read.read_transaction, []

    @contextmanager
    def recording(*args, **kwargs):
        calls.append(args)
        with real(*args, **kwargs) as conn:
            yield conn

    with integrated_app(engine, pr22_roles, monkeypatch) as (app, _, _), TestClient(app) as c:
        monkeypatch.setattr(read, "read_transaction", recording)
        read_paths = [p for p in v1_routes(app) if not p.startswith(("/v1/auth", "/v1/me"))]
        assert sorted(read_paths) == ["/v1/securities/{symbol}", "/v1/watchlist"]
        for path in read_paths:
            calls.clear()
            assert c.get(path.replace("{symbol}", "ZZAA")).status_code == 200
            assert len(calls) == 1, path


def _call(app, path, **kwargs):
    from test_rate_limit import call

    return call(app, path, **kwargs)


def test_the_integrated_app_keys_clients_like_create_app(engine, pr22_roles, monkeypatch):  # noqa: F811
    """RB-5d, RL-15: through a trusted proxy, keyed by the forwarded client."""
    env = {"API_RATE_LIMIT_BURST": "2", "API_RATE_LIMIT_PER_MINUTE": "1",
           "API_TRUSTED_PROXIES": "10.0.0.1, 10.0.0.2"}
    chain = lambda client: [("X-Forwarded-For", f"{client}, 10.0.0.1")]  # noqa: E731
    inner = ("10.0.0.2", 1)
    with integrated_app(engine, pr22_roles, monkeypatch, env=env) as (app, _, _):
        assert [_call(app, "/v1/watchlist", client=inner, headers=chain("198.51.100.7"))[0]
                for _ in range(2)] == [404, 404]
        assert _call(app, "/v1/watchlist", client=inner,
                     headers=chain("198.51.100.7"))[0] == 429
        assert _call(app, "/v1/watchlist", client=inner,
                     headers=chain("198.51.100.8"))[0] == 404
        # IPv4-mapped and /64 normalization are the shared installer's (RL-6).
        assert _call(app, "/v1/watchlist", client=("::ffff:198.51.100.9", 1))[0] == 404


def _signup(client, email):
    return client.post("/v1/auth/signup",
                       json={"email": email, "full_name": "Test Person", "password": "Password123"})


def _forgot(client, email):
    return client.post("/v1/auth/forgot-password", json={"email": email})


def test_enumeration_routes_share_a_small_auth_bucket(engine, pr22_roles, monkeypatch):  # noqa: F811
    """RB-5d: 429 with Retry-After once the auth bucket is empty; nothing is called."""
    env = {"API_AUTH_RATE_LIMIT_BURST": "3", "API_AUTH_RATE_LIMIT_PER_HOUR": "1"}
    with integrated_app(engine, pr22_roles, monkeypatch, env=env) as (app, _, _), \
            account_client(app, monkeypatch) as (client, provider, counting):
        calls = []
        provider.signup = lambda *a, **k: calls.append("signup")
        provider.send_code = lambda *a, **k: calls.append("send_code")
        statuses = [_signup(client, "a@example.com").status_code,
                    _forgot(client, "b@example.com").status_code,
                    _signup(client, "c@example.com").status_code]
        assert 429 not in statuses
        before = (counting.lookups, len(calls))
        for limited in (_signup(client, "d@example.com"), _forgot(client, "e@example.com")):
            assert limited.status_code == 429
            assert re.fullmatch(r"[1-9][0-9]*", limited.headers["retry-after"])
            assert limited.json()["code"] == "rate_limited"
        assert (counting.lookups, len(calls)) == before  # no oracle, no provider call


def test_the_auth_limit_is_the_first_dependency_on_the_enumeration_routes():
    """RB-5d (coordinator): limited before any other dependency or validation."""
    from fastapi.routing import APIRoute

    from src.api import accounts, read

    routes = {(r.path, m): r for r in accounts.router.routes if isinstance(r, APIRoute)
              for m in r.methods}
    for key in (("/auth/signup", "POST"), ("/auth/forgot-password", "POST")):
        first = routes[key].dependant.dependencies[0]
        assert first.call is read.auth_rate_limit, key


def test_read_and_auth_buckets_are_independent(engine, pr22_roles, monkeypatch):  # noqa: F811
    """RL-4, #22 half."""
    env = {"API_AUTH_RATE_LIMIT_BURST": "1", "API_AUTH_RATE_LIMIT_PER_HOUR": "1",
           "API_RATE_LIMIT_BURST": "2", "API_RATE_LIMIT_PER_MINUTE": "1"}
    with integrated_app(engine, pr22_roles, monkeypatch, env=env) as (app, _, _), \
            account_client(app, monkeypatch) as (client, _, _):
        assert _signup(client, "a@example.com").status_code != 429
        assert _signup(client, "b@example.com").status_code == 429
        assert client.get("/v1/watchlist").status_code == 404  # reads still served
        assert client.get("/v1/watchlist").status_code == 404
        assert client.get("/v1/watchlist").status_code == 429
        assert _forgot(client, "c@example.com").status_code == 429  # its own bucket, still empty


def test_the_integrated_app_has_exactly_one_proxy_middleware(engine, pr22_roles, monkeypatch):  # noqa: F811
    """RL-15b, #22 half."""
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

    with integrated_app(engine, pr22_roles, monkeypatch) as (app, _, _):
        assert sum(m.cls is ProxyHeadersMiddleware for m in app.user_middleware) == 1


def test_python_m_src_api_runs_uvicorn_with_safe_settings(monkeypatch):
    """RL-15b and RB-10: proxy_headers=False, access_log=False, log_config=None."""
    import runpy

    import uvicorn

    seen = {}
    monkeypatch.setattr(uvicorn, "run", lambda *a, **k: seen.update(k, args=a))
    runpy.run_module("src.api", run_name="__main__")
    assert seen.get("proxy_headers") is False
    assert seen.get("access_log") is False
    assert "log_config" in seen and seen["log_config"] is None


# ── RB-6: the extended startup check ─────────────────────


def _refused(engine, names, monkeypatch, caplog, **login_kwargs):
    from fastapi.testclient import TestClient

    caplog.set_level("DEBUG")
    with integrated_app(engine, names, monkeypatch, **login_kwargs) as (app, user, password):
        with pytest.raises(Exception) as caught:
            with TestClient(app):
                pass
    shown = f"{caught.value}\n{caplog.text}"
    for hidden in (user, password, names["api"], "postgresql", "SELECT "):
        assert hidden not in shown
    return type(caught.value), str(caught.value)


@pytest.fixture
def extra_objects(engine):
    """An app table outside the six, and a stand-in for Supabase's auth schema."""
    suffix = secrets.token_hex(4)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS auth"))
        conn.execute(text(f"CREATE TABLE app.rb_extra_{suffix} (id int)"))
    try:
        yield suffix
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS app.rb_extra_{suffix}"))
            conn.execute(text("DROP SCHEMA IF EXISTS auth CASCADE"))


RB6_GRANTS = {
    "perennial_write": "GRANT INSERT ON perennial.publications TO {login}",
    "app_write_outside_the_six": "GRANT INSERT ON app.rb_extra_SUFFIX TO {login}",
    "create_on_app": "GRANT CREATE ON SCHEMA app TO {login}",
    "usage_on_auth": "GRANT USAGE ON SCHEMA auth TO {login}",
}


@pytest.mark.parametrize("case", sorted(RB6_GRANTS))
def test_startup_refuses_writes_outside_the_app_set_and_auth_usage(
    engine, pr22_roles, monkeypatch, caplog, extra_objects, case  # noqa: F811
):
    """RB-6: each granted directly to an otherwise correct login."""
    grant = RB6_GRANTS[case].replace("SUFFIX", extra_objects)
    refused = _refused(engine, pr22_roles, monkeypatch, caplog, grants=[grant])
    reference = _refused(engine, pr22_roles, monkeypatch, caplog, options="BYPASSRLS")
    assert refused == reference  # R1's one fixed error


def test_startup_accepts_a_provisioned_login_with_execute_on_the_lookup(
    engine, pr22_roles, monkeypatch  # noqa: F811
):
    from fastapi.testclient import TestClient

    with engine.connect() as conn:
        assert conn.execute(text(
            "SELECT has_function_privilege(:r, 'app.email_is_registered(text)', 'EXECUTE')"),
            {"r": pr22_roles["api"]}).scalar() is True
    with integrated_app(engine, pr22_roles, monkeypatch) as (app, _, _), TestClient(app) as c:
        assert c.get("/health").status_code == 200


def test_startup_without_api_database_url_refuses_with_a_fixed_error(monkeypatch, caplog):
    """RB-6b: no fallback to DATABASE_URL."""
    from fastapi.testclient import TestClient

    from src.api import accounts

    monkeypatch.delenv("API_DATABASE_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://owner:synthetic-pw@localhost/perennial_test")
    accounts.get_engine.cache_clear()
    try:
        with pytest.raises(Exception) as caught:
            from src.api.app import create_app

            with TestClient(create_app()):
                pass
        shown = f"{caught.value}\n{caplog.text}"
        assert "synthetic-pw" not in shown and "postgresql" not in shown
        assert "API_DATABASE_URL" in str(caught.value)
    finally:
        accounts.get_engine.cache_clear()


# ── RB-7: provisioning ───────────────────────────────────


def _app_privileges(engine, role):
    """(table, privilege) the role holds in app, plus schema and function rights."""
    with engine.connect() as conn:
        tables = conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'app'")).scalars().all()
        held = {
            (t, p) for t in tables for p in ("SELECT", "INSERT", "UPDATE", "DELETE",
                                             "TRUNCATE", "REFERENCES", "TRIGGER")
            if conn.execute(text("SELECT has_table_privilege(:r, :t, :p)"),
                            {"r": role, "t": f"app.{t}", "p": p}).scalar()
        }
        for p in ("USAGE", "CREATE"):
            if conn.execute(text("SELECT has_schema_privilege(:r, 'app', :p)"),
                            {"r": role, "p": p}).scalar():
                held.add(("schema", p))
        if conn.execute(text("SELECT has_function_privilege(:r, 'app.email_is_registered(text)',"
                             " 'EXECUTE')"), {"r": role}).scalar():
            held.add(("email_is_registered", "EXECUTE"))
    return held


EXPECTED_APP = {(t, p) for t in APP_TABLES for p in APP_WRITES} | {
    ("schema", "USAGE"), ("email_is_registered", "EXECUTE")}


def test_provision_api_role_never_touches_perennial(engine, pr22_roles):  # noqa: F811
    """RB-7a."""
    from test_read_api import _expected_readable, _readable

    api = pr22_roles["api"]
    assert _readable(engine, api) == _expected_readable(engine)
    provision_api(engine, pr22_roles)
    assert _readable(engine, api) == _expected_readable(engine)
    assert _app_privileges(engine, api) == EXPECTED_APP


def test_provision_api_role_requires_the_role_and_changes_nothing(engine):
    """RB-7b."""
    names = role_names()
    with engine.connect() as conn:
        before = conn.execute(text("SELECT nspowner FROM pg_namespace WHERE nspname = 'app'")).scalar()
    with pytest.raises(ValueError) as caught:
        provision_api(engine, names)
    assert names["api"] not in str(caught.value)
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM pg_roles WHERE rolname = :r"),
                            {"r": names["api"]}).scalar() == 0
        assert conn.execute(text("SELECT nspowner FROM pg_namespace WHERE nspname = 'app'")).scalar() == before


@pytest.mark.parametrize("order", ["roles_then_api", "api_then_grants"])
def test_either_order_and_reruns_leave_exact_grants(engine, order):
    """RB-7c (RU-6)."""
    from test_read_api import _expected_readable, _readable

    names = role_names()
    try:
        provision_roles(engine, names, "bootstrap")
        if order == "roles_then_api":
            provision_roles(engine, names, "grants")
            provision_api(engine, names)
        else:
            provision_api(engine, names)
            provision_roles(engine, names, "grants")
        provision_api(engine, names)
        provision_roles(engine, names, "grants")
        assert _readable(engine, names["api"]) == _expected_readable(engine)
        assert _app_privileges(engine, names["api"]) == EXPECTED_APP
    finally:
        drop_roles(engine, names.values())


@pytest.mark.parametrize("grant", [
    "GRANT SELECT ON app.profiles TO {role}",
    "GRANT SELECT (email) ON app.profiles TO {role}",
    "GRANT USAGE ON SCHEMA app TO {role}",
])
def test_provisioning_refuses_supabase_roles_that_reach_app(engine, pr22_roles, grant):  # noqa: F811
    """RB-7d, exposure."""
    suffix = secrets.token_hex(4)
    supabase = tuple(f"rb_{r}_{suffix}" for r in ("anon", "authenticated", "service_role"))
    try:
        with engine.begin() as conn:
            for name in supabase:
                conn.execute(text(f'CREATE ROLE "{name}" NOLOGIN'))
        provision_api(engine, pr22_roles, supabase_roles=supabase)  # nothing held yet
        with engine.connect() as conn:
            conn.execute(text(grant.format(role=f'"{supabase[0]}"')))
            conn.execute(text(f'REVOKE SELECT ON app.preferences FROM "{pr22_roles["api"]}"'))
            with pytest.raises(ValueError) as caught:
                ops = __import__("pr22_support").ops_module("provision_api_role")
                ops.provision(conn.connection.driver_connection, pr22_roles["api"],
                              owner=pr22_roles["owner"], supabase_roles=supabase)
            assert not any(n in str(caught.value) for n in supabase)
            assert not conn.execute(text(  # the canary: nothing was re-granted
                "SELECT has_table_privilege(:r, 'app.preferences', 'SELECT')"),
                {"r": pr22_roles["api"]}).scalar()
            conn.rollback()
    finally:
        drop_roles(engine, supabase)


def test_tables_the_owner_creates_in_app_later_are_not_readable(engine, pr22_roles):  # noqa: F811
    """RB-7d, default privileges (as RA-14b)."""
    with engine.begin() as conn:
        conn.execute(text(f'SET LOCAL ROLE "{pr22_roles["owner"]}"'))
        conn.execute(text("CREATE TABLE app.rb_future (id bigint)"))
    try:
        with engine.connect() as conn:
            for p in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                assert not conn.execute(text(
                    "SELECT has_table_privilege(:r, 'app.rb_future', :p)"),
                    {"r": pr22_roles["api"], "p": p}).scalar()
    finally:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE app.rb_future"))


def test_ownership_of_app_and_the_lookup_function(engine, pr22_roles):  # noqa: F811
    """RB-7f."""
    owner = pr22_roles["owner"]
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS auth"))
    try:
        provision_api(engine, pr22_roles)
        with engine.connect() as conn:
            admin = conn.execute(text("SELECT current_user")).scalar()
            assert conn.execute(text(
                "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'app'")).scalar() == owner
            owners = dict(conn.execute(text(
                "SELECT c.relname, pg_get_userbyid(c.relowner) FROM pg_class c"
                " WHERE c.relnamespace = 'app'::regnamespace AND c.relkind IN ('r', 'S')")).all())
            assert {t: owners[t] for t in APP_TABLES} == dict.fromkeys(APP_TABLES, owner)
            assert set(owners.values()) == {owner}  # sequences too
            assert conn.execute(text(
                "SELECT pg_get_userbyid(proowner) FROM pg_proc"
                " WHERE oid = 'app.email_is_registered(text)'::regprocedure")).scalar() == admin
            executors = set(conn.execute(text(
                "SELECT r.rolname FROM pg_roles r WHERE NOT r.rolsuper AND has_function_privilege("
                "r.oid, 'app.email_is_registered(text)', 'EXECUTE')"
                " AND r.rolname = ANY(:names)"), {"names": list(pr22_roles.values())}).scalars())
            assert executors == {pr22_roles["api"]}
            assert not conn.execute(text(
                "SELECT has_function_privilege('public', 'app.email_is_registered(text)', 'EXECUTE')")).scalar()
            for p in ("USAGE", "CREATE"):
                assert not conn.execute(text("SELECT has_schema_privilege(:r, 'auth', :p)"),
                                        {"r": owner, "p": p}).scalar()
    finally:
        with engine.begin() as conn:
            conn.execute(text("DROP SCHEMA IF EXISTS auth CASCADE"))


def test_pipeline_roles_have_nothing_in_app(engine, pr22_roles):  # noqa: F811
    """RB-7e."""
    for role in (pr22_roles["importer"], pr22_roles["reader"]):
        assert _app_privileges(engine, role) == set(), role


# ── RB-8: RLS, as the roles that matter ──────────────────


def _seed_users(engine, *users):
    with engine.begin() as conn:  # superuser: seed past RLS
        for user in users:
            conn.execute(text("INSERT INTO app.profiles (id, email, full_name)"
                              " VALUES (:i, :e, 'Person')"), {"i": user, "e": f"{user}@example.com"})
            conn.execute(text("INSERT INTO app.preferences (user_id) VALUES (:i)"), {"i": user})
            conn.execute(text("INSERT INTO app.notifications (user_id) VALUES (:i)"), {"i": user})


def test_two_users_are_isolated_through_the_api_and_directly(
    engine, accounts_api, pr22_roles  # noqa: F811
):
    """RB-8a."""
    client, provider, _ = accounts_api
    seed_securities(engine, "AAPL", "MSFT")
    first_body, first = register(client, provider)
    second_body, second = register(client, provider, "two@example.com")
    client.put("/v1/me/preferences", headers=first, json={"sectors": ["Technology"]})
    client.put("/v1/me/notifications", headers=first, json={"confidence_score_enabled": False})
    client.post("/v1/me/watchlist", headers=first, json={"ticker": "AAPL"})
    assert client.get("/v1/me/watchlist", headers=second).json() == []
    assert client.get("/v1/me/preferences", headers=second).json()["sectors"] == []
    b = second_body["user"]["id"]
    with as_role(engine, pr22_roles["api"], subject=b) as conn:
        for table, column in (("profiles", "id"), ("preferences", "user_id"),
                              ("notifications", "user_id"), ("watchlist", "user_id")):
            others = conn.execute(text(f"SELECT count(*) FROM app.{table} WHERE {column} <> :b"),
                                  {"b": b}).scalar()
            assert others == 0, table


def test_policies_in_the_catalog(engine, pr22_roles):  # noqa: F811
    """RB-8b."""
    provision_api(engine, pr22_roles)  # a re-run keeps exactly one own_rows per table
    api = pr22_roles["api"]
    with engine.connect() as conn:
        flags = dict(conn.execute(text(
            "SELECT relname, relrowsecurity AND relforcerowsecurity FROM pg_class"
            " WHERE relnamespace = 'app'::regnamespace AND relkind = 'r'")).all())
        policies = conn.execute(text(
            "SELECT tablename, policyname, roles, qual, with_check FROM pg_policies"
            " WHERE schemaname = 'app'")).all()
    assert all(flags[t] for t in APP_TABLES), flags
    assert sorted((p.tablename, p.policyname) for p in policies) == sorted(
        (t, "own_rows") for t in APP_TABLES)
    for p in policies:
        assert list(p.roles) == [api], (p.tablename, p.roles)  # never {public}
        if p.tablename in USER_TABLES:
            for expr in (p.qual.lower(), p.with_check.lower()):  # the catalog renders NULLIF
                assert "request.jwt.claim.sub" in expr and "nullif" in expr and "uuid" in expr
        else:
            assert p.qual == "true" and p.with_check == "true"


def test_no_or_empty_subject_sees_nothing_and_a_bad_subject_is_refused_first(
    engine, pr22_roles  # noqa: F811
):
    """RB-8c."""
    from src.api import accounts

    user = str(uuid4())
    _seed_users(engine, user)
    for subject in (None, ""):
        with as_role(engine, pr22_roles["api"], subject=subject) as conn:
            for table in ("profiles", "preferences", "notifications", "watchlist"):
                assert conn.execute(text(f"SELECT count(*) FROM app.{table}")).scalar() == 0
    statements = []
    listener = lambda *a: statements.append(a[2])  # noqa: E731
    event.listen(engine, "before_cursor_execute", listener)
    try:
        with pytest.raises(ValueError):
            with accounts.user_connection(engine, "not-a-uuid"):
                pass
        with pytest.raises(ValueError), engine.connect() as conn:
            accounts.sync_profile(conn, {"id": "not-a-uuid", "email": "x@example.com",
                                         "user_metadata": {}})
    finally:
        event.remove(engine, "before_cursor_execute", listener)
    assert not any("set_config" in s for s in statements)


def test_the_subject_never_carries_over_to_the_next_transaction(engine, pr22_roles):  # noqa: F811
    """RB-8d, on one pooled connection as the API login."""
    from src.api import accounts
    from test_read_api import login
    from tests.conftest import pinned_engine

    with login(engine, pr22_roles) as (api_engine, _, _):
        one = pinned_engine(api_engine.url, pool_size=1, max_overflow=0)
        try:
            user = str(uuid4())
            with accounts.user_connection(one, user) as conn:
                pid = conn.execute(text("SELECT pg_backend_pid()")).scalar()
                assert conn.execute(text(
                    "SELECT current_setting('request.jwt.claim.sub', true)")).scalar() == user
            with one.begin() as conn:
                assert conn.execute(text("SELECT pg_backend_pid()")).scalar() == pid
                assert conn.execute(text(
                    "SELECT coalesce(current_setting('request.jwt.claim.sub', true), '')")).scalar() == ""
        finally:
            one.dispose()


def test_pre_login_tables_are_open_only_to_the_api_role(engine, pr22_roles):  # noqa: F811
    """RB-8e: USING (true) for the API role only; the owner and others see nothing."""
    user = str(uuid4())
    _seed_users(engine, user)
    with engine.begin() as conn:  # superuser: seed past RLS
        conn.execute(text(
            "INSERT INTO app.auth_challenges (token_hash, email, flow, expires_at)"
            " VALUES (:h, 'x@example.com', 'login', now() + interval '1 hour')"),
            {"h": "a" * 64})
        conn.execute(text("INSERT INTO app.verified_sessions (session_id, user_id)"
                          " VALUES (:s, :u)"), {"s": str(uuid4()), "u": user})
        # A role other than the API role that somehow holds SELECT still sees nothing.
        conn.execute(text(f'GRANT USAGE ON SCHEMA app TO "{pr22_roles["reader"]}"'))
        for table in PRE_LOGIN_TABLES:
            conn.execute(text(f'GRANT SELECT ON app.{table} TO "{pr22_roles["reader"]}"'))
    for table in PRE_LOGIN_TABLES:
        with as_role(engine, pr22_roles["api"]) as conn:
            assert conn.execute(text(f"SELECT count(*) FROM app.{table}")).scalar() == 1, table
        for role in (pr22_roles["owner"], pr22_roles["reader"]):
            with as_role(engine, role) as conn:
                assert conn.execute(text(f"SELECT count(*) FROM app.{table}")).scalar() == 0, (role, table)


# ── RB-9, RB-10: tokens and logs ─────────────────────────


def test_bad_tokens_on_every_me_route_are_401_without_private_data(engine, accounts_api):
    """RB-9 (RU-1)."""
    from pr22_support import raise_limits

    client, provider, _ = accounts_api
    body, _ = register(client, provider)
    # Adapted (AH-3, limits): 4 headers on every /me route outrun the session bucket's
    # default burst of 30 from one client; the limit is raised, the dependency stays.
    raise_limits(client.app, session_burst=1000)
    email = body["user"]["email"]
    me_routes = [(p, m) for p, m in _account_routes() if p.startswith("/me")]
    assert me_routes
    for header in ({}, {"Authorization": "Bearer not.a.token"},
                   {"Authorization": "Bearer " + "e30." * 2 + "x"},
                   {"Authorization": "Basic dXNlcjpwYXNz"}):
        for path, method in me_routes:
            response = client.request(method, "/v1" + path.replace("{ticker}", "ZZAA"),
                                      headers=header, json={})
            assert response.status_code == 401, (method, path, header)
            assert email not in response.text


def test_tokens_in_query_strings_never_reach_the_logs(engine, pr22_roles, monkeypatch, capfd):  # noqa: F811
    """RB-10: the app logs JSON and no request line."""
    from fastapi.testclient import TestClient

    with integrated_app(engine, pr22_roles, monkeypatch) as (app, _, _):
        with TestClient(app) as client:
            capfd.readouterr()
            client.get("/v1/watchlist?access_token=synthetic-token-secret")
            client.post("/v1/auth/confirm-email?token=synthetic-token-secret", json={})
            logged = capfd.readouterr().err
    assert "synthetic-token-secret" not in logged
    for line in filter(None, logged.splitlines()):
        json.loads(line)


def test_a_startup_failure_logs_json_with_the_fixed_code(engine, pr22_roles, monkeypatch, capfd):  # noqa: F811
    """RB-10, startup clause (decision 31): JSON lines, the fixed code, no URL or driver text."""
    from fastapi.testclient import TestClient

    with integrated_app(engine, pr22_roles, monkeypatch, options="BYPASSRLS") as (app, user, pw):
        capfd.readouterr()
        with pytest.raises(Exception):
            with TestClient(app):
                pass
        logged = capfd.readouterr().err
    lines = [json.loads(line) for line in filter(None, logged.splitlines())]
    assert lines, "a refused start writes JSON log lines"
    shown = json.dumps(lines)
    assert "api_login_refused" in shown  # R1's fixed code
    for hidden in (user, pw, "postgresql", "psycopg"):
        assert hidden not in shown


# ── RB-7f: app.email_is_registered re-homed to the provisioning admin ─
# The auditor's option (a), security-m5-pr22-packet-report.md: provision_api_role
# runs ALTER FUNCTION ... OWNER TO CURRENT_USER, with five constraints.

LOOKUP = "app.email_is_registered(text)"


def _lookup(conn):
    return conn.execute(text(
        "SELECT pg_get_userbyid(proowner) AS owner, prosecdef, proconfig, provolatile,"
        " proacl::text[] AS acl FROM pg_proc WHERE oid = CAST(:f AS regprocedure)"), {"f": LOOKUP}).one()


def _api_module():
    from pr22_support import ops_module

    return ops_module("provision_api_role")


@pytest.mark.parametrize("group", ["owner", "importer", "reader", "api"])
def test_provisioning_refuses_to_run_as_any_group_role(engine, pr22_roles, group):  # noqa: F811
    """(1) The definer must never become a group role."""
    with engine.connect() as conn:
        before = _lookup(conn).owner
        conn.execute(text(f'SET ROLE "{pr22_roles[group]}"'))
        try:
            with pytest.raises(ValueError) as caught:
                _api_module().provision(
                    conn.connection.driver_connection, pr22_roles["api"],
                    owner=pr22_roles["owner"], importer=pr22_roles["importer"],
                    reader=pr22_roles["reader"])
            assert not any(n in str(caught.value) for n in pr22_roles.values())
        finally:
            conn.rollback()
            conn.execute(text("RESET ROLE"))
        assert _lookup(conn).owner == before


def test_the_lookup_is_a_pinned_definer_owned_by_the_admin(engine, pr22_roles):  # noqa: F811
    """(2) Shape after the ALTER: owner = current user, definer, empty search_path, STABLE."""
    provision_api(engine, pr22_roles)
    with engine.connect() as conn:
        admin = conn.execute(text("SELECT current_user")).scalar()
        row = _lookup(conn)
    assert row.owner == admin and row.prosecdef is True and row.provolatile == "s"
    assert any(re.fullmatch(r'search_path=("")?', c) for c in (row.proconfig or [])), row.proconfig


def test_an_unpinned_search_path_is_refused(engine, pr22_roles):  # noqa: F811
    """(2) A later definition that loosens search_path fails provisioning."""
    with engine.connect() as conn:
        original = conn.execute(text("SELECT pg_get_functiondef(CAST(:f AS regprocedure))"),
                                {"f": LOOKUP}).scalar()
    loosened = re.sub(r"\s*SET search_path TO ''[^\n]*\n", "\n", original)
    assert loosened != original
    try:
        with engine.begin() as conn:
            conn.execute(text(loosened))
        with pytest.raises(ValueError):
            provision_api(engine, pr22_roles)
    finally:
        with engine.begin() as conn:
            conn.execute(text(original))


def test_execute_is_exclusive_to_the_api_role(engine, pr22_roles):  # noqa: F811
    """(3) and (4): no other grantee, the owner included; PUBLIC revoked."""
    suffix = secrets.token_hex(4)
    supabase = tuple(f"rb_{r}_{suffix}" for r in ("anon", "authenticated", "service_role"))
    try:
        with engine.begin() as conn:
            for name in supabase:
                conn.execute(text(f'CREATE ROLE "{name}" NOLOGIN'))
        provision_api(engine, pr22_roles, supabase_roles=supabase)
        with engine.connect() as conn:
            admin = conn.execute(text("SELECT current_user")).scalar()
            grantees = {entry.split("=")[0] for entry in (_lookup(conn).acl or [])}
            for role in (pr22_roles["owner"], pr22_roles["importer"], pr22_roles["reader"],
                         *supabase):
                assert not conn.execute(text("SELECT has_function_privilege(:r, :f, 'EXECUTE')"),
                                        {"r": role, "f": LOOKUP}).scalar(), role
            assert conn.execute(text("SELECT has_function_privilege(:r, :f, 'EXECUTE')"),
                                {"r": pr22_roles["api"], "f": LOOKUP}).scalar()
        assert grantees <= {pr22_roles["api"], admin} and "" not in grantees  # no PUBLIC
    finally:
        drop_roles(engine, supabase)


def test_a_supabase_role_that_can_execute_an_app_function_is_refused(engine, pr22_roles):  # noqa: F811
    """(3) PostgREST exposes callable functions as RPC: part of the exposure refusal."""
    suffix = secrets.token_hex(4)
    supabase = tuple(f"rb_{r}_{suffix}" for r in ("anon", "authenticated", "service_role"))
    try:
        with engine.begin() as conn:
            for name in supabase:
                conn.execute(text(f'CREATE ROLE "{name}" NOLOGIN'))
            conn.execute(text(f'GRANT EXECUTE ON FUNCTION {LOOKUP} TO "{supabase[0]}"'))
        with pytest.raises(ValueError) as caught:
            provision_api(engine, pr22_roles, supabase_roles=supabase)
        assert not any(n in str(caught.value) for n in supabase)
    finally:
        drop_roles(engine, supabase)


def test_provisioning_re_homes_a_lookup_the_owner_migrated(engine, pr22_roles):  # noqa: F811
    """The migration ran as perennial_owner: the function starts owner-owned."""
    with engine.begin() as conn:
        conn.execute(text(f'ALTER FUNCTION {LOOKUP} OWNER TO "{pr22_roles["owner"]}"'))
    provision_api(engine, pr22_roles)
    with engine.connect() as conn:
        admin = conn.execute(text("SELECT current_user")).scalar()
        assert _lookup(conn).owner == admin
        assert not conn.execute(text("SELECT has_function_privilege(:r, :f, 'EXECUTE')"),
                                {"r": pr22_roles["owner"], "f": LOOKUP}).scalar()


def test_re_runs_are_idempotent_and_another_admin_takes_ownership(engine, pr22_roles):  # noqa: F811
    """(5) Same admin: no change. Another admin: ownership moves to it, accepted."""
    provision_api(engine, pr22_roles)
    with engine.connect() as conn:
        first = _lookup(conn)
    provision_api(engine, pr22_roles)
    with engine.connect() as conn:
        assert _lookup(conn) == first
    other = f"rb_admin_{secrets.token_hex(4)}"
    with engine.begin() as conn:
        conn.execute(text(f'CREATE ROLE "{other}" SUPERUSER NOLOGIN'))
    try:
        with engine.begin() as conn:  # SET LOCAL: the pooled connection never keeps the role
            conn.execute(text(f'SET LOCAL ROLE "{other}"'))
            _api_module().provision(conn.connection.driver_connection, pr22_roles["api"],
                                    owner=pr22_roles["owner"])
        with engine.connect() as conn:
            assert _lookup(conn).owner == other
    finally:
        provision_api(engine, pr22_roles)  # back to the test admin
        drop_roles(engine, [other])
