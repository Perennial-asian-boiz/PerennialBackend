"""#22 fix branch: the edge pass after the build (fix/pr22-main-merge @ 2211343).

Spec: docs/work-items/m5-pr22-fix-packet.md (FINAL at 596a68f), OPERATIONS.md and
ADR 0001. These pin edges the first pass (test_pr22_fix.py, test_pr22_restore.py)
did not: the column-level and TRUNCATE/REFERENCES/TRIGGER forms of the startup
check, provisioning's refusal for an owner that can reach auth, the downgrade
order with users present, the documented migrate-as-owner path, restoring a
backup from an older revision, Jaden's flows under the auth limit and with
expired tokens, the lazy module-level app, and the two locks agreeing.

Every RLS or privilege assertion runs as the role that matters, never as the
superuser test login; every role has a unique suffix and is dropped in finally.
"""

import base64
import inspect
import json
import re
import secrets
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from pr22_support import (
    APP_TABLES,
    account_client,
    drop_roles,
    integrated_app,
    ops_module,
    pr22_roles,  # noqa: F401  (pytest fixture)
    provision_api,
    provision_roles,
    register,
    role_names,
    seed_securities,
)
from test_pr22_fix import (
    EXPECTED_APP,
    _account_routes,
    _app_privileges,
    _app_revision,
    _forgot,
    _lookup,
    _refused,
    _script,
    _signup,
    _version,
    accounts_api,  # noqa: F401  (pytest fixture)
    extra_objects,  # noqa: F401  (pytest fixture)
)
from test_backup import keypair, loaded  # noqa: F401  (pytest fixtures)
from test_pr22_restore import _backup_as_admin, _database, _pg_args, _pg_env, _restore

pytestmark = pytest.mark.db

REPO = Path(__file__).resolve().parents[3]


def _session_role(conn, role):
    """A session-level role, committed, so Alembic opens its own transaction as it."""
    conn.execute(text(f'SET ROLE "{role}"'))
    conn.commit()


def _reset_role(conn):
    conn.rollback()
    conn.execute(text("RESET ROLE"))
    conn.commit()


# ── RB-6 / RA-15: the startup check's privilege forms ────


# The implementer's rule beyond the packet: no TRUNCATE, REFERENCES or TRIGGER on
# any app table, the six included.
TABLE_LEVEL_GRANTS = {
    "truncate_on_profiles": "GRANT TRUNCATE ON app.profiles TO {login}",
    "references_on_profiles": "GRANT REFERENCES ON app.profiles TO {login}",
    "trigger_on_watchlist": "GRANT TRIGGER ON app.watchlist TO {login}",
}


@pytest.mark.parametrize("case", sorted(TABLE_LEVEL_GRANTS))
def test_startup_refuses_truncate_references_and_trigger_on_app_tables(
    engine, pr22_roles, monkeypatch, caplog, case  # noqa: F811
):
    refused = _refused(engine, pr22_roles, monkeypatch, caplog,
                       grants=[TABLE_LEVEL_GRANTS[case]])
    reference = _refused(engine, pr22_roles, monkeypatch, caplog, options="BYPASSRLS")
    assert refused == reference  # R1's one fixed error


# A column privilege is still a write privilege (RB-6 bullets 1-2, RA-15 clause 3):
# INSERT (col) adds rows and UPDATE (col) changes them. has_table_privilege sees
# only table-level grants, so each is granted directly to an otherwise correct login.
COLUMN_LEVEL_GRANTS = {
    "perennial_column_update": "GRANT UPDATE (name) ON perennial.securities TO {login}",
    "app_column_insert_outside_the_six": "GRANT INSERT (id) ON app.rb_extra_SUFFIX TO {login}",
    "app_column_update_outside_the_six": "GRANT UPDATE (id) ON app.rb_extra_SUFFIX TO {login}",
    # The implementer's REFERENCES rule above, in its column form.
    "app_column_references": "GRANT REFERENCES (id) ON app.profiles TO {login}",
}


@pytest.mark.parametrize("case", sorted(COLUMN_LEVEL_GRANTS))
def test_startup_refuses_column_level_write_privileges(
    engine, pr22_roles, monkeypatch, caplog, extra_objects, case  # noqa: F811
):
    grant = COLUMN_LEVEL_GRANTS[case].replace("SUFFIX", extra_objects)
    refused = _refused(engine, pr22_roles, monkeypatch, caplog, grants=[grant])
    reference = _refused(engine, pr22_roles, monkeypatch, caplog, options="BYPASSRLS")
    assert refused == reference


# ── RB-7f: provisioning refuses an owner that can reach auth ─


@pytest.mark.parametrize("privilege", ["USAGE", "CREATE"])
def test_provisioning_refuses_an_owner_holding_anything_on_auth(
    engine, pr22_roles, privilege  # noqa: F811
):
    """'The owner role holds nothing on schema auth': refused before any change."""
    owner, api = pr22_roles["owner"], pr22_roles["api"]
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS auth"))
        conn.execute(text(f'GRANT {privilege} ON SCHEMA auth TO "{owner}"'))
    try:
        with engine.connect() as conn:
            conn.execute(text(f'REVOKE SELECT ON app.preferences FROM "{api}"'))  # the canary
            with pytest.raises(ValueError) as caught:
                ops_module("provision_api_role").provision(
                    conn.connection.driver_connection, api, owner=owner)
            assert "auth" in str(caught.value)
            assert not any(n in str(caught.value) for n in pr22_roles.values())
            assert not conn.execute(text(  # nothing was re-granted
                "SELECT has_table_privilege(:r, 'app.preferences', 'SELECT')"), {"r": api}).scalar()
            conn.rollback()
    finally:
        with engine.begin() as conn:
            conn.execute(text("DROP SCHEMA IF EXISTS auth CASCADE"))


# ── RB-2d: the downgrade order with users present ────────


def test_a_refused_downgrade_leaves_0012_and_the_lookup_in_place(engine, pr22_roles):  # noqa: F811
    """Downgrading past the app migration runs 0012's drop first; 0011's refusal
    must take it back too. Alembic opens its own transaction, as the CLI does, and
    nothing is repaired before the checks."""
    from alembic import command

    from tests.conftest import alembic_config

    _, down = _app_revision()
    with engine.begin() as conn:  # superuser: populate past RLS
        conn.execute(text("INSERT INTO app.profiles (id, email, full_name)"
                          " VALUES (:i, 'one@example.com', 'One')"), {"i": str(uuid4())})
    with engine.connect() as conn:
        head, before = _version(conn), _lookup(conn)
        conn.commit()
        _session_role(conn, pr22_roles["owner"])
        try:
            with pytest.raises((RuntimeError, DBAPIError)):
                command.downgrade(alembic_config(conn), down)
        finally:
            _reset_role(conn)
        try:
            assert _version(conn) == head
            assert _lookup(conn) == before  # same owner, definer settings and ACL
            assert conn.execute(text(
                "SELECT count(*) FROM pg_policies WHERE schemaname = 'app'")).scalar() == len(APP_TABLES)
        finally:
            if _version(conn) != head:  # leave the shared database at head for later tests
                command.upgrade(alembic_config(conn), "head")
                conn.commit()
                provision_api(engine, pr22_roles)


# ── The documented deploy order (OPERATIONS.md, README:329) ─


def _upgrade_as_the_owner_then_provision(scratch, names):
    """`alembic upgrade head` as the owner (the migration login's session role), then
    --phase grants and provision_api_role.py; the API ends exactly provisioned."""
    from alembic import command

    from tests.conftest import alembic_config

    with scratch.connect() as conn:
        _session_role(conn, names["owner"])
        try:
            command.upgrade(alembic_config(conn), "head")
        finally:
            _reset_role(conn)
        assert _version(conn) == _script().get_current_head()
    provision_roles(scratch, names, "grants")
    provision_api(scratch, names)
    with scratch.connect() as conn:
        assert conn.execute(text(
            "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'app'"
        )).scalar() == names["owner"]
    assert _app_privileges(scratch, names["api"]) == EXPECTED_APP


def _deployed_at_0010(engine, scratch, names):
    """Main's deployment today: bootstrapped and granted by today's code (so no
    schema app), migrated to 0010."""
    from alembic import command

    from tests.conftest import alembic_config

    provision_roles(scratch, names, "bootstrap")
    with scratch.connect() as conn:
        command.upgrade(alembic_config(conn), "0010")  # however 0001-0010 were applied
        conn.commit()
    provision_roles(scratch, names, "grants")
    with scratch.begin() as conn:  # newer provisioning makes an (empty) app; today's doesn't
        conn.execute(text("DROP SCHEMA IF EXISTS app"))


def test_mo2_a_deployment_at_0010_rebootstraps_then_upgrades_as_the_owner(engine):
    """MO-2 (decision 37, option 2): re-running bootstrap, then upgrade head as the
    owner, then the grants, gives MO-1's end state."""
    names = role_names("rd")
    try:
        with _database(engine) as (_, scratch):
            _deployed_at_0010(engine, scratch, names)
            provision_roles(scratch, names, "bootstrap")  # before every upgrade
            _upgrade_as_the_owner_then_provision(scratch, names)
    finally:
        drop_roles(engine, names.values())


def test_mo2_without_rebootstrap_the_owners_upgrade_fails_and_stays_at_0010(engine):
    """MO-2: skip the bootstrap and the upgrade fails at 0011, in one transaction."""
    from alembic import command

    from tests.conftest import alembic_config

    names = role_names("rd")
    try:
        with _database(engine) as (_, scratch):
            _deployed_at_0010(engine, scratch, names)
            with scratch.connect() as conn:
                _session_role(conn, names["owner"])
                try:
                    with pytest.raises(DBAPIError) as caught:
                        command.upgrade(alembic_config(conn), "head")
                finally:
                    _reset_role(conn)
                assert caught.value.orig.sqlstate == "42501"  # permission denied
                assert _version(conn) == "0010"
                assert conn.execute(text("SELECT to_regnamespace('app')")).scalar() is None
    finally:
        drop_roles(engine, names.values())


# ── RB-13: a backup from an older revision ───────────────


def _restore_order_section():
    doc = (REPO / "docs" / "technical-specs" / "OPERATIONS.md").read_text()
    start = doc.index("**Restore order.**")
    return doc[start:doc.index("**On Supabase", start)]


def test_the_restore_order_covers_a_backup_from_an_older_revision():
    """With --roles, a backup older than the code needs `alembic upgrade head` between
    pg_restore and the grants (ops' limit); the runbook's restore order must say so."""
    assert "upgrade head" in _restore_order_section()


@pytest.fixture(params=["0009", "0010"])
def older_dump(engine, tmp_path, request):
    """A dump (-Fc --no-acl --no-owner, as the superuser) of a database at an older
    revision: 0009 has fewer perennial tables, 0010 is main's head before #22."""
    from alembic import command

    from tests.conftest import alembic_config

    dump = tmp_path / f"{request.param}.dump"
    with _database(engine) as (source, scratch):
        with scratch.connect() as conn:
            command.upgrade(alembic_config(conn), request.param)
            conn.commit()
        subprocess.run(["pg_dump", "-Fc", "--no-acl", "--no-owner", "-f", str(dump),
                        *_pg_args(engine, source)], env=_pg_env(engine), check=True,
                       capture_output=True)
    return dump


def _release(scratch, names):
    """Nothing left in the scratch database that pins the roles."""
    with scratch.begin() as conn:
        admin = conn.execute(text("SELECT current_user")).scalar()
        for role in names.values():
            conn.execute(text(f'REASSIGN OWNED BY "{role}" TO "{admin}"'))
            conn.execute(text(f'DROP OWNED BY "{role}" CASCADE'))


def test_an_older_backup_fails_closed_in_the_documented_restore_order(engine, older_dump):
    """Roles, pg_restore, then grants and provision_api_role.py: one of them refuses,
    and no half-provisioned schema app is left behind."""
    names = role_names("rr")
    try:
        with _database(engine) as (target, scratch):
            provision_roles(scratch, names, "roles")
            assert _restore(engine, target, older_dump).returncode == 0
            try:
                with pytest.raises(Exception):
                    provision_roles(scratch, names, "grants")
                    provision_api(scratch, names)
                with scratch.connect() as conn:  # option 2's provisioning may leave app empty
                    assert conn.execute(text("SELECT count(*) FROM pg_class"
                                             " WHERE relnamespace = to_regnamespace('app')")).scalar() == 0
            finally:
                _release(scratch, names)
    finally:
        drop_roles(engine, names.values())


def test_an_older_backup_restores_with_upgrade_head_as_the_admin_before_grants(
    engine, older_dump
):
    """Roles, pg_restore, `alembic upgrade head` as the administrative login (the
    restored objects belong to the restorer), then grants and provision_api_role.py:
    the target ends exactly provisioned."""
    from alembic import command

    from tests.conftest import alembic_config

    names = role_names("rr")
    try:
        with _database(engine) as (target, scratch):
            provision_roles(scratch, names, "roles")
            assert _restore(engine, target, older_dump).returncode == 0
            try:
                with scratch.connect() as conn:
                    admin = conn.execute(text("SELECT current_user")).scalar()
                    command.upgrade(alembic_config(conn), "head")
                    conn.commit()
                provision_roles(scratch, names, "grants")
                provision_api(scratch, names)
                with scratch.connect() as conn:
                    assert _version(conn) == _script().get_current_head()
                    owners = set(conn.execute(text(
                        "SELECT pg_get_userbyid(c.relowner) FROM pg_class c"
                        " WHERE c.relnamespace IN ('perennial'::regnamespace, 'app'::regnamespace)"
                        " AND c.relkind IN ('r', 'S')")).scalars())
                    assert owners == {names["owner"]}
                    assert conn.execute(text(
                        "SELECT pg_get_userbyid(proowner) FROM pg_proc"
                        " WHERE oid = 'app.email_is_registered(text)'::regprocedure")).scalar() == admin
                    assert set(conn.execute(text(
                        "SELECT DISTINCT unnest(roles) FROM pg_policies WHERE schemaname = 'app'"
                    )).scalars()) == {names["api"]}
                assert _app_privileges(scratch, names["api"]) == EXPECTED_APP
            finally:
                _release(scratch, names)
    finally:
        drop_roles(engine, names.values())


# ── RB-5d: the auth limit comes before validation ────────


def test_a_limited_enumeration_request_is_429_even_with_an_invalid_body(
    engine, pr22_roles, monkeypatch  # noqa: F811
):
    """The bucket is spent before validation: an empty bucket answers 429, never a
    422 that reflects the body, and calls neither the lookup nor Supabase."""
    env = {"API_AUTH_RATE_LIMIT_BURST": "1", "API_AUTH_RATE_LIMIT_PER_HOUR": "1"}
    with integrated_app(engine, pr22_roles, monkeypatch, env=env) as (app, _, _), \
            account_client(app, monkeypatch) as (client, provider, counting):
        assert _signup(client, "a@example.com").status_code != 429
        calls = []
        provider.signup = lambda *a, **k: calls.append("signup")
        provider.send_code = lambda *a, **k: calls.append("send_code")
        lookups = counting.lookups
        for response in (
            client.post("/v1/auth/signup", json={"email": "rb-not-an-email", "password": "x"}),
            client.post("/v1/auth/forgot-password", json={"email": "rb-not-an-email"}),
            client.post("/v1/auth/signup", json={}),
        ):
            assert response.status_code == 429, response.text
            assert response.json()["code"] == "rate_limited"
            assert re.fullmatch(r"[1-9][0-9]*", response.headers["retry-after"])
            assert "rb-not-an-email" not in response.text
        assert (counting.lookups, calls) == (lookups, [])
        assert _forgot(client, "b@example.com").status_code == 429


# ── RB-9: expired tokens through the real Supabase client ─


SUPABASE_URL = "https://rb-synthetic.supabase.co"


def _jwt(claims):
    def part(value):
        return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()

    return f"{part({'alg': 'HS256', 'typ': 'JWT'})}.{part(claims)}.c3ludGhldGlj"


class _Response:
    def __init__(self, status, body):
        self.status_code, self._body = status, body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def json(self):
        return self._body


@pytest.fixture
def supabase_session(engine, pr22_roles, monkeypatch):  # noqa: F811
    """The integrated app with the real SupabaseAuth; its /user endpoint is faked.

    Yields (client, make_token, user_answer): make_token(exp) signs nothing (the
    real /user call is what validates a token), and user_answer["status"] picks
    what /user returns. One user, with a profile and an approved session.
    """
    from fastapi.testclient import TestClient

    from src.api import accounts, supabase_auth

    user_id, session_id = str(uuid4()), str(uuid4())
    with engine.begin() as conn:  # superuser: seed past RLS
        conn.execute(text("INSERT INTO app.profiles (id, email, full_name)"
                          " VALUES (:i, 'rb-private@example.com', 'Private Person')"),
                     {"i": user_id})
        conn.execute(text("INSERT INTO app.verified_sessions (session_id, user_id)"
                          " VALUES (:s, :u)"), {"s": session_id, "u": user_id})
    user_answer = {"status": 200}
    user = {"id": user_id, "email": "rb-private@example.com",
            "email_confirmed_at": "2026-10-24T00:00:00Z", "user_metadata": {}}

    def fake_request(method, url, **kwargs):
        assert url == f"{SUPABASE_URL}/auth/v1/user", url  # nothing else is called
        if user_answer["status"] == 200:
            return _Response(200, user)
        return _Response(user_answer["status"],
                         {"error_code": "bad_jwt", "msg": "invalid JWT: token is expired"})

    def make_token(exp):
        return _jwt({"iss": f"{SUPABASE_URL}/auth/v1", "aud": "authenticated", "exp": exp,
                     "role": "authenticated", "sub": user_id, "session_id": session_id})

    monkeypatch.setattr(supabase_auth.requests, "request", fake_request)
    with integrated_app(engine, pr22_roles, monkeypatch) as (app, _, _):
        app.dependency_overrides[accounts.get_provider] = lambda: supabase_auth.SupabaseAuth(
            SUPABASE_URL, "synthetic-publishable-key")
        try:
            with TestClient(app) as client:
                yield client, make_token, user_answer
        finally:
            app.dependency_overrides.clear()


@pytest.mark.parametrize("expiry", ["rejected_by_supabase", "expired_locally"])
def test_expired_tokens_are_401_on_every_me_route(supabase_session, expiry):
    """Supabase answers 403 bad_jwt for an expired token; and a token whose exp has
    passed is refused even if /user still answers (clock skew)."""
    client, make_token, user_answer = supabase_session
    expired = {"Authorization": "Bearer " + make_token(int(time.time()) - 60)}
    user_answer["status"] = 403 if expiry == "rejected_by_supabase" else 200
    me_routes = [(p, m) for p, m in _account_routes() if p.startswith("/me")]
    assert me_routes
    for path, method in me_routes:
        response = client.request(method, "/v1" + path.replace("{ticker}", "ZZAA"),
                                  headers=expired, json={})
        assert response.status_code == 401, (method, path, response.text)
        assert "rb-private@example.com" not in response.text
        assert "Private Person" not in response.text
        assert "expired" not in response.text.lower()  # no provider text relayed


def test_a_current_token_is_served_by_the_same_setup(supabase_session):
    """The control for the expired cases: same user, session and fake, future exp."""
    client, make_token, user_answer = supabase_session
    user_answer["status"] = 200
    current = {"Authorization": "Bearer " + make_token(int(time.time()) + 600)}
    me = client.get("/v1/me", headers=current)
    assert me.status_code == 200, me.text
    assert me.json()["email"] == "rb-private@example.com"
    assert client.get("/v1/me/watchlist", headers=current).json() == []


# ── RB-3 and decision 4: one watchlist per user ──────────


def test_a_users_delete_never_touches_another_users_watchlist(engine, accounts_api):  # noqa: F811
    client, provider, _ = accounts_api
    seed_securities(engine, "AAPL", "MSFT")
    _, first = register(client, provider)
    _, second = register(client, provider, "two@example.com")
    for headers, ticker in ((first, "AAPL"), (second, "AAPL"), (second, "MSFT")):
        assert client.post("/v1/me/watchlist", headers=headers,
                           json={"ticker": ticker}).status_code == 201
    both = [{"ticker": "AAPL"}, {"ticker": "MSFT"}]
    assert client.delete("/v1/me/watchlist/MSFT", headers=first).status_code == 204  # only second's
    assert client.get("/v1/me/watchlist", headers=first).json() == [{"ticker": "AAPL"}]
    assert client.get("/v1/me/watchlist", headers=second).json() == both
    assert client.delete("/v1/me/watchlist/AAPL", headers=first).status_code == 204  # both watch it
    assert client.get("/v1/me/watchlist", headers=first).json() == []
    assert client.get("/v1/me/watchlist", headers=second).json() == both
    with engine.connect() as conn:  # superuser, counting stored rows only
        assert conn.execute(text("SELECT count(*) FROM app.watchlist")).scalar() == 2


# ── RB-3c: a malformed ticker on DELETE (was a 500) ──────


@pytest.mark.parametrize("ticker", [
    "BAD%20TICKER", "A$B", "A" * 21, "-AAPL", "%E2%80%8BAAPL", "AA%00PL",
])
def test_deleting_a_malformed_ticker_is_422_and_changes_nothing(engine, accounts_api, ticker):  # noqa: F811
    client, provider, _ = accounts_api
    seed_securities(engine, "AAPL")
    _, headers = register(client, provider)
    assert client.post("/v1/me/watchlist", headers=headers, json={"ticker": "AAPL"}).status_code == 201
    response = client.delete(f"/v1/me/watchlist/{ticker}", headers=headers)
    assert response.status_code == 422, response.text
    assert ticker not in response.text and "BAD TICKER" not in response.text
    assert client.delete(f"/v1/me/watchlist/{ticker}").status_code == 401  # login first
    assert client.get("/v1/me/watchlist", headers=headers).json() == [{"ticker": "AAPL"}]


# ── RB-6b: the lazy module-level app ─────────────────────


def test_the_module_level_app_is_built_on_first_use_and_needs_the_url(monkeypatch):
    """`uvicorn src.api.app:app` (python -m src.api) resolves the app lazily: importing
    builds nothing, and resolving it without API_DATABASE_URL refuses with fixed text."""
    import importlib

    from uvicorn.importer import import_from_string

    from src.api import accounts

    module = importlib.import_module("src.api.app")
    assert "app" not in vars(module)  # nothing was built at import time
    monkeypatch.delenv("API_DATABASE_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://owner:synthetic-pw@localhost/perennial_test")
    accounts.get_engine.cache_clear()
    try:
        with pytest.raises(accounts.APIDatabaseURLMissing) as caught:
            import_from_string("src.api.app:app")
        assert "API_DATABASE_URL" in str(caught.value)
        assert "synthetic-pw" not in str(caught.value) and "postgresql" not in str(caught.value)
        with pytest.raises(AttributeError):
            module.not_an_app
    finally:
        accounts.get_engine.cache_clear()


def test_the_module_level_app_uses_api_database_url_and_the_api_role(monkeypatch):
    from fastapi import FastAPI
    from uvicorn.importer import import_from_string

    from src.api import accounts

    monkeypatch.setenv(
        "API_DATABASE_URL", "postgresql+psycopg://rb_api_login:synthetic-pw@127.0.0.1:5432/perennial_test")
    accounts.get_engine.cache_clear()
    try:
        app = import_from_string("src.api.app:app")
        assert isinstance(app, FastAPI)
        assert app.state.engine.url.username == "rb_api_login"
        assert app.state.api_role == "perennial_api"
        app.state.engine.dispose()
    finally:
        accounts.get_engine.cache_clear()


# ── ADR 0001: tests run what production runs ─────────────


def _locked(path):
    """{package: (version, hashes)} from a pip-compile --generate-hashes lock."""
    found = {}
    pattern = (r"(?m)^([A-Za-z0-9][A-Za-z0-9_.\-]*)(?:\[[^\]]*\])?==(\S+) \\\n"
               r"((?:[ \t]+--hash=sha256:[0-9a-f]{64}(?: \\)?\n)+)")
    for match in re.finditer(pattern, (REPO / path).read_text()):
        found[match.group(1).lower()] = (
            match.group(2), frozenset(re.findall(r"sha256:([0-9a-f]{64})", match.group(3))))
    return found


def test_the_dev_lock_pins_every_runtime_package_at_the_runtime_version():
    """The suite installs requirements-dev.lock; production installs requirements.lock.
    Every runtime package (starlette 1.9.0 under fastapi included) must be the same
    version with the same hashes in both, or the tests exercise other code."""
    runtime, dev = _locked("requirements.lock"), _locked("requirements-dev.lock")
    assert "starlette" in runtime and "fastapi" in runtime
    assert {name: (pin, dev.get(name)) for name, pin in runtime.items() if dev.get(name) != pin} == {}


# ── Round 2: fixes from the three #22 reviews ────────────
# PR22-1 (checker): /api/watchlist is public; in publication mode (production's
# default) it reads publications on the API pool, so it gets R1's protections.


@contextmanager
def _counting_statements(engine):
    statements = []
    listener = lambda *a: statements.append(a[2])  # noqa: E731
    event.listen(engine, "before_cursor_execute", listener)
    try:
        yield statements
    finally:
        event.remove(engine, "before_cursor_execute", listener)


def test_the_dashboard_route_is_charged_to_the_read_bucket(engine, pr22_roles, monkeypatch):  # noqa: F811
    """PR22-1: read.rate_limit's bucket, shared with /v1; a 429 does no database work."""
    from fastapi.testclient import TestClient

    env = {"API_RATE_LIMIT_BURST": "2", "API_RATE_LIMIT_PER_MINUTE": "1",
           "DASHBOARD_DATA_MODE": "publication"}
    with integrated_app(engine, pr22_roles, monkeypatch, env=env) as (app, _, _), \
            TestClient(app) as client:
        assert client.get("/v1/watchlist").status_code == 404  # no publication: one token
        assert client.get("/api/watchlist").status_code == 200  # the second token
        with _counting_statements(app.state.engine) as statements:
            limited = client.get("/api/watchlist")
        assert limited.status_code == 429, limited.text
        assert limited.json()["code"] == "rate_limited"
        assert re.fullmatch(r"[1-9][0-9]*", limited.headers["retry-after"])
        assert statements == []  # limited before any database work
        assert client.get("/v1/watchlist").status_code == 429  # the same bucket, still empty


def test_the_dashboard_route_reads_inside_one_read_only_transaction(
    engine, pr22_roles, monkeypatch  # noqa: F811
):
    """PR22-1, as RB-5c and RA-16b: through read_transaction exactly once, where a write
    fails with 25006. The probe writes to a table the API role may write (inside a
    savepoint, so the route carries on): outside a read-only transaction it would land."""
    from fastapi.testclient import TestClient

    from src.api import app as app_module
    from src.api import read

    real, outcomes = read.read_transaction, []

    @contextmanager
    def probing(*args, **kwargs):
        with real(*args, **kwargs) as conn:
            try:
                with conn.begin_nested():
                    conn.execute(text(
                        "INSERT INTO app.auth_challenges (token_hash, email, flow, expires_at)"
                        " VALUES (:h, 'probe@example.com', 'login', now() + interval '1 hour')"),
                        {"h": "b" * 64})
            except DBAPIError as error:
                outcomes.append(error.orig.sqlstate)
            else:
                outcomes.append("wrote")
            yield conn

    env = {"DASHBOARD_DATA_MODE": "publication"}
    with integrated_app(engine, pr22_roles, monkeypatch, env=env) as (app, _, _), \
            TestClient(app) as client:
        for module in (read, app_module):  # however app.py names the helper
            if hasattr(module, "read_transaction"):
                monkeypatch.setattr(module, "read_transaction", probing)
        assert client.get("/api/watchlist").status_code == 200
    assert outcomes == ["25006"]
    with engine.connect() as conn:  # superuser: nothing was written
        assert conn.execute(text("SELECT count(*) FROM app.auth_challenges")).scalar() == 0


@pytest.mark.parametrize("bucket", ["read", "auth"])
def test_browsers_can_read_retry_after_on_a_429(engine, pr22_roles, monkeypatch, bucket):  # noqa: F811
    """Auditor note 5: Retry-After is in CORS expose_headers, so a cross-origin
    client can read the 429's hint."""
    origin = "https://app.example.test"
    env = {"CORS_ORIGINS": origin, "API_RATE_LIMIT_BURST": "1", "API_RATE_LIMIT_PER_MINUTE": "1",
           "API_AUTH_RATE_LIMIT_BURST": "1", "API_AUTH_RATE_LIMIT_PER_HOUR": "1"}
    with integrated_app(engine, pr22_roles, monkeypatch, env=env) as (app, _, _), \
            account_client(app, monkeypatch) as (client, _, _):
        if bucket == "read":
            send = lambda: client.get("/v1/watchlist", headers={"Origin": origin})  # noqa: E731
        else:
            send = lambda: client.post("/v1/auth/forgot-password", headers={"Origin": origin},  # noqa: E731
                                       json={"email": "one@example.com"})
        send()
        limited = send()
        assert limited.status_code == 429
        assert limited.headers.get("access-control-allow-origin") == origin
        exposed = limited.headers.get("access-control-expose-headers", "")
        assert "retry-after" in [h.strip().lower() for h in exposed.split(",")], exposed


def test_the_drill_passes_every_group_name_to_provision_api_role(
    engine, pr22_roles, loaded, keypair, tmp_path, monkeypatch  # noqa: F811
):
    """Auditor note 4, checker nit: with --roles, provision_api_role's group-role and
    duplicate-name checks see the drill's importer and reader, not the defaults."""
    from restore_drill import run_backup_drill

    store = _backup_as_admin(engine, loaded, keypair, tmp_path)
    names = role_names("rd")
    ops = ops_module("provision_api_role")
    real, seen = ops.provision, []

    def recording(*args, **kwargs):
        bound = inspect.signature(real).bind(*args, **kwargs)
        bound.apply_defaults()
        seen.append({k: v for k, v in bound.arguments.items() if k != "conn"})
        return real(*args, **kwargs)

    monkeypatch.setattr(ops, "provision", recording)
    try:
        drill = run_backup_drill(loaded, store, keypair / "key.pem", tmp_path / "drill",
                                 roles=(names["owner"], names["importer"], names["reader"],
                                        names["api"]))
        assert drill["verified"] is True
        assert len(seen) == 1
        assert {k: seen[0][k] for k in ("role", "owner", "importer", "reader")} == {
            "role": names["api"], "owner": names["owner"],
            "importer": names["importer"], "reader": names["reader"]}
    finally:
        drop_roles(engine, names.values())


# ── RB-6, completed: a write through any object in app ───
# As R1's RA-15 follow-up (test_read_api_write_objects.py): _APP counts tables
# (r, p) only. Already refused by #22's check, so not repeated here: TRUNCATE,
# REFERENCES and TRIGGER on app tables (test_startup_refuses_truncate_...).


@pytest.fixture
def app_write_objects(engine):
    """A standalone sequence, an auto-updatable view over app.profiles and a foreign
    table, all in app and outside the six; made by the superuser test login."""
    suffix = secrets.token_hex(4)
    objects = {"sequence": f"app.rb_seq_{suffix}", "view": f"app.rb_view_{suffix}",
               "foreign": f"app.rb_foreign_{suffix}"}
    wrapper = f"rb_fdw_{suffix}"
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SEQUENCE {objects['sequence']}"))
        conn.execute(text(  # the view's owner bypasses RLS: its writes reach every row
            f"CREATE VIEW {objects['view']} AS SELECT id, email, full_name FROM app.profiles"))
        conn.execute(text(f"CREATE FOREIGN DATA WRAPPER {wrapper}"))  # no handler needed
        conn.execute(text(f"CREATE SERVER {wrapper}_server FOREIGN DATA WRAPPER {wrapper}"))
        conn.execute(text(
            f"CREATE FOREIGN TABLE {objects['foreign']} (id bigint) SERVER {wrapper}_server"))
    try:
        yield objects
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP VIEW IF EXISTS {objects['view']}"))
            conn.execute(text(f"DROP SEQUENCE IF EXISTS {objects['sequence']}"))
            conn.execute(text(f"DROP FOREIGN DATA WRAPPER IF EXISTS {wrapper} CASCADE"))


APP_OBJECT_WRITES = {
    "sequence_update": "GRANT UPDATE ON SEQUENCE {sequence} TO {login}",  # setval
    "sequence_usage": "GRANT USAGE ON SEQUENCE {sequence} TO {login}",  # nextval
    "view_update": "GRANT UPDATE ON {view} TO {login}",
    "view_column_update": "GRANT UPDATE (email) ON {view} TO {login}",
    "foreign_table_insert": "GRANT INSERT ON {foreign} TO {login}",
}


@pytest.mark.parametrize("case", sorted(APP_OBJECT_WRITES))
def test_startup_refuses_a_write_through_any_app_object_outside_the_six(
    engine, pr22_roles, monkeypatch, caplog, app_write_objects, case  # noqa: F811
):
    grant = APP_OBJECT_WRITES[case]
    for key, name in app_write_objects.items():
        grant = grant.replace("{" + key + "}", name)
    refused = _refused(engine, pr22_roles, monkeypatch, caplog, grants=[grant])
    reference = _refused(engine, pr22_roles, monkeypatch, caplog, options="BYPASSRLS")
    assert refused == reference


def test_app_objects_alone_do_not_refuse_startup(
    engine, pr22_roles, monkeypatch, app_write_objects  # noqa: F811
):
    """The control: the objects exist and the login holds nothing on them; a correctly
    provisioned login still starts. (It used to grant SELECT on the sequence too; the
    final rule set refuses any privilege on an app relation outside the six.)"""
    from fastapi.testclient import TestClient

    with integrated_app(engine, pr22_roles, monkeypatch) as (app, _, _), TestClient(app) as client:
        assert client.get("/health").status_code == 200


# ── RB-6 / RA-15, the final rule set (both reviewers) ────
# The API login holds exactly what provisioning grants, on the objects it grants: no
# privilege at all on an app relation outside the six, or on a view, materialized
# view or foreign table in perennial; no EXECUTE on a perennial or app function but
# app.email_is_registered (pg_catalog and other schemas are out of scope); no
# REFERENCES on perennial.


@pytest.fixture
def owner_objects(engine):
    """Superuser-owned objects, in app outside the six and in perennial."""
    suffix = secrets.token_hex(4)
    o = {"app_view": f"app.rb_view_{suffix}", "app_matview": f"app.rb_matview_{suffix}",
         "app_foreign": f"app.rb_foreign_{suffix}", "app_sequence": f"app.rb_seq_{suffix}",
         "app_table": f"app.rb_table_{suffix}", "app_function": f"app.rb_function_{suffix}()",
         "perennial_view": f"perennial.rb_view_{suffix}",
         "perennial_function": f"perennial.rb_function_{suffix}()",
         "public_function": f"public.rb_function_{suffix}()"}
    wrapper = f"rb_fdw_{suffix}"
    with engine.begin() as conn:
        conn.execute(text(f"CREATE VIEW {o['app_view']} AS SELECT id, email FROM app.profiles"))
        conn.execute(text(f"CREATE MATERIALIZED VIEW {o['app_matview']} AS SELECT id FROM app.profiles"))
        conn.execute(text(f"CREATE FOREIGN DATA WRAPPER {wrapper}"))
        conn.execute(text(f"CREATE SERVER {wrapper}_server FOREIGN DATA WRAPPER {wrapper}"))
        conn.execute(text(f"CREATE FOREIGN TABLE {o['app_foreign']} (id bigint) SERVER {wrapper}_server"))
        conn.execute(text(f"CREATE SEQUENCE {o['app_sequence']}"))
        conn.execute(text(f"CREATE TABLE {o['app_table']} (id int)"))
        conn.execute(text(f"CREATE VIEW {o['perennial_view']} AS SELECT id, source FROM perennial.source_batches"))
        for name in ("app_function", "perennial_function", "public_function"):
            conn.execute(text(f"CREATE FUNCTION {o[name]} RETURNS int LANGUAGE sql AS 'SELECT 1'"))
        for name in ("app_function", "perennial_function"):
            conn.execute(text(f"REVOKE EXECUTE ON FUNCTION {o[name]} FROM PUBLIC"))
    try:
        yield o
    finally:
        with engine.begin() as conn:
            for name in ("app_view", "perennial_view"):
                conn.execute(text(f"DROP VIEW IF EXISTS {o[name]}"))
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {o['app_matview']}"))
            conn.execute(text(f"DROP FOREIGN DATA WRAPPER IF EXISTS {wrapper} CASCADE"))
            conn.execute(text(f"DROP SEQUENCE IF EXISTS {o['app_sequence']}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {o['app_table']}"))
            for name in ("app_function", "perennial_function", "public_function"):
                conn.execute(text(f"DROP FUNCTION IF EXISTS {o[name]}"))


OWNER_PATHS = {
    "app_view_select": "GRANT SELECT ON {app_view} TO {login}",
    "app_matview_select": "GRANT SELECT ON {app_matview} TO {login}",
    "app_foreign_table_select": "GRANT SELECT ON {app_foreign} TO {login}",
    "app_sequence_select": "GRANT SELECT ON SEQUENCE {app_sequence} TO {login}",
    "app_other_table_select": "GRANT SELECT ON {app_table} TO {login}",
    "app_other_table_column_select": "GRANT SELECT (id) ON {app_table} TO {login}",
    "app_function_execute": "GRANT EXECUTE ON FUNCTION {app_function} TO {login}",
    "app_function_execute_through_public": "GRANT EXECUTE ON FUNCTION {app_function} TO PUBLIC",
    "perennial_view_select": "GRANT SELECT ON {perennial_view} TO {login}",
    "perennial_function_execute": "GRANT EXECUTE ON FUNCTION {perennial_function} TO {login}",
    "perennial_table_references": "GRANT REFERENCES ON perennial.securities TO {login}",
    "perennial_column_references": "GRANT REFERENCES (id) ON perennial.securities TO {login}",
}


@pytest.mark.parametrize("case", sorted(OWNER_PATHS))
def test_startup_refuses_a_path_through_another_owner(
    engine, pr22_roles, monkeypatch, caplog, owner_objects, case  # noqa: F811
):
    grant = OWNER_PATHS[case]
    for key, name in owner_objects.items():
        grant = grant.replace("{" + key + "}", name)
    refused = _refused(engine, pr22_roles, monkeypatch, caplog, grants=[grant])
    reference = _refused(engine, pr22_roles, monkeypatch, caplog, options="BYPASSRLS")
    assert refused == reference


def test_owner_objects_alone_and_a_function_elsewhere_do_not_refuse_startup(
    engine, pr22_roles, monkeypatch, owner_objects  # noqa: F811
):
    """Guards: the objects exist and the login holds nothing on them, and it can EXECUTE
    a function in public (PUBLIC's default), outside the rule's scope. Provisioning's own
    EXECUTE on app.email_is_registered is test_pr22_fix.py's guard."""
    from fastapi.testclient import TestClient

    with integrated_app(engine, pr22_roles, monkeypatch) as (app, _, _), TestClient(app) as client:
        with app.state.engine.connect() as conn:
            assert conn.execute(text(f"SELECT {owner_objects['public_function'][:-2]}()")).scalar() == 1
        assert client.get("/health").status_code == 200
