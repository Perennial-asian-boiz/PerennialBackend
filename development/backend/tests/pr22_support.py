"""Shared setup for the #22 fix-branch tests (docs/work-items/m5-pr22-fix-packet.md).

Every role is created with a unique suffix and dropped in `finally` (MR-1). The
integrated app always runs as a correctly provisioned API login read from
API_DATABASE_URL, never as the superuser test login, which bypasses RLS.
"""

import secrets
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

OPS = Path(__file__).resolve().parents[2] / "database" / "ops"
APP_TABLES = (
    "profiles",
    "preferences",
    "notifications",
    "watchlist",
    "auth_challenges",
    "verified_sessions",
)
USER_TABLES = APP_TABLES[:4]
PRE_LOGIN_TABLES = APP_TABLES[4:]


def ops_module(name):
    if str(OPS) not in sys.path:
        sys.path.insert(0, str(OPS))
    return __import__(name)


def role_names(prefix="rb"):
    suffix = secrets.token_hex(5)
    return {r: f"{prefix}_{r}_{suffix}" for r in ("owner", "importer", "reader", "api")}


def group_roles(names):
    return (names["owner"], names["importer"], names["reader"], names["api"])


def provision_roles(engine, names, *phases, **kwargs):
    with engine.begin() as conn:
        for phase in phases or ("bootstrap", "grants"):
            ops_module("provision_roles").provision(
                conn.connection.driver_connection, group_roles(names), phase=phase, **kwargs
            )


def provision_api(engine, names, **kwargs):
    with engine.begin() as conn:
        ops_module("provision_api_role").provision(
            conn.connection.driver_connection, names["api"], owner=names["owner"], **kwargs
        )


def drop_roles(engine, names):
    from test_read_api import _drop_roles

    _drop_roles(engine, list(names))


@pytest.fixture
def pr22_roles(engine):
    """R1's four groups, then #22's app grants and policies, all uniquely named."""
    names = role_names()
    try:
        provision_roles(engine, names)
        provision_api(engine, names)
        yield names
    finally:
        drop_roles(engine, names.values())


def api_url(engine, user, password):
    return engine.url.set(username=user, password=password).render_as_string(
        hide_password=False
    )


@contextmanager
def integrated_app(engine, names, monkeypatch, *, options="", members=("api",),
                   env=None, database=None, connect=False, grants=()):
    """The integrated #22 app, its engine taken from API_DATABASE_URL.

    `grants` are SQL templates run for the new login (`{login}`) before the app starts.
    """
    from src.api import accounts
    from test_read_api import login

    with login(engine, names, options=options, members=members, database=database,
               connect=connect) as (_, user, password):
        if grants:
            from sqlalchemy import text

            with engine.begin() as conn:
                for grant in grants:
                    conn.execute(text(grant.format(login=f'"{user}"')))
        url = api_url(engine, user, password)
        if database is not None:
            url = url.replace(f"/{engine.url.database}", f"/{database}")
        monkeypatch.setenv("API_DATABASE_URL", url)
        for key, value in (env or {}).items():
            monkeypatch.setenv(key, value)
        accounts.get_engine.cache_clear()
        from src.api.app import create_app

        app = create_app(api_role=names["api"])
        try:
            yield app, user, password
        finally:
            state_engine = getattr(app.state, "engine", None)
            if state_engine is not None:
                state_engine.dispose()
            accounts.get_engine.cache_clear()


def raise_limits(app, **limits):
    """Raise one app's rate limits, for a test that needs more requests than a default
    bucket allows (the AH packet's "Limits" adaptation). Fields the build doesn't have
    yet are skipped, so the same test runs before and after the new buckets."""
    import dataclasses

    from src.api.rate_limit import RateLimiter

    config = app.state.rate_limiter.config
    names = {f.name for f in dataclasses.fields(config)}
    app.state.rate_limiter = RateLimiter(
        dataclasses.replace(config, **{k: v for k, v in limits.items() if k in names}))


class Counting:
    """Counts calls to the enumeration oracle and the auth provider."""

    def __init__(self):
        self.lookups = 0


@contextmanager
def account_client(app, monkeypatch, provider=None, counting=None):
    from fastapi.testclient import TestClient

    from src.api import accounts
    from test_supabase_accounts import FakeProvider

    provider = provider or FakeProvider()
    counting = counting or Counting()

    def lookup(_connection, email):
        counting.lookups += 1
        return email in provider.users

    monkeypatch.setattr(accounts, "email_is_registered", lookup)
    app.dependency_overrides[accounts.get_provider] = lambda: provider
    try:
        with TestClient(app) as client:
            yield client, provider, counting
    finally:
        app.dependency_overrides.clear()


def register(client, provider, email="one@example.com"):
    """Jaden's register(), on the /v1 paths; returns the auth headers."""
    response = client.post(
        "/v1/auth/signup",
        json={"email": email, "full_name": "Test Person", "password": "Password123"},
    )
    assert response.status_code == 201, response.text
    link = provider.verify_code(email, "123456", "signup")
    confirmed = client.post("/v1/auth/confirm-email",
                            json={"access_token": link["access_token"]})
    assert confirmed.status_code == 200, confirmed.text
    challenge = client.post(
        "/v1/auth/login", json={"email": email, "password": "Password123"}
    ).json()
    verified = client.post(
        "/v1/auth/verify-otp",
        json={"email": email, "challenge_id": challenge["challenge_id"], "code": "123456"},
    )
    assert verified.status_code == 200, verified.text
    body = verified.json()
    return body, {"Authorization": "Bearer " + body["access_token"]}


def seed_securities(engine, *symbols, aliases=None):
    """Securities with canonical ticker aliases (plus extra aliases)."""
    from sqlalchemy import insert

    from src.db import models

    ids = {}
    with engine.begin() as conn:
        for symbol in symbols:
            sid = conn.execute(
                insert(models.securities).values(symbol=symbol).returning(models.securities.c.id)
            ).scalar_one()
            ids[symbol] = sid
            for value in (symbol, *(aliases or {}).get(symbol, ())):
                conn.execute(insert(models.security_identifiers).values(
                    scheme="ticker", value=value, security_id=sid))
    return ids
