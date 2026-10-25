"""#22 auth hardening (decision 36): Part 1 of the planner's packet, AH-1a to AH-8b.

Spec: docs/work-items/m5-pr22-auth-hardening-packet.md (docs/m5-pr22-fix-packet @
3fd4e3c, with the S1 amendment), written spec-first on fix/pr22-main-merge @ 0c2f78a. Everything runs
against the integrated app with a fake provider (Jaden's FakeProvider, recorded),
or with the real SupabaseAuth over a faked `requests`; synthetic data only.

Cases that depend on a decision Bryan hasn't made yet carry its H number in the
test name: H1 (AH-1a), H2 (AH-1b), H4 (AH-7b, AH-7c), H5 (AH-5b), H6 (AH-3), L4 (AH-9) and
L6 (the refresh bucket).

Interfaces this file assumes, from the packet or proposed for the build:
- accounts.normalize_email(email): strip() then lower() (J6)
- rate-limit kinds "credential", "session", "refresh" and "email" in RateLimiter, with
  API_CREDENTIAL_RATE_LIMIT_BURST / _PER_HOUR, API_SESSION_RATE_LIMIT_BURST /
  _PER_MINUTE and API_REFRESH_RATE_LIMIT_BURST / _PER_HOUR; the route limits live in
  src.api.read and are named *_rate_limit
- AH-9's API_EMAIL_BUDGET_PER_HOUR, _RECOVERY_PER_HOUR, _SIGNUP_PER_HOUR and
  _PER_CLIENT_PER_HOUR, and an EMAIL_BUDGET_LOCK = 170101 constant in src/api
- accounts.lock_email(connection, email) stays the one email-lock helper (J3)
"""

import base64
import hashlib
import json
import logging
import re
import secrets
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pr22_support import (
    DEFAULT_EMAIL_BUDGET,
    account_client,
    integrated_app,
    pr22_roles,  # noqa: F401  (pytest fixture)
)
from sqlalchemy import event, text
from test_supabase_accounts import FakeProvider

pytestmark = pytest.mark.db

REPO = Path(__file__).resolve().parents[3]
EMAIL = "one@example.com"
PASSWORD = "Password123"
UNIFORM = {
    "message": "If an account exists for this email, password reset instructions will be sent."
}
START_AGAIN = "Verification expired or invalid. Please start again."
RECOVERY_EXPIRED = "This reset session has expired. Request a new reset link."
REAUTH = "Sign in again to change your password."
RATE_LIMITED = {"detail": "too many requests; retry later", "code": "rate_limited"}
KEY = "host:testclient"  # client_key() of TestClient's default peer
WAIT = 20  # seconds: a safety bound on every Event wait, never an expected delay

# The packet's bucket table: (method, path, bucket).
ROUTES = [
    ("GET", "/v1/watchlist", "read"),
    ("GET", "/v1/securities/ZZAA", "read"),
    ("GET", "/api/watchlist", "read"),
    ("POST", "/v1/auth/signup", "auth"),
    ("POST", "/v1/auth/forgot-password", "auth"),
    ("POST", "/v1/auth/login", "credential"),
    ("POST", "/v1/auth/verify-otp", "credential"),
    ("POST", "/v1/auth/resend-otp", "credential"),
    ("POST", "/v1/auth/resend-confirmation", "credential"),
    ("POST", "/v1/auth/confirm-email", "credential"),
    ("POST", "/v1/auth/accept-recovery-link", "credential"),
    ("POST", "/v1/auth/reset-password", "credential"),
    ("PUT", "/v1/me/password", "credential"),
    ("POST", "/v1/auth/refresh", "refresh"),
    ("POST", "/v1/auth/logout", "session"),
    ("GET", "/v1/me", "session"),
    ("GET", "/v1/me/preferences", "session"),
    ("PUT", "/v1/me/preferences", "session"),
    ("GET", "/v1/me/notifications", "session"),
    ("PUT", "/v1/me/notifications", "session"),
    ("GET", "/v1/me/watchlist", "session"),
    ("POST", "/v1/me/watchlist", "session"),
    ("DELETE", "/v1/me/watchlist/ZZAA", "session"),
]
KINDS = ("read", "auth", "credential", "session", "refresh")
TIGHT = {  # every bucket one token deep, refilling far slower than a test runs
    "API_RATE_LIMIT_BURST": "1",
    "API_RATE_LIMIT_PER_MINUTE": "1",
    "API_AUTH_RATE_LIMIT_BURST": "1",
    "API_AUTH_RATE_LIMIT_PER_HOUR": "1",
    "API_CREDENTIAL_RATE_LIMIT_BURST": "1",
    "API_CREDENTIAL_RATE_LIMIT_PER_HOUR": "1",
    "API_SESSION_RATE_LIMIT_BURST": "1",
    "API_SESSION_RATE_LIMIT_PER_MINUTE": "1",
    "API_REFRESH_RATE_LIMIT_BURST": "1",
    "API_REFRESH_RATE_LIMIT_PER_HOUR": "1",
}
ROOMY = {  # for concurrency tests: the limiter must never turn a race into a 429
    "API_CREDENTIAL_RATE_LIMIT_BURST": "500",
    "API_SESSION_RATE_LIMIT_BURST": "500",
    "API_AUTH_RATE_LIMIT_BURST": "500",
    "API_REFRESH_RATE_LIMIT_BURST": "500",
}


# ── the recorded fake provider ───────────────────────────

PROVIDER_METHODS = (
    "signup",
    "check_password",
    "send_code",
    "verify_code",
    "identity",
    "recovery_identity",
    "refresh",
    "logout",
    "change_password",
)


def _recorded(name):
    def method(self, *args, **kwargs):
        checked_out = self.engine.pool.checkedout() if self.engine is not None else None
        call = SimpleNamespace(
            name=name, args=args, kwargs=kwargs, checked_out=checked_out, result=None
        )
        with self.lock:
            self.calls.append(call)
        hook = self.hooks.get(name)
        if hook is not None:
            hook(*args, **kwargs)
        call.result = getattr(FakeProvider, name)(self, *args, **kwargs)
        return call.result

    return method


class Recording(FakeProvider):
    """FakeProvider, recording every call with the API pool's checked-out count;
    hooks[name] runs first (to block on an Event, or to raise)."""

    def __init__(self):
        super().__init__()
        self.calls, self.hooks, self.engine = [], {}, None
        self.lock = threading.Lock()

    def named(self, name):
        return [c for c in self.calls if c.name == name]


for _name in PROVIDER_METHODS:
    setattr(Recording, _name, _recorded(_name))


@contextmanager
def hardened(engine, roles, monkeypatch, env=None):
    """The integrated app with a Recording provider: (client, provider, app)."""
    provider = Recording()
    with (
        integrated_app(engine, roles, monkeypatch, env=env) as (app, _, _),
        account_client(app, monkeypatch, provider) as (client, _, _),
    ):
        provider.engine = app.state.engine
        yield client, provider, app


@pytest.fixture
def api(engine, pr22_roles, monkeypatch):  # noqa: F811
    with hardened(engine, pr22_roles, monkeypatch) as parts:
        yield parts


@pytest.fixture
def roomy(engine, pr22_roles, monkeypatch):  # noqa: F811
    with hardened(engine, pr22_roles, monkeypatch, env=ROOMY) as parts:
        yield parts


# ── flows and rows ───────────────────────────────────────


def confirmed_user(provider, email=EMAIL):
    FakeProvider.signup(provider, email, PASSWORD, "Test Person")
    provider.users[email]["email_confirmed_at"] = "2026-10-24T00:00:00Z"


def login(client, email=EMAIL):
    response = client.post(
        "/v1/auth/login", json={"email": email, "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    return response.json()["challenge_id"]


def verify(client, challenge_id, email=EMAIL, code="123456"):
    return client.post(
        "/v1/auth/verify-otp",
        json={"email": email, "challenge_id": challenge_id, "code": code},
    )


def session(client, provider, email=EMAIL):
    """A confirmed user with an approved login session: (body, headers)."""
    confirmed_user(provider, email)
    verified = verify(client, login(client, email), email)
    assert verified.status_code == 200, verified.text
    body = verified.json()
    return body, {"Authorization": "Bearer " + body["access_token"]}


def recovery_session(client, provider, email=EMAIL):
    """forgot-password, then the emailed link accepted: (link tokens, headers)."""
    assert (
        client.post("/v1/auth/forgot-password", json={"email": email}).status_code
        == 200
    )
    link = FakeProvider.verify_code(provider, email, "123456", "recovery")
    accepted = client.post(
        "/v1/auth/accept-recovery-link", json={"access_token": link["access_token"]}
    )
    assert accepted.status_code == 200, accepted.text
    return link, {"Authorization": "Bearer " + link["access_token"]}


def digest(challenge_id):
    return hashlib.sha256(challenge_id.encode()).hexdigest()


def challenges(engine, email=EMAIL):
    with engine.connect() as conn:  # superuser: past RLS, read-only
        return conn.execute(
            text(
                "SELECT token_hash, flow, consumed_at, created_at, expires_at, attempts,"
                " send_count FROM app.auth_challenges WHERE email = :e ORDER BY created_at"
            ),
            {"e": email},
        ).all()


def open_hashes(engine, email=EMAIL, flow=None):
    return {
        r.token_hash
        for r in challenges(engine, email)
        if r.consumed_at is None and (flow is None or r.flow == flow)
    }


def seed_challenge(
    engine, flow, email=EMAIL, *, minutes_back=5, send_count=1, consumed=False
):
    """A challenge row on the database clock; returns its challenge id."""
    challenge_id = secrets.token_urlsafe(32)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO app.auth_challenges (token_hash, email, flow, send_count, expires_at,"
                " consumed_at, created_at, last_sent_at) VALUES (:h, :e, :f, :n,"
                " now() + interval '10 minutes', CASE WHEN :c THEN now() END,"
                " now() - make_interval(mins => :m), now() - make_interval(mins => :m))"
            ),
            {
                "h": digest(challenge_id),
                "e": email,
                "f": flow,
                "n": send_count,
                "c": consumed,
                "m": minutes_back,
            },
        )
    return challenge_id


def age_challenge(engine, challenge_id, *, created_minutes=None, sent_seconds=None):
    sets = []
    if created_minutes is not None:
        sets.append(f"created_at = now() - interval '{created_minutes} minutes'")
    if sent_seconds is not None:
        sets.append(f"last_sent_at = now() - interval '{sent_seconds} seconds'")
    with engine.begin() as conn:
        conn.execute(
            text(
                f"UPDATE app.auth_challenges SET {', '.join(sets)} WHERE token_hash = :h"
            ),
            {"h": digest(challenge_id)},
        )


def sessions(engine, user_id):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT session_id, recovery_only, revoked_at"
                " FROM app.verified_sessions WHERE user_id = :u"
            ),
            {"u": str(user_id)},
        ).all()


def session_id_of(token):
    payload = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))[
        "session_id"
    ]


def run(target, *args):
    """Run a request on a thread; returns (thread, result list)."""
    result = []
    thread = threading.Thread(target=lambda: result.append(target(*args)), daemon=True)
    thread.start()
    return thread, result


def blocking(arrived, release, when=lambda *a, **k: True):
    """A provider hook that signals `arrived` and waits for `release` when `when` holds."""

    def hook(*args, **kwargs):
        if when(*args, **kwargs):
            arrived.release() if isinstance(
                arrived, threading.Semaphore
            ) else arrived.set()
            assert release.wait(WAIT), "a blocked provider call was never released"

    return hook


# ── J1: AH-1a to AH-1c ───────────────────────────────────


def test_ah1a_h1_forgot_password_over_an_open_login_sends_nothing(api, engine):
    client, provider, _ = api
    confirmed_user(provider)
    challenge_id = login(client)
    sends = len(provider.named("send_code"))
    response = client.post("/v1/auth/forgot-password", json={"email": EMAIL})
    assert response.status_code == 200 and response.json() == UNIFORM
    assert len(provider.named("send_code")) == sends, (
        "a recovery code was sent over a login"
    )
    assert digest(challenge_id) in open_hashes(engine, flow="login")
    assert verify(client, challenge_id).status_code == 200


def test_ah1b_h2_five_recovery_requests_use_up_the_hour_but_login_still_works(
    api, engine
):
    """The per-email recovery cap. AH-9's share and ceilings would bind first at their
    defaults, so integrated_app's raised email budget (pr22_support) keeps them out."""
    client, provider, app = api
    from fastapi.testclient import TestClient

    confirmed_user(provider)
    for _ in range(5):  # one client's whole auth burst
        assert (
            client.post("/v1/auth/forgot-password", json={"email": EMAIL}).json()
            == UNIFORM
        )
    assert [c.args for c in provider.named("send_code")] == [(EMAIL, "recovery")] * 5
    owner = TestClient(
        app, client=("198.51.100.200", 4000)
    )  # anyone else, the owner too
    sixth = owner.post("/v1/auth/forgot-password", json={"email": EMAIL})
    assert sixth.status_code == 200 and sixth.json() == UNIFORM
    assert len(provider.named("send_code")) == 5, (
        "the sixth recovery request in an hour sent"
    )
    login(client)  # the recovery cap never makes login 429
    assert provider.named("send_code")[-1].args == (EMAIL, "login")


@pytest.mark.parametrize(
    "seeded, login_limited", [("login", True), ("signup", False), ("recovery", False)]
)
def test_ah1b_caps_count_only_their_own_flow(api, engine, seeded, login_limited):
    """60 sends this hour in one flow: login is capped only by its own."""
    client, provider, _ = api
    confirmed_user(provider)
    seed_challenge(engine, seeded, send_count=60, consumed=True)
    response = client.post(
        "/v1/auth/login", json={"email": EMAIL, "password": PASSWORD}
    )
    assert (response.status_code == 429) is login_limited, response.text


@pytest.mark.parametrize("new_flow", ["login", "signup", "recovery"])
def test_ah1c_a_new_challenge_supersedes_only_its_own_flow(api, engine, new_flow):
    client, provider, _ = api
    if new_flow == "login":
        confirmed_user(provider)
        others = [seed_challenge(engine, f) for f in ("signup", "recovery")]
        login(client)
    elif new_flow == "signup":  # an address not yet registered
        others = [seed_challenge(engine, f) for f in ("login", "recovery")]
        assert (
            client.post(
                "/v1/auth/signup",
                json={"email": EMAIL, "full_name": "Test Person", "password": PASSWORD},
            ).status_code
            == 201
        )
    else:  # an open login would hold the recovery send back (AH-1a), so none here
        confirmed_user(provider)
        others = [seed_challenge(engine, "signup")]
        assert (
            client.post("/v1/auth/forgot-password", json={"email": EMAIL}).status_code
            == 200
        )
    still_open = open_hashes(engine)
    assert {digest(c) for c in others} <= still_open, (
        "another flow's challenge was consumed"
    )
    assert open_hashes(engine, flow=new_flow)


# ── J2: AH-2a to AH-2f ───────────────────────────────────


def _signup(client, email=EMAIL):
    response = client.post(
        "/v1/auth/signup",
        json={"email": email, "full_name": "Test Person", "password": PASSWORD},
    )
    assert response.status_code == 201, response.text
    return response.json()["challenge_id"]


def _route_signup(client, provider, engine):
    return lambda: _signup(client)


def _route_login(client, provider, engine):
    confirmed_user(provider)
    return lambda: login(client)


def _route_forgot(client, provider, engine):
    confirmed_user(provider)
    return lambda: client.post("/v1/auth/forgot-password", json={"email": EMAIL})


def _route_resend_otp(client, provider, engine):
    confirmed_user(provider)
    challenge_id = login(client)
    age_challenge(engine, challenge_id, sent_seconds=61)
    return lambda: client.post(
        "/v1/auth/resend-otp", json={"email": EMAIL, "challenge_id": challenge_id}
    )


def _route_resend_confirmation(client, provider, engine):
    challenge_id = _signup(client)
    age_challenge(engine, challenge_id, sent_seconds=61)
    return lambda: client.post(
        "/v1/auth/resend-confirmation",
        json={"email": EMAIL, "challenge_id": challenge_id},
    )


def _route_verify(client, provider, engine):
    confirmed_user(provider)
    challenge_id = login(client)
    return lambda: verify(client, challenge_id)


def _route_confirm(client, provider, engine):
    _signup(client)
    link = FakeProvider.verify_code(provider, EMAIL, "123456", "signup")
    return lambda: client.post(
        "/v1/auth/confirm-email", json={"access_token": link["access_token"]}
    )


def _route_accept_link(client, provider, engine):
    session(client, provider)
    assert (
        client.post("/v1/auth/forgot-password", json={"email": EMAIL}).status_code
        == 200
    )
    link = FakeProvider.verify_code(provider, EMAIL, "123456", "recovery")
    return lambda: client.post(
        "/v1/auth/accept-recovery-link", json={"access_token": link["access_token"]}
    )


def _route_refresh(client, provider, engine):
    body, _ = session(client, provider)
    return lambda: client.post(
        "/v1/auth/refresh", json={"refresh_token": body["refresh_token"]}
    )


def _route_logout(client, provider, engine):
    _, headers = session(client, provider)
    return lambda: client.post("/v1/auth/logout", headers=headers)


def _route_reset(client, provider, engine):
    session(client, provider)
    _, headers = recovery_session(client, provider)
    return lambda: client.post(
        "/v1/auth/reset-password",
        headers=headers,
        json={"new_password": "Newpassword123"},
    )


def _route_password(client, provider, engine):
    _, headers = session(client, provider)
    return lambda: client.put(
        "/v1/me/password",
        headers=headers,
        json={"old_password": PASSWORD, "new_password": "Newpassword123"},
    )


def _route_me(client, provider, engine):
    _, headers = session(client, provider)
    return lambda: client.get("/v1/me", headers=headers)


AH2A_ROUTES = {
    "signup": _route_signup,
    "login": _route_login,
    "forgot_password": _route_forgot,
    "resend_otp": _route_resend_otp,
    "resend_confirmation": _route_resend_confirmation,
    "verify_otp": _route_verify,
    "confirm_email": _route_confirm,
    "accept_recovery_link": _route_accept_link,
    "refresh": _route_refresh,
    "logout": _route_logout,
    "reset_password": _route_reset,
    "change_password": _route_password,
    "me": _route_me,
}


@pytest.mark.parametrize("route", sorted(AH2A_ROUTES))
def test_ah2a_no_connection_is_checked_out_during_a_provider_call(api, engine, route):
    client, provider, _ = api
    request = AH2A_ROUTES[route](client, provider, engine)
    provider.calls.clear()
    response = request()
    status = response.status_code if hasattr(response, "status_code") else 200
    assert status < 500, getattr(response, "text", "")
    assert provider.calls, "the route made no provider call"
    held = [(c.name, c.checked_out) for c in provider.calls if c.checked_out]
    assert held == [], f"provider calls made while holding an API connection: {held}"


def test_ah2b_a_second_login_is_not_held_by_the_first_ones_send(roomy):
    client, provider, _ = roomy
    confirmed_user(provider)
    arrived, release = threading.Semaphore(0), threading.Event()
    provider.hooks["send_code"] = blocking(
        arrived, release, lambda email, flow: flow == "login"
    )
    first, second, both_in = run(login, client), None, False
    try:
        assert arrived.acquire(timeout=WAIT), "the first login never reached send_code"
        second = run(login, client)
        both_in = arrived.acquire(timeout=6)  # a lock_timeout is 5 s
    finally:
        release.set()
    for thread, _ in filter(None, (first, second)):
        thread.join(WAIT)
    assert both_in, "the second login waited on the first one's email lock"
    assert len(first[1]) == len(second[1]) == 1  # both finished with a challenge id


@pytest.mark.parametrize("route", ["signup", "login", "forgot_password"])
def test_ah2c_a_failed_send_leaves_no_open_challenge(api, engine, route):
    from src.api.supabase_auth import AuthProviderError

    client, provider, _ = api

    def fail(*args, **kwargs):
        raise AuthProviderError()

    if route == "signup":
        provider.hooks["signup"] = fail
        client.post(
            "/v1/auth/signup",
            json={"email": EMAIL, "full_name": "Test Person", "password": PASSWORD},
        )
    else:
        confirmed_user(provider)
        provider.hooks["send_code"] = fail
        path = "/v1/auth/login" if route == "login" else "/v1/auth/forgot-password"
        client.post(path, json={"email": EMAIL, "password": PASSWORD})
    assert provider.calls, "the provider was never called"
    assert open_hashes(engine) == set()


def test_ah2c_a_successful_send_consumes_older_challenges_of_its_flow(api, engine):
    client, provider, _ = api
    confirmed_user(provider)
    older = seed_challenge(engine, "login", minutes_back=5)
    newer = login(client)
    assert open_hashes(engine, flow="login") == {digest(newer)}
    assert digest(older) not in open_hashes(engine)


def test_ah2c_a_slower_login_never_consumes_a_newer_challenge(roomy, engine):
    """Login A reserves and blocks in send_code; login B reserves, sends and finishes;
    then A finishes. B's challenge, created later, is the one left open."""
    client, provider, _ = roomy
    confirmed_user(provider)
    arrived, release = threading.Event(), threading.Event()
    calls = []
    provider.hooks["send_code"] = blocking(
        arrived,
        release,
        lambda email, flow: flow == "login" and not calls.append(1) and len(calls) == 1,
    )
    first = run(login, client)
    try:
        assert arrived.wait(WAIT), "login A never reached send_code"
        second = run(login, client)
        second[0].join(6)  # a lock_timeout is 5 s
        b_finished_first = bool(second[1])
    finally:
        release.set()
    first[0].join(WAIT)
    second[0].join(WAIT)
    assert b_finished_first, "login B waited for login A"
    assert open_hashes(engine, flow="login") == {digest(second[1][0])}
    assert digest(first[1][0]) not in open_hashes(engine)


def test_ah2d_ten_concurrent_wrong_codes_reach_the_provider_at_most_max_attempts(
    roomy, engine
):
    from src.api import accounts

    client, provider, _ = roomy
    confirmed_user(provider)
    challenge_id = login(client)
    provider.hooks["verify_code"] = lambda *a, **k: time.sleep(0.2)  # widen the race
    threads = [run(verify, client, challenge_id, EMAIL, "000000") for _ in range(10)]
    for thread, _ in threads:
        thread.join(WAIT * 2)
    statuses = [result[0].status_code for _, result in threads]
    assert all(s == 400 for s in statuses), statuses
    assert len(provider.named("verify_code")) <= accounts.MAX_ATTEMPTS
    assert challenges(engine)[-1].attempts == accounts.MAX_ATTEMPTS


def test_ah2d_a_provider_503_gives_the_attempt_back(api, engine):
    from src.api.supabase_auth import AuthProviderError

    client, provider, _ = api
    confirmed_user(provider)
    challenge_id = login(client)

    def unavailable(*args, **kwargs):
        raise AuthProviderError(503)

    provider.hooks["verify_code"] = unavailable
    assert verify(client, challenge_id).status_code == 503
    assert [
        r.attempts for r in challenges(engine) if r.token_hash == digest(challenge_id)
    ] == [0]


def test_ah2d_after_five_wrong_codes_a_sixth_makes_no_provider_call(api):
    client, provider, _ = api
    confirmed_user(provider)
    challenge_id = login(client)
    for _ in range(5):
        assert verify(client, challenge_id, code="000000").status_code == 400
    calls = len(provider.named("verify_code"))
    assert verify(client, challenge_id).status_code == 400  # the right code, too late
    assert len(provider.named("verify_code")) == calls


def test_ah2e_verify_otp_loses_cleanly_when_the_challenge_is_consumed_meanwhile(
    api, engine
):
    client, provider, _ = api
    confirmed_user(provider)
    challenge_id = login(client)
    consumed = []

    def consume_meanwhile(*args, **kwargs):
        with engine.begin() as conn:  # another transaction, during the provider call
            conn.execute(text("SET LOCAL lock_timeout = '2s'"))
            try:
                with conn.begin_nested():
                    conn.execute(
                        text(
                            "UPDATE app.auth_challenges SET consumed_at = now()"
                            " WHERE token_hash = :h"
                        ),
                        {"h": digest(challenge_id)},
                    )
                consumed.append(True)
            except Exception:
                consumed.append(False)

    provider.hooks["verify_code"] = consume_meanwhile
    response = verify(client, challenge_id)
    assert consumed == [True], "the challenge stayed locked across the provider call"
    assert response.status_code == 400, response.text
    user_id = provider.users[EMAIL]["id"]
    assert sessions(engine, user_id) == []
    token = provider.named("verify_code")[-1].result["access_token"]
    local = [c for c in provider.named("logout") if c.kwargs.get("scope") == "local"]
    assert [c.args[0] for c in local] == [token]


# AH-2f: the real SupabaseAuth, its HTTP faked.

SUPABASE_URL = "https://rb-synthetic.supabase.co"


def b64(value):
    raw = value if isinstance(value, bytes) else json.dumps(value).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def token(user, sid, **changes):
    claims = {
        "iss": f"{SUPABASE_URL}/auth/v1",
        "aud": "authenticated",
        "role": "authenticated",
        "exp": int(time.time()) + 600,
        "sub": str(user),
        "session_id": str(sid),
        "amr": [{"method": "recovery", "timestamp": int(time.time())}],
    }
    claims.update(changes)
    return f"{b64({'alg': 'HS256', 'typ': 'JWT'})}.{b64(claims)}.c3ludGhldGlj"


class _Response:
    def __init__(self, status, body=None):
        self.status_code, self._body = status, body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def json(self):
        if self._body is None:
            raise ValueError("no body")
        return self._body


class FakeSupabase:
    """Supabase Auth's HTTP API as SupabaseAuth calls it; answers set per test."""

    def __init__(self, user):
        self.user, self.requests, self.answers = user, [], {}

    def __call__(self, method, url, **kwargs):
        path = url.split("/auth/v1/", 1)[1]
        self.requests.append((method, path))
        for (m, prefix), answer in self.answers.items():
            if m == method and path.startswith(prefix):
                return answer() if callable(answer) else answer
        if (method, path) == ("GET", "user"):
            return _Response(200, self.user)
        if path.startswith("logout"):
            return _Response(204)
        if path.startswith("token?grant_type=password"):
            return _Response(
                200, {"access_token": "synthetic-checked", "user": self.user}
            )
        raise AssertionError(f"unexpected Supabase call {method} {path}")

    def user_calls(self):
        return [r for r in self.requests if r == ("GET", "user")]


@pytest.fixture
def supabase(engine, pr22_roles, monkeypatch):  # noqa: F811
    """The integrated app with the real SupabaseAuth: (client, fake, user_id, session_id).

    One user with a profile and an approved login session; email_is_registered
    answers from a set the test controls (registered).
    """
    from fastapi.testclient import TestClient
    from src.api import accounts, supabase_auth

    user_id, session_id = uuid4(), uuid4()
    with engine.begin() as conn:  # superuser: seed past RLS
        conn.execute(
            text(
                "INSERT INTO app.profiles (id, email, full_name)"
                " VALUES (:i, :e, 'Private Person')"
            ),
            {"i": str(user_id), "e": EMAIL},
        )
        conn.execute(
            text(
                "INSERT INTO app.verified_sessions (session_id, user_id)"
                " VALUES (:s, :u)"
            ),
            {"s": str(session_id), "u": str(user_id)},
        )
    fake = FakeSupabase(
        {
            "id": str(user_id),
            "email": EMAIL,
            "email_confirmed_at": "2026-10-24T00:00:00Z",
            "user_metadata": {},
        }
    )
    fake.registered = {EMAIL}
    monkeypatch.setattr(supabase_auth.requests, "request", fake)
    monkeypatch.setattr(
        accounts, "email_is_registered", lambda _conn, email: email in fake.registered
    )
    env = {"API_AUTH_RATE_LIMIT_BURST": "1000", **ROOMY}
    with integrated_app(engine, pr22_roles, monkeypatch, env=env) as (app, _, _):
        app.dependency_overrides[accounts.get_provider] = lambda: (
            supabase_auth.SupabaseAuth(SUPABASE_URL, "synthetic-publishable-key")
        )
        try:
            with TestClient(app) as client:
                yield client, fake, user_id, session_id
        finally:
            app.dependency_overrides.clear()


MALFORMED = {
    "two_parts": lambda u, s: f"{b64({'alg': 'HS256'})}.{b64({'sub': str(u)})}",
    "payload_not_json": lambda u, s: f"{b64({'alg': 'HS256'})}.{b64(b'not json')}.c2ln",
    "wrong_iss": lambda u, s: token(u, s, iss="https://elsewhere.example/auth/v1"),
    "wrong_aud": lambda u, s: token(u, s, aud="anon"),
    "wrong_role": lambda u, s: token(u, s, role="service_role"),
    "sub_not_uuid": lambda u, s: token(u, s, sub="not-a-uuid"),
    "session_id_not_uuid": lambda u, s: token(u, s, session_id="not-a-uuid"),
}


def _send(client, where, bearer):
    if where == "me":
        return client.get("/v1/me", headers={"Authorization": "Bearer " + bearer})
    path = (
        "/v1/auth/confirm-email"
        if where == "confirm_email"
        else "/v1/auth/accept-recovery-link"
    )
    return client.post(path, json={"access_token": bearer})


@pytest.mark.parametrize("where", ["me", "confirm_email", "accept_recovery_link"])
@pytest.mark.parametrize("case", sorted(MALFORMED))
def test_ah2f_a_structurally_bad_token_is_refused_with_no_provider_call(
    supabase, where, case
):
    client, fake, user_id, session_id = supabase
    response = _send(client, where, MALFORMED[case](user_id, session_id))
    # accept-recovery-link answers a bad link with Jaden's 400 (contract kept, AH-8a)
    expected = 400 if where == "accept_recovery_link" else 401
    assert response.status_code == expected, response.text
    assert fake.requests == [], "a malformed token cost a Supabase call"


def test_ah2f_a_structurally_valid_token_still_calls_user_and_is_served(supabase):
    client, fake, user_id, session_id = supabase
    response = client.get(
        "/v1/me", headers={"Authorization": "Bearer " + token(user_id, session_id)}
    )
    assert response.status_code == 200 and response.json()["email"] == EMAIL
    assert fake.user_calls() == [("GET", "user")]


def test_ah2f_an_expired_token_is_still_refused_after_the_user_call(supabase):
    client, fake, user_id, session_id = supabase
    expired = token(user_id, session_id, exp=int(time.time()) - 60)
    assert (
        client.get("/v1/me", headers={"Authorization": "Bearer " + expired}).status_code
        == 401
    )
    assert fake.user_calls() == [("GET", "user")]


def test_ah2f_a_valid_recovery_link_still_calls_user_and_is_accepted(supabase, engine):
    """The checker's case: a structurally valid recovery-session token passes the
    local checks, calls /user once, and the link is accepted."""
    client, fake, user_id, _ = supabase
    seed_challenge(engine, "recovery", minutes_back=0)
    link = token(user_id, uuid4())  # a new Supabase session, recovery in its amr
    response = client.post("/v1/auth/accept-recovery-link", json={"access_token": link})
    assert response.status_code == 200 and response.json() == {"recovery_ready": True}
    assert fake.user_calls() == [("GET", "user")]


# ── J2 rate limits: AH-3a to AH-3e (H6) ──────────────────


def _limits():
    from src.api import read

    return {
        obj
        for name, obj in vars(read).items()
        if name.endswith("rate_limit") and callable(obj)
    }


def _api_routes(app):
    """(method, full path, dependency calls) for every API route, as FastAPI runs it.

    FastAPI 0.143 keeps included routers as _IncludedRouter, whose effective route
    contexts carry the full path and the dependencies, include-level ones too.
    """
    from fastapi import routing

    included = getattr(routing, "_IncludedRouter", ())
    for route in app.routes:
        contexts = (
            route.effective_route_contexts()
            if included and isinstance(route, included)
            else ([route] if isinstance(route, routing.APIRoute) else [])
        )
        for context in contexts:
            calls = [d.call for d in context.dependant.dependencies]
            for method in sorted(context.methods):
                yield method, context.path, calls


def test_ah3a_h6_every_route_has_exactly_one_limit_dependency_first(api):
    _, _, app = api
    limits, missing, seen = _limits(), [], set()
    for method, path, calls in _api_routes(app):
        seen.add((method, path))
        if path == "/health":
            continue
        if not calls or calls[0] not in limits or sum(c in limits for c in calls) != 1:
            missing.append(f"{method} {path}")
    documented = {
        (m.upper(), p) for p, ops in app.openapi()["paths"].items() for m in ops
    }
    assert documented <= seen, (
        f"routes the walk missed: {documented - seen}"
    )  # the test's own guard
    assert missing == [], f"routes without exactly one leading limit: {missing}"


def _fresh_limiter(app):
    from src.api.rate_limit import RateLimiter

    app.state.rate_limiter = RateLimiter(app.state.rate_limiter.config)
    return app.state.rate_limiter


def _drain(app, kind, probe=True):
    """Empty `kind`'s one-token bucket for TestClient's peer, on a fresh limiter."""
    limiter = _fresh_limiter(app)
    assert limiter.check(kind, KEY) is None  # the bucket's one token
    if probe:  # (a probe rejection writes the key's one log line itself)
        assert limiter.check(kind, KEY) is not None


def _request(client, method, path, **kwargs):
    if method != "DELETE":
        kwargs.setdefault("json", {})
    return client.request(method, path, **kwargs)


@pytest.fixture
def tight(engine, pr22_roles, monkeypatch):  # noqa: F811
    with hardened(engine, pr22_roles, monkeypatch, env=TIGHT) as parts:
        yield parts


@pytest.mark.parametrize(
    "method, path, bucket", ROUTES, ids=[f"{m} {p}" for m, p, _ in ROUTES]
)
def test_ah3b_h6_each_route_is_limited_by_its_own_bucket_only(
    tight, method, path, bucket
):
    client, _, app = tight
    outcomes = {}
    for kind in KINDS:
        _drain(app, kind)
        outcomes[kind] = _request(client, method, path).status_code == 429
    assert outcomes == {kind: kind == bucket for kind in KINDS}


@contextmanager
def _engine_use(engine):
    used = []
    listeners = [
        ("checkout", lambda *a: used.append("checkout")),
        ("before_cursor_execute", lambda *a: used.append(a[2])),
    ]
    for name, fn in listeners:
        event.listen(engine, name, fn)
    try:
        yield used
    finally:
        for name, fn in listeners:
            event.remove(engine, name, fn)


INVALID_FIELDS = {
    "email": 7,
    "password": None,
    "full_name": [],
    "code": "x",
    "challenge_id": 1,
    "access_token": 5,
    "refresh_token": 5,
    "new_password": 1,
    "old_password": 1,
    "ticker": "BAD TICKER",
    "sectors": "x",
}


@pytest.mark.parametrize(
    "method, path, bucket", ROUTES, ids=[f"{m} {p}" for m, p, _ in ROUTES]
)
def test_ah3c_h6_a_limited_request_runs_nothing(tight, method, path, bucket):
    """429 before validation and the bearer check: no provider call, no engine use
    (so no advisory lock either), even with invalid fields and a garbage token."""
    client, provider, app = tight
    _drain(app, bucket)
    provider.calls.clear()
    with _engine_use(app.state.engine) as used:
        response = _request(
            client,
            method,
            path,
            json=None if method == "DELETE" else INVALID_FIELDS,
            headers={"Authorization": "Bearer rb-garbage-token"},
        )
    assert response.status_code == 429, response.text
    assert provider.calls == [] and used == []


@pytest.mark.parametrize(
    "path", ["/v1/auth/login", "/v1/auth/signup", "/v1/auth/verify-otp"]
)
def test_ah3c_a_body_that_is_not_json_is_a_422_that_runs_nothing(api, path):
    client, provider, app = api
    provider.calls.clear()
    with _engine_use(app.state.engine) as used:
        response = client.post(
            path,
            content=b'{"email": "rb-sentinel@example.com", oops',
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 422
    assert "rb-sentinel" not in response.text
    assert provider.calls == [] and used == []


def _limiter_sequence(app, kind, n):
    limiter = _fresh_limiter(app)
    return [limiter.check(kind, KEY) for _ in range(n)]


@pytest.mark.parametrize(
    "kind, burst_env, rate_env, interval",
    [
        (
            "credential",
            "API_CREDENTIAL_RATE_LIMIT_BURST",
            "API_CREDENTIAL_RATE_LIMIT_PER_HOUR",
            600,
        ),
        (
            "session",
            "API_SESSION_RATE_LIMIT_BURST",
            "API_SESSION_RATE_LIMIT_PER_MINUTE",
            10,
        ),
        (
            "refresh",
            "API_REFRESH_RATE_LIMIT_BURST",
            "API_REFRESH_RATE_LIMIT_PER_HOUR",
            600,
        ),
    ],
)
def test_ah3d_h6_the_new_settings_take_effect(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
    kind,
    burst_env,
    rate_env,
    interval,
):
    """Burst 2 and a rate of 6 (one token per `interval` seconds)."""
    with hardened(
        engine, pr22_roles, monkeypatch, env={burst_env: "2", rate_env: "6"}
    ) as (_, _, app):
        first, second, third = _limiter_sequence(app, kind, 3)
    assert first is None and second is None
    assert third is not None and interval - 2 <= third <= interval


@pytest.mark.parametrize(
    "setting",
    [
        "API_CREDENTIAL_RATE_LIMIT_BURST",
        "API_CREDENTIAL_RATE_LIMIT_PER_HOUR",
        "API_SESSION_RATE_LIMIT_BURST",
        "API_SESSION_RATE_LIMIT_PER_MINUTE",
        "API_REFRESH_RATE_LIMIT_BURST",
        "API_REFRESH_RATE_LIMIT_PER_HOUR",
    ],
)
@pytest.mark.parametrize("value", ["0", "-3", "2.5", "ten"])
def test_ah3d_invalid_new_settings_refuse_at_startup(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
    setting,
    value,
):
    with pytest.raises(Exception) as caught:
        with integrated_app(engine, pr22_roles, monkeypatch, env={setting: value}):
            pass
    assert "rate_limit_config_invalid" in str(caught.value)
    if value != "0":
        assert value not in str(caught.value)


@pytest.mark.parametrize(
    "kind, burst, interval",
    [("credential", 10, 60), ("session", 30, 1), ("refresh", 10, 60)],
)
def test_ah3d_h6_the_new_defaults_match_the_table(api, kind, burst, interval):
    """credential: burst 10, then 60 an hour; session: burst 30, then 60 a minute;
    refresh (L6): burst 10, then 60 an hour."""
    _, _, app = api
    results = _limiter_sequence(app, kind, burst + 1)
    assert results[:burst] == [None] * burst
    assert results[burst] is not None and interval - 1 <= results[burst] <= interval


@pytest.mark.parametrize(
    "method, path", [("POST", "/v1/auth/login"), ("GET", "/v1/me")]
)
def test_ah3e_the_429_is_exactly_rl2s(tight, method, path):
    client, _, app = tight
    _drain(app, "credential" if path == "/v1/auth/login" else "session")
    response = _request(client, method, path)
    assert response.status_code == 429
    assert (
        response.content
        == json.dumps(RATE_LIMITED, sort_keys=True, separators=(",", ":")).encode()
    )
    assert re.fullmatch(r"[1-9][0-9]*", response.headers["retry-after"])
    assert (
        response.headers["cache-control"] == "no-store"
        and "etag" not in response.headers
    )


@pytest.mark.parametrize(
    "kind, method, path",
    [
        ("credential", "POST", "/v1/auth/login"),
        ("session", "GET", "/v1/me"),
        ("refresh", "POST", "/v1/auth/refresh"),
    ],
)
def test_ah3e_new_kinds_log_once_per_key_without_the_client(
    tight, caplog, kind, method, path
):
    client, _, app = tight
    caplog.set_level(logging.DEBUG)
    _drain(app, kind, probe=False)
    caplog.clear()
    for _ in range(3):
        assert _request(client, method, path).status_code == 429
    lines = [r.getMessage() for r in caplog.records if "rate_limited" in r.getMessage()]
    assert lines == [f"rate_limited kind={kind} held_back=0"], lines
    assert all("testclient" not in line and EMAIL not in line for line in lines)


@pytest.mark.parametrize("kind", ["credential", "session", "refresh", "email"])
def test_ah3e_memory_stays_bounded_per_kind(engine, pr22_roles, monkeypatch, kind):  # noqa: F811
    with hardened(
        engine, pr22_roles, monkeypatch, env={"API_RATE_LIMIT_MAX_CLIENTS": "3"}
    ) as (_, _, app):
        limiter = _fresh_limiter(app)
        for i in range(5):
            limiter.check(kind, f"ip:198.51.100.{i}")
        assert limiter.size(kind) == 3


@pytest.mark.parametrize(
    "path, kind",
    [("/v1/auth/login", "credential"), ("/v1/auth/forgot-password", "auth")],
)
def test_ah3e_the_429_never_depends_on_the_email(tight, path, kind):
    client, provider, app = tight
    confirmed_user(provider)
    answers = []
    for email in (EMAIL, "nobody-here@example.com"):
        _drain(app, kind)
        response = client.post(path, json={"email": email, "password": PASSWORD})
        headers = {k: v for k, v in response.headers.items() if k != "retry-after"}
        answers.append((response.status_code, response.content, headers))
    assert answers[0] == answers[1] and answers[0][0] == 429


# ── J3: AH-4a, AH-4b ─────────────────────────────────────


def _revoke(client, via, headers):
    if via == "logout":
        return client.post("/v1/auth/logout", headers=headers)
    return client.put(
        "/v1/me/password",
        headers=headers,
        json={"old_password": PASSWORD, "new_password": "Newpassword123"},
    )


@pytest.mark.parametrize("via", ["logout", "password_change"])
def test_ah4a_revoke_wins_and_the_in_flight_verify_is_refused(roomy, engine, via):
    client, provider, _ = roomy
    body, headers = session(client, provider)
    pending = login(client)
    arrived, release = threading.Event(), threading.Event()
    provider.hooks["verify_code"] = blocking(arrived, release)
    verifying = run(verify, client, pending)
    try:
        assert arrived.wait(WAIT), "verify-otp never reached the provider"
        revoked = run(_revoke, client, via, headers)
        revoked[0].join(6)  # a lock_timeout is 5 s
        revoke_finished = bool(revoked[1]) and revoked[1][0].status_code < 300
    finally:
        release.set()
    verifying[0].join(WAIT)
    revoked[0].join(WAIT)
    assert revoke_finished, (
        "the revoke waited on (or failed against) the in-flight verify"
    )
    assert verifying[1][0].status_code == 400, verifying[1][0].text
    assert all(s.revoked_at is not None for s in sessions(engine, body["user"]["id"]))


@pytest.mark.parametrize("via", ["logout", "password_change"])
def test_ah4a_verify_wins_and_the_revoke_revokes_its_new_session(roomy, engine, via):
    client, provider, _ = roomy
    body, headers = session(client, provider)
    pending = login(client)
    old_token = body["access_token"]
    arrived, release = threading.Event(), threading.Event()
    provider.hooks["identity"] = blocking(
        arrived, release, lambda tok: tok == old_token
    )
    revoked = run(_revoke, client, via, headers)
    try:
        assert arrived.wait(WAIT), "the revoke never reached the provider"
        verified = verify(client, pending)
    finally:
        release.set()
    revoked[0].join(WAIT)
    assert verified.status_code == 200, verified.text
    assert revoked[1][0].status_code < 300, revoked[1][0].text
    new_session = session_id_of(verified.json()["access_token"])
    rows = {str(s.session_id): s for s in sessions(engine, body["user"]["id"])}
    assert rows[new_session].revoked_at is not None
    assert all(s.revoked_at is not None for s in rows.values())


def test_ah4b_revoke_takes_verify_otps_email_lock_through_the_helper(api, monkeypatch):
    from src.api import accounts

    client, provider, _ = api
    real, seen = accounts.lock_email, []
    monkeypatch.setattr(
        accounts,
        "lock_email",
        lambda connection, email: (seen.append(email), real(connection, email))[1],
    )
    _, headers = session(client, provider)
    verify_keys = list(seen)
    seen.clear()
    assert client.post("/v1/auth/logout", headers=headers).status_code == 204
    assert verify_keys and seen, (verify_keys, seen)
    assert set(seen) == {verify_keys[-1]} == {accounts.normalize_email(EMAIL)}


# ── J5 and the oracle: AH-5a, AH-5b (H5) ─────────────────


def _requests_timeout():
    import requests

    raise requests.Timeout("synthetic provider text")


PROVIDER_FAILURES = {
    "503": lambda: _Response(503, {"msg": "synthetic provider text"}),
    "500": lambda: _Response(500, {"msg": "synthetic provider text"}),
    "timeout": _requests_timeout,
    "unmapped_400": lambda: _Response(
        400, {"error_code": "rb_unmapped", "msg": "synthetic provider text"}
    ),
}


@pytest.mark.parametrize("failure", sorted(PROVIDER_FAILURES))
def test_ah5a_forgot_password_answers_the_same_for_registered_and_unknown(
    supabase, failure
):
    client, fake, _, _ = supabase
    fake.answers[("POST", "recover")] = PROVIDER_FAILURES[failure]
    answers = []
    for email in (EMAIL, "nobody-here@example.com"):
        response = client.post("/v1/auth/forgot-password", json={"email": email})
        answers.append((response.status_code, response.content, dict(response.headers)))
    assert answers[0] == answers[1], answers
    assert answers[0][0] == 200 and json.loads(answers[0][1]) == UNIFORM
    assert ("POST", "recover") in [(m, p.split("?")[0]) for m, p in fake.requests]


def _app_lines(caplog):
    return [
        r.getMessage()
        for r in caplog.records
        if r.name.startswith("src.api") and "rate_limited" not in r.getMessage()
    ]


def test_ah5a_the_failure_logs_only_the_fixed_provider_code(supabase, caplog):
    client, fake, _, _ = supabase
    fake.answers[("POST", "recover")] = PROVIDER_FAILURES["503"]
    caplog.set_level(logging.DEBUG)
    caplog.clear()
    assert (
        client.post("/v1/auth/forgot-password", json={"email": EMAIL}).status_code
        == 200
    )
    lines = _app_lines(caplog)
    assert lines, "the provider failure was not logged"
    assert any("unavailable" in line for line in lines), (
        lines
    )  # AuthProviderError's code
    for line in lines:
        for leak in (
            EMAIL,
            "testclient",
            "synthetic provider text",
            "redirect_to",
            "/auth/v1",
        ):
            assert leak not in line, line


def test_ah5a_failure_lines_are_capped_like_rl13(supabase, caplog):
    client, fake, _, _ = supabase
    fake.answers[("POST", "recover")] = PROVIDER_FAILURES["503"]
    caplog.set_level(logging.DEBUG)
    caplog.clear()
    for _ in range(70):  # within one minute
        assert (
            client.post("/v1/auth/forgot-password", json={"email": EMAIL}).status_code
            == 200
        )
    lines = _app_lines(caplog)
    assert 1 <= len(lines) <= 61, len(
        lines
    )  # 60 a minute, plus at most one suppressed-count line


@pytest.mark.parametrize(
    "provider_code, status, detail",
    [
        ("reauthentication_needed", 403, REAUTH),
        ("reauthentication_not_valid", 403, REAUTH),
        ("rb_unmapped", 401, "Invalid email, password, or session."),
    ],
)
def test_ah5b_h5_reauthentication_on_a_password_change_is_a_fixed_403(
    supabase, provider_code, status, detail
):
    client, fake, user_id, session_id = supabase
    fake.answers[("PUT", "user")] = _Response(
        400, {"error_code": provider_code, "msg": "synthetic provider text"}
    )
    response = client.put(
        "/v1/me/password",
        headers={"Authorization": "Bearer " + token(user_id, session_id)},
        json={"old_password": PASSWORD, "new_password": "Newpassword123"},
    )
    assert response.status_code == status, response.text
    assert response.json()["detail"] == detail
    if status == 403:
        assert response.json()["code"] == "reauthentication_needed"
    assert "synthetic provider text" not in response.text


# ── J6: AH-6a, AH-6b ─────────────────────────────────────


def test_ah6a_normalize_email_is_strip_then_lower():
    from src.api import accounts

    assert accounts.normalize_email("  Straße@Example.COM ") == "straße@example.com"


def test_ah6a_signup_uses_the_normalized_address_everywhere(api, engine, monkeypatch):
    from src.api import accounts

    client, provider, _ = api
    real, locked = accounts.lock_email, []
    monkeypatch.setattr(
        accounts,
        "lock_email",
        lambda connection, email: (locked.append(email), real(connection, email))[1],
    )
    response = client.post(
        "/v1/auth/signup",
        json={
            "email": "  Straße@Example.COM ",
            "full_name": "Test Person",
            "password": PASSWORD,
        },
    )
    assert response.status_code == 201, response.text
    assert [c.args[0] for c in provider.named("signup")] == ["straße@example.com"]
    assert [r.flow for r in challenges(engine, "straße@example.com")] == ["signup"]
    assert locked and set(locked) == {"straße@example.com"}


def test_ah6b_no_casefold_on_an_email_in_src_api():
    """The bearer-scheme check is the packet's one exception. casefold() on an
    environment setting (APP_ENV, DASHBOARD_DATA_MODE) isn't an email either."""
    found = []
    for path in sorted((REPO / "development/backend/src/api").glob("*.py")):
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if "casefold(" not in line:
                continue
            if "scheme.casefold()" in line or re.search(r"os\.(getenv|environ)", line):
                continue
            found.append(f"{path.name}:{number}: {line.strip()}")
    assert found == [], found


# ── J7: AH-7a to AH-7c (H4) ──────────────────────────────


@pytest.mark.parametrize(
    "skew_hours", [1, -1], ids=["app_an_hour_fast", "app_an_hour_slow"]
)
def test_ah7a_challenges_follow_the_database_clock(
    api, engine, monkeypatch, skew_hours
):
    from datetime import datetime, timedelta, timezone

    from src.api import accounts

    client, provider, _ = api
    monkeypatch.setattr(
        accounts,
        "now",
        lambda: datetime.now(timezone.utc) + timedelta(hours=skew_hours),
        raising=False,
    )
    confirmed_user(provider)
    fresh = login(client)
    cooldown = client.post(
        "/v1/auth/resend-otp", json={"email": EMAIL, "challenge_id": fresh}
    )
    assert cooldown.status_code == 429, (
        cooldown.text
    )  # the resend cooldown still applies
    assert verify(client, fresh).status_code == 200  # a fresh challenge verifies
    stale = login(client)
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE app.auth_challenges SET expires_at = now() - interval '1 minute'"
                " WHERE token_hash = :h"
            ),
            {"h": digest(stale)},
        )
    assert verify(client, stale).status_code == 400  # past the database's expires_at


def _open_challenge(client, provider, flow):
    if flow == "login":
        confirmed_user(provider)
        return login(client), "/v1/auth/resend-otp"
    return _signup(client), "/v1/auth/resend-confirmation"


@pytest.mark.parametrize("flow", ["login", "signup"])
def test_ah7b_h4_a_resend_never_extends_past_sixty_minutes(api, engine, flow):
    client, provider, _ = api
    challenge_id, path = _open_challenge(client, provider, flow)
    age_challenge(engine, challenge_id, created_minutes=59, sent_seconds=120)
    response = client.post(path, json={"email": EMAIL, "challenge_id": challenge_id})
    assert response.status_code == 200, response.text
    with engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT expires_at = created_at + interval '60 minutes' FROM app.auth_challenges"
                    " WHERE token_hash = :h"
                ),
                {"h": digest(challenge_id)},
            ).scalar()
            is True
        )


@pytest.mark.parametrize("flow", ["login", "signup"])
def test_ah7b_h4_past_sixty_minutes_a_resend_says_start_again(api, engine, flow):
    client, provider, _ = api
    challenge_id, path = _open_challenge(client, provider, flow)
    age_challenge(engine, challenge_id, created_minutes=61, sent_seconds=120)
    sends = len(provider.named("send_code"))
    response = client.post(path, json={"email": EMAIL, "challenge_id": challenge_id})
    assert response.status_code == 400 and response.json()["detail"] == START_AGAIN, (
        response.text
    )
    assert len(provider.named("send_code")) == sends


def _age_session(engine, access_token, minutes):
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE app.verified_sessions SET created_at = now()"
                " - make_interval(mins => :m) WHERE session_id = :s"
            ),
            {"m": minutes, "s": session_id_of(access_token)},
        )


def test_ah7c_h4_a_recovery_session_14_minutes_old_still_resets(api, engine):
    client, provider, _ = api
    session(client, provider)
    link, headers = recovery_session(client, provider)
    _age_session(engine, link["access_token"], 14)
    response = client.post(
        "/v1/auth/reset-password",
        headers=headers,
        json={"new_password": "Newpassword123"},
    )
    assert response.status_code == 200, response.text


@pytest.mark.parametrize("route", ["reset_password", "refresh"])
def test_ah7c_h4_a_recovery_session_16_minutes_old_is_refused(api, engine, route):
    client, provider, _ = api
    session(client, provider)
    link, headers = recovery_session(client, provider)
    _age_session(engine, link["access_token"], 16)
    if route == "reset_password":
        response = client.post(
            "/v1/auth/reset-password",
            headers=headers,
            json={"new_password": "Newpassword123"},
        )
    else:
        response = client.post(
            "/v1/auth/refresh", json={"refresh_token": link["refresh_token"]}
        )
    assert response.status_code == 401, response.text
    assert response.json()["detail"] == RECOVERY_EXPIRED


def test_ah7c_a_login_session_16_minutes_old_is_unaffected(api, engine):
    client, provider, _ = api
    body, headers = session(client, provider)
    _age_session(engine, body["access_token"], 16)
    assert client.get("/v1/me", headers=headers).status_code == 200
    assert (
        client.post(
            "/v1/auth/refresh", json={"refresh_token": body["refresh_token"]}
        ).status_code
        == 200
    )


# ── AH-8b: docs ──────────────────────────────────────────


def test_ah8b_api_md_documents_the_buckets_and_the_new_errors():
    doc = (REPO / "docs" / "technical-specs" / "API.md").read_text()
    for term in (
        "API_CREDENTIAL_RATE_LIMIT_BURST",
        "API_CREDENTIAL_RATE_LIMIT_PER_HOUR",
        "API_SESSION_RATE_LIMIT_BURST",
        "API_SESSION_RATE_LIMIT_PER_MINUTE",
        "credential",
        "session",
        REAUTH,
        RECOVERY_EXPIRED,
    ):
        assert term in doc, term


def test_ah8b_operations_md_has_the_ntp_note():
    doc = (REPO / "docs" / "technical-specs" / "OPERATIONS.md").read_text()
    assert "NTP" in doc


# ── AH-9: the app-wide email budget (L4), from the S1 amendment ─
# Small explicit budgets keep each case to one limit. Every client is its own
# TestClient address. Login's check_password runs before its reserve step (J2), so
# "no provider call" is pinned as "no sending call" there, and as no call at all on
# resend-otp and forgot-password, which make none before it.

SEND_REFUSED = "We can't send a code right now. Please try again in a few minutes."


def budget(total=25, recovery=6, signup=6, share=3):
    return {
        "API_EMAIL_BUDGET_PER_HOUR": str(total),
        "API_EMAIL_BUDGET_RECOVERY_PER_HOUR": str(recovery),
        "API_EMAIL_BUDGET_SIGNUP_PER_HOUR": str(signup),
        "API_EMAIL_BUDGET_PER_CLIENT_PER_HOUR": str(share),
    }


def peer(app, n):
    """A client from its own address (no lifespan: the app has already started)."""
    from fastapi.testclient import TestClient

    return TestClient(app, client=(f"198.51.100.{n}", 4000 + n))


def users(provider, n):
    emails = [f"user{i}@example.com" for i in range(n)]
    for email in emails:
        confirmed_user(provider, email)
    return emails


def sent(provider):
    """The email-sending provider calls so far."""
    return [c for c in provider.calls if c.name in ("send_code", "signup")]


def post_login(client, email):
    return client.post("/v1/auth/login", json={"email": email, "password": PASSWORD})


def post_forgot(client, email):
    return client.post("/v1/auth/forgot-password", json={"email": email})


def post_signup(client, email):
    return client.post(
        "/v1/auth/signup",
        json={"email": email, "full_name": "Test Person", "password": PASSWORD},
    )


def assert_send_refused(response):
    assert response.status_code == 429, response.text
    assert response.json()["detail"] == SEND_REFUSED
    assert response.headers.get("retry-after") == "60"


def assert_uniform(response):
    assert response.status_code == 200 and response.json() == UNIFORM, response.text


@contextmanager
def budgeted(engine, roles, monkeypatch, **limits):
    env = {**ROOMY, **(budget(**limits) if limits else DEFAULT_EMAIL_BUDGET)}
    with hardened(engine, roles, monkeypatch, env=env) as parts:
        yield parts


def test_ah9a_l4_the_app_wide_total_refuses_the_next_send_in_any_flow(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    with budgeted(
        engine, pr22_roles, monkeypatch, total=5, recovery=2, signup=2, share=1
    ) as (_, provider, app):
        emails = users(provider, 5)
        first = login(peer(app, 1), emails[0])
        login(peer(app, 2), emails[1])
        assert_uniform(post_forgot(peer(app, 3), emails[2]))
        assert post_signup(peer(app, 4), "fresh@example.com").status_code == 201
        login(peer(app, 5), emails[3])
        assert len(sent(provider)) == 5  # the whole budget, from five clients
        age_challenge(engine, first, sent_seconds=61)
        calls = len(provider.calls)
        assert_send_refused(
            peer(app, 6).post(
                "/v1/auth/resend-otp", json={"email": emails[0], "challenge_id": first}
            )
        )
        assert len(provider.calls) == calls  # no provider call at all
        assert_send_refused(post_login(peer(app, 7), emails[4]))
        assert_uniform(post_forgot(peer(app, 8), emails[1]))
        assert len(sent(provider)) == 5


def test_ah9b_l4_at_their_ceilings_recovery_and_signup_stop_but_login_sends(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    with budgeted(
        engine, pr22_roles, monkeypatch, total=10, recovery=2, signup=2, share=1
    ) as (_, provider, app):
        emails = users(provider, 4)
        assert_uniform(post_forgot(peer(app, 1), emails[0]))
        assert_uniform(post_forgot(peer(app, 2), emails[1]))
        assert post_signup(peer(app, 3), "new0@example.com").status_code == 201
        assert post_signup(peer(app, 4), "new1@example.com").status_code == 201
        assert len(sent(provider)) == 4
        assert_uniform(post_forgot(peer(app, 5), emails[2]))  # recovery's ceiling
        assert_send_refused(post_signup(peer(app, 6), "new2@example.com"))  # signup's
        assert len(sent(provider)) == 4
        login(peer(app, 7), emails[3])  # login still has the total's room
        assert sent(provider)[-1].args == (emails[3], "login")


def test_ah9b_l4_two_clients_block_recovery_for_everyone_while_login_works(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    """The trade-off, pinned at the defaults (25, 6, 6, 3): two clients spending
    their shares on forgot-password use up recovery's hour for every address."""
    with budgeted(engine, pr22_roles, monkeypatch) as (_, provider, app):
        emails = users(provider, 7)
        for n, batch in ((1, emails[0:3]), (2, emails[3:6])):
            for email in batch:
                assert_uniform(post_forgot(peer(app, n), email))
        assert len(sent(provider)) == 6
        third = peer(app, 3)
        assert_uniform(post_forgot(third, emails[6]))  # nothing sent, whoever asks
        assert len(sent(provider)) == 6
        login(third, emails[6])
        assert sent(provider)[-1].args == (emails[6], "login")


def test_ah9c_two_app_instances_on_one_database_share_one_budget(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    from fastapi.testclient import TestClient
    from src.api import accounts
    from src.api.app import create_app

    with budgeted(
        engine, pr22_roles, monkeypatch, total=5, recovery=2, signup=2, share=1
    ) as (_, provider, app):
        second = create_app(
            api_role=pr22_roles["api"]
        )  # its own limiter, the same database
        second.dependency_overrides[accounts.get_provider] = lambda: provider
        emails = users(provider, 7)
        for n in range(3):
            login(peer(app, n + 1), emails[n])
        for n in (3, 4):
            login(TestClient(second, client=(f"198.51.100.{n + 1}", 5000)), emails[n])
        assert len(sent(provider)) == 5
        assert_send_refused(
            post_login(TestClient(second, client=("198.51.100.20", 5000)), emails[5])
        )
        assert_send_refused(post_login(peer(app, 21), emails[6]))
        assert len(sent(provider)) == 5


def test_ah9c_one_short_concurrent_requests_send_exactly_one_email(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    from src.api import accounts

    with budgeted(
        engine, pr22_roles, monkeypatch, total=5, recovery=2, signup=2, share=1
    ) as (_, provider, app):
        seed_challenge(
            engine, "login", email="earlier@example.com", send_count=4, consumed=True
        )
        emails = users(provider, 5)
        real = accounts.reserve_email

        def held_open(*args, **kwargs):  # force the race: hold count-to-insert open
            key = real(*args, **kwargs)
            time.sleep(0.3)
            return key

        monkeypatch.setattr(accounts, "reserve_email", held_open)
        threads = [
            run(post_login, peer(app, n + 1), email) for n, email in enumerate(emails)
        ]
        for thread, _ in threads:
            thread.join(WAIT * 2)
        statuses = sorted(result[0].status_code for _, result in threads)
        assert statuses == [200, 429, 429, 429, 429], statuses
        assert len(sent(provider)) == 1


def test_ah9c_a_failed_and_released_first_send_doesnt_count(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    from src.api.supabase_auth import AuthProviderError

    with budgeted(
        engine, pr22_roles, monkeypatch, total=5, recovery=2, signup=2, share=1
    ) as (_, provider, app):
        emails = users(provider, 7)
        failures = []

        def fail_once(*args, **kwargs):
            if not failures:
                failures.append(1)
                raise AuthProviderError()

        provider.hooks["send_code"] = fail_once
        assert post_login(peer(app, 1), emails[0]).status_code == 503
        assert open_hashes(engine, emails[0]) == set()  # released
        for n in range(1, 6):  # the whole budget is still there
            login(peer(app, n + 1), emails[n])
        assert_send_refused(post_login(peer(app, 7), emails[6]))


AH9D_INVALID = {
    "total_zero": {"API_EMAIL_BUDGET_PER_HOUR": "0"},
    "recovery_negative": {"API_EMAIL_BUDGET_RECOVERY_PER_HOUR": "-1"},
    "signup_zero": {"API_EMAIL_BUDGET_SIGNUP_PER_HOUR": "0"},
    "share_not_an_integer": {"API_EMAIL_BUDGET_PER_CLIENT_PER_HOUR": "1.5"},
    "recovery_above_the_total": {"API_EMAIL_BUDGET_RECOVERY_PER_HOUR": "26"},
    "signup_above_the_total": {"API_EMAIL_BUDGET_SIGNUP_PER_HOUR": "30"},
    "ceilings_together_reach_the_total": {
        "API_EMAIL_BUDGET_RECOVERY_PER_HOUR": "13",
        "API_EMAIL_BUDGET_SIGNUP_PER_HOUR": "12",
    },
    "share_equal_to_a_ceiling": {"API_EMAIL_BUDGET_PER_CLIENT_PER_HOUR": "6"},
    "share_above_one_ceiling": {
        "API_EMAIL_BUDGET_SIGNUP_PER_HOUR": "4",
        "API_EMAIL_BUDGET_PER_CLIENT_PER_HOUR": "5",
    },
}


@pytest.mark.parametrize("case", sorted(AH9D_INVALID))
def test_ah9d_invalid_email_budgets_refuse_at_startup(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
    case,
):
    env = {**budget(), **AH9D_INVALID[case]}  # otherwise valid: 25, 6, 6, 3
    with pytest.raises(Exception) as caught:
        with integrated_app(engine, pr22_roles, monkeypatch, env=env):
            pass
    assert "rate_limit_config_invalid" in str(caught.value)


def test_ah9d_the_default_budget_is_valid_and_starts(engine, pr22_roles, monkeypatch):  # noqa: F811
    with budgeted(engine, pr22_roles, monkeypatch) as (client, _, _):
        assert client.get("/health").status_code == 200


@pytest.mark.parametrize("code", ["over_email_send_rate_limit", "rate_limited"])
@pytest.mark.parametrize("route", ["login", "resend_otp", "signup", "forgot_password"])
def test_ah9e_supabases_own_429_is_the_fixed_answer(api, engine, route, code):
    """The cause may be the whole project, so never "to this address"."""
    from src.api.supabase_auth import AuthProviderError

    client, provider, _ = api

    def too_many(*args, **kwargs):
        raise AuthProviderError(429, code)

    if route == "signup":
        provider.hooks["signup"] = too_many
        response = post_signup(client, EMAIL)
    elif route == "forgot_password":
        confirmed_user(provider)
        provider.hooks["send_code"] = too_many
        assert_uniform(post_forgot(client, EMAIL))
        return
    else:
        confirmed_user(provider)
        if route == "login":
            provider.hooks["send_code"] = too_many
            response = post_login(client, EMAIL)
        else:
            challenge_id = login(client)
            age_challenge(engine, challenge_id, sent_seconds=61)
            provider.hooks["send_code"] = too_many
            response = client.post(
                "/v1/auth/resend-otp",
                json={"email": EMAIL, "challenge_id": challenge_id},
            )
    assert response.status_code == 429, response.text
    assert response.json()["detail"] == SEND_REFUSED
    assert "address" not in response.text


def test_ah9f_a_clients_share_of_three_covers_every_flow(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    with budgeted(engine, pr22_roles, monkeypatch, share=3) as (_, provider, app):
        emails = users(provider, 4)
        mine, other = peer(app, 1), peer(app, 2)
        login(mine, emails[0])
        assert_uniform(post_forgot(mine, emails[1]))
        assert post_signup(mine, "new0@example.com").status_code == 201
        assert len(sent(provider)) == 3
        assert_send_refused(post_login(mine, emails[2]))
        calls = len(provider.calls)
        assert_uniform(post_forgot(mine, emails[3]))
        assert len(provider.calls) == calls  # no provider call at all
        login(other, emails[2])  # another client's login still sends
        assert len(sent(provider)) == 4


def test_ah9f_a_request_that_sends_nothing_spends_no_share(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    with budgeted(engine, pr22_roles, monkeypatch, share=3) as (_, provider, app):
        emails = users(provider, 4)
        mine = peer(app, 1)
        for _ in range(4):
            assert_uniform(post_forgot(mine, "nobody-here@example.com"))
        for _ in range(2):
            assert (
                mine.post(
                    "/v1/auth/login", json={"email": emails[0], "password": "Wrong1234"}
                ).status_code
                == 401
            )
        for email in emails[:3]:
            login(mine, email)
        assert_send_refused(post_login(mine, emails[3]))


def test_ah9f_failed_first_sends_refund_the_share_but_never_above_full(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    """Three failed logins from one address, then the provider is back: three send,
    and the fourth is refused (the refunds didn't lift the share above 3)."""
    from src.api.supabase_auth import AuthProviderError

    with budgeted(engine, pr22_roles, monkeypatch, share=3) as (_, provider, app):
        emails = users(provider, 4)
        mine, down = peer(app, 1), [True]

        def outage(*args, **kwargs):
            if down[0]:
                raise AuthProviderError()

        provider.hooks["send_code"] = outage
        for email in emails[:3]:
            assert post_login(mine, email).status_code == 503
        down[0] = False
        for email in emails[:3]:
            login(mine, email)
        assert_send_refused(post_login(mine, emails[3]))


def test_ah9f_a_failed_resend_still_spends_the_share(engine, pr22_roles, monkeypatch):  # noqa: F811
    from src.api.supabase_auth import AuthProviderError

    with budgeted(engine, pr22_roles, monkeypatch, share=3) as (_, provider, app):
        emails = users(provider, 2)
        mine = peer(app, 1)
        challenge_id = login(mine, emails[0])

        def fail(*args, **kwargs):
            raise AuthProviderError()

        provider.hooks["send_code"] = fail
        for _ in range(2):
            age_challenge(engine, challenge_id, sent_seconds=61)
            assert (
                mine.post(
                    "/v1/auth/resend-otp",
                    json={"email": emails[0], "challenge_id": challenge_id},
                ).status_code
                == 503
            )
        provider.hooks.pop("send_code")
        assert_send_refused(post_login(mine, emails[1]))


def test_ah9g_the_budget_lock_is_named_unique_and_taken_after_the_email_lock(api):
    client, provider, app = api
    sources = {
        path: path.read_text()
        for root in ("development/backend/src", "development/database/ops")
        for path in (REPO / root).rglob("*.py")
    }
    definitions = [
        p
        for p, text_ in sources.items()
        if re.search(r"^EMAIL_BUDGET_LOCK = 170101\b", text_, re.M)
    ]
    assert len(definitions) == 1 and "/src/api/" in str(definitions[0]), definitions
    others = set()
    for path, text_ in sources.items():
        for line in text_.splitlines():
            if not line.lstrip().startswith("#") and not line.startswith(
                "EMAIL_BUDGET_LOCK ="
            ):
                others |= {
                    int(v) for v in re.findall(r"\b(17\d{4})\b", line.split("#")[0])
                }
    assert 170101 not in others, "another fixed advisory-lock key is 170101"
    confirmed_user(provider)
    with _engine_use(app.state.engine) as used:
        login(client)
    statements = [u for u in used if u != "checkout"]
    locks = [i for i, s in enumerate(statements) if "pg_advisory_xact_lock" in s]
    email_lock = next(i for i in locks if "hashtextextended" in statements[i])
    budget_lock = next(
        (i for i in locks if "hashtextextended" not in statements[i]), None
    )
    assert budget_lock is not None and email_lock < budget_lock, [
        statements[i] for i in locks
    ]


def test_ah9h_share_refusals_are_counted_not_logged_one_by_one(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
    caplog,
):
    with budgeted(engine, pr22_roles, monkeypatch, share=1) as (_, provider, app):
        emails = users(provider, 11)
        mine = peer(app, 1)
        login(mine, emails[0])
        caplog.set_level(logging.DEBUG)
        caplog.clear()
        for email in emails[1:]:
            assert_send_refused(post_login(mine, email))
        lines = _app_lines(caplog)
        assert len(lines) <= 1, lines  # one line a minute for this kind, with the count
        for line in lines:
            for leak in ("example.com", "198.51.100", "testclient"):
                assert leak not in line, line


def test_ah9h_supabase_429_refusals_are_counted_not_logged_one_by_one(api, caplog):
    from src.api.supabase_auth import AuthProviderError

    client, provider, _ = api
    emails = users(provider, 10)

    def too_many(*args, **kwargs):
        raise AuthProviderError(429, "over_email_send_rate_limit")

    provider.hooks["send_code"] = too_many
    caplog.set_level(logging.DEBUG)
    caplog.clear()
    for email in emails:
        assert post_login(client, email).status_code == 429
    lines = _app_lines(caplog)
    assert len(lines) <= 1, lines
    assert all("example.com" not in line for line in lines)
