"""Offline provider tests + isolated PostgreSQL integration; never real accounts/email."""

import base64
import json
import time
from datetime import timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text, update
from src.api import account_models as tables
from src.api import accounts
from src.api.app import app
from src.api.supabase_auth import AuthProviderError, SupabaseAuth
from src.db.config import DatabaseConfigError, get_database_url


def jwt(user_id, sid, **changes):
    claims = {
        "sub": str(user_id),
        "session_id": str(sid),
        "iss": "https://example.supabase.co/auth/v1",
        "aud": "authenticated",
        "role": "authenticated",
        "exp": int(time.time()) + 3600,
    }
    claims.update(changes)
    encoded = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"header.{encoded}.signature"


class FakeProvider:
    """The provider, not the browser, owns identities and codes."""

    def __init__(self):
        self.users = {}
        self.tokens = {}
        self.sent = []
        self.fail = False

    def signup(self, email, password, name):
        if self.fail:
            raise AuthProviderError()
        if email not in self.users:
            self.users[email] = {
                "id": str(uuid4()),
                "email": email,
                "user_metadata": {"full_name": name},
                "email_confirmed_at": None,
                "password": password,
            }

    def check_password(self, email, password):
        user = self.users.get(email)
        if not user or user["password"] != password:
            raise AuthProviderError(401)
        if not user["email_confirmed_at"]:
            raise AuthProviderError(401, "email_not_confirmed")
        return user

    def send_code(self, email, flow):
        if self.fail:
            raise AuthProviderError()
        self.sent.append((email, flow))

    def verify_code(self, email, code, flow):
        if self.fail:
            raise AuthProviderError()
        if code != "123456" or email not in self.users:
            raise AuthProviderError(401)
        user = self.users[email]
        user["email_confirmed_at"] = "2026-10-18T00:00:00Z"
        token = jwt(
            user["id"],
            uuid4(),
            amr=[{"method": flow, "timestamp": int(time.time())}],
        )
        self.tokens[token] = user
        return {"access_token": token, "refresh_token": token}

    def identity(self, token):
        if token not in self.tokens:
            raise AuthProviderError(401)
        claims = json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "=="))
        from uuid import UUID

        return self.tokens[token], UUID(claims["session_id"])

    def recovery_identity(self, token):
        return SupabaseAuth.recovery_identity(self, token)

    def refresh(self, token):
        self.identity(token)
        return {"access_token": token, "refresh_token": token}

    def logout(self, token, *, scope="global"):
        if self.fail:
            raise AuthProviderError()

    def change_password(self, token, password):
        self.tokens[token]["password"] = password


@pytest.fixture
def api(engine, monkeypatch):
    provider = FakeProvider()
    monkeypatch.setattr(
        accounts,
        "email_is_registered",
        lambda _connection, email: email in provider.users,
    )
    app.dependency_overrides[accounts.get_engine] = lambda: engine
    app.dependency_overrides[accounts.get_provider] = lambda: provider
    with TestClient(app) as client:
        yield client, provider, engine
    app.dependency_overrides.clear()


def register(api, email="one@example.com"):
    client, provider, engine = api
    response = client.post(
        "/auth/signup",
        json={"email": email, "full_name": "Test Person", "password": "password123"},
    )
    assert response.status_code == 201, response.text
    assert response.json()["verification_method"] == "email_link"
    # Simulate consuming the link at Supabase, not typing a signup OTP in the app.
    link_session = provider.verify_code(email, "123456", "signup")
    confirmed = client.post(
        "/auth/confirm-email", json={"access_token": link_session["access_token"]}
    )
    assert confirmed.status_code == 200, confirmed.text
    assert "access_token" not in confirmed.json()
    allow_resend(engine)
    response = client.post(
        "/auth/login", json={"email": email, "password": "password123"}
    )
    challenge = response.json()
    verified = client.post(
        "/auth/verify-otp",
        json={
            "email": email,
            "challenge_id": challenge["challenge_id"],
            "code": "123456",
        },
    )
    assert verified.status_code == 200, verified.text
    body = verified.json()
    return body, {"Authorization": "Bearer " + body["access_token"]}


def allow_resend(engine):
    with engine.begin() as conn:
        conn.execute(
            update(tables.auth_challenges).values(
                last_sent_at=accounts.now() - timedelta(seconds=61)
            )
        )


def test_signup_link_then_login_otp_persistence_and_private_headers(api):
    client, _, engine = api
    body, headers = register(api)
    assert body["user"]["onboarding_completed"] is False
    assert client.get("/me", headers=headers).json()["id"] == body["user"]["id"]
    assert client.get("/me").status_code == 401
    assert client.get("/me", headers=headers).headers["cache-control"] == "no-store"
    with engine.connect() as conn:
        challenges = conn.execute(select(tables.auth_challenges)).mappings().all()
        assert len(challenges) == 2
        assert all(len(row.token_hash) == 64 and row.consumed_at for row in challenges)
    assert "password" not in body and "development_code" not in body


def test_first_login_after_email_confirmation_does_not_wait_for_signup_cooldown(api):
    client, provider, _ = api
    email = "first-login@example.com"
    signup = client.post(
        "/auth/signup",
        json={"email": email, "full_name": "First Login", "password": "password123"},
    )
    assert signup.status_code == 201
    link = provider.verify_code(email, "123456", "signup")
    assert (
        client.post(
            "/auth/confirm-email", json={"access_token": link["access_token"]}
        ).status_code
        == 200
    )

    login = client.post("/auth/login", json={"email": email, "password": "password123"})
    assert login.status_code == 200, login.text
    assert login.json()["flow"] == "login"
    assert provider.sent == [(email, "login")]

    repeated = client.post(
        "/auth/login", json={"email": email, "password": "password123"}
    )
    assert repeated.status_code == 429  # Active login OTP still has a cooldown.
    wrong_password = client.post(
        "/auth/login", json={"email": email, "password": "incorrect"}
    )
    assert wrong_password.status_code == 401


def test_wrong_login_and_unapproved_native_tokens_denied(api):
    client, provider, engine = api
    body, _ = register(api)
    allow_resend(engine)
    assert (
        client.post(
            "/auth/login",
            json={"email": "missing@example.com", "password": "random123"},
        ).status_code
        == 401
    )
    assert (
        client.post(
            "/auth/login",
            json={"email": body["user"]["email"], "password": "incorrect"},
        ).status_code
        == 401
    )
    direct = provider.verify_code(body["user"]["email"], "123456", "login")
    assert (
        client.get(
            "/me", headers={"Authorization": "Bearer " + direct["access_token"]}
        ).status_code
        == 401
    )
    assert (
        client.post(
            "/auth/refresh", json={"refresh_token": direct["refresh_token"]}
        ).status_code
        == 401
    )


def test_existing_user_still_needs_otp_then_keeps_onboarding(api):
    client, _, engine = api
    body, headers = register(api)
    client.put(
        "/me/preferences", headers=headers, json={"sectors": [], "risk_level": None}
    ).raise_for_status()
    allow_resend(engine)
    challenge = client.post(
        "/auth/login", json={"email": body["user"]["email"], "password": "password123"}
    ).json()
    assert challenge["flow"] == "login" and "access_token" not in challenge
    payload = {
        "email": body["user"]["email"],
        "challenge_id": challenge["challenge_id"],
        "code": "123456",
    }
    result = client.post("/auth/verify-otp", json=payload)
    assert result.json()["user"]["onboarding_completed"] is True
    assert client.post("/auth/verify-otp", json=payload).status_code == 400


def test_unconfirmed_login_resumes_signup(api):
    client, provider, _ = api
    provider.signup("pending@example.com", "password123", "Pending")
    response = client.post(
        "/auth/login", json={"email": "pending@example.com", "password": "password123"}
    )
    assert response.status_code == 200 and response.json()["flow"] == "signup"
    assert provider.sent == [("pending@example.com", "signup")]


def test_five_failed_attempts_persist_and_block_even_correct_code(api):
    client, _, engine = api
    register(api)
    allow_resend(engine)
    challenge = client.post(
        "/auth/login",
        json={
            "email": "one@example.com",
            "full_name": "One",
            "password": "password123",
        },
    ).json()
    payload = {
        "email": "one@example.com",
        "challenge_id": challenge["challenge_id"],
        "code": "000000",
    }
    for _ in range(5):
        assert client.post("/auth/verify-otp", json=payload).status_code == 400
    with engine.connect() as conn:
        assert (
            conn.execute(
                select(tables.auth_challenges.c.attempts)
                .where(tables.auth_challenges.c.flow == "login")
                .order_by(tables.auth_challenges.c.created_at.desc())
                .limit(1)
            ).scalar_one()
            == 5
        )
    payload["code"] = "123456"
    assert client.post("/auth/verify-otp", json=payload).status_code == 400


def test_resend_cooldown_hourly_limit_and_expiry(api):
    client, provider, engine = api
    provider.signup("one@example.com", "password123", "One")
    provider.users["one@example.com"]["email_confirmed_at"] = "2026-10-18T00:00:00Z"
    challenge = client.post(
        "/auth/login", json={"email": "one@example.com", "password": "password123"}
    ).json()
    payload = {"email": "one@example.com", "challenge_id": challenge["challenge_id"]}
    assert client.post("/auth/resend-otp", json=payload).status_code == 429
    for _ in range(4):
        allow_resend(engine)
        assert client.post("/auth/resend-otp", json=payload).status_code == 200
    allow_resend(engine)
    assert client.post("/auth/resend-otp", json=payload).status_code == 429
    with engine.begin() as conn:
        conn.execute(
            update(tables.auth_challenges).values(
                expires_at=accounts.now() - timedelta(seconds=1)
            )
        )
    assert (
        client.post("/auth/verify-otp", json={**payload, "code": "123456"}).status_code
        == 400
    )


def test_provider_outage_does_not_consume_attempts(api):
    client, provider, engine = api
    register(api)
    allow_resend(engine)
    challenge = client.post(
        "/auth/login",
        json={
            "email": "one@example.com",
            "full_name": "One",
            "password": "password123",
        },
    ).json()
    provider.fail = True
    response = client.post(
        "/auth/verify-otp",
        json={
            "email": "one@example.com",
            "challenge_id": challenge["challenge_id"],
            "code": "123456",
        },
    )
    assert response.status_code == 503
    with engine.connect() as conn:
        assert all(
            value == 0
            for value in conn.execute(
                select(tables.auth_challenges.c.attempts)
            ).scalars()
        )


@pytest.mark.parametrize("confirm", [False, True])
def test_duplicate_signup_rejected_without_replacing_password(api, confirm):
    client, provider, _engine = api
    if confirm:
        register(api)
    else:
        client.post(
            "/auth/signup",
            json={
                "email": "one@example.com",
                "full_name": "Original",
                "password": "password123",
            },
        ).raise_for_status()
    response = client.post(
        "/auth/signup",
        json={
            "email": " ONE@EXAMPLE.COM ",
            "full_name": "Replacement",
            "password": "different-password",
        },
    )
    assert response.status_code == 409
    assert "already registered" in response.json()["detail"]
    assert (
        len(provider.users) == 1
        and provider.users["one@example.com"]["password"] == "password123"
    )


def test_signup_cannot_verify_with_six_digit_code(api):
    client, provider, engine = api
    body = client.post(
        "/auth/signup",
        json={
            "email": "one@example.com",
            "full_name": "One",
            "password": "password123",
        },
    ).json()
    response = client.post(
        "/auth/verify-otp",
        json={
            "email": "one@example.com",
            "challenge_id": body["challenge_id"],
            "code": "123456",
        },
    )
    assert response.status_code == 400 and "link" in response.json()["detail"]
    assert provider.users["one@example.com"]["email_confirmed_at"] is None
    with engine.connect() as conn:
        assert conn.execute(select(tables.verified_sessions)).all() == []


def test_email_confirmation_does_not_log_user_in(api):
    client, provider, engine = api
    client.post(
        "/auth/signup",
        json={
            "email": "one@example.com",
            "full_name": "One",
            "password": "password123",
        },
    ).raise_for_status()
    token = provider.verify_code("one@example.com", "123456", "signup")["access_token"]
    assert (
        client.post("/auth/confirm-email", json={"access_token": "invalid"}).status_code
        == 401
    )
    result = client.post("/auth/confirm-email", json={"access_token": token})
    assert result.json() == {"email_confirmed": True}
    assert (
        client.get("/me", headers={"Authorization": "Bearer " + token}).status_code
        == 401
    )
    with engine.connect() as conn:
        assert conn.execute(select(tables.profiles)).first() is not None
        assert conn.execute(select(tables.verified_sessions)).all() == []


def test_resend_confirmation_keeps_link_flow(api):
    client, provider, engine = api
    body = client.post(
        "/auth/signup",
        json={
            "email": "one@example.com",
            "full_name": "One",
            "password": "password123",
        },
    ).json()
    payload = {"email": "one@example.com", "challenge_id": body["challenge_id"]}
    assert client.post("/auth/resend-confirmation", json=payload).status_code == 429
    allow_resend(engine)
    response = client.post("/auth/resend-confirmation", json=payload)
    assert (
        response.status_code == 200
        and response.json()["verification_method"] == "email_link"
    )
    assert provider.sent[-1] == ("one@example.com", "signup")


def test_boolean_directory_lookup_has_no_public_or_direct_auth_access(engine):
    from pathlib import Path
    from runpy import run_path

    from sqlalchemy import event
    from sqlalchemy.exc import DBAPIError
    from src.db.session import make_engine

    provision = run_path(
        str(Path(__file__).resolve().parents[2] / "database/ops/provision_api_role.py")
    )["provision"]
    role = "lookup_test_" + uuid4().hex[:12]
    with engine.begin() as conn:
        assert conn.execute(text("SELECT to_regnamespace('auth')")).scalar_one() is None
        conn.execute(text("CREATE SCHEMA auth"))
        conn.execute(text("CREATE TABLE auth.users (email text PRIMARY KEY)"))
        conn.execute(text("INSERT INTO auth.users VALUES ('existing@example.com')"))
        provision(conn.connection.driver_connection, role)
    restricted = make_engine(engine.url, connect_args={"hostaddr": "127.0.0.1"})

    @event.listens_for(restricted, "connect")
    def select_role(connection, _record):
        connection.execute(f"SET ROLE {role}")
        connection.commit()

    try:
        with restricted.connect() as conn:
            assert accounts.email_is_registered(conn, "EXISTING@example.com") is True
            assert accounts.email_is_registered(conn, "missing@example.com") is False
        with pytest.raises(DBAPIError), restricted.begin() as conn:
            conn.execute(text("SELECT * FROM auth.users"))
    finally:
        restricted.dispose()
        with engine.begin() as conn:
            conn.execute(text(f"DROP OWNED BY {role}"))
            conn.execute(text(f"DROP ROLE {role}"))
            conn.execute(text("DROP TABLE auth.users"))
            conn.execute(text("DROP SCHEMA auth RESTRICT"))


def test_signup_provider_detects_sanitized_duplicate_and_sets_callback(monkeypatch):
    provider = SupabaseAuth("https://example.supabase.co", "test")
    monkeypatch.setenv("AUTH_EMAIL_REDIRECT_URL", "http://127.0.0.1:5173/auth/confirm")
    with patch.object(provider, "request", return_value={"identities": []}) as request:
        with pytest.raises(AuthProviderError) as error:
            provider.signup("one@example.com", "password123", "One")
        assert error.value.status == 409
        assert (
            "redirect_to=http%3A%2F%2F127.0.0.1%3A5173%2Fauth%2Fconfirm"
            in request.call_args.args[1]
        )


def test_two_accounts_preferences_watchlists_settings_are_separate(api):
    client, _, _ = api
    _, first = register(api)
    _, second = register(api, "two@example.com")
    assert (
        client.put(
            "/me/preferences",
            headers=first,
            json={"sectors": ["Technology"], "risk_level": "High"},
        ).status_code
        == 200
    )
    assert (
        client.put(
            "/me/notifications", headers=first, json={"confidence_score_enabled": False}
        ).status_code
        == 200
    )
    assert (
        client.post("/me/watchlist", headers=first, json={"ticker": "aapl"}).status_code
        == 201
    )
    assert client.get("/me/preferences", headers=second).json()["sectors"] == []
    assert (
        client.get("/me/notifications", headers=second).json()[
            "confidence_score_enabled"
        ]
        is True
    )
    assert client.get("/me/watchlist", headers=second).json() == []
    assert client.delete("/me/watchlist/AAPL", headers=second).status_code == 204
    assert client.get("/me/watchlist", headers=first).json() == [{"ticker": "AAPL"}]
    assert (
        client.put(
            "/me/preferences", headers=first, json={"sectors": ["not a sector"]}
        ).status_code
        == 422
    )


@pytest.mark.parametrize("action", ["logout", "password"])
def test_revocation_blocks_access_and_refresh(api, action):
    client, provider, engine = api
    body, headers = register(api)
    allow_resend(engine)
    pending = client.post(
        "/auth/login", json={"email": body["user"]["email"], "password": "password123"}
    ).json()
    if action == "password":
        assert (
            client.put(
                "/me/password",
                headers=headers,
                json={"old_password": "incorrect", "new_password": "newpassword"},
            ).status_code
            == 401
        )
        response = client.put(
            "/me/password",
            headers=headers,
            json={"old_password": "password123", "new_password": "newpassword"},
        )
    else:
        provider.fail = True  # Local revocation must work even during provider outage.
        response = client.post("/auth/logout", headers=headers)
    assert response.status_code in (200, 204)
    assert (
        client.post(
            "/auth/verify-otp",
            json={
                "email": body["user"]["email"],
                "challenge_id": pending["challenge_id"],
                "code": "123456",
            },
        ).status_code
        == 400
    )
    assert client.get("/me", headers=headers).status_code == 401
    assert (
        client.post(
            "/auth/refresh", json={"refresh_token": body["refresh_token"]}
        ).status_code
        == 401
    )


def test_recovery_session_cannot_read_data_and_resets_password(api):
    client, provider, engine = api
    body, normal = register(api)
    assert (
        client.post(
            "/auth/reset-password", headers=normal, json={"new_password": "newpassword"}
        ).status_code
        == 403
    )
    allow_resend(engine)
    challenge = client.post(
        "/auth/forgot-password", json={"email": body["user"]["email"]}
    ).json()
    assert challenge["verification_method"] == "email_link"
    assert (
        client.post(
            "/auth/verify-otp",
            json={
                "email": body["user"]["email"],
                "challenge_id": challenge["challenge_id"],
                "code": "123456",
            },
        ).status_code
        == 400
    )
    assert (
        client.post(
            "/auth/accept-recovery-link", json={"access_token": body["access_token"]}
        ).status_code
        == 400
    )  # An ordinary login JWT cannot approve password recovery.
    link = provider.verify_code(body["user"]["email"], "123456", "recovery")
    recovery = client.post(
        "/auth/accept-recovery-link", json={"access_token": link["access_token"]}
    )
    assert recovery.status_code == 200 and recovery.json() == {"recovery_ready": True}
    assert (
        client.post(
            "/auth/accept-recovery-link", json={"access_token": link["access_token"]}
        ).status_code
        == 400
    )  # The link is single-use in the app.
    headers = {"Authorization": "Bearer " + link["access_token"]}
    assert client.get("/me/preferences", headers=headers).status_code == 401
    assert (
        client.post(
            "/auth/reset-password",
            headers=headers,
            json={"new_password": "newpassword"},
        ).status_code
        == 200
    )
    assert client.get("/me", headers=normal).status_code == 401


def test_rls_for_non_owner_even_without_where_clause(api):
    _, _, engine = api
    first, _ = register(api)
    register(api, "two@example.com")
    role = "auth_test_" + uuid4().hex[:12]
    with engine.begin() as conn:
        conn.execute(text(f"CREATE ROLE {role} NOLOGIN NOBYPASSRLS"))
        conn.execute(text(f"GRANT USAGE ON SCHEMA app TO {role}"))
        conn.execute(text(f"GRANT SELECT ON app.profiles TO {role}"))
    try:
        with engine.begin() as conn:
            conn.execute(text(f"SET LOCAL ROLE {role}"))
            assert conn.execute(select(tables.profiles)).all() == []
            conn.execute(
                text("SELECT set_config('request.jwt.claim.sub', :id, true)"),
                {"id": first["user"]["id"]},
            )
            assert [str(row.id) for row in conn.execute(select(tables.profiles))] == [
                first["user"]["id"]
            ]
        with engine.begin() as conn:
            conn.execute(text(f"SET LOCAL ROLE {role}"))
            assert (
                conn.execute(select(tables.profiles)).all() == []
            )  # Pool context did not leak.
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP OWNED BY {role}"))
            conn.execute(text(f"DROP ROLE {role}"))


def test_full_flow_with_restricted_api_group(api):
    from pathlib import Path
    from runpy import run_path

    from sqlalchemy import event
    from sqlalchemy.exc import DBAPIError
    from src.db.session import make_engine

    client, _, engine = api
    role = "api_test_" + uuid4().hex[:12]
    provision = run_path(
        str(Path(__file__).resolve().parents[2] / "database/ops/provision_api_role.py")
    )["provision"]
    with engine.begin() as conn:
        provision(conn.connection.driver_connection, role)
    restricted = make_engine(engine.url, connect_args={"hostaddr": "127.0.0.1"})

    @event.listens_for(restricted, "connect")
    def select_role(connection, _record):
        connection.execute(f"SET ROLE {role}")
        connection.commit()

    app.dependency_overrides[accounts.get_engine] = lambda: restricted
    try:
        _, headers = register(api)
        assert (
            client.put(
                "/me/preferences", headers=headers, json={"sectors": ["Technology"]}
            ).status_code
            == 200
        )
        assert (
            client.post(
                "/me/watchlist", headers=headers, json={"ticker": "MSFT"}
            ).status_code
            == 201
        )
        assert client.get("/me/watchlist", headers=headers).json() == [
            {"ticker": "MSFT"}
        ]
        with restricted.begin() as conn:
            assert conn.execute(select(tables.profiles)).all() == []
        with pytest.raises(DBAPIError), restricted.begin() as conn:
            conn.execute(text("DELETE FROM perennial.publications"))
        with pytest.raises(DBAPIError), restricted.begin() as conn:
            conn.execute(text("SELECT * FROM perennial.users"))
    finally:
        app.dependency_overrides[accounts.get_engine] = lambda: engine
        restricted.dispose()
        with engine.begin() as conn:
            conn.execute(text(f"DROP OWNED BY {role}"))
            conn.execute(text(f"DROP ROLE {role}"))


def test_invalid_fields_never_echo_passwords(api):
    client, _, _ = api
    response = client.post(
        "/auth/login", json={"email": "invalid", "password": "secret"}
    )
    assert response.status_code == 422
    assert "secret" not in response.text
    assert response.json()["field_errors"] == {
        "email": "Enter a valid email address in the format name@domain.com.",
        "password": "Use at least 8 characters for your password.",
    }


def test_signup_field_errors_are_specific_and_do_not_create_accounts(api):
    client, provider, _ = api
    response = client.post(
        "/auth/signup",
        json={"email": "invalid", "full_name": "   ", "password": "secret"},
    )
    assert response.status_code == 422
    assert set(response.json()["field_errors"]) == {"email", "full_name", "password"}
    assert response.json()["code"] == "invalid_fields"
    assert "secret" not in response.text
    assert not provider.users


def test_provider_field_errors_reach_the_client(api):
    client, provider, _ = api
    with patch.object(
        provider, "signup", side_effect=AuthProviderError(422, "weak_password")
    ):
        response = client.post(
            "/auth/signup",
            json={
                "email": "new@example.com",
                "full_name": "Ád",
                "password": "password123",
            },
        )
    assert response.status_code == 422
    assert response.json()["code"] == "weak_password"
    assert "password" in response.json()["field_errors"]
    assert "password123" not in response.text
    assert not provider.users


@pytest.mark.parametrize(
    "code,status,field,expected",
    [
        ("weak_password", 422, "password", "longer, unique password"),
        ("email_address_invalid", 422, "email", "another valid email"),
        ("email_exists", 409, "email", "already registered"),
        ("user_already_exists", 409, "email", "already registered"),
        ("email_address_not_authorized", 503, None, "isn't fully set up"),
        ("email_provider_disabled", 503, None, "registration is currently unavailable"),
        ("signup_disabled", 503, None, "registration is currently unavailable"),
        ("over_email_send_rate_limit", 429, None, "can't send another email"),
        ("validation_failed", 422, None, "couldn't validate"),
        ("unknown-secret-sentinel", 401, None, "Invalid email, password, or session"),
        ({"secret": "sentinel"}, 401, None, "Invalid email, password, or session"),
    ],
)
def test_provider_codes_have_safe_actionable_messages(code, status, field, expected):
    provider = SupabaseAuth("https://example.supabase.co", "secret-sentinel")
    response = MagicMock(status_code=429 if status == 429 else 422)
    response.json.return_value = {"error_code": code, "msg": "secret-sentinel"}
    response.__enter__.return_value = response
    with (
        patch("requests.request", return_value=response),
        pytest.raises(AuthProviderError) as error,
    ):
        provider.request("POST", "signup", {"password": "secret-sentinel"})
    result = error.value
    assert result.status == status
    assert expected in result.message
    assert set(result.field_errors) == ({field} if field else set())
    assert "secret-sentinel" not in str(result)
    assert "secret-sentinel" not in result.code


@pytest.mark.parametrize(
    "reasons,expected",
    [
        (["length"], "Choose a longer password."),
        (["characters"], "uppercase and lowercase letters, a number, and a symbol"),
        (["pwned"], "known to be compromised"),
        (["length", "secret-sentinel", {"unsafe": True}], "Choose a longer password."),
        ("secret-sentinel", "longer, unique password"),
    ],
)
def test_weak_password_reasons_are_allowlisted(reasons, expected):
    provider = SupabaseAuth("https://example.supabase.co", "test")
    response = MagicMock(status_code=422)
    response.json.return_value = {
        "error_code": "weak_password",
        "msg": "secret-sentinel",
        "weak_password": {"reasons": reasons},
    }
    response.__enter__.return_value = response
    with (
        patch("requests.request", return_value=response),
        pytest.raises(AuthProviderError) as error,
    ):
        provider.request("POST", "signup")
    assert expected in error.value.field_errors["password"]
    assert "secret-sentinel" not in str(error.value)


def test_db_password_placeholder_is_encoded_and_never_logged(tmp_path):
    env = tmp_path / ".env"
    env.write_text(
        'DATABASE_URL=postgresql://postgres.example:[YOUR-PASSWORD]@localhost:5432/postgres\nSUPABASE_DB_PASSWORD="a@b:/?#%"\n'
    )
    assert get_database_url({}, env).password == "a@b:/?#%"
    assert (
        get_database_url({"SUPABASE_DB_PASSWORD": "override"}, env).password
        == "override"
    )
    with pytest.raises(DatabaseConfigError, match="SUPABASE_DB_PASSWORD"):
        get_database_url(
            {"DATABASE_URL": "postgresql://user:[YOUR-PASSWORD]@localhost/postgres"},
            tmp_path / "missing",
        )


def test_provider_never_creates_users_for_login_otp():
    provider = SupabaseAuth("https://example.supabase.co", "test")
    with patch.object(provider, "request", return_value={}) as request:
        provider.send_code("one@example.com", "login")
        request.assert_called_once_with(
            "POST", "otp", {"email": "one@example.com", "create_user": False}
        )


def test_password_check_discards_native_session():
    provider = SupabaseAuth("https://example.supabase.co", "test")
    with patch.object(
        provider,
        "request",
        side_effect=[{"access_token": "temporary", "user": {"id": "one"}}, {}],
    ) as request:
        assert provider.check_password("one@example.com", "password123") == {
            "id": "one"
        }
        assert request.call_args_list[1].args == ("POST", "logout?scope=local")


@pytest.mark.parametrize(
    "changes",
    [
        {"exp": 1},
        {"exp": True},
        {"exp": float("nan")},
        {"iss": "https://wrong.example/auth/v1"},
        {"aud": "anon"},
        {"role": "service_role"},
        {"session_id": "invalid"},
        {"nbf": time.time() + 9999},
    ],
)
def test_invalid_claims_denied_after_provider_verification(changes):
    uid, sid = uuid4(), uuid4()
    provider = SupabaseAuth("https://example.supabase.co", "test")
    with patch.object(
        provider,
        "request",
        return_value={
            "id": str(uid),
            "email": "one@example.com",
            "email_confirmed_at": "yes",
        },
    ):
        with pytest.raises(AuthProviderError):
            provider.identity(jwt(uid, sid, **changes))


def test_signature_validation_is_authoritative_and_provider_errors_sanitized():
    provider = SupabaseAuth("https://example.supabase.co", "secret-sentinel")
    response = MagicMock(status_code=401)
    response.json.return_value = {"message": "secret-sentinel should never be exposed"}
    response.__enter__.return_value = response
    with patch("requests.request", return_value=response) as request:
        with pytest.raises(AuthProviderError) as error:
            provider.identity(jwt(uuid4(), uuid4()))
        assert error.value.status == 401
        assert "secret-sentinel" not in str(error.value)
        assert request.call_args.kwargs["allow_redirects"] is False


def test_confirmations_disabled_fails_closed():
    provider = SupabaseAuth("https://example.supabase.co", "test")
    with patch.object(
        provider, "request", side_effect=[{"access_token": "temporary"}, {}]
    ):
        with pytest.raises(AuthProviderError):
            provider.signup("one@example.com", "password123", "One")
