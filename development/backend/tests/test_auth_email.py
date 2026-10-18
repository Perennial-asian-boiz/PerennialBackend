"""Real account transactions with synthetic, offline Resend responses."""

from datetime import timedelta
from unittest.mock import Mock

import pytest
import requests
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import func, select, update
from src.api import app as accounts
from src.api import mail
from src.api.security import verify_secret
from src.db import models

EMAIL = "user@example.com"
PASSWORD = "Synthetic-password-123"
API_KEY = "re_SYNTHETIC_TEST_ONLY"
CODE = "123456"


@pytest.fixture(autouse=True)
def synthetic_email_environment(monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("EMAIL_PROVIDER", "resend")
    monkeypatch.setenv("RESEND_API_KEY", API_KEY)
    monkeypatch.setenv("EMAIL_FROM", "Perennial <noreply@example.com>")
    # Any unmocked email request is a test failure, never a real send.
    monkeypatch.setattr(
        mail.requests,
        "post",
        Mock(side_effect=AssertionError("Real mail is forbidden")),
    )
    monkeypatch.setattr(accounts, "new_otp", lambda: CODE)


@pytest.fixture
def provider(monkeypatch):
    response = Mock(status_code=200)
    response.json.return_value = {"id": "synthetic-message-id"}
    context = Mock()
    context.__enter__ = Mock(return_value=response)
    context.__exit__ = Mock(return_value=False)
    post = Mock(return_value=context)
    monkeypatch.setattr(mail.requests, "post", post)
    return post, response


def create_user(engine):
    return accounts.signup(
        accounts.SignupRequest(email=EMAIL, full_name="Test User", password=PASSWORD),
        engine,
    )


def age_code(engine):
    with engine.begin() as connection:
        connection.execute(
            update(models.email_verification_codes).values(
                created_at=accounts.utc_now() - timedelta(seconds=31)
            )
        )


def count_rows(engine, table):
    with engine.connect() as connection:
        return connection.execute(select(func.count()).select_from(table)).scalar_one()


def test_resend_receives_only_the_intended_recipient_and_expiry(provider):
    post, _ = provider
    assert mail.send_verification_email(EMAIL, CODE, expires_minutes=15) is True
    args, kwargs = post.call_args
    assert args == ("https://api.resend.com/emails",)
    assert kwargs["headers"]["Authorization"] == f"Bearer {API_KEY}"
    assert kwargs["json"]["to"] == [EMAIL]
    assert CODE in kwargs["json"]["text"]
    assert "15 minutes" in kwargs["json"]["text"]
    assert kwargs["timeout"] == (3.05, 10)
    assert kwargs["allow_redirects"] is False


@pytest.mark.parametrize("status_code", [302, 400, 401, 403, 429, 500])
def test_provider_rejections_do_not_expose_response_content(
    provider, status_code, caplog
):
    _, response = provider
    response.status_code = status_code
    response.text = f"Provider echoed secret {API_KEY} and code {CODE}"
    with pytest.raises(mail.EmailDeliveryError) as error:
        mail.send_verification_email(EMAIL, CODE, expires_minutes=15)
    assert API_KEY not in str(error.value) + caplog.text
    assert CODE not in str(error.value) + caplog.text


@pytest.mark.parametrize("body", [{}, {"id": ""}, {"id": None}, [], "not an object"])
def test_malformed_success_is_not_reported_as_sent(provider, body):
    _, response = provider
    response.json.return_value = body
    with pytest.raises(mail.EmailDeliveryError):
        mail.send_verification_email(EMAIL, CODE, expires_minutes=15)


def test_provider_timeouts_and_bad_json_are_sanitized(provider):
    post, response = provider
    post.side_effect = requests.Timeout(f"{API_KEY} {CODE}")
    with pytest.raises(mail.EmailDeliveryError, match="Unable to send") as error:
        mail.send_verification_email(EMAIL, CODE, expires_minutes=15)
    assert API_KEY not in str(error.value)
    assert CODE not in str(error.value)
    post.side_effect = None
    response.json.side_effect = ValueError(f"{API_KEY} {CODE}")
    with pytest.raises(mail.EmailDeliveryError, match="Unable to send"):
        mail.send_verification_email(EMAIL, CODE, expires_minutes=15)


@pytest.mark.parametrize("setting", ["RESEND_API_KEY", "EMAIL_FROM"])
def test_missing_credentials_never_fall_back_to_development_codes(monkeypatch, setting):
    monkeypatch.delenv(setting)
    with pytest.raises(mail.EmailDeliveryError, match="not configured"):
        mail.send_verification_email(EMAIL, CODE, expires_minutes=15)
    mail.requests.post.assert_not_called()


def test_development_delivery_requires_explicit_opt_in(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "development")
    assert mail.send_verification_email(EMAIL, CODE, expires_minutes=15) is False
    monkeypatch.setenv("APP_ENV", "production")
    with pytest.raises(mail.EmailDeliveryError):
        mail.send_verification_email(EMAIL, CODE, expires_minutes=15)
    mail.requests.post.assert_not_called()


def test_resend_is_the_default_provider(monkeypatch, provider):
    monkeypatch.delenv("EMAIL_PROVIDER")
    assert mail.send_verification_email(EMAIL, CODE, expires_minutes=15) is True


@pytest.mark.parametrize("email", ["bad", "one@two", "a@b.com,c@d.com", "a\n@b.com"])
def test_invalid_recipient_is_rejected_before_sending(email):
    with pytest.raises(ValidationError):
        accounts.SignupRequest(email=email, full_name="Test User", password=PASSWORD)
    mail.requests.post.assert_not_called()


@pytest.mark.db
def test_signup_sends_real_mode_code_without_returning_it(engine, provider):
    response = create_user(engine)
    assert response.development_code is None
    assert response.verification_required
    with engine.connect() as connection:
        row = (
            connection.execute(select(models.email_verification_codes)).mappings().one()
        )
        assert row.code_hash != CODE
        assert verify_secret(CODE, row.code_hash)
        assert row.expires_at - row.created_at == timedelta(minutes=15)
    assert count_rows(engine, models.auth_sessions) == 0


@pytest.mark.db
def test_failed_signup_delivery_rolls_back_the_account(engine, provider):
    _, response = provider
    response.status_code = 403
    with pytest.raises(HTTPException) as error:
        create_user(engine)
    assert error.value.status_code == 503
    assert count_rows(engine, models.users) == 0
    assert count_rows(engine, models.email_verification_codes) == 0


@pytest.mark.db
def test_login_and_resend_both_deliver_without_issuing_a_session(engine, provider):
    post, _ = provider
    create_user(engine)
    accounts.verify_otp(accounts.VerifyOTPRequest(email=EMAIL, code=CODE), engine)
    session_count = count_rows(engine, models.auth_sessions)
    age_code(engine)
    login = accounts.login(
        accounts.LoginRequest(email=EMAIL, password=PASSWORD), engine
    )
    assert login.development_code is None
    assert login.verification_required
    assert count_rows(engine, models.auth_sessions) == session_count
    age_code(engine)
    resend = accounts.resend_otp(accounts.ResendOTPRequest(email=EMAIL), engine)
    assert resend.development_code is None
    assert post.call_count == 3


@pytest.mark.db
def test_failed_resend_keeps_the_previous_code_valid(engine, provider):
    _, response = provider
    create_user(engine)
    age_code(engine)
    response.status_code = 500
    with pytest.raises(HTTPException) as error:
        accounts.resend_otp(accounts.ResendOTPRequest(email=EMAIL), engine)
    assert error.value.status_code == 503
    assert count_rows(engine, models.email_verification_codes) == 1
    verified = accounts.verify_otp(
        accounts.VerifyOTPRequest(email=EMAIL, code=CODE), engine
    )
    assert verified.user.is_verified


@pytest.mark.db
def test_five_failed_attempts_persist_and_lock_the_code(engine, provider):
    create_user(engine)
    for _ in range(5):
        with pytest.raises(HTTPException):
            accounts.verify_otp(
                accounts.VerifyOTPRequest(email=EMAIL, code="000000"), engine
            )
    with engine.connect() as connection:
        assert (
            connection.execute(
                select(models.email_verification_codes.c.attempts)
            ).scalar_one()
            == 5
        )
    with pytest.raises(HTTPException):
        accounts.verify_otp(accounts.VerifyOTPRequest(email=EMAIL, code=CODE), engine)
    assert count_rows(engine, models.auth_sessions) == 0


@pytest.mark.db
def test_otp_expires_and_cannot_be_replayed(engine, provider):
    create_user(engine)
    with engine.begin() as connection:
        connection.execute(
            update(models.email_verification_codes).values(
                expires_at=accounts.utc_now() - timedelta(seconds=1)
            )
        )
    with pytest.raises(HTTPException):
        accounts.verify_otp(accounts.VerifyOTPRequest(email=EMAIL, code=CODE), engine)
    age_code(engine)
    accounts.resend_otp(accounts.ResendOTPRequest(email=EMAIL), engine)
    accounts.verify_otp(accounts.VerifyOTPRequest(email=EMAIL, code=CODE), engine)
    with pytest.raises(HTTPException):
        accounts.verify_otp(accounts.VerifyOTPRequest(email=EMAIL, code=CODE), engine)
    assert count_rows(engine, models.auth_sessions) == 1


@pytest.mark.db
def test_delivery_limits_are_enforced_before_contacting_resend(engine, provider):
    post, _ = provider
    create_user(engine)
    with pytest.raises(HTTPException) as error:
        accounts.resend_otp(accounts.ResendOTPRequest(email=EMAIL), engine)
    assert error.value.status_code == 429
    assert int(error.value.headers["Retry-After"]) > 0
    assert post.call_count == 1
    for _ in range(4):
        age_code(engine)
        accounts.resend_otp(accounts.ResendOTPRequest(email=EMAIL), engine)
    age_code(engine)
    with pytest.raises(HTTPException) as error:
        accounts.resend_otp(accounts.ResendOTPRequest(email=EMAIL), engine)
    assert error.value.status_code == 429
    assert post.call_count == 5
