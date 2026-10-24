"""Supabase Auth endpoints and account data served exclusively through FastAPI.

Hardened for #22 (docs/work-items/m5-pr22-auth-hardening-packet.md, Part 1):
- J2: no connection, transaction or lock is held across a provider call. Each
  email-sending route reserves (a short transaction under the email lock),
  calls Supabase with nothing checked out, then finishes (another short
  transaction that re-reads state and never revives what a concurrent request
  consumed).
- J1: challenges supersede and cap only their own flow; no recovery email is
  sent over an open login.
- J3: revoke takes the same email lock as verify-otp.
- J6: one email normalization, strip() then lower().
- J7: every challenge and session time is the database clock.
- AH-9: an app-wide email budget, with ceilings for the flows strangers
  control and a per-client share, under one advisory lock.
"""

import hashlib
import logging
import os
import re
import secrets
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import delete, func, insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from src.api import account_models as tables
from src.api.read import (
    APIError,
    auth_rate_limit,
    client_key_of,
    credential_rate_limit,
    limiter_of,
    refresh_rate_limit,
    session_rate_limit,
)
from src.api.supabase_auth import CLOCK_SKEW_SECONDS, AuthProviderError, SupabaseAuth
from src.db import models
from src.db.session import make_engine
from src.ingestion.schemas import _TICKER

logger = logging.getLogger(__name__)
router = APIRouter()
OTP_MINUTES = 15
CHALLENGE_MAX_MINUTES = 60  # J7: no resend extends a challenge past this
RECOVERY_SESSION_MINUTES = 15  # J7: a reset session's local lifetime
RESEND_COOLDOWN_SECONDS = 60
MAX_ATTEMPTS = 5
MAX_EMAILS_PER_HOUR = 60  # per email, per flow (login, signup)
MAX_RECOVERY_EMAILS_PER_HOUR = 5  # J1: Supabase's shared per-user send timer
# AH-9g: the app-wide email budget's advisory lock, distinct from the other
# fixed keys (sources 170010+, backup 170097, pipeline 170098, publication
# 170099, Congress history 170100).
EMAIL_BUDGET_LOCK = 170101
PASSWORD_REQUIREMENTS_MESSAGE = (
    "Use 8–200 characters, including an uppercase letter, a lowercase letter, "
    "and a number."
)
START_AGAIN = "Verification expired or invalid. Please start again."
SEND_REFUSED = "We can't send a code right now. Please try again in a few minutes."
RECOVERY_EXPIRED = "This reset session has expired. Request a new reset link."
challenges = tables.auth_challenges


def now():
    """The app's clock, for callers and tests. No challenge or session decision
    uses it: those times are the database's (J7)."""
    return datetime.now(timezone.utc)


def normalize_email(email):
    """J6: the one email normalization. Case folding would map ß to ss, another mailbox."""
    return email.strip().lower()


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


class APIDatabaseURLMissing(RuntimeError):
    """Fixed text: names the setting, never a URL or credential."""

    def __init__(self):
        super().__init__("API_DATABASE_URL must name the dedicated API login")


@lru_cache
def get_engine() -> Engine:
    # The restricted API login only, everywhere: never DATABASE_URL (migrations),
    # whose owner login the startup check would refuse anyway (RB-6b).
    raw = os.getenv("API_DATABASE_URL", "").strip()
    if not raw:
        raise APIDatabaseURLMissing()
    return make_engine(raw, workload="api")


def get_provider() -> SupabaseAuth:
    return SupabaseAuth.from_environment()


EngineDep = Annotated[Engine, Depends(get_engine)]
ProviderDep = Annotated[SupabaseAuth, Depends(get_provider)]


class EmailRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)

    @field_validator("email")
    @classmethod
    def normalized(cls, value):
        value = normalize_email(value)
        if not re.fullmatch(r"[^\s@<>(),;]+@[^\s@<>(),;]+\.[^\s@<>(),;]+", value):
            raise ValueError("Email is not valid")
        return value


class SignupRequest(EmailRequest):
    full_name: str = Field(min_length=1, max_length=200)
    password: str = Field(min_length=8, max_length=200)

    @field_validator("password")
    @classmethod
    def validate_password_requirements(cls, value):
        return validate_password(value)

    @field_validator("full_name")
    @classmethod
    def clean_name(cls, value):
        value = " ".join(value.split())
        if not value:
            raise ValueError("Name is required")
        return value


class LoginRequest(EmailRequest):
    password: str = Field(min_length=8, max_length=200)


class ChallengeRequest(EmailRequest):
    challenge_id: str = Field(min_length=32, max_length=128)


class VerifyRequest(ChallengeRequest):
    code: str = Field(pattern=r"^\d{6}$")


class ChallengeResponse(BaseModel):
    challenge_id: str
    flow: Literal["signup", "login", "recovery"]
    verification_required: bool = True
    resend_after_seconds: int = 0
    verification_method: Literal["email_link", "email_otp"] = "email_otp"


class PasswordResetRequestResponse(BaseModel):
    message: str = (
        "If an account exists for this email, password reset instructions will be sent."
    )


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    email: str
    full_name: str
    is_verified: bool = True
    onboarding_completed: bool = False


class AuthResponse(BaseModel):
    access_token: str
    refresh_token: str
    expires_at: int
    token_type: Literal["bearer"] = "bearer"
    user: UserResponse
    flow: str = "login"


class Principal(UserResponse):
    session_id: UUID
    access_token: str = Field(repr=False, exclude=True)
    recovery_only: bool = False


class RefreshRequest(BaseModel):
    refresh_token: str = Field(min_length=1, max_length=8192)


class ConfirmEmailRequest(BaseModel):
    access_token: str = Field(min_length=1, max_length=16384, repr=False)


class RecoveryLinkRequest(BaseModel):
    access_token: str = Field(min_length=1, max_length=16384, repr=False)


class NewPassword(BaseModel):
    new_password: str = Field(min_length=8, max_length=200)

    @field_validator("new_password")
    @classmethod
    def validate_password_requirements(cls, value):
        return validate_password(value)


class ChangePassword(NewPassword):
    old_password: str = Field(min_length=1, max_length=200)


def validate_password(value):
    has_uppercase = re.search(r"[A-Z]", value)
    has_lowercase = re.search(r"[a-z]", value)
    has_number = re.search(r"\d", value)
    if not (has_uppercase and has_lowercase and has_number):
        raise ValueError(PASSWORD_REQUIREMENTS_MESSAGE)
    return value


class Preferences(BaseModel):
    sectors: list[str] = Field(default_factory=list, max_length=11)
    risk_level: Literal["Conservative", "Moderate", "High"] | None = None

    @field_validator("sectors")
    @classmethod
    def clean_sectors(cls, values):
        allowed = {
            "Technology",
            "Financials",
            "Healthcare",
            "Industrials",
            "Consumer Discretionary",
            "Consumer Staples",
            "Energy",
            "Utilities",
            "Materials",
            "Communication Services",
            "Real Estate",
        }
        values = list(dict.fromkeys(" ".join(value.split()) for value in values))
        if any(value not in allowed for value in values):
            raise ValueError("Unknown sector")
        return values


class Notifications(BaseModel):
    confidence_score_enabled: bool = True
    earnings_alerts_enabled: bool = True


class WatchlistItem(BaseModel):
    ticker: str = Field(min_length=1, max_length=20)

    @field_validator("ticker")
    @classmethod
    def clean_ticker(cls, value):
        # The read API's ticker rule (R1's /v1/securities/{symbol}).
        value = value.strip().upper()
        if not _TICKER.fullmatch(value):
            raise ValueError("Ticker is not valid")
        return value


def set_subject(connection, user_id):
    """The one place the RLS subject is set: a UUID, transaction-local.

    Every policy casts the subject to uuid, so anything else is refused here,
    before any statement, rather than failing in the database (change 6).
    """
    subject = str(UUID(str(user_id)))
    connection.execute(
        text("SELECT set_config('request.jwt.claim.sub', :subject, true)"),
        {"subject": subject},
    )


@contextmanager
def user_connection(engine, user_id):
    UUID(str(user_id))  # refuse a bad subject before opening a transaction
    with engine.begin() as connection:
        set_subject(connection, user_id)
        yield connection


def lock_email(connection, email):
    """The per-email advisory lock: the only key for every flow and revoke (J3)."""
    connection.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"perennial-auth:{normalize_email(email)}"},
    )


def check_email_limit(connection, email, flow):
    """J1: this email's sends in this flow over the last hour, on the database clock."""
    sent = connection.execute(
        select(func.coalesce(func.sum(challenges.c.send_count), 0)).where(
            challenges.c.email == email,
            challenges.c.flow == flow,
            challenges.c.last_sent_at > func.now() - timedelta(hours=1),
        )
    ).scalar_one()
    cap = MAX_RECOVERY_EMAILS_PER_HOUR if flow == "recovery" else MAX_EMAILS_PER_HOUR
    if sent >= cap:
        raise HTTPException(
            429, "Too many verification emails. Please try again later."
        )


class SendRefused(Exception):
    """The app-wide email budget, a ceiling or the client's share refused a send."""


def send_refused():
    return HTTPException(429, SEND_REFUSED, headers={"Retry-After": "60"})


def reserve_email(connection, request, flow):
    """AH-9: take the budget lock after the email lock, count the last hour's
    sends (every worker shares the table), then charge the client's share last,
    only when a send is about to happen. Returns the client key charged."""
    limiter = limiter_of(request)
    config = limiter.config
    connection.execute(
        text("SELECT pg_advisory_xact_lock(:key)"), {"key": EMAIL_BUDGET_LOCK}
    )
    counts = dict(
        connection.execute(
            select(challenges.c.flow, func.sum(challenges.c.send_count))
            .where(challenges.c.last_sent_at > func.now() - timedelta(hours=1))
            .group_by(challenges.c.flow)
        )
        .tuples()
        .all()
    )
    ceiling = {
        "recovery": config.email_budget_recovery_per_hour,
        "signup": config.email_budget_signup_per_hour,
    }.get(flow)
    reason = None
    if sum(counts.values()) >= config.email_budget_per_hour:
        reason = "total"
    elif ceiling is not None and counts.get(flow, 0) >= ceiling:
        reason = "ceiling"
    else:
        key = client_key_of(request)
        if limiter.check("email", key) is None:
            return key
        reason = "share"
    limiter.count_event(f"email_send_refused reason={reason}")
    raise SendRefused()


def provider_refused(request, error):
    """AH-9e: Supabase's own 429 on an email-sending call may be project-wide."""
    if error.status != 429:
        return False
    limiter_of(request).count_event("email_send_refused reason=provider")
    return True


def challenge_response(token, flow):
    return ChallengeResponse(
        challenge_id=token,
        flow=flow,
        resend_after_seconds=RESEND_COOLDOWN_SECONDS,
        verification_method="email_otp" if flow == "login" else "email_link",
    )


def first_send(engine, request, email, flow, send, *, check=None, confirm=None):
    """J2's reserve, call, finish for a new challenge; returns its token, or None
    when check() says to send nothing.

    A failed send deletes the reserved row (so it never counts against a cap
    or the budget) and refunds the client's share, then re-raises. Success
    consumes only this flow's older open challenges (J1): a slower request never
    consumes a newer one.
    """
    with engine.begin() as connection:
        lock_email(connection, email)
        if check is not None and not check(connection):
            return None
        check_email_limit(connection, email, flow)
        key = reserve_email(connection, request, flow)
        token = secrets.token_urlsafe(32)
        connection.execute(
            insert(challenges).values(
                token_hash=digest(token),
                email=email,
                flow=flow,
                expires_at=func.now() + timedelta(minutes=OTP_MINUTES),
            )
        )

    def release():
        with engine.begin() as connection:
            lock_email(connection, email)
            connection.execute(
                delete(challenges).where(challenges.c.token_hash == digest(token))
            )
        limiter_of(request).refund("email", key)

    try:
        send()
    except Exception:
        release()
        raise
    with engine.begin() as connection:
        lock_email(connection, email)
        confirmed = confirm is None or confirm(connection)
        if confirmed:
            created = (
                select(challenges.c.created_at)
                .where(challenges.c.token_hash == digest(token))
                .scalar_subquery()
            )
            connection.execute(
                update(challenges)
                .where(
                    challenges.c.email == email,
                    challenges.c.flow == flow,
                    challenges.c.consumed_at.is_(None),
                    challenges.c.token_hash != digest(token),
                    challenges.c.created_at < created,
                )
                .values(consumed_at=func.now())
            )
    if not confirmed:
        release()
        raise AuthProviderError()
    return token


def email_is_registered(connection, email):
    return connection.execute(
        text("SELECT app.email_is_registered(:email)"), {"email": email}
    ).scalar_one()


def pending_challenge(connection, payload):
    """The challenge under the email lock, with its times read on the database clock."""
    lock_email(connection, payload.email)
    row = (
        connection.execute(
            select(
                challenges,
                (challenges.c.expires_at > func.now()).label("live"),
                func.extract("epoch", func.now() - challenges.c.last_sent_at).label(
                    "elapsed"
                ),
                (
                    func.least(
                        func.now() + timedelta(minutes=OTP_MINUTES),
                        challenges.c.created_at
                        + timedelta(minutes=CHALLENGE_MAX_MINUTES),
                    )
                    > func.now()
                ).label("extendable"),
            )
            .where(
                challenges.c.token_hash == digest(payload.challenge_id),
                challenges.c.email == payload.email,
            )
            .with_for_update(of=challenges)
        )
        .mappings()
        .first()
    )
    if row is None or row.consumed_at or not row.live or row.attempts >= MAX_ATTEMPTS:
        raise HTTPException(400, START_AGAIN)
    return row


def check_resend_cooldown(row):
    remaining = RESEND_COOLDOWN_SECONDS - int(row.elapsed)
    if remaining > 0:
        raise HTTPException(
            429,
            f"Please wait {remaining} seconds before requesting another code.",
            headers={"Retry-After": str(remaining)},
        )


def user_from_identity(identity):
    metadata = identity.get("user_metadata") or {}
    return UserResponse(
        id=identity["id"],
        email=normalize_email(identity["email"]),
        full_name=str(metadata.get("full_name") or "")[:200],
    )


def sync_profile(connection, identity):
    UUID(str(identity.get("id")))  # refused before any statement (change 6)
    user = user_from_identity(identity)
    set_subject(connection, user.id)
    statement = pg_insert(tables.profiles).values(
        id=user.id, email=user.email, full_name=user.full_name
    )
    connection.execute(
        statement.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "email": user.email,
                "full_name": user.full_name,
                "updated_at": func.now(),
            },
        )
    )
    return UserResponse.model_validate(
        connection.execute(
            select(tables.profiles).where(tables.profiles.c.id == user.id)
        )
        .mappings()
        .one()
    )


def authenticated_session(token, engine, provider):
    identity, session_id = provider.identity(token)
    user_id = UUID(identity["id"])
    sessions = tables.verified_sessions
    with user_connection(engine, user_id) as connection:
        session = (
            connection.execute(
                select(
                    sessions.c.recovery_only,
                    (
                        sessions.c.created_at
                        < func.now() - timedelta(minutes=RECOVERY_SESSION_MINUTES)
                    ).label("aged"),
                ).where(
                    sessions.c.session_id == session_id,
                    sessions.c.user_id == user_id,
                    sessions.c.revoked_at.is_(None),
                )
            )
            .mappings()
            .first()
        )
        profile = (
            connection.execute(
                select(tables.profiles).where(tables.profiles.c.id == user_id)
            )
            .mappings()
            .first()
        )
    if session is None or profile is None:
        raise HTTPException(401, "Complete email verification or sign in again.")
    if session.recovery_only and session.aged:  # J7: reset sessions are short-lived
        raise HTTPException(401, RECOVERY_EXPIRED)
    return Principal(
        **dict(profile),
        session_id=session_id,
        access_token=token,
        recovery_only=session.recovery_only,
    )


def get_verified_session(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(HTTPBearer(auto_error=False))
    ],
    engine: EngineDep,
    provider: ProviderDep,
) -> Principal:
    if credentials is None or credentials.scheme.casefold() != "bearer":
        raise HTTPException(401, "Authentication required")
    return authenticated_session(credentials.credentials, engine, provider)


def get_current_user(
    user: Annotated[Principal, Depends(get_verified_session)],
) -> Principal:
    if user.recovery_only:
        raise HTTPException(401, "Complete your password reset before continuing.")
    return user


UserDep = Annotated[Principal, Depends(get_current_user)]


def token_response(body, user, flow="login"):
    import base64
    import json

    try:
        token = body["access_token"]
        part = token.split(".")[1]
        expires = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))[
            "exp"
        ]
        return AuthResponse(
            access_token=token,
            refresh_token=body["refresh_token"],
            expires_at=expires,
            user=user,
            flow=flow,
        )
    except (KeyError, ValueError, TypeError, IndexError):
        raise AuthProviderError() from None


# Every route's first dependency is its rate limit (AH-3a). auth_rate_limit
# covers the two email-lookup routes, an enumeration oracle.
@router.post(
    "/auth/signup",
    response_model=ChallengeResponse,
    status_code=201,
    dependencies=[Depends(auth_rate_limit)],
)
def signup(
    payload: SignupRequest, request: Request, engine: EngineDep, provider: ProviderDep
):
    def check(connection):
        if email_is_registered(connection, payload.email):
            raise HTTPException(
                409, "This email is already registered. Please sign in instead."
            )
        return True

    try:
        token = first_send(
            engine,
            request,
            payload.email,
            "signup",
            lambda: provider.signup(payload.email, payload.password, payload.full_name),
            check=check,
            # Detect wrong-project configuration or a malformed provider success.
            confirm=lambda connection: email_is_registered(connection, payload.email),
        )
    except SendRefused:
        raise send_refused() from None
    except AuthProviderError as error:
        if provider_refused(request, error):
            raise send_refused() from None
        raise
    return challenge_response(token, "signup")


@router.post("/auth/confirm-email", dependencies=[Depends(credential_rate_limit)])
def confirm_email(
    payload: ConfirmEmailRequest, engine: EngineDep, provider: ProviderDep
):
    # The emailed link has already been consumed by Supabase's /verify endpoint.
    # Validate its JWT, sync only the profile, and never approve an app login.
    identity, _session_id = provider.identity(payload.access_token)
    email = normalize_email(identity["email"])
    with engine.begin() as connection:
        lock_email(connection, email)
        sync_profile(connection, identity)
        connection.execute(
            update(challenges)
            .where(
                challenges.c.email == email,
                challenges.c.flow == "signup",
                challenges.c.consumed_at.is_(None),
            )
            .values(consumed_at=func.now())
        )
    try:
        provider.logout(payload.access_token, scope="local")
    except AuthProviderError:
        pass  # No app session was approved; a valid password + login OTP is still required.
    return {"email_confirmed": True}


@router.post(
    "/auth/login",
    response_model=ChallengeResponse,
    dependencies=[Depends(credential_rate_limit)],
)
def login(
    payload: LoginRequest, request: Request, engine: EngineDep, provider: ProviderDep
):
    try:
        provider.check_password(payload.email, payload.password)
    except AuthProviderError as error:
        if error.code == "email_not_confirmed":
            raise HTTPException(
                403,
                "Your account hasn’t been confirmed yet. Open the confirmation link sent to your email, then sign in.",
            ) from None
        raise
    try:
        token = first_send(
            engine,
            request,
            payload.email,
            "login",
            lambda: provider.send_code(payload.email, "login"),
        )
    except SendRefused:
        raise send_refused() from None
    except AuthProviderError as error:
        if provider_refused(request, error):
            raise send_refused() from None
        raise
    return challenge_response(token, "login")


@router.post(
    "/auth/forgot-password",
    response_model=PasswordResetRequestResponse,
    dependencies=[Depends(auth_rate_limit)],
)
def forgot_password(
    payload: EmailRequest, request: Request, engine: EngineDep, provider: ProviderDep
):
    """Every outcome but a validation error is the same 200 (the oracle fix)."""

    def check(connection):
        if not email_is_registered(connection, payload.email):
            return False
        # J1: whoever opened a login knows the password; never overwrite its code.
        open_login = connection.execute(
            select(challenges.c.token_hash).where(
                challenges.c.email == payload.email,
                challenges.c.flow == "login",
                challenges.c.consumed_at.is_(None),
                challenges.c.expires_at > func.now(),
            )
        ).first()
        return open_login is None

    try:
        first_send(
            engine,
            request,
            payload.email,
            "recovery",
            lambda: provider.send_code(payload.email, "recovery"),
            check=check,
        )
    except HTTPException as error:
        if error.status_code != 429:
            raise
        # Keep rate-limit outcomes indistinguishable from unknown emails.
    except SendRefused:
        pass
    except AuthProviderError as error:
        # Provider failures must not reveal whether an account exists either.
        if not provider_refused(request, error):
            limiter_of(request).log_event(
                "forgot_password_send_failed code=%s", error.code
            )
    return PasswordResetRequestResponse()


@router.post(
    "/auth/accept-recovery-link", dependencies=[Depends(credential_rate_limit)]
)
def accept_recovery_link(
    payload: RecoveryLinkRequest, engine: EngineDep, provider: ProviderDep
):
    try:
        identity, session_id, recovery_at = provider.recovery_identity(
            payload.access_token
        )
    except AuthProviderError as error:
        if error.status == 401:
            raise HTTPException(400, "This reset link is invalid or expired.") from None
        raise
    email = normalize_email(identity["email"])
    with engine.begin() as connection:
        lock_email(connection, email)
        challenge = (
            connection.execute(
                select(
                    challenges.c.token_hash,
                    func.extract("epoch", challenges.c.last_sent_at).label("sent_at"),
                )
                .where(
                    challenges.c.email == email,
                    challenges.c.flow == "recovery",
                    challenges.c.consumed_at.is_(None),
                    challenges.c.expires_at > func.now(),
                )
                .order_by(challenges.c.created_at.desc())
                .with_for_update()
            )
            .mappings()
            .first()
        )
        if (
            challenge is None
            # Supabase's clock against the database's, one named tolerance (J7).
            or recovery_at < float(challenge.sent_at) - CLOCK_SKEW_SECONDS
            or connection.execute(
                select(tables.verified_sessions.c.session_id).where(
                    tables.verified_sessions.c.session_id == session_id
                )
            ).first()
        ):
            raise HTTPException(400, "This reset link is invalid or expired.")
        user = sync_profile(connection, identity)
        connection.execute(
            insert(tables.verified_sessions).values(
                session_id=session_id, user_id=user.id, recovery_only=True
            )
        )
        connection.execute(
            update(challenges)
            .where(challenges.c.token_hash == challenge.token_hash)
            .values(consumed_at=func.now())
        )
    return {"recovery_ready": True}


def resend(engine, request, payload, flow, wrong_flow, send):
    """Reserve under the lock, then send. A failed send still counts and restarts
    the cooldown, which is conservative. No resend outlives J7's ceiling."""
    with engine.begin() as connection:
        row = pending_challenge(connection, payload)
        if row.flow != flow:
            raise HTTPException(400, wrong_flow)
        check_resend_cooldown(row)
        if not row.extendable:
            raise HTTPException(400, START_AGAIN)
        check_email_limit(connection, payload.email, flow)
        try:
            reserve_email(connection, request, flow)
        except SendRefused:
            raise send_refused() from None
        connection.execute(
            update(challenges)
            .where(challenges.c.token_hash == row.token_hash)
            .values(
                last_sent_at=func.now(),
                send_count=challenges.c.send_count + 1,
                expires_at=func.least(
                    func.now() + timedelta(minutes=OTP_MINUTES),
                    challenges.c.created_at + timedelta(minutes=CHALLENGE_MAX_MINUTES),
                ),
            )
        )
    try:
        send()
    except AuthProviderError as error:
        if provider_refused(request, error):
            raise send_refused() from None
        raise


@router.post(
    "/auth/resend-otp",
    response_model=ChallengeResponse,
    dependencies=[Depends(credential_rate_limit)],
)
def resend_otp(
    payload: ChallengeRequest,
    request: Request,
    engine: EngineDep,
    provider: ProviderDep,
):
    resend(
        engine,
        request,
        payload,
        "login",
        "Request a new email link to continue.",
        lambda: provider.send_code(payload.email, "login"),
    )
    return ChallengeResponse(
        challenge_id=payload.challenge_id,
        flow="login",
        resend_after_seconds=RESEND_COOLDOWN_SECONDS,
    )


@router.post(
    "/auth/resend-confirmation",
    response_model=ChallengeResponse,
    dependencies=[Depends(credential_rate_limit)],
)
def resend_confirmation(
    payload: ChallengeRequest,
    request: Request,
    engine: EngineDep,
    provider: ProviderDep,
):
    resend(
        engine,
        request,
        payload,
        "signup",
        "An email confirmation request is required.",
        lambda: provider.send_code(payload.email, "signup"),
    )
    return ChallengeResponse(
        challenge_id=payload.challenge_id,
        flow="signup",
        resend_after_seconds=RESEND_COOLDOWN_SECONDS,
        verification_method="email_link",
    )


@router.post(
    "/auth/verify-otp",
    response_model=AuthResponse,
    dependencies=[Depends(credential_rate_limit)],
)
def verify_otp(payload: VerifyRequest, engine: EngineDep, provider: ProviderDep):
    # Reserve: one attempt, counted before the provider sees the code (AH-2d).
    with engine.begin() as connection:
        row = pending_challenge(connection, payload)
        if row.flow != "login":
            raise HTTPException(400, "Use the email link to continue.")
        reserved = connection.execute(
            update(challenges)
            .where(
                challenges.c.token_hash == row.token_hash,
                challenges.c.attempts < MAX_ATTEMPTS,
            )
            .values(attempts=challenges.c.attempts + 1)
            .returning(challenges.c.token_hash)
        ).first()
        if reserved is None:
            raise HTTPException(400, START_AGAIN)

    def give_back(connection):
        # Attempts count wrong codes only: any other outcome returns its attempt.
        connection.execute(
            update(challenges)
            .where(challenges.c.token_hash == row.token_hash)
            .values(attempts=func.greatest(challenges.c.attempts - 1, 0))
        )

    def returned():
        with engine.begin() as connection:
            lock_email(connection, payload.email)
            give_back(connection)

    try:
        body = provider.verify_code(payload.email, payload.code, row.flow)
    except AuthProviderError as error:
        if error.status == 401:  # a wrong code: its attempt stays counted
            raise HTTPException(400, "Invalid verification code.") from None
        returned()
        raise
    try:
        if not isinstance(body, dict):
            raise AuthProviderError()
        identity, session_id = provider.identity(body.get("access_token"))
        token_response(body, user_from_identity(identity))  # well-formed first
    except AuthProviderError:
        returned()
        raise
    if normalize_email(identity.get("email", "")) != row.email:
        raise HTTPException(400, "Invalid verification code.")
    # Finish: re-read under the lock; a revoke or a concurrent winner may have
    # consumed the challenge meanwhile (J3), and then this login loses cleanly.
    with engine.begin() as connection:
        lock_email(connection, payload.email)
        current = (
            connection.execute(
                select(
                    challenges.c.consumed_at,
                    (challenges.c.expires_at > func.now()).label("live"),
                )
                .where(challenges.c.token_hash == row.token_hash)
                .with_for_update()
            )
            .mappings()
            .first()
        )
        lost = current is None or current.consumed_at is not None or not current.live
        if not lost:
            user = sync_profile(connection, identity)
            # A native Supabase password/OTP token alone cannot bypass this approval.
            connection.execute(
                insert(tables.verified_sessions).values(
                    session_id=session_id,
                    user_id=user.id,
                    recovery_only=False,
                )
            )
            connection.execute(
                update(challenges)
                .where(challenges.c.token_hash == row.token_hash)
                .values(consumed_at=func.now())
            )
            give_back(connection)
    if lost:
        try:  # best effort: the new Supabase session is never approved here
            provider.logout(body["access_token"], scope="local")
        except AuthProviderError:
            pass
        raise HTTPException(400, START_AGAIN)
    return token_response(body, user, row.flow)


@router.post(
    "/auth/refresh",
    response_model=AuthResponse,
    dependencies=[Depends(refresh_rate_limit)],
)
def refresh(payload: RefreshRequest, engine: EngineDep, provider: ProviderDep):
    body = provider.refresh(payload.refresh_token)
    user = authenticated_session(body.get("access_token"), engine, provider)
    return token_response(
        body,
        UserResponse.model_validate(user),
        "recovery" if user.recovery_only else "login",
    )


def revoke(user, engine, provider):
    email = normalize_email(user.email)
    with engine.begin() as connection:
        # J3: verify-otp's lock, so a concurrent verify either sees its challenge
        # consumed here or has its new session revoked here.
        lock_email(connection, email)
        connection.execute(
            update(tables.verified_sessions)
            .where(tables.verified_sessions.c.user_id == user.id)
            .values(revoked_at=func.now())
        )
        # A pre-change password challenge must not restore a revoked session.
        connection.execute(
            update(challenges)
            .where(
                challenges.c.email == email,
                challenges.c.consumed_at.is_(None),
            )
            .values(consumed_at=func.now())
        )
    try:
        provider.logout(user.access_token)
    except AuthProviderError:
        # Local approvals are already revoked, so even an unexpired JWT is denied.
        pass


@router.post(
    "/auth/logout", status_code=204, dependencies=[Depends(session_rate_limit)]
)
def logout(
    user: Annotated[Principal, Depends(get_verified_session)],
    engine: EngineDep,
    provider: ProviderDep,
):
    revoke(user, engine, provider)
    return Response(status_code=204)


@router.get(
    "/me", response_model=UserResponse, dependencies=[Depends(session_rate_limit)]
)
def me(user: UserDep):
    return user


@router.get(
    "/me/preferences",
    response_model=Preferences,
    dependencies=[Depends(session_rate_limit)],
)
def get_preferences(user: UserDep, engine: EngineDep):
    with user_connection(engine, user.id) as connection:
        row = (
            connection.execute(
                select(tables.preferences).where(
                    tables.preferences.c.user_id == user.id
                )
            )
            .mappings()
            .first()
        )
    return Preferences(**dict(row)) if row else Preferences()


@router.put(
    "/me/preferences",
    response_model=Preferences,
    dependencies=[Depends(session_rate_limit)],
)
def save_preferences(payload: Preferences, user: UserDep, engine: EngineDep):
    with user_connection(engine, user.id) as connection:
        statement = pg_insert(tables.preferences).values(
            user_id=user.id, **payload.model_dump()
        )
        connection.execute(
            statement.on_conflict_do_update(
                index_elements=["user_id"],
                set_={**payload.model_dump(), "updated_at": func.now()},
            )
        )
        connection.execute(
            update(tables.profiles)
            .where(tables.profiles.c.id == user.id)
            .values(onboarding_completed=True, updated_at=func.now())
        )
    return payload


@router.get(
    "/me/notifications",
    response_model=Notifications,
    dependencies=[Depends(session_rate_limit)],
)
def get_notifications(user: UserDep, engine: EngineDep):
    with user_connection(engine, user.id) as connection:
        row = (
            connection.execute(
                select(tables.notifications).where(
                    tables.notifications.c.user_id == user.id
                )
            )
            .mappings()
            .first()
        )
    return Notifications(**dict(row)) if row else Notifications()


@router.put(
    "/me/notifications",
    response_model=Notifications,
    dependencies=[Depends(session_rate_limit)],
)
def save_notifications(payload: Notifications, user: UserDep, engine: EngineDep):
    with user_connection(engine, user.id) as connection:
        statement = pg_insert(tables.notifications).values(
            user_id=user.id, **payload.model_dump()
        )
        connection.execute(
            statement.on_conflict_do_update(
                index_elements=["user_id"],
                set_={**payload.model_dump(), "updated_at": func.now()},
            )
        )
    return payload


@router.put("/me/password", dependencies=[Depends(credential_rate_limit)])
def change_password(
    payload: ChangePassword, user: UserDep, engine: EngineDep, provider: ProviderDep
):
    if payload.new_password == payload.old_password:
        raise HTTPException(400, "New password must differ from your current password.")
    identity = provider.check_password(user.email, payload.old_password)
    if UUID(identity["id"]) != user.id:
        raise HTTPException(401, "Invalid current password.")
    provider.change_password(user.access_token, payload.new_password)
    revoke(user, engine, provider)
    return {"message": "Password changed successfully"}


@router.post("/auth/reset-password", dependencies=[Depends(credential_rate_limit)])
def reset_password(
    payload: NewPassword,
    user: Annotated[Principal, Depends(get_verified_session)],
    engine: EngineDep,
    provider: ProviderDep,
):
    if not user.recovery_only:
        raise HTTPException(403, "Password recovery verification is required.")
    provider.change_password(user.access_token, payload.new_password)
    revoke(user, engine, provider)
    return {"message": "Password reset successfully"}


def security_id(connection, ticker):
    """A ticker or alias resolved as R1 resolves it (BRK-B finds BRK.B), or None."""
    return connection.execute(
        select(models.security_identifiers.c.security_id).where(
            models.security_identifiers.c.scheme == "ticker",
            models.security_identifiers.c.value == ticker,
        )
    ).scalar_one_or_none()


@router.get(
    "/me/watchlist",
    response_model=list[WatchlistItem],
    dependencies=[Depends(session_rate_limit)],
)
def get_watchlist(user: UserDep, engine: EngineDep):
    with user_connection(engine, user.id) as connection:
        rows = (
            connection.execute(
                select(models.securities.c.symbol)
                .select_from(
                    tables.watchlist.join(
                        models.securities,
                        models.securities.c.id == tables.watchlist.c.security_id,
                    )
                )
                .where(tables.watchlist.c.user_id == user.id)
                .order_by(models.securities.c.symbol)
            )
            .scalars()
            .all()
        )
    return [WatchlistItem(ticker=symbol) for symbol in rows]


@router.post(
    "/me/watchlist",
    response_model=WatchlistItem,
    status_code=201,
    dependencies=[Depends(session_rate_limit)],
)
def add_watchlist(payload: WatchlistItem, user: UserDep, engine: EngineDep):
    with user_connection(engine, user.id) as connection:
        found = security_id(connection, payload.ticker)
        if found is None:  # decision 28: only a known security can be watched
            raise APIError(404, "unknown_security", "no security has that ticker")
        connection.execute(
            pg_insert(tables.watchlist)
            .values(user_id=user.id, security_id=found)
            .on_conflict_do_nothing()
        )
    return payload


@router.delete(
    "/me/watchlist/{ticker}",
    status_code=204,
    dependencies=[Depends(session_rate_limit)],
)
def remove_watchlist(ticker: str, user: UserDep, engine: EngineDep):
    wanted = ticker.strip().upper()
    if not _TICKER.fullmatch(wanted):
        raise HTTPException(422, "Ticker is not valid")
    with user_connection(engine, user.id) as connection:
        found = security_id(connection, wanted)
        if found is not None:  # an unknown ticker is already absent: 204
            connection.execute(
                delete(tables.watchlist).where(
                    tables.watchlist.c.user_id == user.id,
                    tables.watchlist.c.security_id == found,
                )
            )
    return Response(status_code=204)
