"""Supabase Auth endpoints and account data served exclusively through FastAPI."""

import hashlib
import os
import re
import secrets
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from src.api import account_models as tables
from src.api.supabase_auth import AuthProviderError, SupabaseAuth
from src.db.session import make_engine

router = APIRouter()
OTP_MINUTES = 15
RESEND_COOLDOWN_SECONDS = 60
MAX_ATTEMPTS = 5
MAX_EMAILS_PER_HOUR = 60
PASSWORD_REQUIREMENTS_MESSAGE = (
    "Use 8–200 characters, including an uppercase letter, a lowercase letter, "
    "and a number."
)


def now():
    return datetime.now(timezone.utc)


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


@lru_cache
def get_engine() -> Engine:
    # Separate restricted runtime credentials from DATABASE_URL (migrations).
    raw = os.getenv("API_DATABASE_URL", "").strip()
    if not raw and os.getenv("APP_ENV", "development").casefold() == "production":
        raise RuntimeError(
            "API_DATABASE_URL must use a dedicated API login in production"
        )
    return make_engine(raw or None)


def get_provider() -> SupabaseAuth:
    return SupabaseAuth.from_environment()


EngineDep = Annotated[Engine, Depends(get_engine)]
ProviderDep = Annotated[SupabaseAuth, Depends(get_provider)]


class EmailRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, value):
        value = value.strip().casefold()
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
        value = value.strip().upper()
        if not re.fullmatch(r"[A-Z0-9][A-Z0-9./-]*", value):
            raise ValueError("Ticker is not valid")
        return value


@contextmanager
def user_connection(engine, user_id):
    with engine.begin() as connection:
        connection.execute(
            text("SELECT set_config('request.jwt.claim.sub', :subject, true)"),
            {"subject": str(user_id)},
        )
        yield connection


def lock_email(connection, email):
    connection.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"perennial-auth:{email}"},
    )


def check_email_limit(connection, email):
    rows = (
        connection.execute(
            select(tables.auth_challenges).where(
                tables.auth_challenges.c.email == email,
                tables.auth_challenges.c.last_sent_at > now() - timedelta(hours=1),
            )
        )
        .mappings()
        .all()
    )
    if sum(row.send_count for row in rows) >= MAX_EMAILS_PER_HOUR:
        raise HTTPException(
            429, "Too many verification emails. Please try again later."
        )


def check_resend_cooldown(row):
    if row.last_sent_at is None:
        return
    elapsed = int((now() - row.last_sent_at).total_seconds())
    remaining = RESEND_COOLDOWN_SECONDS - elapsed
    if remaining > 0:
        raise HTTPException(
            429,
            f"Please wait {remaining} seconds before requesting another code.",
            headers={"Retry-After": str(remaining)},
        )


def email_is_registered(connection, email):
    return connection.execute(
        text("SELECT app.email_is_registered(:email)"), {"email": email}
    ).scalar_one()


def start_challenge(connection, email, flow):
    token = secrets.token_urlsafe(32)
    connection.execute(
        update(tables.auth_challenges)
        .where(
            tables.auth_challenges.c.email == email,
            tables.auth_challenges.c.consumed_at.is_(None),
        )
        .values(consumed_at=now())
    )
    connection.execute(
        insert(tables.auth_challenges).values(
            token_hash=digest(token),
            email=email,
            flow=flow,
            last_sent_at=now(),
            expires_at=now() + timedelta(minutes=OTP_MINUTES),
        )
    )
    return ChallengeResponse(
        challenge_id=token,
        flow=flow,
        resend_after_seconds=RESEND_COOLDOWN_SECONDS,
        verification_method="email_otp" if flow == "login" else "email_link",
    )


def pending_challenge(connection, payload):
    lock_email(connection, payload.email)
    row = (
        connection.execute(
            select(tables.auth_challenges)
            .where(
                tables.auth_challenges.c.token_hash == digest(payload.challenge_id),
                tables.auth_challenges.c.email == payload.email,
            )
            .with_for_update()
        )
        .mappings()
        .first()
    )
    if (
        row is None
        or row.consumed_at
        or row.expires_at <= now()
        or row.attempts >= MAX_ATTEMPTS
    ):
        raise HTTPException(400, "Verification expired or invalid. Please start again.")
    return row


def user_from_identity(identity):
    metadata = identity.get("user_metadata") or {}
    return UserResponse(
        id=identity["id"],
        email=identity["email"],
        full_name=str(metadata.get("full_name") or "")[:200],
    )


def sync_profile(connection, identity):
    user = user_from_identity(identity)
    connection.execute(
        text("SELECT set_config('request.jwt.claim.sub', :subject, true)"),
        {"subject": str(user.id)},
    )
    statement = pg_insert(tables.profiles).values(
        id=user.id, email=user.email, full_name=user.full_name
    )
    connection.execute(
        statement.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "email": user.email,
                "full_name": user.full_name,
                "updated_at": now(),
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
    with user_connection(engine, user_id) as connection:
        session = (
            connection.execute(
                select(tables.verified_sessions).where(
                    tables.verified_sessions.c.session_id == session_id,
                    tables.verified_sessions.c.user_id == user_id,
                    tables.verified_sessions.c.revoked_at.is_(None),
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


@router.post("/auth/signup", response_model=ChallengeResponse, status_code=201)
def signup(payload: SignupRequest, engine: EngineDep, provider: ProviderDep):
    with engine.begin() as connection:
        lock_email(connection, payload.email)
        if email_is_registered(connection, payload.email):
            raise HTTPException(
                409, "This email is already registered. Please sign in instead."
            )
        check_email_limit(connection, payload.email)
        provider.signup(payload.email, payload.password, payload.full_name)
        if not email_is_registered(connection, payload.email):
            # Detect wrong-project configuration or a malformed provider success.
            raise AuthProviderError()
        return start_challenge(connection, payload.email, "signup")


@router.post("/auth/confirm-email")
def confirm_email(
    payload: ConfirmEmailRequest, engine: EngineDep, provider: ProviderDep
):
    # The emailed link has already been consumed by Supabase's /verify endpoint.
    # Validate its JWT, sync only the profile, and never approve an app login.
    identity, _session_id = provider.identity(payload.access_token)
    with engine.begin() as connection:
        sync_profile(connection, identity)
        connection.execute(
            update(tables.auth_challenges)
            .where(
                tables.auth_challenges.c.email == identity["email"].casefold(),
                tables.auth_challenges.c.flow == "signup",
                tables.auth_challenges.c.consumed_at.is_(None),
            )
            .values(consumed_at=now())
        )
    try:
        provider.logout(payload.access_token, scope="local")
    except AuthProviderError:
        pass  # No app session was approved; a valid password + login OTP is still required.
    return {"email_confirmed": True}


@router.post("/auth/login", response_model=ChallengeResponse)
def login(payload: LoginRequest, engine: EngineDep, provider: ProviderDep):
    with engine.begin() as connection:
        lock_email(connection, payload.email)
        flow = "login"
        try:
            provider.check_password(payload.email, payload.password)
        except AuthProviderError as error:
            if error.code == "email_not_confirmed":
                raise HTTPException(
                    403,
                    "Your account hasn’t been confirmed yet. Open the confirmation link sent to your email, then sign in.",
                ) from None
            raise
        check_email_limit(connection, payload.email)
        provider.send_code(payload.email, flow)
        return start_challenge(connection, payload.email, flow)


@router.post("/auth/forgot-password", response_model=PasswordResetRequestResponse)
def forgot_password(payload: EmailRequest, engine: EngineDep, provider: ProviderDep):
    with engine.begin() as connection:
        lock_email(connection, payload.email)
        if email_is_registered(connection, payload.email):
            try:
                check_email_limit(connection, payload.email)
                provider.send_code(payload.email, "recovery")
            except HTTPException as error:
                if error.status_code != 429:
                    raise
                # Keep rate-limit outcomes indistinguishable from unknown emails.
                return PasswordResetRequestResponse()
            except AuthProviderError as error:
                if error.status != 429:
                    raise
                # Provider email limits must not reveal whether an account exists.
                return PasswordResetRequestResponse()
            start_challenge(connection, payload.email, "recovery")
    return PasswordResetRequestResponse()


@router.post("/auth/accept-recovery-link")
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
    email = identity["email"].casefold()
    with engine.begin() as connection:
        lock_email(connection, email)
        challenge = (
            connection.execute(
                select(tables.auth_challenges)
                .where(
                    tables.auth_challenges.c.email == email,
                    tables.auth_challenges.c.flow == "recovery",
                    tables.auth_challenges.c.consumed_at.is_(None),
                    tables.auth_challenges.c.expires_at > now(),
                )
                .order_by(tables.auth_challenges.c.created_at.desc())
                .with_for_update()
            )
            .mappings()
            .first()
        )
        if (
            challenge is None
            or recovery_at < challenge.last_sent_at.timestamp() - 60
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
            update(tables.auth_challenges)
            .where(tables.auth_challenges.c.token_hash == challenge.token_hash)
            .values(consumed_at=now())
        )
    return {"recovery_ready": True}


@router.post("/auth/resend-otp", response_model=ChallengeResponse)
def resend_otp(payload: ChallengeRequest, engine: EngineDep, provider: ProviderDep):
    with engine.begin() as connection:
        row = pending_challenge(connection, payload)
        if row.flow != "login":
            raise HTTPException(400, "Request a new email link to continue.")
        check_resend_cooldown(row)
        check_email_limit(connection, payload.email)
        provider.send_code(payload.email, row.flow)
        connection.execute(
            update(tables.auth_challenges)
            .where(tables.auth_challenges.c.token_hash == row.token_hash)
            .values(
                last_sent_at=now(),
                send_count=row.send_count + 1,
                expires_at=now() + timedelta(minutes=OTP_MINUTES),
            )
        )
    return ChallengeResponse(
        challenge_id=payload.challenge_id,
        flow=row.flow,
        resend_after_seconds=RESEND_COOLDOWN_SECONDS,
    )


@router.post("/auth/resend-confirmation", response_model=ChallengeResponse)
def resend_confirmation(
    payload: ChallengeRequest, engine: EngineDep, provider: ProviderDep
):
    with engine.begin() as connection:
        row = pending_challenge(connection, payload)
        if row.flow != "signup":
            raise HTTPException(400, "An email confirmation request is required.")
        check_resend_cooldown(row)
        check_email_limit(connection, payload.email)
        provider.send_code(payload.email, "signup")
        connection.execute(
            update(tables.auth_challenges)
            .where(tables.auth_challenges.c.token_hash == row.token_hash)
            .values(
                last_sent_at=now(),
                send_count=row.send_count + 1,
                expires_at=now() + timedelta(minutes=OTP_MINUTES),
            )
        )
    return ChallengeResponse(
        challenge_id=payload.challenge_id,
        flow="signup",
        resend_after_seconds=RESEND_COOLDOWN_SECONDS,
        verification_method="email_link",
    )


@router.post("/auth/verify-otp", response_model=AuthResponse)
def verify_otp(payload: VerifyRequest, engine: EngineDep, provider: ProviderDep):
    with engine.begin() as connection:
        row = pending_challenge(connection, payload)
        if row.flow != "login":
            raise HTTPException(400, "Use the email link to continue.")
        try:
            body = provider.verify_code(payload.email, payload.code, row.flow)
        except AuthProviderError as error:
            if error.status == 401:
                connection.execute(
                    update(tables.auth_challenges)
                    .where(tables.auth_challenges.c.token_hash == row.token_hash)
                    .values(attempts=row.attempts + 1)
                )
                connection.commit()
                raise HTTPException(400, "Invalid verification code.") from None
            raise
        identity, session_id = provider.identity(body.get("access_token"))
        if identity.get("email", "").casefold() != row.email:
            raise HTTPException(400, "Invalid verification code.")
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
            update(tables.auth_challenges)
            .where(tables.auth_challenges.c.token_hash == row.token_hash)
            .values(consumed_at=now())
        )
        result = token_response(body, user, row.flow)
    return result


@router.post("/auth/refresh", response_model=AuthResponse)
def refresh(payload: RefreshRequest, engine: EngineDep, provider: ProviderDep):
    body = provider.refresh(payload.refresh_token)
    user = authenticated_session(body.get("access_token"), engine, provider)
    return token_response(
        body,
        UserResponse.model_validate(user),
        "recovery" if user.recovery_only else "login",
    )


def revoke(user, engine, provider):
    with engine.begin() as connection:
        connection.execute(
            update(tables.verified_sessions)
            .where(tables.verified_sessions.c.user_id == user.id)
            .values(revoked_at=now())
        )
        # A pre-change password challenge must not restore a revoked session.
        connection.execute(
            update(tables.auth_challenges)
            .where(
                tables.auth_challenges.c.email == user.email,
                tables.auth_challenges.c.consumed_at.is_(None),
            )
            .values(consumed_at=now())
        )
    try:
        provider.logout(user.access_token)
    except AuthProviderError:
        # Local approvals are already revoked, so even an unexpired JWT is denied.
        pass


@router.post("/auth/logout", status_code=204)
def logout(
    user: Annotated[Principal, Depends(get_verified_session)],
    engine: EngineDep,
    provider: ProviderDep,
):
    revoke(user, engine, provider)
    return Response(status_code=204)


@router.get("/me", response_model=UserResponse)
def me(user: UserDep):
    return user


@router.get("/me/preferences", response_model=Preferences)
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


@router.put("/me/preferences", response_model=Preferences)
def save_preferences(payload: Preferences, user: UserDep, engine: EngineDep):
    with user_connection(engine, user.id) as connection:
        statement = pg_insert(tables.preferences).values(
            user_id=user.id, **payload.model_dump()
        )
        connection.execute(
            statement.on_conflict_do_update(
                index_elements=["user_id"],
                set_={**payload.model_dump(), "updated_at": now()},
            )
        )
        connection.execute(
            update(tables.profiles)
            .where(tables.profiles.c.id == user.id)
            .values(onboarding_completed=True, updated_at=now())
        )
    return payload


@router.get("/me/notifications", response_model=Notifications)
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


@router.put("/me/notifications", response_model=Notifications)
def save_notifications(payload: Notifications, user: UserDep, engine: EngineDep):
    with user_connection(engine, user.id) as connection:
        statement = pg_insert(tables.notifications).values(
            user_id=user.id, **payload.model_dump()
        )
        connection.execute(
            statement.on_conflict_do_update(
                index_elements=["user_id"],
                set_={**payload.model_dump(), "updated_at": now()},
            )
        )
    return payload


@router.put("/me/password")
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


@router.post("/auth/reset-password")
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


@router.get("/me/watchlist", response_model=list[WatchlistItem])
def get_watchlist(user: UserDep, engine: EngineDep):
    with user_connection(engine, user.id) as connection:
        rows = (
            connection.execute(
                select(tables.watchlist.c.ticker)
                .where(tables.watchlist.c.user_id == user.id)
                .order_by(tables.watchlist.c.ticker)
            )
            .scalars()
            .all()
        )
    return [WatchlistItem(ticker=ticker) for ticker in rows]


@router.post("/me/watchlist", response_model=WatchlistItem, status_code=201)
def add_watchlist(payload: WatchlistItem, user: UserDep, engine: EngineDep):
    with user_connection(engine, user.id) as connection:
        connection.execute(
            pg_insert(tables.watchlist)
            .values(user_id=user.id, ticker=payload.ticker)
            .on_conflict_do_nothing()
        )
    return payload


@router.delete("/me/watchlist/{ticker}", status_code=204)
def remove_watchlist(ticker: str, user: UserDep, engine: EngineDep):
    item = WatchlistItem(ticker=ticker)
    with user_connection(engine, user.id) as connection:
        connection.execute(
            delete(tables.watchlist).where(
                tables.watchlist.c.user_id == user.id,
                tables.watchlist.c.ticker == item.ticker,
            )
        )
    return Response(status_code=204)
