"""FastAPI application for the Sprint 1 account and personalization flow.

The API intentionally uses opaque database-backed sessions instead of JWTs for
the first integration. This keeps token revocation simple and gives the
frontend one stable contract while the backend team connects a mail provider.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import and_, delete, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from src.api.dashboard import DashboardResponse, dashboard_response_from_publication
from src.api.dashboard_fixture import temporary_dashboard_response
from src.api.security import (
    hash_secret,
    hash_token,
    new_otp,
    new_session_token,
    verify_secret,
)
from src.db import models
from src.db.session import make_engine


RiskLevel = Literal["Conservative", "Moderate", "High"]
OTP_TTL_MINUTES = 15
SESSION_TTL_DAYS = 7
MAX_OTP_ATTEMPTS = 5


class APIModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class UserResponse(APIModel):
    id: int
    email: str
    full_name: str
    is_verified: bool


class SignupRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    full_name: str = Field(min_length=1, max_length=200)
    password: str = Field(min_length=8, max_length=200)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, value: str) -> str:
        return value.strip().casefold()

    @field_validator("full_name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        return " ".join(value.strip().split())


class SignupResponse(APIModel):
    user: UserResponse
    verification_required: bool
    development_code: str | None = None


class VerifyOTPRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    code: str = Field(min_length=6, max_length=6)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, value: str) -> str:
        return value.strip().casefold()


class ResendOTPRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, value: str) -> str:
        return value.strip().casefold()


class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=1, max_length=200)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, value: str) -> str:
        return value.strip().casefold()


class AuthResponse(APIModel):
    access_token: str
    token_type: Literal["bearer"]
    user: UserResponse


class PreferencesRequest(BaseModel):
    sectors: list[str] = Field(default_factory=list, max_length=11)
    risk_level: RiskLevel | None = None

    @field_validator("sectors")
    @classmethod
    def normalize_sectors(cls, values: list[str]) -> list[str]:
        cleaned = [" ".join(value.strip().split()) for value in values]
        return list(dict.fromkeys(value for value in cleaned if value))


class PreferencesResponse(APIModel):
    sectors: list[str]
    risk_level: RiskLevel | None


class WatchlistItem(APIModel):
    ticker: str


class WatchlistRequest(BaseModel):
    ticker: str = Field(min_length=1, max_length=20)

    @field_validator("ticker")
    @classmethod
    def normalize_ticker(cls, value: str) -> str:
        return value.strip().upper()


class OTPResponse(APIModel):
    verification_required: bool
    development_code: str | None = None


@lru_cache
def get_engine() -> Engine:
    return make_engine()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def development_code_response(code: str) -> str | None:
    """Expose OTP only for local testing, never when APP_ENV is production."""
    if os.getenv("APP_ENV", "development").casefold() != "production":
        return code
    return None


def user_response(row: object) -> UserResponse:
    return UserResponse(
        id=row.id, email=row.email, full_name=row.full_name, is_verified=row.is_verified
    )


def find_user(connection: object, normalized_email: str) -> object | None:
    return (
        connection.execute(
            select(models.users).where(
                models.users.c.normalized_email == normalized_email
            )
        )
        .mappings()
        .first()
    )


def issue_verification_code(connection: object, user_id: int) -> str:
    code = new_otp()
    now = utc_now()
    connection.execute(
        update(models.email_verification_codes)
        .where(
            and_(
                models.email_verification_codes.c.user_id == user_id,
                models.email_verification_codes.c.used_at.is_(None),
            )
        )
        .values(used_at=now)
    )
    connection.execute(
        insert(models.email_verification_codes).values(
            user_id=user_id,
            code_hash=hash_secret(code),
            expires_at=now + timedelta(minutes=OTP_TTL_MINUTES),
        )
    )
    return code


def get_current_user(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(HTTPBearer(auto_error=False))
    ],
    engine: Annotated[Engine, Depends(get_engine)],
) -> object:
    if credentials is None or credentials.scheme.casefold() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required"
        )
    with engine.begin() as connection:
        row = (
            connection.execute(
                select(models.users)
                .join(
                    models.auth_sessions,
                    models.auth_sessions.c.user_id == models.users.c.id,
                )
                .where(
                    and_(
                        models.auth_sessions.c.token_hash
                        == hash_token(credentials.credentials),
                        models.auth_sessions.c.revoked_at.is_(None),
                        models.auth_sessions.c.expires_at > utc_now(),
                    )
                )
            )
            .mappings()
            .first()
        )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session",
        )
    return row


app = FastAPI(title="Perennial API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/watchlist", response_model=DashboardResponse)
def dashboard_watchlist(
    engine: Annotated[Engine, Depends(get_engine)],
) -> DashboardResponse:
    """Expose the current published ranking in the frontend display contract."""

    default_mode = (
        "publication"
        if os.getenv("APP_ENV", "development").casefold() == "production"
        else "fixture"
    )
    data_mode = os.getenv("DASHBOARD_DATA_MODE", default_mode).casefold()
    if data_mode == "fixture":
        return temporary_dashboard_response()

    with engine.begin() as connection:
        publication = (
            connection.execute(
                select(models.publications)
                .join(
                    models.current_publication,
                    models.current_publication.c.publication_id
                    == models.publications.c.id,
                )
                .where(models.current_publication.c.channel == "production")
            )
            .mappings()
            .first()
        )
    return dashboard_response_from_publication(publication)


@app.post(
    "/auth/signup", response_model=SignupResponse, status_code=status.HTTP_201_CREATED
)
def signup(
    payload: SignupRequest, engine: Annotated[Engine, Depends(get_engine)]
) -> SignupResponse:
    with engine.begin() as connection:
        if find_user(connection, payload.email) is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Email is already registered",
            )
        result = connection.execute(
            insert(models.users)
            .values(
                email=payload.email,
                normalized_email=payload.email,
                full_name=payload.full_name,
                password_hash=hash_secret(payload.password),
            )
            .returning(*models.users.c)
        )
        user = result.mappings().one()
        code = issue_verification_code(connection, user.id)
    return SignupResponse(
        user=user_response(user),
        verification_required=True,
        development_code=development_code_response(code),
    )


@app.post("/auth/verify-otp", response_model=AuthResponse)
def verify_otp(
    payload: VerifyOTPRequest, engine: Annotated[Engine, Depends(get_engine)]
) -> AuthResponse:
    with engine.begin() as connection:
        user = find_user(connection, payload.email)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid verification code",
            )
        code_row = (
            connection.execute(
                select(models.email_verification_codes)
                .where(
                    and_(
                        models.email_verification_codes.c.user_id == user.id,
                        models.email_verification_codes.c.used_at.is_(None),
                    )
                )
                .order_by(models.email_verification_codes.c.created_at.desc())
            )
            .mappings()
            .first()
        )
        if (
            code_row is None
            or code_row.expires_at <= utc_now()
            or code_row.attempts >= MAX_OTP_ATTEMPTS
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid verification code",
            )
        if not verify_secret(payload.code, code_row.code_hash):
            connection.execute(
                update(models.email_verification_codes)
                .where(models.email_verification_codes.c.id == code_row.id)
                .values(attempts=code_row.attempts + 1)
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid verification code",
            )
        connection.execute(
            update(models.email_verification_codes)
            .where(models.email_verification_codes.c.id == code_row.id)
            .values(used_at=utc_now())
        )
        connection.execute(
            update(models.users)
            .where(models.users.c.id == user.id)
            .values(is_verified=True, updated_at=utc_now())
        )
        token = new_session_token()
        connection.execute(
            insert(models.auth_sessions).values(
                user_id=user.id,
                token_hash=hash_token(token),
                expires_at=utc_now() + timedelta(days=SESSION_TTL_DAYS),
            )
        )
        verified_user = (
            connection.execute(select(models.users).where(models.users.c.id == user.id))
            .mappings()
            .one()
        )
    return AuthResponse(
        access_token=token, token_type="bearer", user=user_response(verified_user)
    )


@app.post("/auth/resend-otp", response_model=OTPResponse)
def resend_otp(
    payload: ResendOTPRequest, engine: Annotated[Engine, Depends(get_engine)]
) -> OTPResponse:
    with engine.begin() as connection:
        user = find_user(connection, payload.email)
        if user is None or user.is_verified:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Unable to resend verification code",
            )
        code = issue_verification_code(connection, user.id)
    return OTPResponse(
        verification_required=True, development_code=development_code_response(code)
    )


@app.post("/auth/login", response_model=AuthResponse)
def login(
    payload: LoginRequest, engine: Annotated[Engine, Depends(get_engine)]
) -> AuthResponse:
    with engine.begin() as connection:
        user = find_user(connection, payload.email)
        if user is None or not verify_secret(payload.password, user.password_hash):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Email or password is invalid",
            )
        if not user.is_verified:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Email verification is required",
            )
        token = new_session_token()
        connection.execute(
            insert(models.auth_sessions).values(
                user_id=user.id,
                token_hash=hash_token(token),
                expires_at=utc_now() + timedelta(days=SESSION_TTL_DAYS),
            )
        )
    return AuthResponse(
        access_token=token, token_type="bearer", user=user_response(user)
    )


@app.get("/me", response_model=UserResponse)
def me(user: Annotated[object, Depends(get_current_user)]) -> UserResponse:
    return user_response(user)


@app.get("/me/preferences", response_model=PreferencesResponse)
def get_preferences(
    user: Annotated[object, Depends(get_current_user)],
    engine: Annotated[Engine, Depends(get_engine)],
) -> PreferencesResponse:
    with engine.begin() as connection:
        row = (
            connection.execute(
                select(models.user_preferences).where(
                    models.user_preferences.c.user_id == user.id
                )
            )
            .mappings()
            .first()
        )
    if row is None:
        return PreferencesResponse(sectors=[], risk_level=None)
    return PreferencesResponse(sectors=row.sectors or [], risk_level=row.risk_level)


@app.put("/me/preferences", response_model=PreferencesResponse)
def save_preferences(
    payload: PreferencesRequest,
    user: Annotated[object, Depends(get_current_user)],
    engine: Annotated[Engine, Depends(get_engine)],
) -> PreferencesResponse:
    with engine.begin() as connection:
        existing = connection.execute(
            select(models.user_preferences.c.user_id).where(
                models.user_preferences.c.user_id == user.id
            )
        ).first()
        values = {
            "sectors": payload.sectors,
            "risk_level": payload.risk_level,
            "updated_at": utc_now(),
        }
        if existing is None:
            connection.execute(
                insert(models.user_preferences).values(user_id=user.id, **values)
            )
        else:
            connection.execute(
                update(models.user_preferences)
                .where(models.user_preferences.c.user_id == user.id)
                .values(**values)
            )
    return PreferencesResponse(sectors=payload.sectors, risk_level=payload.risk_level)


@app.get("/me/watchlist", response_model=list[WatchlistItem])
def get_watchlist(
    user: Annotated[object, Depends(get_current_user)],
    engine: Annotated[Engine, Depends(get_engine)],
) -> list[WatchlistItem]:
    with engine.begin() as connection:
        rows = connection.execute(
            select(models.securities.c.symbol)
            .join(
                models.user_watchlist,
                models.user_watchlist.c.security_id == models.securities.c.id,
            )
            .where(models.user_watchlist.c.user_id == user.id)
            .order_by(models.securities.c.symbol)
        ).all()
    return [WatchlistItem(ticker=row.symbol) for row in rows]


@app.post(
    "/me/watchlist", response_model=WatchlistItem, status_code=status.HTTP_201_CREATED
)
def add_watchlist(
    payload: WatchlistRequest,
    user: Annotated[object, Depends(get_current_user)],
    engine: Annotated[Engine, Depends(get_engine)],
) -> WatchlistItem:
    with engine.begin() as connection:
        security = (
            connection.execute(
                select(models.securities).where(
                    models.securities.c.symbol == payload.ticker
                )
            )
            .mappings()
            .first()
        )
        if security is None:
            security = (
                connection.execute(
                    insert(models.securities)
                    .values(symbol=payload.ticker)
                    .returning(*models.securities.c)
                )
                .mappings()
                .one()
            )
        connection.execute(
            pg_insert(models.user_watchlist)
            .values(user_id=user.id, security_id=security.id)
            .on_conflict_do_nothing(
                index_elements=[
                    models.user_watchlist.c.user_id,
                    models.user_watchlist.c.security_id,
                ]
            )
        )
    return WatchlistItem(ticker=payload.ticker)


@app.delete("/me/watchlist/{ticker}", status_code=status.HTTP_204_NO_CONTENT)
def remove_watchlist(
    ticker: str,
    user: Annotated[object, Depends(get_current_user)],
    engine: Annotated[Engine, Depends(get_engine)],
) -> Response:
    normalized_ticker = ticker.strip().upper()
    with engine.begin() as connection:
        security = connection.execute(
            select(models.securities.c.id).where(
                models.securities.c.symbol == normalized_ticker
            )
        ).first()
        if security is not None:
            connection.execute(
                delete(models.user_watchlist).where(
                    and_(
                        models.user_watchlist.c.user_id == user.id,
                        models.user_watchlist.c.security_id == security.id,
                    )
                )
            )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
