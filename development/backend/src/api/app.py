"""FastAPI is the only application-data gateway; Supabase owns authentication."""

import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Annotated

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.engine import Engine

from src.api import read
from src.api.accounts import get_engine, router
from src.api.dashboard import DashboardResponse, dashboard_response_from_publication
from src.api.dashboard_fixture import temporary_dashboard_response
from src.api.read import StartupRefused, check_startup
from src.api.supabase_auth import AuthProviderError
from src.db import models
from src.services.consensus_watchlist.paths import ENV_PATH
from src.utils.logger import configure_logging

load_dotenv(ENV_PATH)
logger = logging.getLogger(__name__)


def cors_origins() -> list[str]:
    configured = os.getenv("CORS_ORIGINS", "")
    if configured.strip():
        origins = [
            item.strip().rstrip("/") for item in configured.split(",") if item.strip()
        ]
    else:
        if os.getenv("APP_ENV", "development").casefold() == "production":
            raise RuntimeError("CORS_ORIGINS must be configured in production")
        origins = ["http://localhost:5173", "http://127.0.0.1:5173"]
    if "*" in origins:
        raise RuntimeError("CORS_ORIGINS cannot use '*'")
    return origins


def create_app(*, api_role: str = "perennial_api") -> FastAPI:
    """The integrated app: #22's account routes and R1's reads under /v1.

    One ``api``-workload engine from API_DATABASE_URL serves both routers
    (required: there is no DATABASE_URL fallback). Startup logs JSON and runs
    R1's login check, extended to schema app; rate limiting and the only proxy
    middleware come from ``read.install`` with the API_* settings.
    """
    engine = get_engine()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging()
        try:
            check_startup(app.state.engine, app.state.api_role)
        except StartupRefused as refused:
            logger.error("%s", refused)  # fixed text: no login, URL or driver text
            raise
        yield

    app = FastAPI(title="Perennial API", version="0.2.0", lifespan=lifespan)
    app.state.engine = engine
    app.state.api_role = api_role
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins(),
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["Authorization", "Content-Type"],
    )
    app.include_router(router, prefix="/v1")  # decision 17
    app.include_router(read.router)  # already carries /v1
    read.install(app, read.RateLimitConfig.from_environ(os.environ))
    app.add_exception_handler(AuthProviderError, provider_error)
    app.add_exception_handler(RequestValidationError, invalid_input)
    app.add_exception_handler(read.APIError, api_error)
    app.middleware("http")(private_responses)
    app.get("/health")(health)
    app.get("/api/watchlist", response_model=DashboardResponse)(dashboard_watchlist)
    return app


def __getattr__(name: str) -> FastAPI:
    # `uvicorn src.api.app:app` (and python -m src.api) builds the app on first
    # use, so importing this module needs no API_DATABASE_URL.
    if name == "app":
        return create_app()
    raise AttributeError(name)


async def provider_error(_request: Request, error: Exception) -> Response:
    assert isinstance(error, AuthProviderError)
    return JSONResponse(
        status_code=error.status,
        content={
            "detail": error.message,
            "code": error.code,
            "field_errors": error.field_errors,
        },
    )


async def api_error(_request: Request, error: Exception) -> Response:
    # R1's fixed {"detail", "code"} errors: its reads and unknown_security.
    assert isinstance(error, read.APIError)
    return read.CanonicalJSONResponse(
        {"detail": error.detail, "code": error.code}, status_code=error.status
    )


async def invalid_input(_request: Request, error: Exception) -> Response:
    assert isinstance(error, RequestValidationError)
    # Pydantic's default details include rejected input (including passwords).
    # Inspect only location/type and return our own messages, never input/msg/ctx.
    fields = {}
    for issue in error.errors():
        location = issue.get("loc", ())
        if len(location) != 2 or location[0] != "body":
            continue
        name, kind = location[1], issue["type"]
        if name == "email":
            fields[name] = "Enter a valid email address in the format name@domain.com."
        elif name == "full_name":
            fields[name] = (
                "Use no more than 200 characters for your name."
                if kind == "string_too_long"
                else "Enter your full name."
            )
        elif name in {"password", "new_password", "old_password"}:
            fields[name] = {
                "string_too_short": (
                    "Enter your current password."
                    if name == "old_password"
                    else "Use at least 8 characters for your password."
                ),
                "string_too_long": "Use no more than 200 characters for your password.",
                "value_error": (
                    "Include an uppercase letter, a lowercase letter, and a number."
                ),
            }.get(kind, "Enter your password.")
        elif name == "code":
            fields[name] = "Enter the 6-digit verification code from your email."
    detail = next(iter(fields.values()), "Please check the fields and try again.")
    return JSONResponse(
        status_code=422,
        content={"detail": detail, "code": "invalid_fields", "field_errors": fields},
    )


async def private_responses(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    response = await call_next(request)
    if request.url.path.startswith(("/v1/auth/", "/v1/me")):
        response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


def health() -> dict[str, str]:
    return {"status": "ok"}


def dashboard_watchlist(
    engine: Annotated[Engine, Depends(get_engine)],
) -> DashboardResponse:
    """Keep the team's publication contract and clearly labelled temporary data."""
    default_mode = (
        "publication"
        if os.getenv("APP_ENV", "development").casefold() == "production"
        else "fixture"
    )
    if os.getenv("DASHBOARD_DATA_MODE", default_mode).casefold() == "fixture":
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
    return dashboard_response_from_publication(
        dict(publication) if publication is not None else None
    )
