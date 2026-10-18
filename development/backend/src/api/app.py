"""FastAPI is the only application-data gateway; Supabase owns authentication."""

import os
from typing import Annotated

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.engine import Engine

from src.api.accounts import get_engine, router
from src.api.dashboard import DashboardResponse, dashboard_response_from_publication
from src.api.dashboard_fixture import temporary_dashboard_response
from src.api.supabase_auth import AuthProviderError
from src.db import models
from src.services.consensus_watchlist.paths import ENV_PATH

load_dotenv(ENV_PATH)


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


app = FastAPI(title="Perennial API", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins(),
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)
app.include_router(router)


@app.exception_handler(AuthProviderError)
async def provider_error(_request: Request, error: AuthProviderError):
    return JSONResponse(status_code=error.status, content={"detail": error.message})


@app.exception_handler(RequestValidationError)
async def invalid_input(_request: Request, _error: RequestValidationError):
    # Pydantic's default details include rejected input (including passwords).
    return JSONResponse(
        status_code=422, content={"detail": "Please check the fields and try again."}
    )


@app.middleware("http")
async def private_responses(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith(("/auth/", "/me")):
        response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/watchlist", response_model=DashboardResponse)
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
    return dashboard_response_from_publication(publication)
