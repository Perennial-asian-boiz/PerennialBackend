"""
Database configuration.

DATABASE_URL comes from the process environment first, then from
development/backend/.env (read without modifying os.environ). Error messages
are fixed text that never include any part of the URL.
"""

import os
import re
from pathlib import Path
from typing import Mapping, Optional

from dotenv import dotenv_values
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError

# Same credentials file the fetchers use (repository convention: paths.py).
from src.services.consensus_watchlist.paths import ENV_PATH

SUPPORTED_DRIVERNAMES = ("postgresql+psycopg",)


class DatabaseConfigError(RuntimeError):
    """Missing or invalid database configuration. Message is credential-free."""


def parse_database_url(raw: Optional[str], *, name: str = "DATABASE_URL") -> URL:
    if raw is None or not raw.strip():
        raise DatabaseConfigError(
            f"{name} is not set. Copy development/backend/.env.example to "
            f"development/backend/.env and set {name}, or export it."
        )
    raw = raw.strip()
    # Accept the plain libpq scheme and pin it to psycopg 3.
    raw = re.sub(r"^postgres(ql)?://", "postgresql+psycopg://", raw)
    try:
        url = make_url(raw)
    except (ArgumentError, ValueError, TypeError):
        raise DatabaseConfigError(f"{name} is not a valid database URL.") from None
    if url.drivername not in SUPPORTED_DRIVERNAMES:
        raise DatabaseConfigError(f"{name} must use the postgresql+psycopg:// scheme.")
    if not url.host or not url.database:
        raise DatabaseConfigError(f"{name} must include a host and database name.")
    return url


def get_database_url(
    environ: Optional[Mapping[str, str]] = None, env_path: Optional[Path] = None
) -> URL:
    environ = os.environ if environ is None else environ
    env_path = ENV_PATH if env_path is None else env_path
    raw = environ.get("DATABASE_URL")
    if not raw and env_path.is_file():
        raw = dotenv_values(env_path).get("DATABASE_URL")
    return parse_database_url(raw)
