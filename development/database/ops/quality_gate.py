"""Migration round-trip and drift check on a newly created synthetic database.

TEST_DATABASE_URL identifies a loopback test server, never the database to
migrate. The gate creates and drops only its own random database. No .env file
is read. Invoke from any directory with the repository Python interpreter.
"""

import logging
import os
import secrets
import sys
from pathlib import Path

from alembic import command
from alembic.config import Config
from restore_drill import test_connection
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

REPO = Path(__file__).resolve().parents[3]
LOGGER = logging.getLogger(__name__)


def check_migrations(raw: str) -> None:
    params = test_connection(raw)
    for key in list(os.environ):
        if key.startswith("PG"):
            del os.environ[key]
    url = URL.create(
        "postgresql+psycopg",
        username=params["user"],
        password=params["password"],
        host=params["host"],
        port=params["port"],
        database=params["dbname"],
        query={"sslmode": params["sslmode"]},
    )
    connect_args = {"hostaddr": params["hostaddr"], "connect_timeout": 10}
    admin = create_engine(
        url,
        isolation_level="AUTOCOMMIT",
        connect_args=connect_args,
        hide_parameters=True,
    )
    name = "perennial_test_quality_" + secrets.token_hex(12)
    created = False
    try:
        with admin.connect() as conn:
            version = int(conn.execute(text("SHOW server_version_num")).scalar_one())
            if version // 10000 != 17:
                raise ValueError("quality gate requires PostgreSQL 17")
            conn.execute(text(f'CREATE DATABASE "{name}" TEMPLATE template0'))
            created = True
        engine = create_engine(
            url.set(database=name),
            connect_args=connect_args,
            hide_parameters=True,
        )
        try:
            with engine.connect() as conn:
                cfg = Config(str(REPO / "development/database/alembic.ini"))
                cfg.attributes["connection"] = conn
                command.upgrade(cfg, "head")
                command.downgrade(cfg, "base")
                command.upgrade(cfg, "head")
                command.check(cfg)
            LOGGER.info("Migration round-trip and model drift checks passed")
        finally:
            engine.dispose()
    finally:
        try:
            if created:
                with admin.connect() as conn:
                    conn.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        finally:
            admin.dispose()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        if sys.version_info[:2] != (3, 12):
            raise ValueError("quality gate requires Python 3.12")
        check_migrations(os.environ.get("TEST_DATABASE_URL", ""))
    except Exception as exc:
        LOGGER.error("Migration quality gate failed (%s)", type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
