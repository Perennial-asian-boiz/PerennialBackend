"""
Test setup.

Database tests need TEST_DATABASE_URL pointing at a disposable PostgreSQL
server on loopback, with "test" in the database name. They never use
DATABASE_URL. Each pytest session creates its own database named
perennial_test_<random>, migrates it with Alembic, and drops exactly that
database at the end, so concurrent sessions and other databases on the server
are untouched. Without TEST_DATABASE_URL the database tests are skipped.

The real development/backend/.env is never loaded: dotenv.load_dotenv is
replaced before any fetcher module is imported.
"""

import os
import secrets
from pathlib import Path

import dotenv
import pytest

dotenv.load_dotenv = lambda *args, **kwargs: False  # keep real credentials out of tests

# libpq reads PGHOST, PGHOSTADDR, PGSERVICE, PGSERVICEFILE, ... as connection
# defaults, which could send a loopback-looking URL elsewhere. Tests never
# inherit them (this also covers subprocesses started by tests).
for _name in [n for n in os.environ if n.startswith("PG")]:
    del os.environ[_name]

from alembic import command  # noqa: E402
from alembic.config import Config  # noqa: E402
from sqlalchemy import text  # noqa: E402

from src.db.config import parse_database_url  # noqa: E402
from src.db.models import metadata  # noqa: E402
from src.db.session import make_engine  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"
ALEMBIC_INI = Path(__file__).resolve().parents[2] / "database" / "alembic.ini"
LOOPBACK = {"127.0.0.1": "127.0.0.1", "localhost": "127.0.0.1", "::1": "::1"}


# Query parameters libpq honours can reroute a loopback-looking URL (host=,
# hostaddr=, port=, dbname=, service=, ...), so only TLS mode is allowed.
ALLOWED_QUERY_KEYS = {"sslmode"}


def check_test_server_url(url) -> None:
    """Raise ValueError unless the URL can only reach a loopback *test* database."""
    if url.host not in LOOPBACK or "test" not in (url.database or ""):
        raise ValueError("TEST_DATABASE_URL must be a loopback server and a *test* database")
    if set(url.query) - ALLOWED_QUERY_KEYS:
        raise ValueError("TEST_DATABASE_URL may not carry connection-routing query parameters")


def _test_server_url():
    raw = os.environ.get("TEST_DATABASE_URL")
    if not raw:
        pytest.skip("TEST_DATABASE_URL not set; database tests skipped")
    url = parse_database_url(raw, name="TEST_DATABASE_URL")
    try:
        check_test_server_url(url)
    except ValueError as exc:
        pytest.exit(str(exc), returncode=2)
    return url


def alembic_config(connection) -> Config:
    cfg = Config(str(ALEMBIC_INI))
    cfg.attributes["connection"] = connection
    return cfg


def pinned_engine(url, **kwargs):
    """Engine whose effective target is pinned to loopback (hostaddr overrides any default)."""
    return make_engine(url, connect_args={"hostaddr": LOOPBACK[url.host]}, **kwargs)


@pytest.fixture(scope="session")
def db_url():
    server = _test_server_url()
    name = f"perennial_test_{secrets.token_hex(4)}"
    admin = pinned_engine(server, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        yield server.set(database=name)
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


@pytest.fixture(scope="session")
def migrated_engine(db_url):
    engine = pinned_engine(db_url)
    with engine.connect() as conn:
        command.upgrade(alembic_config(conn), "head")
    yield engine
    engine.dispose()


@pytest.fixture
def engine(migrated_engine):
    """A migrated database with every application table emptied."""
    names = ", ".join(f"perennial.{t.name}" for t in metadata.sorted_tables)
    with migrated_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {names} RESTART IDENTITY CASCADE"))
    return migrated_engine


@pytest.fixture
def fixture_path():
    return lambda name: FIXTURES / name
