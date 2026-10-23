"""The demo database guard: demo publishing only ever reaches a marked local DB.

A demo publication may pin fixture-mode runs, which every other database
refuses. The guard runs client-side before any work and server-side inside the
publishing transaction, so a misdirected URL can never publish demo data.
"""

import re
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine, make_url

DEMO_DATABASE = re.compile(r"perennial_demo(_[a-z0-9]+)?")
DEMO_MARKER = "perennial-demo"  # COMMENT ON DATABASE, set only by the demo bootstrap
LOOPBACK = frozenset({"127.0.0.1", "localhost", "::1"})
LOOPBACK_ADDRESSES = frozenset({"127.0.0.1", "::1"})
LIVE_MODES = ("live",)
DEMO_MODES = ("live", "fixture")


class DemoGuardError(RuntimeError):
    """Refused: not the marked local demo database. Fixed text only."""

    code = "demo_database_required"

    def __init__(self) -> None:
        super().__init__("demo mode requires the marked local demo database")


def _target_is_local(info: Any) -> bool:
    """The connection's *effective* target, from libpq: a hostaddr if one was
    used, else the host (a socket directory counts as local)."""
    hostaddr = getattr(info, "hostaddr", "") or ""
    host = getattr(info, "host", "") or ""
    if hostaddr:
        return hostaddr in LOOPBACK_ADDRESSES
    return host.startswith("/") or host in LOOPBACK


def require_local(conn: Connection) -> None:
    """Refuse unless this live connection actually reached a loopback server.

    Checked on the driver connection, not the URL: a URL can say localhost while
    a hostaddr parameter, connect_args or ambient libpq settings route elsewhere
    (SEC-DEMO-02). The server's own address is not used: a local container
    answers from a bridge address.
    """
    info = getattr(conn.connection.dbapi_connection, "info", None)
    if info is None or not _target_is_local(info):
        raise DemoGuardError()


def check_demo_database(conn: Connection) -> None:
    """Effective loopback target, then (server side) the demo name and the
    marker comment that only the demo bootstrap sets on a database it created."""
    require_local(conn)
    name, comment = conn.execute(
        text(
            "SELECT current_database(), shobj_description(d.oid, 'pg_database') "
            "FROM pg_database d WHERE d.datname = current_database()"
        )
    ).one()
    if not (DEMO_DATABASE.fullmatch(name) and comment == DEMO_MARKER):
        raise DemoGuardError()


def database_marker(conn: Connection, name: str) -> str | None:
    return conn.execute(
        text(
            "SELECT shobj_description(oid, 'pg_database') FROM pg_database "
            "WHERE datname = :name"
        ),
        {"name": name},
    ).scalar_one_or_none()


ALLOWED_QUERY_KEYS = frozenset({"sslmode"})


def local_engine(url: str, *, demo_name: bool = True, **kwargs: Any) -> Engine:
    """A pinned, sanitized engine for the demo: loopback host, no routing query
    options, hostaddr pinned so ambient libpq routing cannot redirect it."""
    from src.db.session import make_engine

    parsed = make_url(url)
    if parsed.host not in LOOPBACK or set(parsed.query) - ALLOWED_QUERY_KEYS:
        raise DemoGuardError()
    if demo_name and not DEMO_DATABASE.fullmatch(parsed.database or ""):
        raise DemoGuardError()
    hostaddr = "::1" if parsed.host == "::1" else "127.0.0.1"
    connect_args = dict(kwargs.pop("connect_args", {}), hostaddr=hostaddr)
    return make_engine(
        parsed.render_as_string(hide_password=False),
        connect_args=connect_args,
        **kwargs,
    )
