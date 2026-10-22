"""The demo database guard: demo publishing only ever reaches a marked local DB.

A demo publication may pin fixture-mode runs, which every other database
refuses. The guard runs client-side before any work and server-side inside the
publishing transaction, so a misdirected URL can never publish demo data.
"""

import re

from sqlalchemy import text
from sqlalchemy.engine import Connection

DEMO_DATABASE = re.compile(r"perennial_demo(_[a-z0-9]+)?")
DEMO_MARKER = "perennial-demo"  # COMMENT ON DATABASE, set only by the demo bootstrap
LOOPBACK = frozenset({"127.0.0.1", "localhost", "::1"})
LIVE_MODES = ("live",)
DEMO_MODES = ("live", "fixture")


class DemoGuardError(RuntimeError):
    """Refused: not the marked local demo database. Fixed text only."""

    code = "demo_database_required"

    def __init__(self) -> None:
        super().__init__("demo mode requires the marked local demo database")


def check_demo_database(conn: Connection) -> None:
    """Client side: the URL host is loopback. Server side: the database's name
    and the marker comment that only the demo bootstrap sets. (The server's own
    address is not checked: a local container answers from a bridge address.)"""
    if conn.engine.url.host not in LOOPBACK:
        raise DemoGuardError()
    name, comment = conn.execute(
        text(
            "SELECT current_database(), shobj_description(d.oid, 'pg_database') "
            "FROM pg_database d WHERE d.datname = current_database()"
        )
    ).one()
    if not (DEMO_DATABASE.fullmatch(name) and comment == DEMO_MARKER):
        raise DemoGuardError()
