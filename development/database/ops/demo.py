"""Walk through the database pipeline on recorded fixtures, in a local demo DB.

    DEMO_DATABASE_URL=postgresql+psycopg://USER@127.0.0.1:5433/perennial_demo \\
        python development/database/ops/demo.py [--create]

--create makes (if absent), marks and migrates the demo database on that local
server first. The walkthrough publishes, shows the current publication and a
byte-identical replay, then an ARK outage (degraded publication), a quality
refusal, an audited override, a rollback (once the rollback API is merged) and
health. It refuses any URL that is not loopback or whose database is not
perennial_demo[_suffix]; publishing re-checks the database's demo marker inside
the transaction. Output is compact JSON per step: ids, codes and counts only.
No provider is contacted and no provider key is read.
"""

import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "backend"))

from sqlalchemy.engine import make_url  # noqa: E402

LOOPBACK = {"127.0.0.1", "localhost", "::1"}


def main(argv: list[str]) -> int:
    from src.db.session import make_engine
    from src.pipeline.demo import create_demo_database, walkthrough
    from src.pipeline.demo_guard import DEMO_DATABASE

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    try:
        url = make_url(os.environ.get("DEMO_DATABASE_URL", ""))
    except Exception:
        print("error: DEMO_DATABASE_URL is not a database URL", file=sys.stderr)
        return 2
    if url.host not in LOOPBACK or not DEMO_DATABASE.fullmatch(url.database or ""):
        print(
            "error: demo mode needs a loopback DEMO_DATABASE_URL naming perennial_demo[_suffix]",
            file=sys.stderr,
        )
        return 2
    hostaddr = {"hostaddr": "127.0.0.1" if url.host == "localhost" else url.host}
    if "--create" in argv:
        admin = make_engine(
            url.set(database="postgres").render_as_string(hide_password=False),
            connect_args=hostaddr,
            isolation_level="AUTOCOMMIT",
        )
        try:
            create_demo_database(admin, url.database or "")
        finally:
            admin.dispose()
    engine = make_engine(url.render_as_string(hide_password=False), connect_args=hostaddr)
    try:
        for step in walkthrough(engine):
            print(json.dumps(step, default=str))
    except Exception as exc:
        print(f"error: demo stopped ({type(exc).__name__})", file=sys.stderr)
        return 1
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
