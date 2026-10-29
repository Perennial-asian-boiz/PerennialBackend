"""Fill empty securities.name from SEC's ticker list (decision 92 (b)).

Run by hand as the schema owner after new securities appear; never scheduled:
    SEC_USER_AGENT=... OPS_DATABASE_URL=... python development/database/ops/seed_names.py
One request, to SEC's company_tickers.json, with only the User-Agent header.
Only a NULL or blank name is filled, with SEC's title as filed (no recasing);
a name set by anyone else, even while this runs, is kept. A share class is
looked up with SEC's dash (BRK.B -> BRK-B). Output is counts only.
"""

import json
import os
import sys
from pathlib import Path

import requests
from sqlalchemy import create_engine, or_, select, update
from sqlalchemy.engine import Engine, make_url

BACKEND = Path(__file__).resolve().parents[2] / "backend"
sys.path.insert(0, str(BACKEND))

from src.db.models import securities  # noqa: E402
from src.services.fundamentals.edgar import (  # noqa: E402
    EdgarClient,
    EdgarError,
    user_agent_ok,
)


def _empty():  # a NULL or blank name
    return or_(securities.c.name.is_(None), securities.c.name.regexp_match(r"^\s*$"))


def run(engine: Engine, client: EdgarClient) -> dict[str, int]:
    titles = client.ticker_titles()  # before the transaction: a failure writes nothing
    filled = missing = 0
    with engine.begin() as conn:
        empty = conn.execute(
            select(securities.c.id, securities.c.symbol).where(_empty())
        ).all()
        for row in empty:
            title = titles.get(row.symbol.upper().replace(".", "-"))
            if title is None:
                missing += 1
                continue
            # Guarded: a name written since the read above is never overwritten.
            filled += conn.execute(
                update(securities)
                .where(securities.c.id == row.id, _empty())
                .values(name=title)
            ).rowcount
    return {"names_filled": filled, "not_in_sec_list": missing}


def main() -> int:
    agent = os.environ.get("SEC_USER_AGENT")
    if not user_agent_ok(agent):  # before any request or write
        sys.stdout.write(json.dumps({"error_code": "missing_credential"}) + "\n")
        return 1
    try:
        url = make_url(os.environ.get("OPS_DATABASE_URL", ""))
        if (
            url.drivername not in {"postgresql", "postgresql+psycopg"}
            or not url.host
            or not url.database
        ):
            raise ValueError("Expected explicit PostgreSQL OPS_DATABASE_URL")
        engine = create_engine(url.set(drivername="postgresql+psycopg"))
        counts = run(engine, EdgarClient(requests.Session(), agent))
    except EdgarError as exc:
        # A fixed code (http_503, ticker_map_malformed, ...), never response text.
        code = exc.args[0] if exc.args else "request_failed"
        sys.stderr.write(f"Name seed failed ({code}); nothing written\n")
        return 1
    except Exception as exc:
        sys.stderr.write(f"Name seed failed ({type(exc).__name__}); nothing written\n")
        return 1
    sys.stdout.write(json.dumps(counts, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
