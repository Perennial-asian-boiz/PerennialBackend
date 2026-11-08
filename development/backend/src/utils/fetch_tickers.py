"""
Tickers to track, read from the database (perennial.securities).

Every active security is one ticker. The map form is {company name: ticker},
taken from securities.name and securities.symbol. When a row has no usable
name (NULL, empty or whitespace), the ticker itself is used as the key, so the
entry becomes {ticker: ticker}. If two securities share a name (e.g. GOOG and
GOOGL are both "Alphabet Inc."), the first symbol in alphabetical order keeps
the name and the others fall back to {ticker: ticker}, so no ticker is lost.

The database URL comes from src.db.config (DATABASE_URL, or the local .env when
PERENNIAL_ENV=local). Every public function returns None when the database is
not configured, unreachable, or has no active securities, so callers can keep
their current set or use their own fallback instead of crashing.
"""

import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.db.config import DatabaseConfigError
from src.db.models import securities

log = logging.getLogger(__name__)


def _clean(entry) -> str | None:
    """Normalise one entry to an UPPERCASE ticker without '$', or None if unusable."""
    if not isinstance(entry, str):
        return None
    ticker = entry.strip().upper().lstrip("$")
    return ticker or None


def _query_rows(engine: Engine | None = None) -> list[tuple[str, str | None]]:
    """(symbol, name) of every active security, ordered by symbol.

    Raises DatabaseConfigError / SQLAlchemyError / OSError on failure. Pass an
    engine to reuse one; otherwise a short-lived engine is created and disposed.
    """
    owned = engine is None
    if owned:
        from src.db.session import make_engine

        engine = make_engine(workload="pipeline")
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                select(securities.c.symbol, securities.c.name)
                .where(securities.c.is_active.is_(True))
                .order_by(securities.c.symbol)
            ).all()
    finally:
        if owned:
            engine.dispose()
    return [(r.symbol, r.name) for r in rows]


def _read_rows(engine: Engine | None = None) -> list[tuple[str, str | None]] | None:
    """Like _query_rows, but returns None instead of raising."""
    try:
        return _query_rows(engine)
    except DatabaseConfigError as e:
        log.warning("cannot read tickers: %s", e)  # message is credential-free
    except (SQLAlchemyError, OSError) as e:
        # Only the type: SQLAlchemy messages can include connection details.
        log.warning("cannot read tickers from securities: %s", type(e).__name__)
    return None


def _read_ticker_list(engine: Engine | None = None) -> list[str] | None:
    """Active tickers in symbol order, de-duplicated, or None if unavailable/empty."""
    rows = _read_rows(engine)
    if rows is None:
        return None
    tickers = list(dict.fromkeys(t for t in (_clean(s) for s, _ in rows) if t))
    return tickers or None


def build_ticker_map(rows) -> dict[str, str]:
    """{company name: ticker} from (symbol, name) rows.

    A row without a usable name becomes {ticker: ticker}. A name already taken by
    an earlier row also falls back to {ticker: ticker}. Rows without a usable
    symbol are skipped.
    """
    mapping: dict[str, str] = {}
    for symbol, name in rows:
        ticker = _clean(symbol)
        if not ticker:
            continue
        key = name.strip() if isinstance(name, str) else ""
        if not key or (key in mapping and mapping[key] != ticker):
            key = ticker
        mapping.setdefault(key, ticker)
    return mapping


def _read_ticker_map(engine: Engine | None = None) -> dict[str, str] | None:
    rows = _read_rows(engine)
    if rows is None:
        return None
    return build_ticker_map(rows) or None


async def fetch_ticker_list() -> list[str] | None:
    """
    Ordered, de-duplicated list of tickers (for callers that care about order,
    e.g. round-robin processing). None if the source is unavailable right now.
    """
    return await asyncio.to_thread(_read_ticker_list)


async def fetch_tickers() -> set[str] | None:
    """
    Return the current ticker set from the database.
    Return None if the source is unavailable or unreadable right now, so the
    watcher keeps the current set instead of wiping it.

    Returns a set of UPPERCASE tickers without "$", or None.
    """
    tickers = await asyncio.to_thread(_read_ticker_list)
    return set(tickers) if tickers is not None else None


async def fetch_ticker_map() -> dict[str, str] | None:
    """
    Return the active securities as {company name: UPPERCASE ticker}, e.g.
    {"NVIDIA Corp": "NVDA", "AAPL": "AAPL"} (the second had no name). None if
    the source is unavailable or has no usable entries.
    """
    return await asyncio.to_thread(_read_ticker_map)
