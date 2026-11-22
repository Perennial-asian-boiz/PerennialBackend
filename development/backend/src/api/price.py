"""S3-01: a security's latest close, for GET /v1/securities/{symbol}/price.

Serves the security's newest daily_prices row from a licensed provider
(licensed_price_providers.json, AC 3). Rows from any other provider are left
out before anything is computed, so an unlicensed price can't show up in the
change either, and with no licensed row the body is the same as with no row at
all (AC 4).

Computed here: change = close - previous close, and change_percent against the
previous close; both null without a previous close. stale is true once the
trading day is more than STALE_AFTER_DAYS calendar days old (about five trading
days, with no holiday calendar).
"""

import hashlib
import json
import re
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.engine import Connection

from src.db import models as m

LICENCE_FILE = Path(__file__).with_name("licensed_price_providers.json")
STALE_AFTER_DAYS = 7
PLACES = 6
# Exactly the daily_prices columns the API is granted (never row_number or quoted_at).
COLUMNS = (
    "id",
    "batch_id",
    "security_id",
    "trading_date",
    "close",
    "previous_close",
    "high_52w",
    "high_52w_date",
    "low_52w",
    "low_52w_date",
    "currency",
    "provider",
)
_PROVIDER = re.compile(r"[a-z0-9_]{1,32}")
_ENTRY_KEYS = {"provider", "name", "terms", "scope", "approved_by", "approved_on"}


class PriceResponse(BaseModel):
    api_version: str = Field(examples=["v1"])
    symbol: str = Field(description="The canonical ticker")
    status: str = Field(
        description="ok, or unavailable when no licensed price is stored"
    )
    source: str | None = Field(description="The licensed provider, e.g. Finnhub")
    as_of: str | None = Field(description="The trading day of the close")
    currency: str | None = Field(examples=["USD"])
    close: float | None
    previous_close: float | None
    change: float | None = Field(description="close - previous_close")
    change_percent: float | None = Field(description="change / previous_close x 100")
    high_52w: float | None
    high_52w_date: str | None
    low_52w: float | None
    low_52w_date: str | None
    stale: bool = Field(
        description=f"The close is more than {STALE_AFTER_DAYS} days old"
    )


def parse_licences(data: Any) -> dict[str, str]:
    """provider -> display name. Refuses an entry without its terms or approver."""
    if not isinstance(data, dict) or not isinstance(data.get("providers"), list):
        raise ValueError("licences must list providers")
    names: dict[str, str] = {}
    for entry in data["providers"]:
        if not isinstance(entry, dict) or set(entry) != _ENTRY_KEYS:
            raise ValueError(f"each licence needs exactly {sorted(_ENTRY_KEYS)}")
        if not all(isinstance(entry[k], str) and entry[k].strip() for k in _ENTRY_KEYS):
            raise ValueError("licence fields must be non-empty text")
        provider = entry["provider"]
        if not _PROVIDER.fullmatch(provider) or provider in names:
            raise ValueError("a licence names a provider once, as stored")
        if not entry["terms"].startswith("https://"):
            raise ValueError("a licence links its terms over https")
        date.fromisoformat(entry["approved_on"])
        names[provider] = entry["name"]
    return names


LICENSED = parse_licences(json.loads(LICENCE_FILE.read_text()))

UNAVAILABLE: dict[str, Any] = {
    "status": "unavailable",
    "source": None,
    "as_of": None,
    "currency": None,
    "close": None,
    "previous_close": None,
    "change": None,
    "change_percent": None,
    "high_52w": None,
    "high_52w_date": None,
    "low_52w": None,
    "low_52w_date": None,
    "stale": False,
}


def _served(value: Decimal | None) -> float | None:
    return None if value is None else float(round(value, PLACES))


def _day(value: date | None) -> str | None:
    return None if value is None else value.isoformat()


def served(
    conn: Connection,
    security_id: int,
    today: date,
    licensed: dict[str, str] | None = None,
) -> tuple[dict[str, Any], str]:
    """(body without api_version and symbol, ETag). Rows never change, so the row
    id names the price; the licences and the stale flag complete the ETag."""
    licensed = LICENSED if licensed is None else licensed
    digest = hashlib.sha256(",".join(sorted(licensed)).encode()).hexdigest()[:8]
    p = m.daily_prices
    row = (
        conn.execute(
            select(*(p.c[name] for name in COLUMNS))
            .where(p.c.security_id == security_id, p.c.provider.in_(list(licensed)))
            .order_by(p.c.batch_id.desc())
            .limit(1)
        ).one_or_none()
        if licensed
        else None
    )
    if row is None:
        return dict(UNAVAILABLE), f'"v1-price-0-{digest}"'
    change = None if row.previous_close is None else row.close - row.previous_close
    stale = (today - row.trading_date).days > STALE_AFTER_DAYS
    body = {
        "status": "ok",
        "source": licensed[row.provider],
        "as_of": row.trading_date.isoformat(),
        "currency": row.currency,
        "close": _served(row.close),
        "previous_close": _served(row.previous_close),
        "change": _served(change),
        "change_percent": _served(
            None if change is None else change / row.previous_close * 100
        ),
        "high_52w": _served(row.high_52w),
        "high_52w_date": _day(row.high_52w_date),
        "low_52w": _served(row.low_52w),
        "low_52w_date": _day(row.low_52w_date),
        "stale": stale,
    }
    return body, f'"v1-price-{row.id}-{digest}-{int(stale)}"'
