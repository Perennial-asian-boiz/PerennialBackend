"""Finnhub's free company profile (GET /stock/profile2), read for market caps.

Shared by the database adapter (market_caps.FinnhubMarketCapAdapter) and the
legacy JSON pipeline (consensus.py), so both read the response the same way.

Finnhub reports marketCapitalization in MILLIONS of the profile's currency;
parse_profile returns whole units. The value is company-wide: every share
class of one company reports the same total. An unknown symbol is an empty
object, and a missing, null or zero marketCapitalization means "no value".
"""

from decimal import Decimal, InvalidOperation
from typing import Any, NamedTuple

PROFILE_URL = "https://finnhub.io/api/v1/stock/profile2"
MILLION = Decimal(1_000_000)
MAX_VALUE = Decimal("1e22")  # market_caps.value is NUMERIC(24, 2)


class Profile(NamedTuple):
    market_cap: Decimal | None  # whole units of `currency`
    currency: str | None
    industry: str | None  # Finnhub's single finnhubIndustry label


class MalformedProfile(ValueError):
    """The response is not a Finnhub profile for the requested symbol."""


def _spelling(symbol: str) -> str:
    # Finnhub may write a share class as BRK.B or BRK-B.
    return symbol.strip().upper().replace(".", "-")


def parse_profile(payload: Any, symbol: str) -> Profile | None:
    """The profile for `symbol` (the spelling sent to Finnhub), or None if unknown."""
    if not isinstance(payload, dict):
        raise MalformedProfile
    if not payload:
        return None
    ticker = payload.get("ticker")
    if ticker is not None and (
        not isinstance(ticker, str) or _spelling(ticker) != _spelling(symbol)
    ):
        raise MalformedProfile
    raw = payload.get("marketCapitalization")
    market_cap = None
    if raw is not None:
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise MalformedProfile
        try:
            millions = Decimal(str(raw))
        except InvalidOperation:
            raise MalformedProfile from None
        if not millions.is_finite() or millions < 0:
            raise MalformedProfile
        if millions > 0:
            market_cap = millions * MILLION
            if market_cap >= MAX_VALUE:
                raise MalformedProfile
    currency = payload.get("currency")
    industry = payload.get("finnhubIndustry")
    return Profile(
        market_cap=market_cap,
        currency=currency.strip().upper() if isinstance(currency, str) else None,
        industry=industry if isinstance(industry, str) else None,
    )
