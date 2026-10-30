"""Finnhub's free company profile (GET /stock/profile2), read for market caps.

Shared by the database adapter (market_caps.FinnhubMarketCapAdapter) and the
legacy JSON pipeline (consensus.py), so both read the response the same way.

Finnhub reports marketCapitalization in MILLIONS; parse_profile returns whole
units. The value is company-wide: every share class of one company reports
the same total. An unknown symbol is an empty object, and a missing, null or
zero marketCapitalization means "no value".

A profile describes the company's primary listing, which need not be the
symbol asked for: BRK-B answers as BRK.A, TSM as 2330.TW, ASML as ASML.AS.
usd_value() keeps a value only when it is plainly US dollars for a US company
or for the listing asked for. For a foreign primary listing the `currency`
field can disagree with the listing's own currency (AZN.L, GIL.TO and WPM.TO
all say USD), so the number may not be dollars and is not used.
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
    ticker: str | None  # the listing the profile describes
    country: str | None


class MalformedProfile(ValueError):
    """The response is not a Finnhub company profile."""


def _spelling(symbol: str) -> str:
    # Finnhub may write a share class as BRK.B or BRK-B.
    return symbol.strip().upper().replace(".", "-")


def _text(value: Any) -> str | None:
    return value.strip().upper() if isinstance(value, str) else None


def parse_profile(payload: Any) -> Profile | None:
    """The profile in a /stock/profile2 response, or None for an unknown symbol."""
    if not isinstance(payload, dict):
        raise MalformedProfile
    if not payload:
        return None
    ticker = payload.get("ticker")
    if ticker is not None and not isinstance(ticker, str):
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
    industry = payload.get("finnhubIndustry")
    return Profile(
        market_cap=market_cap,
        currency=_text(payload.get("currency")),
        industry=industry if isinstance(industry, str) else None,
        ticker=_text(ticker),
        country=_text(payload.get("country")),
    )


def usd_value(profile: Profile | None, asked: str) -> Decimal | None:
    """The company-wide USD market cap to use for `asked`, or None."""
    if profile is None or profile.market_cap is None or profile.currency != "USD":
        return None
    same_listing = profile.ticker is None or _spelling(profile.ticker) == _spelling(
        asked
    )
    if same_listing or profile.country == "US":  # e.g. BRK-B answered as BRK.A
        return profile.market_cap
    return None
