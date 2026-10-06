"""
Pydantic models for each fetcher's record shape.

Input keys are the fetcher's own (`amount_range`, `short_interest`,
`avg_daily_volume`); attribute names are the database's. Unknown keys are
ignored and never archived, including the volatile per-record `fetched_at`.

Rules shared by every source:
  * Tickers: strip + upper-case, reject placeholders (N/A, --, NONE, NULL, NAN)
    and anything outside [A-Z0-9./-]. Class-share punctuation is kept as given,
    so BRK.B and BRK-B are distinct securities. A recognized US composite
    exchange suffix (US_EXCHANGE_SUFFIXES, e.g. "RKLB UQ") becomes the base
    symbol plus that exchange; other suffixes are rejected.
  * Dates: ISO YYYY-MM-DD only. "" and null mean unknown; a missing required
    date or any non-ISO text fails the record (and so the whole batch).
  * Numbers: floats become Decimal via str(), never Decimal(float). NaN/Inf and
    booleans are rejected. "" and null mean unknown.
  * Validation messages never include the offending input value.
"""

import math
import re
from datetime import date
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any, ClassVar, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing_extensions import Annotated
from pydantic.functional_validators import BeforeValidator

PLACEHOLDER_TICKERS = frozenset({"N/A", "NONE", "NULL", "--", "NAN"})
BIGINT_MAX = 2**63 - 1
MAX_NUMBER_TEXT = 64
MAX_DECIMAL_EXPONENT = 30
_TICKER = re.compile(r"^[A-Z0-9][A-Z0-9./-]{0,19}$")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_FUND = re.compile(r"^[A-Z0-9]{1,10}$")
_SENSITIVE_QUERY_KEYS = re.compile(
    r"(?i)^(api[_-]?key|apikey|key|token|access[_-]?token|secret|password|sig|signature|auth.*)$"
)


# Bloomberg-style US composite exchange suffixes seen in ARK holdings
# ("RKLB UQ"). Only these are recognized; any other suffix fails validation.
US_EXCHANGE_SUFFIXES = {
    "UQ": "NASDAQ", "UW": "NASDAQ", "UR": "NASDAQ",
    "UN": "NYSE", "UA": "NYSEAMERICAN", "UP": "NYSEARCA",
}
_SUFFIXED = re.compile(r"^([A-Z0-9][A-Z0-9./-]{0,19}) +([A-Z]{2})$")


def split_exchange_suffix(value: Any) -> Tuple[Any, Optional[str]]:
    """'RKLB UQ' -> ('RKLB', 'NASDAQ'); anything else is returned unchanged."""
    if not isinstance(value, str):
        return value, None
    match = _SUFFIXED.match(value.strip().upper())
    if match and match.group(2) in US_EXCHANGE_SUFFIXES:
        return match.group(1), US_EXCHANGE_SUFFIXES[match.group(2)]
    return value, None


def normalize_symbol(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("ticker must be a string")
    cleaned = value.strip().upper()
    if not cleaned or cleaned in PLACEHOLDER_TICKERS:
        raise ValueError("ticker is missing or a placeholder")
    if not _TICKER.match(cleaned):
        raise ValueError("ticker has an unsupported format")
    return cleaned


def _optional_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not hasattr(value, "hour"):
        return value
    if not isinstance(value, str):
        raise ValueError("date must be an ISO YYYY-MM-DD string")
    value = value.strip()
    if not value:
        return None
    if not _ISO_DATE.match(value):
        raise ValueError("date must be ISO YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError("date is not a valid calendar date") from None


def _required_date(value: Any) -> date:
    parsed = _optional_date(value)
    if parsed is None:
        raise ValueError("required date is missing")
    return parsed


def _optional_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("number must not be a boolean")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("number must be finite")
        result = Decimal(str(value))
    elif isinstance(value, str):
        text = value.strip().replace(",", "")
        if not text:
            return None
        if len(text) > MAX_NUMBER_TEXT:
            raise ValueError("number text is too long")
        try:
            result = Decimal(text)
        except InvalidOperation:
            raise ValueError("number is not numeric") from None
    else:
        raise ValueError("number has an unsupported type")
    if not result.is_finite():
        raise ValueError("number must be finite")
    # Bound magnitude both ways so canonical text stays small (1e100000 would
    # otherwise expand to a 100 KB string) and quantize() cannot overflow.
    if result != 0 and not (-MAX_DECIMAL_EXPONENT <= result.adjusted() <= MAX_DECIMAL_EXPONENT):
        raise ValueError("number is out of range")
    return result


def _optional_int(value: Any) -> Optional[int]:
    dec = _optional_decimal(value)
    if dec is None:
        return None
    if dec != dec.to_integral_value():
        raise ValueError("number must be a whole number")
    if abs(dec) > BIGINT_MAX:
        raise ValueError("number is out of range")
    return int(dec)


def _decimal(places: int, digits: int, *, zero_is_unknown: bool = False):
    """
    Decimal matching a NUMERIC(digits, places) column: rounded half-even to the
    column scale here, so the hashed/payload value equals the stored value.
    Negative values are rejected (no stored quantity here can be negative).
    """
    quantum = Decimal(1).scaleb(-places)
    bound = Decimal(10) ** (digits - places)

    def validate(value: Any) -> Optional[Decimal]:
        dec = _optional_decimal(value)
        if dec is None or (zero_is_unknown and dec == 0):
            return None
        if dec < 0:
            raise ValueError("number must not be negative")
        if dec >= bound:
            raise ValueError("number is out of range")
        try:
            dec = dec.quantize(quantum, rounding=ROUND_HALF_EVEN)
        except InvalidOperation:
            raise ValueError("number is out of range") from None
        if dec >= bound:
            raise ValueError("number is out of range")
        return dec

    return Annotated[Optional[Decimal], BeforeValidator(validate)]


def _optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("value must be a string")
    value = " ".join(value.split())
    return value or None


def _required_text(value: Any) -> str:
    text = _optional_text(value)
    if text is None:
        raise ValueError("required text is missing")
    return text


def _optional_link(value: Any) -> Optional[str]:
    text = _optional_text(value)
    if text is None:
        return None
    parts = urlsplit(text)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ValueError("link must be an http(s) URL")
    if parts.username or parts.password or "@" in parts.netloc:
        raise ValueError("link must not embed credentials")
    for key, _ in parse_qsl(parts.query, keep_blank_values=True):
        if _SENSITIVE_QUERY_KEYS.match(key.strip()):
            raise ValueError("link contains a credential-like parameter")
    # Filing links never use fragments, and fragments can carry opaque tokens.
    if parts.fragment or text.endswith("#"):
        raise ValueError("link must not have a fragment")
    return text


Ticker = Annotated[str, BeforeValidator(normalize_symbol)]
RequiredDate = Annotated[date, BeforeValidator(_required_date)]
OptionalDate = Annotated[Optional[date], BeforeValidator(_optional_date)]
OptionalDecimal = Annotated[Optional[Decimal], BeforeValidator(_optional_decimal)]
OptionalInt = Annotated[Optional[int], BeforeValidator(_optional_int)]
def _bounded(validator, max_length: int):
    # Length is checked inside the validator: a Field(max_length) on an
    # Optional would also be applied to the None produced from "".
    def validate(value: Any):
        result = validator(value)
        if result is not None and len(result) > max_length:
            raise ValueError(f"text is longer than {max_length} characters")
        return result

    return validate


OptionalLink = Annotated[Optional[str], BeforeValidator(_bounded(_optional_link, 2048))]


def _text(max_length: int, required: bool = False):
    validator = _required_text if required else _optional_text
    kind = str if required else Optional[str]
    return Annotated[kind, BeforeValidator(_bounded(validator, max_length))]


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        normalized = value.normalize()
        if normalized == 0:
            return "0"
        return format(normalized, "f")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [_json_value(v) for v in value]
    if isinstance(value, BaseModel):
        return {k: _json_value(v) for k, v in value}
    return value


class SourceRecord(BaseModel):
    """Base for one normalized fetcher record."""

    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)

    # Columns stored in the source table, in addition to batch/security ids.
    DB_COLUMNS: ClassVar[Tuple[str, ...]] = ()

    ticker: Ticker
    exchange: _text(20) = None
    currency: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _exchange_suffix(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        base, exchange = split_exchange_suffix(data.get("ticker"))
        if exchange is None:
            return data
        given = data.get("exchange")
        if isinstance(given, str) and given.strip() and given.strip().upper() != exchange:
            raise ValueError("ticker exchange suffix conflicts with exchange")
        return dict(data, ticker=base, exchange=exchange)

    @field_validator("exchange")
    @classmethod
    def _upper_exchange(cls, v: Optional[str]) -> Optional[str]:
        return v.upper() if v else None

    @field_validator("currency", mode="before")
    @classmethod
    def _currency(cls, v: Any) -> Optional[str]:
        text = _optional_text(v)
        if text is None:
            return None
        text = text.upper()
        if not re.fullmatch(r"[A-Z]{3}", text):
            raise ValueError("currency must be a 3-letter code")
        return text

    def canonical(self) -> Dict[str, Any]:
        """JSON-safe, deterministic representation used for hashing and the batch payload."""
        return {name: _json_value(getattr(self, name)) for name in type(self).model_fields}

    def db_values(self) -> Dict[str, Any]:
        return {name: getattr(self, name) for name in self.DB_COLUMNS}


class CongressTrade(SourceRecord):
    DB_COLUMNS = (
        "politician_name", "chamber", "trade_type", "transaction_date", "disclosure_date",
        "amount_label", "asset_description", "asset_type", "district", "source_link", "data_source",
    )

    politician_name: _text(200, required=True)
    chamber: _text(16) = None
    trade_type: _text(100, required=True)
    transaction_date: RequiredDate
    disclosure_date: OptionalDate = None
    amount_label: _text(100) = Field(default=None, alias="amount_range")
    asset_description: _text(500) = None
    asset_type: _text(100) = None
    district: _text(20) = None
    source_link: OptionalLink = None
    data_source: _text(32) = None


class ArkHolding(SourceRecord):
    DB_COLUMNS = ("company", "funds", "fund_count", "total_weight", "share_price")

    company: _text(300) = None
    funds: List[str] = Field(min_length=1, max_length=20)
    fund_count: int = Field(ge=1, strict=True)
    # Sum of weights across funds; not a 0-100 allocation, so no upper bound.
    total_weight: _decimal(8, 20)
    # ark.parse_holdings defaults a missing price to 0.0, so 0 means unknown.
    share_price: _decimal(6, 20, zero_is_unknown=True) = None
    bucket: _text(32) = None
    data_source: _text(32) = None

    @field_validator("funds", mode="before")
    @classmethod
    def _funds(cls, v: Any) -> List[str]:
        if not isinstance(v, list):
            raise ValueError("funds must be a list")
        funds = []
        for item in v:
            if not isinstance(item, str) or not _FUND.match(item.strip().upper()):
                raise ValueError("fund symbol has an unsupported format")
            funds.append(item.strip().upper())
        if len(set(funds)) != len(funds):
            raise ValueError("funds must not repeat")
        return sorted(funds)

    @model_validator(mode="after")
    def _fund_count_matches(self) -> "ArkHolding":
        if self.total_weight is None:
            raise ValueError("total_weight is required")
        if self.fund_count != len(self.funds):
            raise ValueError("fund_count does not match funds")
        return self


class InsiderTrade(SourceRecord):
    DB_COLUMNS = (
        "insider_name", "transaction_type", "transaction_date", "shares", "value",
        "bucket", "data_source",
    )

    insider_name: _text(200, required=True)
    transaction_type: _text(50, required=True)
    transaction_date: RequiredDate
    shares: _decimal(6, 24) = None
    value: _decimal(2, 20) = None
    bucket: _text(32) = None
    data_source: _text(32) = None


class ShortInterestHistory(BaseModel):
    """One settlement row of returned history. Stored in the batch payload only."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    settlement_date: OptionalDate = None
    short_interest: OptionalInt = None
    avg_daily_volume: OptionalInt = None
    days_to_cover: OptionalDecimal = None


class ShortInterestRecord(SourceRecord):
    DB_COLUMNS = ("settlement_date", "short_interest_shares", "average_daily_volume", "days_to_cover")

    settlement_date: RequiredDate
    # Explicit rename at the database boundary (fetcher key -> column).
    short_interest_shares: OptionalInt = Field(default=None, alias="short_interest")
    average_daily_volume: OptionalInt = Field(default=None, alias="avg_daily_volume")
    days_to_cover: _decimal(4, 12) = None
    history: List[ShortInterestHistory] = Field(default_factory=list, max_length=120)
    data_source: _text(32) = None

    @field_validator("short_interest_shares", "average_daily_volume")
    @classmethod
    def _non_negative(cls, v):
        if v is not None and v < 0:
            raise ValueError("value must not be negative")
        return v


RECORD_MODELS = {
    "congress_trades": CongressTrade,
    "ark_holdings": ArkHolding,
    "insider_trades": InsiderTrade,
    "short_interest": ShortInterestRecord,
}

# Top-level key holding the record list in each fetcher's JSON output.
RECORDS_KEY = {
    "congress_trades": "trades",
    "ark_holdings": "holdings",
    "insider_trades": "transactions",
    "short_interest": "records",
}

# Sources whose rows are one-per-security (unique (batch_id, security_id)).
ONE_ROW_PER_SECURITY = frozenset({"ark_holdings", "short_interest"})
