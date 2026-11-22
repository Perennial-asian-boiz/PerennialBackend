"""Finnhub quote and 52-week range per ticker (S3-01).

The free plan has no daily price history (stock/candle answers 403, measured
2026-11-16), so each run takes a snapshot instead: `quote` for the latest price
and the previous close, `stock/metric` for the 52-week high and low with their
dates. Bryan approved displaying Finnhub's prices on 2026-11-16.

The key goes in the X-Finnhub-Token header, never the URL. The free plan allows
60 calls a minute, so requests are spaced MIN_REQUEST_INTERVAL_SECONDS apart.
Retries on 429/5xx, size caps and JSON validation come from collectors._get_json.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import requests
from src.ingestion.collectors import _get_json
from src.services.news.finnhub import MIN_REQUEST_INTERVAL_SECONDS, FinnhubError

QUOTE_URL = "https://finnhub.io/api/v1/quote"
METRIC_URL = "https://finnhub.io/api/v1/stock/metric"


@dataclass(frozen=True)
class Quote:
    price: Decimal
    previous_close: Decimal | None
    quoted_at: datetime


@dataclass(frozen=True)
class Range:
    high: Decimal
    high_date: date | None
    low: Decimal
    low_date: date | None


def _positive(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return number if number.is_finite() and number > 0 else None


def _date(value: Any) -> date | None:
    try:
        return date.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        return None


def parse_quote(payload: Any) -> Quote | None:
    """None when Finnhub has no price for the ticker (it answers zeros)."""
    if not isinstance(payload, dict):
        raise FinnhubError("malformed_response")
    price, stamp = _positive(payload.get("c")), payload.get("t")
    if (
        price is None
        or not isinstance(stamp, int)
        or isinstance(stamp, bool)
        or stamp <= 0
    ):
        return None
    return Quote(
        price=price,
        previous_close=_positive(payload.get("pc")),
        quoted_at=datetime.fromtimestamp(stamp, tz=timezone.utc),
    )


def parse_range(payload: Any) -> Range | None:
    """None unless both ends are present, positive and in order."""
    if not isinstance(payload, dict):
        raise FinnhubError("malformed_response")
    metric = payload.get("metric")
    if not isinstance(metric, dict):
        return None
    high, low = _positive(metric.get("52WeekHigh")), _positive(metric.get("52WeekLow"))
    if high is None or low is None or low > high:
        return None
    return Range(
        high=high,
        high_date=_date(metric.get("52WeekHighDate")),
        low=low,
        low_date=_date(metric.get("52WeekLowDate")),
    )


class FinnhubPriceClient:
    def __init__(
        self,
        session: requests.Session,
        api_key: str,
        *,
        interval: float = MIN_REQUEST_INTERVAL_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.session, self.headers = session, {"X-Finnhub-Token": api_key}
        self.interval, self.monotonic, self.sleep = interval, monotonic, sleep
        self._last: float | None = None

    def _get(self, url: str, params: dict[str, str]) -> Any:
        if self._last is not None:
            wait = self.interval - (self.monotonic() - self._last)
            if wait > 0:
                self.sleep(wait)
        self._last = self.monotonic()
        response = _get_json(self.session, url, params=params, headers=self.headers)
        if not response.ok:
            raise FinnhubError(response.code or "request_failed")
        return response.data

    def quote(self, symbol: str) -> Quote | None:
        return parse_quote(self._get(QUOTE_URL, {"symbol": symbol}))

    def range_52w(self, symbol: str) -> Range | None:
        return parse_range(self._get(METRIC_URL, {"symbol": symbol, "metric": "all"}))
