"""Finnhub company-news: articles per ticker over a date window.

The key goes in the X-Finnhub-Token header, never the URL. The free plan allows
60 calls a minute, so requests are spaced MIN_REQUEST_INTERVAL_SECONDS apart.
Retries on 429/5xx, size caps and JSON validation come from collectors._get_json.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests
from src.ingestion.collectors import _get_json

COMPANY_NEWS_URL = "https://finnhub.io/api/v1/company-news"
MIN_REQUEST_INTERVAL_SECONDS = 1.1


class FinnhubError(RuntimeError):
    """A request failed; carries a short code, never response text."""


@dataclass(frozen=True)
class RawArticle:
    finnhub_id: int
    headline: str
    summary: str
    outlet: str
    url: str
    published_at: datetime


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def parse_articles(payload: Any) -> tuple[list[RawArticle], int]:
    """Usable articles and the count skipped for missing id, headline, url or time."""
    if not isinstance(payload, list):
        raise FinnhubError("response_malformed")
    articles: list[RawArticle] = []
    skipped = 0
    for item in payload:
        if not isinstance(item, dict):
            skipped += 1
            continue
        fid, stamp = item.get("id"), item.get("datetime")
        headline, url = _text(item.get("headline")), _text(item.get("url"))
        if (
            not isinstance(fid, int)
            or isinstance(fid, bool)
            or fid <= 0
            or not isinstance(stamp, int)
            or isinstance(stamp, bool)
            or stamp <= 0
            or not headline
            or not url.startswith(("http://", "https://"))
        ):
            skipped += 1
            continue
        articles.append(
            RawArticle(
                finnhub_id=fid,
                headline=headline[:500],
                summary=_text(item.get("summary")),
                outlet=(_text(item.get("source")) or "unknown")[:100],
                url=url[:2048],
                published_at=datetime.fromtimestamp(stamp, tz=timezone.utc),
            )
        )
    return articles, skipped


class FinnhubNewsClient:
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

    def company_news(
        self, symbol: str, start: date, end: date
    ) -> tuple[list[RawArticle], int]:
        if self._last is not None:
            wait = self.interval - (self.monotonic() - self._last)
            if wait > 0:
                self.sleep(wait)
        self._last = self.monotonic()
        response = _get_json(
            self.session,
            COMPANY_NEWS_URL,
            params={"symbol": symbol, "from": start.isoformat(), "to": end.isoformat()},
            headers=self.headers,
        )
        if not response.ok:
            raise FinnhubError(response.code or "request_failed")
        return parse_articles(response.data)
