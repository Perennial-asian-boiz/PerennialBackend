"""GDELT DOC 2.0 API: daily article volume, daily tone, and example articles per theme.

GDELT allows one request every 5 seconds; Pacer enforces that across all calls.
429 retries and response-size caps come from the shared collectors._get_json.
"""
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable

import requests

from src.ingestion.collectors import _get_json

DOC_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
MIN_REQUEST_INTERVAL_SECONDS = 5.5
TIMELINE_SPAN = "3m"
ARTICLE_SAMPLE_SIZE = 75


class GdeltError(RuntimeError):
    """A theme's data could not be fetched or parsed; carries a short code, never payload text."""


class Pacer:
    def __init__(
        self,
        interval: float = MIN_REQUEST_INTERVAL_SECONDS,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.interval, self.monotonic, self.sleep = interval, monotonic, sleep
        self.last: float | None = None

    def wait(self) -> None:
        if self.last is not None:
            remaining = self.interval - (self.monotonic() - self.last)
            if remaining > 0:
                self.sleep(remaining)
        self.last = self.monotonic()


@dataclass(frozen=True)
class DayVolume:
    article_count: int
    total_articles: int | None


@dataclass(frozen=True)
class Article:
    url: str
    title: str | None
    domain: str | None
    seen_at: datetime | None


def _query(theme: str) -> str:
    return f"theme:{theme} sourcelang:english"


def _day(stamp: Any) -> date:
    # Timeline points look like "20261013T000000Z"; sub-daily points share a date prefix.
    if not isinstance(stamp, str) or len(stamp) < 8 or not stamp[:8].isdigit():
        raise GdeltError("timeline_date_malformed")
    return datetime.strptime(stamp[:8], "%Y%m%d").date()


def _timeline_points(payload: Any) -> list[dict[str, Any]]:
    if payload == {}:
        return []  # GDELT answers a query with no matches with an empty object.
    if not isinstance(payload, dict) or not isinstance(payload.get("timeline"), list):
        raise GdeltError("timeline_malformed")
    if not payload["timeline"]:
        return []
    points = payload["timeline"][0].get("data") if isinstance(payload["timeline"][0], dict) else None
    if not isinstance(points, list) or not all(isinstance(p, dict) for p in points):
        raise GdeltError("timeline_malformed")
    return points


def _number(value: Any, code: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GdeltError(code)
    return value


def parse_volume(payload: Any) -> dict[date, DayVolume]:
    counts: dict[date, int] = defaultdict(int)
    totals: dict[date, int] = defaultdict(int)
    has_total: set[date] = set()
    for point in _timeline_points(payload):
        day = _day(point.get("date"))
        count = _number(point.get("value"), "volume_malformed")
        if count < 0:
            raise GdeltError("volume_malformed")
        counts[day] += int(count)
        if point.get("norm") is not None:
            totals[day] += int(_number(point["norm"], "volume_malformed"))
            has_total.add(day)
    return {d: DayVolume(counts[d], totals[d] if d in has_total else None) for d in counts}


def parse_tone(payload: Any) -> dict[date, float]:
    values: dict[date, list[float]] = defaultdict(list)
    for point in _timeline_points(payload):
        values[_day(point.get("date"))].append(float(_number(point.get("value"), "tone_malformed")))
    return {d: sum(v) / len(v) for d, v in values.items()}


def _seen_at(raw: Any) -> datetime | None:
    try:
        return datetime.strptime(raw, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _text(raw: Any) -> str | None:
    return raw.strip() or None if isinstance(raw, str) else None


def parse_articles(payload: Any) -> list[Article]:
    if payload == {}:
        return []
    if not isinstance(payload, dict) or not isinstance(payload.get("articles"), list):
        raise GdeltError("articles_malformed")
    articles = []
    for item in payload["articles"]:
        if not isinstance(item, dict) or not isinstance(item.get("url"), str) or not item["url"]:
            continue
        domain = _text(item.get("domain"))
        articles.append(Article(
            url=item["url"],
            title=_text(item.get("title")),
            domain=domain.lower() if domain else None,
            seen_at=_seen_at(item.get("seendate")),
        ))
    return articles


class DocApiClient:
    def __init__(self, session: requests.Session, pacer: Pacer | None = None):
        self.session, self.pacer = session, pacer or Pacer()

    def _get(self, params: dict[str, Any]) -> Any:
        self.pacer.wait()
        response = _get_json(self.session, DOC_API_URL, params={**params, "format": "json"})
        if not response.ok:
            raise GdeltError(response.code or "request_failed")
        return response.data

    def volume(self, theme: str) -> dict[date, DayVolume]:
        return parse_volume(self._get(
            {"query": _query(theme), "mode": "timelinevolraw", "timespan": TIMELINE_SPAN}))

    def tone(self, theme: str) -> dict[date, float]:
        return parse_tone(self._get(
            {"query": _query(theme), "mode": "timelinetone", "timespan": TIMELINE_SPAN}))

    def articles(self, theme: str, day: date) -> list[Article]:
        stamp = day.strftime("%Y%m%d")
        return parse_articles(self._get({
            "query": _query(theme),
            "mode": "artlist",
            "maxrecords": ARTICLE_SAMPLE_SIZE,
            "sort": "hybridrel",
            "startdatetime": f"{stamp}000000",
            "enddatetime": f"{stamp}235959",
        }))
