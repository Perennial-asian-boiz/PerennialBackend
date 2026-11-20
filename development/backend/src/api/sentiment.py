"""S3-02 piece 1: a security's Sentiment Pulse breakdown, for
GET /v1/securities/{symbol}/sentiment.

Spec: docs/technical-specs/SENTIMENT_PULSE.md. The current reading is the
security's row in the newest ticker_sentiment batch (only successful imports
store rows, so that batch is the newest successful tally). Its
ticker_sentiment_items (0026) are summed per channel:

* news: the weighted share of each label, sum(weight of the label) / sum(weight),
  with the final weights the tally used (tier x age decay x relevance);
* social: the plain share of each label, count(label) / count.

A channel with fewer than MIN_COVERAGE_ITEMS items is "not_enough_coverage" and
has no shares. A security with no row in the newest batch is unavailable
("no_reading"), and so is a row without items ("no_breakdown": tallied before
0026). Reads only the columns perennial_api is granted on ticker_sentiment and
ticker_sentiment_items, never a headline, URL or post id.
"""

from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.engine import Connection

from src.db import models as m

MIN_COVERAGE_ITEMS = 3
LABELS = ("positive", "neutral", "negative")
PLACES = 4
TREND_DAYS = 30
# Exactly provision_roles.API_COLUMNS (src can't import ops).
SENTIMENT_COLUMNS = ("id", "batch_id", "security_id", "tallied_on", "value")
ITEM_COLUMNS = ("id", "ticker_sentiment_id", "kind", "label", "weight", "item_at")


class LabelCounts(BaseModel):
    positive: int
    neutral: int
    negative: int


class LabelShares(BaseModel):
    positive: float
    neutral: float
    negative: float


class Channel(BaseModel):
    status: str = Field(description="ok, or not_enough_coverage below min_items")
    weighted: bool = Field(
        description="True for news: shares are weighted by source credibility and age"
    )
    count: int = Field(description="Items counted in this channel")
    first_at: str | None = Field(description="The earliest item's date, ISO 8601 UTC")
    last_at: str | None = Field(description="The latest item's date, ISO 8601 UTC")
    counts: LabelCounts
    shares: LabelShares | None = Field(
        description="Each label's share, to 4 places; null when not_enough_coverage"
    )


class SentimentResponse(BaseModel):
    api_version: str = Field(examples=["v1"])
    symbol: str = Field(description="The canonical ticker")
    status: str = Field(description="ok, or unavailable")
    reason: str | None = Field(
        default=None, description="Only when unavailable: no_reading or no_breakdown"
    )
    tallied_on: str | None = Field(
        default=None, description="The UTC date the reading was tallied"
    )
    min_items: int | None = Field(
        default=None, description="Items a channel needs for shares"
    )
    news: Channel | None = None
    social: Channel | None = None


class TrendPoint(BaseModel):
    date: str = Field(description="A UTC calendar day, YYYY-MM-DD")
    value: float | None = Field(
        description="That day's stored reading, -1 to +1, to 4 places; null when "
        "nothing was tallied or scored that day (a gap, not zero)"
    )


class SentimentTrendResponse(BaseModel):
    api_version: str = Field(examples=["v1"])
    symbol: str = Field(description="The canonical ticker")
    days: int = Field(description="Days in the window (30)")
    start: str = Field(description="The window's first day, YYYY-MM-DD (UTC)")
    end: str = Field(description="The window's last day: today, UTC")
    points: list[TrendPoint] = Field(description="One point per day, oldest first")


def _iso(at: Any) -> str | None:
    return None if at is None else at.astimezone(timezone.utc).isoformat()


def _round(value: Decimal) -> float:
    return float(round(value, PLACES))


def _channel(items: Sequence[Any], weighted: bool) -> dict[str, Any]:
    counts = {label: sum(i.label == label for i in items) for label in LABELS}
    shares: dict[str, float] | None = None
    if len(items) >= MIN_COVERAGE_ITEMS:
        total = sum((Decimal(i.weight) for i in items), Decimal(0))
        if weighted and total > 0:
            shares = {
                label: _round(
                    sum(
                        (Decimal(i.weight) for i in items if i.label == label),
                        Decimal(0),
                    )
                    / total
                )
                for label in LABELS
            }
        else:  # social, or (an edge case) news whose every weight faded to 0
            shares = {
                label: _round(Decimal(counts[label]) / len(items)) for label in LABELS
            }
    dates = [i.item_at for i in items]
    return {
        "status": "ok" if shares is not None else "not_enough_coverage",
        "weighted": weighted,
        "count": len(items),
        "first_at": _iso(min(dates)) if dates else None,
        "last_at": _iso(max(dates)) if dates else None,
        "counts": counts,
        "shares": shares,
    }


def served(conn: Connection, security_id: int) -> tuple[dict[str, Any], str]:
    """(body without api_version and symbol, ETag). Stored batches and items never
    change, so the batch and row ids are the ETag."""
    ts, ti = m.ticker_sentiment, m.ticker_sentiment_items
    batch = conn.execute(select(func.max(ts.c.batch_id))).scalar()
    row = None
    if batch is not None:
        row = conn.execute(
            select(*(ts.c[name] for name in SENTIMENT_COLUMNS)).where(
                ts.c.batch_id == batch, ts.c.security_id == security_id
            )
        ).one_or_none()
    if row is None:
        return {"status": "unavailable", "reason": "no_reading"}, (
            f'"v1-sentiment-{batch or 0}-0"'
        )
    etag = f'"v1-sentiment-{batch}-{row.id}"'
    items = conn.execute(
        select(*(ti.c[name] for name in ITEM_COLUMNS))
        .where(ti.c.ticker_sentiment_id == row.id)
        .order_by(ti.c.id)
    ).all()
    if not items:
        return {"status": "unavailable", "reason": "no_breakdown"}, etag
    return {
        "status": "ok",
        "tallied_on": row.tallied_on.isoformat(),
        "min_items": MIN_COVERAGE_ITEMS,
        "news": _channel([i for i in items if i.kind == "news"], weighted=True),
        "social": _channel([i for i in items if i.kind == "social"], weighted=False),
    }, etag


def today() -> date:
    """The trend window's last day: the current UTC date (tests replace this)."""
    return datetime.now(timezone.utc).date()


def trend(conn: Connection, security_id: int, end: date) -> tuple[dict[str, Any], str]:
    """(body without api_version and symbol, ETag) for the TREND_DAYS days ending on
    `end`: each day's stored ticker_sentiment value, or null. A day tallied more than
    once uses its newest batch. A day with no row, or whose row counted nothing
    (value NULL), is a gap."""
    ts = m.ticker_sentiment
    start = end - timedelta(days=TREND_DAYS - 1)
    rows = conn.execute(
        select(ts.c.tallied_on, ts.c.batch_id, ts.c.value)
        .where(
            ts.c.security_id == security_id,
            ts.c.tallied_on >= start,
            ts.c.tallied_on <= end,
        )
        .order_by(ts.c.tallied_on, ts.c.batch_id)
    ).all()
    by_day: dict[date, Any] = {}
    for row in rows:  # ordered by batch, so the newest batch of a day wins
        by_day[row.tallied_on] = row
    points = []
    for offset in range(TREND_DAYS):
        day = start + timedelta(days=offset)
        stored = by_day.get(day)
        value = (
            None
            if stored is None or stored.value is None
            else _round(Decimal(stored.value))
        )
        points.append({"date": day.isoformat(), "value": value})
    newest = max((row.batch_id for row in rows), default=0)
    etag = f'"v1-sentiment-trend-{end.isoformat()}-{newest}"'
    return {
        "days": TREND_DAYS,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "points": points,
    }, etag
