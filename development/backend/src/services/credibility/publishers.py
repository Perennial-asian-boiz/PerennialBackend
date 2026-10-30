"""The publisher behind each Finnhub article link.

Finnhub labels most articles "Yahoo", but each link answers with one redirect to
the real publisher. The redirect's Location header names it, so the page itself
is never downloaded. Lookups are spaced like Finnhub API calls. Only successful
lookups are stored; a failed one is retried while the article is in the window.
"""

import logging
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import requests
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from src.db import models as m
from src.services.news.finnhub import MIN_REQUEST_INTERVAL_SECONDS

logger = logging.getLogger(__name__)

REDIRECTS = {301, 302, 303, 307, 308}
MAX_LOOKUPS_PER_RUN = 1500  # about 28 minutes; a first backfill finishes next run
TIMEOUT_SECONDS = 15


def host_of(location: str | None) -> str | None:
    """The redirect target's host, lowercased and without www.; None if unusable."""
    parts = urlsplit(location or "")
    host = (parts.hostname or "").lower().removeprefix("www.")
    if parts.scheme not in ("http", "https") or not host or host.endswith("finnhub.io"):
        return None
    return host


class PublisherResolver:
    def __init__(
        self,
        session: requests.Session,
        *,
        interval: float = MIN_REQUEST_INTERVAL_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.session, self.interval = session, interval
        self.monotonic, self.sleep = monotonic, sleep
        self._last: float | None = None

    def resolve(self, url: str) -> str | None:
        if self._last is not None:
            wait = self.interval - (self.monotonic() - self._last)
            if wait > 0:
                self.sleep(wait)
        self._last = self.monotonic()
        try:
            response = self.session.get(
                url, allow_redirects=False, stream=True, timeout=TIMEOUT_SECONDS
            )
        except requests.RequestException:
            return None
        try:
            if response.status_code not in REDIRECTS:
                return None
            return host_of(response.headers.get("Location"))
        finally:
            response.close()


def resolve_publishers(
    engine: Engine,
    resolver: PublisherResolver,
    now: datetime,
    *,
    window_days: int,
    limit: int = MAX_LOOKUPS_PER_RUN,
) -> tuple[int, int]:
    """Look up articles in the window with no publisher yet; (resolved, failed)."""
    na, np_ = m.news_articles, m.news_publishers
    with engine.connect() as conn:
        rows = conn.execute(
            select(na.c.finnhub_id, na.c.url)
            .distinct(na.c.finnhub_id)
            .where(
                na.c.published_at >= now - timedelta(days=window_days),
                na.c.finnhub_id.not_in(select(np_.c.finnhub_id)),
            )
            .order_by(na.c.finnhub_id)
            .limit(limit)
        ).all()
    resolved = failed = 0
    for fid, url in rows:
        domain = resolver.resolve(url)
        if domain is None:
            failed += 1
            continue
        with engine.begin() as conn:
            conn.execute(
                insert(np_)
                .values(
                    finnhub_id=fid,
                    domain=domain,
                    resolved_at=datetime.now(timezone.utc),
                )
                .on_conflict_do_nothing()
            )
        resolved += 1
    logger.info("publishers resolved=%d failed=%d", resolved, failed)
    return resolved, failed
