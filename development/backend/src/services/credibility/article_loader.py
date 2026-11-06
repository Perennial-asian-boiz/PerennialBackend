"""The article loader: which stored news articles the scorer reads next, and the
record of failed scoring attempts (AC 4).

unscored_articles(conn, start, end, model_version) returns, by news_article_id,
every news_articles row published in [start, end] that has no article_sentiment
row from model_version and fewer than MAX_ATTEMPTS failed attempts for it, with
the fields the scorer needs (id, symbol, headline, summary).

record_attempts(engine, model_version, scored, failed) runs after a scoring
run: scored articles lose their attempts row; each failed article gains one
attempt (the row is created at 1) with its error code. An article at
MAX_ATTEMPTS stays unscored and is not read again for that model version.

The commented-out block at the end is the article downloader (full article text
from each url, as an honest bot that honours robots.txt). It is kept here,
disabled, until downloading is picked up again; enabling it means uncommenting
it, adding requests/trafilatura handling to the CI rules and its tests back.
"""

from collections.abc import Collection, Mapping
from datetime import datetime
from typing import Any

from sqlalchemy import delete, exists, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection, Engine
from src.db import models as m

MAX_ATTEMPTS = m.ARTICLE_SCORING_MAX_ATTEMPTS


def unscored_articles(
    conn: Connection, start: datetime, end: datetime, model_version: str
) -> list[Any]:
    """The window's articles with no score from model_version and attempts left,
    by id."""
    na, a, t = m.news_articles, m.article_sentiment, m.article_scoring_attempts
    scored = exists().where(a.c.news_article_id == na.c.id, a.c.model == model_version)
    exhausted = exists().where(
        t.c.news_article_id == na.c.id,
        t.c.model == model_version,
        t.c.attempts >= MAX_ATTEMPTS,
    )
    return list(
        conn.execute(
            select(na.c.id, m.securities.c.symbol, na.c.headline, na.c.summary)
            .join(m.securities, m.securities.c.id == na.c.security_id)
            .where(
                na.c.published_at >= start,
                na.c.published_at <= end,
                ~scored,
                ~exhausted,
            )
            .order_by(na.c.id)
        ).all()
    )


def record_attempts(
    engine: Engine,
    model_version: str,
    scored: Collection[int],
    failed: Mapping[int, str],
) -> None:
    """In one transaction: forget the attempts of `scored` articles, and add one
    attempt with its error code (one of ARTICLE_SCORING_ERRORS) to each `failed`
    article."""
    t = m.article_scoring_attempts
    with engine.begin() as conn:
        if scored:
            conn.execute(
                delete(t).where(
                    t.c.model == model_version, t.c.news_article_id.in_(scored)
                )
            )
        if failed:
            stmt = insert(t).values(
                [
                    {
                        "news_article_id": article_id,
                        "model": model_version,
                        "attempts": 1,
                        "last_error": code,
                    }
                    for article_id, code in sorted(failed.items())
                ]
            )
            conn.execute(
                stmt.on_conflict_do_update(
                    index_elements=[t.c.news_article_id, t.c.model],
                    set_={
                        "attempts": t.c.attempts + 1,
                        "last_error": stmt.excluded.last_error,
                        "updated_at": func.now(),
                    },
                )
            )


# ── the article downloader (deferred; disabled) ───────────────────────────
#
# import logging
# import os
# import threading
# from collections.abc import Sequence
# from concurrent.futures import ThreadPoolExecutor
# from dataclasses import dataclass, field
# from urllib.parse import urljoin, urlsplit
# from urllib.robotparser import RobotFileParser
#
# import requests
#
# logger = logging.getLogger(__name__)
#
# WORKERS = 8  # pages downloaded at once
# TIMEOUT = 20  # seconds per request
# MIN_BODY_CHARS = 200  # a shorter extraction counts as a failed download
# MAX_REDIRECTS = 5
# # Honest bot identity. Sites can see who we are, read our contact address, and
# # block us through robots.txt. SCRAPER_CONTACT overrides the contact address.
# BOT_NAME = "PerennialSentimentBot"
# BOT_VERSION = "1.0"
# CONTACT = os.environ.get("SCRAPER_CONTACT", "").strip() or "perennial491@gmail.com"
# USER_AGENT = f"{BOT_NAME}/{BOT_VERSION} (+mailto:{CONTACT})"
#
#
# @dataclass
# class _SiteRobots:
#     """One site's robots.txt, fetched at most once; lock serialises the fetch."""
#
#     lock: threading.Lock = field(default_factory=threading.Lock)
#     parser: RobotFileParser | None = None
#
#
# _robots: dict[str, _SiteRobots] = {}  # "scheme://host" -> its robots.txt
# _robots_lock = threading.Lock()
# _session_local = threading.local()
#
#
# def _session() -> requests.Session:
#     """One requests.Session per download thread, carrying the bot's headers."""
#     session: requests.Session | None = getattr(_session_local, "s", None)
#     if session is None:
#         session = _session_local.s = requests.Session()
#         session.headers.update(
#             {
#                 "User-Agent": USER_AGENT,
#                 "From": CONTACT,  # the HTTP header meant for a bot's contact
#                 "Accept-Language": "en-US,en;q=0.9",
#             }
#         )
#     return session
#
#
# def _fetch_robots(site: str) -> RobotFileParser:
#     parser = RobotFileParser()
#     try:
#         response = _session().get(site + "/robots.txt", timeout=TIMEOUT)
#     except requests.RequestException:
#         response = None
#     if response is None or response.status_code in (401, 403):
#         # Can't tell, or refused (the rule urllib's own read() uses): crawl nothing.
#         parser.parse(["User-agent: *", "Disallow: /"])
#     elif response.status_code >= 400:
#         parser.parse([])  # no robots.txt: everything allowed
#     else:
#         parser.parse(response.text.splitlines())
#     return parser
#
#
# def _robots_for(url: str) -> RobotFileParser:
#     """Parsed robots.txt for url's site, fetched once per site per process."""
#     parts = urlsplit(url)
#     site = f"{parts.scheme}://{parts.netloc}"
#     with _robots_lock:
#         entry = _robots.setdefault(site, _SiteRobots())
#     with entry.lock:  # other threads for this site wait for one fetch
#         if entry.parser is None:
#             entry.parser = _fetch_robots(site)
#         return entry.parser
#
#
# def allowed_by_robots(url: str) -> bool:
#     """True if url's robots.txt lets our user agent fetch it."""
#     return _robots_for(url).can_fetch(USER_AGENT, url)
#
#
# def _fetch_page(url: str) -> tuple[str, str] | None:
#     """(final url, html) of the page at url, or None if it can't be had.
#
#     Redirects (e.g. Finnhub's redirect links) are followed one hop at a time so
#     robots.txt is checked on every url we request.
#     """
#     try:
#         for _ in range(MAX_REDIRECTS + 1):
#             if urlsplit(url).scheme not in ("http", "https"):
#                 return None
#             if not allowed_by_robots(url):
#                 return None
#             response = _session().get(url, timeout=TIMEOUT, allow_redirects=False)
#             if response.is_redirect and "location" in response.headers:
#                 url = urljoin(url, response.headers["location"])
#                 continue
#             if response.status_code >= 400:  # incl. 401/403/429: never retried
#                 return None
#             return url, response.text
#     except requests.RequestException:
#         return None
#     return None  # too many redirects
#
#
# def download_article_text(url: str | None) -> str | None:
#     """The main text of the article at url, or None if it can't be had."""
#     import trafilatura
#
#     if not url:
#         return None
#     page = _fetch_page(url)
#     if page is None:
#         return None
#     final_url, html = page
#     try:
#         body = trafilatura.extract(
#             html,
#             url=final_url,
#             include_comments=False,
#             include_tables=False,
#             favor_precision=True,
#         )
#     except Exception:  # a page trafilatura can't parse is a failed download
#         return None
#     if not isinstance(body, str) or len(body.strip()) < MIN_BODY_CHARS:
#         return None
#     return body.strip()
#
#
# def download_bodies(urls: Sequence[str | None]) -> list[str | None]:
#     """download_article_text for every url, in parallel; results in input order.
#     Without trafilatura installed every url gives None."""
#     try:
#         import trafilatura  # noqa: F401
#     except ImportError:
#         logger.warning("article downloader: trafilatura missing, no bodies")
#         return [None] * len(urls)
#     if not urls:
#         return []
#     with ThreadPoolExecutor(max_workers=WORKERS) as pool:
#         return list(pool.map(download_article_text, urls))
