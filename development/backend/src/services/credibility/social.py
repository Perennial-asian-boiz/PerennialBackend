"""Social post scores from the sentiment pipeline (development/backend/sentiment).

sentiment_manager runs the platform scripts. Each hands its per-ticker report to
the manager on its output (sentiment/results.py), nothing is written to disk:
    [{"ticker": "NVDA", "average_sentiment": 0.1378, "post_count": 2,
      "posts": {"<post id>": 0.4222, ...}}, ...]
with scores in [-1, 1]. The manager then calls run_social with {platform:
report}, which stores one social_posts row per (post, ticker) as one batch,
dated the day of the run. Tickers that are not stored canonical securities are
skipped and counted.
"""

import logging
from datetime import datetime
from typing import Any, Callable

from sqlalchemy.engine import Engine
from src.ingestion.diagnostics import exception_diagnostics
from src.ingestion.identity import resolve_tickers
from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection
from src.pipeline.lifecycle import begin_run, heartbeat_worker, source_lock

logger = logging.getLogger(__name__)

SOURCE = "social_posts"
MODE = "live"
PLATFORMS = ("bluesky", "mastodon", "youtube")  # reddit is not run


def read_report(data: Any) -> list[tuple[str, str, float]]:
    """(ticker, post id, score) for every well-formed entry; malformed ones are skipped."""
    out: list[tuple[str, str, float]] = []
    for entry in data if isinstance(data, list) else []:
        if not isinstance(entry, dict) or not isinstance(entry.get("ticker"), str):
            continue
        posts = entry.get("posts")
        for post_id, score in posts.items() if isinstance(posts, dict) else ():
            if (
                isinstance(score, (int, float))
                and not isinstance(score, bool)
                and -1 <= score <= 1
                and 0 < len(str(post_id)) <= 500
            ):
                out.append(
                    (entry["ticker"].strip().upper(), str(post_id), float(score))
                )
    return out


def collect_social(
    reports: dict[str, Any],
    known: Callable[[list[str]], set[str]],
    now: datetime,
    window_days: int,
) -> CollectionOutcome:
    """reports is {platform: report}; a platform that produced nothing is left out.
    known(tickers) returns the tickers that are stored canonical securities."""
    received = [p for p in PLATFORMS if reports.get(p) is not None]
    missing = [p for p in PLATFORMS if p not in received]
    if not received:
        return CollectionOutcome.failure(SOURCE, MODE, "missing_upstream")
    found: dict[tuple[str, str, str], float] = {}
    for platform in received:
        for ticker, post_id, score in read_report(reports[platform]):
            found.setdefault((ticker, platform, post_id), score)
    tickers = sorted({t for t, _, _ in found})
    usable = known(tickers)
    skipped = [t for t in tickers if t not in usable]
    records: list[dict[str, Any]] = [
        {"ticker": t, "platform": p, "post_id": pid, "score": round(score, 4)}
        for (t, p, pid), score in sorted(found.items())
        if t in usable
    ]
    return CollectionOutcome.success(
        SOURCE,
        MODE,
        records,
        source_as_of=now.date(),
        coverage={"complete": not missing, "scope": "recent_window"},
        envelope={
            "source": "sentiment_pipeline",
            "window_days": window_days,
            "fresh": received,
            "stale": [],
            "missing": missing,
            "posts": len(records),
            "skipped_tickers": skipped,
            "skipped_tickers_count": len(skipped),
        },
    )


def run_social(
    engine: Engine, now: datetime, reports: dict[str, Any], *, window_days: int
) -> ImportResult:
    """Store the platforms' reports as one social_posts batch."""

    def known(tickers: list[str]) -> set[str]:
        with engine.connect() as conn:
            ids, _failures = resolve_tickers(conn, SOURCE, tickers)
        return set(ids)

    with source_lock(engine, SOURCE):
        run_id = begin_run(engine, SOURCE, MODE)
        with heartbeat_worker(engine, run_id):
            try:
                outcome = collect_social(reports, known, now, window_days)
            except Exception as exc:  # a bug here: record it, write nothing (F2)
                outcome = CollectionOutcome.failure(
                    SOURCE,
                    MODE,
                    "internal_error",
                    diagnostics=exception_diagnostics(exc),
                )
            result = import_collection(engine, outcome, run_id=run_id)
    logger.info(
        "social run_id=%s status=%s code=%s posts=%s",
        result.run_id,
        result.status,
        result.error_code,
        result.accepted_count,
    )
    return result
