"""Social post scores from the sentiment pipeline's JSON files (development/backend/sentiment).

Each platform script writes <platform>/<platform>_sentiments.json:
    [{"ticker": "NVDA", "average_sentiment": 6.12, "post_count": 2,
      "posts": {"<post id>": 7.4, ...}}, ...]
with scores on the pipeline's 1-10 scale. This importer reads those files as
they are and stores one row per (post, ticker). A file older than the window is
stale and skipped; the files carry no post dates, so a file's modification time
stands for when its posts were collected. Tickers that are not stored canonical
securities are skipped and counted.
"""

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from sqlalchemy.engine import Engine
from src.ingestion.identity import resolve_tickers
from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection
from src.pipeline.lifecycle import begin_run, heartbeat_worker, source_lock

logger = logging.getLogger(__name__)

SOURCE = "social_posts"
SOCIAL_DIR = Path(__file__).resolve().parents[3] / "sentiment"
PLATFORMS = ("reddit", "bluesky", "mastodon", "youtube")


def platform_file(directory: Path, platform: str) -> Path:
    return directory / platform / f"{platform}_sentiments.json"


def read_report(path: Path) -> list[tuple[str, str, float]]:
    """(ticker, post id, score) for every well-formed entry; malformed ones are skipped."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    out: list[tuple[str, str, float]] = []
    for entry in data if isinstance(data, list) else []:
        if not isinstance(entry, dict) or not isinstance(entry.get("ticker"), str):
            continue
        posts = entry.get("posts")
        for post_id, score in posts.items() if isinstance(posts, dict) else ():
            if (
                isinstance(score, (int, float))
                and not isinstance(score, bool)
                and 1 <= score <= 10
                and 0 < len(str(post_id)) <= 500
            ):
                out.append(
                    (entry["ticker"].strip().upper(), str(post_id), float(score))
                )
    return out


def collect_social(
    directory: Path,
    known: Callable[[list[str]], set[str]],
    now: datetime,
    window_days: int,
) -> CollectionOutcome:
    """known(tickers) returns the tickers that are stored canonical securities."""
    fresh, stale, missing = [], [], []
    as_of: date | None = None
    found: dict[tuple[str, str, str], float] = {}
    for platform in PLATFORMS:
        path = platform_file(directory, platform)
        if not path.is_file():
            missing.append(platform)
            continue
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        if modified < now - timedelta(days=window_days):
            stale.append(platform)
            continue
        fresh.append(platform)
        as_of = max(as_of, modified.date()) if as_of else modified.date()
        for ticker, post_id, score in read_report(path):
            found.setdefault((ticker, platform, post_id), score)
    if not fresh:
        return CollectionOutcome.failure(SOURCE, "file", "missing_upstream")
    tickers = sorted({t for t, _, _ in found})
    usable = known(tickers)
    skipped = [t for t in tickers if t not in usable]
    records: list[dict[str, Any]] = [
        {"ticker": t, "platform": p, "post_id": pid, "score": round(score, 2)}
        for (t, p, pid), score in sorted(found.items())
        if t in usable
    ]
    return CollectionOutcome.success(
        SOURCE,
        "file",
        records,
        source_as_of=as_of,
        coverage={"complete": len(fresh) == len(PLATFORMS), "scope": "recent_window"},
        envelope={
            "source": "sentiment_pipeline_json",
            "window_days": window_days,
            "fresh": fresh,
            "stale": stale,
            "missing": missing,
            "posts": len(records),
            "skipped_tickers": skipped,
            "skipped_tickers_count": len(skipped),
        },
    )


def run_social(
    engine: Engine,
    now: datetime,
    *,
    window_days: int,
    directory: Path | None = None,
) -> ImportResult:
    directory = directory or SOCIAL_DIR

    def known(tickers: list[str]) -> set[str]:
        with engine.connect() as conn:
            ids, _failures = resolve_tickers(conn, SOURCE, tickers)
        return set(ids)

    with source_lock(engine, SOURCE):
        run_id = begin_run(engine, SOURCE, "file")
        with heartbeat_worker(engine, run_id):
            outcome = collect_social(directory, known, now, window_days)
            result = import_collection(engine, outcome, run_id=run_id)
    logger.info(
        "social run_id=%s status=%s code=%s posts=%s",
        result.run_id,
        result.status,
        result.error_code,
        result.accepted_count,
    )
    return result
