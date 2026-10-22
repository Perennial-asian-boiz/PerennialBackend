"""Daily GDELT macro-event run. Run once with: python -m src.services.gdelt.job

For each watched theme: store daily volume and tone up to the most recent
complete UTC day, keep up to five example articles from evidence outlets for
that day, and record whether that day's coverage is a spike. Every write is
an insert or upsert, never a delete, so rerunning the same day converges to
the same rows.
A failing theme is logged and skipped; the others still run.
"""

import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

import requests
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from src.db import models as m
from src.services.gdelt.client import DocApiClient, GdeltError
from src.services.gdelt.spikes import check_spike
from src.services.gdelt.themes import ThemeMap, load_theme_map
from src.utils.logger import configure_logging, log_exception

logger = logging.getLogger(__name__)
EVIDENCE_PER_DAY = 5


@dataclass
class RunSummary:
    day: date
    succeeded: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)
    events: list[str] = field(default_factory=list)


def _store_theme(
    engine: Engine, theme: str, day: date, volume, tone, evidence, now: datetime
) -> bool:
    days = [d for d in volume if d <= day]
    check = check_spike({d: volume[d].article_count for d in days}, day)
    with engine.begin() as conn:
        if days:
            rows = [
                dict(
                    theme=theme,
                    day=d,
                    article_count=volume[d].article_count,
                    total_articles=volume[d].total_articles,
                    avg_tone=round(tone[d], 4) if d in tone else None,
                    retrieved_at=now,
                )
                for d in days
            ]
            stmt = insert(m.gdelt_theme_days).values(rows)
            conn.execute(
                stmt.on_conflict_do_update(
                    index_elements=["theme", "day"],
                    set_={
                        c: stmt.excluded[c]
                        for c in (
                            "article_count",
                            "total_articles",
                            "avg_tone",
                            "retrieved_at",
                        )
                    },
                )
            )
        if evidence:
            conn.execute(
                insert(m.gdelt_articles)
                .values(
                    [
                        dict(
                            theme=theme,
                            day=day,
                            url=a.url,
                            title=a.title,
                            domain=a.domain,
                            seen_at=a.seen_at,
                        )
                        for a in evidence
                    ]
                )
                .on_conflict_do_nothing(index_elements=["theme", "day", "url"])
            )
        if check is not None:
            # Upsert, never delete: a rerun with revised counts flips is_spike in place.
            values = dict(
                article_count=check.article_count,
                baseline_median=round(check.baseline_median, 2),
                ratio=round(check.ratio, 2) if check.ratio is not None else None,
                is_spike=check.is_spike,
                checked_at=now,
            )
            conn.execute(
                insert(m.gdelt_spike_checks)
                .values(theme=theme, day=day, **values)
                .on_conflict_do_update(index_elements=["theme", "day"], set_=values)
            )
    return check is not None and check.is_spike


def run_gdelt(
    engine: Engine,
    *,
    client: DocApiClient | None = None,
    theme_map: ThemeMap | None = None,
    now: datetime | None = None,
) -> RunSummary:
    now = now or datetime.now(timezone.utc)
    theme_map = theme_map or load_theme_map()
    owned = client is None
    client = client or DocApiClient(requests.Session())
    summary = RunSummary(day=now.date() - timedelta(days=1))
    try:
        for theme in theme_map.themes:
            try:
                volume = client.volume(theme)
                tone = client.tone(theme)
                articles = client.articles(theme, summary.day)
            except GdeltError as exc:
                summary.failed[theme] = str(exc)
                log_exception(logger, "gdelt_theme_failed", exc)
                continue
            seen, evidence = set(), []
            for a in articles:
                if theme_map.is_evidence_domain(a.domain) and a.url not in seen:
                    seen.add(a.url)
                    evidence.append(a)
            if _store_theme(
                engine,
                theme,
                summary.day,
                volume,
                tone,
                evidence[:EVIDENCE_PER_DAY],
                now,
            ):
                summary.events.append(theme)
            summary.succeeded.append(theme)
    finally:
        if owned:
            client.session.close()
    logger.info(
        "gdelt run day=%s ok=%d failed=%d events=%s",
        summary.day,
        len(summary.succeeded),
        len(summary.failed),
        summary.events,
    )
    return summary


def run_scheduled() -> None:
    """Scheduler entry point: own the engine, log failures, never take the scheduler down."""
    from src.db.session import make_engine
    from src.ingestion.redaction import describe_exception

    engine = None
    try:
        engine = make_engine()
        run_gdelt(engine)
    except Exception as exc:
        logger.error("gdelt run failed: %s", describe_exception(exc))
    finally:
        if engine is not None:
            engine.dispose()


def main() -> int:
    from src.db.session import make_engine

    configure_logging()
    engine = make_engine()
    try:
        summary = run_gdelt(engine)
    finally:
        engine.dispose()
    logger.info(
        "gdelt_complete succeeded=%d failed=%d events=%d",
        len(summary.succeeded),
        len(summary.failed),
        len(summary.events),
    )
    return 1 if summary.failed and not summary.succeeded else 0


if __name__ == "__main__":
    sys.exit(main())
