"""Machine-readable checks for a supervisor/monitor. No credentials or payloads."""

from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from sqlalchemy import func, select, text
from sqlalchemy.engine import Engine

from src.db import models as m
from src.pipeline.publication import MAX_AGE, current
from src.settings import (
    PUBLICATION_DEADLINE,
    PUBLICATION_TIMEZONE,
    TRADING_HOLIDAYS,
    Settings,
)

# M2: degraded publishing is visible but not an outage; everything else is.
SEV2, SEV3 = "SEV2", "SEV3"


def publication_window_start(now: datetime, holidays: Iterable[date]) -> datetime:
    """Start of the trading day whose D6 deadline (10:30 PT) most recently passed.

    A publication created at or after this instant serves that trading day;
    weekends and listed NYSE holidays are skipped.
    """
    zone = ZoneInfo(PUBLICATION_TIMEZONE)
    local = now.astimezone(zone)
    closed = set(holidays)
    day = local.date()
    while True:
        deadline = datetime.combine(day, PUBLICATION_DEADLINE, zone)
        if day.weekday() < 5 and day not in closed and deadline <= local:
            return datetime.combine(day, datetime.min.time(), zone)
        day -= timedelta(days=1)


def _severity(issue: str, issues: list[str]) -> str:
    if issue.startswith("publication:degraded"):
        return SEV3
    source, _, state = issue.partition(":")
    if state in ("failed", "abandoned"):
        # A failed run the served publication still covers within policy.
        served = not any(
            i in issues
            for i in (
                "publication:missing",
                "publication:missed",
                "publication:stale",
                f"publication:{source}:stale",
                f"{source}:stale",
            )
        )
        return SEV3 if served else SEV2
    return SEV2


def health(
    engine: Engine,
    *,
    settings: Settings | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc) if now is None else now
    holidays = settings.trading_holidays if settings is not None else TRADING_HOLIDAYS
    issues: list[str] = []
    sources: dict[str, dict[str, Any] | None] = {}
    with engine.connect() as conn:
        for source in m.WATCHLIST_SOURCES:
            run = (
                conn.execute(
                    select(m.ingestion_runs)
                    .where(
                        m.ingestion_runs.c.source == source,
                        m.ingestion_runs.c.collection_mode == "live",
                    )
                    .order_by(
                        m.ingestion_runs.c.started_at.desc(),
                        m.ingestion_runs.c.id.desc(),
                    )
                    .limit(1)
                )
                .mappings()
                .one_or_none()
            )
            success = (
                conn.execute(
                    select(m.ingestion_runs)
                    .where(
                        m.ingestion_runs.c.source == source,
                        m.ingestion_runs.c.status == "succeeded",
                        m.ingestion_runs.c.collection_mode == "live",
                    )
                    .order_by(
                        m.ingestion_runs.c.collected_at.desc().nullslast(),
                        m.ingestion_runs.c.id.desc(),
                    )
                    .limit(1)
                )
                .mappings()
                .one_or_none()
            )
            if not run:
                issues.append(f"{source}:never_run")
                sources[source] = None
                continue
            sources[source] = {
                "run_id": run["id"],
                "status": run["status"],
                "error_code": run["error_code"],
                "accepted_count": run["accepted_count"],
                "coverage": run["coverage"],
                "duration_seconds": (
                    run["finished_at"] - run["started_at"]
                ).total_seconds()
                if run["finished_at"]
                else None,
                "last_success_at": success["collected_at"] if success else None,
            }
            if run["status"] in ("failed", "abandoned"):
                issues.append(f"{source}:{run['status']}")
            if run["status"] == "running" and (
                run["heartbeat_at"] is None
                or run["heartbeat_at"] < now - timedelta(minutes=10)
            ):
                issues.append(f"{source}:heartbeat_expired")
            if (
                not success
                or success["collected_at"] is None
                or now - success["collected_at"] > MAX_AGE[source]
            ):
                issues.append(f"{source}:stale")
        size = conn.execute(
            text("SELECT pg_database_size(current_database())")
        ).scalar_one()
        batches = conn.execute(
            select(func.count()).select_from(m.source_batches)
        ).scalar_one()
    pub = current(engine)
    if pub is None:
        issues.append("publication:missing")
    else:
        if now - pub["created_at"] > timedelta(days=3):
            issues.append("publication:stale")
        if pub["created_at"] < publication_window_start(now, holidays):
            issues.append("publication:missed")
        with engine.connect() as conn:
            marker = conn.execute(
                select(m.publication_events.c.checks).where(
                    m.publication_events.c.publication_id == pub["id"],
                    m.publication_events.c.kind == "degraded",
                )
            ).scalar_one_or_none()
        if marker is not None:
            issues.append(
                "publication:degraded:all_sources_reused"
                if marker.get("all_sources_reused")
                else "publication:degraded"
            )
    if pub is not None:
        # New imports do not refresh the actual served publication's inputs.
        with engine.connect() as conn:
            selected = conn.execute(
                select(
                    m.ingestion_runs.c.source,
                    m.ingestion_runs.c.collected_at,
                    m.source_batches.c.source_as_of,
                )
                .select_from(
                    m.publication_sources.join(
                        m.ingestion_runs,
                        m.ingestion_runs.c.id == m.publication_sources.c.run_id,
                    ).join(
                        m.source_batches,
                        m.source_batches.c.id == m.publication_sources.c.batch_id,
                    )
                )
                .where(m.publication_sources.c.publication_id == pub["id"])
            ).all()
            policy = pub["ranking_config"].get("freshness_seconds", {})
            for row in selected:
                age = timedelta(
                    seconds=policy.get(row.source, MAX_AGE[row.source].total_seconds())
                )
                if row.collected_at is None or now - row.collected_at > age:
                    issues.append(f"publication:{row.source}:stale")
                if row.source == "ark_holdings" and (
                    row.source_as_of is None
                    or row.source_as_of < (now - timedelta(days=7)).date()
                ):
                    issues.append("publication:ark_holdings:observation_stale")
            settlement = conn.execute(
                select(func.min(m.short_interest.c.settlement_date))
                .join(
                    m.publication_sources,
                    m.publication_sources.c.batch_id == m.short_interest.c.batch_id,
                )
                .where(
                    m.publication_sources.c.publication_id == pub["id"],
                    m.publication_sources.c.source == "short_interest",
                )
            ).scalar_one()
            if settlement and settlement < (now - timedelta(days=45)).date():
                issues.append("publication:short_interest:observation_stale")
    severities = {issue: _severity(issue, issues) for issue in issues}
    return {
        # Any issue, SEV3 included, is unhealthy: the CLI keeps exiting 1 (M2 Q4).
        "healthy": not issues,
        "severity": max(severities.values(), key=[SEV3, SEV2].index)
        if severities
        else "ok",
        "issue_severity": severities,
        "checked_at": now,
        "issues": issues,
        "sources": sources,
        "publication_id": pub["id"] if pub else None,
        "database_bytes": size,
        "batch_count": batches,
    }
