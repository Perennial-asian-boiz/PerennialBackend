"""Machine-readable checks for a supervisor/monitor. No credentials or payloads."""

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.engine import Engine

from src.db import models as m
from src.pipeline.publication import MAX_AGE, current


def health(engine: Engine) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    issues: list[str] = []
    sources: dict[str, dict[str, Any] | None] = {}
    with engine.connect() as conn:
        for source in m.SOURCES:
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
    elif now - pub["created_at"] > timedelta(days=3):
        issues.append("publication:stale")
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
    return {
        "healthy": not issues,
        "checked_at": now,
        "issues": issues,
        "sources": sources,
        "publication_id": pub["id"] if pub else None,
        "database_bytes": size,
        "batch_count": batches,
    }
