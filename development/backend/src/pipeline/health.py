"""Machine-readable checks for a supervisor/monitor. No credentials or payloads."""
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, func, text

from src.db import models as m
from src.pipeline.publication import MAX_AGE, current


def health(engine):
    now = datetime.now(timezone.utc)
    issues, sources = [], {}
    with engine.connect() as conn:
        for source in m.SOURCES:
            run = conn.execute(select(m.ingestion_runs).where(m.ingestion_runs.c.source == source)
                .order_by(m.ingestion_runs.c.started_at.desc(), m.ingestion_runs.c.id.desc()).limit(1)).mappings().one_or_none()
            success = conn.execute(select(m.ingestion_runs).where(m.ingestion_runs.c.source == source,
                m.ingestion_runs.c.status == "succeeded").order_by(m.ingestion_runs.c.collected_at.desc().nullslast(),
                m.ingestion_runs.c.id.desc()).limit(1)).mappings().one_or_none()
            if not run:
                issues.append(f"{source}:never_run")
                sources[source] = None
                continue
            sources[source] = {"run_id": run["id"], "status": run["status"], "error_code": run["error_code"],
                "accepted_count": run["accepted_count"], "coverage": run["coverage"],
                "duration_seconds": (run["finished_at"]-run["started_at"]).total_seconds() if run["finished_at"] else None,
                "last_success_at": success["collected_at"] if success else None}
            if run["status"] in ("failed", "abandoned"):
                issues.append(f"{source}:{run['status']}")
            if run["status"] == "running" and (run["heartbeat_at"] is None or run["heartbeat_at"] < now-timedelta(minutes=10)):
                issues.append(f"{source}:heartbeat_expired")
            if not success or success["collected_at"] is None or now-success["collected_at"] > MAX_AGE[source]:
                issues.append(f"{source}:stale")
        size = conn.execute(text("SELECT pg_database_size(current_database())")).scalar_one()
        batches = conn.execute(select(func.count()).select_from(m.source_batches)).scalar_one()
    pub = current(engine)
    if pub is None:
        issues.append("publication:missing")
    elif now-pub["created_at"] > timedelta(days=3):
        issues.append("publication:stale")
    return {"healthy": not issues, "checked_at": now, "issues": issues, "sources": sources,
            "publication_id": pub["id"] if pub else None, "database_bytes": size, "batch_count": batches}
