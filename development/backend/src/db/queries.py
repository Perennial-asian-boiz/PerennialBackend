"""
Operational read helpers. Latest data for a source is the batch referenced by
its most recent successful ingestion run, including fixture and file modes.
Production reads instead use the current_publication pointer and pinned batches.
Rows are never aggregated across snapshots.
"""

from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.engine import Connection, Row

from src.db.models import SOURCE_TABLES, SOURCES, ingestion_runs, securities, source_batches


def latest_successful_run(conn: Connection, source: str) -> Optional[Row]:
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}")
    return conn.execute(
        select(
            ingestion_runs.c.id.label("run_id"),
            ingestion_runs.c.batch_id,
            ingestion_runs.c.finished_at,
            ingestion_runs.c.reused_batch,
            ingestion_runs.c.collection_mode,
            source_batches.c.content_hash,
            source_batches.c.record_count,
            source_batches.c.source_as_of,
        )
        .join(source_batches, source_batches.c.id == ingestion_runs.c.batch_id)
        .where(ingestion_runs.c.source == source, ingestion_runs.c.status == "succeeded")
        .order_by(ingestion_runs.c.finished_at.desc(), ingestion_runs.c.id.desc())
        .limit(1)
    ).one_or_none()


def latest_successful_batch_id(conn: Connection, source: str) -> Optional[int]:
    row = latest_successful_run(conn, source)
    return row.batch_id if row else None


def latest_rows(conn: Connection, source: str) -> List[Dict[str, Any]]:
    """Rows of the current batch for `source`, with the security symbol."""
    batch_id = latest_successful_batch_id(conn, source)
    if batch_id is None:
        return []
    return batch_rows(conn, source, batch_id)


def batch_rows(conn: Connection, source: str, batch_id: int, *, limit: Optional[int] = None,
               offset: int = 0, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    """Read one pinned batch; apply pagination in SQL, never re-resolve latest."""
    if source not in SOURCES:
        raise ValueError("unknown source")
    actual = conn.execute(select(source_batches.c.source).where(source_batches.c.id == batch_id)).scalar_one_or_none()
    if actual != source:
        raise ValueError("batch does not belong to requested source")
    if offset < 0 or (limit is not None and limit < 0):
        raise ValueError("pagination must be nonnegative")
    table = SOURCE_TABLES[source]
    order = table.c.row_number if "row_number" in table.c else securities.c.symbol
    query = (
        select(securities.c.symbol, table)
        .join(securities, securities.c.id == table.c.security_id)
        .where(table.c.batch_id == batch_id)
        .order_by(order)
        .offset(offset)
    )
    if limit is not None:
        query = query.limit(limit)
    if symbol is not None:
        query = query.where(securities.c.symbol == symbol.strip().upper())
    result = conn.execute(query)
    return [dict(row._mapping) for row in result]
