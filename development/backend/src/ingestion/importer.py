"""
Shared batch importer. Every entry point (JSON file, fixture, live fetch)
goes through import_collection(); there is no other write path.

Flow for one call:
  1. A failed collection is recorded as a failed run. No batch is written, so
     the latest successful batch for that source is untouched.
  2. Records are validated. Any invalid record fails the whole import.
  3. Records are canonicalized and hashed (src/ingestion/canonical.py).
  4. One transaction inserts the batch (or finds the identical existing one),
     resolves securities, inserts source rows for a new batch, and records the
     succeeded run. Any error rolls all of it back.
  5. After a rollback, the failed run is recorded in its own transaction.

Concurrent identical imports: the batch insert uses ON CONFLICT DO NOTHING on
(source, content_hash). A second transaction waits for the first, then finds
its committed batch and records a reuse; if the first rolled back, the second
inserts. Deadlocks/serialization failures are retried a few times.

Failed-input diagnostics and retention policy:
  * Failed inputs are never archived. A run records an error code, a summary
    rendered from that code's fixed template, and diagnostics in the closed
    shape of src/ingestion/diagnostics.py (counts, record indexes, known field
    paths, error types, ticker-shaped unit names). Input values, payload text,
    URLs and exception messages are never stored, on success or failure.
  * Bounds: at most 20 entries per list and 8 KB per run.
  * Retention: after each failed run, diagnostics on that source's older
    failed runs are cleared once they are beyond the newest
    DIAGNOSTICS_KEEP_PER_SOURCE or older than DIAGNOSTICS_MAX_AGE_DAYS. Run
    rows (status, counts, code, summary) are kept as the audit trail.
    Succeeded runs keep only collection counters, which are not input data.
"""

import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence

from pydantic import ValidationError
from sqlalchemy import insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from src.db.models import (
    COLLECTION_MODES,
    ERROR_SUMMARY_MAX,
    SOURCE_TABLES,
    SOURCES,
    ingestion_runs,
    securities,
    source_batches,
)
from src.ingestion.canonical import HASH_VERSION, CanonicalBatch, canonicalize
from src.ingestion.diagnostics import (
    MAX_ENTRIES,
    exception_diagnostics,
    render_summary,
    safe_code,
    safe_diagnostics,
)
from src.ingestion.schemas import ONE_ROW_PER_SECURITY, RECORD_MODELS, SourceRecord
from src.pipeline.lifecycle import (
    RunIdentityError,
    RunInactive,
    begin_run,
    require_active,
)


def safe_coverage(value: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    scope = value.get("scope")
    if scope not in ("snapshot", "recent_window", "selected_universe"):
        return None
    return {"complete": value.get("complete") is True, "scope": scope}


MAX_RECORDS = 50_000
DIAGNOSTICS_KEEP_PER_SOURCE = 20
DIAGNOSTICS_MAX_AGE_DAYS = 30
RETRYABLE_SQLSTATES = frozenset({"40001", "40P01"})
MAX_ATTEMPTS = 3

# Fault-injection seam for rollback tests; None in normal operation.
_after_batch_write_hook: Optional[Callable[[Connection], None]] = None


@dataclass
class CollectionOutcome:
    """
    What a collector or file reader hands the importer. `succeeded` must be
    decided by the caller from observed results, never inferred from an empty
    record list. Failures carry a code and diagnostics; the stored summary is
    rendered from those, so there is no free-text field to leak through.
    """

    source: str
    mode: str
    succeeded: bool
    records: Optional[List[Any]] = None
    error_code: Optional[str] = None
    diagnostics: Optional[Dict[str, Any]] = None
    source_as_of: Optional[date] = None
    envelope: Optional[Dict[str, Any]] = (
        None  # fetcher top-level metadata; sanitized before storage
    )
    coverage: Optional[Dict[str, Any]] = None

    @classmethod
    def success(
        cls, source: str, mode: str, records: List[Any], **kw: Any
    ) -> "CollectionOutcome":
        return cls(source=source, mode=mode, succeeded=True, records=records, **kw)

    @classmethod
    def failure(
        cls, source: str, mode: str, error_code: str, **kw: Any
    ) -> "CollectionOutcome":
        return cls(
            source=source, mode=mode, succeeded=False, error_code=error_code, **kw
        )


@dataclass
class ImportResult:
    source: str
    status: str
    run_id: int
    batch_id: Optional[int] = None
    reused_batch: Optional[bool] = None
    content_hash: Optional[str] = None
    input_count: Optional[int] = None
    accepted_count: Optional[int] = None
    error_code: Optional[str] = None
    error_summary: Optional[str] = None

    @property
    def succeeded(self) -> bool:
        return self.status == "succeeded"


class ImportFailure(Exception):
    """An expected import failure: a known code plus closed-shape diagnostics."""

    def __init__(self, code: str, diagnostics: Optional[Dict[str, Any]] = None):
        super().__init__(code)
        self.code = code
        self.diagnostics = diagnostics


@dataclass
class _Errors:
    errors: List[Dict[str, Any]] = field(default_factory=list)
    count: int = 0

    def add(self, record: Optional[int], field_path: str, kind: str) -> None:
        self.count += 1
        if len(self.errors) < MAX_ENTRIES:
            self.errors.append({"record": record, "field": field_path, "type": kind})


def _now() -> datetime:
    return datetime.now(timezone.utc)


def validate_records(source: str, raw_records: Any) -> List[SourceRecord]:
    """Validate every record; raise ImportFailure describing (without values) what failed."""
    if not isinstance(raw_records, list):
        raise ImportFailure("malformed_input")
    if len(raw_records) > MAX_RECORDS:
        raise ImportFailure("input_too_large", {"input_count": len(raw_records)})
    model = RECORD_MODELS[source]
    found = _Errors()
    parsed: List[SourceRecord] = []
    exchanges: Dict[str, str] = {}
    seen: set[str] = set()
    for index, raw in enumerate(raw_records):
        if not isinstance(raw, dict):
            found.add(index, "", "not_an_object")
            continue
        try:
            record = model.model_validate(raw)
        except ValidationError as exc:
            for err in exc.errors(
                include_url=False, include_input=False, include_context=False
            ):
                loc = ".".join(str(part) for part in err.get("loc", ()))
                found.add(index, loc, err.get("type", "invalid"))
            continue
        if source in ONE_ROW_PER_SECURITY:
            if record.ticker in seen:
                found.add(index, "ticker", "duplicate_security")
                continue
            seen.add(record.ticker)
        if record.exchange:
            prior = exchanges.setdefault(record.ticker, record.exchange)
            if prior != record.exchange:
                found.add(index, "exchange", "conflicting_exchange")
                continue
        parsed.append(record)
    if found.count:
        raise ImportFailure(
            "validation_failed",
            {
                "input_count": len(raw_records),
                "error_count": found.count,
                "errors": found.errors,
            },
        )
    return parsed


def _conflict(symbol: str, column: str) -> ImportFailure:
    return ImportFailure(
        "security_conflict",
        {"failures": [{"unit": symbol, "code": f"{column}_conflict"}]},
    )


def _resolve_securities(
    conn: Connection, records: Sequence[SourceRecord]
) -> Dict[str, int]:
    wanted: Dict[str, Dict[str, Optional[str]]] = {}
    for r in records:
        entry = wanted.setdefault(r.ticker, {"exchange": None, "currency": None})
        entry["exchange"] = entry["exchange"] or r.exchange
        if r.currency:
            if entry["currency"] and entry["currency"] != r.currency:
                raise _conflict(r.ticker, "currency")
            entry["currency"] = r.currency
    if not wanted:
        return {}
    symbols = sorted(wanted)  # consistent lock order across concurrent imports
    conn.execute(
        pg_insert(securities)
        .values([{"symbol": s, **wanted[s]} for s in symbols])
        .on_conflict_do_nothing(index_elements=["symbol"])
    )
    rows = conn.execute(
        select(
            securities.c.id,
            securities.c.symbol,
            securities.c.exchange,
            securities.c.currency,
        )
        .where(securities.c.symbol.in_(symbols))
        .order_by(securities.c.symbol)
        .with_for_update()
    ).all()
    ids: Dict[str, int] = {}
    for row in rows:
        want = wanted[row.symbol]
        changes = {}
        for column in ("exchange", "currency"):
            new, existing = want[column], getattr(row, column)
            if new and existing and new != existing:
                raise _conflict(row.symbol, column)
            if new and not existing:
                changes[column] = new
        if changes:
            conn.execute(
                update(securities).where(securities.c.id == row.id).values(**changes)
            )
        ids[row.symbol] = row.id
    if set(symbols) - set(ids):
        raise RuntimeError("securities could not be resolved")
    return ids


def _write_batch(
    conn: Connection, batch: CanonicalBatch, source_as_of: Optional[date]
) -> tuple[int, bool]:
    batch_id = conn.execute(
        pg_insert(source_batches)
        .values(
            source=batch.source,
            content_hash=batch.content_hash,
            hash_version=HASH_VERSION,
            source_as_of=source_as_of,
            record_count=len(batch.records),
            payload=batch.payload,
        )
        .on_conflict_do_nothing(index_elements=["source", "content_hash"])
        .returning(source_batches.c.id)
    ).scalar_one_or_none()
    if batch_id is None:
        existing = conn.execute(
            select(source_batches.c.id).where(
                source_batches.c.source == batch.source,
                source_batches.c.content_hash == batch.content_hash,
            )
        ).scalar_one()
        return existing, True

    security_ids = _resolve_securities(conn, batch.records)
    table = SOURCE_TABLES[batch.source]
    has_row_number = "row_number" in table.c
    rows = []
    for position, record in enumerate(batch.records, start=1):
        values = record.db_values()
        values["batch_id"] = batch_id
        values["security_id"] = security_ids[record.ticker]
        if has_row_number:
            values["row_number"] = position
        rows.append(values)
    if rows:
        conn.execute(insert(table), rows)
    return batch_id, False


def _inactive_result(outcome: CollectionOutcome, run_id: int) -> ImportResult:
    """Leave a finalized run untouched when its collection loses the lease."""
    return ImportResult(
        source=outcome.source,
        status="failed",
        run_id=run_id,
        accepted_count=0,
        error_code="worker_abandoned",
        error_summary="Worker heartbeat expired.",
    )


def _record_failed_run(
    engine: Engine,
    outcome: CollectionOutcome,
    started_at: datetime,
    code: Any,
    diagnostics: Any,
    input_count: Optional[int],
    run_id: int,
) -> ImportResult:
    code = safe_code(code)
    diag = safe_diagnostics(diagnostics)
    summary = render_summary(code, diag)[:ERROR_SUMMARY_MAX]
    with engine.begin() as conn:
        try:
            require_active(conn, run_id, outcome.source, outcome.mode)
        except RunInactive:
            return _inactive_result(outcome, run_id)
        conn.execute(
            update(ingestion_runs)
            .where(ingestion_runs.c.id == run_id)
            .values(
                source=outcome.source,
                status="failed",
                collection_mode=outcome.mode,
                started_at=started_at,
                finished_at=max(_now(), started_at),
                input_count=input_count,
                accepted_count=0,
                batch_id=None,
                reused_batch=None,
                hash_version=HASH_VERSION,
                error_code=code,
                error_summary=summary,
                diagnostics=diag,
            )
        )
    # Separate transaction: a pruning problem must never lose the failed-run record.
    try:
        with engine.begin() as conn:
            prune_failed_diagnostics(conn, outcome.source)
    except SQLAlchemyError:
        pass
    return ImportResult(
        source=outcome.source,
        status="failed",
        run_id=run_id,
        input_count=input_count,
        accepted_count=0,
        error_code=code,
        error_summary=summary,
    )


def prune_failed_diagnostics(
    conn: Connection,
    source: str,
    keep: int = DIAGNOSTICS_KEEP_PER_SOURCE,
    max_age_days: int = DIAGNOSTICS_MAX_AGE_DAYS,
) -> int:
    """Clear diagnostics on older failed runs according to the retention policy."""
    result = conn.execute(
        text(
            """
            UPDATE perennial.ingestion_runs SET diagnostics = NULL
            WHERE source = :source AND status = 'failed' AND diagnostics IS NOT NULL
              AND (finished_at < :cutoff OR id NOT IN (
                    SELECT id FROM perennial.ingestion_runs
                    WHERE source = :source AND status = 'failed' AND diagnostics IS NOT NULL
                    ORDER BY finished_at DESC, id DESC LIMIT :keep))
            """
        ),
        {
            "source": source,
            "cutoff": _now() - timedelta(days=max_age_days),
            "keep": keep,
        },
    )
    return result.rowcount


def import_collection(
    engine: Engine, outcome: CollectionOutcome, *, run_id: Optional[int] = None
) -> ImportResult:
    """Import one collection outcome. Returns the recorded run; never raises for bad input."""
    if outcome.source not in SOURCES:
        raise ValueError("unknown source")
    if outcome.mode not in COLLECTION_MODES:
        raise ValueError("unknown collection mode")
    if run_id is None:
        run_id = begin_run(engine, outcome.source, outcome.mode)
    with engine.begin() as conn:
        try:
            active = require_active(conn, run_id, outcome.source, outcome.mode)
        except RunInactive:
            return _inactive_result(outcome, run_id)
        started_at = active["started_at"]
    collected_at = _now()

    if not outcome.succeeded:
        return _record_failed_run(
            engine,
            outcome,
            started_at,
            outcome.error_code,
            outcome.diagnostics,
            None,
            run_id,
        )

    raw = outcome.records
    input_count = len(raw) if isinstance(raw, list) else None
    try:
        records = validate_records(outcome.source, raw)
        batch = canonicalize(
            outcome.source, records, outcome.source_as_of, outcome.envelope
        )
    except ImportFailure as failure:
        return _record_failed_run(
            engine,
            outcome,
            started_at,
            failure.code,
            failure.diagnostics,
            input_count,
            run_id,
        )
    except Exception as exc:  # unexpected input shape must still leave a failed run
        return _record_failed_run(
            engine,
            outcome,
            started_at,
            "validation_error",
            exception_diagnostics(exc),
            input_count,
            run_id,
        )

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with engine.begin() as conn:
                require_active(conn, run_id, outcome.source, outcome.mode)
                batch_id, reused = _write_batch(conn, batch, outcome.source_as_of)
                if _after_batch_write_hook is not None:
                    _after_batch_write_hook(conn)
                conn.execute(
                    update(ingestion_runs)
                    .where(ingestion_runs.c.id == run_id)
                    .values(
                        source=outcome.source,
                        status="succeeded",
                        collection_mode=outcome.mode,
                        started_at=started_at,
                        finished_at=max(_now(), started_at),
                        input_count=input_count,
                        accepted_count=len(batch.records),
                        batch_id=batch_id,
                        reused_batch=reused,
                        hash_version=HASH_VERSION,
                        diagnostics=safe_diagnostics(outcome.diagnostics),
                        collected_at=collected_at,
                        heartbeat_at=_now(),
                        coverage=safe_coverage(outcome.coverage),
                    )
                )
            return ImportResult(
                source=outcome.source,
                status="succeeded",
                run_id=run_id,
                batch_id=batch_id,
                reused_batch=reused,
                content_hash=batch.content_hash,
                input_count=input_count,
                accepted_count=len(batch.records),
            )
        except RunInactive:
            return _inactive_result(outcome, run_id)
        except RunIdentityError:
            raise
        except ImportFailure as failure:
            code, diag = failure.code, failure.diagnostics
        except DBAPIError as exc:
            sqlstate = getattr(exc.orig, "sqlstate", None)
            if sqlstate in RETRYABLE_SQLSTATES and attempt < MAX_ATTEMPTS:
                time.sleep(0.05 * attempt)
                continue
            code, diag = "database_error", exception_diagnostics(exc)
        except SQLAlchemyError as exc:
            code, diag = "database_error", exception_diagnostics(exc)
        except (
            Exception
        ) as exc:  # anything else still rolls back and leaves a failed run
            code, diag = "internal_error", exception_diagnostics(exc)
        break
    return _record_failed_run(
        engine, outcome, started_at, code, diag, input_count, run_id
    )
