"""Durable collection lifecycle; no transaction is held during provider I/O."""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Event, Thread
from time import monotonic

from sqlalchemy import insert, select, update, text

from src.db.models import SOURCES, COLLECTION_MODES, ingestion_runs, run_dependencies, source_batches


def now():
    return datetime.now(timezone.utc)


def begin_run(engine, source, mode="live", *, dependencies=None):
    if source not in SOURCES or mode not in COLLECTION_MODES:
        raise ValueError("invalid source or collection mode")
    with engine.begin() as conn:
        run_id = conn.execute(insert(ingestion_runs).values(source=source, collection_mode=mode,
            status="running", started_at=now(), heartbeat_at=now()).returning(ingestion_runs.c.id)).scalar_one()
        for upstream, batch_id in (dependencies or {}).items():
            actual = conn.execute(select(source_batches.c.source).where(source_batches.c.id == batch_id)).scalar_one_or_none()
            if actual != upstream:
                raise ValueError("dependency batch does not match source")
            conn.execute(insert(run_dependencies).values(run_id=run_id, source=upstream, batch_id=batch_id))
    return run_id


class RunIdentityError(RuntimeError):
    """The caller supplied a missing run or the wrong source/mode."""


class RunInactive(RuntimeError):
    """The matching run has already reached a final durable state."""


def require_active(conn, run_id, source, mode):
    run = conn.execute(select(ingestion_runs).where(ingestion_runs.c.id == run_id).with_for_update()).mappings().one_or_none()
    if not run or run["source"] != source or run["collection_mode"] != mode:
        raise RunIdentityError("run identity does not match this source and mode")
    if run["status"] != "running":
        raise RunInactive("run is no longer active")
    return run


def heartbeat(engine, run_id):
    with engine.begin() as conn:
        return conn.execute(update(ingestion_runs).where(ingestion_runs.c.id == run_id,
            ingestion_runs.c.status == "running").values(heartbeat_at=now())).rowcount == 1


def abandon_stale_runs(engine, max_idle=timedelta(minutes=10)):
    if max_idle.total_seconds() <= 0:
        raise ValueError("max_idle must be positive")
    with engine.begin() as conn:
        return conn.execute(update(ingestion_runs).where(ingestion_runs.c.status == "running",
            ingestion_runs.c.heartbeat_at < now()-max_idle).values(status="abandoned", finished_at=now(),
            error_code="worker_abandoned", error_summary="Worker heartbeat expired.", accepted_count=0)).rowcount


@dataclass
class HeartbeatLease:
    lost: bool = False


@contextmanager
def heartbeat_worker(engine, run_id, interval=15, *, max_backoff=60,
                     wait=None, clock=monotonic):
    if interval <= 0 or max_backoff < interval:
        raise ValueError("heartbeat intervals must be positive and backoff at least the interval")
    stop = Event()
    lease = HeartbeatLease()
    wait = stop.wait if wait is None else lambda delay, injected=wait: injected(stop, delay)
    def beat():
        retry_delay = interval
        due = clock() + interval
        while not wait(max(0, due - clock())):
            try:
                if not heartbeat(engine, run_id):
                    lease.lost = True
                    return
            except Exception:
                # Connectivity failure does not prove the guarded lease is gone.
                due = clock() + retry_delay
                retry_delay = min(max_backoff, retry_delay * 2)
            else:
                due = clock() + interval
                retry_delay = interval
    worker = Thread(target=beat, daemon=True)
    worker.start()
    try:
        yield lease
    finally:
        stop.set()
        worker.join(timeout=2)


@contextmanager
def source_lock(engine, source):
    """Session advisory lock: no open transaction while network calls run."""
    key = 170098 if source == "pipeline" else 170010 + SOURCES.index(source)
    with engine.connect() as conn:
        acquired = conn.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": key}).scalar_one()
        conn.commit()
        if not acquired:
            raise RuntimeError("source collection already running")
        try:
            yield
        finally:
            try:
                conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
                conn.commit()
            except Exception:
                conn.invalidate()
