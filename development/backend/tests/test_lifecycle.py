"""Heartbeat recovery and immutable outcomes after a durable lease is lost."""
from datetime import timedelta
from threading import Event

import pytest
from sqlalchemy import select, update

from src.db import models
from src.ingestion import importer
from src.pipeline import lifecycle


def test_heartbeat_default_clock_recovers_without_injection(monkeypatch):
    calls = []
    recovered = Event()
    def beat(*args):
        calls.append(True)
        if len(calls) <= 2:
            raise RuntimeError('synthetic transient database failure')
        recovered.set()
        return True
    monkeypatch.setattr(lifecycle, 'heartbeat', beat)
    with lifecycle.heartbeat_worker(None, 7, interval=0.001) as lease:
        assert recovered.wait(1), 'heartbeat permanently exited on its first error'
    assert len(calls) >= 3 and lease.lost is False


def test_heartbeat_recovers_after_transient_errors_and_resets_backoff(monkeypatch):
    calls, waits = [], []
    finished = Event()
    clock = [0.0]
    def wait(stop, delay):
        waits.append(delay)
        clock[0] += delay
        if len(waits) == 5:
            finished.set()
            return True
        return stop.is_set()
    def beat(engine, run):
        calls.append(run)
        if len(calls) <= 2:
            raise RuntimeError('synthetic transient connection failure')
        return True
    monkeypatch.setattr(lifecycle, 'heartbeat', beat)
    with lifecycle.heartbeat_worker(None, 7, interval=15, max_backoff=60,
                                    wait=wait, clock=lambda: clock[0]) as lease:
        assert finished.wait(1), 'worker exited before recovering'
        assert lease.lost is False
    assert calls == [7, 7, 7, 7]
    assert waits == [15, 15, 30, 15, 15]


def test_heartbeat_backoff_is_capped_and_shutdown_interrupts_wait(monkeypatch):
    waits = []
    reached_cap = Event()
    def wait(stop, delay):
        waits.append(delay)
        if len(waits) >= 5:
            reached_cap.set()
            return stop.wait(1)
        return stop.is_set()
    def beat(*args):
        raise RuntimeError('synthetic persistent connection failure')
    monkeypatch.setattr(lifecycle, 'heartbeat', beat)
    with lifecycle.heartbeat_worker(None, 7, interval=15, max_backoff=60, wait=wait) as lease:
        assert reached_cap.wait(1)
        assert lease.lost is False
    assert waits == pytest.approx([15, 15, 30, 60, 60], abs=0.01)


def test_heartbeat_stops_only_when_guarded_update_loses_lease(monkeypatch):
    calls = []
    lost = Event()
    def beat(*args):
        calls.append(True)
        lost.set()
        return False
    monkeypatch.setattr(lifecycle, 'heartbeat', beat)
    with lifecycle.heartbeat_worker(None, 7, interval=0.001) as lease:
        assert lost.wait(1)
    assert lease.lost is True
    assert calls == [True]


@pytest.mark.db
@pytest.mark.parametrize('phase', ['before_import', 'before_write', 'before_failure_record'])
def test_abandoned_import_returns_failure_without_mutating_final_run(engine, monkeypatch, phase):
    run_id = lifecycle.begin_run(engine, 'ark_holdings', 'live')
    frozen = []
    def abandon():
        with engine.begin() as conn:
            conn.execute(update(models.ingestion_runs).where(models.ingestion_runs.c.id == run_id)
                         .values(heartbeat_at=lifecycle.now() - timedelta(hours=1)))
        assert lifecycle.abandon_stale_runs(engine) == 1
        with engine.connect() as conn:
            frozen.append(dict(conn.execute(select(models.ingestion_runs)).mappings().one()))
    if phase == 'before_import':
        abandon()
    elif phase == 'before_write':
        original = importer.canonicalize
        def canonicalize(*args):
            batch = original(*args)
            abandon()
            return batch
        monkeypatch.setattr(importer, 'canonicalize', canonicalize)
    else:
        def invalid(*args):
            abandon()
            raise ValueError('synthetic malformed input')
        monkeypatch.setattr(importer, 'validate_records', invalid)
    result = importer.import_collection(engine, importer.CollectionOutcome.success(
        'ark_holdings', 'live', []), run_id=run_id)
    assert (result.status, result.error_code, result.run_id) == ('failed', 'worker_abandoned', run_id)
    assert result.batch_id is None and result.accepted_count == 0
    with engine.connect() as conn:
        assert dict(conn.execute(select(models.ingestion_runs)).mappings().one()) == frozen[0]
        assert conn.execute(select(models.source_batches.c.id)).first() is None


@pytest.mark.db
@pytest.mark.parametrize('mismatch', ['missing', 'source', 'mode'])
def test_import_run_identity_errors_still_raise(engine, mismatch):
    run_id = lifecycle.begin_run(engine, 'ark_holdings', 'live')
    source = 'congress_trades' if mismatch == 'source' else 'ark_holdings'
    mode = 'fixture' if mismatch == 'mode' else 'live'
    with pytest.raises(RuntimeError, match='active|identity'):
        importer.import_collection(engine, importer.CollectionOutcome.success(source, mode, []),
                                   run_id=run_id + 100 if mismatch == 'missing' else run_id)
