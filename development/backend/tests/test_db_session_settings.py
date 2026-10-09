"""Synthetic PostgreSQL checks for each workload's connection policy."""

import pytest
from sqlalchemy import text

from src.db.session import make_engine
from src.pipeline.lifecycle import begin_run, heartbeat_worker, source_lock


@pytest.mark.db
@pytest.mark.parametrize(
    "workload,statement_ms,pool_size",
    [
        ("pipeline", "60000", 2),
        ("api", "5000", 5),
        ("migrate", "0", 1),
        ("cli", "60000", 2),
    ],
)
def test_workload_settings_are_active_in_postgres(
    engine, workload, statement_ms, pool_size
):
    scoped = make_engine(
        engine.url, workload=workload, connect_args={"hostaddr": "127.0.0.1"}
    )
    try:
        assert scoped.pool.size() == pool_size
        with scoped.connect() as conn:
            settings = dict(
                conn.execute(
                    text(
                        "SELECT name, setting FROM pg_settings WHERE name IN "
                        "('statement_timeout', 'lock_timeout', "
                        "'idle_in_transaction_session_timeout', 'application_name')"
                    )
                )
                .tuples()
                .all()
            )
            assert settings == {
                "statement_timeout": statement_ms,
                "lock_timeout": "5000",
                "idle_in_transaction_session_timeout": "60000",
                "application_name": f"perennial-{workload}",
            }
            assert (
                conn.scalar(
                    text(
                        "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()"
                    )
                )
                == f"perennial-{workload}"
            )
    finally:
        scoped.dispose()


@pytest.mark.db
def test_pipeline_lock_and_heartbeat_use_separate_pool_connections(engine):
    first = make_engine(
        engine.url, workload="pipeline", connect_args={"hostaddr": "127.0.0.1"}
    )
    second = make_engine(
        engine.url, workload="pipeline", connect_args={"hostaddr": "127.0.0.1"}
    )
    try:
        with source_lock(first, "pipeline"):
            with pytest.raises(RuntimeError, match="already running"):
                with source_lock(second, "pipeline"):
                    pass
            with source_lock(first, "congress_trades"):
                run_id = begin_run(first, "congress_trades")
                with first.connect() as conn:
                    before = conn.scalar(
                        text(
                            "SELECT heartbeat_at FROM perennial.ingestion_runs WHERE id=:id"
                        ),
                        {"id": run_id},
                    )
                calls = 0

                def immediate_beat(_stop, _delay):
                    nonlocal calls
                    calls += 1
                    return calls > 1

                with heartbeat_worker(
                    first, run_id, interval=0.01, wait=immediate_beat
                ) as lease:
                    pass
                with first.connect() as conn:
                    after = conn.scalar(
                        text(
                            "SELECT heartbeat_at FROM perennial.ingestion_runs WHERE id=:id"
                        ),
                        {"id": run_id},
                    )
                assert not lease.lost and after > before
    finally:
        first.dispose()
        second.dispose()
