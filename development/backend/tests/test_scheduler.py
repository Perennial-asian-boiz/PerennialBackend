"""Legacy schedule stays callable while production uses explicit database mode."""
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest


def test_legacy_schedule_preserves_daily_and_settlement_jobs(monkeypatch):
    from src.services.consensus_watchlist.scheduler import cron
    jobs = {}
    class Scheduler:
        def __init__(self, **kwargs):
            pass
        def add_job(self, function, trigger, **kwargs):
            jobs[kwargs['id']] = (function, trigger)
        def get_jobs(self):
            return []
        def start(self):
            pass
    monkeypatch.setattr(cron, 'BlockingScheduler', Scheduler)
    monkeypatch.setattr(cron, 'run_full_pipeline', lambda: None)
    cron.main()
    assert set(jobs) == {'daily_pipeline', 'short_interest'}
    assert jobs['short_interest'][0] is cron.run_short_interest
    trigger = jobs['short_interest'][1]
    next_time = trigger.get_next_fire_time(None, datetime(2026, 10, 2, tzinfo=ZoneInfo('America/Los_Angeles')))
    assert (next_time.day, next_time.hour, next_time.minute) == (15, 9, 10)


def test_database_schedule_runs_all_stages_at_startup_and_daily(monkeypatch):
    from src.pipeline import scheduler
    jobs, called = {}, []
    class Scheduler:
        def __init__(self, **kwargs):
            assert kwargs['timezone'] == 'America/Los_Angeles'
        def add_job(self, function, trigger, **kwargs):
            jobs[kwargs['id']] = (function, trigger, kwargs)
        def start(self):
            called.append('started')
    monkeypatch.setattr(scheduler, 'BlockingScheduler', Scheduler)
    monkeypatch.setattr(scheduler, 'run_full_pipeline', lambda: called.append('run'))
    scheduler.main()
    assert called == ['run', 'started']
    assert set(jobs) == {'daily_pipeline'}
    _, trigger, options = jobs['daily_pipeline']
    assert str(trigger.timezone) == 'America/Los_Angeles'
    assert options['max_instances'] == 1 and options['coalesce'] is True


def test_database_scheduler_propagates_failure_and_disposes(monkeypatch):
    import pytest
    from src.pipeline import scheduler
    class Engine:
        disposed = False
        def dispose(self):
            self.disposed = True
    engine = Engine()
    monkeypatch.setattr(scheduler, 'make_engine', lambda: engine)
    def broken(_):
        raise RuntimeError('synthetic failure')
    monkeypatch.setattr(scheduler, 'run_pipeline', broken)
    with pytest.raises(scheduler.PipelineRunFailed, match='^database pipeline run failed$'):
        scheduler.run_full_pipeline()
    assert engine.disposed


@pytest.mark.parametrize('failure_stage', ['pipeline', 'engine', 'pipeline_and_dispose'])
def test_real_scheduler_executor_keeps_failure_logs_and_events_safe(monkeypatch, caplog, failure_stage):
    """Exercise APScheduler's own traceback logging and listener exception object."""
    import logging
    from types import SimpleNamespace
    from apscheduler.executors.base import run_job
    from sqlalchemy.exc import OperationalError
    from src.pipeline import scheduler

    sentinel = 'SYNTHETIC_SCHEDULER_SECRET'
    url = 'https://provider.invalid/?apikey=' + sentinel
    class Engine:
        disposed = False
        def dispose(self):
            self.disposed = True
            if failure_stage == 'pipeline_and_dispose':
                raise RuntimeError(url)
    engine = Engine()
    def broken(*args):
        try:
            raise ValueError(url)
        except ValueError as cause:
            raise OperationalError('SELECT 1', {}, Exception(url)) from cause
    monkeypatch.setattr(scheduler, 'make_engine', broken if failure_stage == 'engine' else lambda: engine)
    monkeypatch.setattr(scheduler, 'run_pipeline', broken)
    job = SimpleNamespace(misfire_grace_time=None, func=scheduler.run_full_pipeline,
                          args=(), kwargs={}, id='synthetic_safe_failure')
    with caplog.at_level(logging.INFO):
        events = run_job(job, 'default', [datetime.now(ZoneInfo('UTC'))], 'apscheduler.executors.default')
    event, = events
    assert sentinel not in caplog.text and url not in caplog.text
    assert sentinel not in str(event.exception) and url not in str(event.exception)
    assert sentinel not in event.traceback and url not in event.traceback
    assert type(event.exception).__name__ == 'PipelineRunFailed'
    assert event.exception.__context__ is None
    assert event.exception.__cause__ is None
    assert str(event.exception) == 'database pipeline run failed'
    assert engine.disposed is (failure_stage != 'engine')


def test_scheduler_sanitizes_disposal_errors(monkeypatch, caplog):
    from src.pipeline import scheduler
    class Engine:
        def dispose(self):
            raise RuntimeError('SYNTHETIC_DISPOSAL_SECRET')
    monkeypatch.setattr(scheduler, 'make_engine', Engine)
    monkeypatch.setattr(scheduler, 'run_pipeline', lambda engine: 42)
    assert scheduler.run_full_pipeline() == 42
    assert 'SYNTHETIC_DISPOSAL_SECRET' not in caplog.text
    assert 'engine dispose failed: RuntimeError' in caplog.text
