"""M3 operations code: audited rollback, the one-shot scheduled entry, missed runs.

Synthetic fixtures only; no live provider, network or Supabase.
"""

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import select, text, update

from src.db import models
from src.pipeline import publication
from src.pipeline.publication import (
    OverrideInputError,
    PublicationError,
    PublicationRegressionError,
    RollbackError,
    current,
    publish,
    replay,
    rollback,
)
from test_degraded_publishing import THRESHOLDS, Feed, import_all, market_caps, run

ACTOR, REASON = "synthetic-op", "synthetic rollback after a bad provider day"


def _events(engine, publication_id, kind):
    with engine.connect() as conn:
        return [
            dict(r)
            for r in conn.execute(
                select(models.publication_events).where(
                    models.publication_events.c.publication_id == publication_id,
                    models.publication_events.c.kind == kind,
                )
            ).mappings()
        ]


def _two_publications(engine, fixture_path):
    feed = Feed(fixture_path)
    first = run(engine, feed)
    feed.change("congress_trades")
    second = run(engine, feed)
    assert current(engine)["id"] == second != first
    return first, second


# ── rollback ──


@pytest.mark.db
def test_rollback_moves_the_pointer_and_records_who_and_why(engine, fixture_path):
    first, second = _two_publications(engine, fixture_path)
    assert rollback(engine, first, actor=ACTOR, reason=REASON) == second
    assert current(engine)["id"] == first
    [event] = _events(engine, first, "rollback")
    assert (event["actor"], event["reason"]) == (ACTOR, REASON)
    assert event["checks"] == {"from_publication_id": second, "to_publication_id": first}
    assert replay(engine, first) == current(engine)["output"]
    # Rolling forward again is also an explicit, audited rollback.
    assert rollback(engine, second, actor=ACTOR, reason=REASON) == first
    assert current(engine)["id"] == second


@pytest.mark.db
@pytest.mark.parametrize("target", ["current", "missing"])
def test_rollback_rejects_a_missing_or_current_target(engine, fixture_path, target):
    first, second = _two_publications(engine, fixture_path)
    to = second if target == "current" else second + 1000
    with pytest.raises(RollbackError) as caught:
        rollback(engine, to, actor=ACTOR, reason=REASON)
    assert caught.value.code == "rollback_rejected"
    assert current(engine)["id"] == second
    assert _events(engine, to, "rollback") == []


@pytest.mark.db
def test_rollback_refuses_a_target_that_does_not_replay(engine, fixture_path):
    first, second = _two_publications(engine, fixture_path)
    with engine.begin() as conn:
        conn.execute(
            update(models.publications)
            .where(models.publications.c.id == first)
            .values(output={"tampered": True})
        )
    with pytest.raises(RollbackError, match="replay"):
        rollback(engine, first, actor=ACTOR, reason=REASON)
    assert current(engine)["id"] == second


@pytest.mark.db
@pytest.mark.parametrize(
    "actor,reason",
    [
        ("operator password=SYNTHETIC_SECRET", REASON),
        (ACTOR, "see https://example.invalid/?apikey=SYNTHETIC_SECRET"),
        (ACTOR, ""),
    ],
)
def test_rollback_audit_text_follows_the_override_policy(engine, fixture_path, actor, reason):
    first, second = _two_publications(engine, fixture_path)
    with pytest.raises(OverrideInputError):
        rollback(engine, first, actor=actor, reason=reason)
    assert current(engine)["id"] == second
    assert _events(engine, first, "rollback") == []


@pytest.mark.db
def test_rollback_fails_fast_while_a_publisher_holds_the_lock(engine, fixture_path):
    first, second = _two_publications(engine, fixture_path)
    with engine.begin() as conn:
        conn.execute(
            text("SELECT pg_advisory_xact_lock(:key)"),
            {"key": publication.PUBLICATION_LOCK},
        )
        with pytest.raises(PublicationError) as caught:
            rollback(engine, first, actor=ACTOR, reason=REASON)
    assert caught.value.code == "publication_in_progress"
    assert current(engine)["id"] == second


def _ark_baseline(engine, fixture_path, as_of):
    """An older unpublished ARK batch: the quality baseline for the next candidate,
    so a refusal below comes from the observation-date guard, not quality."""
    from src.ingestion.importer import import_collection
    from src.pipeline.lifecycle import begin_run

    feed = Feed(fixture_path)
    for _ in range(3):
        feed.change("ark_holdings")
    feed.as_of_override["ark_holdings"] = as_of
    run_id = begin_run(engine, "ark_holdings", "live")
    assert import_collection(engine, feed.collectors()["ark_holdings"](), run_id=run_id).succeeded


@pytest.mark.db
def test_only_rollback_can_serve_older_observations(engine, fixture_path):
    """publish() keeps the observation-date guard against whatever is current."""
    today = datetime.now(timezone.utc).date()
    older = Feed(fixture_path)
    older.change("ark_holdings")
    older.as_of_override["ark_holdings"] = today - timedelta(days=1)
    first = run(engine, older)
    second = run(engine, Feed(fixture_path))  # ARK as of today

    stale = Feed(fixture_path)
    stale.change("ark_holdings")
    stale.change("ark_holdings")
    stale.as_of_override["ark_holdings"] = today - timedelta(days=2)

    def publish_stale():
        _ark_baseline(engine, fixture_path, today - timedelta(days=3))
        runs = import_all(engine, stale)
        publish(engine, runs, market_caps(_symbols(engine, runs)), quality_thresholds=THRESHOLDS)

    with pytest.raises(PublicationRegressionError):
        publish_stale()  # today -> two days ago: refused while `second` is current

    rollback(engine, first, actor=ACTOR, reason=REASON)  # explicit: today -> yesterday
    assert current(engine)["ranking_config"]["source_as_of"]["ark_holdings"] == (
        today - timedelta(days=1)
    ).isoformat()
    # The guard now compares against the rolled-back publication: still no
    # silent move to an even older date.
    with pytest.raises(PublicationRegressionError):
        publish_stale()
    assert current(engine)["id"] == first != second


def _symbols(engine, runs):
    from src.db.queries import batch_rows
    from src.pipeline.ranking import candidate_symbols

    with engine.connect() as conn:
        batches = dict(
            conn.execute(
                select(models.ingestion_runs.c.source, models.ingestion_runs.c.batch_id).where(
                    models.ingestion_runs.c.id.in_(list(runs.values()))
                )
            ).all()
        )
        return candidate_symbols({s: batch_rows(conn, s, b) for s, b in batches.items()})


@pytest.mark.db
def test_cli_rollback_contract(engine, db_url, fixture_path, monkeypatch, capsys):
    from src.ingestion import cli

    first, second = _two_publications(engine, fixture_path)
    monkeypatch.setenv("DATABASE_URL", db_url.render_as_string(hide_password=False))

    argv = ["rollback", "--to", str(first), "--reason", REASON, "--actor", ACTOR]
    assert cli.main(argv) == 0
    assert json.loads(capsys.readouterr().out) == {
        "publication_id": first,
        "rolled_back_from": second,
    }
    assert cli.main(argv) == 1  # already current
    assert json.loads(capsys.readouterr().out) == {"error_code": "rollback_rejected"}
    bad = ["rollback", "--to", str(second), "--reason", "token: SYNTHETIC_SECRET"]
    assert cli.main(bad) == 2
    out = capsys.readouterr()
    assert "override_input_rejected" in out.err and "SYNTHETIC_SECRET" not in out.err
    with pytest.raises(SystemExit) as caught:
        cli.main(["rollback", "--to", str(second)])
    assert caught.value.code == 2
    assert current(engine)["id"] == first


# ── one-shot scheduled entry ──


def _settings(url="postgresql+psycopg://u@127.0.0.1/x_test"):
    from src.settings import Settings

    return Settings(database_url=url, fmp_api_key="synthetic-fmp", finnhub_api_key="synthetic-fh")


class _Engine:
    def dispose(self):
        pass


def test_run_once_exit_codes(monkeypatch):
    from src.pipeline import scheduler

    monkeypatch.setattr(scheduler, "make_engine", lambda _url, **kwargs: _Engine())
    monkeypatch.setattr(scheduler, "run_pipeline", lambda engine, **kwargs: 42)
    assert scheduler.run_once("pipeline", _settings()) == 0

    def broken(engine, **kwargs):
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr(scheduler, "run_pipeline", broken)
    assert scheduler.run_once("pipeline", _settings()) == 1
    with pytest.raises(ValueError):
        scheduler.run_once("bogus", _settings())


def test_run_once_gdelt_fails_on_any_failed_theme(monkeypatch, caplog):
    """M3-N3: a partial theme failure is not success; only counts and a code are logged."""
    from src.pipeline import scheduler
    from src.services.gdelt import job

    monkeypatch.setattr(scheduler, "make_engine", lambda *a, **kwargs: _Engine())

    def summary(succeeded, failed):
        result = job.RunSummary(day="2026-10-22")
        result.succeeded.extend(succeeded)
        result.failed.update(failed)
        return result

    monkeypatch.setattr(job, "run_gdelt", lambda engine: summary(["A", "B"], {}))
    assert scheduler.run_once("gdelt", _settings()) == 0

    secret = "https://example.invalid/?apikey=SYNTHETIC_SECRET"
    monkeypatch.setattr(job, "run_gdelt", lambda engine: summary(["A"], {"B": secret}))
    with caplog.at_level("ERROR"):
        assert scheduler.run_once("gdelt", _settings()) == 1
    assert "code=gdelt_incomplete ok=1 failed=1" in caplog.text
    assert "SYNTHETIC_SECRET" not in caplog.text

    def broken(engine):
        raise ConnectionError(secret)

    monkeypatch.setattr(job, "run_gdelt", broken)
    with caplog.at_level("ERROR"):
        assert scheduler.run_once("gdelt", _settings()) == 1
    assert "SYNTHETIC_SECRET" not in caplog.text


@pytest.mark.db
def test_overlapping_scheduled_run_exits_1_not_success(engine, db_url, monkeypatch):
    from src.pipeline import scheduler
    from src.pipeline.lifecycle import source_lock

    monkeypatch.setattr(scheduler, "make_engine", lambda _url, **kwargs: engine)
    monkeypatch.setattr(engine, "dispose", lambda: None)
    settings = _settings(db_url.render_as_string(hide_password=False))
    with source_lock(engine, "pipeline"):
        assert scheduler.run_once("pipeline", settings) == 1


@pytest.mark.parametrize("job_args", [["--job", "bogus"], ["--job"]])
def test_once_rejects_a_missing_or_unknown_job(job_args):
    """M3-N4: argparse exits 2 instead of an IndexError traceback."""
    backend = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-m", "src.pipeline.scheduler", "--once", *job_args],
        cwd=backend,
        capture_output=True,
        text=True,
        timeout=60,
        env={"PATH": "", "PYTHONPATH": str(backend)},
    )
    assert result.returncode == 2
    assert "--job" in result.stderr and "Traceback" not in result.stderr


# ── missed-run monitor ──


@pytest.mark.db
def test_health_reports_a_missed_scheduled_run_as_sev2(engine, fixture_path):
    from src.pipeline.health import health

    run(engine, Feed(fixture_path))
    assert "pipeline:missed_run" not in health(engine)["issues"]
    with engine.begin() as conn:
        conn.execute(
            update(models.ingestion_runs).values(
                started_at=datetime.now(timezone.utc) - timedelta(days=10)
            )
        )
    report = health(engine)
    assert "pipeline:missed_run" in report["issues"]
    assert report["issue_severity"]["pipeline:missed_run"] == "SEV2"


# ── M3-N1: rolled-back state in health ──


@pytest.mark.db
def test_health_shows_a_rolled_back_publication_until_the_next_publish(
    engine, fixture_path
):
    from src.pipeline.health import health

    first, second = _two_publications(engine, fixture_path)
    assert "publication:rolled_back" not in health(engine)["issues"]
    rollback(engine, first, actor=ACTOR, reason=REASON)
    report = health(engine)
    assert "publication:rolled_back" in report["issues"]
    assert report["issue_severity"]["publication:rolled_back"] == "SEV3"
    third = run(engine, Feed(fixture_path))
    assert current(engine)["id"] == third
    assert "publication:rolled_back" not in health(engine)["issues"]


def test_gdelt_engine_disposal_failure_is_logged_safely(monkeypatch, caplog):
    """SEC-M3-01: a cleanup error neither escapes run_once nor exposes its text."""
    from src.pipeline import scheduler
    from src.services.gdelt import job

    class LeakyEngine:
        def dispose(self):
            raise RuntimeError("postgresql://u:SYNTHETIC_SECRET@db/x")

    monkeypatch.setattr(scheduler, "make_engine", lambda *a, **kwargs: LeakyEngine())
    monkeypatch.setattr(job, "run_gdelt", lambda engine: job.RunSummary(day="2026-10-22"))
    with caplog.at_level("ERROR"):
        assert scheduler.run_once("gdelt", _settings()) == 0
    assert "engine dispose failed: RuntimeError" in caplog.text
    assert "SYNTHETIC_SECRET" not in caplog.text
