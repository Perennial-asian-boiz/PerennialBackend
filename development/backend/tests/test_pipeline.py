"""Production boundaries: lifecycle, publication eligibility, lineage and replay."""
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select, update

from src.db import models
from src.db.queries import batch_rows
from src.ingestion.importer import CollectionOutcome, import_collection
from src.pipeline.lifecycle import begin_run, abandon_stale_runs
from src.pipeline.publication import PublicationError, publish, current, replay

pytestmark = pytest.mark.db


def fixture_records(fixture_path, filename, key):
    records = json.loads(fixture_path(filename).read_text())[key]
    if key == "records":
        today = datetime.now(timezone.utc).date()
        for record in records:
            record["settlement_date"] = (today-timedelta(days=10)).isoformat()
            for index, row in enumerate(record.get("history", [])):
                row["settlement_date"] = (today-timedelta(days=10+15*index)).isoformat()
    return records


def inputs(engine, fixture_path, mode="live"):
    files = {"congress_trades": ("trades_congress.json", "trades"),
             "ark_holdings": ("ark_holdings.json", "holdings"),
             "insider_trades": ("trades_insider.json", "transactions"),
             "short_interest": ("short_interest.json", "records")}
    runs, batches = {}, {}
    for source, (filename, key) in files.items():
        deps = batches.copy() if source in ("insider_trades", "short_interest") else {}
        deps = {s: b for s, b in deps.items() if s in ("congress_trades", "ark_holdings")}
        run = begin_run(engine, source, mode, dependencies=deps)
        records = fixture_records(fixture_path, filename, key)
        outcome = CollectionOutcome.success(source, mode, records,
            source_as_of=datetime.now(timezone.utc).date() if source == "ark_holdings" else None,
            coverage={"complete": True, "scope": "snapshot"})
        result = import_collection(engine, outcome, run_id=run)
        assert result.succeeded
        runs[source], batches[source] = result.run_id, result.batch_id
    return runs, batches


def caps(engine, batches):
    from src.pipeline.ranking import candidate_symbols
    with engine.connect() as conn:
        rows = {s: batch_rows(conn, s, b) for s, b in batches.items()}
    return {s: {"value": Decimal("20000000000"), "currency": "USD", "provider": "fmp",
                "retrieved_at": datetime.now(timezone.utc), "observed_at": None}
            for s in candidate_symbols(rows)}


def test_publication_replay_and_failed_candidate_preserves_current(engine, fixture_path):
    runs, batches = inputs(engine, fixture_path)
    pub = publish(engine, runs, caps(engine, batches))
    assert current(engine)["id"] == pub
    assert replay(engine, pub) == current(engine)["output"]
    fixture_runs, _ = inputs(engine, fixture_path, mode="fixture")
    with pytest.raises(PublicationError, match="mode"):
        publish(engine, fixture_runs, caps(engine, batches))
    assert current(engine)["id"] == pub


def test_stale_source_and_incomplete_coverage_cannot_publish(engine, fixture_path):
    runs, batches = inputs(engine, fixture_path)
    with engine.begin() as conn:
        conn.execute(update(models.ingestion_runs).where(models.ingestion_runs.c.id == runs["ark_holdings"])
                     .values(coverage={"complete": False, "scope": "snapshot"}))
    with pytest.raises(PublicationError, match="coverage"):
        publish(engine, runs, caps(engine, batches))
    with engine.begin() as conn:
        conn.execute(update(models.ingestion_runs).where(models.ingestion_runs.c.id == runs["ark_holdings"])
                     .values(coverage={"complete": True, "scope": "snapshot"},
                             collected_at=datetime.now(timezone.utc)-timedelta(days=10)))
    with pytest.raises(PublicationError, match="stale"):
        publish(engine, runs, caps(engine, batches))
    assert current(engine) is None


def test_abandoned_worker_cannot_finish_or_write_batch(engine):
    run = begin_run(engine, "ark_holdings", "live")
    with engine.begin() as conn:
        conn.execute(update(models.ingestion_runs).where(models.ingestion_runs.c.id == run)
                     .values(heartbeat_at=datetime.now(timezone.utc)-timedelta(hours=1)))
    assert abandon_stale_runs(engine, timedelta(minutes=10)) == 1
    result = import_collection(engine, CollectionOutcome.success("ark_holdings", "live", []), run_id=run)
    assert result.status == "failed" and result.error_code == "worker_abandoned"
    with engine.connect() as conn:
        assert conn.execute(select(models.source_batches.c.id)).first() is None


def test_explicit_batch_reads_validate_source_and_page(engine, fixture_path):
    _, batches = inputs(engine, fixture_path)
    with engine.connect() as conn:
        rows = batch_rows(conn, "ark_holdings", batches["ark_holdings"], limit=1)
        assert len(rows) == 1
        with pytest.raises(ValueError, match="source"):
            batch_rows(conn, "ark_holdings", batches["congress_trades"])


def test_missing_market_cap_and_lineage_are_rejected(engine, fixture_path):
    runs, batches = inputs(engine, fixture_path)
    with pytest.raises(PublicationError, match="market.cap"):
        publish(engine, runs, {})
    with engine.begin() as conn:
        conn.execute(models.run_dependencies.delete().where(
            models.run_dependencies.c.run_id == runs["insider_trades"]))
    with pytest.raises(PublicationError, match="lineage"):
        publish(engine, runs, caps(engine, batches))


def test_database_pipeline_passes_pinned_upstream_without_files(engine, fixture_path):
    from src.pipeline.runner import run_pipeline
    seen = []
    def collector(source, filename, key):
        def collect(**kwargs):
            if source in ("insider_trades", "short_interest"):
                assert set(kwargs["upstream"]) == {"trades", "holdings"}
                assert kwargs["upstream"]["holdings"][0]["ticker"]
            seen.append(source)
            return CollectionOutcome.success(source, "live", fixture_records(fixture_path, filename, key),
                coverage={"complete": True, "scope": "snapshot"},
                source_as_of=datetime.now(timezone.utc).date() if source == "ark_holdings" else None)
        return collect
    collectors = {s: collector(s, f, k) for s, f, k in [
        ("congress_trades", "trades_congress.json", "trades"), ("ark_holdings", "ark_holdings.json", "holdings"),
        ("insider_trades", "trades_insider.json", "transactions"), ("short_interest", "short_interest.json", "records")]}
    def market_caps(symbols):
        return {s: {"value": Decimal("1000000000"), "currency": "USD", "provider": "fmp",
                    "retrieved_at": datetime.now(timezone.utc)} for s in symbols}
    pub = run_pipeline(engine, collectors=collectors, market_cap_collector=market_caps)
    assert seen == list(models.SOURCES)
    assert current(engine)["id"] == pub
    assert replay(engine, pub) == current(engine)["output"]
    seen.clear()
    collectors["ark_holdings"] = lambda: CollectionOutcome.failure("ark_holdings", "live", "collection_failed")
    with pytest.raises(RuntimeError, match="ark_holdings"):
        run_pipeline(engine, collectors=collectors, market_cap_collector=market_caps)
    assert seen == ["congress_trades"]
    assert current(engine)["id"] == pub


def test_collector_crash_is_recorded_and_run_exists_before_fetch(engine):
    from src.pipeline.runner import collect_source
    def crash():
        with engine.connect() as conn:
            assert conn.execute(select(models.ingestion_runs.c.status)).scalar_one() == "running"
        raise RuntimeError("DO_NOT_LOG_PRIVATE_PROVIDER_CONTENT")
    result = collect_source(engine, "ark_holdings", collector=crash)
    assert result.status == "failed"
    with engine.connect() as conn:
        run = conn.execute(select(models.ingestion_runs)).mappings().one()
        assert "DO_NOT_LOG" not in str(run)


def test_publication_cannot_regress_collection_time(engine, fixture_path):
    older, batches = inputs(engine, fixture_path)
    newer, _ = inputs(engine, fixture_path)
    pub = publish(engine, newer, caps(engine, batches))
    with pytest.raises(PublicationError, match="regress"):
        publish(engine, older, caps(engine, batches))
    assert current(engine)["id"] == pub


def test_health_uses_live_runs_not_fixture_success(engine, fixture_path):
    from src.pipeline.health import health
    runs, batches = inputs(engine, fixture_path)
    publish(engine, runs, caps(engine, batches))
    failed = import_collection(engine, CollectionOutcome.failure('ark_holdings', 'live', 'collection_failed'))
    inputs(engine, fixture_path, mode='fixture')
    report = health(engine)
    assert report['sources']['ark_holdings']['run_id'] == failed.run_id
    assert 'ark_holdings:failed' in report['issues']
    assert report['healthy'] is False


def test_health_checks_published_inputs_even_if_new_unpublished_runs_are_fresh(engine, fixture_path):
    from src.pipeline.health import health
    runs, batches = inputs(engine, fixture_path)
    publish(engine, runs, caps(engine, batches))
    with engine.begin() as conn:
        conn.execute(update(models.ingestion_runs).where(models.ingestion_runs.c.id == runs['ark_holdings'])
                     .values(collected_at=datetime.now(timezone.utc)-timedelta(days=4)))
    inputs(engine, fixture_path)
    report = health(engine)
    assert 'publication:ark_holdings:stale' in report['issues']
    assert report['healthy'] is False


def test_workflow_lock_prevents_overlap_and_releases_after_failure(engine):
    from src.pipeline.lifecycle import source_lock
    with pytest.raises(ValueError):
        with source_lock(engine, 'pipeline'):
            with pytest.raises(RuntimeError, match='already running'):
                with source_lock(engine, 'pipeline'):
                    pytest.fail('second workflow acquired the same lock')
            raise ValueError('synthetic worker failure')
    with source_lock(engine, 'pipeline'):
        pass


def test_pipeline_reports_abandoned_collection_and_preserves_publication(engine, fixture_path):
    from src.pipeline.runner import run_pipeline
    runs, batches = inputs(engine, fixture_path)
    previous = publish(engine, runs, caps(engine, batches))
    def abandoned_collector():
        with engine.begin() as conn:
            conn.execute(update(models.ingestion_runs).where(models.ingestion_runs.c.status == 'running')
                         .values(heartbeat_at=datetime.now(timezone.utc)-timedelta(hours=1)))
        assert abandon_stale_runs(engine) == 1
        return CollectionOutcome.success('congress_trades', 'live', [],
                                         coverage={'complete': True, 'scope': 'recent_window'})
    with pytest.raises(RuntimeError, match='^collection failed for congress_trades: worker_abandoned$'):
        run_pipeline(engine, collectors={'congress_trades': abandoned_collector})
    assert current(engine)['id'] == previous
    with engine.connect() as conn:
        lost = conn.execute(select(models.ingestion_runs).where(
            models.ingestion_runs.c.status == 'abandoned')).mappings().one()
        assert lost['error_code'] == 'worker_abandoned' and lost['batch_id'] is None
