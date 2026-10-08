"""PostgreSQL tests: migration, import semantics, rollback, latest-successful, securities, concurrency."""

import copy
import json
import threading
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import func, insert, select, text
from sqlalchemy.exc import IntegrityError

from src.db import models
from src.db.models import ingestion_runs, securities, source_batches
from src.db.queries import latest_rows, latest_successful_batch_id, latest_successful_run
from src.ingestion import importer
from src.ingestion.files import read_fetcher_file
from src.ingestion.importer import CollectionOutcome, import_collection, prune_failed_diagnostics
from tests.conftest import alembic_config

pytestmark = pytest.mark.db

FILES = {
    "congress_trades": "trades_congress.json",
    "ark_holdings": "ark_holdings.json",
    "insider_trades": "trades_insider.json",
    "short_interest": "short_interest.json",
}
KEYS = {"congress_trades": "trades", "ark_holdings": "holdings",
        "insider_trades": "transactions", "short_interest": "records"}
SECRET = "SYNTHETIC_SECRET_7f3a"


def fixture_outcome(fixture_path, source, mutate=None):
    data = json.loads(fixture_path(FILES[source]).read_text())
    if mutate:
        mutate(data[KEYS[source]])
    envelope = {k: v for k, v in data.items() if k != KEYS[source]}
    return CollectionOutcome.success(source, "fixture", data[KEYS[source]], envelope=envelope)


def count(engine, table, **where):
    with engine.connect() as conn:
        q = select(func.count()).select_from(table)
        for k, v in where.items():
            q = q.where(table.c[k] == v)
        return conn.execute(q).scalar_one()


# ── migrations ───────────────────────────────────────────

def test_migration_matches_models_and_round_trips(migrated_engine):
    with migrated_engine.connect() as conn:
        diff = compare_metadata(
            MigrationContext.configure(conn, opts={"include_schemas": True, "compare_type": True,
                                                   "include_name": lambda n, t, p: t != "schema" or n == "perennial"}),
            models.metadata,
        )
        assert diff == []
        command.downgrade(alembic_config(conn), "base")
        assert conn.execute(text("SELECT to_regnamespace('perennial')")).scalar() is None
        command.upgrade(alembic_config(conn), "head")
        tables = conn.execute(text(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'perennial'"
        )).scalar_one()
        assert tables == len(models.metadata.tables)


def test_constraints_reject_bad_rows(engine):
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(insert(models.ark_holdings).values(
                batch_id=999, security_id=999, funds=["ARKK"], fund_count=1, total_weight=1))
    batch = dict(source="ark_holdings", content_hash="a" * 64, hash_version="v1",
                 record_count=0, payload={})
    with engine.begin() as conn:
        conn.execute(insert(source_batches).values(**batch))
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(insert(source_batches).values(**batch))
    with pytest.raises(IntegrityError):  # a run cannot point at another source's batch
        with engine.begin() as conn:
            conn.execute(insert(ingestion_runs).values(
                source="short_interest", status="succeeded", collection_mode="fixture",
                started_at=datetime.now(timezone.utc), finished_at=datetime.now(timezone.utc),
                batch_id=1, reused_batch=False))


# ── import semantics ─────────────────────────────────────

@pytest.mark.parametrize("source", list(FILES))
def test_each_fixture_imports_then_reuses(engine, fixture_path, source):
    first = import_collection(engine, fixture_outcome(fixture_path, source))
    assert first.succeeded and first.reused_batch is False
    table = models.SOURCE_TABLES[source]
    rows = count(engine, table)
    assert rows == first.accepted_count == first.input_count

    second = import_collection(engine, fixture_outcome(fixture_path, source))
    assert second.succeeded and second.reused_batch is True and second.batch_id == first.batch_id
    assert count(engine, source_batches) == 1 and count(engine, table) == rows
    assert count(engine, ingestion_runs, status="succeeded") == 2


def test_fixture_values_round_trip(engine, fixture_path):
    for source in FILES:
        assert import_collection(engine, fixture_outcome(fixture_path, source)).succeeded
    with engine.connect() as conn:
        congress = latest_rows(conn, "congress_trades")
        ark = {r["symbol"]: r for r in latest_rows(conn, "ark_holdings")}
        insider = latest_rows(conn, "insider_trades")
        si = {r["symbol"]: r for r in latest_rows(conn, "short_interest")}
        payload = conn.execute(select(source_batches.c.payload)
                               .where(source_batches.c.source == "short_interest")).scalar_one()

    assert len(congress) == 5
    repeated = [r for r in congress if r["symbol"] == "ZZAA"]
    assert len(repeated) == 2 and repeated[0]["row_number"] != repeated[1]["row_number"]
    injected = next(r for r in congress if r["symbol"] == "ZZB.B")
    assert injected["politician_name"].startswith("Robert'); DROP TABLE")
    zzbb = next(r for r in congress if r["symbol"] == "ZZBB")
    assert zzbb["disclosure_date"] is None and zzbb["source_link"] is None
    assert zzbb["transaction_date"] == date(2026, 7, 1)

    assert ark["ZZAA"]["total_weight"] == Decimal("10.30000000")
    assert ark["ZZAA"]["funds"] == ["ARKK", "ARKW"]
    assert ark["ZZDD"]["share_price"] is None
    assert ark["ZZEE"]["total_weight"] == Decimal("101.25000000")

    assert len(insider) == 3
    assert sorted(r["value"] for r in insider) == [Decimal("25000.10"), Decimal("25000.10"), Decimal("1500000.00")]

    assert si["ZZAA"]["short_interest_shares"] == 2926673 and si["ZZAA"]["average_daily_volume"] == 1500000
    assert si["ZZAA"]["days_to_cover"] == Decimal("1.9500")
    assert si["ZZDD"]["average_daily_volume"] is None and si["ZZDD"]["days_to_cover"] is None
    assert len(payload["records"][0]["history"]) == 2 and payload["envelope"]["source"] == "nasdaq"

    with engine.connect() as conn:  # securities are shared across sources
        assert conn.execute(select(func.count()).select_from(securities)
                            .where(securities.c.symbol == "ZZAA")).scalar_one() == 1


def test_fetched_at_only_change_reuses_batch(engine, fixture_path):
    first = import_collection(engine, fixture_outcome(fixture_path, "insider_trades"))

    def restamp(rows):
        for r in rows:
            r["fetched_at"] = "2031-01-01T00:00:00+00:00"

    again = import_collection(engine, fixture_outcome(fixture_path, "insider_trades", restamp))
    assert again.reused_batch and again.batch_id == first.batch_id


def test_changed_snapshot_creates_new_batch_and_keeps_old(engine, fixture_path):
    first = import_collection(engine, fixture_outcome(fixture_path, "ark_holdings"))

    def change(rows):
        rows[0]["total_weight"] = 11.0

    second = import_collection(engine, fixture_outcome(fixture_path, "ark_holdings", change))
    assert second.succeeded and second.batch_id != first.batch_id
    assert count(engine, models.ark_holdings, batch_id=first.batch_id) == 3
    assert count(engine, models.ark_holdings, batch_id=second.batch_id) == 3
    with engine.connect() as conn:
        assert latest_successful_batch_id(conn, "ark_holdings") == second.batch_id
        assert len(latest_rows(conn, "ark_holdings")) == 3  # never summed across batches


def test_latest_follows_run_even_when_it_reuses_older_batch(engine, fixture_path):
    a = import_collection(engine, fixture_outcome(fixture_path, "ark_holdings"))
    b = import_collection(engine, fixture_outcome(
        fixture_path, "ark_holdings", lambda rows: rows.pop()))
    again_a = import_collection(engine, fixture_outcome(fixture_path, "ark_holdings"))
    assert b.batch_id != a.batch_id and again_a.batch_id == a.batch_id and again_a.reused_batch
    with engine.connect() as conn:
        latest = latest_successful_run(conn, "ark_holdings")
    assert latest.batch_id == a.batch_id and latest.run_id == again_a.run_id


# ── failures and rollback ────────────────────────────────

def test_malformed_record_fails_without_partial_rows(engine, fixture_path):
    good = import_collection(engine, fixture_outcome(fixture_path, "congress_trades"))

    def corrupt(rows):
        rows[3]["transaction_date"] = "13/45/2026"
        rows[4]["politician_name"] = SECRET  # valid text; must not appear in diagnostics either

    bad = import_collection(engine, fixture_outcome(fixture_path, "congress_trades", corrupt))
    assert not bad.succeeded and bad.error_code == "validation_failed"
    assert count(engine, source_batches) == 1 and count(engine, models.congress_trades) == 5
    with engine.connect() as conn:
        run = conn.execute(select(ingestion_runs).where(ingestion_runs.c.id == bad.run_id)).one()
        assert latest_successful_batch_id(conn, "congress_trades") == good.batch_id
    assert run.batch_id is None and run.diagnostics["errors"][0]["field"] == "transaction_date"
    assert "record 3" in run.error_summary
    assert SECRET not in json.dumps(run.diagnostics) + run.error_summary


def test_database_error_mid_import_rolls_back_everything(engine, fixture_path, monkeypatch):
    good = import_collection(engine, fixture_outcome(fixture_path, "ark_holdings"))

    def boom(conn):
        conn.execute(text("SELECT 1/0"))  # raises after batch + rows were inserted

    monkeypatch.setattr(importer, "_after_batch_write_hook", boom)
    bad = import_collection(engine, fixture_outcome(
        fixture_path, "ark_holdings", lambda rows: rows.pop()))
    assert bad.error_code == "database_error" and "22012" in bad.error_summary
    assert count(engine, source_batches) == 1 and count(engine, models.ark_holdings) == 3
    assert count(engine, securities) == 3

    def crash(conn):
        raise RuntimeError(SECRET)

    monkeypatch.setattr(importer, "_after_batch_write_hook", crash)
    worse = import_collection(engine, fixture_outcome(
        fixture_path, "ark_holdings", lambda rows: rows.pop()))
    assert worse.error_code == "internal_error" and SECRET not in worse.error_summary
    with engine.connect() as conn:
        assert latest_successful_batch_id(conn, "ark_holdings") == good.batch_id


def test_failed_collection_keeps_previous_batch(engine, fixture_path):
    good = import_collection(engine, fixture_outcome(fixture_path, "short_interest"))
    failed = import_collection(engine, CollectionOutcome.failure(
        "short_interest", "live", "partial_collection",
        diagnostics={"unit_kind": "short-interest tickers", "attempted": 2, "completed": 1, "failed": 1,
                     "failures": [{"unit": "ZZAA", "code": "http_503"}], "secret": SECRET},
    ))
    assert failed.error_code == "partial_collection"
    assert failed.error_summary.endswith("1 of 2 short-interest tickers failed: ZZAA (http_503)")
    assert count(engine, source_batches) == 1
    with engine.connect() as conn:
        assert latest_successful_batch_id(conn, "short_interest") == good.batch_id
        stored = conn.execute(select(ingestion_runs.c.diagnostics)
                              .where(ingestion_runs.c.id == failed.run_id)).scalar_one()
    assert SECRET not in json.dumps(stored)


def test_successful_empty_collection_is_distinct_from_failure(engine):
    empty = import_collection(engine, CollectionOutcome.success("insider_trades", "live", []))
    assert empty.succeeded and empty.accepted_count == 0
    with engine.connect() as conn:
        assert latest_successful_batch_id(conn, "insider_trades") == empty.batch_id
        assert latest_rows(conn, "insider_trades") == []
    failed = import_collection(engine, CollectionOutcome.failure("insider_trades", "live", "collection_failed"))
    assert not failed.succeeded
    with engine.connect() as conn:
        assert latest_successful_batch_id(conn, "insider_trades") == empty.batch_id


def test_unattested_file_is_recorded_as_failed(engine, fixture_path):
    outcome = read_fetcher_file("ark_holdings", fixture_path("ark_holdings.json"), mode="file")
    result = import_collection(engine, outcome)
    assert result.error_code == "collection_unattested" and count(engine, source_batches) == 0


def test_diagnostics_retention(engine):
    def fail():
        return import_collection(engine, CollectionOutcome.failure(
            "ark_holdings", "live", "collection_failed",
            diagnostics={"attempted": 4, "completed": 0, "failed": 4}))

    runs = [fail() for _ in range(importer.DIAGNOSTICS_KEEP_PER_SOURCE + 3)]
    with engine.connect() as conn:
        kept = conn.execute(select(ingestion_runs.c.id).where(
            ingestion_runs.c.diagnostics.isnot(None))).scalars().all()
    assert len(kept) == importer.DIAGNOSTICS_KEEP_PER_SOURCE
    assert runs[0].run_id not in kept and runs[-1].run_id in kept

    with engine.begin() as conn:  # age-based expiry
        conn.execute(text("UPDATE perennial.ingestion_runs SET started_at = started_at - interval '40 days', "
                          "finished_at = finished_at - interval '40 days'"))
        assert prune_failed_diagnostics(conn, "ark_holdings") == importer.DIAGNOSTICS_KEEP_PER_SOURCE
    assert count(engine, ingestion_runs) == len(runs)  # run rows themselves are kept


# ── securities ───────────────────────────────────────────

def test_security_ids_are_stable_and_exchange_conflicts_rejected(engine, fixture_path):
    def with_exchange(exchange):
        def mutate(rows):
            rows[0]["exchange"] = exchange
        return mutate

    a = import_collection(engine, fixture_outcome(fixture_path, "ark_holdings", with_exchange("nasdaq")))
    with engine.connect() as conn:
        before = dict(conn.execute(select(securities.c.symbol, securities.c.id)).all())
        assert conn.execute(select(securities.c.exchange)
                            .where(securities.c.symbol == "ZZAA")).scalar_one() == "NASDAQ"
    assert a.succeeded

    b = import_collection(engine, fixture_outcome(fixture_path, "insider_trades"))
    assert b.succeeded
    conflict = import_collection(engine, fixture_outcome(
        fixture_path, "short_interest", with_exchange("NYSE")))
    assert conflict.error_code == "security_conflict" and "ZZAA (exchange_conflict)" in conflict.error_summary
    with engine.connect() as conn:
        after = dict(conn.execute(select(securities.c.symbol, securities.c.id)).all())
    assert {k: after[k] for k in before} == before
    assert count(engine, source_batches, source="short_interest") == 0


# ── concurrency ──────────────────────────────────────────

def _run_parallel(n, target):
    barrier = threading.Barrier(n)
    results, errors = [None] * n, []

    def work(i):
        try:
            barrier.wait()
            results[i] = target(i)
        except Exception as exc:  # pragma: no cover - surfaced by the assertion below
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    return results


def test_concurrent_identical_imports_create_one_batch(engine, fixture_path):
    results = _run_parallel(6, lambda i: import_collection(engine, fixture_outcome(fixture_path, "congress_trades")))
    assert all(r.succeeded for r in results)
    assert len({r.batch_id for r in results}) == 1
    assert sum(1 for r in results if not r.reused_batch) == 1
    assert count(engine, source_batches) == 1 and count(engine, models.congress_trades) == 5


def test_concurrent_sources_share_new_securities(engine, fixture_path):
    sources = list(FILES)
    results = _run_parallel(len(sources), lambda i: import_collection(engine, fixture_outcome(fixture_path, sources[i])))
    assert all(r.succeeded for r in results)
    with engine.connect() as conn:
        symbols = conn.execute(select(securities.c.symbol)).scalars().all()
    assert len(symbols) == len(set(symbols))
    assert {"ZZAA", "ZZDD", "ZZEE"} <= set(symbols)
