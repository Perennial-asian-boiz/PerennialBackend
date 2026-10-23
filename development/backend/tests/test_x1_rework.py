"""Checker X1-1 scenarios inverted: query coverage must survive universe changes."""

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from src.db import models
from src.ingestion import collectors
from src.ingestion.canonical import canonicalize
from src.ingestion.importer import (
    CollectionOutcome,
    import_collection,
    validate_records,
)
from src.pipeline.lifecycle import begin_run
from src.pipeline.publication import PublicationQualityError, current, publish, replay
from test_collectors import FakeResponse, FakeSession, purchase
from test_pipeline import caps, fixture_records


def collect(monkeypatch, day, tickers, rows):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.combine(day, datetime.min.time(), tzinfo=timezone.utc)

    monkeypatch.setattr(collectors, "datetime", Clock)
    session = FakeSession(lambda u, p: FakeResponse(body={"data": rows[p["symbol"]]}))
    result = collectors.collect_insider(
        session,
        upstream={
            "trades": [
                {"ticker": ticker, "trade_type": "Purchase"} for ticker in tickers
            ],
            "holdings": [],
        },
        api_key="synthetic",
        sleep=lambda _: None,
    )
    assert result.succeeded
    assert all(params["to"] == day.isoformat() for _, params in session.calls)
    return result


def setup_inputs(engine, fixture_path):
    runs, batches = {}, {}
    for source, filename, key in [
        ("congress_trades", "trades_congress.json", "trades"),
        ("ark_holdings", "ark_holdings.json", "holdings"),
        ("short_interest", "short_interest.json", "records"),
    ]:
        deps = (
            {s: batches[s] for s in ("congress_trades", "ark_holdings")}
            if source == "short_interest"
            else {}
        )
        run = begin_run(engine, source, dependencies=deps)
        result = import_collection(
            engine,
            CollectionOutcome.success(
                source,
                "live",
                fixture_records(fixture_path, filename, key),
                source_as_of=date.today(),
                coverage={"complete": True, "scope": "snapshot"},
            ),
            run_id=run,
        )
        assert result.succeeded
        runs[source], batches[source] = result.run_id, result.batch_id
    return runs, batches


@pytest.mark.db
@pytest.mark.parametrize("scenario", ["universe_shrinks", "no_qualifying_buys"])
def test_checker_lockout_now_publishes_or_allows_quality_override(
    engine, fixture_path, monkeypatch, scenario
):
    runs, batches = setup_inputs(engine, fixture_path)
    day = date.today() - timedelta(days=2)
    rows = {
        "ZZAA": [purchase(filingDate=(day - timedelta(days=5)).isoformat())],
        "ZZBB": [purchase(filingDate=day.isoformat())],
    }
    saved = []
    previous_day = None
    for offset in range(3):
        tickers = ["ZZAA", "ZZBB"]
        selected = rows
        if offset == 1:
            if scenario == "universe_shrinks":
                tickers = ["ZZAA"]
            else:
                selected = {
                    ticker: [dict(row, transactionCode="S") for row in values]
                    for ticker, values in rows.items()
                }
        result = collect(monkeypatch, day + timedelta(days=offset), tickers, selected)
        assert result.source_as_of == day + timedelta(days=offset)
        assert previous_day is None or result.source_as_of >= previous_day
        previous_day = result.source_as_of
        run = begin_run(
            engine,
            "insider_trades",
            dependencies={s: batches[s] for s in ("congress_trades", "ark_holdings")},
        )
        imported = import_collection(engine, result, run_id=run)
        assert imported.succeeded
        runs["insider_trades"] = imported.run_id
        with engine.connect() as conn:
            payload = conn.execute(
                select(models.source_batches.c.payload).where(
                    models.source_batches.c.id == imported.batch_id
                )
            ).scalar_one()
        assert payload["source_as_of"] == result.source_as_of.isoformat()
        try:
            publication = publish(engine, runs, caps(engine, batches))
        except PublicationQualityError as error:
            assert "insider_trades:source_as_of" not in error.checks
            publication = publish(
                engine,
                runs,
                caps(engine, batches),
                override_actor="synthetic",
                override_reason="synthetic count change",
                override_checks=list(error.checks),
            )
        saved.append((publication, current(engine)["output"]))
    for publication, output in saved:
        assert replay(engine, publication) == output


def test_identical_insider_content_gets_query_date_per_day(monkeypatch):
    day = date.today() - timedelta(days=1)
    rows = {"ZZAA": [purchase(filingDate="2000-01-01")]}
    first = collect(monkeypatch, day, ["ZZAA"], rows)
    second = collect(monkeypatch, day + timedelta(days=1), ["ZZAA"], rows)
    assert first.source_as_of == day
    assert second.source_as_of == day + timedelta(days=1)
    hashes = [
        canonicalize(
            "insider_trades",
            validate_records("insider_trades", item.records),
            item.source_as_of,
        ).content_hash
        for item in (first, second)
    ]
    assert hashes[0] != hashes[1]


@pytest.mark.db
def test_empty_short_interest_fails_and_writes_no_batch(engine):
    result = collectors.collect_short_interest(
        FakeSession(lambda u, p: FakeResponse(body={"data": None})),
        upstream={
            "trades": [{"ticker": "ZZAA", "trade_type": "Purchase"}],
            "holdings": [],
        },
        sleep=lambda _: None,
    )
    assert not result.succeeded and result.error_code == "empty_short_interest"
    with engine.connect() as conn:
        before = conn.execute(
            select(func.count()).select_from(models.source_batches)
        ).scalar_one()
    imported = import_collection(engine, result)
    assert not imported.succeeded and imported.error_code == "empty_short_interest"
    with engine.connect() as conn:
        assert (
            conn.execute(
                select(func.count()).select_from(models.source_batches)
            ).scalar_one()
            == before
        )
