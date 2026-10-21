"""X1: real collector outcomes carry content dates through publication."""

from datetime import date, timedelta

import pytest
from src.ingestion import collectors
from src.ingestion.canonical import canonicalize
from src.ingestion.importer import (
    CollectionOutcome,
    import_collection,
    validate_records,
)
from src.pipeline.lifecycle import begin_run
from src.pipeline.publication import PublicationQualityError, current, publish, replay
from src.settings import _quality_defaults
from test_collectors import FakeResponse, FakeSession, fmp_item, purchase, si_rows
from test_pipeline import caps, fixture_records

SOURCES = ("congress_trades", "insider_trades", "short_interest")
UPSTREAM = {"trades": [{"ticker": "ZZAA", "trade_type": "Purchase"}], "holdings": []}


def outcome(source, day, marker=0):
    if source == "congress_trades":
        row = dict(
            fmp_item(),
            transactionDate=(day - timedelta(days=2)).isoformat(),
            disclosureDate=day.isoformat(),
            firstName=f"Synthetic{marker}",
        )
        return collectors.collect_congress(
            FakeSession(lambda u, p: FakeResponse(body=[row.copy()])),
            api_key="synthetic",
            sleep=lambda _: None,
        )
    if source == "insider_trades":
        row = purchase(
            transactionDate=(day - timedelta(days=2)).isoformat(),
            filingDate=day.isoformat(),
            name=f"Synthetic{marker}",
        )
        return collectors.collect_insider(
            FakeSession(lambda u, p: FakeResponse(body={"data": [row]})),
            upstream=UPSTREAM,
            api_key="synthetic",
            sleep=lambda _: None,
        )
    body = si_rows(day.strftime("%m/%d/%Y"))
    body["data"]["shortInterestTable"]["rows"][0]["interest"] = str(1000 + marker)
    return collectors.collect_short_interest(
        FakeSession(lambda u, p: FakeResponse(body=body)),
        upstream=UPSTREAM,
        sleep=lambda _: None,
    )


@pytest.mark.parametrize("source", SOURCES)
def test_content_date_and_stable_batch_identity(source):
    day = date.today() - timedelta(days=2)
    first, second = outcome(source, day), outcome(source, day)
    assert first.succeeded and second.succeeded
    assert first.source_as_of == second.source_as_of == day
    hashes = [
        canonicalize(
            source, validate_records(source, o.records), o.source_as_of
        ).content_hash
        for o in (first, second)
    ]
    assert hashes[0] == hashes[1]


def test_congress_uses_latest_retained_disclosure():
    day = date.today()
    rows = [
        dict(
            fmp_item(),
            symbol="ZZAA",
            transactionDate=day.isoformat(),
            disclosureDate=day.isoformat(),
        ),
        dict(
            fmp_item(),
            symbol="ZZBB",
            transactionDate=day.isoformat(),
            disclosureDate=(day - timedelta(days=1)).isoformat(),
        ),
        dict(
            fmp_item(),
            symbol="ZZCC",
            transactionDate="2000-01-01",
            disclosureDate=(day + timedelta(days=1)).isoformat(),
        ),
    ]
    result = collectors.collect_congress(
        FakeSession(lambda u, p: FakeResponse(body=rows)),
        api_key="synthetic",
        sleep=lambda _: None,
    )
    assert result.succeeded and len(result.records) == 4
    assert result.source_as_of == day


def test_insider_uses_latest_filing_of_retained_buys_only():
    day = date.today()
    rows = [
        purchase(filingDate=(day - timedelta(days=1)).isoformat()),
        purchase(filingDate=day.isoformat(), name="Other synthetic buyer"),
        purchase(filingDate=(day + timedelta(days=1)).isoformat(), transactionCode="S"),
        purchase(filingDate=(day + timedelta(days=1)).isoformat(), change=1),
    ]
    result = collectors.collect_insider(
        FakeSession(lambda u, p: FakeResponse(body={"data": rows})),
        upstream=UPSTREAM,
        api_key="synthetic",
        sleep=lambda _: None,
    )
    assert result.succeeded and len(result.records) == 2
    assert result.source_as_of == day


def test_short_interest_mixed_settlements_use_latest():
    days = [date.today() - timedelta(days=15), date.today()]
    responses = iter(si_rows(day.strftime("%m/%d/%Y")) for day in days)
    upstream = {
        "trades": [{"ticker": t, "trade_type": "Purchase"} for t in ("ZZAA", "ZZBB")],
        "holdings": [],
    }
    result = collectors.collect_short_interest(
        FakeSession(lambda u, p: FakeResponse(body=next(responses))),
        upstream=upstream,
        sleep=lambda _: None,
    )
    assert result.succeeded and result.source_as_of == max(days)


@pytest.mark.parametrize("source", SOURCES)
def test_empty_batch_has_no_invented_observation_date(source):
    body = (
        []
        if source == "congress_trades"
        else {"data": [] if source == "insider_trades" else None}
    )
    session = FakeSession(lambda u, p: FakeResponse(body=body))
    if source == "congress_trades":
        result = collectors.collect_congress(
            session, api_key="synthetic", sleep=lambda _: None
        )
    elif source == "insider_trades":
        result = collectors.collect_insider(
            session, upstream=UPSTREAM, api_key="synthetic", sleep=lambda _: None
        )
    else:
        result = collectors.collect_short_interest(
            session, upstream=UPSTREAM, sleep=lambda _: None
        )
    assert result.succeeded and result.records == [] and result.source_as_of is None


@pytest.mark.parametrize("source", ("congress_trades", "insider_trades"))
def test_same_date_amendments_allowed_by_default(source):
    assert _quality_defaults()[source].allow_equal_as_of


def import_round(engine, fixture_path, changed_source=None, delta=0, marker=0):
    runs, batches = {}, {}
    day = date.today() - timedelta(days=2)
    for source in (
        "congress_trades",
        "ark_holdings",
        "insider_trades",
        "short_interest",
    ):
        if source == "ark_holdings":
            collected = CollectionOutcome.success(
                source,
                "live",
                fixture_records(fixture_path, "ark_holdings.json", "holdings"),
                source_as_of=day,
                coverage={"complete": True, "scope": "snapshot"},
            )
        else:
            collected = outcome(
                source,
                day + timedelta(days=delta if source == changed_source else 0),
                marker if source == changed_source else 0,
            )
        dependencies = (
            {s: batches[s] for s in ("congress_trades", "ark_holdings")}
            if source in ("insider_trades", "short_interest")
            else {}
        )
        run = begin_run(engine, source, dependencies=dependencies)
        result = import_collection(engine, collected, run_id=run)
        assert result.succeeded, result
        runs[source], batches[source] = result.run_id, result.batch_id
    return runs, batches


@pytest.mark.db
@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("delta", [0, 1, -1])
def test_changed_collector_batch_publishes_unless_date_regresses(
    engine, fixture_path, source, delta
):
    runs, batches = import_round(engine, fixture_path)
    first = publish(engine, runs, caps(engine, batches))
    saved = current(engine)["output"]
    runs, batches = import_round(engine, fixture_path, source, delta, marker=1)
    if delta < 0:
        with pytest.raises(PublicationQualityError) as error:
            publish(engine, runs, caps(engine, batches))
        assert f"{source}:source_as_of" in error.value.checks
        assert current(engine)["id"] == first
    else:
        second = publish(engine, runs, caps(engine, batches))
        assert second != first
        assert replay(engine, second) == current(engine)["output"]
    assert replay(engine, first) == saved


@pytest.mark.parametrize(
    "dates", [[], [None], ["2026-10-01", None], ["not-a-date"], ["2026-02-30"]]
)
def test_unknown_content_dates_are_not_filled_with_collection_time(dates):
    assert collectors._latest_iso_date(dates) is None


@pytest.mark.db
def test_empty_first_publication_and_identical_retry_keep_no_baseline(
    engine, fixture_path
):
    day = date.today()
    runs, batches = {}, {}
    for source in (
        "congress_trades",
        "ark_holdings",
        "insider_trades",
        "short_interest",
    ):
        records = (
            fixture_records(fixture_path, "ark_holdings.json", "holdings")
            if source == "ark_holdings"
            else []
        )
        dependencies = (
            {s: batches[s] for s in ("congress_trades", "ark_holdings")}
            if source in ("insider_trades", "short_interest")
            else {}
        )
        run = begin_run(engine, source, dependencies=dependencies)
        result = import_collection(
            engine,
            CollectionOutcome.success(
                source,
                "live",
                records,
                source_as_of=day if source == "ark_holdings" else None,
                coverage={"complete": True, "scope": "snapshot"},
            ),
            run_id=run,
        )
        assert result.succeeded
        runs[source], batches[source] = result.run_id, result.batch_id
    first = publish(engine, runs, caps(engine, batches))
    saved = current(engine)["output"]
    assert all(
        item["status"] == "no_baseline"
        for item in current(engine)["ranking_config"]["quality_audit"][
            "sources"
        ].values()
    )
    second = publish(engine, runs, caps(engine, batches))
    assert replay(engine, first) == saved
    assert replay(engine, second) == current(engine)["output"]
