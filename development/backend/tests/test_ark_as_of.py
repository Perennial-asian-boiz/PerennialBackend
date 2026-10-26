"""ARK holdings' as-of date, per fund (staging run 19), spec-first.

collect_ark set source_as_of to _single_iso_date over every ticker row of every
fund: None unless all four funds report one identical date. ARK's funds report
different dates, so every staging ARK batch had source_as_of NULL and publish
refused it ("ARK observation date is stale or unknown").

The rule pinned here:
- A fund's as-of is the latest ISO date among its ticker rows; any ticker row
  with a missing or non-ISO date makes that fund unknown.
- The batch's source_as_of is the oldest fund date (a batch is only as fresh as
  its stalest fund, so publish's 7-day check stays honest); any unknown fund
  makes it None.
- A fund dated in the future must not hide behind older funds: the batch is
  then unknown or future, so publish refuses either way (an amendment to
  "oldest fund").
- Identity: identical collections give the same source_as_of, and nothing else
  in the envelope changes.
Fake HTTP only (test_collectors' fakes).
"""

from datetime import date, datetime, timedelta, timezone

import pytest
from src.ingestion import collectors
from test_collectors import NO_SLEEP, FakeResponse, ark_session, holding

FUNDS = ("ARKK", "ARKQ", "ARKG", "ARKW")


def fund(*days, symbol="ARKQ"):
    """One fund's body: a ticker row per day, plus a cash row with no date."""
    rows = [holding(f"ZZ{symbol[-1]}{i}", 1.0, day=d) for i, d in enumerate(days)]
    return FakeResponse(
        body={"symbol": symbol, "holdings": [*rows, {"ticker": None, "weight": 0.1}]}
    )


def as_of(overrides):
    outcome = collectors.collect_ark(ark_session(overrides), sleep=NO_SLEEP)
    assert outcome.succeeded, outcome.error_code
    return outcome.source_as_of


def test_funds_on_different_dates_give_the_oldest_fund_date():
    # ARKQ is a day behind the default 2026-09-30 of the other three funds.
    assert as_of({"ARKQ": fund("2026-09-29")}) == date(2026, 9, 29)


def test_all_funds_on_one_date_give_that_date():
    assert as_of({}) == date(2026, 9, 30)


def test_a_fund_with_mixed_dates_counts_as_its_latest():
    assert as_of({"ARKQ": fund("2026-09-28", "2026-09-30", "2026-09-29")}) == date(
        2026, 9, 30
    )
    assert as_of({"ARKQ": fund("2026-09-27", "2026-09-28")}) == date(2026, 9, 28)


def test_the_stalest_fund_sets_the_batch_date_even_eight_days_back():
    """Not hidden behind fresher funds, so publish's 7-day check refuses it."""
    assert as_of({"ARKG": fund("2026-09-20", symbol="ARKG")}) == date(2026, 9, 20)


@pytest.mark.parametrize(
    "bad_day",
    [None, "", "09/30/2026", "2026-9-30", "2026-02-30", 20260930],
    ids=["missing", "empty", "us_format", "unpadded", "impossible", "integer"],
)
def test_one_ticker_row_without_an_iso_date_makes_the_batch_unknown(bad_day):
    assert as_of({"ARKW": fund("2026-09-30", bad_day, symbol="ARKW")}) is None


def test_rows_without_a_ticker_dont_count():
    body = {
        "symbol": "ARKK",
        "holdings": [
            holding("ZZK1", day="2026-09-29"),
            {"ticker": None, "weight": 0.2, "date": "garbage"},
        ],
    }
    assert as_of({"ARKK": FakeResponse(body=body)}) == date(2026, 9, 29)


def test_a_future_dated_fund_is_not_hidden_behind_older_funds():
    today = datetime.now(timezone.utc).date()
    future = (today + timedelta(days=3)).isoformat()
    recent = (today - timedelta(days=1)).isoformat()
    overrides = {f: fund(recent, symbol=f) for f in FUNDS}
    overrides["ARKG"] = fund(future, symbol="ARKG")
    day = as_of(overrides)
    assert day is None or day > today, "publish's future-dated check must still see it"


def test_identical_collections_have_the_same_date_and_envelope():
    overrides = {"ARKQ": fund("2026-09-29")}
    first = collectors.collect_ark(ark_session(overrides), sleep=NO_SLEEP)
    second = collectors.collect_ark(ark_session(overrides), sleep=NO_SLEEP)
    assert first.source_as_of == second.source_as_of == date(2026, 9, 29)
    assert first.records == second.records
    drop = lambda e: {k: v for k, v in e.items() if k != "fetched_at"}  # noqa: E731
    assert drop(first.envelope) == drop(second.envelope)
    assert set(first.envelope) == {
        "source",
        "funds_tracked",
        "fetched_at",
        "total_tickers",
        "multi_fund_tickers",
        "single_fund_tickers",
    }


@pytest.mark.db
def test_identical_collections_dedupe_to_one_batch(engine):
    from src.ingestion.importer import import_collection

    overrides = {"ARKQ": fund("2026-09-29")}
    first = import_collection(
        engine, collectors.collect_ark(ark_session(overrides), sleep=NO_SLEEP)
    )
    second = import_collection(
        engine, collectors.collect_ark(ark_session(overrides), sleep=NO_SLEEP)
    )
    assert first.succeeded and second.succeeded
    assert first.batch_id == second.batch_id
    from sqlalchemy import text

    with engine.connect() as conn:
        assert conn.execute(
            text("SELECT source_as_of FROM perennial.source_batches WHERE id = :i"),
            {"i": first.batch_id},
        ).scalar() == date(2026, 9, 29)
