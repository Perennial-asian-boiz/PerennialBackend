"""Stale ARK funds are excluded from the batch (staging, 2026-10-26), spec-first.

Staging on f65f3e3: arkfunds.io served ARKW dated 2026-10-06 while ARKK, ARKQ
and ARKG were dated today. Since #70 the batch is as old as its stalest fund,
so ARKW alone made source_as_of 20 days old and publish refused the whole
publication (publication.py's 7-day ARK check).

Bryan's rule, pinned here:
1. A fund whose date (the latest ISO date over its ticker rows) is older than
   the publish limit (today UTC minus ARK_MAX_AGE, 7 days, the boundary publish
   uses) is excluded: its holdings never reach parse_holdings.
2. The excluded funds and their dates go in the envelope as
   excluded_stale_funds, ["ARKW:2026-10-06"] (fund and date only, like GDELT's
   "THEME:YYYY-MM-DD"; present and empty when nothing is excluded), and the
   envelope sanitizer keeps that key.
3. source_as_of is the stalest kept fund's date.
4. No fresh fund: the collection fails with error_code "ark_funds_stale" and a
   {"unit": <fund>, "code": "stale_fund"} failure per fund; the previous good
   batch stays current.
5. Unchanged: an unknown or future-dated fund still makes source_as_of None (or
   future), so publish refuses; cash rows don't count; a fetch failure fails as
   before.

Proposed names (adopt them, or send the tests back): collectors.ARK_MAX_AGE,
the same object as publication.ARK_MAX_AGE; envelope key excluded_stale_funds;
error code ark_funds_stale; failure code stale_fund.
Fake HTTP only (test_collectors' fakes).
"""

from datetime import date, datetime, timedelta, timezone

import pytest
from src.ingestion import collectors
from src.ingestion.diagnostics import ERROR_SUMMARIES, render_summary, safe_diagnostics
from src.ingestion.envelope import sanitize_envelope
from src.ingestion.importer import CollectionOutcome, import_collection
from src.pipeline import publication
from test_collectors import NO_SLEEP, FakeResponse, ark_session, holding

FUNDS = ("ARKK", "ARKQ", "ARKG", "ARKW")
CASH = {"ticker": None, "weight": 0.1}


def ago(days: int) -> date:
    return datetime.now(timezone.utc).date() - timedelta(days=days)


def rows(day: date, *tickers: str) -> list:
    return [holding(t, 1.0, day=day.isoformat()) for t in tickers]


def fund(symbol: str, day: date, *tickers: str) -> FakeResponse:
    """One fund's body: a ticker row per ticker on `day`, plus a cash row."""
    return FakeResponse(
        body={"symbol": symbol, "holdings": [*rows(day, *tickers), CASH]}
    )


def collect(overrides):
    return collectors.collect_ark(ark_session(overrides), sleep=NO_SLEEP)


def fresh_except(symbol: str, day: date):
    """Three funds dated today with one ticker each; `symbol` dated `day`."""
    return {f: fund(f, day if f == symbol else ago(0), f"ZZ{f[-1]}1") for f in FUNDS}


def staging_shape():
    """ARKW 20 days old, the others today. ZZSTALE is held only by ARKW; ZZBOTH by ARKW and ARKK."""
    return {
        "ARKK": fund("ARKK", ago(0), "ZZBOTH", "ZZK1"),
        "ARKQ": fund("ARKQ", ago(0), "ZZQ1"),
        "ARKG": fund("ARKG", ago(0), "ZZG1"),
        "ARKW": fund("ARKW", ago(20), "ZZBOTH", "ZZSTALE"),
    }


def tickers(outcome):
    return {r["ticker"]: r for r in outcome.records}


def excluded(outcome):
    return outcome.envelope["excluded_stale_funds"]


# ── 1-3: the staging shape ───────────────────────────────


def test_staging_shape_excludes_the_stale_fund_and_dates_the_batch_today():
    outcome = collect(staging_shape())
    assert outcome.succeeded, outcome.error_code
    assert outcome.source_as_of == ago(0)
    assert excluded(outcome) == [f"ARKW:{ago(20).isoformat()}"]


def test_staging_shape_records_come_from_the_three_kept_funds_only():
    outcome = collect(staging_shape())
    ark = collectors._fetcher("ark")
    kept = {
        "ARKK": [*rows(ago(0), "ZZBOTH", "ZZK1"), CASH],
        "ARKQ": [*rows(ago(0), "ZZQ1"), CASH],
        "ARKG": [*rows(ago(0), "ZZG1"), CASH],
    }
    assert outcome.records == ark.parse_holdings(kept)
    assert all("ARKW" not in r["funds"] for r in outcome.records)


def test_a_ticker_held_only_by_the_stale_fund_disappears():
    assert "ZZSTALE" not in tickers(collect(staging_shape()))


def test_a_ticker_held_by_a_stale_and_a_fresh_fund_counts_the_fresh_one_only():
    both = tickers(collect(staging_shape()))["ZZBOTH"]
    assert both["funds"] == ["ARKK"]
    assert both["fund_count"] == 1
    assert both["total_weight"] == 1.0


def test_the_envelope_sanitizer_keeps_the_excluded_funds():
    outcome = collect(staging_shape())
    clean = sanitize_envelope("ark_holdings", outcome.envelope)
    assert clean["excluded_stale_funds"] == [f"ARKW:{ago(20).isoformat()}"]


def test_the_envelope_counts_describe_the_kept_records():
    envelope = collect(staging_shape()).envelope
    assert envelope["total_tickers"] == 4  # ZZBOTH, ZZK1, ZZQ1, ZZG1
    assert envelope["multi_fund_tickers"] == 0
    assert envelope["single_fund_tickers"] == 4


def test_the_stalest_kept_fund_dates_the_batch():
    overrides = staging_shape()
    overrides["ARKQ"] = fund("ARKQ", ago(5), "ZZQ1")
    outcome = collect(overrides)
    assert outcome.source_as_of == ago(5)
    assert excluded(outcome) == [f"ARKW:{ago(20).isoformat()}"]


def test_every_stale_fund_is_named_with_its_own_date():
    overrides = staging_shape()
    overrides["ARKQ"] = fund("ARKQ", ago(9), "ZZQ1")
    outcome = collect(overrides)
    assert outcome.succeeded, outcome.error_code
    assert sorted(excluded(outcome)) == sorted(
        [f"ARKQ:{ago(9).isoformat()}", f"ARKW:{ago(20).isoformat()}"]
    )
    assert "ZZQ1" not in tickers(outcome)
    assert outcome.source_as_of == ago(0)


# ── The limit: exactly publish's ─────────────────────────


@pytest.mark.parametrize("days,kept", [(0, True), (7, True), (8, False), (30, False)])
def test_the_boundary_matches_publish(days, kept):
    """publish refuses day < (now - 7 days).date(): 7 days old is kept, 8 is not."""
    outcome = collect(fresh_except("ARKG", ago(days)))
    assert outcome.succeeded, outcome.error_code
    if kept:
        assert outcome.source_as_of == ago(days)
        assert excluded(outcome) == []
        assert "ZZG1" in tickers(outcome)
    else:
        assert outcome.source_as_of == ago(0)
        assert excluded(outcome) == [f"ARKG:{ago(days).isoformat()}"]
        assert "ZZG1" not in tickers(outcome)


def test_the_collector_limit_is_publishs_limit():
    assert collectors.ARK_MAX_AGE == timedelta(days=7)
    assert publication.ARK_MAX_AGE is collectors.ARK_MAX_AGE


def test_the_collector_reads_the_shared_limit(monkeypatch):
    monkeypatch.setattr(collectors, "ARK_MAX_AGE", timedelta(days=3))
    outcome = collect(fresh_except("ARKG", ago(4)))
    assert excluded(outcome) == [f"ARKG:{ago(4).isoformat()}"]
    assert collect(fresh_except("ARKG", ago(3))).source_as_of == ago(3)


@pytest.mark.db
def test_publish_accepts_what_the_collector_keeps_and_refuses_one_day_older(
    engine, monkeypatch
):
    def check(day):
        records = collect({}).records
        outcome = CollectionOutcome.success(
            "ark_holdings",
            "live",
            records,
            source_as_of=day,
            coverage={"complete": True, "scope": "snapshot"},
        )
        result = import_collection(engine, outcome)
        assert result.succeeded, result.error_code
        with engine.connect() as conn:
            publication._check_run(
                conn,
                "ark_holdings",
                result.run_id,
                datetime.now(timezone.utc),
                timedelta(days=3),
            )

    check(ago(7))
    with pytest.raises(publication.PublicationError, match="ARK observation"):
        check(ago(8))
    # publish reads the shared limit too
    monkeypatch.setattr(publication, "ARK_MAX_AGE", timedelta(days=3))
    with pytest.raises(publication.PublicationError, match="ARK observation"):
        check(ago(4))


# ── 4: no fresh fund ─────────────────────────────────────


def all_stale(days=8):
    return {f: fund(f, ago(days + i), f"ZZ{f[-1]}1") for i, f in enumerate(FUNDS)}


def test_all_funds_stale_fails_with_a_fixed_code():
    outcome = collect(all_stale())
    assert not outcome.succeeded
    assert outcome.error_code == "ark_funds_stale"
    assert outcome.error_code in ERROR_SUMMARIES
    assert not outcome.records


def test_all_funds_stale_names_each_fund_in_safe_diagnostics():
    outcome = collect(all_stale())
    safe = safe_diagnostics(outcome.diagnostics)
    assert safe is not None
    assert sorted(f["unit"] for f in safe["failures"]) == sorted(FUNDS)
    assert {f["code"] for f in safe["failures"]} == {"stale_fund"}
    summary = render_summary(outcome.error_code, safe)
    assert "ARKW (stale_fund)" in summary


@pytest.mark.db
def test_all_funds_stale_records_a_failed_run_and_keeps_the_previous_batch(engine):
    from src.db.queries import latest_successful_run

    good = import_collection(engine, collect({}))
    assert good.succeeded
    failed = import_collection(engine, collect(all_stale()))
    assert failed.status == "failed"
    assert failed.error_code == "ark_funds_stale"
    assert failed.batch_id is None
    with engine.connect() as conn:
        assert latest_successful_run(conn, "ark_holdings").batch_id == good.batch_id


# ── 5: unchanged rules ───────────────────────────────────


def test_all_fresh_funds_are_unchanged():
    outcome = collect({})
    assert outcome.succeeded
    assert excluded(outcome) == []
    assert outcome.source_as_of == ago(1)
    zzaa = tickers(outcome)["ZZAA"]
    assert zzaa["fund_count"] == 4 and zzaa["total_weight"] == 10.0


@pytest.mark.parametrize("bad_day", [None, "09/30/2026", "2026-02-30"])
def test_an_unknown_fund_still_makes_the_batch_unknown_beside_a_stale_one(bad_day):
    overrides = staging_shape()
    unknown = [*rows(ago(0), "ZZG1"), {**holding("ZZG2"), "date": bad_day}, CASH]
    overrides["ARKG"] = FakeResponse(body={"symbol": "ARKG", "holdings": unknown})
    outcome = collect(overrides)
    assert outcome.succeeded, outcome.error_code
    assert outcome.source_as_of is None


def test_a_future_fund_still_is_not_hidden_beside_a_stale_one():
    overrides = staging_shape()
    overrides["ARKG"] = fund("ARKG", ago(-3), "ZZG1")
    outcome = collect(overrides)
    assert outcome.source_as_of is None or outcome.source_as_of > ago(0)


def test_cash_rows_dont_date_a_fund():
    overrides = staging_shape()
    # A stale fund whose cash row is dated today is still stale.
    overrides["ARKW"] = FakeResponse(
        body={
            "symbol": "ARKW",
            "holdings": [
                *rows(ago(20), "ZZSTALE"),
                {**CASH, "date": ago(0).isoformat()},
            ],
        }
    )
    # A fresh fund whose cash row is a year old is still fresh.
    overrides["ARKQ"] = FakeResponse(
        body={
            "symbol": "ARKQ",
            "holdings": [*rows(ago(0), "ZZQ1"), {**CASH, "date": ago(365).isoformat()}],
        }
    )
    outcome = collect(overrides)
    assert excluded(outcome) == [f"ARKW:{ago(20).isoformat()}"]
    assert "ZZQ1" in tickers(outcome) and "ZZSTALE" not in tickers(outcome)


def test_a_fetch_failure_still_fails_as_before_beside_a_stale_fund():
    # Funds are fetched in ARK_FUNDS order (ARKK, ARKQ, ...) and the loop stops
    # at the first failure, so the stale fund must come before the broken one.
    broken = FakeResponse(status=500, body={})
    baseline = collect({"ARKQ": broken})
    overrides = staging_shape()
    overrides["ARKK"] = fund("ARKK", ago(20), "ZZBOTH", "ZZK1")
    overrides["ARKQ"] = broken
    outcome = collect(overrides)
    assert not outcome.succeeded
    assert outcome.error_code == baseline.error_code != "ark_funds_stale"
    assert outcome.diagnostics["failures"] == baseline.diagnostics["failures"]


# ── Batch identity ───────────────────────────────────────


def test_identical_collections_with_an_excluded_fund_are_identical():
    first, second = collect(staging_shape()), collect(staging_shape())
    assert first.records == second.records
    assert first.source_as_of == second.source_as_of
    drop = lambda e: {k: v for k, v in e.items() if k != "fetched_at"}  # noqa: E731
    assert drop(first.envelope) == drop(second.envelope)


@pytest.mark.db
def test_identical_collections_with_an_excluded_fund_dedupe_to_one_batch(engine):
    from sqlalchemy import text

    first = import_collection(engine, collect(staging_shape()))
    second = import_collection(engine, collect(staging_shape()))
    assert first.succeeded and second.succeeded
    assert first.batch_id == second.batch_id
    with engine.connect() as conn:
        payload_envelope, as_of = conn.execute(
            text(
                "SELECT payload -> 'envelope', source_as_of"
                " FROM perennial.source_batches WHERE id = :i"
            ),
            {"i": first.batch_id},
        ).one()
    assert as_of == ago(0)
    assert payload_envelope["excluded_stale_funds"] == [f"ARKW:{ago(20).isoformat()}"]
