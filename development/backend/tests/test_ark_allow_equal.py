"""Decision 87 (a): ARK's quality threshold allows an equal as-of, regression test.

Checker R1 on #70: since the ARK batch is dated by its stalest kept fund, a day
when one fund lags while holdings change gives the same source_as_of as the
previous batch, and the quality check refused it (allow_equal_as_of=False for
ark_holdings only). Bryan approved option (a) on 2026-10-28: ark_holdings gets
allow_equal_as_of=True, as the other sources have.

Unchanged: an older or unknown as-of still fails the quality check, and
publish's own ARK check still refuses a batch more than 7 days old or with no
date (test_ark_stale_funds.py pins the boundary and the stale-fund exclusion).
"""

from datetime import date, datetime, timedelta, timezone

import pytest
from src.pipeline.publication import _quality_failures
from src.settings import QualityThreshold, _quality_defaults

DAY = date(2026, 10, 20)
BEFORE = [{"ticker": f"ZZ{i:02d}", "share_price": 10.0} for i in range(10)]
CHANGED = [{"ticker": f"ZZ{i:02d}", "share_price": 11.0} for i in range(2, 12)]


def ark():
    return _quality_defaults()["ark_holdings"]


def test_ark_allows_an_equal_as_of():
    assert ark().allow_equal_as_of is True


def test_the_rest_of_arks_threshold_is_unchanged():
    defaults = QualityThreshold()
    assert ark().null_fields == ("share_price",)
    assert (ark().min_count_ratio, ark().max_count_ratio, ark().null_share_margin) == (
        defaults.min_count_ratio,
        defaults.max_count_ratio,
        defaults.null_share_margin,
    )


def test_the_other_sources_are_unchanged():
    thresholds = _quality_defaults()
    assert set(thresholds) == {
        "congress_trades",
        "ark_holdings",
        "insider_trades",
        "short_interest",
    }
    assert all(
        t.allow_equal_as_of for s, t in thresholds.items() if s != "ark_holdings"
    )
    assert thresholds["congress_trades"].null_fields == ("disclosure_date",)
    assert thresholds["insider_trades"].null_fields == ("shares", "value")
    assert thresholds["short_interest"].null_fields == (
        "short_interest_shares",
        "average_daily_volume",
    )


def test_changed_holdings_on_the_same_as_of_pass_the_quality_check():
    assert _quality_failures(BEFORE, CHANGED, DAY, DAY, ark()) == []


def test_an_older_as_of_still_fails():
    issues = _quality_failures(BEFORE, CHANGED, DAY, DAY - timedelta(days=1), ark())
    assert "source_as_of" in issues


def test_an_unknown_as_of_still_fails():
    assert "source_as_of" in _quality_failures(BEFORE, CHANGED, DAY, None, ark())


def test_a_newer_as_of_still_passes():
    assert _quality_failures(BEFORE, CHANGED, DAY, DAY + timedelta(days=1), ark()) == []


@pytest.mark.db
@pytest.mark.parametrize("days_old", [8, None], ids=["stale", "unknown"])
def test_publishs_ark_check_still_refuses_stale_or_unknown(engine, days_old):
    from src.ingestion import collectors
    from src.ingestion.importer import CollectionOutcome, import_collection
    from src.pipeline import publication
    from test_collectors import NO_SLEEP, ark_session

    now = datetime.now(timezone.utc)
    day = None if days_old is None else now.date() - timedelta(days=days_old)
    records = collectors.collect_ark(ark_session(), sleep=NO_SLEEP).records
    result = import_collection(
        engine,
        CollectionOutcome.success(
            "ark_holdings",
            "live",
            records,
            source_as_of=day,
            coverage={"complete": True, "scope": "snapshot"},
        ),
    )
    assert result.succeeded, result.error_code
    with (
        engine.connect() as conn,
        pytest.raises(publication.PublicationError, match="ARK observation"),
    ):
        publication._check_run(
            conn, "ark_holdings", result.run_id, now, timedelta(days=3)
        )
