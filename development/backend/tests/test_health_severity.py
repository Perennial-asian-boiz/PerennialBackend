"""M2 health severities: degraded is SEV3, a missed D6 deadline is SEV2.

Synthetic fixtures; `now` is passed explicitly so the trading calendar is fixed.
"""

import json
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import update

from src.db import models
from src.pipeline.health import health, publication_window_start
from src.pipeline.publication import current
from test_degraded_publishing import Feed, failing_market_caps, run

PT = ZoneInfo("America/Los_Angeles")


def at(day, hour, minute=0):
    return datetime(*day, hour, minute, tzinfo=PT)


@pytest.mark.parametrize(
    "now,expected_day",
    [
        (at((2026, 10, 21), 11), date(2026, 10, 21)),  # Wednesday after 10:30
        (at((2026, 10, 21), 10, 29), date(2026, 10, 20)),  # before the deadline
        (at((2026, 10, 24), 12), date(2026, 10, 23)),  # Saturday -> Friday
        (at((2026, 10, 26), 9), date(2026, 10, 23)),  # Monday morning -> Friday
        (at((2026, 11, 26), 12), date(2026, 11, 25)),  # Thanksgiving -> Wednesday
        (at((2026, 11, 27), 11), date(2026, 11, 27)),  # Friday after Thanksgiving
    ],
)
def test_publication_window_follows_d6_and_the_trading_calendar(now, expected_day):
    from src.settings import TRADING_HOLIDAYS

    start = publication_window_start(now, TRADING_HOLIDAYS)
    assert start.astimezone(PT).date() == expected_day
    assert (start.astimezone(PT).hour, start.astimezone(PT).minute) == (0, 0)


def _created_at(engine, when):
    with engine.begin() as conn:
        conn.execute(
            update(models.publications)
            .where(models.publications.c.id == current(engine)["id"])
            .values(created_at=when)
        )


@pytest.mark.db
def test_missed_publication_is_sev2_and_weekends_are_not_missed(engine, fixture_path):
    run(engine, Feed(fixture_path))
    _created_at(engine, at((2026, 10, 22), 9, 5).astimezone(timezone.utc))  # Thursday

    friday_late = health(engine, now=at((2026, 10, 23), 11).astimezone(timezone.utc))
    assert "publication:missed" in friday_late["issues"]
    assert friday_late["issue_severity"]["publication:missed"] == "SEV2"
    assert friday_late["severity"] == "SEV2"

    friday_early = health(engine, now=at((2026, 10, 23), 10).astimezone(timezone.utc))
    assert "publication:missed" not in friday_early["issues"]

    _created_at(engine, at((2026, 10, 23), 9, 5).astimezone(timezone.utc))  # Friday
    saturday = health(engine, now=at((2026, 10, 24), 12).astimezone(timezone.utc))
    assert "publication:missed" not in saturday["issues"]


@pytest.mark.db
def test_degraded_publication_is_sev3_including_the_covered_failed_source(
    engine, fixture_path
):
    feed = Feed(fixture_path)
    run(engine, feed)
    feed.fail = {"ark_holdings"}
    run(engine, feed)
    report = health(engine)
    assert "publication:degraded" in report["issues"]
    assert report["issue_severity"]["publication:degraded"] == "SEV3"
    assert report["issue_severity"]["ark_holdings:failed"] == "SEV3"
    assert set(report["issue_severity"].values()) == {"SEV3"}
    assert report["severity"] == "SEV3"
    assert report["healthy"] is False  # Q4: SEV3 still exits non-zero


@pytest.mark.db
def test_all_sources_reused_is_named_in_health(engine, fixture_path):
    feed = Feed(fixture_path)
    run(engine, feed)
    feed.change("congress_trades")
    feed.fail = {"insider_trades"}
    run(engine, feed)
    report = health(engine)
    assert "publication:degraded:all_sources_reused" in report["issues"]
    assert report["issue_severity"]["publication:degraded:all_sources_reused"] == "SEV3"


@pytest.mark.db
def test_failed_source_without_cover_is_sev2(engine, fixture_path):
    from src.ingestion.importer import CollectionOutcome, import_collection

    run(engine, Feed(fixture_path))
    import_collection(engine, CollectionOutcome.failure("ark_holdings", "live", "collection_failed"))
    with engine.begin() as conn:
        conn.execute(
            update(models.ingestion_runs)
            .where(models.ingestion_runs.c.source == "ark_holdings",
                   models.ingestion_runs.c.status == "succeeded")
            .values(collected_at=datetime.now(timezone.utc) - timedelta(days=4))
        )
    report = health(engine)
    assert report["issue_severity"]["ark_holdings:failed"] == "SEV2"
    assert report["severity"] == "SEV2"


@pytest.mark.db
def test_cli_health_reports_severity_and_exits_1_when_degraded(
    engine, db_url, fixture_path, monkeypatch, capsys
):
    from src.ingestion import cli

    feed = Feed(fixture_path)
    run(engine, feed)
    run(engine, feed, market_cap_collector=failing_market_caps)
    monkeypatch.setenv("DATABASE_URL", db_url.render_as_string(hide_password=False))
    assert cli.main(["health"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["severity"] == "SEV3"
    assert report["issue_severity"] == {"publication:degraded": "SEV3"}
    assert report["issues"] == ["publication:degraded"]
