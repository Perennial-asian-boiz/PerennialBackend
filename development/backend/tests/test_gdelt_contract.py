"""GDELT through the ingestion contract (design note PR 2). Synthetic client only."""

import secrets
from datetime import date, datetime, timedelta, timezone

import pytest
from alembic import command
from sqlalchemy import select, text

from src.db import models
from src.db.queries import batch_rows, latest_successful_run
from src.pipeline.publication import current
from src.services.gdelt import contract
from src.services.gdelt.client import Article, DayVolume, GdeltError
from src.services.gdelt.contract import SpikeRule, derive_spikes, run_contract
from src.services.gdelt.themes import ThemeMap

NOW = datetime(2026, 10, 14, 16, 30, tzinfo=timezone.utc)
DAY = date(2026, 10, 13)  # target day: the last complete UTC day before NOW
THEMES = ThemeMap({"ECON_OILPRICE": {}, "ECON_INFLATION": {}}, ("reuters.com",))


def counts(target, baseline=30, days=89, gaps=()):
    """A full 90-day window (89 baseline days plus the target day), as GDELT serves."""
    out = {DAY - timedelta(days=n): baseline for n in range(1, days + 1)}
    out[DAY] = target
    for gap in gaps:
        out.pop(DAY - timedelta(days=gap), None)
    return out


class Client:
    def __init__(self, volumes, failing=(), articles=None):
        self.volumes, self.failing = volumes, set(failing)
        self._articles = articles

    def volume(self, theme):
        if theme in self.failing:
            raise GdeltError("http_429")
        return {d: DayVolume(c, 10_000) for d, c in self.volumes[theme].items()}

    def tone(self, theme):
        return {d: -1.23456 for d in self.volumes[theme]}

    def articles(self, theme, day):
        if self._articles is not None:
            return self._articles
        return [
            Article(f"https://www.reuters.com/{theme}/{i}", f"t{i}", "reuters.com", None)
            for i in range(7)
        ] + [Article("https://spam.example/x", "x", "spam.example", None)]


def client(oil=90, inflation=31, **kwargs):
    return Client({"ECON_OILPRICE": counts(oil), "ECON_INFLATION": counts(inflation)}, **kwargs)


def runs(engine, source):
    with engine.connect() as conn:
        return conn.execute(
            select(models.ingestion_runs).where(models.ingestion_runs.c.source == source)
            .order_by(models.ingestion_runs.c.id)
        ).mappings().all()


# ── collected sources ──


@pytest.mark.db
def test_volume_and_articles_import_as_batches_without_securities(engine):
    run = run_contract(engine, client(), THEMES, NOW)
    assert run.succeeded and run.day == DAY
    volume = run.results["gdelt_theme_volume"]
    with engine.connect() as conn:
        rows = batch_rows(conn, "gdelt_theme_volume", volume.batch_id)
        batch = conn.execute(
            select(models.source_batches).where(models.source_batches.c.id == volume.batch_id)
        ).mappings().one()
        assert conn.execute(select(models.securities)).all() == []
    assert {r["theme"] for r in rows} == {"ECON_OILPRICE", "ECON_INFLATION"}
    assert len(rows) == 2 * 90 and max(r["day"] for r in rows) == DAY
    assert str(rows[0]["avg_tone"]) == "-1.2346"
    assert batch["source_as_of"] == DAY
    envelope = batch["payload"]["envelope"]
    assert envelope["target_day"] == DAY.isoformat() and envelope["missing_days_count"] == 0
    [volume_run] = runs(engine, "gdelt_theme_volume")
    assert volume_run["coverage"] == {"complete": True, "scope": "window"}
    assert volume_run["collection_mode"] == "live"
    articles = run.results["gdelt_theme_articles"]
    with engine.connect() as conn:
        evidence = batch_rows(conn, "gdelt_theme_articles", articles.batch_id)
    assert len(evidence) == 2 * contract.EVIDENCE_PER_DAY
    assert all(r["domain"] == "reuters.com" for r in evidence)


@pytest.mark.db
def test_missing_timeline_days_are_coverage_gaps_never_zero_rows(engine):
    gappy = Client({"ECON_OILPRICE": counts(90, gaps=(3, 4)), "ECON_INFLATION": counts(31)})
    run = run_contract(engine, gappy, THEMES, NOW)
    with engine.connect() as conn:
        rows = batch_rows(conn, "gdelt_theme_volume", run.results["gdelt_theme_volume"].batch_id)
        envelope = conn.execute(
            select(models.source_batches.c.payload).where(
                models.source_batches.c.id == run.results["gdelt_theme_volume"].batch_id
            )
        ).scalar_one()["envelope"]
    oil_days = {r["day"] for r in rows if r["theme"] == "ECON_OILPRICE"}
    assert DAY - timedelta(days=3) not in oil_days
    assert all(r["article_count"] > 0 for r in rows)  # no zero-filled gap rows
    assert envelope["missing_days"] == [
        f"ECON_OILPRICE:{DAY - timedelta(days=4)}",
        f"ECON_OILPRICE:{DAY - timedelta(days=3)}",
    ]


@pytest.mark.db
def test_any_failed_theme_fails_the_batch_and_keeps_the_last_good_one(engine):
    good = run_contract(engine, client(), THEMES, NOW)
    bad = run_contract(engine, client(failing={"ECON_INFLATION"}), THEMES, NOW)
    assert not bad.succeeded
    failed = bad.results["gdelt_theme_volume"]
    assert failed.error_code == "partial_collection"
    with engine.connect() as conn:
        latest = latest_successful_run(conn, "gdelt_theme_volume")
    assert latest.batch_id == good.results["gdelt_theme_volume"].batch_id
    diagnostics = runs(engine, "gdelt_theme_volume")[-1]["diagnostics"]
    assert diagnostics["unit_kind"] == "GDELT themes"
    assert diagnostics["failures"] == [{"unit": "ECON_INFLATION", "code": "http_429"}]
    # The derivation still runs, from the last good volume batch.
    assert bad.results["gdelt_spike_results"].succeeded


@pytest.mark.db
def test_identical_recollection_reuses_batches(engine):
    first = run_contract(engine, client(), THEMES, NOW)
    second = run_contract(engine, client(), THEMES, NOW)
    for source in ("gdelt_theme_volume", "gdelt_theme_articles", "gdelt_spike_results"):
        assert second.results[source].batch_id == first.results[source].batch_id
        assert second.results[source].reused_batch is True


@pytest.mark.db
def test_unusable_evidence_links_are_skipped_not_fatal(engine):
    links = [
        Article("https://www.reuters.com/a#frag", "a", "reuters.com", None),
        Article("https://www.reuters.com/b?apikey=SYNTHETIC", "b", "reuters.com", None),
        Article("https://www.reuters.com/c", "c", "reuters.com", None),
    ]
    run = run_contract(engine, client(articles=links), THEMES, NOW)
    result = run.results["gdelt_theme_articles"]
    assert result.succeeded
    with engine.connect() as conn:
        rows = batch_rows(conn, "gdelt_theme_articles", result.batch_id)
        envelope = conn.execute(
            select(models.source_batches.c.payload).where(models.source_batches.c.id == result.batch_id)
        ).scalar_one()["envelope"]
    assert {r["url"] for r in rows} == {"https://www.reuters.com/c"}
    assert envelope["skipped_articles"] == 4  # two per theme
    assert "SYNTHETIC" not in str(envelope)


# ── the spike derivation ──


@pytest.mark.db
def test_derivation_records_lineage_mode_and_rule(engine):
    run = run_contract(engine, client(), THEMES, NOW)
    spikes = run.results["gdelt_spike_results"]
    [derived] = runs(engine, "gdelt_spike_results")
    assert derived["collection_mode"] == "derived"
    assert derived["coverage"] == {"complete": True, "scope": "derivation"}
    with engine.connect() as conn:
        deps = dict(conn.execute(
            select(models.run_dependencies.c.source, models.run_dependencies.c.batch_id)
            .where(models.run_dependencies.c.run_id == derived["id"])
        ).all())
        envelope = conn.execute(
            select(models.source_batches.c.payload).where(models.source_batches.c.id == spikes.batch_id)
        ).scalar_one()["envelope"]
        # G-c: the latest-run reader returns derived runs.
        assert latest_successful_run(conn, "gdelt_spike_results").run_id == derived["id"]
    assert deps == {"gdelt_theme_volume": run.results["gdelt_theme_volume"].batch_id}
    assert envelope == {**SpikeRule().envelope(), "input_batch_id": deps["gdelt_theme_volume"]}
    assert run.events == ["ECON_OILPRICE"]  # 90 vs median 30; inflation 31 is not


@pytest.mark.parametrize(
    "target,gaps,expect",
    [
        (60, (), True),  # exactly 2.0x the median, at least 20 articles
        (59, (), False),
        (19, (), False),  # 2x of a small median but under 20 articles
        (90, tuple(range(1, 15)), None),  # 14 gaps leave 14 observed days: judged
        (90, tuple(range(1, 16)), None),  # 15 gaps leave 13: unknown, not "no spike"
    ],
)
def test_spike_v2_boundaries_and_missing_days(target, gaps, expect):
    baseline = 30 if target >= 20 else 9
    result = SpikeRule().check(counts(target, baseline=baseline, gaps=gaps), DAY)
    if gaps and len(gaps) == 14:
        assert result["observed_baseline_days"] == 14 and result["is_spike"] is True
    else:
        assert result["is_spike"] is expect


def test_spike_v2_treats_missing_days_as_unknown_not_zero():
    # v1 counted 10 missing days as 0, pulling the median to 0 and calling 25 a
    # spike; v2 uses only observed days (median 30), so 25 is not a spike.
    result = SpikeRule().check(counts(25, baseline=30, gaps=range(1, 11)), DAY)
    assert result["baseline_median"] == 30.0 and result["is_spike"] is False
    assert SpikeRule().check({}, DAY) is None


@pytest.mark.db
def test_derivation_without_a_volume_batch_fails_with_missing_upstream(engine):
    result = derive_spikes(engine)
    assert result.error_code == "missing_upstream"


# ── isolation from the watchlist ──


@pytest.mark.db
def test_gdelt_never_touches_the_watchlist_publication_or_health(engine, fixture_path):
    from src.pipeline.health import health
    from test_pipeline import caps, inputs
    from src.pipeline.publication import publish

    runs_, batches = inputs(engine, fixture_path)
    pub = publish(engine, runs_, caps(engine, batches))
    run_contract(engine, client(failing={"ECON_OILPRICE"}), THEMES, NOW)
    assert current(engine)["id"] == pub
    report = health(engine)
    assert not any("gdelt" in issue for issue in report["issues"])
    assert set(report["sources"]) == set(models.WATCHLIST_SOURCES)


# ── migration downgrade guard ──


@pytest.mark.db
def test_downgrade_refuses_while_gdelt_contract_rows_exist(db_url):
    from conftest import alembic_config, pinned_engine

    server = db_url
    name = f"perennial_test_gdelt_{secrets.token_hex(4)}"
    admin = pinned_engine(server, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    engine = pinned_engine(server.set(database=name))
    try:
        with engine.connect() as conn:
            command.upgrade(alembic_config(conn), "head")
        run_contract(engine, client(), THEMES, NOW)
        with engine.connect() as conn, pytest.raises(Exception, match="orphan lineage"):
            command.downgrade(alembic_config(conn), "0006")
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT count(*) FROM perennial.gdelt_theme_volume")).scalar_one() > 0
    finally:
        engine.dispose()
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


# ── health (G-b, G5) ──


@pytest.mark.db
def test_gdelt_staleness_is_an_advisory_not_a_watchlist_issue(engine):
    from sqlalchemy import update

    from src.pipeline.health import health

    report = health(engine)
    assert set(report["advisories"]) == {f"{s}:stale" for s in models.SUBJECTLESS_SOURCES}
    assert not any("gdelt" in issue for issue in report["issues"])

    run_contract(engine, client(), THEMES, datetime.now(timezone.utc))
    assert health(engine)["advisories"] == []

    with engine.begin() as conn:
        conn.execute(
            update(models.ingestion_runs)
            .where(models.ingestion_runs.c.source == "gdelt_theme_volume")
            .values(
                started_at=datetime.now(timezone.utc) - timedelta(days=2, minutes=2),
                finished_at=datetime.now(timezone.utc) - timedelta(days=2, minutes=1),
            )
        )
    report = health(engine)
    assert report["advisories"] == ["gdelt_theme_volume:stale"]
    assert not any("gdelt" in issue for issue in report["issues"])


@pytest.mark.db
@pytest.mark.parametrize("reported_days,expect_missing", [(10, 80), (0, 90)])
def test_missing_days_cover_the_whole_window(engine, reported_days, expect_missing):
    """GD-1: a sparse start and an empty timeline are gaps, not complete coverage."""
    sparse = {DAY - timedelta(days=n): 30 for n in range(reported_days)}
    gappy = Client({"ECON_OILPRICE": sparse, "ECON_INFLATION": counts(31)})
    run = run_contract(engine, gappy, THEMES, NOW)
    volume = run.results["gdelt_theme_volume"]
    with engine.connect() as conn:
        envelope = conn.execute(
            select(models.source_batches.c.payload).where(
                models.source_batches.c.id == volume.batch_id
            )
        ).scalar_one()["envelope"]
    assert envelope["missing_days_count"] == expect_missing
    assert all(day.startswith("ECON_OILPRICE:") for day in envelope["missing_days"])
    assert envelope["missing_days"][0] == f"ECON_OILPRICE:{DAY - timedelta(days=89)}"


@pytest.mark.db
def test_every_theme_empty_fails_instead_of_a_quiet_success(engine):
    """GD-2: all-empty timelines are a provider outage; the last good batch stays."""
    good = run_contract(engine, client(), THEMES, NOW)
    empty = Client({"ECON_OILPRICE": {}, "ECON_INFLATION": {}})
    run = run_contract(engine, empty, THEMES, NOW)
    assert run.results["gdelt_theme_volume"].error_code == "empty_gdelt_timeline"
    with engine.connect() as conn:
        latest = latest_successful_run(conn, "gdelt_theme_volume")
    assert latest.batch_id == good.results["gdelt_theme_volume"].batch_id


@pytest.mark.db
def test_legacy_0005_tables_are_dropped(engine):
    """PR 3 contract step: only the contract tables hold GDELT data."""
    with engine.connect() as conn:
        for table in ("gdelt_theme_days", "gdelt_articles", "gdelt_spike_checks"):
            assert conn.execute(
                text("SELECT to_regclass(:name)"), {"name": f"perennial.{table}"}
            ).scalar_one() is None

def test_a_gdelt_run_does_not_mask_a_missed_watchlist_run(engine):
    """M3's missed-run monitor counts only watchlist sources (merge with #32)."""
    from src.pipeline.health import health

    run_contract(engine, client(), THEMES, datetime.now(timezone.utc))
    report = health(engine)
    assert "pipeline:missed_run" in report["issues"]
    assert report["advisories"] == []
