"""M4 MC-B: market-cap selection, pinning, replay, demo and health.

Spec: docs/work-items/m4-market-caps-packet.md (MC-B, MCB-*). Synthetic data only.
New interfaces (run_pipeline(market_cap_adapter=...), publish(market_caps=...))
are reached inside each test so the rest of the suite still collects.

Bryan settled Q3-Q5 as recommended (decisions 11-13, 2026-10-23): the latest
row wins (a NULL means unavailable), partial gaps publish as unresolved and only
an all-unavailable candidate set is refused, and the legacy tables stay for
replay with no backfill. Their tests are at the end of this file.
"""

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from functools import partial

import pytest
from sqlalchemy import select, text, update
from src.db import models
from src.ingestion.importer import import_collection
from test_degraded_publishing import (
    THRESHOLDS,
    Feed,
    age_run,
    degraded,
    import_all,
    market_caps,
    pins,
)
from test_market_caps_source import quote, upstream

pytestmark = pytest.mark.db

ADAPTER = "stub_live"
NEW_SHAPE = {"refreshed", "reused", "unavailable", "oldest_age_seconds"}
LEGACY_SHAPE = {"fallback", "oldest_retrieved_at", "unavailable"}
THREE_DAYS = 259200


# ── helpers ──────────────────────────────────────────────


class CapAdapter:
    """A live adapter defined only in tests: a value for every symbol unless overridden."""

    def __init__(self, value="20000000000.00", *, overrides=None, name=ADAPTER,
                 mode="live", error=None):
        self.name, self.collection_mode = name, mode
        self.value, self.overrides, self.error = value, overrides or {}, error
        self.calls = []

    def fetch(self, symbols, *, session=None, sleep=None):
        self.calls.append(list(symbols))
        if self.error is not None:
            raise self.error
        return {
            s: self.overrides[s] if s in self.overrides else quote(self.value)
            for s in symbols
        }


def failing():
    from src.ingestion.market_caps import MarketCapAdapterError

    return CapAdapter(error=MarketCapAdapterError("timeout"))


def new_run(engine, feed, adapter, **kwargs):
    """run_pipeline on the new path, with an injected adapter."""
    from src.pipeline.runner import run_pipeline

    kwargs.setdefault("quality_thresholds", THRESHOLDS)
    return run_pipeline(
        engine, collectors=feed.collectors(), market_cap_adapter=adapter, **kwargs
    )


def legacy_run(engine, feed, **kwargs):
    from test_degraded_publishing import run

    return run(engine, feed, **kwargs)


def settings_with(adapter_name):
    from src.settings import Settings

    return Settings(
        _env_file=None,
        database_url="postgresql+psycopg://user:synthetic@localhost/test",
        market_cap_adapter=adapter_name,
    )


def cap_runs(engine):
    with engine.connect() as conn:
        return conn.execute(
            select(models.ingestion_runs)
            .where(models.ingestion_runs.c.source == "market_caps")
            .order_by(models.ingestion_runs.c.id)
        ).all()


def cap_pins(engine, publication_id):
    """symbol -> pin row (run_id, batch_id, market_cap_id, value)."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT s.symbol, p.run_id, p.batch_id, p.market_cap_id, m.value"
                " FROM perennial.publication_market_cap_rows p"
                " JOIN perennial.securities s ON s.id = p.security_id"
                " LEFT JOIN perennial.market_caps m ON m.id = p.market_cap_id"
                " WHERE p.publication_id = :p"
            ),
            {"p": publication_id},
        ).mappings()
        return {r["symbol"]: dict(r) for r in rows}


def legacy_rows(engine, publication_id):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT count(*) FROM perennial.publication_market_caps"
                " WHERE publication_id = :p"
            ),
            {"p": publication_id},
        ).scalar()


def output(engine, publication_id):
    with engine.connect() as conn:
        return conn.execute(
            select(models.publications.c.output).where(
                models.publications.c.id == publication_id
            )
        ).scalar_one()


def records(doc):
    return {
        r["ticker"]: r
        for b in ("popular_stable", "affordable_growing", "unresolved")
        for r in doc.get(b, [])
    }


def quality_checks(engine, publication_id):
    with engine.connect() as conn:
        return conn.execute(
            select(models.publication_events.c.checks).where(
                models.publication_events.c.publication_id == publication_id,
                models.publication_events.c.kind.in_(["quality_passed", "quality_override"]),
            )
        ).scalar_one()


def assert_replays(engine, publication_id):
    from src.pipeline.publication import replay

    assert json.dumps(replay(engine, publication_id), sort_keys=True) == json.dumps(
        output(engine, publication_id), sort_keys=True
    )


def assert_new_path(engine, publication_id):
    """MCB-4a: exactly one pin per candidate, nothing in the legacy tables."""
    pinned = cap_pins(engine, publication_id)
    assert set(pinned) == set(records(output(engine, publication_id)))
    assert legacy_rows(engine, publication_id) == 0
    return pinned


# ── MCB-1a: no adapter means today's path, unchanged ─────


def test_without_an_adapter_the_legacy_path_runs_and_records_no_market_caps_run(
    engine, fixture_path
):
    calls = []

    def counting(symbols):
        calls.append(tuple(symbols))
        return market_caps(symbols)

    pub = legacy_run(engine, Feed(fixture_path), market_cap_collector=counting,
                     settings=settings_with(None))
    assert len(calls) == 1
    assert cap_runs(engine) == []
    assert legacy_rows(engine, pub) == len(records(output(engine, pub))) > 0
    assert cap_pins(engine, pub) == {}


# ── MCB-2: collection on the new path ────────────────────


def test_one_collection_per_run_after_selection_planned_from_the_ranked_rows(
    engine, fixture_path
):
    adapter = CapAdapter()
    pub = new_run(engine, Feed(fixture_path), adapter)
    assert len(adapter.calls) == 1
    assert adapter.calls[0] == sorted(records(output(engine, pub)))
    (run,) = cap_runs(engine)
    assert run.status == "succeeded" and run.collection_mode == "live"
    with engine.connect() as conn:
        watchlist_runs = conn.execute(
            select(models.ingestion_runs.c.id).where(
                models.ingestion_runs.c.source.in_(models.WATCHLIST_SOURCES)
            )
        ).scalars().all()
    assert run.id > max(watchlist_runs)  # after select_sources
    pinned = assert_new_path(engine, pub)
    assert {p["run_id"] for p in pinned.values()} == {run.id}
    assert_replays(engine, pub)


def test_failed_collection_is_recorded_and_the_publication_uses_earlier_rows(
    engine, fixture_path
):
    """MCB-2b and MCB-2c: the pinned rows' run was planned from other batches."""
    feed = Feed(fixture_path)
    new_run(engine, feed, CapAdapter())
    first_run = cap_runs(engine)[0]
    feed.change("congress_trades")
    feed.change("ark_holdings")
    pub = new_run(engine, feed, failing())
    second_run = cap_runs(engine)[-1]
    assert second_run.status == "failed" and second_run.error_code == "collection_failed"
    pinned = assert_new_path(engine, pub)
    assert {p["run_id"] for p in pinned.values()} == {first_run.id}
    assert_replays(engine, pub)


def test_market_caps_run_records_its_upstream_lineage(engine, fixture_path):
    """MCB-2d: market_caps is downstream, with dependencies and a history watermark."""
    pub = new_run(engine, Feed(fixture_path), CapAdapter())
    (run,) = cap_runs(engine)
    published = pins(engine, pub)
    with engine.connect() as conn:
        deps = {
            r.source: r
            for r in conn.execute(
                select(models.run_dependencies).where(
                    models.run_dependencies.c.run_id == run.id
                )
            )
        }
        insider_through = conn.execute(
            select(models.run_dependencies.c.history_through_id).where(
                models.run_dependencies.c.run_id == published["insider_trades"][0],
                models.run_dependencies.c.source == "congress_trades",
            )
        ).scalar_one()
    assert set(deps) == {"congress_trades", "ark_holdings"}
    assert deps["congress_trades"].batch_id == published["congress_trades"][1]
    assert deps["ark_holdings"].batch_id == published["ark_holdings"][1]
    assert deps["congress_trades"].history_through_id is not None
    assert deps["congress_trades"].history_through_id == insider_through


def test_planned_watermark_counts_only_insider_and_short_interest(engine, fixture_path):
    """MCB-2d guard: a market_caps run's watermark never joins the agreement check."""
    from src.pipeline import congress_history
    from src.pipeline.lifecycle import begin_run

    runs = import_all(engine, Feed(fixture_path))
    with engine.connect() as conn:
        batches = {
            s: conn.execute(
                select(models.ingestion_runs.c.batch_id).where(
                    models.ingestion_runs.c.id == runs[s]
                )
            ).scalar_one()
            for s in ("congress_trades", "ark_holdings")
        }
    downstream = {
        s: begin_run(engine, s, "live", dependencies=batches, history_through=5)
        for s in ("insider_trades", "short_interest")
    }
    cap = begin_run(engine, "market_caps", "live", dependencies=batches, history_through=9)
    with engine.connect() as conn:
        got = congress_history.planned_watermark(
            conn, {**runs, **downstream, "market_caps": cap}
        )
    assert got == 5


def test_live_pipeline_refuses_the_fixture_adapter_before_collecting(engine, fixture_path):
    """MCB-2e: injected or configured, before any collection."""
    from src.ingestion.market_caps import FixtureMarketCapAdapter
    from src.pipeline.runner import run_pipeline

    feed = Feed(fixture_path)
    with pytest.raises(ValueError):
        run_pipeline(engine, collectors=feed.collectors(),
                     market_cap_adapter=FixtureMarketCapAdapter())
    with pytest.raises(ValueError):
        run_pipeline(engine, collectors=feed.collectors(), quality_thresholds=THRESHOLDS,
                     settings=settings_with("fixture"))
    assert feed.calls == []
    with engine.connect() as conn:
        assert conn.execute(select(models.ingestion_runs.c.id)).first() is None


def test_collect_source_refuses_the_fixture_adapter_in_live_mode(engine, fixture_path, monkeypatch):
    """MCB-2f: market_caps plans from the stored upstream; fetch is never called."""
    from src.ingestion.market_caps import FixtureMarketCapAdapter, collect
    from src.pipeline.runner import collect_source

    import_all(engine, Feed(fixture_path))  # live Congress and ARK runs to plan from
    calls = []
    monkeypatch.setattr(FixtureMarketCapAdapter, "fetch", lambda self, *a, **k: calls.append(a))
    result = collect_source(engine, "market_caps",
                            collector=partial(collect, FixtureMarketCapAdapter()),
                            record_mode="live")
    assert not result.succeeded and result.error_code == "fixture_adapter_not_allowed"
    assert calls == []
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM perennial.market_caps")).scalar() == 0


@pytest.mark.parametrize("record_mode", ["fixture", "file", "derived"])
def test_collect_refuses_a_live_adapter_outside_live_recording(record_mode):
    """Auditor MC-A note 2: the lowest layer is symmetric; live only for live."""
    from src.ingestion.market_caps import collect

    adapter = CapAdapter()
    outcome = collect(adapter, upstream=upstream(holdings=["ZZAA"]), record_mode=record_mode)
    assert not outcome.succeeded
    assert outcome.error_code == "adapter_mode_mismatch"
    assert adapter.calls == []


# ── MCB-3a/3b: selection at publish (publish(market_caps="pinned")) ─


# Every ticker in the pipeline fixtures. publish() called after import_all() ranks
# only the ARK ones (ZZAA, ZZDD, ZZEE): no Congress history is folded there.
CANDIDATES = ("ZZAA", "ZZB.B", "ZZCC", "ZZDD", "ZZEE")


def cap_run(engine, values, *, name=ADAPTER, mode="live", age=None, observed_at=None):
    """One collected market_caps run for exactly these symbols."""
    from src.ingestion.market_caps import collect

    adapter = CapAdapter(
        name=name,
        mode=mode,
        overrides={
            s: None if v is None else quote(v, observed_at=observed_at) for s, v in values.items()
        },
    )
    outcome = collect(adapter, upstream=upstream(holdings=sorted(values)), record_mode=mode)
    result = import_collection(engine, replace(outcome, mode=mode))
    assert result.succeeded, result.error_code
    if age is not None:
        age_run(engine, result.run_id, age)
    return result


def publish_pinned(engine, runs):
    from src.pipeline.publication import publish

    return publish(engine, runs, None, market_caps="pinned", quality_thresholds=THRESHOLDS)


def every(value):
    return dict.fromkeys(CANDIDATES, value)


def test_the_latest_collected_row_is_selected(engine, fixture_path):
    runs = import_all(engine, Feed(fixture_path))
    cap_run(engine, every("1000.00"), age=timedelta(hours=2))
    newer = cap_run(engine, every("2000.00"), age=timedelta(hours=1))
    pub = publish_pinned(engine, runs)
    pinned = assert_new_path(engine, pub)
    assert {p["run_id"] for p in pinned.values()} == {newer.run_id}
    assert {r["market_cap"] for r in records(output(engine, pub)).values()} == {2000}
    assert_replays(engine, pub)


def test_ties_on_collected_at_go_to_the_higher_run_id(engine, fixture_path):
    runs = import_all(engine, Feed(fixture_path))
    first = cap_run(engine, every("2000.00"))
    second = cap_run(engine, every("1000.00"))
    same = datetime.now(timezone.utc) - timedelta(hours=1)
    with engine.begin() as conn:
        conn.execute(
            update(models.ingestion_runs)
            .where(models.ingestion_runs.c.id.in_([first.run_id, second.run_id]))
            .values(collected_at=same)
        )
    pinned = cap_pins(engine, publish_pinned(engine, runs))
    assert {p["run_id"] for p in pinned.values()} == {max(first.run_id, second.run_id)}


@pytest.mark.parametrize(
    "age, eligible",
    [(timedelta(days=3) - timedelta(minutes=1), True),
     (timedelta(days=3) + timedelta(minutes=1), False)],
)
def test_rows_are_eligible_up_to_three_days(engine, fixture_path, age, eligible):
    """MCB-3b boundary, one minute either side."""
    runs = import_all(engine, Feed(fixture_path))
    cap_run(engine, {s: "20000000000.00" for s in CANDIDATES if s != "ZZAA"})
    aged = cap_run(engine, {"ZZAA": "30000000000.00"}, age=age)
    pinned = cap_pins(engine, publish_pinned(engine, runs))
    if eligible:
        assert pinned["ZZAA"]["run_id"] == aged.run_id
    else:
        assert (pinned["ZZAA"]["run_id"], pinned["ZZAA"]["market_cap_id"]) == (None, None)


def test_a_reused_batch_is_aged_from_the_latest_run_that_retrieved_it(engine, fixture_path):
    runs = import_all(engine, Feed(fixture_path))
    old = cap_run(engine, every("1000.00"), age=timedelta(days=4))
    again = cap_run(engine, every("1000.00"), age=timedelta(hours=1))
    assert again.batch_id == old.batch_id and again.reused_batch is True
    pinned = cap_pins(engine, publish_pinned(engine, runs))
    assert {(p["run_id"], p["batch_id"]) for p in pinned.values()} == {
        (again.run_id, old.batch_id)
    }


def test_live_publications_never_select_fixture_mode_rows(engine, fixture_path):
    runs = import_all(engine, Feed(fixture_path))
    live = cap_run(engine, every("1000.00"), age=timedelta(hours=2))
    cap_run(engine, every("2000.00"), name="fixture", mode="fixture")
    pinned = cap_pins(engine, publish_pinned(engine, runs))
    assert {p["run_id"] for p in pinned.values()} == {live.run_id}


@pytest.mark.parametrize("observed", ["stale", "future"])
def test_rows_observed_outside_three_days_are_not_eligible(engine, fixture_path, observed):
    """MCB-3a: observed_at within 3 days and not in the future."""
    runs = import_all(engine, Feed(fixture_path))
    cap_run(engine, {s: "20000000000.00" for s in CANDIDATES if s != "ZZAA"})
    bad = cap_run(engine, {"ZZAA": "30000000000.00"},
                  observed_at=datetime.now(timezone.utc) - timedelta(days=4)
                  if observed == "stale" else None)
    if observed == "future":  # collect() refuses this, so set it on the stored row
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE perennial.market_caps SET observed_at = now() + interval '1 day'"
                     " WHERE batch_id = :b"),
                {"b": bad.batch_id},
            )
    pinned = cap_pins(engine, publish_pinned(engine, runs))
    assert pinned["ZZAA"]["run_id"] is None


def test_unavailability_is_keyed_on_a_null_value_only(engine, fixture_path):
    """Checker MC-A: a NULL row's placeholder currency and issuer_level are never read."""
    runs = import_all(engine, Feed(fixture_path))
    run = cap_run(engine, {**every("20000000000.00"), "ZZAA": None})
    with engine.begin() as conn:  # placeholders a reader must not trust
        conn.execute(
            text("UPDATE perennial.market_caps SET currency = 'XXX', issuer_level = false"
                 " WHERE batch_id = :b AND value IS NULL"),
            {"b": run.batch_id},
        )
    pub = publish_pinned(engine, runs)
    record = records(output(engine, pub))["ZZAA"]
    assert record["market_cap"] is None
    assert "market_cap_unavailable" in record["warnings"]
    assert_replays(engine, pub)


# ── MCB-5, MCB-6: the degraded event and the policy ──────


def test_a_fully_refreshed_publication_has_no_degraded_event_but_records_its_policy(
    engine, fixture_path
):
    pub = new_run(engine, Feed(fixture_path), CapAdapter())
    assert degraded(engine, pub) is None
    assert quality_checks(engine, pub)["policy"]["market_caps"] == {
        "max_age_seconds": THREE_DAYS,
        "adapter": ADAPTER,
    }


def test_reused_rows_degrade_with_counts_only(engine, fixture_path):
    feed = Feed(fixture_path)
    new_run(engine, feed, CapAdapter())
    pub = new_run(engine, feed, failing())
    body = degraded(engine, pub)["checks"]["market_caps"]
    assert set(body) == NEW_SHAPE
    candidates = len(records(output(engine, pub)))
    assert (body["refreshed"], body["reused"], body["unavailable"]) == (0, candidates, 0)
    assert isinstance(body["oldest_age_seconds"], int) and body["oldest_age_seconds"] >= 0
    shown = json.dumps(body)
    assert ADAPTER not in shown and not any(s in shown for s in CANDIDATES)
    assert quality_checks(engine, pub)["policy"]["market_caps"]["adapter"] == ADAPTER


def test_an_unavailable_candidate_is_counted(engine, fixture_path):
    """The only row for ZZAA has no value."""
    adapter = CapAdapter(overrides={"ZZAA": None})
    pub = new_run(engine, Feed(fixture_path), adapter)
    body = degraded(engine, pub)["checks"]["market_caps"]
    candidates = len(records(output(engine, pub)))
    assert body == {
        "refreshed": candidates - 1,
        "reused": 0,
        "unavailable": 1,
        "oldest_age_seconds": body["oldest_age_seconds"],
    }


def test_market_caps_merge_into_the_m2_event(engine, fixture_path):
    """MCB-5b: still at most one degraded event per publication."""
    feed = Feed(fixture_path)
    new_run(engine, feed, CapAdapter())
    feed.fail = {"insider_trades"}
    pub = new_run(engine, feed, failing())
    checks = degraded(engine, pub)["checks"]  # degraded() asserts at most one
    assert set(checks["reused"]) == {"insider_trades"}
    assert set(checks["market_caps"]) == NEW_SHAPE


def test_each_path_writes_its_own_market_caps_shape(engine, fixture_path):
    """Legacy fallback {fallback, ...} versus the new path's counts; never mixed."""
    from test_degraded_publishing import failing_market_caps

    feed = Feed(fixture_path)
    legacy_run(engine, feed)
    legacy = legacy_run(engine, feed, market_cap_collector=failing_market_caps)
    assert set(degraded(engine, legacy)["checks"]["market_caps"]) == LEGACY_SHAPE
    assert "market_caps" not in quality_checks(engine, legacy).get("policy", {})

    new_run(engine, feed, CapAdapter())
    new = new_run(engine, feed, failing())
    assert set(degraded(engine, new)["checks"]["market_caps"]) == NEW_SHAPE


# ── MCB-8: a Q7 retry never calls the adapter again ──────


def test_a_retry_with_the_same_candidates_reselects_stored_rows(engine, fixture_path):
    feed = Feed(fixture_path)
    new_run(engine, feed, CapAdapter())
    feed.records["insider_trades"] = feed.records["insider_trades"][:1]  # refused by quality
    adapter = CapAdapter("30000000000.00")
    pub = new_run(engine, feed, adapter)
    assert degraded(engine, pub)["checks"]["reused"]["insider_trades"]["failure_code"] == (
        "publication_quality_failed"
    )
    assert len(adapter.calls) == 1
    this_run = cap_runs(engine)[-1]
    assert {p["run_id"] for p in cap_pins(engine, pub).values()} == {this_run.id}


def test_a_cohort_retry_that_adds_candidates_uses_earlier_rows(engine, fixture_path):
    feed = Feed(fixture_path)
    new_run(engine, feed, CapAdapter())
    earlier = cap_runs(engine)[0]
    feed.records["ark_holdings"] = feed.records["ark_holdings"][:1]  # refused by quality
    adapter = CapAdapter("30000000000.00")
    pub = new_run(engine, feed, adapter)
    assert degraded(engine, pub)["checks"]["reused"]["ark_holdings"]["failure_code"] == (
        "publication_quality_failed"
    )
    assert len(adapter.calls) == 1
    planned = set(adapter.calls[0])
    pinned = assert_new_path(engine, pub)
    added = set(pinned) - planned
    assert added  # the retry ranks the pinned ARK batch's extra holdings
    this_run = cap_runs(engine)[-1]
    for symbol, pin in pinned.items():
        assert pin["run_id"] == (earlier.id if symbol in added else this_run.id), symbol
    body = degraded(engine, pub)["checks"]["market_caps"]
    assert (body["refreshed"], body["reused"]) == (len(planned), len(added))


# ── MCB-9a, MCB-9c: replay and rollback ──────────────────


def test_new_path_publications_replay_after_later_runs_and_when_degraded(engine, fixture_path):
    feed = Feed(fixture_path)
    first = new_run(engine, feed, CapAdapter())
    assert_replays(engine, first)
    second = new_run(engine, feed, CapAdapter("30000000000.00"))
    degraded_pub = new_run(engine, feed, failing())
    assert degraded(engine, degraded_pub) is not None
    for pub in (first, second, degraded_pub):
        assert_replays(engine, pub)


def test_rollback_to_a_new_path_publication(engine, fixture_path):
    from src.pipeline.publication import current, rollback

    feed = Feed(fixture_path)
    first = new_run(engine, feed, CapAdapter())
    new_run(engine, feed, CapAdapter("30000000000.00"))
    rollback(engine, first, actor="synthetic-operator", reason="synthetic rollback check")
    assert current(engine)["id"] == first


# ── MCB-10: demo mode runs the new path with the fixture adapter ─

from test_demo import demo_engine  # noqa: E402,F401  (pytest fixture)


def test_demo_publications_use_the_fixture_adapter(demo_engine):  # noqa: F811
    from src.pipeline.demo import walkthrough

    steps = {s["step"]: s for s in walkthrough(demo_engine)}
    assert steps["replay"]["byte_identical"] is True
    with demo_engine.connect() as conn:
        publications = conn.execute(select(models.publications.c.id)).scalars().all()
        caps_runs = conn.execute(
            select(models.ingestion_runs.c.collection_mode).where(
                models.ingestion_runs.c.source == "market_caps"
            )
        ).scalars().all()
        providers = set(conn.execute(text("SELECT provider FROM perennial.market_caps")).scalars())
        assert conn.execute(text("SELECT count(*) FROM perennial.publication_market_caps")).scalar() == 0
        assert conn.execute(text("SELECT count(*) FROM perennial.market_cap_observations")).scalar() == 0
    assert publications and caps_runs and set(caps_runs) == {"fixture"}
    assert providers == {"fixture"}
    for pub in publications:
        assert set(cap_pins(demo_engine, pub)) == set(records(output(demo_engine, pub)))
        assert_replays(demo_engine, pub)


# ── MCB-11: health ───────────────────────────────────────


@pytest.mark.parametrize(
    "configured, age, advisory",
    [("fixture", timedelta(days=3, hours=1), True),
     ("fixture", timedelta(days=2), False),
     (None, timedelta(days=10), False)],
)
def test_health_advises_on_stale_market_caps_only_with_an_adapter(
    engine, configured, age, advisory
):
    from src.pipeline.health import health
    from test_market_caps_source import seed

    seed(engine, "ZZAA")  # market_caps is lookup-only
    cap_run(engine, {"ZZAA": "1000.00"}, age=age)
    then = datetime.now(timezone.utc) - age
    with engine.begin() as conn:  # health reads finished_at for advisories
        conn.execute(
            update(models.ingestion_runs)
            .where(models.ingestion_runs.c.source == "market_caps")
            .values(finished_at=then, started_at=then)
        )
    report = health(engine, settings=settings_with(configured))
    assert ("market_caps:stale" in report["advisories"]) is advisory
    assert not any("market_caps" in issue for issue in report["issues"])


def test_health_with_an_adapter_but_no_runs_advises_stale(engine):
    from src.pipeline.health import health

    report = health(engine, settings=settings_with("fixture"))
    assert "market_caps:stale" in report["advisories"]
    assert not any("market_caps" in issue for issue in report["issues"])


# ── MCB-12: a new provider is one adapter ────────────────


def test_a_stub_adapter_registered_in_a_test_runs_the_whole_path(
    engine, fixture_path, monkeypatch
):
    from src.ingestion import market_caps as mc
    from src.pipeline.runner import run_pipeline

    adapter = CapAdapter(name="stub_registered")
    monkeypatch.setitem(mc.ADAPTERS, "stub_registered", lambda settings: adapter)
    feed = Feed(fixture_path)
    pub = run_pipeline(engine, collectors=feed.collectors(), quality_thresholds=THRESHOLDS,
                       settings=settings_with("stub_registered"))
    assert len(adapter.calls) == 1
    pinned = assert_new_path(engine, pub)
    (run,) = cap_runs(engine)
    assert {p["run_id"] for p in pinned.values()} == {run.id}
    with engine.connect() as conn:
        assert set(conn.execute(text("SELECT provider FROM perennial.market_caps")).scalars()) == {
            "stub_registered"
        }
    assert_replays(engine, pub)


# ── Bryan's decisions 11-13 (2026-10-23): Q3-Q5 as recommended ─


def test_a_newer_null_row_beats_an_older_value(engine, fixture_path):
    """MCB-3c (Q3: the latest row wins): unavailable, pinned to the NULL row."""
    runs = import_all(engine, Feed(fixture_path))
    cap_run(engine, every("20000000000.00"), age=timedelta(hours=2))
    newer = cap_run(engine, {**every("20000000000.00"), "ZZAA": None}, age=timedelta(hours=1))
    pub = publish_pinned(engine, runs)
    pin = cap_pins(engine, pub)["ZZAA"]
    assert pin["run_id"] == newer.run_id and pin["market_cap_id"] is not None
    assert pin["value"] is None
    record = records(output(engine, pub))["ZZAA"]
    assert record["market_cap"] is None and "market_cap_unavailable" in record["warnings"]


def test_a_later_value_makes_the_security_available_again(engine, fixture_path):
    """MCB-3d (Q3)."""
    runs = import_all(engine, Feed(fixture_path))
    cap_run(engine, {**every("20000000000.00"), "ZZAA": None}, age=timedelta(hours=2))
    later = cap_run(engine, every("30000000000.00"), age=timedelta(hours=1))
    pub = publish_pinned(engine, runs)
    assert cap_pins(engine, pub)["ZZAA"]["run_id"] == later.run_id
    assert records(output(engine, pub))["ZZAA"]["market_cap"] == 30000000000


def test_some_unavailable_candidates_publish_as_unresolved(engine, fixture_path):
    """MCB-7a (Q4: today's rule)."""
    runs = import_all(engine, Feed(fixture_path))
    cap_run(engine, {**every("20000000000.00"), "ZZAA": None, "ZZDD": None})
    pub = publish_pinned(engine, runs)
    doc = output(engine, pub)
    unresolved = {r["ticker"]: r for r in doc["unresolved"]}
    for symbol in ("ZZAA", "ZZDD"):
        assert "market_cap_unavailable" in unresolved[symbol]["warnings"]
    assert_new_path(engine, pub)


def test_every_candidate_unavailable_refuses_and_writes_nothing(engine, fixture_path):
    """MCB-7b (Q4)."""
    from src.pipeline.publication import PublicationError, current

    feed = Feed(fixture_path)
    prior = new_run(engine, feed, CapAdapter())
    runs = import_all(engine, feed)
    with engine.connect() as conn:
        count = conn.execute(text("SELECT count(*) FROM perennial.publications")).scalar()
    with engine.begin() as conn:
        conn.execute(
            update(models.ingestion_runs)
            .where(models.ingestion_runs.c.source == "market_caps")
            .values(collected_at=datetime.now(timezone.utc) - timedelta(days=4))
        )
    cap_run(engine, every(None))
    with pytest.raises(PublicationError):
        publish_pinned(engine, runs)
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM perennial.publications")).scalar() == count
        assert conn.execute(
            text("SELECT count(*) FROM perennial.publication_market_cap_rows"
                 " WHERE publication_id <> :p"), {"p": prior}
        ).scalar() == 0
    assert current(engine)["id"] == prior


def test_legacy_publications_still_replay_through_the_legacy_tables(engine, fixture_path):
    """MCB-9b (Q5: keep them for replay), alongside new-path publications."""
    feed = Feed(fixture_path)
    legacy = legacy_run(engine, feed)
    new_run(engine, feed, CapAdapter())
    new_run(engine, feed, failing())
    assert legacy_rows(engine, legacy) > 0 and cap_pins(engine, legacy) == {}
    assert_replays(engine, legacy)


def test_legacy_rows_are_untouched_and_nothing_is_backfilled(engine, fixture_path):
    """MCB-4c (Q5: no backfill into market_caps runs or pins)."""
    feed = Feed(fixture_path)
    legacy_run(engine, feed)
    with engine.connect() as conn:
        before = (
            conn.execute(text("SELECT * FROM perennial.market_cap_observations ORDER BY id")).all(),
            conn.execute(text(
                "SELECT * FROM perennial.publication_market_caps ORDER BY 1, 2")).all(),
        )
    new_run(engine, feed, CapAdapter())
    with engine.connect() as conn:
        after = (
            conn.execute(text("SELECT * FROM perennial.market_cap_observations ORDER BY id")).all(),
            conn.execute(text(
                "SELECT * FROM perennial.publication_market_caps ORDER BY 1, 2")).all(),
        )
    assert after == before
    assert len(cap_runs(engine)) == 1  # only the new path's own collection
