"""M2 degraded publishing: reuse instead of abort, within the D7 limits.

Synthetic fixtures only. Collectors are injected per source so a test can fail
any one of them; everything after collection is the production path.

Dates follow the X1 contract (dev-sources, fix/source-as-of): every source
reports a monotonic source_as_of (Congress and insider: today; short interest:
latest settlement), and equal dates pass quality for every source.
"""

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select, text, update

from src.db import models
from src.ingestion.importer import CollectionOutcome
from src.pipeline import publication
from src.pipeline.publication import (
    PublicationError,
    PublicationReuseError,
    Reuse,
    current,
    publish,
    replay,
)
from src.pipeline.runner import RequiredSourceUnavailableError, run_pipeline
from src.settings import _quality_defaults

# X1 contract: same-day re-collection with changed content is not a regression.
THRESHOLDS = {
    s: t.model_copy(update={"allow_equal_as_of": True})
    for s, t in _quality_defaults().items()
}

FILES = {
    "congress_trades": ("trades_congress.json", "trades", "recent_window"),
    "ark_holdings": ("ark_holdings.json", "holdings", "snapshot"),
    "insider_trades": ("trades_insider.json", "transactions", "snapshot"),
    "short_interest": ("short_interest.json", "records", "snapshot"),
}


class Feed:
    """Fixture-backed collectors; `fail` makes a source's collection fail."""

    def __init__(self, fixture_path):
        self.records = {
            source: json.loads(fixture_path(name).read_text())[key]
            for source, (name, key, _) in FILES.items()
        }
        self.fail: set[str] = set()
        self.calls: list[str] = []

    def change(self, source):
        """New content for a source, so its next batch gets a new batch id."""
        rows = [dict(r) for r in self.records[source]]
        if source == "congress_trades":
            extra = dict(rows[0], politician_name="Synthetic Extra Member")
            extra.pop("representative", None)
            rows.append(extra)
        elif source == "ark_holdings":
            rows[0]["total_weight"] = round(rows[0]["total_weight"] + 0.5, 2)
        self.records[source] = rows

    def as_of(self, source):
        if source == "short_interest":
            return max(
                datetime.fromisoformat(r["settlement_date"]).date()
                for r in self.records[source]
            )
        return datetime.now(timezone.utc).date()

    def collectors(self):
        def make(source):
            def collect(**kwargs):
                self.calls.append(source)
                if source in self.fail:
                    return CollectionOutcome.failure(source, "live", "collection_failed")
                return CollectionOutcome.success(
                    source,
                    "live",
                    self.records[source],
                    coverage={"complete": True, "scope": FILES[source][2]},
                    source_as_of=self.as_of(source),
                )

            return collect

        return {source: make(source) for source in FILES}


def market_caps(symbols):
    return {
        s: {
            "value": Decimal("20000000000"),
            "currency": "USD",
            "provider": "fmp",
            "retrieved_at": datetime.now(timezone.utc),
            "observed_at": None,
        }
        for s in symbols
    }


def run(engine, feed, **kwargs):
    kwargs.setdefault("quality_thresholds", THRESHOLDS)
    kwargs.setdefault("market_cap_collector", market_caps)
    return run_pipeline(engine, collectors=feed.collectors(), **kwargs)


def import_all(engine, feed):
    """Fresh live runs for every source, outside the runner; returns run ids."""
    from src.ingestion.importer import import_collection
    from src.pipeline.lifecycle import begin_run

    runs, batches = {}, {}
    for source in FILES:
        deps = (
            {s: batches[s] for s in ("congress_trades", "ark_holdings")}
            if source in ("insider_trades", "short_interest")
            else {}
        )
        run_id = begin_run(engine, source, "live", dependencies=deps)
        outcome = feed.collectors()[source]()
        result = import_collection(engine, outcome, run_id=run_id)
        assert result.succeeded
        runs[source], batches[source] = result.run_id, result.batch_id
    return runs


def pins(engine, publication_id):
    with engine.connect() as conn:
        return {
            row.source: (row.run_id, row.batch_id)
            for row in conn.execute(
                select(models.publication_sources).where(
                    models.publication_sources.c.publication_id == publication_id
                )
            )
        }


def degraded(engine, publication_id):
    with engine.connect() as conn:
        rows = conn.execute(
            select(models.publication_events).where(
                models.publication_events.c.publication_id == publication_id,
                models.publication_events.c.kind == "degraded",
            )
        ).mappings().all()
    assert len(rows) <= 1
    return dict(rows[0]) if rows else None


def quality_audit(engine, publication_id):
    with engine.connect() as conn:
        return conn.execute(
            select(models.publication_events.c.checks).where(
                models.publication_events.c.publication_id == publication_id,
                models.publication_events.c.kind.in_(["quality_passed", "quality_override"]),
            )
        ).scalar_one()["sources"]


def age_run(engine, run_id, delta):
    with engine.begin() as conn:
        conn.execute(
            update(models.ingestion_runs)
            .where(models.ingestion_runs.c.id == run_id)
            .values(collected_at=datetime.now(timezone.utc) - delta)
        )


# ── Gate M2 test 1: fault injection per source ──


@pytest.mark.db
@pytest.mark.parametrize("failing", list(FILES))
def test_one_failed_source_publishes_degraded_with_its_pinned_batch(
    engine, fixture_path, failing
):
    feed = Feed(fixture_path)
    baseline = run(engine, feed)
    before = pins(engine, baseline)
    assert degraded(engine, baseline) is None

    feed.fail = {failing}
    second = run(engine, feed)
    after = pins(engine, second)

    assert current(engine)["id"] == second != baseline
    assert after[failing] == before[failing]
    for source in set(FILES) - {failing}:
        assert after[source][0] != before[source][0]  # a fresh run today
    event = degraded(engine, second)
    assert event["actor"] is None and event["reason"] is None
    checks = event["checks"]
    assert set(checks["reused"]) == {failing}
    entry = checks["reused"][failing]
    assert (entry["run_id"], entry["batch_id"]) == before[failing]
    assert entry["from_publication_id"] == baseline
    assert entry["failure_code"] == "collection_failed"
    assert entry["failed_run_id"] is not None
    assert checks["cohort"] == [] and checks["all_sources_reused"] is False
    assert checks["policy"][failing]["max_reuse_age_seconds"] > 0
    assert quality_audit(engine, second)[failing]["status"] == "reused"
    # Gate M2: a degraded publication replays byte-identically.
    assert json.dumps(replay(engine, second), sort_keys=True) == json.dumps(
        current(engine)["output"], sort_keys=True
    )


@pytest.mark.db
def test_downstream_reuse_with_unchanged_upstream_needs_no_cohort(engine, fixture_path):
    """Identical upstream content keeps its batch id, so lineage already holds."""
    feed = Feed(fixture_path)
    baseline = run(engine, feed)
    feed.fail = {"insider_trades"}
    second = run(engine, feed)
    assert pins(engine, second)["insider_trades"] == pins(engine, baseline)["insider_trades"]
    assert pins(engine, second)["congress_trades"][1] == pins(engine, baseline)["congress_trades"][1]
    assert degraded(engine, second)["checks"]["cohort"] == []


# ── Gate M2 tests 2 and 3: limits and the first publication ──


@pytest.mark.db
def test_reuse_past_max_reuse_age_aborts_and_keeps_the_pointer(engine, fixture_path):
    feed = Feed(fixture_path)
    baseline = run(engine, feed)
    age_run(engine, pins(engine, baseline)["congress_trades"][0], timedelta(days=3, minutes=1))
    feed.fail = {"congress_trades"}
    with pytest.raises(RequiredSourceUnavailableError) as caught:
        run(engine, feed)
    assert caught.value.code == "required_source_unavailable"
    assert caught.value.source == "congress_trades"
    assert current(engine)["id"] == baseline


@pytest.mark.db
def test_reuse_limit_boundary_comes_from_settings(engine, fixture_path):
    from src.settings import Settings, SourcePolicy

    feed = Feed(fixture_path)
    baseline = run(engine, feed)
    policies = Settings(database_url="postgresql+psycopg://u@127.0.0.1/x_test").source_policies
    policies["short_interest"] = SourcePolicy(
        max_reuse_age=timedelta(days=2), max_age=timedelta(days=45)
    )
    settings = Settings(
        database_url="postgresql+psycopg://u@127.0.0.1/x_test", source_policies=policies
    )
    short_run = pins(engine, baseline)["short_interest"][0]
    feed.fail = {"short_interest"}

    age_run(engine, short_run, timedelta(days=2, seconds=30))
    with pytest.raises(RequiredSourceUnavailableError):
        run(engine, feed, settings=settings)
    age_run(engine, short_run, timedelta(days=2, seconds=-30))
    assert run(engine, feed, settings=settings) != baseline


@pytest.mark.db
def test_without_a_publication_nothing_can_be_reused(engine, fixture_path):
    feed = Feed(fixture_path)
    feed.fail = {"ark_holdings"}
    with pytest.raises(RequiredSourceUnavailableError, match="ark_holdings"):
        run(engine, feed)
    assert current(engine) is None


# ── Gate M2 test 10: a reusable pin must still pass every publication rule ──


@pytest.mark.db
@pytest.mark.parametrize("rule", ["ark_observation", "short_settlement", "coverage"])
def test_pin_failing_a_non_age_rule_is_not_reusable(engine, fixture_path, rule):
    feed = Feed(fixture_path)
    baseline = run(engine, feed)
    pinned = pins(engine, baseline)
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        if rule == "ark_observation":
            source = "ark_holdings"
            conn.execute(
                update(models.source_batches)
                .where(models.source_batches.c.id == pinned[source][1])
                .values(source_as_of=(now - timedelta(days=8)).date())
            )
        elif rule == "short_settlement":
            source = "short_interest"
            conn.execute(
                update(models.short_interest)
                .where(models.short_interest.c.batch_id == pinned[source][1])
                .values(settlement_date=(now - timedelta(days=46)).date())
            )
        else:
            source = "insider_trades"
            conn.execute(
                update(models.ingestion_runs)
                .where(models.ingestion_runs.c.id == pinned[source][0])
                .values(coverage={"complete": False, "scope": "snapshot"})
            )
    feed.fail = {source}
    with pytest.raises(RequiredSourceUnavailableError) as caught:
        run(engine, feed)
    assert caught.value.source == source
    assert current(engine)["id"] == baseline


# ── Gate M2 tests 5, 6, 13: publish() re-validates reuse under the lock ──


@pytest.mark.db
def test_publish_refuses_reuse_of_a_run_that_is_not_pinned(engine, fixture_path):
    from test_pipeline import caps, inputs

    runs, batches = inputs(engine, fixture_path)
    publish(engine, runs, caps(engine, batches))
    newer, newer_batches = inputs(engine, fixture_path)
    with pytest.raises(PublicationReuseError) as caught:
        publish(
            engine,
            newer,
            caps(engine, newer_batches),
            reuse={"ark_holdings": Reuse("collection_failed")},
        )
    assert caught.value.code == "reuse_not_current"


@pytest.mark.db
def test_publish_refuses_reuse_past_its_limit(engine, fixture_path):
    from test_pipeline import caps, inputs

    runs, batches = inputs(engine, fixture_path)
    publish(engine, runs, caps(engine, batches))
    limits = {s: timedelta(days=3) for s in models.SOURCES}
    limits["ark_holdings"] = timedelta(seconds=1)
    age_run(engine, runs["ark_holdings"], timedelta(seconds=5))
    with pytest.raises(PublicationReuseError):
        publish(
            engine,
            runs,
            caps(engine, batches),
            reuse={"ark_holdings": Reuse("collection_failed")},
            reuse_limits=limits,
        )


@pytest.mark.db
def test_pointer_moving_after_selection_refuses_the_stale_reuse(engine, fixture_path):
    """Gate M2 test 13: the race between choosing a pin and publishing."""
    feed = Feed(fixture_path)
    baseline = run(engine, feed)
    feed.fail = {"ark_holdings"}
    raced = {}

    def caps_after_another_publisher(symbols):
        # Another publisher promotes new runs while this one sits between
        # choosing the pinned ARK run and publishing.
        raced["id"] = publish(
            engine,
            import_all(engine, Feed(fixture_path)),
            market_caps(symbols),
            quality_thresholds=THRESHOLDS,
        )
        return market_caps(symbols)

    with pytest.raises(PublicationReuseError):
        run(engine, feed, market_cap_collector=caps_after_another_publisher)
    assert current(engine)["id"] == raced["id"] != baseline


@pytest.mark.db
def test_fresh_source_failing_quality_still_blocks_a_degraded_publication(
    engine, fixture_path
):
    feed = Feed(fixture_path)
    run(engine, feed)
    feed.fail = {"ark_holdings"}
    feed.records["insider_trades"] = feed.records["insider_trades"][:1]
    with pytest.raises(PublicationError) as caught:
        run(engine, feed)
    assert caught.value.code == "publication_quality_failed"
    assert caught.value.checks == ("insider_trades:row_count",)


# ── Gate M2 test 9: concurrency ──


@pytest.mark.db
def test_degraded_run_fails_fast_while_another_publisher_holds_the_lock(
    engine, fixture_path
):
    feed = Feed(fixture_path)
    baseline = run(engine, feed)
    feed.fail = {"congress_trades"}
    with engine.begin() as conn:
        conn.execute(
            text("SELECT pg_advisory_xact_lock(:key)"),
            {"key": publication.PUBLICATION_LOCK},
        )
        with pytest.raises(PublicationError) as caught:
            run(engine, feed)
    assert caught.value.code == "publication_in_progress"
    assert current(engine)["id"] == baseline


# ── Gate M2 tests 1 and 11: cohort reuse when upstream content changed ──


def _reused(engine, publication_id):
    checks = degraded(engine, publication_id)["checks"]
    return checks, {s: e for s, e in checks["reused"].items()}


@pytest.mark.db
def test_insider_failure_with_changed_upstream_reuses_the_whole_prior_cohort(
    engine, fixture_path
):
    feed = Feed(fixture_path)
    baseline = run(engine, feed)
    feed.change("congress_trades")
    feed.fail = {"insider_trades"}
    feed.calls.clear()
    second = run(engine, feed)

    assert pins(engine, second) == pins(engine, baseline)
    # The cohort costs no provider calls: short interest is never collected.
    assert feed.calls == ["congress_trades", "ark_holdings", "insider_trades"]
    checks, reused = _reused(engine, second)
    assert checks["cohort"] == ["ark_holdings", "congress_trades", "short_interest"]
    assert checks["all_sources_reused"] is True
    assert reused["insider_trades"]["failure_code"] == "collection_failed"
    assert reused["congress_trades"]["failure_code"] is None
    # Today's changed Congress batch was collected but is not published.
    assert reused["congress_trades"]["unpublished_run_id"] is not None
    assert replay(engine, second) == current(engine)["output"]


@pytest.mark.db
def test_short_interest_failure_with_changed_upstream_reuses_pinned_insider(
    engine, fixture_path
):
    """M2-5: no second insider pass when the cohort is decided after insider ran."""
    feed = Feed(fixture_path)
    baseline = run(engine, feed)
    feed.change("congress_trades")
    feed.fail = {"short_interest"}
    feed.calls.clear()
    second = run(engine, feed)

    assert feed.calls.count("insider_trades") == 1
    assert pins(engine, second)["insider_trades"] == pins(engine, baseline)["insider_trades"]
    checks, reused = _reused(engine, second)
    assert checks["cohort"] == ["ark_holdings", "congress_trades", "insider_trades"]
    assert reused["insider_trades"]["unpublished_run_id"] is not None
    assert reused["short_interest"]["failure_code"] == "collection_failed"
    assert replay(engine, second) == current(engine)["output"]


@pytest.mark.db
@pytest.mark.parametrize(
    "failing,changed,expect_cohort",
    [
        # Congress reused for its own failure; ARK unchanged keeps lineage intact.
        ({"congress_trades", "insider_trades"}, set(), []),
        # ARK changed too, so the reused insider needs the prior ARK and sibling.
        ({"congress_trades", "insider_trades"}, {"ark_holdings"}, ["ark_holdings", "short_interest"]),
        # ARK reused; insider planned fresh from it; short's pin needs the prior Congress.
        ({"ark_holdings", "short_interest"}, {"congress_trades"}, ["congress_trades", "insider_trades"]),
    ],
)
def test_two_failed_sources_compose(engine, fixture_path, failing, changed, expect_cohort):
    feed = Feed(fixture_path)
    baseline = run(engine, feed)
    for source in changed:
        feed.change(source)
    feed.fail = set(failing)
    second = run(engine, feed)

    checks, reused = _reused(engine, second)
    assert checks["cohort"] == expect_cohort
    assert set(reused) == set(failing) | set(expect_cohort)
    before, after = pins(engine, baseline), pins(engine, second)
    for source in reused:
        assert after[source] == before[source]
    for source in set(FILES) - set(reused):
        assert after[source][0] != before[source][0]
    assert replay(engine, second) == current(engine)["output"]


@pytest.mark.db
def test_cohort_member_past_its_limit_aborts(engine, fixture_path):
    feed = Feed(fixture_path)
    baseline = run(engine, feed)
    age_run(engine, pins(engine, baseline)["short_interest"][0], timedelta(days=16))
    feed.change("congress_trades")
    feed.fail = {"insider_trades"}
    with pytest.raises(RequiredSourceUnavailableError) as caught:
        run(engine, feed)
    assert caught.value.source == "short_interest"
    assert current(engine)["id"] == baseline


# ── Gate M2 test 7: market-cap fallback (Q3) ──


def failing_market_caps(symbols):
    raise PublicationError("market-cap collection failed")


@pytest.mark.db
def test_market_cap_outage_falls_back_to_stored_observations(engine, fixture_path):
    feed = Feed(fixture_path)
    baseline = run(engine, feed)
    second = run(engine, feed, market_cap_collector=failing_market_caps)

    checks = degraded(engine, second)["checks"]
    assert checks["reused"] == {} and checks["all_sources_reused"] is False
    assert checks["market_caps"]["fallback"] is True
    assert checks["market_caps"]["unavailable"] == 0
    assert checks["market_caps"]["oldest_retrieved_at"] is not None
    published = {r["ticker"]: r["market_cap"] for b in ("popular_stable", "affordable_growing")
              for r in current(engine)["output"][b]}
    assert published and all(v == 20000000000 for v in published.values())
    assert current(engine)["id"] == second != baseline
    assert replay(engine, second) == current(engine)["output"]


@pytest.mark.db
def test_market_cap_fallback_ignores_observations_past_three_days(engine, fixture_path):
    feed = Feed(fixture_path)
    baseline = run(engine, feed)
    with engine.begin() as conn:
        symbol_ids = conn.execute(
            select(models.publication_market_caps.c.security_id).where(
                models.publication_market_caps.c.publication_id == baseline
            )
        ).scalars().all()
        conn.execute(
            update(models.market_cap_observations)
            .where(models.market_cap_observations.c.security_id == symbol_ids[0])
            .values(retrieved_at=datetime.now(timezone.utc) - timedelta(days=3, minutes=1))
        )
    second = run(engine, feed, market_cap_collector=failing_market_caps)
    assert degraded(engine, second)["checks"]["market_caps"]["unavailable"] == 1

    with engine.begin() as conn:
        conn.execute(
            update(models.market_cap_observations).values(
                retrieved_at=datetime.now(timezone.utc) - timedelta(days=4)
            )
        )
    with pytest.raises(PublicationError, match="all market-cap values are unavailable"):
        run(engine, feed, market_cap_collector=failing_market_caps)
    assert current(engine)["id"] == second


@pytest.mark.db
def test_market_cap_collector_bug_is_not_swallowed_by_the_fallback(engine, fixture_path):
    feed = Feed(fixture_path)
    run(engine, feed)

    def broken(symbols):
        raise KeyError("synthetic bug")

    with pytest.raises(KeyError):
        run(engine, feed, market_cap_collector=broken)
