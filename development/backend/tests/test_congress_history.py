"""Incremental Congress collection: accumulated history, gaps, watermark replay."""

import copy
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from src.db import models
from src.ingestion.importer import CollectionOutcome, import_collection
from src.pipeline import congress_history
from src.pipeline.health import health
from src.pipeline.lifecycle import begin_run
from src.pipeline.publication import current, publish, replay
from src.pipeline.ranking import candidate_symbols
from src.pipeline.runner import collect_source

pytestmark = pytest.mark.db

TODAY = datetime.now(timezone.utc).date()
FILES = {
    "congress_trades": ("trades_congress.json", "trades"),
    "ark_holdings": ("ark_holdings.json", "holdings"),
    "insider_trades": ("trades_insider.json", "transactions"),
    "short_interest": ("short_interest.json", "records"),
}


def records(fixture_path, source):
    from test_pipeline import fixture_records

    return fixture_records(fixture_path, *FILES[source])


def trade(politician, days_ago, ticker="ZZAA", chamber="Senate", **extra):
    day = (TODAY - timedelta(days=days_ago)).isoformat()
    return {
        "politician_name": politician,
        "chamber": chamber,
        "ticker": ticker,
        "trade_type": "Purchase",
        "amount_range": "$1,001 - $15,000",
        "transaction_date": day,
        "disclosure_date": day,
        **extra,
    }


def congress_run(engine, rows, served=None):
    """One succeeded live Congress run; `served` mimics the collector's counts."""
    diagnostics = {f"{c.lower()}_rows_served": n for c, n in (served or {}).items()}
    run = begin_run(engine, "congress_trades", "live")
    result = import_collection(
        engine,
        CollectionOutcome.success(
            "congress_trades",
            "live",
            rows,
            diagnostics=diagnostics or None,
            coverage={"complete": True, "scope": "recent_window"},
            # As the collector does: the latest disclosure date on the page.
            source_as_of=max(
                (
                    datetime.fromisoformat(r["disclosure_date"]).date()
                    for r in rows
                    if r.get("disclosure_date")
                ),
                default=None,
            ),
        ),
        run_id=run,
    )
    assert result.succeeded
    return result


def derive(engine, run_id):
    with engine.begin() as conn:
        return congress_history.derive(conn, run_id)


def history_count(engine, **where):
    h = models.congress_trade_history
    with engine.connect() as conn:
        q = select(func.count()).select_from(h)
        for k, v in where.items():
            q = q.where(h.c[k] == v)
        return conn.execute(q).scalar_one()


def gaps(engine):
    with engine.connect() as conn:
        return conn.execute(select(models.congress_history_gaps)).mappings().all()


def full_pipeline(engine, fixture_path, congress_rows=None, served=None, through=None):
    """Live runs for every source with downstream lineage on this Congress batch."""
    congress = congress_run(
        engine,
        congress_rows
        if congress_rows is not None
        else records(fixture_path, "congress_trades"),
        served,
    )
    runs, batches = (
        {"congress_trades": congress.run_id},
        {"congress_trades": congress.batch_id},
    )
    for source in ("ark_holdings", "insider_trades", "short_interest"):
        deps = (
            {s: batches[s] for s in ("congress_trades", "ark_holdings")}
            if source in ("insider_trades", "short_interest")
            else {}
        )
        run = begin_run(
            engine,
            source,
            "live",
            dependencies=deps,
            history_through=(through or {}).get(source),
        )
        result = import_collection(
            engine,
            CollectionOutcome.success(
                source,
                "live",
                records(fixture_path, source),
                source_as_of=TODAY if source == "ark_holdings" else None,
                coverage={"complete": True, "scope": "snapshot"},
            ),
            run_id=run,
        )
        assert result.succeeded
        runs[source], batches[source] = result.run_id, result.batch_id
    return runs, batches


def caps_for(engine, batches):
    """Market caps for exactly what v3 ranks: windowed Congress history."""
    from src.db.queries import batch_rows

    with engine.begin() as conn:
        rows = {s: batch_rows(conn, s, b) for s, b in batches.items()}
        rows["congress_trades"] = congress_history.ranking_rows(
            conn, congress_history.watermark(conn), datetime.now(timezone.utc)
        )
    return {
        s: {
            "value": Decimal("20000000000"),
            "currency": "USD",
            "provider": "fmp",
            "retrieved_at": datetime.now(timezone.utc),
            "observed_at": None,
        }
        for s in candidate_symbols(rows)
    }


def publish_now(engine, fixture_path, congress_rows=None, served=None):
    runs, batches = full_pipeline(engine, fixture_path, congress_rows, served)
    # Derive before computing caps, as the runner does.
    derive(engine, runs["congress_trades"])
    return publish(engine, runs, caps_for(engine, batches))


# ── accumulation and dedup ─────────────────────────────────


def test_overlapping_runs_accumulate_distinct_trades_once(engine):
    first = congress_run(engine, [trade("A", 1), trade("B", 2)])
    assert derive(engine, first.run_id)["history_added"] == 2
    second = congress_run(engine, [trade("C", 0), trade("A", 1)])
    assert derive(engine, second.run_id)["history_added"] == 1
    assert derive(engine, second.run_id)["history_added"] == 0  # idempotent
    assert history_count(engine) == 3


def test_identical_rows_keep_multiplicity_without_summing_across_runs(engine):
    twice = congress_run(engine, [trade("A", 1), trade("A", 1)])
    derive(engine, twice.run_id)
    once = congress_run(engine, [trade("A", 1), trade("B", 2)])
    derive(engine, once.run_id)
    assert history_count(engine, politician_name="A") == 2  # not 1, not 3
    thrice = congress_run(engine, [trade("A", 1)] * 3)
    derive(engine, thrice.run_id)
    assert history_count(engine, politician_name="A") == 3


def test_amended_row_is_a_new_trade(engine):
    derive(engine, congress_run(engine, [trade("A", 5)]).run_id)
    amended = trade("A", 5, amount_range="$15,001 - $50,000")
    derive(engine, congress_run(engine, [amended]).run_id)
    assert history_count(engine, politician_name="A") == 2


def test_only_succeeded_live_runs_feed_history(engine):
    run = begin_run(engine, "congress_trades", "fixture")
    result = import_collection(
        engine,
        CollectionOutcome.success("congress_trades", "fixture", [trade("A", 1)]),
        run_id=run,
    )
    with pytest.raises(ValueError):
        derive(engine, result.run_id)
    assert history_count(engine) == 0


def test_old_trades_are_stored_but_outside_the_ranking_window(engine):
    derive(engine, congress_run(engine, [trade("A", 1), trade("Old", 3 * 365)]).run_id)
    assert history_count(engine) == 2  # D13: storage keeps everything
    with engine.begin() as conn:
        ranked = congress_history.ranking_rows(
            conn, congress_history.watermark(conn), datetime.now(timezone.utc)
        )
    assert [r["politician_name"] for r in ranked] == ["A"]


def test_window_start_handles_leap_day():
    leap = datetime(2028, 2, 29, tzinfo=timezone.utc)
    assert congress_history.window_start(leap).isoformat() == "2026-02-28"


# ── gap detection ──────────────────────────────────────────


def page(prefix, start_days_ago, n=20, chamber="Senate", ticker="ZZAA"):
    return [
        trade(f"{prefix}{i}", start_days_ago + i, ticker=ticker, chamber=chamber)
        for i in range(n)
    ]


def test_full_page_with_no_overlap_records_a_gap(engine):
    derive(engine, congress_run(engine, page("old", 30), {"Senate": 20}).run_id)
    result = congress_run(engine, page("new", 0), {"Senate": 20})
    counts = derive(engine, result.run_id)
    assert counts["possible_gaps"] == 1
    [gap] = gaps(engine)
    assert gap["chamber"] == "Senate" and gap["detected_run_id"] == result.run_id
    assert gap["newest_stored_date"] == TODAY - timedelta(days=30)
    assert gap["oldest_seen_date"] == TODAY - timedelta(days=19)
    assert gap["gap_days"] == 11
    assert history_count(engine) == 40  # the new rows are still kept
    derive(engine, result.run_id)
    assert len(gaps(engine)) == 1  # re-deriving records nothing new


@pytest.mark.parametrize(
    "first_served,second_rows,second_served",
    [
        (None, None, {"Senate": 20}),  # empty history: cold start, not a gap
        ({"Senate": 20}, "short", {"Senate": 19}),  # short page is the whole list
        ({"Senate": 20}, "overlap", {"Senate": 20}),  # page reaches stored history
    ],
)
def test_no_gap_without_evidence(engine, first_served, second_rows, second_served):
    if first_served:
        derive(engine, congress_run(engine, page("old", 30), first_served).run_id)
    rows = {
        None: page("new", 0),
        "short": page("new", 0, 19),
        "overlap": page("new", 0, 19) + [trade("old0", 30)],
    }[second_rows]
    assert (
        derive(engine, congress_run(engine, rows, second_served).run_id)[
            "possible_gaps"
        ]
        == 0
    )
    assert gaps(engine) == []


def test_gap_is_per_chamber(engine):
    first = page("s", 30) + page("h", 30, chamber="House")
    derive(engine, congress_run(engine, first, {"Senate": 20, "House": 20}).run_id)
    second = page("s", 30) + page("hn", 0, chamber="House")
    derive(engine, congress_run(engine, second, {"Senate": 20, "House": 20}).run_id)
    assert [g["chamber"] for g in gaps(engine)] == ["House"]


# ── publication pinning and replay ─────────────────────────


def test_publication_pins_history_and_replays_after_more_arrives(engine, fixture_path):
    pub = publish_now(engine, fixture_path)
    stored = current(engine)["output"]
    assert stored["ranking_version"] == "watchlist-v3"
    with engine.connect() as conn:
        through = conn.execute(
            select(models.publication_history_watermarks.c.through_history_id).where(
                models.publication_history_watermarks.c.publication_id == pub
            )
        ).scalar_one()
    assert through == history_count(engine) > 0
    # More history after publishing must not change what the publication replays.
    derive(engine, congress_run(engine, [trade("Later", 0, ticker="ZZCC")]).run_id)
    assert json.dumps(replay(engine, pub), sort_keys=True) == json.dumps(
        stored, sort_keys=True
    )


def test_ranking_sees_history_beyond_todays_page(engine, fixture_path):
    first = publish_now(engine, fixture_path)
    assert first
    # Tomorrow's full page no longer lists the fixture trades.
    later = page("Newer", 0, ticker="ZZCC")
    second = publish_now(engine, fixture_path, later)
    output = current(engine)["output"]
    ranked = {
        row["ticker"]: row
        for name in ("popular_stable", "affordable_growing", "unresolved")
        for row in output[name]
    }
    # ZZAA's Congress buys come from history: none are on today's page.
    assert ranked["ZZAA"]["signals"]["congress"]["buy_count"] >= 1
    assert ranked["ZZCC"]["signals"]["congress"]["buy_count"] >= 20
    assert replay(engine, second) == output


def test_gap_degrades_publication_and_health_reports_sev3(engine, fixture_path):
    publish_now(engine, fixture_path, page("old", 30), {"Senate": 20})
    pub = publish_now(engine, fixture_path, page("new", 0), {"Senate": 20})
    with engine.connect() as conn:
        degraded = conn.execute(
            select(models.publication_events.c.checks).where(
                models.publication_events.c.publication_id == pub,
                models.publication_events.c.kind == "degraded",
            )
        ).scalar_one()
        quality = conn.execute(
            select(models.publication_events.c.checks).where(
                models.publication_events.c.publication_id == pub,
                models.publication_events.c.kind == "quality_passed",
            )
        ).scalar_one()
    assert degraded["congress_gaps"] == {"count": 1, "max_days": 11}
    assert quality["congress_history"]["gaps_in_window"]["count"] == 1
    assert (
        quality["congress_history"]["since"]["Senate"]
        == (TODAY - timedelta(days=49)).isoformat()
    )
    report = health(engine)
    assert report["issue_severity"]["congress_trades:possible_gap"] == "SEV3"
    assert report["congress_history"]["warming"] is True


# ── runner: every live collection is folded in ─────────────


def test_collect_source_folds_unpublished_runs_into_history(engine):
    def collector():
        return CollectionOutcome.success(
            "congress_trades",
            "live",
            [trade("Evening", 0)],
            diagnostics={"senate_rows_served": 1, "house_rows_served": 0},
            coverage={"complete": True, "scope": "recent_window"},
        )

    result = collect_source(engine, "congress_trades", collector=collector)
    assert result.succeeded
    assert history_count(engine, politician_name="Evening") == 1


def test_trade_key_ignores_nothing_but_listed_fields():
    row = {k: None for k in congress_history.KEY_FIELDS}
    other = copy.deepcopy(row)
    other["amount_label"] = "$1"
    assert congress_history.trade_key(row) != congress_history.trade_key(other)
    assert len(congress_history.trade_key(row)) == 64


# ── end to end: a live run-pipeline publishes on the free plan ─


def test_run_pipeline_publishes_on_the_free_plan(engine, monkeypatch):
    """
    FMP as probed (v1/v2): page 0 with limit 20 answers; any deeper or larger
    page is HTTP 402. Two daily runs publish through the real collectors, and
    the second ranks trades that only the first page listed.
    """
    import requests
    from src.ingestion import collectors
    from src.pipeline.runner import run_pipeline
    from test_pipeline_contract import Provider, StubResponse, stub_market_caps

    class FreePlan(Provider):
        day = 0
        fmp_requests = []

        def fmp_trades(self, chamber):
            # 20 rows per chamber; each day's page is entirely new trades.
            base = self.today - timedelta(days=5 - self.day)
            return [
                {
                    "symbol": "ZZAA" if self.day == 0 else "ZZCC",
                    "firstName": "Synthetic",
                    "lastName": f"{chamber} {self.day} {i}",
                    "type": "Purchase",
                    "amount": "$1,001 - $15,000",
                    "transactionDate": (base - timedelta(days=i)).isoformat(),
                    "disclosureDate": (base - timedelta(days=i)).isoformat(),
                    "assetDescription": "Synthetic Corp",
                    "assetType": "Stock",
                    "district": "",
                    "link": f"https://disclosures.example.invalid/{chamber}/{i}",
                }
                for i in range(20)
            ]

        def get(self, session, url, params=None, **kwargs):
            params = params or {}
            if "financialmodelingprep.com" in url and url.endswith("-latest"):
                self.fmp_requests.append((int(params["page"]), int(params["limit"])))
                if int(params["page"]) > 0 or int(params["limit"]) > 20:
                    return StubResponse({}, status=402)
            return super().get(session, url, params=params, **kwargs)

    stub = FreePlan()
    monkeypatch.setattr(
        requests.Session,
        "get",
        lambda self, url, params=None, **kw: stub.get(self, url, params, **kw),
    )
    for name in ("ark", "insider", "short_interest"):
        monkeypatch.setattr(collectors._fetcher(name), "REQUEST_DELAY", 0)
    monkeypatch.setattr(collectors._fetcher("fmp"), "FMP_API_KEY", "synthetic-test-key")

    first = run_pipeline(engine, market_cap_collector=stub_market_caps)
    first_output = current(engine)["output"]
    stub.day = 1
    second = run_pipeline(engine, market_cap_collector=stub_market_caps)

    assert stub.fmp_requests == [(0, 20)] * 4  # never page 1, never a larger page
    assert history_count(engine) == 80
    output = current(engine)["output"]
    ranked = {
        row["ticker"]: row
        for name in ("popular_stable", "affordable_growing", "unresolved")
        for row in output[name]
    }
    # Day 0's ZZAA trades are no longer on the page but still rank from history.
    assert ranked["ZZAA"]["signals"]["congress"]["buy_count"] == 40
    assert ranked["ZZCC"]["signals"]["congress"]["buy_count"] == 40
    assert replay(engine, first) == first_output
    assert replay(engine, second) == output


def test_downstream_sources_are_planned_from_history(engine, monkeypatch):
    """
    CI-1: a Congress-only ticker that has scrolled off today's page still ranks
    from history, so insider and short interest must be requested for it, and
    the downstream runs must record the history watermark the publication pins.
    """
    import requests
    from src.ingestion import collectors
    from src.pipeline.runner import run_pipeline
    from test_pipeline_contract import Provider, StubResponse, stub_market_caps

    class FreePlan(Provider):
        day = 0

        def fmp_trades(self, chamber):
            base = self.today - timedelta(days=5 - self.day)
            return [
                {
                    "symbol": "ZZQQ" if self.day == 0 else "ZZCC",
                    "firstName": "Synthetic",
                    "lastName": f"{chamber} {self.day} {i}",
                    "type": "Purchase",
                    "amount": "$1,001 - $15,000",
                    "transactionDate": (base - timedelta(days=i)).isoformat(),
                    "disclosureDate": (base - timedelta(days=i)).isoformat(),
                    "assetDescription": "Synthetic Corp",
                    "assetType": "Stock",
                    "district": "",
                    "link": f"https://disclosures.example.invalid/{chamber}/{i}",
                }
                for i in range(20)
            ]

        def get(self, session, url, params=None, **kwargs):
            params = params or {}
            if "financialmodelingprep.com" in url and url.endswith("-latest"):
                if int(params["page"]) > 0 or int(params["limit"]) > 20:
                    return StubResponse({}, status=402)
            return super().get(session, url, params=params, **kwargs)

    stub = FreePlan()
    monkeypatch.setattr(
        requests.Session,
        "get",
        lambda self, url, params=None, **kw: stub.get(self, url, params, **kw),
    )
    for name in ("ark", "insider", "short_interest"):
        monkeypatch.setattr(collectors._fetcher(name), "REQUEST_DELAY", 0)
    monkeypatch.setattr(collectors._fetcher("fmp"), "FMP_API_KEY", "synthetic-test-key")

    run_pipeline(engine, market_cap_collector=stub_market_caps)
    stub.day = 1
    stub.requested = {"insider_trades": [], "short_interest": []}
    pub = run_pipeline(engine, market_cap_collector=stub_market_caps)

    assert "ZZQQ" in stub.requested["insider_trades"]
    assert "ZZQQ" in stub.requested["short_interest"]
    output = current(engine)["output"]
    ranked = {
        row["ticker"]: row
        for name in ("popular_stable", "affordable_growing", "unresolved")
        for row in output[name]
    }
    assert ranked["ZZQQ"]["signals"]["congress"]["buy_count"] == 40
    assert ranked["ZZQQ"]["signals"]["insider"] is not None
    assert ranked["ZZQQ"]["signals"]["short_interest"] is not None

    d, w, p = (
        models.run_dependencies,
        models.publication_history_watermarks,
        models.publication_sources,
    )
    with engine.connect() as conn:
        pinned = conn.execute(
            select(w.c.through_history_id).where(w.c.publication_id == pub)
        ).scalar_one()
        planned = conn.execute(
            select(p.c.source, d.c.history_through_id)
            .join(d, d.c.run_id == p.c.run_id)
            .where(p.c.publication_id == pub, d.c.source == "congress_trades")
        ).all()
    assert sorted(planned) == [("insider_trades", pinned), ("short_interest", pinned)]


def test_publish_refuses_downstream_runs_planned_from_different_history(
    engine, fixture_path
):
    from src.pipeline.publication import PublicationError

    derive(engine, congress_run(engine, page("a", 10)).run_id)
    with engine.begin() as conn:
        w = congress_history.watermark(conn)
    runs, batches = full_pipeline(
        engine,
        fixture_path,
        page("a", 10),
        through={"insider_trades": w, "short_interest": w - 1},
    )
    with pytest.raises(PublicationError, match="different Congress history"):
        publish(engine, runs, caps_for(engine, batches))


def test_publish_refuses_plans_that_predate_the_selected_batch(engine, fixture_path):
    from src.pipeline.publication import PublicationError

    derive(engine, congress_run(engine, page("a", 10)).run_id)
    with engine.begin() as conn:
        stale = congress_history.watermark(conn)
    # New trades arrive, but the downstream plans recorded the older watermark.
    runs, batches = full_pipeline(
        engine,
        fixture_path,
        page("b", 0),
        through={"insider_trades": stale, "short_interest": stale},
    )
    with pytest.raises(PublicationError, match="predate the selected Congress batch"):
        publish(engine, runs, caps_for(engine, batches))
