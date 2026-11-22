"""--job sentiment collects social posts before the weighting. Synthetic only:
the platform scripts never run; collect_reports and run_script are replaced."""

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import func, select
from test_credibility import (  # noqa: F401  (seeded is a fixture)
    DOMAINS,
    NEWS,
    NOW,
    T0,
    NewsClient,
    Resolver,
    score_articles,
    seeded,
    stored,
)

from sentiment import sentiment_manager as manager
from src.db import models
from src.services.credibility import job
from src.services.credibility.job import collect_social_posts, run_sentiment
from src.services.credibility.social import run_social
from src.settings import Settings

STEPS = [
    "news_articles",
    "article_sentiment",
    "news_publishers",
    "social_posts",
    "ticker_sentiment",
]


def social_rows(engine):
    with engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(models.social_posts)).scalar()


# ── the job ──────────────────────────────────────────────


@pytest.mark.db
def test_social_posts_are_collected_before_the_weighting_and_counted(engine, seeded):
    order = []

    def collect(engine, now):
        order.append(("social", stored(engine) == {}))  # no weighting yet
        return run_social(engine, now, seeded, window_days=7)

    run = run_sentiment(engine, NewsClient(NEWS), Resolver(DOMAINS),
                        score_articles=score_articles, collect_social=collect, now=NOW)
    assert run.succeeded
    assert list(run.seconds) == STEPS
    assert order == [("social", True)]
    assert run.results["social_posts"].succeeded
    rklb = stored(engine)["RKLB"]
    assert (rklb["news_count"], rklb["social_count"]) == (2, 2)
    # the batch is dated the run's day, so it is fresh for the weighting
    with engine.connect() as conn:
        as_of = conn.execute(select(models.source_batches.c.source_as_of).where(
            models.source_batches.c.id == run.results["social_posts"].batch_id)).scalar()
    assert as_of == NOW.date()


@pytest.mark.db
def test_a_failing_social_collection_does_not_stop_the_weighting(engine, seeded):
    def broken(engine, now):
        raise RuntimeError("boom")

    run = run_sentiment(engine, NewsClient(NEWS), Resolver(DOMAINS),
                        score_articles=score_articles, collect_social=broken, now=NOW)
    assert run.succeeded and "social_posts" not in run.results
    assert list(run.seconds) == STEPS
    assert stored(engine)["RKLB"]["social_count"] == 0


@pytest.mark.db
def test_without_collect_social_the_job_runs_as_before(engine, seeded):
    run = run_sentiment(engine, NewsClient(NEWS), Resolver(DOMAINS),
                        score_articles=score_articles, now=NOW)
    assert "social_posts" not in run.seconds and social_rows(engine) == 0


@pytest.mark.db
def test_collect_social_posts_stores_what_the_scripts_handed_over(
    engine, seeded, monkeypatch
):
    calls = []

    def fake(days=None, minutes=None):
        calls.append(minutes)
        return manager.COLLECTED, seeded, []

    monkeypatch.setattr(manager, "collect_reports", fake)
    result = collect_social_posts(engine, NOW, minutes=12)
    assert result.succeeded and result.accepted_count == 2
    assert calls == [12]


@pytest.mark.db
def test_every_platform_failing_is_recorded_as_missing_upstream(engine, seeded, monkeypatch):
    monkeypatch.setattr(manager, "collect_reports",
                        lambda days=None, minutes=None: (manager.COLLECTED, {}, []))
    result = collect_social_posts(engine, NOW)
    assert result.error_code == "missing_upstream" and social_rows(engine) == 0


@pytest.mark.db
@pytest.mark.parametrize("status", [manager.NO_TICKERS, manager.STOPPED])
def test_nothing_is_stored_without_tickers_or_after_ctrl_c(engine, seeded, monkeypatch,
                                                           status):
    monkeypatch.setattr(manager, "collect_reports",
                        lambda days=None, minutes=None: (status, seeded, []))
    assert collect_social_posts(engine, NOW) is None
    assert social_rows(engine) == 0


@pytest.fixture
def no_scripts(monkeypatch):
    """collect_social_posts without the platform scripts: nothing collected."""
    monkeypatch.setattr(manager, "collect_reports",
                        lambda days=None, minutes=None: (manager.COLLECTED, {}, []))


@pytest.mark.db
def test_an_older_social_batch_still_counts_while_inside_the_window(engine, seeded,
                                                                    no_scripts):
    # the scripts all failed today; yesterday's batch is still in the 7-day window
    assert run_social(engine, T0 - timedelta(days=1), seeded, window_days=7).succeeded
    run = run_sentiment(engine, NewsClient(NEWS), Resolver(DOMAINS),
                        score_articles=score_articles,
                        collect_social=lambda e, now: collect_social_posts(e, now),
                        now=NOW)
    assert run.succeeded
    assert run.results["social_posts"].error_code == "missing_upstream"
    assert stored(engine)["RKLB"]["social_count"] == 2


# ── run_once wiring ──────────────────────────────────────


def settings(**overrides):
    return Settings(_env_file=None,
                    database_url="postgresql+psycopg://user:synthetic@localhost/test",
                    finnhub_api_key="synthetic-finnhub", **overrides)


@pytest.mark.parametrize("enabled", [True, False])
def test_run_once_collects_social_posts_unless_turned_off(monkeypatch, enabled):
    seen = {}

    class Run:
        succeeded = True

    def fake_run(engine, news, resolver, *, score_articles=None, collect_social=None):
        seen["collect"] = collect_social
        return Run()

    class Engine:
        def dispose(self):
            pass

    monkeypatch.setattr(job, "run_sentiment", fake_run)
    monkeypatch.setattr("src.db.session.make_engine", lambda *a, **k: Engine())
    assert job.run_once(settings(social_collection=enabled, social_time_minutes=5)) == 0
    if enabled:
        assert seen["collect"].func is collect_social_posts
        assert seen["collect"].keywords == {"minutes": 5}
    else:
        assert seen["collect"] is None


def test_the_social_settings_have_safe_defaults_and_bounds():
    s = settings()
    assert (s.social_collection, s.social_time_minutes) == (True, 30)
    with pytest.raises(ValueError):
        settings(social_time_minutes=0)


# ── sentiment_manager: collect() never writes ────────────


def fake_scripts(monkeypatch, outcomes, tickers=frozenset({"RKLB"})):
    """Replace the subprocesses: each script 'prints' its report or fails."""
    async def fetch():
        return set(tickers) or None

    async def run_script(script, args=(), reports=None):
        status, report = outcomes.get(script.name, ("ok", None))
        if report is not None:
            reports[script.name] = report
        return manager.Result(script.name, status, note=" ".join(args))

    monkeypatch.setattr(manager, "fetch_tickers", fetch)
    monkeypatch.setattr(manager, "run_script", run_script)
    monkeypatch.setattr(manager, "say", lambda name, text: None)
    monkeypatch.setattr(manager, "print_summary", lambda results, total: None)
    written = []
    monkeypatch.setattr(manager, "write_posts",
                        lambda reports: written.append(reports)
                        or manager.Result(manager.DB_WRITE, "ok"))
    return written


REPORT = [{"ticker": "RKLB", "posts": {"p": 0.5}}]


def test_collect_reports_returns_the_reports_and_writes_nothing(monkeypatch):
    written = fake_scripts(monkeypatch, {"bluesky": ("ok", REPORT),
                                         "youtube": ("failed", None)})
    status, reports, results = manager.collect_reports(minutes=7)
    assert status == manager.COLLECTED and reports == {"bluesky": REPORT}
    assert written == []
    by_name = {r.name: r for r in results}
    assert by_name["youtube"].status == "failed"
    assert by_name["bluesky"].note == "--time 7"  # the cap reaches the scripts


def test_collect_reports_stops_without_tickers(monkeypatch):
    fake_scripts(monkeypatch, {}, tickers=frozenset())
    assert manager.collect_reports()[0] == manager.NO_TICKERS


def test_run_all_still_writes_the_batch_when_run_by_hand(monkeypatch):
    written = fake_scripts(monkeypatch, {"mastodon": ("ok", REPORT)})
    assert asyncio.run(manager.run_all(None, None)) == 0
    assert written == [{"mastodon": REPORT}]


def test_run_all_does_not_write_after_ctrl_c_in_the_crawler(monkeypatch):
    written = fake_scripts(monkeypatch, {"mastodon-crawler": ("stopped", None)})
    assert asyncio.run(manager.run_all(None, None)) == 130
    assert written == []
