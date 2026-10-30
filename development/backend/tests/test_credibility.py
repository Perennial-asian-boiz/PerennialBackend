"""S1-05 source credibility weighting: weights, publishers, social import, ticker sentiment. Synthetic only."""

import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
import requests
from sqlalchemy import insert, select

from src.db import models
from src.ingestion.importer import CollectionOutcome, import_collection
from src.pipeline.lifecycle import begin_run
from src.services.credibility import job
from src.services.credibility.job import (
    Item, derive_sentiment, run_sentiment, social_to_unit, weigh)
from src.services.credibility.publishers import (
    PublisherResolver, host_of, resolve_publishers)
from src.services.credibility.social import collect_social, platform_file, run_social
from src.services.credibility.weights import load_weights, parse_weights
from src.services.fundamentals.job import Universe
from src.services.news import job as news_job
from src.services.news.finnhub import RawArticle
from src.services.news.job import run_news

T0 = datetime(2026, 10, 26, 12, tzinfo=timezone.utc)
NOW = T0 + timedelta(days=1)
TABLE = parse_weights({"version": "test-v1", "tiers": {
    "major": {"weight": 1.0, "sources": ["reuters.com"]},
    "media": {"weight": 0.7, "sources": ["yahoo.com"]},
    "social": {"weight": 0.2, "sources": ["reddit", "youtube"]},
}})


# ── weight table (AC 1, AC 2) ────────────────────────────


def test_the_committed_table_loads_with_a_version_and_four_tiers():
    table = load_weights()
    assert table.version == "weights-v1"
    assert sorted(set(table.weights.values())) == [0.2, 0.4, 0.7, 1.0]
    assert table.weight_for("reuters.com") == 1.0 and table.weight_for("reddit") == 0.2


def test_subdomains_take_their_parent_weight_and_unknown_sources_the_lowest():
    assert TABLE.weight_for("finance.yahoo.com") == 0.7
    assert TABLE.weight_for("REUTERS.COM") == 1.0
    assert TABLE.weight_for("someblog.net") == 0.2  # AC 2: the lowest listed weight
    assert TABLE.weight_for(None) == 0.2
    assert TABLE.weight_for("notreuters.com") == 0.2  # whole labels only


@pytest.mark.parametrize("data", [
    {"tiers": {"a": {"weight": 1, "sources": ["x.com"]}}},  # no version
    {"version": "v", "tiers": {"a": {"weight": 0, "sources": ["x.com"]}}},
    {"version": "v", "tiers": {"a": {"weight": 1.5, "sources": ["x.com"]}}},
    {"version": "v", "tiers": {"a": {"weight": 1, "sources": ["x.com"]},
                               "b": {"weight": 0.5, "sources": ["X.com"]}}},
    {"version": "v", "tiers": {"a": {"weight": 1, "sources": []}}},
])
def test_a_malformed_table_is_refused(data):
    with pytest.raises(ValueError):
        parse_weights(data)


# ── weighting (AC 1) ─────────────────────────────────────


def item(score, weight, social=False, at=T0):
    return Item(score, weight, at, at, social)


def test_the_value_is_the_weighted_average_and_the_plain_average_is_kept():
    record = weigh("RKLB", [item(0.8, 1.0), item(-0.5, 0.2), item(1.0, 0.2, social=True)], "v",
                   "finbert")
    assert record["value"] == Decimal("0.6429")  # (0.8 - 0.1 + 0.2) / 1.4
    assert record["plain_average"] == Decimal("0.4333")
    assert (record["article_count"], record["news_count"], record["social_count"]) == (3, 2, 1)
    assert record["model_version"] == "finbert"


def test_the_model_is_named_only_when_news_is_counted():
    assert weigh("RKLB", [item(1.0, 0.2, social=True)], "v", "finbert")["model_version"] is None


def test_a_ticker_with_nothing_scored_has_counts_of_zero_and_no_value():
    record = weigh("ABSI", [], "v", None)
    assert record["article_count"] == 0 and record["value"] is None and record["first_at"] is None


def test_social_scores_map_onto_minus_one_to_one():
    assert [social_to_unit(s) for s in (1, 5.5, 10)] == [-1.0, 0.0, 1.0]


# ── publisher lookup ─────────────────────────────────────


@pytest.mark.parametrize("location,expected", [
    ("https://www.Reuters.com/markets/x", "reuters.com"),
    ("https://finance.yahoo.com/video/y.html", "finance.yahoo.com"),
    ("https://finnhub.io/api/news?id=1", None),
    ("ftp://example.com/x", None),
    (None, None),
])
def test_host_of(location, expected):
    assert host_of(location) == expected


class Response:
    def __init__(self, status, location=None):
        self.status_code, self.headers = status, {"Location": location} if location else {}

    def close(self):
        pass


class Session:
    def __init__(self, answers):
        self.answers, self.calls = answers, []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        answer = self.answers[url]
        if isinstance(answer, Exception):
            raise answer
        return answer


def test_the_resolver_reads_one_redirect_and_never_follows_it():
    session = Session({"a": Response(302, "https://www.reuters.com/x"), "b": Response(200),
                       "c": requests.ConnectionError()})
    slept, clock = [], [0.0]
    resolver = PublisherResolver(session, interval=1.1, monotonic=lambda: clock[0],
                                 sleep=slept.append)
    assert [resolver.resolve(u) for u in "abc"] == ["reuters.com", None, None]
    assert all(kw["allow_redirects"] is False for _, kw in session.calls)
    assert slept == [1.1, 1.1]


# ── social import ────────────────────────────────────────


def write_report(directory, platform, entries, modified=T0):
    path = platform_file(directory, platform)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(entries))
    os.utime(path, (modified.timestamp(), modified.timestamp()))


def test_fresh_files_are_read_and_stale_or_missing_ones_reported(tmp_path):
    write_report(tmp_path, "reddit", [
        {"ticker": "rklb", "posts": {"t3_a": 7.4, "t3_b": "bad", "t3_c": 11}},
        {"ticker": "ZZZZ", "posts": {"t3_d": 5}},
        "not an entry",
    ])
    write_report(tmp_path, "youtube", [{"ticker": "RKLB", "posts": {"v1": 2}}],
                 modified=T0 - timedelta(days=9))
    outcome = collect_social(tmp_path, lambda tickers: {"RKLB"} & set(tickers), NOW, 7)
    assert outcome.succeeded and outcome.source_as_of == date(2026, 10, 26)
    assert outcome.records == [
        {"ticker": "RKLB", "platform": "reddit", "post_id": "t3_a", "score": 7.4}]
    env = outcome.envelope
    assert (env["fresh"], env["stale"], env["missing"]) == (
        ["reddit"], ["youtube"], ["bluesky", "mastodon"])
    assert env["skipped_tickers"] == ["ZZZZ"]
    assert outcome.coverage == {"complete": False, "scope": "recent_window"}


def test_no_fresh_file_is_missing_upstream(tmp_path):
    outcome = collect_social(tmp_path, lambda tickers: set(tickers), NOW, 7)
    assert not outcome.succeeded and outcome.error_code == "missing_upstream"


# ── database: the whole chain (AC 1, AC 3, AC 4) ─────────


class NewsClient:
    def __init__(self, by_symbol):
        self.by_symbol, self.session = by_symbol, None

    def company_news(self, symbol, start, end):
        return list(self.by_symbol.get(symbol, [])), 0


class Resolver:
    def __init__(self, domains):
        self.domains, self.calls = domains, []

    def resolve(self, url):
        self.calls.append(url)
        return self.domains.get(url)


def article(fid, headline):
    return RawArticle(fid, headline, "", "Yahoo", f"https://finnhub.io/api/news?id={fid}", T0)


def url(fid):
    return f"https://finnhub.io/api/news?id={fid}"


NAMES = {"RKLB": "Rocket Lab", "NVDA": "NVIDIA Corp", "ABSI": "Absci Corp"}
NEWS = {"RKLB": [article(1, "Rocket Lab wins"), article(9, "Rocket Lab delays a launch")],
        "NVDA": [article(2, "NVDA rises")]}
DOMAINS = {url(1): "reuters.com", url(2): "finance.yahoo.com", url(9): "someblog.net"}
SCORES = {1: 0.8, 9: -0.5, 2: -0.2}


@pytest.fixture
def seeded(engine, monkeypatch, tmp_path):
    with engine.begin() as conn:
        for symbol, name in NAMES.items():
            sid = conn.execute(insert(models.securities).values(symbol=symbol, name=name)
                               .returning(models.securities.c.id)).scalar_one()
            conn.execute(insert(models.security_identifiers).values(
                scheme="ticker", value=symbol, security_id=sid))
    universe = Universe(sorted(NAMES), {}, {})
    monkeypatch.setattr(news_job, "current_universe", lambda conn: universe)
    monkeypatch.setattr(job, "current_universe", lambda conn: universe)
    write_report(tmp_path, "reddit", [{"ticker": "RKLB", "posts": {"t3_a": 10}}])
    write_report(tmp_path, "youtube", [{"ticker": "RKLB", "posts": {"v1": 1}}])
    monkeypatch.setattr("src.services.credibility.social.SOCIAL_DIR", tmp_path)
    return tmp_path


def score_articles(engine, model="finbert", scores=SCORES):
    """Stands in for S1-04: one score per stored article, through the contract."""
    with engine.connect() as conn:
        rows = conn.execute(select(models.securities.c.symbol, models.news_articles.c.id,
                                   models.news_articles.c.finnhub_id)
                            .join(models.securities)).all()
    records = [{"ticker": s, "news_article_id": i, "score": scores[f], "model": model}
               for s, i, f in rows]
    run_id = begin_run(engine, "article_sentiment", "derived")
    return import_collection(engine, CollectionOutcome.success(
        "article_sentiment", "derived", records,
        coverage={"complete": True, "scope": "derivation"}), run_id=run_id)


def stored(engine):
    with engine.connect() as conn:
        return {r["symbol"]: r for r in conn.execute(
            select(models.securities.c.symbol, models.ticker_sentiment)
            .join(models.securities)).mappings()}


@pytest.mark.db
def test_the_job_runs_every_step_and_stores_the_weighted_value_per_ticker(engine, seeded):
    resolver = Resolver(DOMAINS)
    run = run_sentiment(engine, NewsClient(NEWS), resolver, score_articles=score_articles,
                        now=NOW)
    assert run.succeeded
    assert list(run.seconds) == ["news_articles", "article_sentiment", "news_publishers",
                                 "social_posts", "ticker_sentiment"]
    assert all(r.succeeded for r in run.results.values())

    rows = stored(engine)
    rklb = rows["RKLB"]
    # reuters 1.0 x 0.8, unknown blog 0.2 x -0.5, reddit 0.2 x 1.0, youtube 0.2 x -1.0
    assert rklb["value"] == Decimal("0.4375")  # 0.7 / 1.6
    assert rklb["plain_average"] == Decimal("0.0750")
    assert (rklb["article_count"], rklb["news_count"], rklb["social_count"]) == (4, 2, 2)
    # social posts span the 7 days before their file's date; news are dated
    assert rklb["first_at"] == datetime(2026, 10, 19, 23, 59, 59, tzinfo=timezone.utc)
    assert rklb["last_at"] == datetime(2026, 10, 26, 23, 59, 59, tzinfo=timezone.utc)
    assert rklb["weights_version"] == "weights-v1"
    assert rklb["model_version"] == "finbert"
    nvda = rows["NVDA"]
    assert (nvda["value"], nvda["article_count"]) == (Decimal("-0.2000"), 1)
    assert nvda["first_at"] == nvda["last_at"] == T0
    absi = rows["ABSI"]
    assert (absi["article_count"], absi["value"], absi["first_at"]) == (0, None, None)
    assert absi["model_version"] is None

    with engine.connect() as conn:
        deps = set(conn.execute(select(models.run_dependencies.c.source).where(
            models.run_dependencies.c.run_id == run.results["ticker_sentiment"].run_id)
        ).scalars())
    assert deps == {"news_articles", "article_sentiment", "social_posts"}


@pytest.mark.db
def test_a_failed_lookup_is_retried_and_a_resolved_one_is_not(engine, seeded):
    run_news(engine, NewsClient(NEWS), T0.date())
    first = Resolver({url(1): "reuters.com"})
    assert resolve_publishers(engine, first, NOW, window_days=7) == (1, 2)
    second = Resolver(DOMAINS)
    assert resolve_publishers(engine, second, NOW, window_days=7) == (2, 0)
    assert sorted(second.calls) == sorted([url(2), url(9)])


@pytest.mark.db
def test_a_stale_social_batch_is_left_out(engine, seeded):
    run_news(engine, NewsClient(NEWS), T0.date())
    score_articles(engine)
    assert run_social(engine, NOW, window_days=7, directory=seeded).succeeded
    later = NOW + timedelta(days=10)  # the social batch and the articles are now old
    result = derive_sentiment(engine, now=later)
    assert result.succeeded
    assert {r["article_count"] for r in stored(engine).values()} == {0}


@pytest.mark.db
def test_only_the_newest_models_scores_are_used(engine, seeded):
    run_news(engine, NewsClient(NEWS), T0.date())
    score_articles(engine, "finbert")
    score_articles(engine, "finbert-v2", {1: 0.1, 9: 0.1, 2: 0.1})
    assert derive_sentiment(engine, now=NOW).succeeded
    nvda = stored(engine)["NVDA"]
    assert (nvda["value"], nvda["model_version"]) == (Decimal("0.1000"), "finbert-v2")


@pytest.mark.db
def test_a_failing_step_does_not_stop_the_weighting(engine, seeded):
    class Broken:
        session = None

        def company_news(self, symbol, start, end):
            raise RuntimeError("boom")

    def exploding(engine):
        raise RuntimeError("boom")

    run = run_sentiment(engine, Broken(), Resolver({}), score_articles=exploding, now=NOW)
    assert run.succeeded and "article_sentiment" not in run.results
    rklb = stored(engine)["RKLB"]
    assert (rklb["social_count"], rklb["model_version"]) == (2, None)


@pytest.mark.db
def test_no_published_watchlist_is_missing_upstream(engine, monkeypatch):
    monkeypatch.setattr(job, "current_universe", lambda conn: None)
    assert derive_sentiment(engine, now=NOW).error_code == "missing_upstream"
