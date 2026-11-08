"""#85's review findings, spec-first (checker and auditor, 2026-11-01). Synthetic only.

F1: the publisher lookup GETs a stored news_articles.url. Only an https URL whose
    host is exactly finnhub.io (no port, no userinfo) is ever requested; anything
    else is never requested and counts as failed. The redirect's host must also
    match news_publishers' domain CHECK (^[a-z0-9.-]+$, at most 253) before it is
    stored: a bad one counts as failed and the remaining lookups still run.
F2: an unexpected exception after begin_run in the credibility job records the
    run as failed with internal_error (the exception's class name and SQLSTATE
    only, like the score jobs) and writes nothing, instead of leaving it running.
"""

from datetime import timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from src.db import models
from src.services.credibility import job, social
from src.services.credibility.publishers import (
    PublisherResolver,
    host_of,
    resolve_publishers,
)
from src.services.news.finnhub import RawArticle
from src.services.news.job import run_news
from test_credibility import (  # noqa: F401  (seeded is a fixture)
    NOW,
    T0,
    NewsClient,
    seeded,
)

GOOD = "https://finnhub.io/api/news?id=1"


class Response:
    def __init__(self, location, status=302):
        self.status_code, self.headers, self.closed = (
            status,
            {"Location": location},
            False,
        )

    def close(self):
        self.closed = True


class Session:
    """Records every URL it's asked for; redirects each to its mapped Location."""

    def __init__(self, locations=None):
        self.locations, self.requested = locations or {}, []

    def get(self, url, **kwargs):
        self.requested.append(url)
        return Response(self.locations.get(url, "https://www.reuters.com/a"))


def resolver(session):
    return PublisherResolver(session, interval=0, sleep=lambda s: None)


# ── F1: only https://finnhub.io is ever requested ────────


def test_a_finnhub_https_link_is_requested_once():
    session = Session()
    assert resolver(session).resolve(GOOD) == "reuters.com"
    assert session.requested == [GOOD]


REFUSED = [
    "http://finnhub.io/api/news?id=1",  # clear text
    "https://evil.example/api/news?id=1",
    "https://api.finnhub.io/api/news?id=1",  # a subdomain is another host
    "https://finnhub.io.evil.example/api/news?id=1",
    "https://evilfinnhub.io/api/news?id=1",
    "https://finnhub.io./api/news?id=1",
    "https://finnhub.io@evil.example/api/news?id=1",  # userinfo: the host is evil.example
    "https://user:pass@finnhub.io/api/news?id=1",
    "https://finnhub.io:8443/api/news?id=1",
    "https://finnhub.io:443/api/news?id=1",
    "https://127.0.0.1/api/news?id=1",
    "https://localhost/api/news?id=1",
    "https://[::1]/api/news?id=1",
    "https://169.254.169.254/latest/meta-data/",
    "https://10.0.0.5/api/news?id=1",
    "ftp://finnhub.io/api/news?id=1",
    "finnhub.io/api/news?id=1",
    "",
]


@pytest.mark.parametrize("url", REFUSED)
def test_any_other_link_is_never_requested(url):
    session = Session()
    assert resolver(session).resolve(url) is None
    assert session.requested == []


BAD_HOSTS = [
    "https://exa_mple.com/a",  # underscore: refused by the domain CHECK
    "https://exämple.com/a",  # non-ASCII
    "https://" + "a" * 250 + ".com/a",  # longer than 253
    "https://exa mple.com/a",
]


@pytest.mark.parametrize("location", BAD_HOSTS)
def test_a_redirect_host_the_domain_check_would_refuse_is_no_host(location):
    assert host_of(location) is None


def test_a_well_formed_redirect_host_is_kept():
    assert host_of("https://www.Reuters.com/business/x") == "reuters.com"
    assert host_of("https://finance.yahoo.com/news/x") == "finance.yahoo.com"


def article(fid, url, headline="Rocket Lab wins"):
    return RawArticle(fid, headline, "", "Yahoo", url, T0)


def publishers(engine):
    with engine.connect() as conn:
        return dict(
            conn.execute(
                select(
                    models.news_publishers.c.finnhub_id, models.news_publishers.c.domain
                )
            ).all()
        )


def finnhub(fid):
    return f"https://finnhub.io/api/news?id={fid}"


@pytest.mark.db
def test_the_lookup_loop_requests_only_finnhub_and_counts_the_rest_failed(
    engine,
    seeded,  # noqa: F811
):
    stored = [
        article(1, finnhub(1)),
        article(2, "https://127.0.0.1/api/news?id=2"),
        article(3, "http://finnhub.io/api/news?id=3"),
        article(4, "https://evil.example/api/news?id=4"),
        article(5, finnhub(5)),
    ]
    run_news(engine, NewsClient({"RKLB": stored}), T0.date())
    with engine.connect() as conn:
        urls = conn.execute(select(models.news_articles.c.url)).scalars().all()
    assert len(urls) == 5, "precondition: every link is stored as Finnhub gave it"
    session = Session()
    assert resolve_publishers(engine, resolver(session), NOW, window_days=7) == (2, 3)
    assert session.requested == [finnhub(1), finnhub(5)]
    assert publishers(engine) == {1: "reuters.com", 5: "reuters.com"}


@pytest.mark.db
def test_a_bad_redirect_host_counts_as_failed_and_the_loop_goes_on(
    engine,
    seeded,  # noqa: F811
):
    run_news(
        engine,
        NewsClient(
            {
                "RKLB": [
                    article(1, finnhub(1)),
                    article(2, finnhub(2)),
                    article(3, finnhub(3)),
                ]
            }
        ),
        T0.date(),
    )
    session = Session(
        {finnhub(1): "https://exa_mple.com/a", finnhub(3): "https://www.reuters.com/x"}
    )
    session.locations[finnhub(2)] = "https://" + "b" * 250 + ".com/a"
    assert resolve_publishers(engine, resolver(session), NOW, window_days=7) == (1, 2)
    assert session.requested == [finnhub(1), finnhub(2), finnhub(3)]
    assert publishers(engine) == {3: "reuters.com"}


# ── F2: a crash after begin_run is recorded, not left running ──


def runs(engine, source):
    with engine.connect() as conn:
        return (
            conn.execute(
                select(models.ingestion_runs)
                .where(models.ingestion_runs.c.source == source)
                .order_by(models.ingestion_runs.c.id)
            )
            .mappings()
            .all()
        )


def batch_count(engine, source):
    with engine.connect() as conn:
        return conn.execute(
            select(func.count()).where(models.source_batches.c.source == source)
        ).scalar_one()


class Orig(Exception):
    sqlstate = "57014"


SECRET = "Rocket Lab headline text that must not be stored"


def boom(*args, **kwargs):
    raise RuntimeError(SECRET)


def db_boom(*args, **kwargs):
    raise OperationalError("SELECT secret", {"p": SECRET}, Orig(SECRET))


def assert_recorded(engine, source, result, name, sqlstate):
    [run] = runs(engine, source)
    assert (run["status"], run["error_code"]) == ("failed", "internal_error")
    assert run["finished_at"] is not None
    assert run["batch_id"] is None and batch_count(engine, source) == 0
    exception = {"name": name, **({"sqlstate": sqlstate} if sqlstate else {})}
    assert run["diagnostics"] == {
        "exception": exception
    }  # class name and SQLSTATE only
    assert SECRET not in str(run["error_summary"]) and SECRET not in str(
        run["diagnostics"]
    )
    assert result is not None and result.run_id == run["id"]
    assert (result.status, result.error_code) == ("failed", "internal_error")


@pytest.mark.db
@pytest.mark.parametrize(
    "where,raiser,name,sqlstate",
    [
        ("news_items", boom, "RuntimeError", None),
        ("news_items", db_boom, "OperationalError", "57014"),
        ("weigh", boom, "RuntimeError", None),
    ],
)
def test_a_crash_while_weighting_is_recorded_as_internal_error(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
    where,
    raiser,
    name,
    sqlstate,
):
    run_news(engine, NewsClient({"RKLB": [article(1, finnhub(1))]}), T0.date())
    monkeypatch.setattr(job, where, raiser)
    result = job.derive_sentiment(engine, now=NOW)
    assert_recorded(engine, "ticker_sentiment", result, name, sqlstate)


@pytest.mark.db
def test_a_crash_while_reading_social_reports_is_recorded_as_internal_error(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
):
    """The same rule for the package's other run, the social import."""
    monkeypatch.setattr(social, "collect_social", boom)
    result = social.run_social(engine, NOW, seeded, window_days=7)
    assert_recorded(engine, "social_posts", result, "RuntimeError", None)


@pytest.mark.db
def test_after_a_crash_the_next_weighting_runs_normally(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
):
    run_news(engine, NewsClient({"RKLB": [article(1, finnhub(1))]}), T0.date())
    with monkeypatch.context() as m:
        m.setattr(job, "news_items", boom)
        assert not job.derive_sentiment(engine, now=NOW).succeeded
    assert job.derive_sentiment(engine, now=NOW + timedelta(minutes=1)).succeeded
    assert [r["status"] for r in runs(engine, "ticker_sentiment")] == [
        "failed",
        "succeeded",
    ]
