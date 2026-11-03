"""Decision 114, JOB-7 article text tier 1: store Finnhub's summary and score
headline + summary. Spec-first; synthetic only, a fake scorer everywhere.

Pinned (each a contract choice; the reasons are in the handoff):

* Migration 0020 (after 0019) adds perennial.news_articles.summary, character
  varying(2000), nullable, no default. Rows stored before it keep NULL. Grants
  are unchanged: the importer inserts it and the reader reads it through their
  table grants; the API role still has nothing on news_articles.
* 0020's downgrade refuses, before any change, while any summary is stored (as
  0018 and 0019 refuse to lose lineage). With every summary NULL it drops the
  column and keeps the rows. A deliberate rollback nulls the summaries first.
* The news job stores Finnhub's summary, whitespace-normalized like the
  headline: blank or not a string -> NULL; over 2000 characters -> the first
  2000, never a refused article or a failed batch; a NUL character never fails
  the run. content_retrieved is "headline_and_summary" exactly when a summary
  is stored.
* JOB-7: article_text() = headline + "\\n" + summary, each stripped, capped at
  MAX_TEXT_CHARS (still 512); a NULL, blank or absent summary -> the headline
  alone. MODEL_VERSION = "ProsusAI/finbert@<the same revision>+text2" (63
  characters; the column holds 64), so headline-only scores (no suffix) and
  tier-1 scores never share a version.
* The bump re-scores the window once (recommendation, pending the
  coordinator): derive_sentiment reads only the newest batch's model, so the
  window must be scored under the new version for the sentiment counts to stay
  whole. Articles outside the window are never re-read.
* No summary text reaches a log line, a run's error_summary or its diagnostics,
  from the news job or the scorer.
"""

import ast
import re
import secrets
from datetime import timedelta

import pytest
from sqlalchemy import func, insert, select, text
from sqlalchemy.exc import DBAPIError
from src.db import models
from src.services.credibility import job as credibility_job
from src.services.fundamentals.job import Universe
from src.services.news import job as news_job
from src.services.news.finnhub import RawArticle, parse_articles
from src.services.news.job import run_news
from test_article_scorer import (
    OLD,
    Fake,
    batches,
    captured,
    run_of,
    scorer_module,
    scores,
    stored_articles,
)
from test_credibility import (  # noqa: F401  (seeded is a fixture)
    NOW,
    NewsClient,
    seeded,
)
from test_owner_migrations import MIGRATIONS
from test_read_api import groups  # noqa: F401  (a pytest fixture)

CAP = 2000
REVISION = "0bc2ac10dda6929bcb1f77ccc43c7ec5c424f031"  # unchanged by decision 114
HEADLINE_ONLY_VERSION = f"ProsusAI/finbert@{REVISION}"
TIER_1_VERSION = f"ProsusAI/finbert@{REVISION}+text2"
MARKER = "Zqxv synthetic summary phrase"  # never in a headline, a code or a name


def item(fid, headline, summary, at=NOW - timedelta(days=1)):
    """A Finnhub company-news item, as the API answers it."""
    return {
        "id": fid,
        "headline": headline,
        "summary": summary,
        "source": "Yahoo",
        "url": f"https://finnhub.io/api/news?id={fid}",
        "datetime": int(at.timestamp()),
        "related": "X",
    }


def parsed(*items):
    """The items through the real parser, as the client would hand them on."""
    articles, skipped = parse_articles(list(items))
    assert skipped == 0
    return articles


def news(fid, headline, summary, at):
    return RawArticle(
        fid, headline, summary, "Yahoo", f"https://finnhub.io/api/news?id={fid}", at
    )


def summaries(engine):
    with engine.connect() as conn:
        return dict(
            conn.execute(
                text("SELECT finnhub_id, summary FROM perennial.news_articles")
            ).all()
        )


def column_shape(conn, table):
    return {
        name: (kind, notnull)
        for name, kind, notnull in conn.execute(
            text(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull"
                " FROM pg_attribute a WHERE a.attrelid = to_regclass(:t)"
                " AND a.attnum > 0 AND NOT a.attisdropped"
            ),
            {"t": f"perennial.{table}"},
        )
    }


BEFORE_0020 = {
    "id": ("bigint", True),
    "batch_id": ("bigint", True),
    "security_id": ("bigint", True),
    "row_number": ("integer", True),
    "finnhub_id": ("bigint", True),
    "url": ("character varying(2048)", True),
    "headline": ("character varying(500)", True),
    "outlet": ("character varying(100)", True),
    "published_at": ("timestamp with time zone", True),
    "content_retrieved": ("character varying(24)", True),
    "match_rule": ("character varying(8)", True),
    "content_hash": ("character varying(64)", True),
}


# ── migration 0020: the file ─────────────────────────────


def _migration():
    found = sorted(MIGRATIONS.glob("*_0020_*.py"))
    assert len(found) == 1, found
    return found[0]


def _assigned(tree, name):
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == name for t in node.targets
        ):
            return node.value.value
    return None


def test_migration_0020_follows_0019():
    path = _migration()
    assert re.fullmatch(r"\d{8}_0020_[a-z0-9_]+\.py", path.name), path.name
    tree = ast.parse(path.read_text())
    assert (_assigned(tree, "revision"), _assigned(tree, "down_revision")) == (
        "0020",
        "0019",
    )


@pytest.mark.parametrize("function", ["upgrade", "downgrade"])
def test_both_directions_set_a_local_lock_timeout(function):
    source = _migration().read_text()
    body = next(
        n
        for n in ast.parse(source).body
        if isinstance(n, ast.FunctionDef) and n.name == function
    )
    assert re.search(
        r"SET LOCAL lock_timeout = '5s'", ast.get_source_segment(source, body)
    )


# ── migration 0020: the column ───────────────────────────


@pytest.mark.db
def test_summary_is_a_nullable_2000_character_column_and_nothing_else_changed(engine):
    with engine.connect() as conn:
        shape = column_shape(conn, "news_articles")
        default = conn.execute(
            text(
                "SELECT column_default FROM information_schema.columns WHERE"
                " table_schema = 'perennial' AND table_name = 'news_articles'"
                " AND column_name = 'summary'"
            )
        ).scalar()
    assert shape == {**BEFORE_0020, "summary": ("character varying(2000)", False)}
    assert default is None
    assert models.news_articles.c.summary.type.length == CAP


@pytest.mark.db
def test_the_database_refuses_a_summary_over_2000_characters(
    engine,
    seeded,  # noqa: F811
):
    run_news(
        engine,
        NewsClient({"RKLB": parsed(item(1, "Rocket Lab wins", "x"))}),
        NOW.date(),
    )
    with engine.begin() as conn:
        conn.execute(
            text(f"UPDATE perennial.news_articles SET summary = repeat('y', {CAP})")
        )
    with pytest.raises(DBAPIError) as refused, engine.begin() as conn:
        conn.execute(
            text(f"UPDATE perennial.news_articles SET summary = repeat('y', {CAP + 1})")
        )
    assert refused.value.orig.sqlstate == "22001"


@pytest.mark.db
def test_grants_are_unchanged_and_the_api_role_cannot_read_a_summary(
    engine,
    groups,  # noqa: F811
):
    from test_scores_schema import _provision_roles

    roles = _provision_roles()
    assert "news_articles" in roles.IMMUTABLE
    assert "news_articles" not in roles.API_TABLES
    assert "news_articles" not in roles.API_COLUMNS
    allowed = {
        ("importer", "INSERT"): True,
        ("importer", "SELECT"): True,
        ("importer", "UPDATE"): False,
        ("reader", "SELECT"): True,
        ("reader", "INSERT"): False,
        ("api", "SELECT"): False,
        ("api", "INSERT"): False,
        ("api", "UPDATE"): False,
    }
    with engine.connect() as conn:
        for (role, privilege), expected in allowed.items():
            assert (
                conn.execute(
                    text(
                        "SELECT has_column_privilege(:r, 'perennial.news_articles', 'summary', :p)"
                    ),
                    {"r": groups[role], "p": privilege},
                ).scalar()
                is expected
            ), (role, privilege)


# ── migration 0020: up and down on a database of its own ─


@pytest.fixture
def scratch(migrated_engine, monkeypatch):
    """An empty database on the test server, dropped afterwards."""
    from conftest import pinned_engine

    url = migrated_engine.url
    name = f"perennial_test_d114_{secrets.token_hex(4)}"
    admin = pinned_engine(url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    target = pinned_engine(url.set(database=name))
    monkeypatch.setattr(
        news_job, "current_universe", lambda conn: Universe(["RKLB"], {}, {})
    )
    try:
        yield target
    finally:
        target.dispose()
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


def migrate(target, revision, *, down=False):
    from alembic import command
    from conftest import alembic_config

    with target.connect() as conn:
        (command.downgrade if down else command.upgrade)(alembic_config(conn), revision)
        conn.commit()


def version(target):
    with target.connect() as conn:
        return conn.execute(
            text("SELECT version_num FROM public.alembic_version")
        ).scalar()


def has_summary(target):
    with target.connect() as conn:
        return "summary" in column_shape(conn, "news_articles")


def seed_rklb(target):
    with target.begin() as conn:
        sid = conn.execute(
            insert(models.securities)
            .values(symbol="RKLB", name="Rocket Lab")
            .returning(models.securities.c.id)
        ).scalar_one()
        conn.execute(
            insert(models.security_identifiers).values(
                scheme="ticker", value="RKLB", security_id=sid
            )
        )


def article_count(target):
    with target.connect() as conn:
        return conn.execute(
            text("SELECT count(*) FROM perennial.news_articles")
        ).scalar()


@pytest.mark.db
def test_with_no_summary_stored_the_downgrade_drops_the_column_and_keeps_the_rows(
    scratch,
):
    migrate(scratch, "head")
    seed_rklb(scratch)
    first = run_news(
        scratch,
        NewsClient(
            {
                "RKLB": parsed(
                    item(1, "Rocket Lab wins", ""), item(2, "Rocket Lab delays", " ")
                )
            }
        ),
        NOW.date(),
    )
    assert first.succeeded and article_count(scratch) == 2
    migrate(scratch, "0019", down=True)
    assert version(scratch) == "0019"
    assert not has_summary(scratch)
    assert article_count(scratch) == 2
    migrate(scratch, "head")  # over stored rows: they keep a NULL summary
    assert version(scratch) == "0020" and has_summary(scratch)
    assert set(summaries(scratch).values()) == {None}


@pytest.mark.db
def test_while_a_summary_is_stored_the_downgrade_refuses_and_changes_nothing(scratch):
    migrate(scratch, "head")
    seed_rklb(scratch)
    run_news(
        scratch,
        NewsClient({"RKLB": parsed(item(1, "Rocket Lab wins", f"{MARKER} one"))}),
        NOW.date(),
    )
    with pytest.raises(RuntimeError) as refused:
        migrate(scratch, "0019", down=True)
    assert MARKER not in str(refused.value)
    assert version(scratch) == "0020" and has_summary(scratch)
    assert summaries(scratch) == {1: f"{MARKER} one"}
    # The runbook's deliberate rollback: null the summaries, then downgrade.
    with scratch.begin() as conn:
        conn.execute(text("UPDATE perennial.news_articles SET summary = NULL"))
    migrate(scratch, "0019", down=True)
    assert version(scratch) == "0019" and article_count(scratch) == 1


# ── the news job stores the summary ──────────────────────


@pytest.mark.db
def test_the_news_job_stores_finnhubs_summary_and_null_when_there_is_none(
    engine,
    seeded,  # noqa: F811
):
    client = NewsClient(
        {
            "RKLB": parsed(
                item(
                    1,
                    "Rocket Lab wins a launch contract",
                    "  Rocket Lab won a NASA contract.  ",
                ),
                item(
                    2,
                    "Rocket Lab delays a launch",
                    "Weather  moved\n the launch\tto Friday.",
                ),
                item(3, "Rocket Lab hires", ""),
                item(4, "Rocket Lab expands", "   \n\t "),
                item(5, "Rocket Lab files", None),
                item(6, "Rocket Lab ships", 12345),
            )
        }
    )
    result = run_news(engine, client, NOW.date())
    assert result.succeeded and result.accepted_count == 6
    assert summaries(engine) == {
        1: "Rocket Lab won a NASA contract.",
        2: "Weather moved the launch to Friday.",  # normalized like the headline
        3: None,
        4: None,
        5: None,
        6: None,
    }
    retrieved = {f: a.content_retrieved for f, a in stored_articles(engine).items()}
    assert retrieved == {
        1: "headline_and_summary",
        2: "headline_and_summary",
        3: "headline_only",
        4: "headline_only",
        5: "headline_only",
        6: "headline_only",
    }


@pytest.mark.db
def test_an_over_long_summary_is_cut_to_2000_characters_not_refused(
    engine,
    seeded,  # noqa: F811
):
    long = "Rocket Lab grew. " + "x" * 3000  # the cut falls inside the x's
    client = NewsClient(
        {
            "RKLB": parsed(
                item(1, "Rocket Lab wins", long),
                item(2, "Rocket Lab delays", "short"),
                item(3, "Rocket Lab hires", "z" * CAP),
            )
        }
    )
    result = run_news(engine, client, NOW.date())
    assert result.succeeded and result.accepted_count == 3
    stored = summaries(engine)
    assert stored[1] == long[:CAP] and len(stored[1]) == CAP
    assert stored[2] == "short"
    assert stored[3] == "z" * CAP  # exactly at the cap: kept whole


@pytest.mark.db
def test_a_nul_character_in_a_summary_never_fails_the_run(
    engine,
    seeded,  # noqa: F811
):
    client = NewsClient(
        {
            "RKLB": parsed(
                item(1, "Rocket Lab wins", "Rocket\x00 Lab grew"),
                item(2, "Rocket Lab delays", "fine"),
            )
        }
    )
    result = run_news(engine, client, NOW.date())
    assert result.succeeded and result.accepted_count == 2
    assert summaries(engine) == {1: "Rocket Lab grew", 2: "fine"}


# ── JOB-7: the text and the version ──────────────────────


def test_the_model_version_names_the_tier_1_text_recipe():
    mod = scorer_module()
    assert mod.MODEL_REVISION == REVISION
    assert mod.MODEL_VERSION == TIER_1_VERSION
    assert mod.MODEL_VERSION != HEADLINE_ONLY_VERSION
    assert (
        len(mod.MODEL_VERSION) <= 64
    )  # article_sentiment.model, ticker_sentiment.model_version
    assert mod.MAX_TEXT_CHARS == 512


@pytest.mark.parametrize(
    "article,expected",
    [
        (
            {"headline": " Rocket Lab wins ", "summary": " It won a contract. "},
            "Rocket Lab wins\nIt won a contract.",
        ),
        ({"headline": "Rocket Lab wins", "summary": None}, "Rocket Lab wins"),
        ({"headline": "Rocket Lab wins", "summary": ""}, "Rocket Lab wins"),
        ({"headline": "Rocket Lab wins", "summary": "  \n "}, "Rocket Lab wins"),
        ({"headline": " Rocket Lab wins "}, "Rocket Lab wins"),
    ],
    ids=["headline_and_summary", "null", "empty", "blank", "absent"],
)
def test_article_text_is_the_headline_then_the_summary(article, expected):
    assert scorer_module().article_text(article) == expected


def test_article_text_is_capped_at_max_text_chars():
    mod = scorer_module()
    article = {"headline": "Rocket Lab wins", "summary": "x" * CAP}
    got = mod.article_text(article)
    assert got == ("Rocket Lab wins\n" + "x" * CAP)[: mod.MAX_TEXT_CHARS]
    assert len(got) == mod.MAX_TEXT_CHARS


# ── JOB-7: the run ───────────────────────────────────────


@pytest.mark.db
def test_the_scorer_reads_headline_and_stored_summary_and_stores_the_new_version(
    engine,
    seeded,  # noqa: F811
):
    client = NewsClient(
        {
            "RKLB": [
                news(
                    11,
                    "Rocket Lab wins a launch contract",
                    "It beat two rivals.",
                    NOW - timedelta(days=1),
                ),
                news(12, "Rocket Lab delays a launch", "", NOW - timedelta(days=2)),
            ]
        }
    )
    run_news(engine, client, NOW.date())
    fake = Fake(
        {
            "Rocket Lab wins a launch contract\nIt beat two rivals.": {
                "positive": 0.8,
                "negative": 0.1,
                "neutral": 0.1,
            },
            "Rocket Lab delays a launch": {
                "positive": 0.05,
                "negative": 0.9,
                "neutral": 0.05,
            },
        }
    )
    result = scorer_module().score_articles(engine, scorer=fake, now=NOW)
    assert result.succeeded, result.error_code
    assert sorted(fake.texts) == [  # their order is test_article_scorer's
        "Rocket Lab delays a launch",
        "Rocket Lab wins a launch contract\nIt beat two rivals.",
    ]
    stored = scores(engine)
    assert {f: str(r.score) for f, r in stored.items()} == {11: "0.7000", 12: "-0.8500"}
    assert {r.model for r in stored.values()} == {TIER_1_VERSION}


WINDOW_NEWS = {
    "RKLB": [
        news(11, "Rocket Lab wins a launch contract", "", NOW - timedelta(days=1)),
        news(12, "Rocket Lab delays a launch", "", NOW - timedelta(days=2)),
    ],
    "NVDA": [news(13, "NVDA rises", "", NOW - timedelta(days=3))],
}


def sentiment_counts(engine):
    with engine.connect() as conn:
        return {
            r.symbol: (r.news_count, r.model_version)
            for r in conn.execute(
                select(
                    models.securities.c.symbol,
                    models.ticker_sentiment.c.news_count,
                    models.ticker_sentiment.c.model_version,
                )
                .select_from(models.ticker_sentiment)
                .join(models.securities)
                .where(
                    models.ticker_sentiment.c.batch_id
                    == select(
                        func.max(models.ticker_sentiment.c.batch_id)
                    ).scalar_subquery()
                )
            )
        }


@pytest.mark.db
def test_the_bump_rescores_the_window_once_and_sentiment_keeps_every_article(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
):
    """The re-score recommendation: scored under the headline-only version before the
    bump, the window is scored once more under the new one, so derive_sentiment
    (which reads only the newest model) still counts every article."""
    mod = scorer_module()
    run_news(
        engine,
        NewsClient({**WINDOW_NEWS, "NVDA": WINDOW_NEWS["NVDA"] + OLD["NVDA"]}),
        NOW.date(),
    )
    with monkeypatch.context() as before_the_bump:
        before_the_bump.setattr(mod, "MODEL_VERSION", HEADLINE_ONLY_VERSION)
        assert mod.score_articles(engine, scorer=Fake(), now=NOW).succeeded
    assert credibility_job.derive_sentiment(engine, now=NOW).succeeded
    before = sentiment_counts(engine)
    assert before["RKLB"] == (2, HEADLINE_ONLY_VERSION) and before["NVDA"][0] == 1

    fake = Fake()
    result = mod.score_articles(engine, scorer=fake, now=NOW)
    assert result is not None and result.succeeded  # None: nothing re-scored
    assert sorted(fake.texts) == [
        "NVDA rises",
        "Rocket Lab delays a launch",
        "Rocket Lab wins a launch contract",
    ]  # the window only: 14 is older and never read
    with engine.connect() as conn:
        versions = conn.execute(
            select(models.news_articles.c.finnhub_id, models.article_sentiment.c.model)
            .select_from(models.article_sentiment)
            .join(models.news_articles)
        ).all()
    assert sorted(versions) == sorted(
        (f, v) for f in (11, 12, 13) for v in (HEADLINE_ONLY_VERSION, TIER_1_VERSION)
    )  # the old rows stay; one new row per article
    assert credibility_job.derive_sentiment(engine, now=NOW).succeeded
    after = sentiment_counts(engine)
    assert {s: n for s, (n, _) in after.items()} == {
        s: n for s, (n, _) in before.items()
    }
    assert after["RKLB"][1] == TIER_1_VERSION

    again = Fake()
    mod.score_articles(engine, scorer=again, now=NOW)
    assert again.texts == []  # once


# ── no summary text in a log line or a run record ────────


@pytest.mark.db
def test_the_news_job_never_logs_a_summary(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
):
    client = NewsClient(
        {
            "RKLB": parsed(item(1, "Rocket Lab wins", f"{MARKER} for Rocket Lab")),
            "NVDA": parsed(item(2, "NVDA rises", f"{MARKER} for NVDA")),
        }
    )
    with captured(monkeypatch) as handler:
        result = run_news(engine, client, NOW.date())
    assert result.succeeded
    assert MARKER in " ".join(
        v or "" for v in summaries(engine).values()
    )  # precondition
    run = run_of(engine, result)
    leaked = " ".join(
        [*handler.messages, str(run["error_summary"]), str(run["diagnostics"])]
    )
    assert MARKER not in leaked


class Quoting:
    """A scorer whose failure quotes the text it was given, as a tokenizer might."""

    def __init__(self):
        self.texts = []

    def __call__(self, texts):
        self.texts += list(texts)
        raise ValueError(f"cannot score {texts[0]!r}")


@pytest.mark.db
def test_a_scoring_failure_never_logs_or_records_a_summary(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
):
    client = NewsClient(
        {
            "RKLB": [
                news(
                    11,
                    "Rocket Lab wins",
                    f"{MARKER} in the summary",
                    NOW - timedelta(days=1),
                )
            ]
        }
    )
    run_news(engine, client, NOW.date())
    quoting = Quoting()
    with captured(monkeypatch) as handler:
        result = scorer_module().score_articles(engine, scorer=quoting, now=NOW)
    assert any(MARKER in t for t in quoting.texts)  # precondition: the scorer saw it
    assert (result.status, result.error_code) == ("failed", "scoring_failed")
    assert batches(engine) == 0
    run = run_of(engine, result)
    leaked = " ".join(
        [*handler.messages, str(run["error_summary"]), str(run["diagnostics"])]
    )
    assert MARKER not in leaked
