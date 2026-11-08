"""Social sentiment on [-1, 1]: the scorer's mapping and migration 0024 (social_posts.score)."""

import secrets
import sys
import types
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError


class _P:
    def __init__(self, v):
        self.v = v

    def item(self):
        return self.v


@pytest.fixture
def summarize(monkeypatch):
    """sentiment_scorer._summarize without torch / transformers installed."""
    for name in ("torch", "transformers"):
        if name not in sys.modules:
            stub = types.ModuleType(name)
            stub.AutoModelForSequenceClassification = stub.AutoTokenizer = None
            monkeypatch.setitem(sys.modules, name, stub)
    monkeypatch.delitem(sys.modules, "sentiment.sentiment_scorer", raising=False)
    from sentiment.sentiment_scorer import _summarize

    return _summarize


@pytest.mark.parametrize(
    "bullish,bearish,expected",
    [(0.0, 1.0, -1.0), (1.0, 0.0, 1.0), (0.3, 0.3, 0.0), (0.7, 0.1, 0.6)],
)
def test_scorer_maps_bearish_to_minus_one_and_bullish_to_one(
    summarize, bullish, bearish, expected
):
    neutral = 1.0 - bullish - bearish
    # index order: 0 neutral, 1 bullish, 2 bearish
    score, *_ = summarize([_P(neutral), _P(bullish), _P(bearish)], 1, 2, 0)
    assert score == pytest.approx(expected)


# ── migration 0024: up and down on a database of its own ─


@pytest.fixture
def scratch(migrated_engine):
    from conftest import pinned_engine

    url = migrated_engine.url
    name = f"perennial_test_s24_{secrets.token_hex(4)}"
    admin = pinned_engine(url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    target = pinned_engine(url.set(database=name))
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


def scores(target):
    with target.connect() as conn:
        return conn.execute(
            text("SELECT score FROM perennial.social_posts ORDER BY row_number")
        ).scalars().all()


SEED = """
WITH s AS (INSERT INTO perennial.securities (symbol) VALUES ('RKLB') RETURNING id),
     b AS (INSERT INTO perennial.source_batches (source, content_hash, hash_version,
                                                 record_count, payload)
           VALUES ('social_posts', repeat('a', 64), 'v1', 3, '{}') RETURNING id)
INSERT INTO perennial.social_posts (batch_id, security_id, row_number, platform, post_id, score)
SELECT b.id, s.id, v.n, 'youtube', 'v' || v.n, v.score
FROM s, b, (VALUES (1, 1.00), (2, 5.50), (3, 10.00)) AS v(n, score)
"""


@pytest.mark.db
def test_0024_converts_stored_scores_and_back(scratch):
    migrate(scratch, "0023")
    with scratch.begin() as conn:
        conn.execute(text(SEED))
    migrate(scratch, "0024")
    assert scores(scratch) == [Decimal("-1.0000"), Decimal("0.0000"), Decimal("1.0000")]
    with pytest.raises(DBAPIError) as caught, scratch.begin() as conn:
        conn.execute(text("UPDATE perennial.social_posts SET score = 1.5"))
    assert caught.value.orig.sqlstate == "23514"
    migrate(scratch, "0023", down=True)
    assert scores(scratch) == [Decimal("1.00"), Decimal("5.50"), Decimal("10.00")]


# ── the results hand-off: one stdout line, no file ───────


def test_results_line_round_trips_and_is_kept_out_of_the_console(capsys, monkeypatch):
    import asyncio

    from sentiment import sentiment_manager
    from sentiment.results import emit_results, parse_results_line

    report = [{"ticker": "NVDA", "average_sentiment": 0.5, "post_count": 1,
               "posts": {"a\r\nb": 0.5}}]
    emit_results(report)
    line = capsys.readouterr().out
    assert line.count("\n") == 1  # one line, whatever the post ids hold
    assert parse_results_line(line.rstrip("\n")) == report
    assert parse_results_line("plain console text") is None

    shown = []
    monkeypatch.setattr(sentiment_manager, "say", lambda name, text: shown.append(text))

    class Stream:
        def __init__(self, data):
            self.chunks = [data[i : i + 7] for i in range(0, len(data), 7)]

        async def read(self, n):
            return self.chunks.pop(0) if self.chunks else b""

    reports = {}
    asyncio.run(sentiment_manager.pump(
        Stream(("hello\n" + line + "bye").encode()), "bluesky", reports))
    assert reports == {"bluesky": report}
    assert shown == ["hello", "results received (1 tickers)", "bye"]
