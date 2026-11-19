"""S3-02 piece 1: social post labels and the items behind each sentiment reading.

Spec: docs/technical-specs/SENTIMENT_PULSE.md. Synthetic only.

Pinned:
* One label rule (labels.py): score <= -0.2 negative, >= 0.2 positive, else
  neutral, used by the article scorer, the social import and migration 0026's
  CHECK and backfill; the numbers agree everywhere.
* run_social stores each post's label; a social_posts record whose label isn't
  its score's is refused.
* derive_sentiment stores, with each ticker_sentiment row, one
  ticker_sentiment_items row per counted article and post: its stored id, score,
  label and final weight. The counts equal news_count and social_count, and the
  weights and scores give back the stored value. An identical rerun on the same
  day reuses the batch and writes no items. An article scored before 0022 (no
  stored label) gets its score's label.
* The items are written in the batch's transaction: if they fail, no reading is
  stored and the run fails.
* Migration 0026 backfills labels, refuses a label that disagrees with its
  score, and refuses to downgrade while items exist.
"""

import secrets
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from test_credibility import (  # noqa: F401  (seeded: a pytest fixture)
    DOMAINS,
    NEWS,
    NOW,
    T0,
    NewsClient,
    Resolver,
    seeded,
)

from src.db import models
from src.ingestion.importer import CollectionOutcome, import_collection
from src.ingestion.schemas import SocialPost
from src.pipeline.lifecycle import begin_run
from src.services.credibility import labels
from src.services.credibility.job import Item, item_records, run_sentiment
from src.services.credibility.social import collect_social, run_social
from src.services.credibility.weights import load_weights

LABELED = {1: (0.8, "positive"), 9: (-0.5, "negative"), 2: (-0.2, "negative")}


# ── one label rule ───────────────────────────────────────


def _migration():
    import importlib.util
    from pathlib import Path

    path = (Path(__file__).resolve().parents[2] / "database" / "migrations" / "versions"
            / "20261118_0026_sentiment_breakdown.py")
    spec = importlib.util.spec_from_file_location("migration_0026", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_cutoffs_agree_in_code_migration_and_model():
    migration = _migration()
    assert Decimal(migration.NEGATIVE_AT_MOST) == labels.NEGATIVE_AT_MOST == Decimal("-0.2")
    assert Decimal(migration.POSITIVE_AT_LEAST) == labels.POSITIVE_AT_LEAST == Decimal("0.2")
    assert migration.LABEL_OF_SCORE == models.LABEL_OF_SCORE
    assert set(migration.LABELS) == set(labels.LABELS) == set(models.SENTIMENT_LABELS)


@pytest.mark.parametrize("score,label", [
    ("-1", "negative"), ("-0.2", "negative"), ("-0.1999", "neutral"), ("0", "neutral"),
    ("0.1999", "neutral"), ("0.2", "positive"), ("1", "positive"),
])
def test_label_for_is_inclusive_at_the_cutoffs(score, label):
    assert labels.label_for(Decimal(score)) == label


def test_the_article_scorer_uses_the_shared_rule():
    from src.services.credibility import article_scorer

    assert article_scorer.label_for is labels.label_for
    assert article_scorer.NEGATIVE_AT_MOST == labels.NEGATIVE_AT_MOST


# ── social labels ────────────────────────────────────────


def test_collected_posts_carry_their_scores_labels():
    reports = {"bluesky": [{"ticker": "RKLB", "posts": {
        "a": -0.2, "b": -0.19999, "c": -0.19994, "d": 0.19994, "e": 0.2, "f": 0.0}}]}
    outcome = collect_social(reports, lambda tickers: set(tickers), NOW, 7)
    got = {r["post_id"]: (r["score"], r["label"]) for r in outcome.records}
    # scores are stored to 4 places first, so -0.19999 is -0.2: negative
    assert got == {"a": (-0.2, "negative"), "b": (-0.2, "negative"), "c": (-0.1999, "neutral"),
                   "d": (0.1999, "neutral"), "e": (0.2, "positive"), "f": (0.0, "neutral")}


def test_a_post_whose_label_is_not_its_scores_is_refused():
    base = {"ticker": "RKLB", "platform": "bluesky", "post_id": "a", "score": "0.5"}
    assert SocialPost.model_validate({**base, "label": "positive"}).label == "positive"
    for wrong in ("neutral", "negative", "bullish", None):
        with pytest.raises(ValueError):
            SocialPost.model_validate({**base, "label": wrong})


# ── items behind each reading ────────────────────────────


def test_item_records_store_ids_labels_and_final_weights():
    news = Item(0.8, 0.123456789012, T0, T0, False, 11, "positive")
    old = Item(-0.3, 0.5, T0, T0, False, 12, None)  # scored before 0022: no label
    post = Item(-0.1, 0.2, T0, NOW, True, 7, "neutral")
    unstored = Item(0.5, 1.0, T0, T0, False)  # no id: not stored
    assert item_records([news, old, post, unstored]) == [
        {"kind": "news", "news_article_id": 11, "social_post_id": None,
         "score": Decimal("0.8000"), "label": "positive", "weight": Decimal("0.1234567890"),
         "item_at": T0},
        {"kind": "news", "news_article_id": 12, "social_post_id": None,
         "score": Decimal("-0.3000"), "label": "negative", "weight": Decimal("0.5000000000"),
         "item_at": T0},
        {"kind": "social", "news_article_id": None, "social_post_id": 7,
         "score": Decimal("-0.1000"), "label": "neutral", "weight": Decimal("0.2000000000"),
         "item_at": NOW},
    ]


def score_articles(engine, labeled=LABELED):
    """S1-04's stand-in with real labels: one score per stored article."""
    with engine.connect() as conn:
        rows = conn.execute(select(models.securities.c.symbol, models.news_articles.c.id,
                                   models.news_articles.c.finnhub_id)
                            .join(models.securities)).all()
    records = [{"ticker": s, "news_article_id": i, "score": labeled[f][0], "model": "finbert",
                "label": labeled[f][1], "confidence": "0.5000"} for s, i, f in rows]
    run_id = begin_run(engine, "article_sentiment", "derived")
    return import_collection(engine, CollectionOutcome.success(
        "article_sentiment", "derived", records,
        coverage={"complete": True, "scope": "derivation"}), run_id=run_id)


def run(engine, seeded):  # noqa: F811
    assert run_social(engine, T0, seeded, window_days=7).succeeded
    result = run_sentiment(engine, NewsClient(NEWS), Resolver(DOMAINS),
                           score_articles=score_articles, now=NOW)
    assert result.succeeded
    return result.results["ticker_sentiment"]


def readings(engine, batch_id):
    ts, ti, s = models.ticker_sentiment, models.ticker_sentiment_items, models.securities
    with engine.connect() as conn:
        rows = conn.execute(select(s.c.symbol, ts).join(s).where(ts.c.batch_id == batch_id)
                            ).mappings().all()
        items = conn.execute(select(ti).order_by(ti.c.id)).mappings().all()
    return ({r["symbol"]: dict(r) for r in rows},
            {r["symbol"]: [dict(i) for i in items if i["ticker_sentiment_id"] == r["id"]]
             for r in rows})


@pytest.mark.db
def test_every_counted_item_is_stored_with_its_reading(engine, seeded):  # noqa: F811
    result = run(engine, seeded)
    rows, items = readings(engine, result.batch_id)
    prior = Decimal(str(load_weights().prior_weight))
    for symbol, row in rows.items():
        mine = items[symbol]
        assert sum(i["kind"] == "news" for i in mine) == row["news_count"], symbol
        assert sum(i["kind"] == "social" for i in mine) == row["social_count"], symbol
        if mine:  # the weights and scores give back the stored value
            total = sum(i["weight"] for i in mine)
            value = sum(i["weight"] * i["score"] for i in mine) / (total + prior)
            assert abs(value - row["value"]) <= Decimal("0.0001"), symbol
    assert items["ABSI"] == []  # nothing counted, nothing stored

    rklb = items["RKLB"]
    news = sorted((i["score"], i["label"]) for i in rklb if i["kind"] == "news")
    assert news == [(Decimal("-0.5000"), "negative"), (Decimal("0.8000"), "positive")]
    social = sorted((i["score"], i["label"]) for i in rklb if i["kind"] == "social")
    assert social == [(Decimal("-1.0000"), "negative"), (Decimal("1.0000"), "positive")]
    with engine.connect() as conn:  # each item names the stored article or post
        for i in rklb:
            if i["kind"] == "news":
                assert i["social_post_id"] is None and conn.execute(select(
                    models.news_articles.c.id).where(
                    models.news_articles.c.id == i["news_article_id"])).scalar()
            else:
                assert i["news_article_id"] is None and conn.execute(select(
                    models.social_posts.c.label).where(
                    models.social_posts.c.id == i["social_post_id"])).scalar() == i["label"]
    # reuters 1.0 x 0.7937: the fade is in the stored weight
    assert max(i["weight"] for i in rklb if i["kind"] == "news") == Decimal("0.7937005260")
    # news are dated by publication; posts by the end of their collection day
    assert {i["item_at"] for i in rklb if i["kind"] == "news"} == {T0}
    assert {i["item_at"] for i in rklb if i["kind"] == "social"} == {
        datetime(2026, 10, 26, 23, 59, 59, tzinfo=timezone.utc)}


@pytest.mark.db
def test_stored_social_posts_carry_their_labels(engine, seeded):  # noqa: F811
    run(engine, seeded)
    with engine.connect() as conn:
        stored = set(conn.execute(select(models.social_posts.c.score,
                                         models.social_posts.c.label)).all())
    assert stored == {(Decimal("1.0000"), "positive"), (Decimal("-1.0000"), "negative")}


@pytest.mark.db
def test_an_identical_rerun_reuses_the_batch_and_writes_no_items(engine, seeded):  # noqa: F811
    first = run(engine, seeded)
    with engine.connect() as conn:
        before = conn.execute(text(
            "SELECT count(*) FROM perennial.ticker_sentiment_items")).scalar_one()
    again = run_sentiment(engine, NewsClient(NEWS), Resolver(DOMAINS),
                          score_articles=score_articles, now=NOW).results["ticker_sentiment"]
    assert again.reused_batch and again.batch_id == first.batch_id
    with engine.connect() as conn:
        assert conn.execute(text(
            "SELECT count(*) FROM perennial.ticker_sentiment_items")).scalar_one() == before


@pytest.mark.db
def test_items_that_fail_to_store_leave_no_reading(engine):
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO perennial.securities (symbol) VALUES ('ZZIT')"))
        conn.execute(text("INSERT INTO perennial.security_identifiers (scheme, value, security_id)"
                          " SELECT 'ticker', 'ZZIT', id FROM perennial.securities"
                          " WHERE symbol = 'ZZIT'"))
    record = {"ticker": "ZZIT", "value": "0.4000", "plain_average": "0.8000", "article_count": 1,
              "news_count": 1, "social_count": 0, "first_at": T0, "last_at": T0,
              "weights_version": "weights-v4", "model_version": "finbert",
              "social_sentiment": "0", "tallied_on": NOW.date(),
              "items": [{"kind": "news", "news_article_id": 2**40, "score": "0.8",
                         "label": "positive", "weight": "1", "item_at": T0}]}  # no such article
    run_id = begin_run(engine, "ticker_sentiment", "derived")
    result = import_collection(engine, CollectionOutcome.success(
        "ticker_sentiment", "derived", [record],
        coverage={"complete": True, "scope": "derivation"}), run_id=run_id)
    assert not result.succeeded and result.error_code == "database_error"
    with engine.connect() as conn:
        assert conn.execute(text(
            "SELECT count(*) FROM perennial.ticker_sentiment t JOIN perennial.securities s"
            " ON s.id = t.security_id WHERE s.symbol = 'ZZIT'")).scalar_one() == 0


def test_a_record_whose_items_disagree_with_its_counts_is_refused():
    from src.ingestion.schemas import TickerSentiment

    record = {"ticker": "ZZIT", "value": "0.4", "plain_average": "0.8", "article_count": 2,
              "news_count": 2, "social_count": 0, "first_at": T0, "last_at": T0,
              "weights_version": "weights-v4", "model_version": "finbert",
              "social_sentiment": "0", "tallied_on": NOW.date(),
              "items": [{"kind": "news", "news_article_id": 1, "score": "0.8",
                         "label": "positive", "weight": "1", "item_at": T0}]}
    with pytest.raises(ValueError):
        TickerSentiment.model_validate(record)
    bad_item = {"kind": "news", "social_post_id": 1, "score": "0.8", "label": "positive",
                "weight": "1", "item_at": T0}
    with pytest.raises(ValueError):
        TickerSentiment.model_validate({**record, "items": [bad_item, bad_item]})


# ── grants ───────────────────────────────────────────────


def test_the_api_may_read_only_the_breakdown_columns():
    from test_scores_schema import _provision_roles

    roles = _provision_roles()
    assert "ticker_sentiment_items" in roles.IMMUTABLE
    assert roles.API_COLUMNS["ticker_sentiment"] == ("id", "batch_id", "security_id", "tallied_on")
    assert roles.API_COLUMNS["ticker_sentiment_items"] == (
        "id", "ticker_sentiment_id", "kind", "label", "weight", "item_at")
    for table in ("news_articles", "article_sentiment", "social_posts", "ingestion_runs"):
        assert table not in roles.API_TABLES and table not in roles.API_COLUMNS, table


# ── migration 0026: up and down on a database of its own ─


@pytest.fixture
def scratch(migrated_engine):
    from conftest import pinned_engine

    url = migrated_engine.url
    name = f"perennial_test_s26_{secrets.token_hex(4)}"
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


SEED = """
WITH s AS (INSERT INTO perennial.securities (symbol) VALUES ('RKLB') RETURNING id),
     b AS (INSERT INTO perennial.source_batches (source, content_hash, hash_version,
                                                 record_count, payload)
           VALUES ('social_posts', repeat('a', 64), 'v1', 5, '{}') RETURNING id)
INSERT INTO perennial.social_posts (batch_id, security_id, row_number, platform, post_id, score)
SELECT b.id, s.id, v.n, 'youtube', 'v' || v.n, v.score
FROM s, b, (VALUES (1, -0.2), (2, -0.1999), (3, 0.1999), (4, 0.2), (5, -1)) AS v(n, score)
"""
ITEM = """
WITH b AS (INSERT INTO perennial.source_batches (source, content_hash, hash_version,
                                                 record_count, payload)
           VALUES ('ticker_sentiment', repeat('b', 64), 'v1', 1, '{}') RETURNING id),
     t AS (INSERT INTO perennial.ticker_sentiment (batch_id, security_id, row_number, value,
             plain_average, article_count, news_count, social_count, first_at, last_at,
             weights_version, social_sentiment, tallied_on)
           SELECT b.id, s.id, 1, -0.2, -0.2, 1, 0, 1, now(), now(), 'weights-v4', -0.2,
                  current_date FROM b, perennial.securities s RETURNING id)
INSERT INTO perennial.ticker_sentiment_items (ticker_sentiment_id, kind, social_post_id, score,
                                              label, weight, item_at)
SELECT t.id, 'social', p.id, p.score, p.label, 0.2, now()
FROM t, perennial.social_posts p WHERE p.row_number = 1
"""


@pytest.mark.db
def test_0026_labels_stored_posts_and_keeps_labels_true(scratch):
    migrate(scratch, "0025")
    with scratch.begin() as conn:
        conn.execute(text(SEED))
    migrate(scratch, "0026")
    with scratch.connect() as conn:
        got = conn.execute(text(
            "SELECT label FROM perennial.social_posts ORDER BY row_number")).scalars().all()
    assert got == ["negative", "neutral", "neutral", "positive", "negative"]
    for change in ("UPDATE perennial.social_posts SET label = 'positive' WHERE row_number = 1",
                   "UPDATE perennial.social_posts SET label = NULL WHERE row_number = 1",
                   "UPDATE perennial.social_posts SET label = 'bullish'"):
        with pytest.raises(DBAPIError) as caught, scratch.begin() as conn:
            conn.execute(text(change))
        assert caught.value.orig.sqlstate in ("23514", "23502"), change


@pytest.mark.db
def test_0026_items_check_their_kind_and_ranges(scratch):
    migrate(scratch, "0025")
    with scratch.begin() as conn:
        conn.execute(text(SEED))
    migrate(scratch, "0026")
    with scratch.begin() as conn:
        conn.execute(text(ITEM))
    bad = {
        "news item naming a post": "kind = 'news'",
        "negative weight": "weight = -0.1",
        "score out of range": "score = 1.5",
        "unknown label": "label = 'bullish'",
        "unknown kind": "kind = 'video'",
    }
    for what, change in bad.items():
        with pytest.raises(DBAPIError) as caught, scratch.begin() as conn:
            conn.execute(text(f"UPDATE perennial.ticker_sentiment_items SET {change}"))
        assert caught.value.orig.sqlstate == "23514", what
    with pytest.raises(DBAPIError) as caught, scratch.begin() as conn:  # same post twice
        conn.execute(text(
            "INSERT INTO perennial.ticker_sentiment_items (ticker_sentiment_id, kind,"
            " social_post_id, score, label, weight, item_at) SELECT ticker_sentiment_id, kind,"
            " social_post_id, score, label, weight, item_at"
            " FROM perennial.ticker_sentiment_items"))
    assert caught.value.orig.sqlstate == "23505"


@pytest.mark.db
def test_0026_downgrade_refuses_while_items_exist_then_drops_cleanly(scratch):
    migrate(scratch, "0025")
    with scratch.begin() as conn:
        conn.execute(text(SEED))
    migrate(scratch, "0026")
    with scratch.begin() as conn:
        conn.execute(text(ITEM))
    with pytest.raises(RuntimeError, match="ticker_sentiment_items"):
        migrate(scratch, "0025", down=True)
    with scratch.begin() as conn:
        conn.execute(text("DELETE FROM perennial.ticker_sentiment_items"))
    migrate(scratch, "0025", down=True)
    with scratch.connect() as conn:
        assert conn.execute(text(
            "SELECT to_regclass('perennial.ticker_sentiment_items')")).scalar() is None
        assert conn.execute(text(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'perennial'"
            " AND table_name = 'social_posts' AND column_name = 'label'")).scalar() == 0
        assert conn.execute(text("SELECT count(*) FROM perennial.social_posts")).scalar() == 5
