"""ticker_sentiment.tallied_on (migration 0025): the date the scores were tallied.

* The weighting run stamps every row with its own UTC date (test_credibility
  checks the stored value end to end).
* tallied_on is part of the batch identity: an identical rerun on the same day
  reuses the batch and writes nothing, while one on a later day stores a new
  batch, since its tally date differs.
* 0025 backfills every existing row from its batch's imported_at (a UTC date),
  then makes the column NOT NULL; downgrade drops it and keeps the rows.
Synthetic only.
"""

import secrets
from datetime import date

import pytest
from pydantic import ValidationError
from sqlalchemy import func, insert, select, text
from sqlalchemy.exc import DBAPIError

from src.db import models
from src.ingestion.importer import CollectionOutcome, import_collection
from src.ingestion.schemas import TickerSentiment


def record(ticker="ZZTA", tallied_on="2026-11-17", **over):
    return {
        "ticker": ticker,
        "value": "0.3100",
        "plain_average": "0.2800",
        "article_count": 12,
        "news_count": 12,
        "social_count": 0,
        "first_at": "2026-11-10T14:05:00+00:00",
        "last_at": "2026-11-16T16:30:00+00:00",
        "weights_version": "weights-v1",
        "model_version": "ProsusAI/finbert@synthetic0001",
        "social_sentiment": "0",
        "tallied_on": tallied_on,
        **over,
    }


def test_the_record_requires_a_tally_date():
    assert TickerSentiment(**record()).tallied_on == date(2026, 11, 17)
    with pytest.raises(ValidationError, match="tallied_on"):
        TickerSentiment(**{k: v for k, v in record().items() if k != "tallied_on"})
    with pytest.raises(ValidationError, match="tallied_on"):
        TickerSentiment(**record(tallied_on="2026-11-17T09:30:00+00:00"))


def test_the_tally_date_is_part_of_the_batch_identity():
    from src.ingestion.canonical import canonicalize

    def digest(day):
        return canonicalize("ticker_sentiment", [TickerSentiment(**record(tallied_on=day))])

    assert digest("2026-11-17").content_hash == digest("2026-11-17").content_hash
    assert digest("2026-11-17").content_hash != digest("2026-11-18").content_hash


def _import(engine, records):
    return import_collection(
        engine,
        CollectionOutcome.success(
            "ticker_sentiment",
            "derived",
            records,
            coverage={"complete": True, "scope": "derivation"},
            envelope={"weights_version": "weights-v1", "window_days": 7},
        ),
    )


def _row_count(engine):
    with engine.connect() as conn:
        return conn.execute(
            select(func.count()).select_from(models.ticker_sentiment)
        ).scalar_one()


@pytest.mark.db
def test_a_same_day_rerun_is_deduped_and_a_next_day_one_is_stored(engine):
    with engine.begin() as conn:
        sid = conn.execute(insert(models.securities).values(symbol="ZZTA")
                           .returning(models.securities.c.id)).scalar_one()
        conn.execute(insert(models.security_identifiers).values(
            scheme="ticker", value="ZZTA", security_id=sid))

    first = _import(engine, [record()])
    assert first.succeeded and not first.reused_batch
    again = _import(engine, [record()])
    assert again.succeeded and again.reused_batch
    assert again.batch_id == first.batch_id
    assert _row_count(engine) == 1

    later = _import(engine, [record(tallied_on="2026-11-18")])
    assert later.succeeded and not later.reused_batch
    assert later.batch_id != first.batch_id
    with engine.connect() as conn:
        stored = dict(conn.execute(
            select(models.ticker_sentiment.c.batch_id, models.ticker_sentiment.c.tallied_on)
        ).all())
    assert stored == {first.batch_id: date(2026, 11, 17), later.batch_id: date(2026, 11, 18)}


# ── migration 0025: up and down on a database of its own ─


@pytest.fixture
def scratch(migrated_engine):
    from conftest import pinned_engine

    url = migrated_engine.url
    name = f"perennial_test_s25_{secrets.token_hex(4)}"
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


# Two batches imported late on 16 Nov in New York, which is already 17 Nov in UTC,
# and on 10 Nov; the backfill takes the UTC date.
SEED = """
WITH s AS (INSERT INTO perennial.securities (symbol) VALUES ('ZZTA') RETURNING id),
     b AS (INSERT INTO perennial.source_batches (source, content_hash, hash_version,
                                                 imported_at, record_count, payload)
           VALUES ('ticker_sentiment', repeat('a', 64), 'v2',
                   '2026-11-16 22:30:00-05', 1, '{}'),
                  ('ticker_sentiment', repeat('b', 64), 'v2',
                   '2026-11-10 08:00:00+00', 1, '{}')
           RETURNING id)
INSERT INTO perennial.ticker_sentiment (batch_id, security_id, row_number, article_count,
                                        news_count, social_count, weights_version)
SELECT b.id, s.id, 1, 0, 0, 0, 'weights-v1' FROM s, b
"""


def tally_dates(target):
    with target.connect() as conn:
        return conn.execute(text(
            "SELECT b.content_hash, t.tallied_on FROM perennial.ticker_sentiment t"
            " JOIN perennial.source_batches b ON b.id = t.batch_id ORDER BY 1"
        )).all()


@pytest.mark.db
def test_0025_backfills_from_the_batch_import_date_and_drops_cleanly(scratch):
    migrate(scratch, "0024")
    with scratch.begin() as conn:
        conn.execute(text(SEED))
    migrate(scratch, "0025")
    assert [d for _, d in tally_dates(scratch)] == [date(2026, 11, 17), date(2026, 11, 10)]
    with pytest.raises(DBAPIError) as caught, scratch.begin() as conn:
        conn.execute(text("UPDATE perennial.ticker_sentiment SET tallied_on = NULL"))
    assert caught.value.orig.sqlstate == "23502"  # NOT NULL
    migrate(scratch, "0024", down=True)
    with scratch.connect() as conn:
        assert conn.execute(text(
            "SELECT count(*) FROM perennial.ticker_sentiment")).scalar_one() == 2
        assert conn.execute(text(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'perennial'"
            " AND table_name = 'ticker_sentiment' AND column_name = 'tallied_on'"
        )).scalar_one() == 0

