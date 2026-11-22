"""S3-01: the daily_prices source, its table and migration 0027.

Pinned:
* daily_prices is the newest source in the vocabulary, with its own lock key,
  envelope keys and record model, one row per (batch, security), planned from
  canonical tickers.
* The importer may only add rows (an immutable batch table) and the reader may
  read them. The API reads twelve columns (decision 118), never row_number or
  quoted_at, and writes nothing.
* The record and the table refuse a missing or non-positive close, a lone or
  inverted 52-week range, a range date without its value, and a bad currency or
  provider name.
* 0027 follows 0026. Its downgrade drops the table while no price run or batch
  exists, and refuses otherwise.
"""

import secrets
from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError
from sqlalchemy import insert, select, text
from sqlalchemy.exc import IntegrityError
from src.db import models
from src.ingestion import envelope, importer, schemas
from src.ingestion.identity import CANONICAL_ONLY_SOURCES
from src.pipeline.lifecycle import SOURCE_LOCK_KEYS
from test_d114_article_summary import migrate, scratch, version  # noqa: F401
from test_owner_migrations import provision_roles
from test_read_api import groups  # noqa: F401  (a pytest fixture)

TABLE = "perennial.daily_prices"
QUOTED = datetime(2026, 11, 13, 21, 0, tzinfo=timezone.utc)


def record(**changes):
    values = {
        "ticker": "ZZPA",
        "trading_date": "2026-11-13",
        "quoted_at": QUOTED,
        "close": "182.41",
        "previous_close": "184.58",
        "high_52w": "207.52",
        "high_52w_date": "2026-02-18",
        "low_52w": "63.40",
        "low_52w_date": "2025-11-21",
        "price_currency": "USD",
        "provider": "finnhub",
        **changes,
    }
    return schemas.DailyPrice(**values)


# ── registration ─────────────────────────────────────────


def test_daily_prices_is_the_newest_source_with_its_own_lock_key():
    assert models.SOURCES[-1] == "daily_prices"
    assert SOURCE_LOCK_KEYS["daily_prices"] == 170026
    assert len(set(SOURCE_LOCK_KEYS.values())) == len(SOURCE_LOCK_KEYS)


def test_it_is_one_row_per_security_from_canonical_tickers():
    assert importer.RECORD_MODELS["daily_prices"] is schemas.DailyPrice
    assert "daily_prices" in importer.ONE_ROW_PER_SECURITY
    assert "daily_prices" in CANONICAL_ONLY_SOURCES
    assert models.SOURCE_TABLES["daily_prices"] is models.daily_prices


def test_the_envelope_keeps_only_counts_and_ticker_lists():
    clean = envelope.sanitize_envelope(
        "daily_prices",
        {"source": "finnhub_quote", "universe": 3, "priced": 2, "no_quote": ["ZZPB"],
         "no_quote_count": 1, "errors": [], "errors_count": 0, "api_key": "secret"},
    )
    assert "api_key" not in clean
    assert clean["no_quote"] == ["ZZPB"] and clean["priced"] == 2


def test_policy_lets_the_importer_only_add_rows_and_never_lists_it_for_the_whole_api():
    roles = provision_roles()
    assert "daily_prices" in roles.IMMUTABLE
    assert "daily_prices" not in roles.MUTABLE + roles.REFERENCE
    assert "daily_prices" not in roles.API_TABLES


API_SERVED = (
    "id", "batch_id", "security_id", "trading_date", "close", "previous_close", "high_52w",
    "high_52w_date", "low_52w", "low_52w_date", "currency", "provider",
)


def test_the_api_may_read_exactly_the_served_columns():
    assert provision_roles().API_COLUMNS["daily_prices"] == API_SERVED


@pytest.mark.db
def test_the_api_reads_the_served_columns_and_nothing_else(engine, groups):  # noqa: F811
    with engine.connect() as conn:
        columns = conn.execute(
            text("SELECT column_name FROM information_schema.columns"
                 " WHERE table_schema = 'perennial' AND table_name = 'daily_prices'")
        ).scalars().all()
        readable = {
            c for c in columns
            if conn.execute(text("SELECT has_column_privilege(:r, :t, :c, 'SELECT')"),
                            {"r": groups["api"], "t": TABLE, "c": c}).scalar()
        }
    assert readable == set(API_SERVED)
    assert {"row_number", "quoted_at"} <= set(columns) - readable


@pytest.mark.db
def test_grants_on_the_table(engine, groups):  # noqa: F811
    allowed = {"importer": {"SELECT", "INSERT"}, "reader": {"SELECT"}}
    with engine.connect() as conn:
        for role, granted in allowed.items():
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"):
                assert (
                    conn.execute(
                        text("SELECT has_table_privilege(:r, :t, :p)"),
                        {"r": groups[role], "t": TABLE, "p": privilege},
                    ).scalar()
                    is (privilege in granted)
                ), (role, privilege)
        for privilege in ("INSERT", "UPDATE", "DELETE"):
            assert not conn.execute(
                text("SELECT has_table_privilege(:r, :t, :p)"),
                {"r": groups["api"], "t": TABLE, "p": privilege},
            ).scalar()


# ── the record ───────────────────────────────────────────


def test_a_full_record_maps_its_currency_to_the_currency_column():
    values = record().db_values()
    assert values["currency"] == "USD" and "price_currency" not in values
    assert values["close"] == schemas.Decimal("182.410000")


def test_the_range_and_previous_close_are_optional():
    bare = record(previous_close=None, high_52w=None, high_52w_date=None,
                  low_52w=None, low_52w_date=None)
    assert bare.high_52w is None and bare.previous_close is None


def test_a_zero_previous_close_means_unknown():
    assert record(previous_close=0).previous_close is None


@pytest.mark.parametrize(
    "changes",
    [
        {"close": None},
        {"close": 0},
        {"close": -1},
        {"high_52w": None, "high_52w_date": None},  # a lone low
        {"low_52w": "300", "low_52w_date": None},  # low above high
        {"low_52w": None},  # a date without its value
        {"price_currency": "usd"},
        {"provider": "Finnhub"},
        {"quoted_at": datetime(2026, 11, 13, 21, 0)},  # naive
    ],
)
def test_a_bad_record_is_refused(changes):
    with pytest.raises(ValidationError):
        record(**changes)


# ── the table ────────────────────────────────────────────


def _batch(conn):
    return conn.execute(
        text(
            "INSERT INTO perennial.source_batches (source, content_hash, hash_version, record_count, payload)"
            " VALUES ('daily_prices', :h, 'v2', 0, '{}') RETURNING id"
        ),
        {"h": secrets.token_hex(32)},
    ).scalar_one()


def _row(batch, security, **changes):
    return {
        "batch_id": batch,
        "security_id": security,
        "row_number": 1,
        "trading_date": date(2026, 11, 13),
        "quoted_at": QUOTED,
        "close": 182.41,
        "previous_close": 184.58,
        "high_52w": 207.52,
        "high_52w_date": date(2026, 2, 18),
        "low_52w": 63.40,
        "low_52w_date": date(2025, 11, 21),
        "currency": "USD",
        "provider": "finnhub",
        **changes,
    }


@pytest.fixture
def stored(engine):
    with engine.begin() as conn:
        security = conn.execute(
            insert(models.securities).values(symbol="ZZPA").returning(models.securities.c.id)
        ).scalar_one()
        batch = _batch(conn)
    return batch, security


@pytest.mark.db
def test_a_valid_row_is_stored(engine, stored):
    with engine.begin() as conn:
        conn.execute(insert(models.daily_prices).values(**_row(*stored)))
        assert conn.execute(select(models.daily_prices.c.close)).scalar_one() == schemas.Decimal("182.410000")


@pytest.mark.db
@pytest.mark.parametrize(
    "changes",
    [
        {"close": 0},
        {"previous_close": 0},
        {"high_52w": None, "high_52w_date": None},
        {"low_52w": 300, "low_52w_date": None},
        {"low_52w": None},
        {"currency": "usd"},
        {"provider": "Finnhub"},
        {"row_number": 0},
    ],
)
def test_the_table_refuses_a_bad_row(engine, stored, changes):
    with pytest.raises(IntegrityError), engine.begin() as conn:
        conn.execute(insert(models.daily_prices).values(**_row(*stored, **changes)))


@pytest.mark.db
def test_one_row_per_security_in_a_batch(engine, stored):
    with engine.begin() as conn:
        conn.execute(insert(models.daily_prices).values(**_row(*stored)))
    with pytest.raises(IntegrityError), engine.begin() as conn:
        conn.execute(insert(models.daily_prices).values(**_row(*stored, row_number=2)))


# ── migration 0027 on a database of its own ──────────────


def _has_table(target):
    with target.connect() as conn:
        return conn.execute(text("SELECT to_regclass(:t) IS NOT NULL"), {"t": TABLE}).scalar()


@pytest.mark.db
def test_with_no_price_batch_the_downgrade_drops_the_table(scratch):  # noqa: F811
    migrate(scratch, "0027")
    assert _has_table(scratch)
    migrate(scratch, "0026", down=True)
    assert version(scratch) == "0026" and not _has_table(scratch)
    with pytest.raises(IntegrityError), scratch.begin() as conn:
        _batch(conn)  # 'daily_prices' has left the vocabulary
    migrate(scratch, "0027")
    assert version(scratch) == "0027" and _has_table(scratch)


@pytest.mark.db
def test_while_a_price_batch_exists_the_downgrade_refuses(scratch):  # noqa: F811
    migrate(scratch, "0027")
    with scratch.begin() as conn:
        _batch(conn)
    with pytest.raises(RuntimeError, match="daily_prices state exists"):
        migrate(scratch, "0026", down=True)
    assert version(scratch) == "0027" and _has_table(scratch)
