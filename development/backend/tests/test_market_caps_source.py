"""M4 MC-A: the provider-neutral market_caps source, storage, adapter and collector.

Spec: docs/work-items/m4-market-caps-packet.md (MC-A, MCA-*). Synthetic data
only; no provider keys and no network. New modules are imported inside each
test so the rest of the suite still collects before MC-A exists.
"""

import json
import logging
import secrets
import socket
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import partial

import pytest
import requests
from sqlalchemy import func, insert, select, text, update
from sqlalchemy.exc import DBAPIError
from src.db import models
from src.ingestion.importer import import_collection

PRIOR_SOURCES = (
    "congress_trades",
    "ark_holdings",
    "insider_trades",
    "short_interest",
    "gdelt_theme_volume",
    "gdelt_theme_articles",
    "gdelt_spike_results",
)
PRIOR_LOCK_KEYS = {
    "congress_trades": 170010,
    "ark_holdings": 170011,
    "insider_trades": 170012,
    "short_interest": 170013,
    "gdelt_theme_volume": 170014,
    "gdelt_theme_articles": 170015,
    "gdelt_spike_results": 170016,
    "pipeline": 170098,
}
FIXTURE_FILE = "providers/market_caps/quotes.json"
SECRET = "synthetic-secret-detail"


# ── helpers ──────────────────────────────────────────────


def quote(value="20000000000.00", **fields):
    from src.ingestion.market_caps import MarketCapQuote

    defaults = dict(
        currency="USD",
        issuer_level=True,
        observed_at=None,
        sector="Technology",
        industry="Software - Infrastructure",
    )
    return MarketCapQuote(
        value=None if value is None else Decimal(value), **{**defaults, **fields}
    )


def bad_quote(**fields):
    """A quote with a field no well-behaved adapter returns.

    Set after construction, so collect() is what must catch it even if
    MarketCapQuote validates its own fields.
    """
    q = quote()
    for name, value in fields.items():
        object.__setattr__(q, name, value)
    return q


class StubAdapter:
    """An adapter defined only in a test (the MCB-12 pattern)."""

    def __init__(self, quotes=None, *, name="stub_live", mode="live", error=None):
        self.name = name
        self.collection_mode = mode
        self.quotes = quotes or {}
        self.error = error
        self.calls = []

    def fetch(self, symbols, *, session=None, sleep=time.sleep):
        self.calls.append(list(symbols))
        if self.error is not None:
            raise self.error
        return {s: self.quotes.get(s) for s in symbols}


def upstream(purchases=(), holdings=(), sales=()):
    """The {trades, holdings} rows collect_source hands downstream collectors."""

    def trade(symbol, kind):
        return {"ticker": symbol, "symbol": symbol, "trade_type": kind}

    return {
        "trades": [trade(s, "Purchase") for s in purchases]
        + [trade(s, "Sale (Full)") for s in sales],
        "holdings": [
            {"ticker": s, "symbol": s, "fund_count": 1, "funds": ["ARKK"]}
            for s in holdings
        ],
    }


def seed(engine, *symbols, inactive=()):
    """Securities with canonical ticker aliases; market_caps never registers them."""
    with engine.begin() as conn:
        for symbol in symbols:
            sid = conn.execute(
                insert(models.securities)
                .values(symbol=symbol, is_active=symbol not in inactive)
                .returning(models.securities.c.id)
            ).scalar_one()
            conn.execute(
                insert(models.security_identifiers).values(
                    scheme="ticker", value=symbol, security_id=sid
                )
            )


def run_row(engine, run_id):
    with engine.connect() as conn:
        return conn.execute(
            select(models.ingestion_runs).where(models.ingestion_runs.c.id == run_id)
        ).one()


def stored_caps(engine):
    """symbol -> stored market_caps row, for every row in the table."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT s.symbol, m.* FROM perennial.market_caps m "
                "JOIN perennial.securities s ON s.id = m.security_id ORDER BY s.symbol"
            )
        ).mappings()
        return {r["symbol"]: dict(r) for r in rows}


def caps_count(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT count(*) FROM perennial.market_caps")).scalar()


def unit_codes(diagnostics):
    return [f["code"] for f in (diagnostics or {}).get("failures", [])]


def assert_failed(engine, result, code, unit_code=None):
    """A failed run with `code`, no batch, nothing imported."""
    assert not result.succeeded and result.error_code == code
    row = run_row(engine, result.run_id)
    assert row.status == "failed" and row.batch_id is None and row.error_code == code
    assert caps_count(engine) == 0
    if unit_code is not None:
        assert unit_code in unit_codes(row.diagnostics)
        assert row.diagnostics["unit_kind"] == "market-cap tickers"
    return row


def has_market_caps(conn):
    return conn.execute(
        select(func.to_regclass("perennial.market_caps"))
    ).scalar() is not None


def require_market_caps(conn):
    """Guard: never downgrade a database whose head lacks the market_caps migration."""
    assert has_market_caps(conn), "perennial.market_caps does not exist at head"


# ── MCA-1e: vocabulary, tables and lock keys ─────────────


def test_market_caps_is_appended_to_the_vocabulary_without_moving_anything():
    from src.pipeline.lifecycle import SOURCE_LOCK_KEYS

    assert models.PINNED_ROW_SOURCES == ("market_caps",)
    assert models.SOURCES == (
        models.WATCHLIST_SOURCES + models.SUBJECTLESS_SOURCES + models.PINNED_ROW_SOURCES
    )
    assert models.SOURCES[: len(PRIOR_SOURCES)] == PRIOR_SOURCES
    assert models.SOURCES[len(PRIOR_SOURCES) :] == ("market_caps",)
    assert "market_caps" not in models.WATCHLIST_SOURCES
    assert models.SOURCE_TABLES["market_caps"] is models.market_caps
    assert {k: SOURCE_LOCK_KEYS[k] for k in PRIOR_LOCK_KEYS} == PRIOR_LOCK_KEYS
    assert SOURCE_LOCK_KEYS["market_caps"] == 170017
    assert len(set(SOURCE_LOCK_KEYS.values())) == len(SOURCE_LOCK_KEYS)


# ── MCA-1a/1c/1d: schema and constraints ─────────────────


def _world(conn):
    """Two market_caps batches with runs and rows, an ARK run, one publication."""
    now = datetime.now(timezone.utc)
    ids = {}
    for key, symbol in (("s1", "ZZM1"), ("s2", "ZZM2")):
        ids[key] = conn.execute(
            insert(models.securities).values(symbol=symbol).returning(models.securities.c.id)
        ).scalar_one()
    for key, source, digest in (
        ("b1", "market_caps", "a"),
        ("b2", "market_caps", "b"),
        ("b_ark", "ark_holdings", "c"),
    ):
        ids[key] = conn.execute(
            insert(models.source_batches)
            .values(
                source=source,
                content_hash=digest * 64,
                hash_version="test",
                record_count=0,
                payload={},
            )
            .returning(models.source_batches.c.id)
        ).scalar_one()
        ids["r" + key[1:]] = conn.execute(
            insert(models.ingestion_runs)
            .values(
                source=source,
                status="succeeded",
                collection_mode="live",
                started_at=now,
                finished_at=now,
                collected_at=now,
                batch_id=ids[key],
                reused_batch=False,
            )
            .returning(models.ingestion_runs.c.id)
        ).scalar_one()
    for key, batch, security, value in (
        ("m1", "b1", "s1", "1000.00"),
        ("m2", "b1", "s2", None),  # a NULL value is a stored, valid row
        ("m3", "b2", "s1", "2000.00"),
    ):
        ids[key] = conn.execute(
            text(
                "INSERT INTO perennial.market_caps (batch_id, security_id, value, currency,"
                " provider, issuer_level, observed_at, sector, industry) VALUES"
                " (:b, :s, :v, 'USD', 'stub_live', true, NULL, NULL, NULL) RETURNING id"
            ),
            {"b": ids[batch], "s": ids[security], "v": value},
        ).scalar_one()
    ids["pub"] = conn.execute(
        insert(models.publications)
        .values(created_at=now, ranking_version="test", ranking_config={}, output={})
        .returning(models.publications.c.id)
    ).scalar_one()
    return ids


def _rejected(conn, statement, params):
    with pytest.raises(DBAPIError):
        with conn.begin_nested():
            conn.execute(text(statement), params)


@pytest.mark.db
def test_migration_admits_market_caps_runs_and_batches(engine):
    with engine.begin() as conn:
        require_market_caps(conn)
        assert conn.execute(
            select(func.to_regclass("perennial.publication_market_cap_rows"))
        ).scalar() is not None
        ids = _world(conn)  # inserts market_caps batches and runs through the CHECKs
        assert conn.execute(
            text("SELECT count(*) FROM perennial.ingestion_runs WHERE source='market_caps'")
        ).scalar() == 2
        assert ids["m2"]


@pytest.mark.db
def test_market_caps_rejects_invalid_rows(engine):
    insert_row = (
        "INSERT INTO perennial.market_caps (batch_id, security_id, value, currency,"
        " provider, issuer_level) VALUES (:b, :s, :v, :c, :p, true)"
    )
    with engine.begin() as conn:
        ids = _world(conn)
        fresh = conn.execute(
            insert(models.securities).values(symbol="ZZM3").returning(models.securities.c.id)
        ).scalar_one()
        good = {"b": ids["b2"], "s": fresh, "v": "1.00", "c": "USD", "p": "stub_live"}
        for change in (
            {"v": "0"},
            {"v": "-1.00"},
            {"c": "usd"},
            {"c": "US1"},
            {"p": ""},
            {"p": "   "},
            {"s": ids["s1"]},  # a second row for (b2, s1)
        ):
            _rejected(conn, insert_row, {**good, **change})
        conn.execute(text(insert_row), good)


@pytest.mark.db
def test_pins_must_name_a_row_of_their_own_run_batch_and_security(engine):
    pin = (
        "INSERT INTO perennial.publication_market_cap_rows (publication_id, security_id,"
        " source, run_id, batch_id, market_cap_id) VALUES (:p, :s, :src, :r, :b, :m)"
    )
    with engine.begin() as conn:
        ids = _world(conn)
        ok = {"p": ids["pub"], "s": ids["s1"], "src": "market_caps",
              "r": ids["r1"], "b": ids["b1"], "m": ids["m1"]}
        for change in (
            {"m": ids["m3"]},  # the row is in b2, not in its run's batch b1
            {"r": ids["r2"]},  # run r2's batch is b2
            {"r": ids["r_ark"], "b": ids["b_ark"]},  # a run of another source
            {"src": "ark_holdings"},
            {"s": ids["s2"]},  # m1 is s1's row
            {"r": None},
            {"b": None, "m": None},
            {"r": None, "b": None},
            {"m": None},
        ):
            _rejected(conn, pin, {**ok, **change})
        conn.execute(text(pin), ok)
        _rejected(conn, pin, ok)  # one pin per (publication, security)
        # Unavailable: all three NULL.
        conn.execute(text(pin), {**ok, "s": ids["s2"], "r": None, "b": None, "m": None})
        assert conn.execute(
            text("SELECT count(*) FROM perennial.publication_market_cap_rows")
        ).scalar() == 2


# ── MCA-1a/1b: migration round trip and downgrade refusal ─


def _version(conn):
    return conn.execute(text("SELECT version_num FROM public.alembic_version")).scalar()


def _previous_revision():
    from alembic.config import Config
    from alembic.script import ScriptDirectory
    from tests.conftest import ALEMBIC_INI

    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    (head,) = script.get_heads()
    return head, script.get_revision(head).down_revision


def _schema(conn):
    """Columns, constraints and indexes of the perennial schema, as text."""
    queries = (
        "SELECT table_name, column_name, data_type, is_nullable, column_default,"
        " character_maximum_length, numeric_precision, numeric_scale"
        " FROM information_schema.columns WHERE table_schema='perennial'",
        "SELECT conrelid::regclass::text, conname, pg_get_constraintdef(oid)"
        " FROM pg_constraint WHERE connamespace='perennial'::regnamespace",
        "SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname='perennial'",
    )
    return [sorted(tuple(map(str, r)) for r in conn.execute(text(q))) for q in queries]


@pytest.fixture
def scratch_database(migrated_engine):
    """An empty database on the test server, dropped afterwards."""
    from tests.conftest import pinned_engine

    url = migrated_engine.url
    name = f"perennial_test_mc_{secrets.token_hex(4)}"
    admin = pinned_engine(url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    scratch = pinned_engine(url.set(database=name))
    try:
        yield scratch
    finally:
        scratch.dispose()
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


@pytest.mark.db
def test_downgrade_restores_the_previous_schema_exactly_and_upgrades_again(
    engine, scratch_database
):
    from alembic import command
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext
    from tests.conftest import alembic_config

    head, previous = _previous_revision()
    with scratch_database.connect() as conn:
        command.upgrade(alembic_config(conn), previous)
        conn.commit()
        expected = _schema(conn)
    with engine.connect() as conn:
        require_market_caps(conn)
        assert _version(conn) == head
        try:
            command.downgrade(alembic_config(conn), previous)
            conn.commit()
            assert not has_market_caps(conn)
            assert _schema(conn) == expected
        finally:
            command.upgrade(alembic_config(conn), "head")
            conn.commit()
        assert _version(conn) == head and has_market_caps(conn)
        diff = compare_metadata(
            MigrationContext.configure(
                conn,
                opts={
                    "include_schemas": True,
                    "compare_type": True,
                    "include_name": lambda n, t, p: t != "schema" or n == "perennial",
                },
            ),
            models.metadata,
        )
        assert diff == []


def _populate(engine, state):
    """One kind of market_caps state at a time."""
    from src.ingestion.importer import CollectionOutcome
    from src.pipeline.lifecycle import begin_run

    if state == "failed_run":
        run = begin_run(engine, "market_caps", "live")
        import_collection(
            engine,
            CollectionOutcome.failure("market_caps", "live", "collection_failed"),
            run_id=run,
        )
    elif state == "batch":
        with engine.begin() as conn:
            _world(conn)  # runs, batches and rows; no pins
    elif state == "pin":
        now = datetime.now(timezone.utc)
        with engine.begin() as conn:
            sid = conn.execute(
                insert(models.securities).values(symbol="ZZM1").returning(models.securities.c.id)
            ).scalar_one()
            pub = conn.execute(
                insert(models.publications)
                .values(created_at=now, ranking_version="test", ranking_config={}, output={})
                .returning(models.publications.c.id)
            ).scalar_one()
            conn.execute(
                text(
                    "INSERT INTO perennial.publication_market_cap_rows"
                    " (publication_id, security_id, source) VALUES (:p, :s, 'market_caps')"
                ),
                {"p": pub, "s": sid},
            )


@pytest.mark.db
@pytest.mark.parametrize("state", ["failed_run", "batch", "pin"])
def test_downgrade_refuses_while_market_caps_state_exists(engine, state):
    from alembic import command
    from tests.conftest import alembic_config

    with engine.connect() as conn:
        require_market_caps(conn)
    head, previous = _previous_revision()
    _populate(engine, state)
    with engine.connect() as conn:
        before = conn.execute(
            text(
                "SELECT (SELECT count(*) FROM perennial.ingestion_runs WHERE source='market_caps'),"
                " (SELECT count(*) FROM perennial.source_batches WHERE source='market_caps'),"
                " (SELECT count(*) FROM perennial.market_caps),"
                " (SELECT count(*) FROM perennial.publication_market_cap_rows)"
            )
        ).one()
        assert any(before)
        try:
            # 0010 raises in Python, 0009 inside the database; either pattern refuses.
            with pytest.raises((RuntimeError, DBAPIError)):
                command.downgrade(alembic_config(conn), previous)
        finally:
            conn.rollback()
            if _version(conn) != head:  # a downgrade that went through: restore the session DB
                command.upgrade(alembic_config(conn), "head")
                conn.commit()
        # Atomic: still at head with every table and row intact.
        assert _version(conn) == head and has_market_caps(conn)
        after = conn.execute(
            text(
                "SELECT (SELECT count(*) FROM perennial.ingestion_runs WHERE source='market_caps'),"
                " (SELECT count(*) FROM perennial.source_batches WHERE source='market_caps'),"
                " (SELECT count(*) FROM perennial.market_caps),"
                " (SELECT count(*) FROM perennial.publication_market_cap_rows)"
            )
        ).one()
        assert after == before


# ── MCA-2: grants ────────────────────────────────────────


@pytest.mark.db
def test_market_cap_tables_are_append_only_for_the_importer(engine):
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "database" / "ops"))
    from provision_roles import IMMUTABLE, provision

    assert {"market_caps", "publication_market_cap_rows"} <= set(IMMUTABLE)
    suffix = secrets.token_hex(6)
    roles = tuple(f"ops_test_{r}_{suffix}" for r in ("owner", "importer", "reader"))
    owner, importer, reader = roles
    try:
        with engine.begin() as conn:
            raw = conn.connection.driver_connection
            provision(raw, roles, phase="bootstrap")
            provision(raw, roles, phase="grants")
            provision(raw, roles, phase="grants")  # re-run is idempotent
        query = text("SELECT has_table_privilege(:role, :table, :privilege)")
        with engine.connect() as conn:
            for table in ("market_caps", "publication_market_cap_rows"):
                for role, privilege, allowed in (
                    (importer, "SELECT", True),
                    (importer, "INSERT", True),
                    (importer, "UPDATE", False),
                    (importer, "DELETE", False),
                    (importer, "TRUNCATE", False),
                    (reader, "SELECT", True),
                    (reader, "INSERT", False),
                    (reader, "UPDATE", False),
                    (reader, "DELETE", False),
                    (reader, "TRUNCATE", False),
                ):
                    got = conn.execute(
                        query,
                        {"role": role, "table": f"perennial.{table}", "privilege": privilege},
                    ).scalar()
                    assert got is allowed, (role[:13], table, privilege)
    finally:
        with engine.begin() as conn:
            admin = conn.execute(text("SELECT current_user")).scalar()
            for role in roles:
                exists = conn.execute(
                    text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": role}
                ).scalar()
                if exists:
                    conn.execute(text(f'REASSIGN OWNED BY "{role}" TO "{admin}"'))
                    conn.execute(text(f'DROP OWNED BY "{role}" CASCADE'))
                    conn.execute(text(f'DROP ROLE "{role}"'))


# ── MCA-3a/3b/3c: planning and success ───────────────────


def test_plan_is_sorted_unique_candidates_from_purchases_and_holdings():
    from src.ingestion.market_caps import collect

    adapter = StubAdapter({"ZZAA": quote()})
    outcome = collect(
        adapter,
        upstream=upstream(purchases=["ZZCC", "ZZAA", "ZZCC"], holdings=["ZZBB", "ZZAA"],
                          sales=["ZZSS"]),
    )
    assert adapter.calls == [["ZZAA", "ZZBB", "ZZCC"]]  # no sale-only ticker, no repeats
    assert outcome.succeeded and len(outcome.records) == 3


@pytest.mark.parametrize("size, code", [(0, "no_input_tickers"), (2001, "plan_too_large")])
def test_empty_or_oversized_plan_fails_without_fetching_or_truncating(size, code):
    from src.ingestion.collectors import MAX_PLANNED_REQUESTS
    from src.ingestion.market_caps import collect

    assert MAX_PLANNED_REQUESTS == 2000
    adapter = StubAdapter()
    outcome = collect(adapter, upstream=upstream(holdings=[f"Z{i:05d}" for i in range(size)]))
    assert not outcome.succeeded and outcome.error_code == code
    assert outcome.source == "market_caps" and outcome.mode == "live"
    assert adapter.calls == []


def test_plan_of_exactly_the_limit_is_collected():
    from src.ingestion.market_caps import collect

    symbols = [f"Z{i:05d}" for i in range(2000)]
    adapter = StubAdapter()
    outcome = collect(adapter, upstream=upstream(holdings=symbols))
    assert outcome.succeeded and len(outcome.records) == 2000
    assert adapter.calls == [symbols]


@pytest.mark.db
def test_successful_collection_stores_one_row_per_planned_symbol(engine):
    from src.ingestion.market_caps import collect

    seed(engine, "ZZAA", "ZZBB", "ZZCC")
    observed = datetime.now(timezone.utc) - timedelta(hours=2)
    adapter = StubAdapter({
        "ZZAA": quote("25000000000.00", observed_at=observed),
        "ZZBB": quote("4800000000.00", sector="Healthcare", industry="Biotechnology"),
        "ZZCC": None,  # MCA-3c: no value is a row, not a failure
    })
    outcome = collect(adapter, upstream=upstream(purchases=["ZZAA"], holdings=["ZZBB", "ZZCC"]))
    assert outcome.succeeded and outcome.source == "market_caps"
    assert outcome.mode == "live"
    assert outcome.coverage == {"scope": "selected_universe", "complete": True}
    assert adapter.name in json.dumps(outcome.envelope)

    result = import_collection(engine, outcome)
    assert result.succeeded
    run = run_row(engine, result.run_id)
    assert run.collection_mode == "live"
    assert run.coverage == {"scope": "selected_universe", "complete": True}
    rows = stored_caps(engine)
    assert set(rows) == {"ZZAA", "ZZBB", "ZZCC"}
    assert all(r["batch_id"] == result.batch_id for r in rows.values())
    assert all(r["provider"] == "stub_live" for r in rows.values())
    aa, bb, cc = rows["ZZAA"], rows["ZZBB"], rows["ZZCC"]
    assert aa["value"] == Decimal("25000000000.00") and aa["currency"] == "USD"
    assert aa["issuer_level"] is True and aa["observed_at"] == observed
    assert (aa["sector"], aa["industry"]) == ("Technology", "Software - Infrastructure")
    assert bb["observed_at"] is None
    assert (bb["sector"], bb["industry"]) == ("Healthcare", "Biotechnology")
    assert cc["value"] is None and cc["sector"] is None and cc["industry"] is None


# ── MCA-3d/3e: adapter errors and response mismatch ──────


@pytest.mark.db
def test_missing_credential_fails_the_run_with_that_code(engine):
    from src.ingestion.market_caps import MarketCapAdapterError, collect

    seed(engine, "ZZAA")
    adapter = StubAdapter(error=MarketCapAdapterError("missing_credential"))
    result = import_collection(engine, collect(adapter, upstream=upstream(holdings=["ZZAA"])))
    assert_failed(engine, result, "missing_credential")


@pytest.mark.db
@pytest.mark.parametrize("code", ["timeout", "http_429", "malformed_response"])
def test_adapter_unit_failure_fails_the_run_with_the_unit_code(engine, code):
    from src.ingestion.market_caps import MarketCapAdapterError, collect

    seed(engine, "ZZAA", "ZZBB")
    adapter = StubAdapter(error=MarketCapAdapterError(code))
    outcome = collect(adapter, upstream=upstream(holdings=["ZZAA", "ZZBB"]))
    assert outcome.mode == "live"
    assert_failed(engine, import_collection(engine, outcome), "collection_failed", code)


@pytest.mark.db
@pytest.mark.parametrize(
    "error, code",
    [
        (requests.ConnectionError(SECRET), "collection_failed"),
        (RuntimeError(SECRET), "internal_error"),
    ],
)
def test_other_exceptions_are_mapped_by_collect_source_without_their_text(
    engine, fixture_path, caplog, error, code
):
    from src.ingestion.importer import CollectionOutcome
    from src.ingestion.market_caps import collect
    from src.pipeline.runner import collect_source
    from test_pipeline import fixture_records

    caplog.set_level(logging.DEBUG)
    # Live upstream batches, so this holds once collect_source plans market_caps
    # from them (MCB-2d) as well as before, when the partial's upstream is used.
    for source, file, key in (("congress_trades", "trades_congress.json", "trades"),
                              ("ark_holdings", "ark_holdings.json", "holdings")):
        assert import_collection(engine, CollectionOutcome.success(
            source, "live", fixture_records(fixture_path, file, key))).succeeded
    adapter = StubAdapter(error=error)  # ZZAA is registered by the ARK import
    result = collect_source(
        engine,
        "market_caps",
        collector=partial(collect, adapter, upstream=upstream(holdings=["ZZAA"])),
    )
    row = assert_failed(engine, result, code)
    assert SECRET not in json.dumps([row.error_summary, row.diagnostics], default=str)
    assert SECRET not in caplog.text


@pytest.mark.db
@pytest.mark.parametrize("change", ["missing", "extra"])
def test_response_symbols_must_match_the_plan(engine, change):
    from src.ingestion.market_caps import collect

    seed(engine, "ZZAA", "ZZBB", "ZZXX")

    class Mismatched(StubAdapter):
        def fetch(self, symbols, *, session=None, sleep=time.sleep):
            result = {s: quote() for s in symbols}
            if change == "missing":
                result.pop(symbols[-1])
            else:
                result["ZZXX"] = quote()
            return result

    outcome = collect(Mismatched(), upstream=upstream(holdings=["ZZAA", "ZZBB"]))
    result = import_collection(engine, outcome)
    assert_failed(engine, result, "collection_failed", "malformed_response")


# ── MCA-3f: value validation ─────────────────────────────


@pytest.mark.db
@pytest.mark.parametrize(
    "raw, stored",
    [
        ("1234.565", "1234.56"),  # half-even: down to the even cent
        ("1234.575", "1234.58"),  # half-even: up to the even cent
        ("9999999999999999999999.99", "9999999999999999999999.99"),
        ("0.01", "0.01"),
    ],
)
def test_values_are_rounded_half_even_to_cents(engine, raw, stored):
    from src.ingestion.market_caps import collect

    seed(engine, "ZZAA")
    adapter = StubAdapter({"ZZAA": quote(raw)})
    result = import_collection(engine, collect(adapter, upstream=upstream(holdings=["ZZAA"])))
    assert result.succeeded
    assert stored_caps(engine)["ZZAA"]["value"] == Decimal(stored)


@pytest.mark.db
@pytest.mark.parametrize(
    "fields, unit_code",
    [
        ({"value": True}, "malformed_response"),
        ({"value": Decimal("NaN")}, "malformed_response"),
        ({"value": Decimal("Infinity")}, "malformed_response"),
        ({"value": float("nan")}, "malformed_response"),
        ({"value": "not a number"}, "malformed_response"),
        ({"value": Decimal("0")}, "malformed_response"),
        ({"value": Decimal("-5.00")}, "malformed_response"),
        ({"value": Decimal("0.004")}, "malformed_response"),  # rounds to 0.00
        ({"value": Decimal("1e22")}, "malformed_response"),
        ({"currency": "EUR"}, "currency_conflict"),
        ({"observed_at": datetime.now(timezone.utc) + timedelta(days=1)}, "malformed_response"),
    ],
)
def test_invalid_quotes_fail_the_whole_run(engine, fields, unit_code):
    from src.ingestion.market_caps import collect

    seed(engine, "ZZAA", "ZZBB")
    adapter = StubAdapter({"ZZAA": quote(), "ZZBB": bad_quote(**fields)})
    result = import_collection(
        engine, collect(adapter, upstream=upstream(holdings=["ZZAA", "ZZBB"]))
    )
    assert_failed(engine, result, "collection_failed", unit_code)


@pytest.mark.db
@pytest.mark.parametrize(
    "sector, industry",
    [(42, "x" * 129), ("", "   "), ("y" * 65, None), (None, ["list"])],
)
def test_invalid_classification_becomes_null_without_failing(engine, sector, industry):
    from src.ingestion.market_caps import collect

    seed(engine, "ZZAA")
    adapter = StubAdapter({"ZZAA": bad_quote(sector=sector, industry=industry)})
    result = import_collection(engine, collect(adapter, upstream=upstream(holdings=["ZZAA"])))
    assert result.succeeded
    row = stored_caps(engine)["ZZAA"]
    assert row["sector"] is None and row["industry"] is None
    assert row["value"] == Decimal("20000000000.00")


# ── MCA-3g: the fixture adapter is refused outside fixture runs ─


@pytest.mark.db
@pytest.mark.parametrize("record_mode", [None, "live"])
@pytest.mark.parametrize("kind", ["fixture_class", "stub_fixture_mode"])
def test_fixture_adapter_is_refused_unless_recording_fixture_mode(
    engine, monkeypatch, record_mode, kind
):
    from src.ingestion.market_caps import FixtureMarketCapAdapter, collect

    seed(engine, "ZZAA")
    if kind == "fixture_class":
        adapter = FixtureMarketCapAdapter()
        calls = []
        monkeypatch.setattr(
            FixtureMarketCapAdapter, "fetch", lambda self, *a, **k: calls.append(a) or {}
        )
    else:
        adapter = StubAdapter({"ZZAA": quote()}, name="fixture_like", mode="fixture")
        calls = adapter.calls
    kwargs = {} if record_mode is None else {"record_mode": record_mode}
    outcome = collect(adapter, upstream=upstream(holdings=["ZZAA"]), **kwargs)
    assert not outcome.succeeded
    assert outcome.error_code == "fixture_adapter_not_allowed"
    assert calls == []
    result = import_collection(engine, outcome)
    assert_failed(engine, result, "fixture_adapter_not_allowed")
    with engine.connect() as conn:
        assert conn.execute(
            text("SELECT count(*) FROM perennial.source_batches WHERE source='market_caps'")
        ).scalar() == 0


@pytest.mark.db
def test_fixture_adapter_runs_when_recording_fixture_mode(engine, fixture_path):
    from src.ingestion.market_caps import FixtureMarketCapAdapter, collect

    quotes = json.loads(fixture_path(FIXTURE_FILE).read_text())["quotes"]
    seed(engine, *quotes)
    outcome = collect(
        FixtureMarketCapAdapter(),
        upstream=upstream(holdings=sorted(quotes)),
        record_mode="fixture",
    )
    assert outcome.succeeded and outcome.mode == "live"  # collect_source relabels
    result = import_collection(engine, replace(outcome, mode="fixture"))
    assert result.succeeded
    assert run_row(engine, result.run_id).collection_mode == "fixture"
    rows = stored_caps(engine)
    assert set(rows) == set(quotes)
    assert all(r["provider"] == "fixture" for r in rows.values())
    for symbol, q in quotes.items():
        assert rows[symbol]["value"] == Decimal(q["value"])


# ── MCA-4: identity and batch reuse ──────────────────────


@pytest.mark.db
@pytest.mark.parametrize("case, unit_code", [("unknown", "unresolved"),
                                              ("inactive", "inactive_security")])
def test_symbols_resolve_lookup_only(engine, case, unit_code):
    from src.ingestion.importer import ROOT_SOURCES
    from src.ingestion.market_caps import collect

    assert "market_caps" not in ROOT_SOURCES
    if case == "unknown":
        seed(engine, "ZZAA")
    else:
        seed(engine, "ZZAA", "ZZBB", inactive=("ZZBB",))
    with engine.connect() as conn:
        securities_before = conn.execute(select(func.count()).select_from(models.securities)).scalar()
    adapter = StubAdapter({"ZZAA": quote(), "ZZBB": quote()})
    result = import_collection(
        engine, collect(adapter, upstream=upstream(holdings=["ZZAA", "ZZBB"]))
    )
    row = assert_failed(engine, result, "unresolved_security")
    assert {"unit": "ZZBB", "code": unit_code} in row.diagnostics["failures"]
    with engine.connect() as conn:  # nothing registered
        assert conn.execute(
            select(func.count()).select_from(models.securities)
        ).scalar() == securities_before


@pytest.mark.db
def test_identical_results_reuse_the_batch_but_another_provider_does_not(engine):
    from src.ingestion.market_caps import collect

    seed(engine, "ZZAA", "ZZBB")
    plan = upstream(holdings=["ZZAA", "ZZBB"])
    quotes = {"ZZAA": quote("1000.00"), "ZZBB": None}
    first = import_collection(engine, collect(StubAdapter(quotes), upstream=plan))
    again = import_collection(engine, collect(StubAdapter(quotes), upstream=plan))
    other = import_collection(
        engine, collect(StubAdapter(quotes, name="stub_other"), upstream=plan)
    )
    assert first.succeeded and again.succeeded and other.succeeded
    assert again.batch_id == first.batch_id and again.reused_batch is True
    assert other.batch_id != first.batch_id and other.reused_batch is False
    with engine.connect() as conn:
        providers = conn.execute(
            text("SELECT batch_id, array_agg(DISTINCT provider) FROM perennial.market_caps"
                 " GROUP BY batch_id ORDER BY batch_id")
        ).all()
    assert providers == [(first.batch_id, ["stub_live"]), (other.batch_id, ["stub_other"])]


@pytest.mark.db
def test_share_classes_keep_separate_issuer_level_rows(engine):
    from src.ingestion.market_caps import collect

    seed(engine, "GOOG", "GOOGL")
    adapter = StubAdapter({"GOOG": quote("2000000000000.00"),
                           "GOOGL": quote("2000000000000.00")})
    result = import_collection(
        engine, collect(adapter, upstream=upstream(holdings=["GOOG", "GOOGL"]))
    )
    assert result.succeeded
    rows = stored_caps(engine)
    assert set(rows) == {"GOOG", "GOOGL"}
    assert rows["GOOG"]["security_id"] != rows["GOOGL"]["security_id"]
    for symbol in ("GOOG", "GOOGL"):  # each its own value; nothing summed
        assert rows[symbol]["issuer_level"] is True
        assert rows[symbol]["value"] == Decimal("2000000000000.00")


# ── MCA-5a/5b: fixture adapter, registry and conformance ──


class _Offline:
    """A session that fails the test if anything uses it."""

    def __getattr__(self, name):
        raise AssertionError("the fixture adapter used the session")


@pytest.fixture
def no_network(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("network access")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(requests.Session, "request", refuse)


def test_fixture_adapter_serves_the_fixture_offline(fixture_path, no_network):
    from src.ingestion.market_caps import FixtureMarketCapAdapter, MarketCapQuote

    quotes = json.loads(fixture_path(FIXTURE_FILE).read_text())["quotes"]
    adapter = FixtureMarketCapAdapter()
    assert adapter.name == "fixture" and adapter.collection_mode == "fixture"
    symbols = sorted(quotes) + ["ZZZZ"]
    got = adapter.fetch(symbols, session=_Offline(), sleep=lambda s: None)
    assert set(got) == set(symbols) and got["ZZZZ"] is None
    for symbol, q in quotes.items():
        assert got[symbol] == MarketCapQuote(
            value=Decimal(q["value"]),
            currency=q["currency"],
            issuer_level=q["issuer_level"],
            observed_at=q["observed_at"],
            sector=q["sector"],
            industry=q["industry"],
        )


def test_fixture_adapter_reads_another_fixture_file(tmp_path, no_network):
    from src.ingestion.market_caps import FixtureMarketCapAdapter

    path = tmp_path / "quotes.json"
    path.write_text(json.dumps({"quotes": {"ZZQQ": {
        "value": "123.45", "currency": "USD", "issuer_level": False,
        "observed_at": "2026-10-22T20:00:00+00:00", "sector": None, "industry": None}}}))
    got = FixtureMarketCapAdapter(path).fetch(["ZZQQ"])
    assert got["ZZQQ"].value == Decimal("123.45") and got["ZZQQ"].issuer_level is False
    assert got["ZZQQ"].observed_at == datetime(2026, 10, 22, 20, tzinfo=timezone.utc)


@pytest.mark.parametrize("code", ["missing_credential", "timeout", "http_503"])
def test_fixture_adapter_raises_the_configured_code(code, no_network):
    from src.ingestion.market_caps import FixtureMarketCapAdapter, MarketCapAdapterError

    with pytest.raises(MarketCapAdapterError) as caught:
        FixtureMarketCapAdapter(fail_with=code).fetch(["ZZAA"])
    assert caught.value.code == code


def _settings(monkeypatch, **env):
    from src.settings import load_settings

    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://user:synthetic@localhost/test")
    monkeypatch.delenv("MARKET_CAP_ADAPTER", raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return load_settings(local_dev=False)


def _fixture_case(settings):
    from src.ingestion.market_caps import ADAPTERS, FixtureMarketCapAdapter

    return ADAPTERS["fixture"](settings), FixtureMarketCapAdapter(fail_with="timeout"), _Offline()


# Every registered adapter needs a case here: name -> (build(settings) returning
# (adapter, the same adapter set up to fail, session), symbols with and without a
# value). A live adapter's session is a fake transport serving synthetic responses.
CONFORMANCE_CASES = {
    "fixture": (_fixture_case, ["ZZAA", "ZZB.B", "ZZZZ"]),
}


def test_every_registered_adapter_has_a_conformance_case():
    from src.ingestion.market_caps import ADAPTERS

    assert set(ADAPTERS) == set(CONFORMANCE_CASES)


@pytest.mark.parametrize("name", sorted(CONFORMANCE_CASES))
def test_registered_adapters_conform(name, monkeypatch, no_network):
    from market_cap_conformance import check_adapter

    build, symbols = CONFORMANCE_CASES[name]
    adapter, failing, session = build(_settings(monkeypatch))
    check_adapter(adapter, symbols, failing=failing, session=session)


def test_conformance_rejects_an_adapter_that_respells_symbols():
    from market_cap_conformance import check_adapter

    class Respelling(StubAdapter):
        def fetch(self, symbols, *, session=None, sleep=time.sleep):
            return {s.replace(".", "-"): quote() for s in symbols}

    with pytest.raises(AssertionError):
        check_adapter(Respelling(), ["ZZB.B", "ZZZZ"])


# ── MCA-5c: settings ─────────────────────────────────────


def test_market_cap_adapter_setting_defaults_to_none_and_is_validated(monkeypatch):
    from src.settings import SettingsError

    assert _settings(monkeypatch).market_cap_adapter is None
    assert _settings(monkeypatch, MARKET_CAP_ADAPTER="fixture").market_cap_adapter == "fixture"
    with pytest.raises(SettingsError):
        _settings(monkeypatch, MARKET_CAP_ADAPTER="nope")


# ── Edge cases found after the build (d1498c6) ───────────


@pytest.mark.db
def test_a_lookup_only_import_never_edits_securities(engine):
    """A cap's currency is the provider's reporting currency, not the listing's."""
    from src.ingestion.market_caps import collect

    seed(engine, "ZZAA", "ZZBB")
    with engine.begin() as conn:
        conn.execute(
            update(models.securities)
            .where(models.securities.c.symbol == "ZZBB")
            .values(currency="CAD", exchange="TSX")
        )
        before = conn.execute(select(models.securities).order_by("id")).all()
    adapter = StubAdapter({"ZZAA": quote(), "ZZBB": quote()})
    result = import_collection(
        engine, collect(adapter, upstream=upstream(holdings=["ZZAA", "ZZBB"]))
    )
    assert result.succeeded
    with engine.connect() as conn:
        assert conn.execute(select(models.securities).order_by("id")).all() == before
    assert {r["currency"] for r in stored_caps(engine).values()} == {"USD"}


@pytest.mark.db
def test_value_that_rounds_up_to_the_bound_is_refused_not_overflowed(engine):
    from src.ingestion.market_caps import collect

    seed(engine, "ZZAA")
    adapter = StubAdapter({"ZZAA": bad_quote(value=Decimal("9999999999999999999999.995"))})
    result = import_collection(engine, collect(adapter, upstream=upstream(holdings=["ZZAA"])))
    assert_failed(engine, result, "collection_failed", "malformed_response")


@pytest.mark.db
@pytest.mark.parametrize(
    "fields",
    [{"value": Decimal("sNaN")}, {"value": Decimal("-0")}, {"issuer_level": "yes"}],
)
def test_other_malformed_quote_fields_fail_cleanly(engine, fields):
    from src.ingestion.market_caps import collect

    seed(engine, "ZZAA")
    adapter = StubAdapter({"ZZAA": bad_quote(**fields)})
    result = import_collection(engine, collect(adapter, upstream=upstream(holdings=["ZZAA"])))
    assert_failed(engine, result, "collection_failed", "malformed_response")


@pytest.mark.db
def test_a_value_that_is_not_a_quote_is_refused(engine):
    from src.ingestion.market_caps import collect

    seed(engine, "ZZAA")
    adapter = StubAdapter({"ZZAA": {"value": "1000.00", "currency": "USD"}})
    result = import_collection(engine, collect(adapter, upstream=upstream(holdings=["ZZAA"])))
    assert_failed(engine, result, "collection_failed", "malformed_response")


@pytest.mark.db
@pytest.mark.parametrize("ahead, ok", [(timedelta(minutes=1), True), (timedelta(minutes=10), False)])
def test_observed_at_tolerates_the_same_clock_skew_as_publishing(engine, ahead, ok):
    """MCA-3f matches _caps, whose _time_ok allows 5 minutes ahead."""
    from src.ingestion.market_caps import collect

    seed(engine, "ZZAA")
    observed = datetime.now(timezone.utc) + ahead
    adapter = StubAdapter({"ZZAA": quote(observed_at=observed)})
    result = import_collection(engine, collect(adapter, upstream=upstream(holdings=["ZZAA"])))
    if ok:
        assert result.succeeded and stored_caps(engine)["ZZAA"]["observed_at"] == observed
    else:
        assert_failed(engine, result, "collection_failed", "malformed_response")


@pytest.mark.db
def test_naive_observed_at_is_refused(engine):
    from src.ingestion.market_caps import collect

    seed(engine, "ZZAA")
    adapter = StubAdapter({"ZZAA": quote(observed_at=datetime(2026, 10, 22, 20, 0))})
    result = import_collection(engine, collect(adapter, upstream=upstream(holdings=["ZZAA"])))
    assert_failed(engine, result, "collection_failed", "malformed_response")


@pytest.mark.db
def test_an_adapter_error_code_is_untrusted(engine, caplog):
    """A code outside the vocabulary is stored as a generic failure, never verbatim."""
    from src.ingestion.market_caps import MarketCapAdapterError, collect

    caplog.set_level(logging.DEBUG)
    seed(engine, "ZZAA")
    adapter = StubAdapter(error=MarketCapAdapterError(f"{SECRET} at https://x.invalid/?key=1"))
    result = import_collection(engine, collect(adapter, upstream=upstream(holdings=["ZZAA"])))
    row = assert_failed(engine, result, "collection_failed")
    stored = json.dumps([row.error_summary, row.diagnostics], default=str)
    assert SECRET not in stored and "x.invalid" not in stored
    assert SECRET not in caplog.text


@pytest.mark.parametrize("mode", ["file", "derived", "", None, "LIVE"])
def test_an_unknown_collection_mode_fails_closed_without_fetching(mode):
    from src.ingestion.market_caps import collect

    adapter = StubAdapter({"ZZAA": quote()}, mode=mode)
    outcome = collect(adapter, upstream=upstream(holdings=["ZZAA"]), record_mode="fixture")
    assert not outcome.succeeded and outcome.error_code == "fixture_adapter_not_allowed"
    assert adapter.calls == []
