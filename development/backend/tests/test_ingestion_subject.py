"""Ingestion contract split (GDELT rework PR 1): no behaviour change.

The source vocabulary is separate from the watchlist set, and the importer can
write records that are not about a security. GDELT itself arrives in PR 2.
"""

import pytest
from sqlalchemy import BigInteger, Column, Integer, MetaData, String, Table, select, text

from src.db import models
from src.ingestion import importer
from src.ingestion.canonical import canonicalize
from src.ingestion.schemas import GlobalRecord, SourceRecord

WATCHLIST = ("congress_trades", "ark_holdings", "insider_trades", "short_interest")


class Label(GlobalRecord):
    DB_COLUMNS = ("label",)

    label: str


def test_watchlist_set_is_unchanged_and_the_vocabulary_is_append_only():
    assert models.WATCHLIST_SOURCES == WATCHLIST
    assert models.SOURCES[: len(WATCHLIST)] == WATCHLIST
    assert set(models.SUBJECTLESS_SOURCES).isdisjoint(WATCHLIST)
    assert models.SOURCES == models.WATCHLIST_SOURCES + models.SUBJECTLESS_SOURCES


def test_global_records_have_no_security_fields():
    assert "ticker" not in GlobalRecord.model_fields
    assert issubclass(SourceRecord, importer.ContractRecord)
    record = Label(label="ECON_OILPRICE")
    assert record.canonical() == {"label": "ECON_OILPRICE"}
    assert record.db_values() == {"label": "ECON_OILPRICE"}


def test_subjectless_validation_skips_security_rules(monkeypatch):
    monkeypatch.setitem(importer.RECORD_MODELS, "test_global", Label)
    # Two identical labels would be a duplicate_security for a one-row-per-
    # security source; a subject-less source has no such rule.
    parsed = importer.validate_records("test_global", [{"label": "A"}, {"label": "A"}])
    assert [r.label for r in parsed] == ["A", "A"]


@pytest.mark.db
def test_watchlist_lock_keys_are_pinned(engine):
    from src.pipeline.lifecycle import source_lock

    for offset, source in enumerate(WATCHLIST):
        with source_lock(engine, source), engine.connect() as conn:
            held = conn.execute(
                text(
                    "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
                    "AND ((classid::bigint << 32) | objid::bigint) = :key"
                ),
                {"key": 170010 + offset},
            ).scalar_one()
        assert held == 1, source


@pytest.mark.db
def test_subjectless_batch_is_written_without_touching_securities(engine, monkeypatch):
    """A real write through _write_batch, inside a transaction that is rolled back."""
    table = Table(
        "test_global",
        MetaData(schema=models.SCHEMA),
        Column("batch_id", BigInteger, nullable=False),
        Column("row_number", Integer, nullable=False),
        Column("label", String(64), nullable=False),
    )
    monkeypatch.setitem(importer.SOURCE_TABLES, "test_global", table)
    allowed = ", ".join(f"'{s}'" for s in (*models.SOURCES, "test_global"))
    batch = canonicalize("test_global", [Label(label="B"), Label(label="A")])
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(text(
                "ALTER TABLE perennial.source_batches "
                "DROP CONSTRAINT ck_source_batches_source, "
                f"ADD CONSTRAINT ck_source_batches_source CHECK (source IN ({allowed}))"
            ))
            table.create(conn)
            before = conn.execute(select(models.securities.c.id)).all()
            # [:2]: M4 (feat/m4-identity) appends a registered-alias count.
            batch_id, reused = importer._write_batch(conn, batch, None)[:2]
            rows = conn.execute(
                select(table.c.row_number, table.c.label).order_by(table.c.row_number)
            ).all()
            assert not reused
            assert [tuple(r) for r in rows] == [(1, "A"), (2, "B")]
            assert conn.execute(select(models.securities.c.id)).all() == before
            again_id, again_reused = importer._write_batch(conn, batch, None)[:2]
            assert (again_id, again_reused) == (batch_id, True)
        finally:
            trans.rollback()
