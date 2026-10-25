"""RA-15 clause 3, completed: a write through any object in perennial refuses startup.

Design note: docs/work-items/m5-read-api-design-note.md, RA-15 ("no write
privilege on any perennial table"). After the column fix (6468931) the clause
still counts tables only (relkind r, p). These pin the rest (checker CP-1 and
CP-2, and the auditor's relkind note in security-m5-read-api-colpriv-report.md):
- sequences: USAGE (nextval) or UPDATE (setval) on an identity sequence
- TRIGGER on a table
- an auto-updatable view, whose writes reach the base table as the view's owner
- a foreign table
and that the clause's "at least one table" rule still means a table.

Each write is granted directly to an otherwise correct login (connect=True), so
no other clause refuses it; the objects are created by the superuser test login
and dropped afterwards.
"""

import secrets

import pytest
from sqlalchemy import text

from test_read_api import (  # noqa: F401  (groups, scratch_database: pytest fixtures)
    _refusal,
    assert_error,
    client,
    groups,
    login,
    scratch_database,
)


@pytest.fixture
def write_objects(engine):
    """Perennial's identity sequence, plus a view and a foreign table made for the test."""
    suffix = secrets.token_hex(4)
    objects = {
        "view": f"perennial.ra_view_{suffix}",
        "foreign": f"perennial.ra_foreign_{suffix}",
    }
    wrapper = f"ra_fdw_{suffix}"
    with engine.begin() as conn:
        objects["sequence"] = conn.execute(
            text("SELECT pg_get_serial_sequence('perennial.securities', 'id')")).scalar()
        conn.execute(text(  # auto-updatable: one table, plain columns
            f"CREATE VIEW {objects['view']} AS SELECT id, symbol, name FROM perennial.securities"))
        conn.execute(text(f"CREATE FOREIGN DATA WRAPPER {wrapper}"))  # no handler needed
        conn.execute(text(f"CREATE SERVER {wrapper}_server FOREIGN DATA WRAPPER {wrapper}"))
        conn.execute(text(
            f"CREATE FOREIGN TABLE {objects['foreign']} (id bigint) SERVER {wrapper}_server"))
    try:
        yield objects
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP VIEW IF EXISTS {objects['view']}"))
            conn.execute(text(f"DROP FOREIGN DATA WRAPPER IF EXISTS {wrapper} CASCADE"))


WRITES = {
    "sequence_update": "GRANT UPDATE ON SEQUENCE {sequence} TO \"{name}\"",  # setval
    "sequence_usage": "GRANT USAGE ON SEQUENCE {sequence} TO \"{name}\"",  # nextval
    "table_trigger": "GRANT TRIGGER ON perennial.securities TO \"{name}\"",
    "view_update": "GRANT UPDATE ON {view} TO \"{name}\"",
    "view_column_insert": "GRANT INSERT (symbol) ON {view} TO \"{name}\"",
    "foreign_table_insert": "GRANT INSERT ON {foreign} TO \"{name}\"",
}


@pytest.mark.db
@pytest.mark.parametrize("case", sorted(WRITES))
def test_a_write_through_any_perennial_object_is_refused(
    engine, groups, caplog, write_objects, case  # noqa: F811
):
    with login(engine, groups, connect=True) as (api_engine, name, password):
        with engine.begin() as conn:
            conn.execute(text(WRITES[case].format(name=name, **write_objects)))
        outcome = _refusal(engine, groups, caplog, api_engine,
                           (name, password, engine.url.database))
    with login(engine, groups, options="SUPERUSER") as (super_engine, n2, p2):
        reference = _refusal(engine, groups, caplog, super_engine, (n2, p2))
    assert outcome == reference  # the same fixed error as every other clause


@pytest.mark.db
def test_those_objects_alone_or_a_sequence_read_do_not_refuse(engine, groups, write_objects):  # noqa: F811
    """The control: the view, foreign table and sequence exist, and SELECT on the
    sequence reads it; none of that is a write, so a correct login still starts."""
    with login(engine, groups, connect=True) as (api_engine, name, _):
        with engine.begin() as conn:
            conn.execute(text(f'GRANT SELECT ON SEQUENCE {write_objects["sequence"]} TO "{name}"'))
        with client(api_engine, groups) as c:
            assert_error(c.get("/v1/watchlist"), 404, "no_current_publication")


@pytest.mark.db
def test_a_perennial_schema_holding_only_a_view_is_refused(
    engine, groups, caplog, scratch_database  # noqa: F811
):
    """The "at least one table exists" rule counts tables (r, p) only, so widening
    the writable predicate must not let a view stand in for the schema's tables."""
    from conftest import pinned_engine

    scratch = pinned_engine(engine.url.set(database=scratch_database))
    try:
        with scratch.begin() as conn:
            conn.execute(text("CREATE VIEW perennial.ra_only_view AS SELECT 1 AS id"))
            conn.execute(text("GRANT SELECT ON perennial.ra_only_view TO PUBLIC"))
    finally:
        scratch.dispose()
    with login(engine, groups, database=scratch_database) as (api_engine, name, password):
        outcome = _refusal(engine, groups, caplog, api_engine,
                           (name, password, scratch_database, engine.url.database))
    with login(engine, groups, options="SUPERUSER") as (super_engine, n2, p2):
        reference = _refusal(engine, groups, caplog, super_engine, (n2, p2))
    assert outcome == reference
