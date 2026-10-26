"""RA-15, the final rule set: no path to data or effects through another object's owner.

Both reviewers' converged rules after the write-objects fix (c8132a5): the API login
holds exactly what provisioning grants, on the objects it grants. So startup also
refuses, with the one fixed error:
- any privilege on a view, materialized view or foreign table in perennial, SELECT
  included (a view runs with its owner's privileges: SELECT on a superuser-owned view
  over source_batches hands over data provisioning withholds)
- EXECUTE on any function in perennial, whether granted or held through PUBLIC's
  default (built-in pg_catalog functions, and other schemas, are out of scope)
- REFERENCES, table or column, on perennial
Guards: those objects merely existing, and EXECUTE on a function outside perennial,
don't refuse a correctly provisioned login.
"""

import secrets

import pytest
from sqlalchemy import text

from test_read_api import (  # noqa: F401  (groups: pytest fixture)
    _refusal,
    assert_error,
    client,
    groups,
    login,
)


@pytest.fixture
def owner_objects(engine):
    """A view, a materialized view (over source_batches, which the API can't read), a
    foreign table and a function in perennial, all superuser-owned. The function's
    default PUBLIC EXECUTE is revoked, so only a deliberate grant reaches it."""
    suffix = secrets.token_hex(4)
    objects = {
        "view": f"perennial.ra_view_{suffix}",
        "matview": f"perennial.ra_matview_{suffix}",
        "foreign": f"perennial.ra_foreign_{suffix}",
        "function": f"perennial.ra_function_{suffix}()",
        "public_function": f"public.ra_function_{suffix}()",
    }
    wrapper = f"ra_fdw_{suffix}"
    with engine.begin() as conn:
        conn.execute(text(f"CREATE VIEW {objects['view']} AS SELECT id, source FROM perennial.source_batches"))
        conn.execute(text(f"CREATE MATERIALIZED VIEW {objects['matview']} AS"
                          " SELECT id FROM perennial.source_batches"))
        conn.execute(text(f"CREATE FOREIGN DATA WRAPPER {wrapper}"))
        conn.execute(text(f"CREATE SERVER {wrapper}_server FOREIGN DATA WRAPPER {wrapper}"))
        conn.execute(text(f"CREATE FOREIGN TABLE {objects['foreign']} (id bigint) SERVER {wrapper}_server"))
        for name in ("function", "public_function"):
            conn.execute(text(f"CREATE FUNCTION {objects[name]} RETURNS int LANGUAGE sql AS 'SELECT 1'"))
        conn.execute(text(f"REVOKE EXECUTE ON FUNCTION {objects['function']} FROM PUBLIC"))
    try:
        yield objects
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP VIEW IF EXISTS {objects['view']}"))
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {objects['matview']}"))
            conn.execute(text(f"DROP FOREIGN DATA WRAPPER IF EXISTS {wrapper} CASCADE"))
            for name in ("function", "public_function"):
                conn.execute(text(f"DROP FUNCTION IF EXISTS {objects[name]}"))


REFUSED = {
    "view_select": 'GRANT SELECT ON {view} TO "{name}"',
    "view_column_select": 'GRANT SELECT (source) ON {view} TO "{name}"',
    "matview_select": 'GRANT SELECT ON {matview} TO "{name}"',
    "foreign_table_select": 'GRANT SELECT ON {foreign} TO "{name}"',
    "function_execute": 'GRANT EXECUTE ON FUNCTION {function} TO "{name}"',
    "function_execute_through_public": "GRANT EXECUTE ON FUNCTION {function} TO PUBLIC",
    "table_references": 'GRANT REFERENCES ON perennial.securities TO "{name}"',
    "column_references": 'GRANT REFERENCES (id) ON perennial.securities TO "{name}"',
}


@pytest.mark.db
@pytest.mark.parametrize("case", sorted(REFUSED))
def test_a_path_through_another_owner_in_perennial_is_refused(
    engine, groups, caplog, owner_objects, case  # noqa: F811
):
    with login(engine, groups, connect=True) as (api_engine, name, password):
        with engine.begin() as conn:
            conn.execute(text(REFUSED[case].format(name=name, **owner_objects)))
        try:
            outcome = _refusal(engine, groups, caplog, api_engine,
                               (name, password, engine.url.database))
        finally:
            if case == "function_execute_through_public":
                with engine.begin() as conn:
                    conn.execute(text(f"REVOKE EXECUTE ON FUNCTION {owner_objects['function']} FROM PUBLIC"))
    with login(engine, groups, options="SUPERUSER") as (super_engine, n2, p2):
        reference = _refusal(engine, groups, caplog, super_engine, (n2, p2))
    assert outcome == reference  # the same fixed error as every other clause


@pytest.mark.db
def test_those_objects_existing_and_a_function_elsewhere_do_not_refuse(
    engine, groups, owner_objects  # noqa: F811
):
    """The guards: perennial's view, materialized view, foreign table and function exist
    and the login holds nothing on them; and it can EXECUTE a function in public (its
    default PUBLIC grant), which is outside the rule's scope, as pg_catalog is."""
    with login(engine, groups, connect=True) as (api_engine, _, _):
        with api_engine.connect() as conn:
            assert conn.execute(text(f"SELECT {owner_objects['public_function'][:-2]}()")).scalar() == 1
        with client(api_engine, groups) as c:
            assert_error(c.get("/v1/watchlist"), 404, "no_current_publication")
