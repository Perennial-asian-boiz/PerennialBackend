"""S2-04 (decision 113): the read API role may read perennial.fundamentals.

The rig builds only the grant; the GET /v1/securities/{symbol}/financials
endpoint is someone else's. The grant is column-level, like company_profiles
and the score tables: the nine columns a financials statement needs, never
concept (the XBRL tag it came from) or row_number (its place in the batch).

Readings this file pins:
- provision_roles.API_COLUMNS["fundamentals"] is exactly the nine columns, and
  fundamentals is not a whole-table grant (API_TABLES).
- After the grants phase the API group reads those nine and nothing else on
  fundamentals, and can't write it.
- Nothing else moves: run_dependencies, ingestion_runs and fundamental_scores
  stay refused, and the rest of API_TABLES/API_COLUMNS is as before.
- The Supabase API roles (stand-ins here) still hold nothing on fundamentals.
- Rerunning the grants leaves the same ACLs.
"""

import secrets
import sys

import pytest
from sqlalchemy import text
from test_read_api import (  # noqa: F401  (groups: pytest fixture)
    COLUMNS,
    FULL_TABLES,
    OPS,
    WRITES,
    _denied,
    _drop_roles,
    _provision,
    _readable,
    as_role,
    groups,
)

SERVED = (
    "id",
    "batch_id",
    "security_id",
    "fiscal_year_end",
    "line_item",
    "value",
    "unit",
    "form",
    "filed",
)
HIDDEN = ("concept", "row_number")
STILL_REFUSED = ("run_dependencies", "ingestion_runs", "fundamental_scores")
# The policy before S2-04, so a widened grant elsewhere fails here too.
API_TABLES_BEFORE = {
    "publications",
    "current_publication",
    "securities",
    "security_identifiers",
    "current_scores",
}
API_COLUMN_TABLES_BEFORE = {
    "company_profiles",
    "publication_events",
    "company_scores",
    "score_explanations",
}


def _roles_module():
    if str(OPS) not in sys.path:
        sys.path.insert(0, str(OPS))
    import provision_roles

    return provision_roles


def _columns(conn, table):
    return set(
        conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_schema = 'perennial' AND table_name = :t"
            ),
            {"t": table},
        ).scalars()
    )


def _table_privilege(conn, role, table, privilege):
    return conn.execute(
        text("SELECT has_table_privilege(:r, :t, :p)"),
        {"r": role, "t": f"perennial.{table}", "p": privilege},
    ).scalar()


def _any_column(conn, role, table, privilege):
    return conn.execute(
        text("SELECT has_any_column_privilege(:r, :t, :p)"),
        {"r": role, "t": f"perennial.{table}", "p": privilege},
    ).scalar()


def _column_privilege(conn, role, table, column, privilege):
    return conn.execute(
        text("SELECT has_column_privilege(:r, :t, :c, :p)"),
        {"r": role, "t": f"perennial.{table}", "c": column, "p": privilege},
    ).scalar()


def _acls(conn, table):
    """The table's ACL and every column's ACL, as stored."""
    relacl = conn.execute(
        text("SELECT relacl::text FROM pg_class WHERE oid = CAST(:t AS regclass)"),
        {"t": f"perennial.{table}"},
    ).scalar()
    columns = conn.execute(
        text(
            "SELECT attname, attacl::text FROM pg_attribute"
            " WHERE attrelid = CAST(:t AS regclass) AND attnum > 0 AND NOT attisdropped"
            " ORDER BY attname"
        ),
        {"t": f"perennial.{table}"},
    ).all()
    return relacl, [tuple(c) for c in columns]


# ── the policy ───────────────────────────────────────────


def test_the_api_columns_for_fundamentals_are_exactly_the_nine():
    roles = _roles_module()
    granted = roles.API_COLUMNS["fundamentals"]
    assert set(granted) == set(SERVED)
    assert len(granted) == len(SERVED)  # no column listed twice
    for column in HIDDEN:
        assert column not in granted, column
    assert "fundamentals" not in roles.API_TABLES  # column-level, never the whole table


def test_the_rest_of_the_api_policy_is_unchanged():
    roles = _roles_module()
    assert set(roles.API_TABLES) == API_TABLES_BEFORE
    assert set(roles.API_COLUMNS) == API_COLUMN_TABLES_BEFORE | {"fundamentals"}
    for table in STILL_REFUSED:
        assert table not in roles.API_TABLES and table not in roles.API_COLUMNS, table
    # test_read_api's exact-readable test compares against its own copy of the policy.
    assert set(FULL_TABLES) == API_TABLES_BEFORE
    assert {t: set(c) for t, c in COLUMNS.items()} == {
        t: set(c) for t, c in roles.API_COLUMNS.items()
    }


# ── what the API role can do after the grants phase ──────


@pytest.mark.db
def test_the_api_role_reads_the_nine_columns_and_nothing_else(engine, groups):  # noqa: F811
    api = groups["api"]
    with engine.connect() as conn:
        columns = _columns(conn, "fundamentals")
        # The table as the spec knows it: a new column must be decided on, not inherited.
        assert columns == set(SERVED) | set(HIDDEN)
        for column in sorted(columns):
            assert _column_privilege(conn, api, "fundamentals", column, "SELECT") is (
                column in SERVED
            ), column
        assert not _table_privilege(conn, api, "fundamentals", "SELECT")
    with as_role(engine, api) as conn:
        conn.execute(text(f"SELECT {', '.join(SERVED)} FROM perennial.fundamentals LIMIT 0"))
        for column in HIDDEN:
            _denied(conn, f"SELECT {column} FROM perennial.fundamentals")
        _denied(conn, "SELECT * FROM perennial.fundamentals")


@pytest.mark.db
def test_the_api_role_cannot_write_fundamentals(engine, groups):  # noqa: F811
    api = groups["api"]
    with engine.connect() as conn:
        for privilege in (*WRITES, "REFERENCES", "TRIGGER"):
            assert not _table_privilege(conn, api, "fundamentals", privilege), privilege
        for privilege in ("INSERT", "UPDATE", "REFERENCES"):
            assert not _any_column(conn, api, "fundamentals", privilege), privilege
    with as_role(engine, api) as conn:
        _denied(conn, "DELETE FROM perennial.fundamentals")
        _denied(conn, "TRUNCATE perennial.fundamentals")
        _denied(conn, "UPDATE perennial.fundamentals SET form = form")
        _denied(
            conn,
            "INSERT INTO perennial.fundamentals (batch_id, security_id, row_number,"
            " fiscal_year_end, line_item, value, unit, concept, form, filed)"
            " VALUES (1, 1, 1, '2025-12-31', 'revenue', 1, 'USD', 'Revenues', '10-K',"
            " '2026-02-01')",
        )


@pytest.mark.db
@pytest.mark.parametrize("table", STILL_REFUSED)
def test_the_api_role_still_has_nothing_on_the_neighbouring_tables(engine, groups, table):  # noqa: F811
    api = groups["api"]
    with engine.connect() as conn:
        for privilege in ("SELECT", *WRITES):
            assert not _table_privilege(conn, api, table, privilege), privilege
        for privilege in ("SELECT", "INSERT", "UPDATE"):
            assert not _any_column(conn, api, table, privilege), privilege
    with as_role(engine, api) as conn:
        _denied(conn, f"SELECT 1 FROM perennial.{table} LIMIT 1")


@pytest.mark.db
def test_the_supabase_api_roles_still_get_nothing_on_fundamentals(engine, groups):  # noqa: F811
    provision = _roles_module().provision
    suffix = secrets.token_hex(5)
    supabase = tuple(f"s204_{r}_{suffix}" for r in ("anon", "authenticated", "service_role"))
    roles = tuple(groups[r] for r in ("owner", "importer", "reader", "api"))
    try:
        with engine.begin() as conn:
            for name in supabase:
                conn.execute(text(f'CREATE ROLE "{name}" NOLOGIN'))
        with engine.begin() as conn:
            provision(conn.connection.driver_connection, roles, phase="grants", supabase_roles=supabase)
        with engine.connect() as conn:
            for name in supabase:
                for privilege in ("SELECT", *WRITES, "REFERENCES", "TRIGGER"):
                    assert not _table_privilege(conn, name, "fundamentals", privilege), (name, privilege)
                for privilege in ("SELECT", "INSERT", "UPDATE", "REFERENCES"):
                    assert not _any_column(conn, name, "fundamentals", privilege), (name, privilege)
            # The API group's grant isn't reachable through PUBLIC either.
            assert not _any_column(conn, "public", "fundamentals", "SELECT")
    finally:
        _drop_roles(engine, supabase)


@pytest.mark.db
def test_a_supabase_role_holding_a_fundamentals_column_is_refused(engine, groups):  # noqa: F811
    """The exposure guard covers the new grant's columns too: one SELECT (value) is enough."""
    provision = _roles_module().provision
    suffix = secrets.token_hex(5)
    supabase = tuple(f"s204_{r}_{suffix}" for r in ("anon", "authenticated", "service_role"))
    roles = tuple(groups[r] for r in ("owner", "importer", "reader", "api"))
    try:
        with engine.begin() as conn:
            for name in supabase:
                conn.execute(text(f'CREATE ROLE "{name}" NOLOGIN'))
        with engine.connect() as conn:
            conn.execute(text(f'GRANT SELECT (value) ON perennial.fundamentals TO "{supabase[0]}"'))
            with pytest.raises(ValueError) as caught:
                provision(conn.connection.driver_connection, roles, phase="grants", supabase_roles=supabase)
            for name in (*supabase, *roles):
                assert name not in str(caught.value)
            conn.rollback()
    finally:
        _drop_roles(engine, supabase)


@pytest.mark.db
def test_rerunning_the_grants_is_idempotent(engine, groups):  # noqa: F811
    api = groups["api"]
    roles = tuple(groups[r] for r in ("owner", "importer", "reader", "api"))
    with engine.connect() as conn:
        before = {t: _acls(conn, t) for t in ("fundamentals", *STILL_REFUSED)}
    readable = _readable(engine, api)
    assert {c for t, c in readable if t == "perennial.fundamentals"} == set(SERVED)
    _provision(engine, roles)
    _provision(engine, roles)
    with engine.connect() as conn:
        assert {t: _acls(conn, t) for t in before} == before
    assert _readable(engine, api) == readable
