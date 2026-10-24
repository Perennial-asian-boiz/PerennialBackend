"""RA-15 clause 3: a column privilege is a write privilege too (M5 R1, PR #50).

Design note: docs/work-items/m5-read-api-design-note.md, RA-15 ("no write
privilege on any perennial table"). has_table_privilege sees only table-level
grants, so INSERT (col) or UPDATE (col) on a perennial table must be refused the
same way as a table-level write. Found in the #22 edge pass (test_pr22_fix_edges).
"""

import pytest
from sqlalchemy import text

from test_read_api import _refusal, groups, login  # noqa: F401  (groups: pytest fixture)

COLUMN_WRITES = {
    "insert_column": 'GRANT INSERT (symbol) ON perennial.securities TO "{name}"',
    "update_column": 'GRANT UPDATE (name) ON perennial.securities TO "{name}"',
}


@pytest.mark.db
@pytest.mark.parametrize("case", sorted(COLUMN_WRITES))
def test_a_column_level_write_on_perennial_is_refused(engine, groups, caplog, case):  # noqa: F811
    """Granted directly to an otherwise correct login, so no other clause refuses it."""
    with login(engine, groups, connect=True) as (api_engine, name, password):
        with engine.begin() as conn:
            conn.execute(text(COLUMN_WRITES[case].format(name=name)))
        outcome = _refusal(engine, groups, caplog, api_engine,
                           (name, password, engine.url.database))
    with login(engine, groups, options="SUPERUSER") as (super_engine, n2, p2):
        reference = _refusal(engine, groups, caplog, super_engine, (n2, p2))
    assert outcome == reference  # the same fixed error as every other clause
