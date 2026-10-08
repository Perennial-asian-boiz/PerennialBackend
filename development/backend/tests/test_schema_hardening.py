"""Inspect real constraints as Alembic autogenerate does not compare CHECK names."""
import pytest
from alembic import command
from sqlalchemy import CheckConstraint, inspect, text
from sqlalchemy.exc import IntegrityError

from src.db.models import metadata
from tests.conftest import alembic_config

pytestmark = pytest.mark.db


def test_historical_checks_normalized_and_still_enforced(engine):
    with engine.connect() as conn:
        for table in metadata.sorted_tables:
            actual = {c['name'] for c in inspect(conn).get_check_constraints(table.name, schema='perennial')}
            expected = {str(c.name) for c in table.constraints if isinstance(c, CheckConstraint)}
            assert actual == expected, table.name
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO perennial.securities(symbol) VALUES ('lowercase')"))


def test_upgrade_preserves_v2_data_and_downgrade_restores_names(engine):
    with engine.connect() as conn:
        command.downgrade(alembic_config(conn), '0002')
        conn.execute(text("INSERT INTO perennial.securities(symbol) VALUES ('ZZKEEP')"))
        old = {c['name'] for c in inspect(conn).get_check_constraints('securities', schema='perennial')}
        assert 'ck_securities_ck_securities_symbol_format' in old
        command.upgrade(alembic_config(conn), 'head')
        new = {c['name'] for c in inspect(conn).get_check_constraints('securities', schema='perennial')}
        assert 'ck_securities_symbol_format' in new
        assert conn.execute(text("SELECT symbol FROM perennial.securities")).scalars().all() == ['ZZKEEP']
        command.downgrade(alembic_config(conn), '0002')
        restored = {c['name'] for c in inspect(conn).get_check_constraints('securities', schema='perennial')}
        assert restored == old
        command.upgrade(alembic_config(conn), 'head')


def test_referencing_columns_have_supporting_indexes(engine):
    expected = {
        'publication_sources': ['run_id', 'batch_id', 'source'],
        'run_dependencies': ['batch_id', 'source'],
        'publication_market_caps': ['observation_id', 'security_id'],
        'ingestion_runs': ['source', 'started_at', 'id'],
    }
    with engine.connect() as conn:
        for table, cols in expected.items():
            assert cols in [i['column_names'] for i in inspect(conn).get_indexes(table, schema='perennial')], table
