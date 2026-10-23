"""Market caps through the ingestion contract: the market_caps source and row pins.

Expand only (M4 MC-A, docs/work-items/m4-market-caps-packet.md). Adds
'market_caps' to the source vocabulary, the immutable market_caps batch table
and publication_market_cap_rows, which pins the exact row each publication
used per candidate. The legacy market_cap_observations and
publication_market_caps tables are untouched; production keeps writing them
until a real provider adapter is registered. Downgrade refuses while any
market_caps run, batch, row or pin exists: dropping them would orphan lineage
and change how retained publications replay. It only runs on a database with
none of that.

Numbered 0011, the next free id after Congress history's 0010; renumber at
merge if another migration lands first.
"""
from alembic import op
import sqlalchemy as sa

revision = '0011'
down_revision = '0010'
branch_labels = None
depends_on = None

OLD_SOURCES = (
    'congress_trades', 'ark_holdings', 'insider_trades', 'short_interest',
    'gdelt_theme_volume', 'gdelt_theme_articles', 'gdelt_spike_results',
)
NEW_SOURCES = OLD_SOURCES + ('market_caps',)


def _in(column, values):
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def _vocabulary(sources):
    for table in ('source_batches', 'ingestion_runs'):
        op.drop_constraint(op.f(f'ck_{table}_source'), table, schema='perennial', type_='check')
        op.create_check_constraint(op.f(f'ck_{table}_source'), table, _in('source', sources), schema='perennial')


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    _vocabulary(NEW_SOURCES)
    op.create_table('market_caps',
    sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
    sa.Column('batch_id', sa.BigInteger(), nullable=False),
    sa.Column('security_id', sa.BigInteger(), nullable=False),
    sa.Column('value', sa.Numeric(24, 2), nullable=True),
    sa.Column('currency', sa.String(length=3), nullable=False),
    sa.Column('provider', sa.String(length=32), nullable=False),
    sa.Column('issuer_level', sa.Boolean(), nullable=False),
    sa.Column('observed_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('sector', sa.String(length=64), nullable=True),
    sa.Column('industry', sa.String(length=128), nullable=True),
    sa.CheckConstraint("value IS NULL OR value > 0", name=op.f('ck_market_caps_positive_value')),
    sa.CheckConstraint("currency ~ '^[A-Z]{3}$'", name=op.f('ck_market_caps_currency')),
    sa.CheckConstraint("provider ~ '^[a-z0-9_]{1,32}$'", name=op.f('ck_market_caps_provider')),
    sa.ForeignKeyConstraint(['batch_id'], ['perennial.source_batches.id'], ondelete='CASCADE',
                            name=op.f('fk_market_caps_batch_id_source_batches')),
    sa.ForeignKeyConstraint(['security_id'], ['perennial.securities.id'],
                            name=op.f('fk_market_caps_security_id_securities')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_market_caps')),
    sa.UniqueConstraint('batch_id', 'security_id', name=op.f('uq_market_caps_batch_id_security_id')),
    sa.UniqueConstraint('id', 'batch_id', 'security_id', name=op.f('uq_market_caps_id_batch_id_security_id')),
    schema='perennial'
    )
    op.create_index('ix_market_caps_security_id', 'market_caps', ['security_id'], unique=False, schema='perennial')
    op.create_table('publication_market_cap_rows',
    sa.Column('publication_id', sa.BigInteger(), nullable=False),
    sa.Column('security_id', sa.BigInteger(), nullable=False),
    sa.Column('source', sa.String(length=32), nullable=False),
    sa.Column('run_id', sa.BigInteger(), nullable=True),
    sa.Column('batch_id', sa.BigInteger(), nullable=True),
    sa.Column('market_cap_id', sa.BigInteger(), nullable=True),
    sa.CheckConstraint("source = 'market_caps'", name=op.f('ck_publication_market_cap_rows_source')),
    sa.CheckConstraint(
        "(run_id IS NULL AND batch_id IS NULL AND market_cap_id IS NULL) OR "
        "(run_id IS NOT NULL AND batch_id IS NOT NULL AND market_cap_id IS NOT NULL)",
        name=op.f('ck_publication_market_cap_rows_lineage_all_or_none')),
    sa.ForeignKeyConstraint(['publication_id'], ['perennial.publications.id'],
                            name=op.f('fk_publication_market_cap_rows_publication_id_publications')),
    sa.ForeignKeyConstraint(['security_id'], ['perennial.securities.id'],
                            name=op.f('fk_publication_market_cap_rows_security_id_securities')),
    sa.ForeignKeyConstraint(['run_id', 'batch_id', 'source'],
                            ['perennial.ingestion_runs.id', 'perennial.ingestion_runs.batch_id',
                             'perennial.ingestion_runs.source'],
                            name=op.f('fk_publication_market_cap_rows_run_id_ingestion_runs')),
    sa.ForeignKeyConstraint(['market_cap_id', 'batch_id', 'security_id'],
                            ['perennial.market_caps.id', 'perennial.market_caps.batch_id',
                             'perennial.market_caps.security_id'],
                            name=op.f('fk_publication_market_cap_rows_market_cap_id_market_caps')),
    sa.PrimaryKeyConstraint('publication_id', 'security_id', name=op.f('pk_publication_market_cap_rows')),
    schema='perennial'
    )
    op.create_index('ix_publication_market_cap_rows_run_batch_source', 'publication_market_cap_rows',
                    ['run_id', 'batch_id', 'source'], unique=False, schema='perennial')
    op.create_index('ix_publication_market_cap_rows_market_cap_batch_security', 'publication_market_cap_rows',
                    ['market_cap_id', 'batch_id', 'security_id'], unique=False, schema='perennial')


def downgrade():
    # Refuse before any change: market_caps runs, batches, rows or pins would be
    # orphaned or dropped, and publications that pinned rows could not replay.
    if op.get_bind().execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM perennial.ingestion_runs WHERE source = 'market_caps')"
        " OR EXISTS (SELECT 1 FROM perennial.source_batches WHERE source = 'market_caps')"
        " OR EXISTS (SELECT 1 FROM perennial.market_caps)"
        " OR EXISTS (SELECT 1 FROM perennial.publication_market_cap_rows)"
    )).scalar():
        raise RuntimeError("market_caps state exists; downgrade would lose publication lineage")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_index('ix_publication_market_cap_rows_market_cap_batch_security',
                  table_name='publication_market_cap_rows', schema='perennial')
    op.drop_index('ix_publication_market_cap_rows_run_batch_source',
                  table_name='publication_market_cap_rows', schema='perennial')
    op.drop_table('publication_market_cap_rows', schema='perennial')
    op.drop_index('ix_market_caps_security_id', table_name='market_caps', schema='perennial')
    op.drop_table('market_caps', schema='perennial')
    _vocabulary(OLD_SOURCES)
