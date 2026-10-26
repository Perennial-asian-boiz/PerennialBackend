"""Pattern evaluation: the fundamentals source and the fundamental_scores derivation.

Expand only (docs/work-items/pattern-evaluation-design.md). Adds 'fundamentals'
and 'fundamental_scores' to the source vocabulary, and two immutable batch
tables:

* fundamentals: SEC EDGAR annual line items, one row per (batch, security,
  fiscal_year_end, line_item), signed Numeric values with their unit.
* fundamental_scores: one row per (batch, security) for the score derived from
  one recorded fundamentals batch, with the measures, the missing-data codes and
  the score_version. The scoring limits live in the batch envelope.

Downgrade refuses while any fundamentals run, batch or row exists: dropping them
would orphan lineage. Numbered 0016, the next free id after auth's 0015;
renumber at merge if another migration lands first.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '0016'
down_revision = '0015'
branch_labels = None
depends_on = None

OLD_SOURCES = (
    'congress_trades', 'ark_holdings', 'insider_trades', 'short_interest',
    'gdelt_theme_volume', 'gdelt_theme_articles', 'gdelt_spike_results',
    'market_caps',
)
NEW_SOURCES = OLD_SOURCES + ('fundamentals', 'fundamental_scores')
LINE_ITEMS = (
    'revenue', 'revenue_net_of_interest', 'gross_profit', 'cost_of_revenue',
    'operating_income', 'pretax_income', 'net_income', 'operating_cash_flow',
    'assets', 'current_assets', 'current_liabilities', 'liabilities',
    'long_term_debt', 'equity', 'diluted_shares', 'noninterest_expense',
)
MISSING_CODES = ('no_revenue', 'not_enough_data', 'insufficient_history')


def _in(column, values):
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def _vocabulary(sources):
    for table in ('source_batches', 'ingestion_runs'):
        op.drop_constraint(op.f(f'ck_{table}_source'), table, schema='perennial', type_='check')
        op.create_check_constraint(op.f(f'ck_{table}_source'), table, _in('source', sources), schema='perennial')


def _batch_and_security(table):
    return [
        sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column('batch_id', sa.BigInteger(), nullable=False),
        sa.Column('security_id', sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(['batch_id'], ['perennial.source_batches.id'], ondelete='CASCADE',
                                name=op.f(f'fk_{table}_batch_id_source_batches')),
        sa.ForeignKeyConstraint(['security_id'], ['perennial.securities.id'],
                                name=op.f(f'fk_{table}_security_id_securities')),
        sa.PrimaryKeyConstraint('id', name=op.f(f'pk_{table}')),
    ]


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    _vocabulary(NEW_SOURCES)
    op.create_table(
        'fundamentals',
        *_batch_and_security('fundamentals'),
        sa.Column('row_number', sa.Integer(), nullable=False),
        sa.Column('fiscal_year_end', sa.Date(), nullable=False),
        sa.Column('line_item', sa.String(length=32), nullable=False),
        sa.Column('value', sa.Numeric(24, 2), nullable=False),
        sa.Column('unit', sa.String(length=8), nullable=False),
        sa.Column('concept', sa.String(length=160), nullable=False),
        sa.Column('form', sa.String(length=8), nullable=False),
        sa.Column('filed', sa.Date(), nullable=False),
        sa.CheckConstraint('row_number >= 1', name=op.f('ck_fundamentals_row_number')),
        sa.CheckConstraint(_in('line_item', LINE_ITEMS), name=op.f('ck_fundamentals_line_item')),
        sa.CheckConstraint("unit ~ '^([A-Z]{3}|shares)$'", name=op.f('ck_fundamentals_unit')),
        sa.UniqueConstraint('batch_id', 'row_number', name=op.f('uq_fundamentals_batch_id_row_number')),
        sa.UniqueConstraint('batch_id', 'security_id', 'fiscal_year_end', 'line_item',
                            name=op.f('uq_fundamentals_batch_id_security_id_fiscal_year_end_line_item')),
        schema='perennial',
    )
    op.create_index('ix_fundamentals_security_id', 'fundamentals', ['security_id'], unique=False,
                    schema='perennial')
    op.create_table(
        'fundamental_scores',
        *_batch_and_security('fundamental_scores'),
        sa.Column('fiscal_year_end', sa.Date(), nullable=True),
        sa.Column('model', sa.String(length=8), nullable=True),
        sa.Column('score', sa.Numeric(4, 1), nullable=True),
        sa.Column('missing', postgresql.ARRAY(sa.String(length=32)), nullable=False),
        sa.Column('revenue_growth_3y', sa.Numeric(16, 6), nullable=True),
        sa.Column('operating_margin', sa.Numeric(16, 6), nullable=True),
        sa.Column('operating_margin_trend', sa.Numeric(16, 6), nullable=True),
        sa.Column('return_on_equity', sa.Numeric(16, 6), nullable=True),
        sa.Column('efficiency_ratio', sa.Numeric(16, 6), nullable=True),
        sa.Column('profit_source', sa.String(length=24), nullable=True),
        sa.Column('gross_margin', sa.Numeric(16, 6), nullable=True),
        sa.Column('piotroski_score', sa.SmallInteger(), nullable=True),
        sa.Column('piotroski_signals', sa.SmallInteger(), nullable=False),
        sa.Column('score_version', sa.String(length=32), nullable=False),
        sa.CheckConstraint("model IS NULL OR model IN ('general', 'bank')",
                           name=op.f('ck_fundamental_scores_model')),
        sa.CheckConstraint('score IS NULL OR (score >= 0 AND score <= 100)',
                           name=op.f('ck_fundamental_scores_score')),
        sa.CheckConstraint('missing <@ ARRAY[' + ', '.join(repr(c) for c in MISSING_CODES) + ']::varchar[]',
                           name=op.f('ck_fundamental_scores_missing_codes')),
        sa.CheckConstraint('piotroski_signals BETWEEN 0 AND 9 AND (piotroski_score IS NULL OR '
                           'piotroski_score BETWEEN 0 AND piotroski_signals)',
                           name=op.f('ck_fundamental_scores_piotroski')),
        sa.UniqueConstraint('batch_id', 'security_id', name=op.f('uq_fundamental_scores_batch_id_security_id')),
        schema='perennial',
    )
    op.create_index('ix_fundamental_scores_security_id', 'fundamental_scores', ['security_id'],
                    unique=False, schema='perennial')


def downgrade():
    # Refuse before any change: fundamentals runs, batches or rows would be orphaned.
    if op.get_bind().execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM perennial.ingestion_runs"
        " WHERE source IN ('fundamentals', 'fundamental_scores'))"
        " OR EXISTS (SELECT 1 FROM perennial.source_batches"
        " WHERE source IN ('fundamentals', 'fundamental_scores'))"
    )).scalar():
        raise RuntimeError("fundamentals state exists; downgrade would lose lineage")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_index('ix_fundamental_scores_security_id', table_name='fundamental_scores', schema='perennial')
    op.drop_table('fundamental_scores', schema='perennial')
    op.drop_index('ix_fundamentals_security_id', table_name='fundamentals', schema='perennial')
    op.drop_table('fundamentals', schema='perennial')
    _vocabulary(OLD_SOURCES)
