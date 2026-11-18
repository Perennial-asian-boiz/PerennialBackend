"""S3-01 company price breakdown: a daily price snapshot per security.

Expand only. Adds 'daily_prices' to the source vocabulary and one immutable batch
table:

* daily_prices: one row per (batch, security), taken after the US close: the
  latest close and its trading date, the previous close, and the 52-week high and
  low with their dates, as the provider reported them, plus the provider's name.
  Readers serve a security's newest row.

Whether a provider's prices may be displayed is decided when the API responds
(the licensed providers list), never stored here.

Downgrade refuses while any daily_prices run or batch exists.
"""
from alembic import op
import sqlalchemy as sa

revision = '0026'
down_revision = '0025'
branch_labels = None
depends_on = None

OLD_SOURCES = (
    'congress_trades', 'ark_holdings', 'insider_trades', 'short_interest',
    'gdelt_theme_volume', 'gdelt_theme_articles', 'gdelt_spike_results',
    'market_caps', 'fundamentals', 'fundamental_scores', 'news_articles',
    'ticker_sentiment', 'company_scores', 'score_explanations',
    'social_posts', 'article_sentiment',
)
NEW_SOURCES = OLD_SOURCES + ('daily_prices',)


def _in(column, values):
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def _vocabulary(sources):
    for table in ('source_batches', 'ingestion_runs'):
        op.drop_constraint(op.f(f'ck_{table}_source'), table, schema='perennial', type_='check')
        op.create_check_constraint(op.f(f'ck_{table}_source'), table, _in('source', sources), schema='perennial')


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    _vocabulary(NEW_SOURCES)
    op.create_table(
        'daily_prices',
        sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column('batch_id', sa.BigInteger(), nullable=False),
        sa.Column('security_id', sa.BigInteger(), nullable=False),
        sa.Column('row_number', sa.Integer(), nullable=False),
        sa.Column('trading_date', sa.Date(), nullable=False),
        sa.Column('quoted_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('close', sa.Numeric(20, 6), nullable=False),
        sa.Column('previous_close', sa.Numeric(20, 6), nullable=True),
        sa.Column('high_52w', sa.Numeric(20, 6), nullable=True),
        sa.Column('high_52w_date', sa.Date(), nullable=True),
        sa.Column('low_52w', sa.Numeric(20, 6), nullable=True),
        sa.Column('low_52w_date', sa.Date(), nullable=True),
        sa.Column('currency', sa.String(length=3), nullable=False),
        sa.Column('provider', sa.String(length=32), nullable=False),
        sa.CheckConstraint('row_number >= 1', name=op.f('ck_daily_prices_row_number')),
        sa.CheckConstraint('close > 0 AND (previous_close IS NULL OR previous_close > 0)',
                           name=op.f('ck_daily_prices_close')),
        # The range comes as a pair, or not at all.
        sa.CheckConstraint('(high_52w IS NULL) = (low_52w IS NULL)'
                           ' AND (low_52w IS NULL OR (low_52w > 0 AND low_52w <= high_52w))',
                           name=op.f('ck_daily_prices_range')),
        sa.CheckConstraint('(high_52w IS NOT NULL OR high_52w_date IS NULL)'
                           ' AND (low_52w IS NOT NULL OR low_52w_date IS NULL)',
                           name=op.f('ck_daily_prices_range_dates')),
        sa.CheckConstraint("currency ~ '^[A-Z]{3}$'", name=op.f('ck_daily_prices_currency')),
        sa.CheckConstraint("provider ~ '^[a-z0-9_]{1,32}$'", name=op.f('ck_daily_prices_provider')),
        sa.ForeignKeyConstraint(['batch_id'], ['perennial.source_batches.id'], ondelete='CASCADE',
                                name=op.f('fk_daily_prices_batch_id_source_batches')),
        sa.ForeignKeyConstraint(['security_id'], ['perennial.securities.id'],
                                name=op.f('fk_daily_prices_security_id_securities')),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_daily_prices')),
        sa.UniqueConstraint('batch_id', 'row_number', name=op.f('uq_daily_prices_batch_id_row_number')),
        sa.UniqueConstraint('batch_id', 'security_id', name=op.f('uq_daily_prices_batch_id_security_id')),
        schema='perennial',
    )
    op.create_index('ix_daily_prices_security_id', 'daily_prices', ['security_id'], unique=False,
                    schema='perennial')


def downgrade():
    # Refuse before any change: price runs or batches would be orphaned.
    if op.get_bind().execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM perennial.ingestion_runs WHERE source = 'daily_prices')"
        " OR EXISTS (SELECT 1 FROM perennial.source_batches WHERE source = 'daily_prices')"
    )).scalar():
        raise RuntimeError("daily_prices state exists; downgrade would lose lineage")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_index('ix_daily_prices_security_id', table_name='daily_prices', schema='perennial')
    op.drop_table('daily_prices', schema='perennial')
    _vocabulary(OLD_SOURCES)
