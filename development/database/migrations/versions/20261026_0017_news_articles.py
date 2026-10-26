"""S1-03 news ingestion: the news_articles source and per-run news_coverage.

Expand only (docs/work-items/news-ingestion-design-note.md). Adds
'news_articles' to the source vocabulary and two tables:

* news_articles: one row per (security, Finnhub article), in the batch of the
  run that first saw it. UNIQUE (security_id, finnhub_id) makes a duplicate
  impossible across runs (AC 4). Headlines and metadata only: no article text.
* news_coverage: one row per (run, security) with the article counts, or
  'no_coverage', or 'error' with a fixed code (AC 5).

Downgrade refuses while any news run, batch or coverage row exists. Numbered
0017, the next free id after 0016; renumber at merge if another lands first.
"""
from alembic import op
import sqlalchemy as sa

revision = '0017'
down_revision = '0016'
branch_labels = None
depends_on = None

OLD_SOURCES = (
    'congress_trades', 'ark_holdings', 'insider_trades', 'short_interest',
    'gdelt_theme_volume', 'gdelt_theme_articles', 'gdelt_spike_results',
    'market_caps', 'fundamentals', 'fundamental_scores',
)
NEW_SOURCES = OLD_SOURCES + ('news_articles',)


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
        'news_articles',
        sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column('batch_id', sa.BigInteger(), nullable=False),
        sa.Column('security_id', sa.BigInteger(), nullable=False),
        sa.Column('row_number', sa.Integer(), nullable=False),
        sa.Column('finnhub_id', sa.BigInteger(), nullable=False),
        sa.Column('url', sa.String(length=2048), nullable=False),
        sa.Column('headline', sa.String(length=500), nullable=False),
        sa.Column('outlet', sa.String(length=100), nullable=False),
        sa.Column('published_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('content_retrieved', sa.String(length=24), nullable=False),
        sa.Column('match_rule', sa.String(length=8), nullable=False),
        sa.Column('content_hash', sa.String(length=64), nullable=False),
        sa.CheckConstraint('row_number >= 1', name=op.f('ck_news_articles_row_number')),
        sa.CheckConstraint('finnhub_id > 0', name=op.f('ck_news_articles_finnhub_id')),
        sa.CheckConstraint("url ~ '^https?://'", name=op.f('ck_news_articles_url_scheme')),
        sa.CheckConstraint("content_retrieved IN ('headline_and_summary', 'headline_only')",
                           name=op.f('ck_news_articles_content_retrieved')),
        sa.CheckConstraint("match_rule IN ('ticker', 'name')", name=op.f('ck_news_articles_match_rule')),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name=op.f('ck_news_articles_content_hash')),
        sa.ForeignKeyConstraint(['batch_id'], ['perennial.source_batches.id'], ondelete='CASCADE',
                                name=op.f('fk_news_articles_batch_id_source_batches')),
        sa.ForeignKeyConstraint(['security_id'], ['perennial.securities.id'],
                                name=op.f('fk_news_articles_security_id_securities')),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_news_articles')),
        sa.UniqueConstraint('batch_id', 'row_number', name=op.f('uq_news_articles_batch_id_row_number')),
        sa.UniqueConstraint('security_id', 'finnhub_id', name=op.f('uq_news_articles_security_id_finnhub_id')),
        schema='perennial',
    )
    op.create_index('ix_news_articles_published_at', 'news_articles', ['published_at'], unique=False,
                    schema='perennial')
    op.create_table(
        'news_coverage',
        sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column('run_id', sa.BigInteger(), nullable=False),
        sa.Column('security_id', sa.BigInteger(), nullable=False),
        sa.Column('status', sa.String(length=16), nullable=False),
        sa.Column('articles_found', sa.Integer(), nullable=False),
        sa.Column('articles_new', sa.Integer(), nullable=False),
        sa.Column('error_code', sa.String(length=64), nullable=True),
        sa.CheckConstraint("status IN ('covered', 'no_coverage', 'error')", name=op.f('ck_news_coverage_status')),
        sa.CheckConstraint('articles_found >= 0 AND articles_new >= 0 AND articles_new <= articles_found',
                           name=op.f('ck_news_coverage_counts')),
        sa.CheckConstraint("(status = 'error') = (error_code IS NOT NULL)", name=op.f('ck_news_coverage_error_code')),
        sa.CheckConstraint("status <> 'no_coverage' OR articles_found = 0",
                           name=op.f('ck_news_coverage_no_coverage_empty')),
        sa.ForeignKeyConstraint(['run_id'], ['perennial.ingestion_runs.id'], ondelete='CASCADE',
                                name=op.f('fk_news_coverage_run_id_ingestion_runs')),
        sa.ForeignKeyConstraint(['security_id'], ['perennial.securities.id'],
                                name=op.f('fk_news_coverage_security_id_securities')),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_news_coverage')),
        sa.UniqueConstraint('run_id', 'security_id', name=op.f('uq_news_coverage_run_id_security_id')),
        schema='perennial',
    )
    op.create_index('ix_news_coverage_security_id', 'news_coverage', ['security_id'], unique=False,
                    schema='perennial')


def downgrade():
    # Refuse before any change: news runs, batches or coverage rows would be orphaned.
    if op.get_bind().execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM perennial.ingestion_runs WHERE source = 'news_articles')"
        " OR EXISTS (SELECT 1 FROM perennial.source_batches WHERE source = 'news_articles')"
        " OR EXISTS (SELECT 1 FROM perennial.news_coverage)"
    )).scalar():
        raise RuntimeError("news state exists; downgrade would lose lineage")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_index('ix_news_coverage_security_id', table_name='news_coverage', schema='perennial')
    op.drop_table('news_coverage', schema='perennial')
    op.drop_index('ix_news_articles_published_at', table_name='news_articles', schema='perennial')
    op.drop_table('news_articles', schema='perennial')
    _vocabulary(OLD_SOURCES)
