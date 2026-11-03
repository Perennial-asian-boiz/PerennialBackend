"""S1-05 source credibility weighting: social posts, article scores, publishers.

Expand only (docs/work-items/source-credibility-design-note.md). Adds
'social_posts' and 'article_sentiment' to the source vocabulary and three tables:

* social_posts: one row per (platform post, security) from the social
  pipeline's JSON files, on its 1-10 scale.
* article_sentiment: S1-04's score in [-1, 1] per news article and model (the
  agreed handoff; S1-04 writes it).
* news_publishers: the publisher domain behind each Finnhub article link.

The weighted result, ticker_sentiment, is created by 0018 (decision 108).
Downgrade refuses while any of these runs, batches or publisher rows exist.
"""
from alembic import op
import sqlalchemy as sa

revision = '0019'
down_revision = '0018'
branch_labels = None
depends_on = None

OLD_SOURCES = (
    'congress_trades', 'ark_holdings', 'insider_trades', 'short_interest',
    'gdelt_theme_volume', 'gdelt_theme_articles', 'gdelt_spike_results',
    'market_caps', 'fundamentals', 'fundamental_scores', 'news_articles',
    'ticker_sentiment', 'company_scores', 'score_explanations',
)
NEW = ('social_posts', 'article_sentiment')
NEW_SOURCES = OLD_SOURCES + NEW


def _in(column, values):
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def _vocabulary(sources):
    for table in ('source_batches', 'ingestion_runs'):
        op.drop_constraint(op.f(f'ck_{table}_source'), table, schema='perennial', type_='check')
        op.create_check_constraint(op.f(f'ck_{table}_source'), table, _in('source', sources), schema='perennial')


def _source_columns(table):
    return [
        sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column('batch_id', sa.BigInteger(), nullable=False),
        sa.Column('security_id', sa.BigInteger(), nullable=False),
        sa.Column('row_number', sa.Integer(), nullable=False),
        sa.CheckConstraint('row_number >= 1', name=op.f(f'ck_{table}_row_number')),
        sa.ForeignKeyConstraint(['batch_id'], ['perennial.source_batches.id'], ondelete='CASCADE',
                                name=op.f(f'fk_{table}_batch_id_source_batches')),
        sa.ForeignKeyConstraint(['security_id'], ['perennial.securities.id'],
                                name=op.f(f'fk_{table}_security_id_securities')),
        sa.PrimaryKeyConstraint('id', name=op.f(f'pk_{table}')),
        sa.UniqueConstraint('batch_id', 'row_number', name=op.f(f'uq_{table}_batch_id_row_number')),
    ]


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    _vocabulary(NEW_SOURCES)
    op.create_table(
        'social_posts',
        *_source_columns('social_posts'),
        sa.Column('platform', sa.String(length=16), nullable=False),
        sa.Column('post_id', sa.String(length=500), nullable=False),
        sa.Column('score', sa.Numeric(4, 2), nullable=False),
        sa.CheckConstraint("platform IN ('reddit', 'bluesky', 'mastodon', 'youtube')",
                           name=op.f('ck_social_posts_platform')),
        sa.CheckConstraint('score >= 1 AND score <= 10', name=op.f('ck_social_posts_score')),
        sa.UniqueConstraint('batch_id', 'security_id', 'platform', 'post_id',
                            name=op.f('uq_social_posts_batch_id_security_id_platform_post_id')),
        schema='perennial',
    )
    op.create_index('ix_social_posts_security_id', 'social_posts', ['security_id'], schema='perennial')
    op.create_table(
        'article_sentiment',
        *_source_columns('article_sentiment'),
        sa.Column('news_article_id', sa.BigInteger(), nullable=False),
        sa.Column('score', sa.Numeric(5, 4), nullable=False),
        sa.Column('model', sa.String(length=64), nullable=False),
        sa.CheckConstraint('score >= -1 AND score <= 1', name=op.f('ck_article_sentiment_score')),
        sa.ForeignKeyConstraint(['news_article_id'], ['perennial.news_articles.id'],
                                name=op.f('fk_article_sentiment_news_article_id_news_articles')),
        sa.UniqueConstraint('news_article_id', 'model',
                            name=op.f('uq_article_sentiment_news_article_id_model')),
        schema='perennial',
    )
    op.create_index('ix_article_sentiment_security_id', 'article_sentiment', ['security_id'],
                    schema='perennial')
    op.create_table(
        'news_publishers',
        sa.Column('finnhub_id', sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column('domain', sa.String(length=253), nullable=False),
        sa.Column('resolved_at', sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint('finnhub_id > 0', name=op.f('ck_news_publishers_finnhub_id')),
        sa.CheckConstraint("domain ~ '^[a-z0-9.-]+$'", name=op.f('ck_news_publishers_domain')),
        sa.PrimaryKeyConstraint('finnhub_id', name=op.f('pk_news_publishers')),
        schema='perennial',
    )


def downgrade():
    # Refuse before any change: runs, batches or publisher rows would be lost.
    sources = ', '.join(repr(s) for s in NEW)
    if op.get_bind().execute(sa.text(
        f"SELECT EXISTS (SELECT 1 FROM perennial.ingestion_runs WHERE source IN ({sources}))"
        f" OR EXISTS (SELECT 1 FROM perennial.source_batches WHERE source IN ({sources}))"
        " OR EXISTS (SELECT 1 FROM perennial.news_publishers)"
    )).scalar():
        raise RuntimeError("sentiment state exists; downgrade would lose lineage")
    op.execute("SET LOCAL lock_timeout = '5s'")
    for table in ('article_sentiment', 'social_posts'):
        op.drop_index(f'ix_{table}_security_id', table_name=table, schema='perennial')
    for table in ('news_publishers', 'article_sentiment', 'social_posts'):
        op.drop_table(table, schema='perennial')
    _vocabulary(OLD_SOURCES)
