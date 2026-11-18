"""S3-02 Sentiment Pulse breakdown: social_posts.label and ticker_sentiment_items.

social_posts.label:
Each post's label from its score, with the article scorer's cutoffs
(src/services/credibility/labels.py): negative for score <= -0.2, positive for
score >= 0.2, neutral between. The sentiment pipeline writes it (run_social);
every existing row is backfilled from its score before the column becomes
NOT NULL, and a CHECK keeps the label equal to what its score implies.

ticker_sentiment_items:
One row per article or post a ticker_sentiment row counted: the score, label
and final weight (tier x age decay x relevance) the weighting used, and the
item's date. The importer writes them with their ticker_sentiment row, in the
same transaction. The read API sums them into the Sentiment Pulse bars, so the
bars are built from exactly the items behind the stored value. Rows written
before this migration have no items.

Grants (provision_roles): ticker_sentiment_items is immutable like the other
batch tables (importer SELECT and INSERT, reader SELECT); the API may read the
listed columns of ticker_sentiment and ticker_sentiment_items.

Downgrade refuses before any change while any item row exists, since the items
of past tallies cannot be rebuilt; with none it drops the table, then the label
column (which its score still implies).
"""
import sqlalchemy as sa
from alembic import op

revision = '0026'
down_revision = '0025'
branch_labels = None
depends_on = None

LABELS = ('negative', 'neutral', 'positive')
# Equal to labels.NEGATIVE_AT_MOST and labels.POSITIVE_AT_LEAST (a test checks).
NEGATIVE_AT_MOST = '-0.2'
POSITIVE_AT_LEAST = '0.2'
LABEL_OF_SCORE = (
    f"CASE WHEN score <= {NEGATIVE_AT_MOST} THEN 'negative' "
    f"WHEN score >= {POSITIVE_AT_LEAST} THEN 'positive' ELSE 'neutral' END"
)
POSTS = 'perennial.social_posts'


def _in(column, values):
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.add_column('social_posts', sa.Column('label', sa.String(length=8), nullable=True),
                  schema='perennial')
    op.execute(f"UPDATE {POSTS} SET label = {LABEL_OF_SCORE}")
    op.alter_column('social_posts', 'label', nullable=False, schema='perennial')
    op.create_check_constraint(op.f('ck_social_posts_label'), 'social_posts',
                               _in('label', LABELS), schema='perennial')
    op.create_check_constraint(op.f('ck_social_posts_label_of_score'), 'social_posts',
                               f"label = {LABEL_OF_SCORE}", schema='perennial')

    op.create_table(
        'ticker_sentiment_items',
        sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column('ticker_sentiment_id', sa.BigInteger(), nullable=False),
        sa.Column('kind', sa.String(length=8), nullable=False),
        sa.Column('news_article_id', sa.BigInteger(), nullable=True),
        sa.Column('social_post_id', sa.BigInteger(), nullable=True),
        sa.Column('score', sa.Numeric(precision=5, scale=4), nullable=False),
        sa.Column('label', sa.String(length=8), nullable=False),
        sa.Column('weight', sa.Numeric(precision=12, scale=10), nullable=False),
        sa.Column('item_at', sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(_in('kind', ('news', 'social')),
                           name=op.f('ck_ticker_sentiment_items_kind')),
        sa.CheckConstraint("(kind = 'news') = (news_article_id IS NOT NULL) "
                           "AND (kind = 'social') = (social_post_id IS NOT NULL)",
                           name=op.f('ck_ticker_sentiment_items_kind_source')),
        sa.CheckConstraint('score >= -1 AND score <= 1',
                           name=op.f('ck_ticker_sentiment_items_score')),
        sa.CheckConstraint(_in('label', LABELS), name=op.f('ck_ticker_sentiment_items_label')),
        sa.CheckConstraint('weight >= 0', name=op.f('ck_ticker_sentiment_items_weight')),
        sa.ForeignKeyConstraint(
            ['ticker_sentiment_id'], ['perennial.ticker_sentiment.id'],
            name=op.f('fk_ticker_sentiment_items_ticker_sentiment_id_ticker_sentiment')),
        sa.ForeignKeyConstraint(
            ['news_article_id'], ['perennial.news_articles.id'],
            name=op.f('fk_ticker_sentiment_items_news_article_id_news_articles')),
        sa.ForeignKeyConstraint(
            ['social_post_id'], ['perennial.social_posts.id'],
            name=op.f('fk_ticker_sentiment_items_social_post_id_social_posts')),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_ticker_sentiment_items')),
        sa.UniqueConstraint('ticker_sentiment_id', 'news_article_id',
                            name=op.f('uq_ticker_sentiment_items_ticker_sentiment_id_news_article_id')),
        sa.UniqueConstraint('ticker_sentiment_id', 'social_post_id',
                            name=op.f('uq_ticker_sentiment_items_ticker_sentiment_id_social_post_id')),
        schema='perennial',
    )


def downgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    # Refuse before any change: past tallies' items cannot be rebuilt.
    if op.get_bind().execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM perennial.ticker_sentiment_items)"
    )).scalar():
        raise RuntimeError("ticker_sentiment_items rows exist; downgrade would lose them")
    op.drop_table('ticker_sentiment_items', schema='perennial')
    op.drop_constraint(op.f('ck_social_posts_label_of_score'), 'social_posts',
                       schema='perennial', type_='check')
    op.drop_constraint(op.f('ck_social_posts_label'), 'social_posts',
                       schema='perennial', type_='check')
    op.drop_column('social_posts', 'label', schema='perennial')
