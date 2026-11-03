"""JOB-7 / AC 4 article-scoring retries: failed attempts per article and model.

Expand only. Adds one state table:

* article_scoring_attempts: one row per (news article, model version) that is
  still unscored after at least one failed scoring attempt. The scorer bumps
  attempts and last_error on each failure and deletes the row once the article
  is scored; at ARTICLE_SCORING_MAX_ATTEMPTS the article is no longer read. The
  primary key (news_article_id, model) keeps one record per article and model,
  and a new model version starts with no rows.

JOB-7 / AC 4 (no numbered decision): a state table keyed on its natural key, not a batch table.
Downgrade refuses while any retry rows exist: they are the only record of which
articles have exhausted their retries.
"""
from alembic import op
import sqlalchemy as sa

revision = '0020'
down_revision = '0019'
branch_labels = None
depends_on = None

ERRORS = ('failed', 'timeout', 'import_failed')


def _in(column, values):
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.create_table(
        'article_scoring_attempts',
        sa.Column('news_article_id', sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column('model', sa.String(length=64), nullable=False),
        sa.Column('attempts', sa.SmallInteger(), nullable=False),
        sa.Column('last_error', sa.String(length=16), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'),
                  nullable=False),
        sa.CheckConstraint('attempts >= 1', name=op.f('ck_article_scoring_attempts_attempts')),
        sa.CheckConstraint(_in('last_error', ERRORS), name=op.f('ck_article_scoring_attempts_last_error')),
        sa.ForeignKeyConstraint(['news_article_id'], ['perennial.news_articles.id'], ondelete='CASCADE',
                                name=op.f('fk_article_scoring_attempts_news_article_id_news_articles')),
        sa.PrimaryKeyConstraint('news_article_id', 'model', name=op.f('pk_article_scoring_attempts')),
        schema='perennial',
    )


def downgrade():
    # Refuse before any change: retry state would be lost.
    if op.get_bind().execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM perennial.article_scoring_attempts)"
    )).scalar():
        raise RuntimeError("article scoring retry state exists; downgrade would lose it")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_table('article_scoring_attempts', schema='perennial')
