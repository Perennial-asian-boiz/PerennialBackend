"""Decision 114, JOB-7 tier 1: news_articles.summary, Finnhub's article summary.

Expand only. Adds one nullable column, perennial.news_articles.summary
(varchar 2000, no default): the news job stores Finnhub's summary there, cut to
2000 characters, and JOB-7 scores headline + summary. Rows stored before 0020
keep a NULL summary. Grants are unchanged: the importer's table-level INSERT
and SELECT cover the new column, and the API has nothing on news_articles.

Downgrade refuses before any change while any summary is stored, since
dropping the column would lose them; with every summary NULL it drops the
column and keeps the rows.
"""
from alembic import op
import sqlalchemy as sa

revision = '0020'
down_revision = '0019'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.add_column('news_articles', sa.Column('summary', sa.String(length=2000), nullable=True),
                  schema='perennial')


def downgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    # Refuse before any change: the stored summaries would be lost. Never quote one.
    if op.get_bind().execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM perennial.news_articles WHERE summary IS NOT NULL)"
    )).scalar():
        raise RuntimeError("article summaries exist; downgrade would lose them")
    op.drop_column('news_articles', 'summary', schema='perennial')
