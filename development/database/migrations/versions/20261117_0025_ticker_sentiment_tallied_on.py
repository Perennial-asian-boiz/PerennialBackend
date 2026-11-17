"""ticker_sentiment.tallied_on: the date each row's scores were tallied.

perennial.ticker_sentiment gains tallied_on (date, NOT NULL): the UTC date the
weighting run computed the row. first_at/last_at are the scored items' window,
not the run, so until now the only way to date a tally was a join to
source_batches.imported_at.

Every existing row is backfilled from its batch's imported_at, as a UTC date,
before the column becomes NOT NULL. Grants are unchanged: table-level INSERT and
SELECT cover the new column.

Downgrade drops the column and keeps the rows; nothing is lost that
source_batches.imported_at does not still hold.
"""
import sqlalchemy as sa
from alembic import op

revision = '0025'
down_revision = '0024'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.add_column('ticker_sentiment', sa.Column('tallied_on', sa.Date(), nullable=True),
                  schema='perennial')
    op.execute(
        "UPDATE perennial.ticker_sentiment t "
        "SET tallied_on = (b.imported_at AT TIME ZONE 'UTC')::date "
        "FROM perennial.source_batches b WHERE b.id = t.batch_id"
    )
    op.alter_column('ticker_sentiment', 'tallied_on', nullable=False, schema='perennial')


def downgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_column('ticker_sentiment', 'tallied_on', schema='perennial')
