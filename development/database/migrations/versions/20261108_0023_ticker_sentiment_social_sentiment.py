"""ticker_sentiment.social_sentiment: the social-only sentiment per (batch, security).

Expand only. perennial.ticker_sentiment gains one nullable column,
social_sentiment, typed like the table's other sentiment columns (value,
plain_average): numeric(5,4) in [-1, 1]. It is NULL when no social post was
counted (social_count = 0) and for every row written before 0023. Grants are
unchanged: table-level INSERT and SELECT cover the new column.

Downgrade refuses before any change while any social_sentiment is stored, since
dropping the column would lose it; with every value NULL it drops the column and
keeps the rows.
"""
import sqlalchemy as sa
from alembic import op

revision = '0023'
down_revision = '0022'
branch_labels = None
depends_on = None

CHECKS = {
    'social_sentiment_range': 'social_sentiment IS NULL OR social_sentiment BETWEEN -1 AND 1',
    'social_sentiment_counted': 'social_sentiment IS NULL OR social_count > 0',
}


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.add_column('ticker_sentiment', sa.Column('social_sentiment', sa.Numeric(5, 4), nullable=True),
                  schema='perennial')
    for name, condition in CHECKS.items():
        op.create_check_constraint(op.f(f'ck_ticker_sentiment_{name}'), 'ticker_sentiment',
                                   condition, schema='perennial')


def downgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    # Refuse before any change: the stored values would be lost.
    if op.get_bind().execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM perennial.ticker_sentiment WHERE social_sentiment IS NOT NULL)"
    )).scalar():
        raise RuntimeError("ticker social sentiment values exist; downgrade would lose them")
    for name in reversed(CHECKS):
        op.drop_constraint(op.f(f'ck_ticker_sentiment_{name}'), 'ticker_sentiment',
                           type_='check', schema='perennial')
    op.drop_column('ticker_sentiment', 'social_sentiment', schema='perennial')
