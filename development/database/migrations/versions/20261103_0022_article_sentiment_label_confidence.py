"""JOB-7 AC 1: article_sentiment.label and article_sentiment.confidence.

Expand only. perennial.article_sentiment gains two nullable columns: label
(varchar 8: negative, neutral or positive, from the score's thresholds) and
confidence (numeric 5,4 in [0, 1], the model's probability for the label's
class). The scorer sets both on every row it writes from now on, together or
not at all; rows scored before 0022 keep both NULL. Grants are unchanged: the
importer's table-level INSERT and SELECT cover the new columns.

Downgrade refuses before any change while any label is stored, since dropping
the columns would lose them; with every label NULL it drops the columns and
keeps the rows.
"""
from alembic import op
import sqlalchemy as sa

revision = '0022'
down_revision = '0021'
branch_labels = None
depends_on = None

CHECKS = {
    'label': "label IS NULL OR label IN ('negative', 'neutral', 'positive')",
    'confidence': 'confidence >= 0 AND confidence <= 1',
    'label_confidence': '(label IS NULL) = (confidence IS NULL)',
}


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.add_column('article_sentiment', sa.Column('label', sa.String(length=8), nullable=True),
                  schema='perennial')
    op.add_column('article_sentiment', sa.Column('confidence', sa.Numeric(5, 4), nullable=True),
                  schema='perennial')
    for name, condition in CHECKS.items():
        op.create_check_constraint(op.f(f'ck_article_sentiment_{name}'), 'article_sentiment',
                                   condition, schema='perennial')


def downgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    # Refuse before any change: the stored labels would be lost. Never quote one.
    if op.get_bind().execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM perennial.article_sentiment WHERE label IS NOT NULL)"
    )).scalar():
        raise RuntimeError("article sentiment labels exist; downgrade would lose them")
    for name in reversed(CHECKS):
        op.drop_constraint(op.f(f'ck_article_sentiment_{name}'), 'article_sentiment',
                           type_='check', schema='perennial')
    op.drop_column('article_sentiment', 'confidence', schema='perennial')
    op.drop_column('article_sentiment', 'label', schema='perennial')
