"""GDELT macro events: daily theme volume and tone, evidence articles, spike checks.

These tables stand apart from source batches and publications, so a GDELT
outage never blocks the consensus watchlist.
"""
from alembic import op
import sqlalchemy as sa

revision = '0005'
down_revision = '0004'
branch_labels = None
depends_on = None


def _id():
    return sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False)


def upgrade():
    op.create_table(
        'gdelt_theme_days',
        _id(),
        sa.Column('theme', sa.String(length=64), nullable=False),
        sa.Column('day', sa.Date(), nullable=False),
        sa.Column('article_count', sa.Integer(), nullable=False),
        sa.Column('total_articles', sa.BigInteger(), nullable=True),
        sa.Column('avg_tone', sa.Numeric(8, 4), nullable=True),
        sa.Column('retrieved_at', sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint('article_count >= 0', name=op.f('ck_gdelt_theme_days_article_count')),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_gdelt_theme_days')),
        sa.UniqueConstraint('theme', 'day', name=op.f('uq_gdelt_theme_days_theme_day')),
        schema='perennial',
    )
    op.create_table(
        'gdelt_articles',
        _id(),
        sa.Column('theme', sa.String(length=64), nullable=False),
        sa.Column('day', sa.Date(), nullable=False),
        sa.Column('url', sa.Text(), nullable=False),
        sa.Column('title', sa.Text(), nullable=True),
        sa.Column('domain', sa.String(length=255), nullable=True),
        sa.Column('seen_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_gdelt_articles')),
        sa.UniqueConstraint('theme', 'day', 'url', name=op.f('uq_gdelt_articles_theme_day_url')),
        schema='perennial',
    )
    op.create_table(
        'gdelt_spike_checks',
        _id(),
        sa.Column('theme', sa.String(length=64), nullable=False),
        sa.Column('day', sa.Date(), nullable=False),
        sa.Column('article_count', sa.Integer(), nullable=False),
        sa.Column('baseline_median', sa.Numeric(12, 2), nullable=False),
        sa.Column('ratio', sa.Numeric(10, 2), nullable=True),
        sa.Column('is_spike', sa.Boolean(), nullable=False),
        sa.Column('checked_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_gdelt_spike_checks')),
        sa.UniqueConstraint('theme', 'day', name=op.f('uq_gdelt_spike_checks_theme_day')),
        schema='perennial',
    )


def downgrade():
    op.drop_table('gdelt_spike_checks', schema='perennial')
    op.drop_table('gdelt_articles', schema='perennial')
    op.drop_table('gdelt_theme_days', schema='perennial')
