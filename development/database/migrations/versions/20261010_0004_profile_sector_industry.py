"""Store sector and industry from the FMP profile alongside each market-cap observation.

Both columns are nullable: existing observations predate the lookup, and FMP
omits classification for some securities (funds, recent listings).
"""
from alembic import op
import sqlalchemy as sa

revision = '0004'
down_revision = '0003'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('market_cap_observations', sa.Column('sector', sa.String(length=64), nullable=True), schema='perennial')
    op.add_column('market_cap_observations', sa.Column('industry', sa.String(length=128), nullable=True), schema='perennial')


def downgrade():
    op.drop_column('market_cap_observations', 'industry', schema='perennial')
    op.drop_column('market_cap_observations', 'sector', schema='perennial')
