"""At most one degraded marker per publication (M2, design note Q5).

Expand only: a partial unique index on publication_events. M1-A (0006) allowed
repeated degraded rows; the M2 publisher writes exactly one, and this makes a
second impossible. No existing rows can conflict before M2 starts writing them.
"""
from alembic import op
import sqlalchemy as sa

revision = '0007'
down_revision = '0006'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.create_index('uq_publication_events_degraded', 'publication_events', ['publication_id'], unique=True,
                    schema='perennial', postgresql_where=sa.text("kind = 'degraded'"))


def downgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_index('uq_publication_events_degraded', table_name='publication_events', schema='perennial')
