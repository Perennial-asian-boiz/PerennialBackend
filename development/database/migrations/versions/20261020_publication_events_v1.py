"""Append-only publication audit: quality verdicts, degraded markers, rollbacks.

Expand only. Publications written before this revision keep their legacy
ranking_config.quality_audit key; they are immutable, replay ignores it, and no
backfill is attempted. Downgrade drops the table and with it any event history.

Revision IDs 0004/0005 are taken on parallel branches, hence the descriptive id.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = 'publication_events_v1'
down_revision = '0003'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.create_table('publication_events',
    sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
    sa.Column('publication_id', sa.BigInteger(), nullable=False),
    sa.Column('kind', sa.String(length=32), nullable=False),
    sa.Column('actor', sa.String(length=200), nullable=True),
    sa.Column('reason', sa.String(length=500), nullable=True),
    sa.Column('checks', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("kind IN ('quality_passed', 'quality_override', 'degraded', 'rollback')", name=op.f('ck_publication_events_kind')),
    sa.CheckConstraint("actor IS NULL OR btrim(actor) <> ''", name=op.f('ck_publication_events_actor_not_blank')),
    sa.CheckConstraint("reason IS NULL OR btrim(reason) <> ''", name=op.f('ck_publication_events_reason_not_blank')),
    sa.CheckConstraint("kind NOT IN ('quality_override', 'rollback') OR (actor IS NOT NULL AND reason IS NOT NULL)", name=op.f('ck_publication_events_decision_attributed')),
    sa.CheckConstraint("jsonb_typeof(checks) = 'object'", name=op.f('ck_publication_events_checks_object')),
    sa.ForeignKeyConstraint(['publication_id'], ['perennial.publications.id'], name=op.f('fk_publication_events_publication_id_publications')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_publication_events')),
    schema='perennial'
    )
    op.create_index('ix_publication_events_publication_id', 'publication_events', ['publication_id'], schema='perennial')
    op.create_index('uq_publication_events_quality', 'publication_events', ['publication_id'], unique=True, schema='perennial',
                    postgresql_where=sa.text("kind IN ('quality_passed', 'quality_override')"))


def downgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_index('uq_publication_events_quality', table_name='publication_events', schema='perennial')
    op.drop_index('ix_publication_events_publication_id', table_name='publication_events', schema='perennial')
    op.drop_table('publication_events', schema='perennial')
