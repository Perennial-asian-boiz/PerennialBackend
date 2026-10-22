"""Accumulated Congress history, gap markers and publication watermarks.

Expand only. The free FMP plan serves only the latest 20 disclosures per
chamber, so live Congress batches are folded into an append-only history that
publications pin by watermark (docs/work-items/congress-incremental-design-note.md).
Publications made before this revision have no watermark and replay from their
pinned batch. Downgrade drops the history; observation batches are untouched,
so it can be re-derived from them.

Placeholder revision id: renumber and re-chain at merge per roadmap §4.
"""
from alembic import op
import sqlalchemy as sa

revision = 'congress_history_v1'
down_revision = '0007'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.create_table('congress_trade_history',
    sa.Column('history_id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
    sa.Column('trade_key', sa.String(length=64), nullable=False),
    sa.Column('occurrence', sa.Integer(), nullable=False),
    sa.Column('security_id', sa.BigInteger(), nullable=False),
    sa.Column('politician_name', sa.String(length=200), nullable=False),
    sa.Column('chamber', sa.String(length=16), nullable=True),
    sa.Column('trade_type', sa.String(length=100), nullable=False),
    sa.Column('transaction_date', sa.Date(), nullable=False),
    sa.Column('disclosure_date', sa.Date(), nullable=True),
    sa.Column('amount_label', sa.String(length=100), nullable=True),
    sa.Column('asset_description', sa.String(length=500), nullable=True),
    sa.Column('asset_type', sa.String(length=100), nullable=True),
    sa.Column('district', sa.String(length=20), nullable=True),
    sa.Column('source_link', sa.String(length=2048), nullable=True),
    sa.Column('data_source', sa.String(length=32), nullable=True),
    sa.Column('first_seen_batch_id', sa.BigInteger(), nullable=False),
    sa.CheckConstraint("trade_key ~ '^[0-9a-f]{64}$'", name=op.f('ck_congress_trade_history_trade_key_format')),
    sa.CheckConstraint('occurrence >= 1', name=op.f('ck_congress_trade_history_occurrence')),
    sa.ForeignKeyConstraint(['security_id'], ['perennial.securities.id'], name=op.f('fk_congress_trade_history_security_id_securities')),
    sa.ForeignKeyConstraint(['first_seen_batch_id'], ['perennial.source_batches.id'], name=op.f('fk_congress_trade_history_first_seen_batch_id_source_batches')),
    sa.PrimaryKeyConstraint('history_id', name=op.f('pk_congress_trade_history')),
    sa.UniqueConstraint('trade_key', 'occurrence', name=op.f('uq_congress_trade_history_trade_key_occurrence')),
    schema='perennial'
    )
    op.create_index('ix_congress_trade_history_security_id', 'congress_trade_history', ['security_id'], schema='perennial')
    op.create_index('ix_congress_trade_history_chamber_disclosure', 'congress_trade_history', ['chamber', 'disclosure_date'], schema='perennial')
    op.create_index('ix_congress_trade_history_first_seen_batch_id', 'congress_trade_history', ['first_seen_batch_id'], schema='perennial')
    op.create_table('congress_history_gaps',
    sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
    sa.Column('chamber', sa.String(length=16), nullable=False),
    sa.Column('newest_stored_date', sa.Date(), nullable=False),
    sa.Column('oldest_seen_date', sa.Date(), nullable=False),
    sa.Column('gap_days', sa.Integer(), nullable=False),
    sa.Column('detected_batch_id', sa.BigInteger(), nullable=False),
    sa.Column('detected_run_id', sa.BigInteger(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint('gap_days >= 0', name=op.f('ck_congress_history_gaps_gap_days')),
    sa.ForeignKeyConstraint(['detected_batch_id'], ['perennial.source_batches.id'], name=op.f('fk_congress_history_gaps_detected_batch_id_source_batches')),
    sa.ForeignKeyConstraint(['detected_run_id'], ['perennial.ingestion_runs.id'], name=op.f('fk_congress_history_gaps_detected_run_id_ingestion_runs')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_congress_history_gaps')),
    sa.UniqueConstraint('detected_batch_id', 'chamber', name=op.f('uq_congress_history_gaps_detected_batch_id_chamber')),
    schema='perennial'
    )
    op.create_index('ix_congress_history_gaps_detected_run_id', 'congress_history_gaps', ['detected_run_id'], schema='perennial')
    op.create_table('publication_history_watermarks',
    sa.Column('publication_id', sa.BigInteger(), nullable=False),
    sa.Column('source', sa.String(length=32), nullable=False),
    sa.Column('through_history_id', sa.BigInteger(), nullable=False),
    sa.CheckConstraint("source = 'congress_trades'", name=op.f('ck_publication_history_watermarks_source')),
    sa.CheckConstraint('through_history_id >= 0', name=op.f('ck_publication_history_watermarks_through_history_id')),
    sa.ForeignKeyConstraint(['publication_id'], ['perennial.publications.id'], name=op.f('fk_publication_history_watermarks_publication_id_publications')),
    sa.PrimaryKeyConstraint('publication_id', 'source', name=op.f('pk_publication_history_watermarks')),
    schema='perennial'
    )


def downgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_table('publication_history_watermarks', schema='perennial')
    op.drop_index('ix_congress_history_gaps_detected_run_id', table_name='congress_history_gaps', schema='perennial')
    op.drop_table('congress_history_gaps', schema='perennial')
    op.drop_index('ix_congress_trade_history_first_seen_batch_id', table_name='congress_trade_history', schema='perennial')
    op.drop_index('ix_congress_trade_history_chamber_disclosure', table_name='congress_trade_history', schema='perennial')
    op.drop_index('ix_congress_trade_history_security_id', table_name='congress_trade_history', schema='perennial')
    op.drop_table('congress_trade_history', schema='perennial')
