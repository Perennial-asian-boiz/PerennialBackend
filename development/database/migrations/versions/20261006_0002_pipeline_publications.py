"""Durable run lifecycle, lineage and explicit reproducible publications.

Existing batches and successful/failed run history remain intact. Legacy runs
have unknown coverage/collection time and require a fresh live collection before
production publication. This migration deliberately does not invent provenance.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '0002'
down_revision = '0001'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('ingestion_runs', sa.Column('heartbeat_at', sa.DateTime(timezone=True), nullable=True), schema='perennial')
    op.add_column('ingestion_runs', sa.Column('collected_at', sa.DateTime(timezone=True), nullable=True), schema='perennial')
    op.add_column('ingestion_runs', sa.Column('coverage', postgresql.JSONB(astext_type=sa.Text()), nullable=True), schema='perennial')
    op.alter_column('ingestion_runs', 'finished_at',
               existing_type=postgresql.TIMESTAMP(timezone=True),
               nullable=True,
               schema='perennial')
    op.create_unique_constraint(op.f('uq_ingestion_runs_id_batch_id_source'), 'ingestion_runs', ['id', 'batch_id', 'source'], schema='perennial')
    op.drop_constraint(op.f('ck_ingestion_runs_ck_ingestion_runs_status'), 'ingestion_runs', schema='perennial', type_='check')
    op.drop_constraint(op.f('ck_ingestion_runs_ck_ingestion_runs_status_consistency'), 'ingestion_runs', schema='perennial', type_='check')
    op.create_check_constraint(op.f('ck_ingestion_runs_status'), 'ingestion_runs', "status IN ('running','succeeded','failed','abandoned')", schema='perennial')
    op.create_check_constraint(op.f('ck_ingestion_runs_status_consistency'), 'ingestion_runs',
        "(status = 'succeeded' AND batch_id IS NOT NULL AND reused_batch IS NOT NULL AND error_code IS NULL AND finished_at IS NOT NULL) OR "
        "(status IN ('failed','abandoned') AND batch_id IS NULL AND error_code IS NOT NULL AND finished_at IS NOT NULL) OR "
        "(status = 'running' AND batch_id IS NULL AND error_code IS NULL AND finished_at IS NULL)", schema='perennial')
    op.execute("UPDATE perennial.ingestion_runs SET heartbeat_at = finished_at")
    op.create_table('publications',
    sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('ranking_version', sa.String(length=32), nullable=False),
    sa.Column('ranking_config', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('output', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_publications')),
    schema='perennial'
    )
    op.create_table('current_publication',
    sa.Column('channel', sa.String(length=32), nullable=False),
    sa.Column('publication_id', sa.BigInteger(), nullable=False),
    sa.Column('promoted_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['publication_id'], ['perennial.publications.id'], name=op.f('fk_current_publication_publication_id_publications')),
    sa.PrimaryKeyConstraint('channel', name=op.f('pk_current_publication')),
    schema='perennial'
    )
    op.create_table('market_cap_observations',
    sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
    sa.Column('security_id', sa.BigInteger(), nullable=False),
    sa.Column('value', sa.Numeric(precision=24, scale=2), nullable=True),
    sa.Column('currency', sa.String(length=3), nullable=False),
    sa.Column('provider', sa.String(length=32), nullable=False),
    sa.Column('observed_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('retrieved_at', sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("currency ~ '^[A-Z]{3}$'", name=op.f('ck_market_cap_observations_currency')),
    sa.CheckConstraint('value IS NULL OR value > 0', name=op.f('ck_market_cap_observations_positive_value')),
    sa.ForeignKeyConstraint(['security_id'], ['perennial.securities.id'], name=op.f('fk_market_cap_observations_security_id_securities')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_market_cap_observations')),
    sa.UniqueConstraint('id', 'security_id', name=op.f('uq_market_cap_observations_id_security_id')),
    schema='perennial'
    )
    op.create_index('ix_market_cap_observations_security_time', 'market_cap_observations', ['security_id', sa.literal_column('retrieved_at DESC')], unique=False, schema='perennial')
    op.create_table('publication_market_caps',
    sa.Column('publication_id', sa.BigInteger(), nullable=False),
    sa.Column('security_id', sa.BigInteger(), nullable=False),
    sa.Column('observation_id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['observation_id', 'security_id'], ['perennial.market_cap_observations.id', 'perennial.market_cap_observations.security_id'], name=op.f('fk_publication_market_caps_observation_id_market_cap_observations')),
    sa.ForeignKeyConstraint(['publication_id'], ['perennial.publications.id'], name=op.f('fk_publication_market_caps_publication_id_publications')),
    sa.PrimaryKeyConstraint('publication_id', 'security_id', name=op.f('pk_publication_market_caps')),
    schema='perennial'
    )
    op.create_table('publication_sources',
    sa.Column('publication_id', sa.BigInteger(), nullable=False),
    sa.Column('source', sa.String(length=32), nullable=False),
    sa.Column('run_id', sa.BigInteger(), nullable=False),
    sa.Column('batch_id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['publication_id'], ['perennial.publications.id'], name=op.f('fk_publication_sources_publication_id_publications')),
    sa.ForeignKeyConstraint(['run_id', 'batch_id', 'source'], ['perennial.ingestion_runs.id', 'perennial.ingestion_runs.batch_id', 'perennial.ingestion_runs.source'], name=op.f('fk_publication_sources_run_id_ingestion_runs')),
    sa.PrimaryKeyConstraint('publication_id', 'source', name=op.f('pk_publication_sources')),
    schema='perennial'
    )
    op.create_table('run_dependencies',
    sa.Column('run_id', sa.BigInteger(), nullable=False),
    sa.Column('source', sa.String(length=32), nullable=False),
    sa.Column('batch_id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['batch_id', 'source'], ['perennial.source_batches.id', 'perennial.source_batches.source'], name=op.f('fk_run_dependencies_batch_id_source_batches')),
    sa.ForeignKeyConstraint(['run_id'], ['perennial.ingestion_runs.id'], name=op.f('fk_run_dependencies_run_id_ingestion_runs')),
    sa.PrimaryKeyConstraint('run_id', 'source', name=op.f('pk_run_dependencies')),
    schema='perennial'
    )


def downgrade():
    if op.get_bind().execute(sa.text("SELECT EXISTS (SELECT 1 FROM perennial.ingestion_runs WHERE status = 'running')")).scalar():
        raise RuntimeError("Finish or abandon active runs before downgrade")
    op.drop_constraint(op.f('ck_ingestion_runs_status'), 'ingestion_runs', schema='perennial', type_='check')
    op.drop_constraint(op.f('ck_ingestion_runs_status_consistency'), 'ingestion_runs', schema='perennial', type_='check')
    op.execute("UPDATE perennial.ingestion_runs SET status = 'failed' WHERE status = 'abandoned'")
    op.create_check_constraint(op.f('ck_ingestion_runs_ck_ingestion_runs_status'), 'ingestion_runs', "status IN ('succeeded','failed')", schema='perennial')
    op.create_check_constraint(op.f('ck_ingestion_runs_ck_ingestion_runs_status_consistency'), 'ingestion_runs',
        "(status = 'succeeded' AND batch_id IS NOT NULL AND reused_batch IS NOT NULL AND error_code IS NULL) OR "
        "(status = 'failed' AND batch_id IS NULL AND error_code IS NOT NULL)", schema='perennial')
    op.drop_table('run_dependencies', schema='perennial')
    op.drop_table('publication_sources', schema='perennial')
    op.drop_table('publication_market_caps', schema='perennial')
    op.drop_index('ix_market_cap_observations_security_time', table_name='market_cap_observations', schema='perennial')
    op.drop_table('market_cap_observations', schema='perennial')
    op.drop_table('current_publication', schema='perennial')
    op.drop_table('publications', schema='perennial')
    op.drop_constraint(op.f('uq_ingestion_runs_id_batch_id_source'), 'ingestion_runs', schema='perennial', type_='unique')
    op.alter_column('ingestion_runs', 'finished_at',
               existing_type=postgresql.TIMESTAMP(timezone=True),
               nullable=False,
               schema='perennial')
    op.drop_column('ingestion_runs', 'coverage', schema='perennial')
    op.drop_column('ingestion_runs', 'collected_at', schema='perennial')
    op.drop_column('ingestion_runs', 'heartbeat_at', schema='perennial')
