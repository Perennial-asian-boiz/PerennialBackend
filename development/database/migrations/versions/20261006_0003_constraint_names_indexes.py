"""Normalize v1 CHECK names without rewriting history; index lineage and health reads.

0001 applied the naming convention twice. 0002 already replaced the two run
status checks. Rename the remaining checks in place, preserving their definitions
and data. Downgrade restores exactly the v2 names, including PostgreSQL truncation.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.schema import conv

revision = '0003'
down_revision = '0002'
branch_labels = None
depends_on = None

CHECKS = {
    'securities': ('symbol_format', 'exchange_upper', 'currency_format'),
    'source_batches': ('source', 'content_hash_format', 'record_count'),
    'ingestion_runs': ('source', 'collection_mode', 'error_summary_length',
                       'diagnostics_size', 'finished_after_started'),
    'congress_trades': ('row_number',),
    'ark_holdings': ('fund_count', 'total_weight', 'share_price'),
    'insider_trades': ('row_number',),
    'short_interest': ('shares', 'volume', 'days_to_cover'),
}
INDEXES = (
    ('ix_publication_sources_run_batch_source', 'publication_sources', ['run_id', 'batch_id', 'source']),
    ('ix_run_dependencies_batch_source', 'run_dependencies', ['batch_id', 'source']),
    ('ix_publication_market_caps_observation_security', 'publication_market_caps', ['observation_id', 'security_id']),
    ('ix_ingestion_runs_source_started', 'ingestion_runs', ['source', sa.literal_column('started_at DESC'), sa.literal_column('id DESC')]),
)


def _rename(reverse=False):
    preparer = op.get_bind().dialect.identifier_preparer
    for table, suffixes in CHECKS.items():
        for suffix in suffixes:
            canonical = f'ck_{table}_{suffix}'
            legacy = preparer.truncate_and_render_constraint_name(
                conv(f'ck_{table}_{canonical}'), _alembic_quote=False)
            old, new = (canonical, legacy) if reverse else (legacy, canonical)
            op.execute(sa.text(f'ALTER TABLE perennial.{preparer.quote(table)} '
                               f'RENAME CONSTRAINT {preparer.quote(old)} TO {preparer.quote(new)}'))


def upgrade():
    _rename()
    for name, table, columns in INDEXES:
        op.create_index(name, table, columns, schema='perennial')


def downgrade():
    for name, table, _ in reversed(INDEXES):
        op.drop_index(name, table_name=table, schema='perennial')
    _rename(reverse=True)
