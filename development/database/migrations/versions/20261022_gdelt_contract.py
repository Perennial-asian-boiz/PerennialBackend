"""GDELT through the ingestion contract: volume, article and spike-result batches.

Expand only (design note docs/work-items/gdelt-contract-design-note.md, PR 2).
Widens the source and collection_mode vocabularies and adds three immutable
batch tables. The 0005 tables (gdelt_theme_days, gdelt_articles,
gdelt_spike_checks) are left untouched; nothing writes them after this change and
a later contract migration drops them. Downgrade refuses while any run or batch
of the new sources exists, so it can never orphan lineage.

The revision id is a placeholder; the merge assigns the next number (0009 after
M2's 0007 and M4's 0008) and re-chains down_revision.
"""
from alembic import op
import sqlalchemy as sa

revision = 'gdelt_contract_v1'
down_revision = '0007'
branch_labels = None
depends_on = None

OLD_SOURCES = ('congress_trades', 'ark_holdings', 'insider_trades', 'short_interest')
NEW_SOURCES = OLD_SOURCES + ('gdelt_theme_volume', 'gdelt_theme_articles', 'gdelt_spike_results')
OLD_MODES = ('fixture', 'file', 'live')
NEW_MODES = OLD_MODES + ('derived',)
THEME = "theme ~ '^[A-Z0-9_]{1,64}$'"


def _in(column, values):
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def _vocabulary(sources, modes):
    for table in ('source_batches', 'ingestion_runs'):
        op.drop_constraint(op.f(f'ck_{table}_source'), table, schema='perennial', type_='check')
        op.create_check_constraint(op.f(f'ck_{table}_source'), table, _in('source', sources), schema='perennial')
    op.drop_constraint(op.f('ck_ingestion_runs_collection_mode'), 'ingestion_runs', schema='perennial', type_='check')
    op.create_check_constraint(op.f('ck_ingestion_runs_collection_mode'), 'ingestion_runs',
                               _in('collection_mode', modes), schema='perennial')


def _common(table):
    return [
        sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column('batch_id', sa.BigInteger(), nullable=False),
        sa.Column('row_number', sa.Integer(), nullable=False),
        sa.Column('theme', sa.String(length=64), nullable=False),
        sa.Column('day', sa.Date(), nullable=False),
    ], [
        sa.ForeignKeyConstraint(['batch_id'], ['perennial.source_batches.id'], ondelete='CASCADE',
                                name=op.f(f'fk_{table}_batch_id_source_batches')),
        sa.PrimaryKeyConstraint('id', name=op.f(f'pk_{table}')),
        sa.UniqueConstraint('batch_id', 'row_number', name=op.f(f'uq_{table}_batch_id_row_number')),
        sa.CheckConstraint('row_number >= 1', name=op.f(f'ck_{table}_row_number')),
        sa.CheckConstraint(THEME, name=op.f(f'ck_{table}_theme_format')),
    ]


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    _vocabulary(NEW_SOURCES, NEW_MODES)

    columns, constraints = _common('gdelt_theme_volume')
    op.create_table(
        'gdelt_theme_volume', *columns,
        sa.Column('article_count', sa.Integer(), nullable=False),
        sa.Column('total_articles', sa.BigInteger(), nullable=True),
        sa.Column('avg_tone', sa.Numeric(8, 4), nullable=True),
        *constraints,
        sa.UniqueConstraint('batch_id', 'theme', 'day', name=op.f('uq_gdelt_theme_volume_batch_id_theme_day')),
        sa.CheckConstraint('article_count >= 0', name=op.f('ck_gdelt_theme_volume_article_count')),
        sa.CheckConstraint('total_articles IS NULL OR total_articles >= 0', name=op.f('ck_gdelt_theme_volume_total_articles')),
        schema='perennial',
    )
    columns, constraints = _common('gdelt_theme_articles')
    op.create_table(
        'gdelt_theme_articles', *columns,
        sa.Column('url', sa.String(length=2048), nullable=False),
        sa.Column('title', sa.String(length=500), nullable=True),
        sa.Column('domain', sa.String(length=255), nullable=True),
        sa.Column('seen_at', sa.DateTime(timezone=True), nullable=True),
        *constraints,
        sa.UniqueConstraint('batch_id', 'theme', 'url', name=op.f('uq_gdelt_theme_articles_batch_id_theme_url')),
        sa.CheckConstraint("url ~ '^https?://'", name=op.f('ck_gdelt_theme_articles_url_scheme')),
        schema='perennial',
    )
    columns, constraints = _common('gdelt_spike_results')
    op.create_table(
        'gdelt_spike_results', *columns,
        sa.Column('article_count', sa.Integer(), nullable=False),
        sa.Column('baseline_median', sa.Numeric(12, 2), nullable=True),
        sa.Column('observed_baseline_days', sa.SmallInteger(), nullable=False),
        sa.Column('ratio', sa.Numeric(10, 2), nullable=True),
        sa.Column('is_spike', sa.Boolean(), nullable=True),
        sa.Column('rule_version', sa.String(length=16), nullable=False),
        *constraints,
        sa.UniqueConstraint('batch_id', 'theme', 'day', name=op.f('uq_gdelt_spike_results_batch_id_theme_day')),
        sa.CheckConstraint('article_count >= 0', name=op.f('ck_gdelt_spike_results_article_count')),
        sa.CheckConstraint('observed_baseline_days >= 0', name=op.f('ck_gdelt_spike_results_observed_baseline_days')),
        schema='perennial',
    )


def downgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    added = tuple(s for s in NEW_SOURCES if s not in OLD_SOURCES)
    op.execute(sa.text(f"""
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM perennial.ingestion_runs WHERE {_in('source', added)}
                                                            OR collection_mode = 'derived')
             OR EXISTS (SELECT 1 FROM perennial.source_batches WHERE {_in('source', added)}) THEN
            RAISE EXCEPTION 'GDELT contract runs or batches exist; downgrade would orphan lineage';
          END IF;
        END $$;
    """))
    for table in ('gdelt_spike_results', 'gdelt_theme_articles', 'gdelt_theme_volume'):
        op.drop_table(table, schema='perennial')
    _vocabulary(OLD_SOURCES, OLD_MODES)
