"""Sprint 2 (S2-01..03): the sentiment contract, company scores, explanations and their pointer.

Expand only (output/sprint-2/s2-stories-plan.md §3.1, §4). Adds 'ticker_sentiment',
'company_scores' and 'score_explanations' to the source vocabulary, and four tables:

* ticker_sentiment: the credibility-weighted sentiment per (batch, security), PR #85's
  table plus model_version (decision 108). Nothing counted means a NULL value.
* company_scores: one row per (batch, security) under score-v1: the score or why it
  is unavailable, the missing components, and per component (fundamentals,
  sentiment, event_exposure) its value, nominal and effective weight, contribution,
  input as-of, input version and record id (no FK: the record's table follows from
  the component).
* score_explanations: one row per (batch, security), naming the company_scores row
  it explains through (company_score_id, security_id), so never another security's
  score, with at most 3 claims.
* current_scores: (channel, batch_id), the scores batch the API serves, like
  current_publication.

Downgrade refuses while any run or batch of the three sources exists. Numbered
0018, the next free id after 0017; renumber at merge if another lands first.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '0018'
down_revision = '0017'
branch_labels = None
depends_on = None

OLD_SOURCES = (
    'congress_trades', 'ark_holdings', 'insider_trades', 'short_interest',
    'gdelt_theme_volume', 'gdelt_theme_articles', 'gdelt_spike_results',
    'market_caps', 'fundamentals', 'fundamental_scores', 'news_articles',
)
SCORE_SOURCES = ('ticker_sentiment', 'company_scores', 'score_explanations')
NEW_SOURCES = OLD_SOURCES + SCORE_SOURCES
COMPONENTS = ('fundamentals', 'sentiment', 'event_exposure')
UNAVAILABLE_REASONS = ('no_fundamentals', 'only_fundamentals')
EXPLANATION_STATUSES = ('ok', 'not_recommended', 'unavailable')
EXPLANATION_REASONS = ('insufficient_supporting_data', 'score_unavailable')


def _in(column, values):
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def _vocabulary(sources):
    for table in ('source_batches', 'ingestion_runs'):
        op.drop_constraint(op.f(f'ck_{table}_source'), table, schema='perennial', type_='check')
        op.create_check_constraint(op.f(f'ck_{table}_source'), table, _in('source', sources), schema='perennial')


def _batch_and_security(table):
    return [
        sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column('batch_id', sa.BigInteger(), nullable=False),
        sa.Column('security_id', sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(['batch_id'], ['perennial.source_batches.id'], ondelete='CASCADE',
                                name=op.f(f'fk_{table}_batch_id_source_batches')),
        sa.ForeignKeyConstraint(['security_id'], ['perennial.securities.id'],
                                name=op.f(f'fk_{table}_security_id_securities')),
        sa.PrimaryKeyConstraint('id', name=op.f(f'pk_{table}')),
        sa.UniqueConstraint('batch_id', 'security_id', name=op.f(f'uq_{table}_batch_id_security_id')),
    ]


def _component(name):
    return [
        sa.Column(f'{name}_value', sa.Numeric(4, 1), nullable=True),
        sa.Column(f'{name}_nominal_weight', sa.Numeric(5, 4), nullable=False),
        sa.Column(f'{name}_weight', sa.Numeric(5, 4), nullable=True),
        sa.Column(f'{name}_contribution', sa.Numeric(5, 2), nullable=True),
        sa.Column(f'{name}_input_as_of', sa.Date(), nullable=True),
        sa.Column(f'{name}_input_version', sa.String(length=32), nullable=True),
        sa.Column(f'{name}_record_id', sa.BigInteger(), nullable=True),
        sa.CheckConstraint(f'({name}_value IS NULL OR {name}_value BETWEEN 0 AND 100)'
                           f' AND {name}_nominal_weight BETWEEN 0 AND 1'
                           f' AND ({name}_weight IS NULL OR {name}_weight BETWEEN 0 AND 1)'
                           f' AND ({name}_contribution IS NULL OR {name}_contribution BETWEEN 0 AND 100)',
                           name=op.f(f'ck_company_scores_{name}')),
        # A component is listed as missing exactly when it has no value.
        sa.CheckConstraint(f"({name}_value IS NULL) = ('{name}' = ANY (missing))",
                           name=op.f(f'ck_company_scores_{name}_missing')),
    ]


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    _vocabulary(NEW_SOURCES)
    op.create_table(
        'ticker_sentiment',
        *_batch_and_security('ticker_sentiment'),
        sa.Column('row_number', sa.Integer(), nullable=False),
        sa.Column('value', sa.Numeric(5, 4), nullable=True),
        sa.Column('plain_average', sa.Numeric(5, 4), nullable=True),
        sa.Column('article_count', sa.Integer(), nullable=False),
        sa.Column('news_count', sa.Integer(), nullable=False),
        sa.Column('social_count', sa.Integer(), nullable=False),
        sa.Column('first_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('weights_version', sa.String(length=32), nullable=False),
        sa.Column('model_version', sa.String(length=64), nullable=True),
        sa.CheckConstraint('row_number >= 1', name=op.f('ck_ticker_sentiment_row_number')),
        sa.CheckConstraint('news_count >= 0 AND social_count >= 0 '
                           'AND article_count = news_count + social_count',
                           name=op.f('ck_ticker_sentiment_counts')),
        sa.CheckConstraint('(article_count = 0) = (value IS NULL) '
                           'AND (value IS NULL) = (plain_average IS NULL) '
                           'AND (value IS NULL) = (first_at IS NULL) '
                           'AND (first_at IS NULL) = (last_at IS NULL)',
                           name=op.f('ck_ticker_sentiment_scored_or_empty')),
        sa.CheckConstraint('value IS NULL OR (value BETWEEN -1 AND 1 AND plain_average BETWEEN -1 AND 1 '
                           'AND first_at <= last_at)',
                           name=op.f('ck_ticker_sentiment_value_range')),
        sa.CheckConstraint('(news_count = 0) = (model_version IS NULL)',
                           name=op.f('ck_ticker_sentiment_model_version')),
        sa.UniqueConstraint('batch_id', 'row_number', name=op.f('uq_ticker_sentiment_batch_id_row_number')),
        schema='perennial',
    )
    op.create_index('ix_ticker_sentiment_security_id', 'ticker_sentiment', ['security_id'], unique=False,
                    schema='perennial')
    op.create_table(
        'company_scores',
        *_batch_and_security('company_scores'),
        sa.Column('status', sa.String(length=16), nullable=False),
        sa.Column('unavailable_reason', sa.String(length=32), nullable=True),
        sa.Column('score', sa.Numeric(4, 1), nullable=True),
        sa.Column('score_version', sa.String(length=32), nullable=False),
        sa.Column('missing', postgresql.ARRAY(sa.String(length=32)), nullable=False),
        sa.Column('recommended', sa.Boolean(), nullable=False),
        sa.Column('watchlist_publication_id', sa.BigInteger(), nullable=True),
        *(part for name in COMPONENTS for part in _component(name)),
        sa.CheckConstraint("status IN ('ok', 'unavailable')", name=op.f('ck_company_scores_status')),
        sa.CheckConstraint('score IS NULL OR score BETWEEN 0 AND 100', name=op.f('ck_company_scores_score')),
        sa.CheckConstraint("(status = 'ok') = (score IS NOT NULL)", name=op.f('ck_company_scores_status_score')),
        sa.CheckConstraint("(status = 'unavailable') = (unavailable_reason IS NOT NULL)",
                           name=op.f('ck_company_scores_status_reason')),
        sa.CheckConstraint('unavailable_reason IS NULL OR ' + _in('unavailable_reason', UNAVAILABLE_REASONS),
                           name=op.f('ck_company_scores_unavailable_reason')),
        sa.CheckConstraint('missing <@ ARRAY[' + ', '.join(repr(c) for c in COMPONENTS) + ']::varchar[]',
                           name=op.f('ck_company_scores_missing_components')),
        sa.CheckConstraint("NOT recommended OR (status = 'ok' AND score >= 60 AND watchlist_publication_id IS NOT NULL)",
                           name=op.f('ck_company_scores_recommended')),
        sa.ForeignKeyConstraint(['watchlist_publication_id'], ['perennial.publications.id'],
                                name=op.f('fk_company_scores_watchlist_publication_id_publications')),
        # The target of score_explanations' composite FK (checker F1).
        sa.UniqueConstraint('id', 'security_id', name=op.f('uq_company_scores_id_security_id')),
        schema='perennial',
    )
    op.create_index('ix_company_scores_security_id', 'company_scores', ['security_id'], unique=False,
                    schema='perennial')
    op.create_table(
        'score_explanations',
        *_batch_and_security('score_explanations'),
        sa.Column('company_score_id', sa.BigInteger(), nullable=False),
        sa.Column('status', sa.String(length=16), nullable=False),
        sa.Column('reason', sa.String(length=32), nullable=True),
        sa.Column('template_version', sa.String(length=32), nullable=False),
        sa.Column('claims', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('input_digest', sa.String(length=64), nullable=False),
        sa.CheckConstraint(_in('status', EXPLANATION_STATUSES), name=op.f('ck_score_explanations_status')),
        sa.CheckConstraint("(status = 'unavailable') = (reason IS NOT NULL)",
                           name=op.f('ck_score_explanations_status_reason')),
        sa.CheckConstraint('reason IS NULL OR ' + _in('reason', EXPLANATION_REASONS),
                           name=op.f('ck_score_explanations_reason')),
        # CASE, not AND: jsonb_array_length raises on a non-array instead of failing the check.
        sa.CheckConstraint("CASE WHEN jsonb_typeof(claims) = 'array' THEN jsonb_array_length(claims) <= 3"
                           " ELSE false END", name=op.f('ck_score_explanations_claims')),
        sa.CheckConstraint("(status = 'ok') = (claims <> '[]'::jsonb)",
                           name=op.f('ck_score_explanations_status_claims')),
        sa.CheckConstraint("input_digest ~ '^[0-9a-f]{64}$'", name=op.f('ck_score_explanations_input_digest')),
        # Composite (checker F1): an explanation can only name a score of its own security.
        sa.ForeignKeyConstraint(['company_score_id', 'security_id'],
                                ['perennial.company_scores.id', 'perennial.company_scores.security_id'],
                                name=op.f('fk_score_explanations_company_score_id_company_scores')),
        schema='perennial',
    )
    op.create_index('ix_score_explanations_security_id', 'score_explanations', ['security_id'], unique=False,
                    schema='perennial')
    op.create_index('ix_score_explanations_company_score_id', 'score_explanations', ['company_score_id'],
                    unique=False, schema='perennial')
    op.create_table(
        'current_scores',
        sa.Column('channel', sa.String(length=32), nullable=False),
        sa.Column('batch_id', sa.BigInteger(), nullable=False),
        sa.Column('promoted_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['batch_id'], ['perennial.source_batches.id'],
                                name=op.f('fk_current_scores_batch_id_source_batches')),
        sa.PrimaryKeyConstraint('channel', name=op.f('pk_current_scores')),
        schema='perennial',
    )


def downgrade():
    # Refuse before any change: score runs or batches would be orphaned. A
    # current_scores row always names a batch, so it is covered too.
    if op.get_bind().execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM perennial.ingestion_runs WHERE source = ANY (:s))"
        " OR EXISTS (SELECT 1 FROM perennial.source_batches WHERE source = ANY (:s))"
    ), {'s': list(SCORE_SOURCES)}).scalar():
        raise RuntimeError("score state exists; downgrade would lose lineage")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_table('current_scores', schema='perennial')
    op.drop_index('ix_score_explanations_company_score_id', table_name='score_explanations', schema='perennial')
    op.drop_index('ix_score_explanations_security_id', table_name='score_explanations', schema='perennial')
    op.drop_table('score_explanations', schema='perennial')
    op.drop_index('ix_company_scores_security_id', table_name='company_scores', schema='perennial')
    op.drop_table('company_scores', schema='perennial')
    op.drop_index('ix_ticker_sentiment_security_id', table_name='ticker_sentiment', schema='perennial')
    op.drop_table('ticker_sentiment', schema='perennial')
    _vocabulary(OLD_SOURCES)
