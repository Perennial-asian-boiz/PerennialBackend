"""M4 security identity: securities metadata, lookup-only aliases, issuer profiles.

Expand only. New securities columns are nullable apart from is_active, whose
constant default makes the add metadata-only. Every existing security gets its
canonical (ticker, symbol) alias here so lookup resolves the current universe
immediately; provider aliases, profiles and the sector/industry backfill are
reviewed seed data applied outside this transaction (ops/seed_identity.py).
market_cap_observations keeps its sector/industry columns until the contract step.
Downgrade drops the new tables and columns, and with them any seeded identity.

Placeholder 0008: chains from 0006 on this base; re-chain after M2's 0007 at merge.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '0008'
down_revision = '0006'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.create_table('company_profiles',
    sa.Column('id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
    sa.Column('legal_name', sa.Text(), nullable=True),
    sa.Column('sector', sa.String(length=64), nullable=True),
    sa.Column('industry', sa.String(length=128), nullable=True),
    sa.Column('alias_names', postgresql.ARRAY(sa.Text()), server_default=sa.text("'{}'"), nullable=False),
    sa.Column('cik', sa.String(length=10), nullable=True),
    sa.Column('insider_reporting_security_id', sa.BigInteger(), nullable=True),
    sa.CheckConstraint("cik IS NULL OR cik ~ '^[0-9]{10}$'", name=op.f('ck_company_profiles_cik_format')),
    sa.CheckConstraint("legal_name IS NULL OR btrim(legal_name) <> ''", name=op.f('ck_company_profiles_legal_name_not_blank')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_company_profiles')),
    sa.UniqueConstraint('cik', name=op.f('uq_company_profiles_cik')),
    schema='perennial'
    )
    op.add_column('securities', sa.Column('name', sa.Text(), nullable=True), schema='perennial')
    op.add_column('securities', sa.Column('security_type', sa.String(length=16), nullable=True), schema='perennial')
    op.add_column('securities', sa.Column('country', sa.String(length=2), nullable=True), schema='perennial')
    op.add_column('securities', sa.Column('figi', sa.String(length=12), nullable=True), schema='perennial')
    op.add_column('securities', sa.Column('is_active', sa.Boolean(), server_default=sa.text('true'), nullable=False), schema='perennial')
    op.add_column('securities', sa.Column('company_profile_id', sa.BigInteger(), nullable=True), schema='perennial')
    op.create_check_constraint(op.f('ck_securities_security_type'), 'securities',
                               "security_type IS NULL OR security_type IN ('common', 'adr', 'etf')", schema='perennial')
    op.create_check_constraint(op.f('ck_securities_country_format'), 'securities',
                               "country IS NULL OR country ~ '^[A-Z]{2}$'", schema='perennial')
    op.create_check_constraint(op.f('ck_securities_figi_format'), 'securities',
                               "figi IS NULL OR figi ~ '^[A-Z0-9]{12}$'", schema='perennial')
    op.create_unique_constraint(op.f('uq_securities_figi'), 'securities', ['figi'], schema='perennial')
    op.create_foreign_key(op.f('fk_securities_company_profile_id_company_profiles'), 'securities', 'company_profiles',
                          ['company_profile_id'], ['id'], source_schema='perennial', referent_schema='perennial')
    op.create_index('ix_securities_company_profile_id', 'securities', ['company_profile_id'], schema='perennial')
    op.create_foreign_key(op.f('fk_company_profiles_insider_reporting_security_id_securities'), 'company_profiles',
                          'securities', ['insider_reporting_security_id'], ['id'],
                          source_schema='perennial', referent_schema='perennial')
    op.create_table('security_identifiers',
    sa.Column('scheme', sa.String(length=16), nullable=False),
    sa.Column('value', sa.String(length=64), nullable=False),
    sa.Column('security_id', sa.BigInteger(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("scheme IN ('ticker', 'figi', 'cusip', 'cik')", name=op.f('ck_security_identifiers_scheme')),
    sa.CheckConstraint("value = btrim(value) AND value <> '' AND value = upper(value)", name=op.f('ck_security_identifiers_value_format')),
    sa.ForeignKeyConstraint(['security_id'], ['perennial.securities.id'], name=op.f('fk_security_identifiers_security_id_securities')),
    sa.PrimaryKeyConstraint('scheme', 'value', name=op.f('pk_security_identifiers')),
    schema='perennial'
    )
    op.create_index('ix_security_identifiers_security_id', 'security_identifiers', ['security_id'], schema='perennial')
    op.execute("INSERT INTO perennial.security_identifiers (scheme, value, security_id) "
               "SELECT 'ticker', symbol, id FROM perennial.securities")


def downgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_index('ix_security_identifiers_security_id', table_name='security_identifiers', schema='perennial')
    op.drop_table('security_identifiers', schema='perennial')
    op.drop_constraint(op.f('fk_company_profiles_insider_reporting_security_id_securities'), 'company_profiles',
                       type_='foreignkey', schema='perennial')
    op.drop_index('ix_securities_company_profile_id', table_name='securities', schema='perennial')
    op.drop_constraint(op.f('fk_securities_company_profile_id_company_profiles'), 'securities',
                       type_='foreignkey', schema='perennial')
    op.drop_constraint(op.f('uq_securities_figi'), 'securities', type_='unique', schema='perennial')
    for name in ('ck_securities_figi_format', 'ck_securities_country_format', 'ck_securities_security_type'):
        op.drop_constraint(op.f(name), 'securities', type_='check', schema='perennial')
    for column in ('company_profile_id', 'is_active', 'figi', 'country', 'security_type', 'name'):
        op.drop_column('securities', column, schema='perennial')
    op.drop_table('company_profiles', schema='perennial')
