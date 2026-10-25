"""S1-12: the stored company summary on company_profiles.

Expand only (docs/work-items/s1-stories-plan.md, S1-12 "Data"). Seven nullable
columns: the excerpt ingestion selects (summary, its grade and status), its
source's name and https link, when it was taken, and description_digest, the
SHA-256 of the description as fetched. The raw description is never stored.
There is no summary_method: Bryan declined the model rewrite (Q9).

The URL check makes the database refuse a summary_source_url that isn't an
https link with a host, so the API never serves another scheme (CS-5).

Numbered 0012, after market caps' 0011. The #22 stack's app migrations also
claim 0011 and 0012 and are renumbered when that stack reaches main (RB-2a).
Downgrade refuses while any summary is stored: it would drop ingestion's
results and their digests.
"""
from alembic import op
import sqlalchemy as sa

revision = '0012'
down_revision = '0011'
branch_labels = None
depends_on = None

TABLE = 'company_profiles'
COLUMNS = (
    'summary', 'summary_grade', 'summary_status', 'summary_source_name',
    'summary_source_url', 'summary_as_of', 'description_digest',
)
CHECKS = {
    'summary_not_blank': "summary IS NULL OR (btrim(summary) <> '' AND char_length(summary) <= 4000)",
    'summary_status': "summary_status IS NULL OR summary_status IN ('ok', 'unavailable')",
    'summary_source_name_not_blank': (
        "summary_source_name IS NULL OR btrim(summary_source_name) <> ''"),
    'summary_source_url_https': (
        "summary_source_url IS NULL OR (char_length(summary_source_url) <= 2048 AND summary_source_url"
        " ~ '^https://[A-Za-z0-9.-]+(:[0-9]{1,5})?([/?#][^[:space:]]*)?$')"),
    'description_digest_sha256': "description_digest IS NULL OR description_digest ~ '^[0-9a-f]{64}$'",
}


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.add_column(TABLE, sa.Column('summary', sa.Text(), nullable=True), schema='perennial')
    op.add_column(TABLE, sa.Column('summary_grade', sa.Numeric(5, 2), nullable=True), schema='perennial')
    op.add_column(TABLE, sa.Column('summary_status', sa.String(length=16), nullable=True), schema='perennial')
    op.add_column(TABLE, sa.Column('summary_source_name', sa.String(length=128), nullable=True),
                  schema='perennial')
    op.add_column(TABLE, sa.Column('summary_source_url', sa.Text(), nullable=True), schema='perennial')
    op.add_column(TABLE, sa.Column('summary_as_of', sa.DateTime(timezone=True), nullable=True),
                  schema='perennial')
    op.add_column(TABLE, sa.Column('description_digest', sa.String(length=64), nullable=True),
                  schema='perennial')
    for name, condition in CHECKS.items():
        op.create_check_constraint(op.f(f'ck_{TABLE}_{name}'), TABLE, condition, schema='perennial')


def downgrade():
    # Refuse before any change: stored summaries and digests would be lost.
    if op.get_bind().execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM perennial.company_profiles WHERE "
        + " OR ".join(f"{c} IS NOT NULL" for c in COLUMNS) + ")"
    )).scalar():
        raise RuntimeError("company summaries are stored; downgrade would drop them")
    op.execute("SET LOCAL lock_timeout = '5s'")
    for name in CHECKS:
        op.drop_constraint(op.f(f'ck_{TABLE}_{name}'), TABLE, schema='perennial', type_='check')
    for column in reversed(COLUMNS):
        op.drop_column(TABLE, column, schema='perennial')
