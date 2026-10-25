"""S1-12: a summary_status of 'ok' requires every stored part of the summary.

Expand only (docs/work-items/s1-stories-plan.md, S1-12; the checker's coupling
note). The API serves an 'ok' summary with its text, source name and https link,
so the database refuses an 'ok' row missing the summary, its grade, source name,
source URL, as-of time or description digest. 'unavailable' and NULL statuses
are unchanged.

Numbered 0013, after 0012's summary columns. The #22 stack's app migrations are
renumbered after it when that stack reaches main. Downgrade only drops the
CHECK; no data is lost.
"""
from alembic import op

revision = '0013'
down_revision = '0012'
branch_labels = None
depends_on = None

TABLE = 'company_profiles'
NAME = 'summary_ok_complete'
CONDITION = (
    "summary_status IS DISTINCT FROM 'ok' OR (summary IS NOT NULL"
    " AND summary_grade IS NOT NULL AND summary_source_name IS NOT NULL"
    " AND summary_source_url IS NOT NULL AND summary_as_of IS NOT NULL"
    " AND description_digest IS NOT NULL)"
)


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.create_check_constraint(op.f(f'ck_{TABLE}_{NAME}'), TABLE, CONDITION, schema='perennial')


def downgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_constraint(op.f(f'ck_{TABLE}_{NAME}'), TABLE, schema='perennial', type_='check')
