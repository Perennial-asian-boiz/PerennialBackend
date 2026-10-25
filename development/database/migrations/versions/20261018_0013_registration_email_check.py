"""Narrow duplicate-email lookup without granting API access to auth.users."""

from alembic import op

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade():
    # A quoted PL/pgSQL body resolves auth.users only when executed. Synthetic
    # local database tests do not install or imitate the whole Supabase service.
    op.execute("""
        CREATE FUNCTION app.email_is_registered(requested_email text)
        RETURNS boolean LANGUAGE plpgsql STABLE SECURITY DEFINER
        SET search_path = ''
        AS $$ BEGIN
          RETURN EXISTS (
            SELECT 1 FROM auth.users AS u
            WHERE pg_catalog.lower(u.email) = pg_catalog.lower(pg_catalog.btrim(requested_email))
          );
        END $$
    """)
    op.execute("REVOKE ALL ON FUNCTION app.email_is_registered(text) FROM PUBLIC")


def downgrade():
    op.execute("DROP FUNCTION app.email_is_registered(text)")
