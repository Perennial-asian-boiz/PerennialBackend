"""Supabase-owned identities with isolated application data and RLS.

Every app table has ENABLE and FORCE ROW LEVEL SECURITY and no policy here, so
a migrated but unprovisioned database denies everything. provision_api_role.py
creates the policies, naming the API role (#22 fix packet, change 6).
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


TABLES = (
    "profiles",
    "preferences",
    "notifications",
    "watchlist",
    "auth_challenges",
    "verified_sessions",
)


def stamp(name="updated_at"):
    return sa.Column(
        name,
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    )


def user_id():
    return sa.Column("user_id", sa.Uuid(), nullable=False)


def upgrade():
    # Bootstrap creates app for the owner, who holds no CREATE on the database, and
    # PostgreSQL checks that right before IF NOT EXISTS: look in the catalog instead.
    op.execute(
        "DO $$ BEGIN IF to_regnamespace('app') IS NULL"
        " THEN CREATE SCHEMA app; END IF; END $$"
    )
    op.execute("REVOKE ALL ON SCHEMA app FROM PUBLIC")
    op.create_table(
        "profiles",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("email", sa.String(320), nullable=False),
        sa.Column("full_name", sa.String(200), nullable=False),
        sa.Column(
            "onboarding_completed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        stamp(),
        sa.PrimaryKeyConstraint("id", name="pk_profiles"),
        schema="app",
    )
    op.create_table(
        "preferences",
        user_id(),
        sa.Column(
            "sectors",
            postgresql.ARRAY(sa.String(64)),
            nullable=False,
            server_default=sa.text("'{}'::text[]"),
        ),
        sa.Column("risk_level", sa.String(32)),
        stamp(),
        sa.PrimaryKeyConstraint("user_id", name="pk_preferences"),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["app.profiles.id"],
            name="fk_preferences_user_id",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "risk_level IS NULL OR risk_level IN ('Conservative', 'Moderate', 'High')",
            name=op.f("ck_preferences_risk_level"),
        ),
        schema="app",
    )
    op.create_table(
        "notifications",
        user_id(),
        sa.Column(
            "confidence_score_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column(
            "earnings_alerts_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        stamp(),
        sa.PrimaryKeyConstraint("user_id", name="pk_notifications"),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["app.profiles.id"],
            name="fk_notifications_user_id",
            ondelete="CASCADE",
        ),
        schema="app",
    )
    op.create_table(
        "watchlist",
        user_id(),
        sa.Column("security_id", sa.BigInteger(), nullable=False),
        stamp("created_at"),
        sa.PrimaryKeyConstraint("user_id", "security_id", name="pk_watchlist"),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["app.profiles.id"],
            name="fk_watchlist_user_id",
            ondelete="CASCADE",
        ),
        # A security on someone's watchlist can't be deleted (change 3).
        sa.ForeignKeyConstraint(
            ["security_id"],
            ["perennial.securities.id"],
            name="fk_watchlist_security_id",
            ondelete="RESTRICT",
        ),
        schema="app",
    )
    op.create_index(
        "ix_watchlist_security_id", "watchlist", ["security_id"], schema="app"
    )
    op.create_table(
        "auth_challenges",
        sa.Column("token_hash", sa.String(64), nullable=False),
        sa.Column("email", sa.String(320), nullable=False),
        sa.Column("flow", sa.String(16), nullable=False),
        sa.Column(
            "attempts", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "send_count", sa.Integer(), nullable=False, server_default=sa.text("1")
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True)),
        stamp("created_at"),
        stamp("last_sent_at"),
        sa.PrimaryKeyConstraint("token_hash", name="pk_auth_challenges"),
        sa.CheckConstraint(
            "flow IN ('signup', 'login', 'recovery')",
            name=op.f("ck_auth_challenges_flow"),
        ),
        sa.CheckConstraint(
            "attempts >= 0 AND send_count >= 1",
            name=op.f("ck_auth_challenges_attempts"),
        ),
        schema="app",
    )
    op.create_index(
        "ix_auth_challenges_email_created",
        "auth_challenges",
        ["email", "created_at"],
        schema="app",
    )
    op.create_table(
        "verified_sessions",
        sa.Column("session_id", sa.Uuid(), nullable=False),
        user_id(),
        sa.Column(
            "recovery_only",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        stamp("created_at"),
        sa.PrimaryKeyConstraint("session_id", name="pk_verified_sessions"),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["app.profiles.id"],
            name="fk_verified_sessions_user_id",
            ondelete="CASCADE",
        ),
        schema="app",
    )
    op.create_index(
        "ix_verified_sessions_user", "verified_sessions", ["user_id"], schema="app"
    )
    for table in TABLES:
        op.execute(f"ALTER TABLE app.{table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE app.{table} FORCE ROW LEVEL SECURITY")
    op.execute("REVOKE ALL ON ALL TABLES IN SCHEMA app FROM PUBLIC")


def downgrade():
    # Refuse while any user exists: every user table cascades from profiles.
    # Migrations run as the NOBYPASSRLS owner, which sees no rows under FORCE
    # RLS, so lift FORCE first; a refusal rolls this back with everything else.
    op.execute("ALTER TABLE app.profiles NO FORCE ROW LEVEL SECURITY")
    if (
        op.get_bind()
        .execute(sa.text("SELECT EXISTS (SELECT 1 FROM app.profiles)"))
        .scalar()
    ):
        raise RuntimeError("app.profiles has rows; downgrade would drop user data")
    for table in (
        "verified_sessions",
        "auth_challenges",
        "watchlist",
        "notifications",
        "preferences",
        "profiles",
    ):
        op.drop_table(table, schema="app")
    # RESTRICT prevents removing a pre-existing schema with unrelated objects.
    op.execute("DROP SCHEMA app RESTRICT")
