"""Supabase-owned identities with isolated application data and RLS."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


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
    op.execute("CREATE SCHEMA IF NOT EXISTS app")
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
        sa.Column("ticker", sa.String(20), nullable=False),
        stamp("created_at"),
        sa.PrimaryKeyConstraint("user_id", "ticker", name="pk_watchlist"),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["app.profiles.id"],
            name="fk_watchlist_user_id",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "ticker ~ '^[A-Z0-9][A-Z0-9./-]*$'", name=op.f("ck_watchlist_ticker")
        ),
        schema="app",
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
    for table, column in (
        ("profiles", "id"),
        ("preferences", "user_id"),
        ("notifications", "user_id"),
        ("watchlist", "user_id"),
    ):
        op.execute(f"ALTER TABLE app.{table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE app.{table} FORCE ROW LEVEL SECURITY")
        owner = f"{column} = nullif(current_setting('request.jwt.claim.sub', true), '')::uuid"
        op.execute(
            f"CREATE POLICY own_rows ON app.{table} USING ({owner}) WITH CHECK ({owner})"
        )
    op.execute("REVOKE ALL ON ALL TABLES IN SCHEMA app FROM PUBLIC")


def downgrade():
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
