"""Add user accounts, verification codes, sessions and personal data."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None

SCHEMA = "perennial"


def _id() -> sa.Column:
    return sa.Column("id", sa.BigInteger, sa.Identity(always=True), nullable=False)


def upgrade() -> None:
    op.create_table(
        "users",
        _id(),
        sa.Column("email", sa.String(320), nullable=False),
        sa.Column("normalized_email", sa.String(320), nullable=False),
        sa.Column("full_name", sa.String(200), nullable=False),
        sa.Column("password_hash", sa.Text, nullable=False),
        sa.Column(
            "is_verified", sa.Boolean, nullable=False, server_default=sa.text("false")
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name="pk_users"),
        sa.UniqueConstraint("normalized_email", name="uq_users_normalized_email"),
        sa.CheckConstraint(
            "position('@' in normalized_email) > 1", name=op.f("ck_users_email_format")
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "email_verification_codes",
        _id(),
        sa.Column("user_id", sa.BigInteger, nullable=False),
        sa.Column("code_hash", sa.Text, nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempts", sa.Integer, nullable=False, server_default=sa.text("0")),
        sa.Column("used_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name="pk_email_verification_codes"),
        sa.ForeignKeyConstraint(
            ["user_id"],
            [f"{SCHEMA}.users.id"],
            name="fk_email_verification_codes_user_id_users",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "attempts >= 0", name=op.f("ck_email_verification_codes_attempts")
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_email_verification_codes_user_active",
        "email_verification_codes",
        ["user_id", "expires_at"],
        schema=SCHEMA,
    )

    op.create_table(
        "user_preferences",
        sa.Column("user_id", sa.BigInteger, nullable=False),
        sa.Column(
            "sectors",
            postgresql.ARRAY(sa.String(64)),
            nullable=False,
            server_default=sa.text("'{}'::text[]"),
        ),
        sa.Column("risk_level", sa.String(32)),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("user_id", name="pk_user_preferences"),
        sa.ForeignKeyConstraint(
            ["user_id"],
            [f"{SCHEMA}.users.id"],
            name="fk_user_preferences_user_id_users",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "risk_level IS NULL OR risk_level IN ('Conservative', 'Moderate', 'High')",
            name=op.f("ck_user_preferences_risk_level"),
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "user_watchlist",
        sa.Column("user_id", sa.BigInteger, nullable=False),
        sa.Column("security_id", sa.BigInteger, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("user_id", "security_id", name="pk_user_watchlist"),
        sa.ForeignKeyConstraint(
            ["user_id"],
            [f"{SCHEMA}.users.id"],
            name="fk_user_watchlist_user_id_users",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["security_id"],
            [f"{SCHEMA}.securities.id"],
            name="fk_user_watchlist_security_id_securities",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_user_watchlist_security_id",
        "user_watchlist",
        ["security_id"],
        schema=SCHEMA,
    )

    op.create_table(
        "auth_sessions",
        _id(),
        sa.Column("user_id", sa.BigInteger, nullable=False),
        sa.Column("token_hash", sa.String(64), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name="pk_auth_sessions"),
        sa.UniqueConstraint("token_hash", name="uq_auth_sessions_token_hash"),
        sa.ForeignKeyConstraint(
            ["user_id"],
            [f"{SCHEMA}.users.id"],
            name="fk_auth_sessions_user_id_users",
            ondelete="CASCADE",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_auth_sessions_user_active",
        "auth_sessions",
        ["user_id", "expires_at"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_auth_sessions_user_active", table_name="auth_sessions", schema=SCHEMA
    )
    op.drop_table("auth_sessions", schema=SCHEMA)
    op.drop_index(
        "ix_user_watchlist_security_id", table_name="user_watchlist", schema=SCHEMA
    )
    op.drop_table("user_watchlist", schema=SCHEMA)
    op.drop_table("user_preferences", schema=SCHEMA)
    op.drop_index(
        "ix_email_verification_codes_user_active",
        table_name="email_verification_codes",
        schema=SCHEMA,
    )
    op.drop_table("email_verification_codes", schema=SCHEMA)
    op.drop_table("users", schema=SCHEMA)
