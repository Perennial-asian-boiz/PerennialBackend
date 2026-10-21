"""Add notification settings for user accounts."""

import sqlalchemy as sa
from alembic import op

revision = "auth_settings_v1"
down_revision = "auth_users_v1"
branch_labels = None
depends_on = None

SCHEMA = "perennial"


def upgrade() -> None:
    op.create_table(
        "user_notification_settings",
        sa.Column("user_id", sa.BigInteger, nullable=False),
        sa.Column(
            "confidence_score_enabled",
            sa.Boolean,
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column(
            "earnings_alerts_enabled",
            sa.Boolean,
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("user_id", name="pk_user_notification_settings"),
        sa.ForeignKeyConstraint(
            ["user_id"],
            [f"{SCHEMA}.users.id"],
            name="fk_user_notification_settings_user_id_users",
            ondelete="CASCADE",
        ),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("user_notification_settings", schema=SCHEMA)
