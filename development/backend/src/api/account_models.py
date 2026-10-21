"""Application-owned data keyed by Supabase identity UUIDs, outside the pipeline."""

from sqlalchemy import (
    ARRAY,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Uuid,
    text,
)

metadata = MetaData(
    schema="app",
    naming_convention={
        "pk": "pk_%(table_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
    },
)


def timestamp(name="updated_at"):
    return Column(
        name, DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


profiles = Table(
    "profiles",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("email", String(320), nullable=False),
    Column("full_name", String(200), nullable=False),
    Column(
        "onboarding_completed", Boolean, nullable=False, server_default=text("false")
    ),
    timestamp(),
)


def user_id():
    return Column(
        "user_id",
        Uuid,
        ForeignKey("app.profiles.id", ondelete="CASCADE"),
        primary_key=True,
    )


preferences = Table(
    "preferences",
    metadata,
    user_id(),
    Column(
        "sectors",
        ARRAY(String(64)),
        nullable=False,
        server_default=text("'{}'::text[]"),
    ),
    Column("risk_level", String(32)),
    timestamp(),
    CheckConstraint(
        "risk_level IS NULL OR risk_level IN ('Conservative', 'Moderate', 'High')",
        name="risk_level",
    ),
)
notifications = Table(
    "notifications",
    metadata,
    user_id(),
    Column(
        "confidence_score_enabled", Boolean, nullable=False, server_default=text("true")
    ),
    Column(
        "earnings_alerts_enabled", Boolean, nullable=False, server_default=text("true")
    ),
    timestamp(),
)
watchlist = Table(
    "watchlist",
    metadata,
    user_id(),
    Column("ticker", String(20), primary_key=True),
    timestamp("created_at"),
    CheckConstraint("ticker ~ '^[A-Z0-9][A-Z0-9./-]*$'", name="ticker"),
)

# These are backend-only security records, never exposed through the Data API.
# No password, OTP value, access token or refresh token is stored here.
auth_challenges = Table(
    "auth_challenges",
    metadata,
    Column("token_hash", String(64), primary_key=True),
    Column("email", String(320), nullable=False),
    Column("flow", String(16), nullable=False),
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("send_count", Integer, nullable=False, server_default=text("1")),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("consumed_at", DateTime(timezone=True)),
    timestamp("created_at"),
    timestamp("last_sent_at"),
    CheckConstraint("flow IN ('signup', 'login', 'recovery')", name="flow"),
    CheckConstraint("attempts >= 0 AND send_count >= 1", name="attempts"),
)
Index(
    "ix_auth_challenges_email_created",
    auth_challenges.c.email,
    auth_challenges.c.created_at,
)
verified_sessions = Table(
    "verified_sessions",
    metadata,
    Column("session_id", Uuid, primary_key=True),
    Column(
        "user_id",
        Uuid,
        ForeignKey("app.profiles.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("recovery_only", Boolean, nullable=False, server_default=text("false")),
    Column("revoked_at", DateTime(timezone=True)),
    timestamp("created_at"),
)
Index("ix_verified_sessions_user", verified_sessions.c.user_id)
