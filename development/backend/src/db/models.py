"""
Table definitions for the `perennial` application schema.

These mirror development/database/migrations; a test asserts that the
migrated database and this metadata do not drift apart.

Batches are immutable snapshots of one source's fetcher output. Source rows
belong to exactly one batch. Operational queries can read the latest successful ingestion run per source.
Production readers use current_publication and its pinned source batches.
Neither read path aggregates rows across snapshots.
"""

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

SCHEMA = "perennial"

SOURCES = ("congress_trades", "ark_holdings", "insider_trades", "short_interest")
RUN_STATUSES = ("running", "succeeded", "failed", "abandoned")
COLLECTION_MODES = ("fixture", "file", "live")

ERROR_SUMMARY_MAX = 1000
DIAGNOSTICS_MAX_BYTES = 16384

metadata = MetaData(
    schema=SCHEMA,
    naming_convention={
        "ix": "ix_%(table_name)s_%(column_0_N_name)s",
        "uq": "uq_%(table_name)s_%(column_0_N_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    },
)


def _in(column: str, values) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def _id() -> Column:
    return Column("id", BigInteger, Identity(always=True), primary_key=True)


securities = Table(
    "securities",
    metadata,
    _id(),
    Column("symbol", String(20), nullable=False),
    Column("exchange", String(20)),
    Column("currency", String(3)),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    UniqueConstraint("symbol"),
    CheckConstraint("symbol = upper(symbol) AND symbol ~ '^[A-Z0-9][A-Z0-9./-]*$'", name="symbol_format"),
    CheckConstraint("exchange IS NULL OR exchange = upper(exchange)", name="exchange_upper"),
    CheckConstraint("currency IS NULL OR currency ~ '^[A-Z]{3}$'", name="currency_format"),
)

source_batches = Table(
    "source_batches",
    metadata,
    _id(),
    Column("source", String(32), nullable=False),
    Column("content_hash", String(64), nullable=False),
    Column("hash_version", String(16), nullable=False),
    Column("imported_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    Column("source_as_of", Date),
    Column("record_count", Integer, nullable=False),
    Column("payload", JSONB, nullable=False),
    UniqueConstraint("source", "content_hash"),
    UniqueConstraint("id", "source"),  # target of the run -> batch composite FK
    CheckConstraint(_in("source", SOURCES), name="source"),
    CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_format"),
    CheckConstraint("record_count >= 0", name="record_count"),
)

ingestion_runs = Table(
    "ingestion_runs",
    metadata,
    _id(),
    Column("source", String(32), nullable=False),
    Column("status", String(16), nullable=False),
    Column("collection_mode", String(16), nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("heartbeat_at", DateTime(timezone=True)),
    Column("collected_at", DateTime(timezone=True)),
    Column("coverage", JSONB),
    Column("input_count", Integer),
    Column("accepted_count", Integer),
    Column("batch_id", BigInteger),
    Column("reused_batch", Boolean),
    Column("hash_version", String(16)),
    Column("error_code", String(64)),
    Column("error_summary", Text),
    Column("diagnostics", JSONB),
    # A run can only reference a batch of its own source.
    ForeignKeyConstraint(
        ["batch_id", "source"],
        [f"{SCHEMA}.source_batches.id", f"{SCHEMA}.source_batches.source"],
        name="fk_ingestion_runs_batch_source",
    ),
    CheckConstraint(_in("source", SOURCES), name="source"),
    CheckConstraint(_in("status", RUN_STATUSES), name="status"),
    CheckConstraint(_in("collection_mode", COLLECTION_MODES), name="collection_mode"),
    CheckConstraint(
        "(status = 'succeeded' AND batch_id IS NOT NULL AND reused_batch IS NOT NULL "
        "AND error_code IS NULL AND finished_at IS NOT NULL) OR "
        "(status IN ('failed', 'abandoned') AND batch_id IS NULL AND error_code IS NOT NULL "
        "AND finished_at IS NOT NULL) OR "
        "(status = 'running' AND batch_id IS NULL AND error_code IS NULL AND finished_at IS NULL)",
        name="status_consistency",
    ),
    CheckConstraint(f"char_length(error_summary) <= {ERROR_SUMMARY_MAX}", name="error_summary_length"),
    CheckConstraint(f"octet_length(diagnostics::text) <= {DIAGNOSTICS_MAX_BYTES}", name="diagnostics_size"),
    CheckConstraint("finished_at >= started_at", name="finished_after_started"),
    UniqueConstraint("id", "batch_id", "source"),
)
Index(
    "ix_ingestion_runs_latest_success",
    ingestion_runs.c.source,
    ingestion_runs.c.finished_at.desc(),
    ingestion_runs.c.id.desc(),
    postgresql_where=text("status = 'succeeded'"),
)
Index("ix_ingestion_runs_batch_id", ingestion_runs.c.batch_id)


def _batch_fk() -> Column:
    return Column(
        "batch_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.source_batches.id", ondelete="CASCADE"),
        nullable=False,
    )


def _security_fk() -> Column:
    return Column("security_id", BigInteger, ForeignKey(f"{SCHEMA}.securities.id"), nullable=False)


congress_trades = Table(
    "congress_trades",
    metadata,
    _id(),
    _batch_fk(),
    _security_fk(),
    Column("row_number", Integer, nullable=False),
    Column("politician_name", String(200), nullable=False),
    Column("chamber", String(16)),
    Column("trade_type", String(100), nullable=False),
    Column("transaction_date", Date, nullable=False),
    Column("disclosure_date", Date),
    Column("amount_label", String(100)),
    Column("asset_description", String(500)),
    Column("asset_type", String(100)),
    Column("district", String(20)),
    Column("source_link", String(2048)),
    Column("data_source", String(32)),
    UniqueConstraint("batch_id", "row_number"),
    CheckConstraint("row_number >= 1", name="row_number"),
)
Index("ix_congress_trades_security_id", congress_trades.c.security_id)

ark_holdings = Table(
    "ark_holdings",
    metadata,
    _id(),
    _batch_fk(),
    _security_fk(),
    Column("company", String(300)),
    Column("funds", ARRAY(String(10)), nullable=False),
    Column("fund_count", Integer, nullable=False),
    # Sum of the ticker's weight across ARK funds; not a single 0-100 allocation.
    Column("total_weight", Numeric(20, 8), nullable=False),
    Column("share_price", Numeric(20, 6)),
    UniqueConstraint("batch_id", "security_id"),
    CheckConstraint("fund_count >= 1 AND fund_count = cardinality(funds)", name="fund_count"),
    CheckConstraint("total_weight >= 0", name="total_weight"),
    CheckConstraint("share_price IS NULL OR share_price > 0", name="share_price"),
)
Index("ix_ark_holdings_security_id", ark_holdings.c.security_id)

insider_trades = Table(
    "insider_trades",
    metadata,
    _id(),
    _batch_fk(),
    _security_fk(),
    Column("row_number", Integer, nullable=False),
    Column("insider_name", String(200), nullable=False),
    Column("transaction_type", String(50), nullable=False),
    Column("transaction_date", Date, nullable=False),
    Column("shares", Numeric(24, 6)),
    Column("value", Numeric(20, 2)),
    Column("bucket", String(32)),
    Column("data_source", String(32)),
    UniqueConstraint("batch_id", "row_number"),
    CheckConstraint("row_number >= 1", name="row_number"),
)
Index("ix_insider_trades_security_id", insider_trades.c.security_id)

short_interest = Table(
    "short_interest",
    metadata,
    _id(),
    _batch_fk(),
    _security_fk(),
    Column("settlement_date", Date, nullable=False),
    Column("short_interest_shares", BigInteger),
    Column("average_daily_volume", BigInteger),
    Column("days_to_cover", Numeric(12, 4)),
    UniqueConstraint("batch_id", "security_id"),
    CheckConstraint("short_interest_shares IS NULL OR short_interest_shares >= 0", name="shares"),
    CheckConstraint("average_daily_volume IS NULL OR average_daily_volume >= 0", name="volume"),
    CheckConstraint("days_to_cover IS NULL OR days_to_cover >= 0", name="days_to_cover"),
)
Index("ix_short_interest_security_id", short_interest.c.security_id)

SOURCE_TABLES = {
    "congress_trades": congress_trades,
    "ark_holdings": ark_holdings,
    "insider_trades": insider_trades,
    "short_interest": short_interest,
}

# These tables make collection lineage and publication explicit. Historical
# imports never change the single current-publication pointer by themselves.
run_dependencies = Table(
    "run_dependencies", metadata,
    Column("run_id", BigInteger, ForeignKey(f"{SCHEMA}.ingestion_runs.id"), primary_key=True),
    Column("source", String(32), primary_key=True),
    Column("batch_id", BigInteger, nullable=False),
    ForeignKeyConstraint(["batch_id", "source"],
        [f"{SCHEMA}.source_batches.id", f"{SCHEMA}.source_batches.source"]),
)

publications = Table(
    "publications", metadata, _id(),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("ranking_version", String(32), nullable=False),
    Column("ranking_config", JSONB, nullable=False),
    Column("output", JSONB, nullable=False),
)

publication_sources = Table(
    "publication_sources", metadata,
    Column("publication_id", BigInteger, ForeignKey(f"{SCHEMA}.publications.id"), primary_key=True),
    Column("source", String(32), primary_key=True),
    Column("run_id", BigInteger, nullable=False),
    Column("batch_id", BigInteger, nullable=False),
    ForeignKeyConstraint(["run_id", "batch_id", "source"],
        [f"{SCHEMA}.ingestion_runs.id", f"{SCHEMA}.ingestion_runs.batch_id", f"{SCHEMA}.ingestion_runs.source"]),
)

market_cap_observations = Table(
    "market_cap_observations", metadata, _id(), _security_fk(),
    Column("value", Numeric(24, 2)),
    Column("currency", String(3), nullable=False),
    Column("provider", String(32), nullable=False),
    Column("observed_at", DateTime(timezone=True)),
    Column("retrieved_at", DateTime(timezone=True), nullable=False),
    CheckConstraint("value IS NULL OR value > 0", name="positive_value"),
    CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency"),
    UniqueConstraint("id", "security_id"),
)
Index("ix_market_cap_observations_security_time", market_cap_observations.c.security_id,
      market_cap_observations.c.retrieved_at.desc())

publication_market_caps = Table(
    "publication_market_caps", metadata,
    Column("publication_id", BigInteger, ForeignKey(f"{SCHEMA}.publications.id"), primary_key=True),
    Column("security_id", BigInteger, primary_key=True),
    Column("observation_id", BigInteger, nullable=False),
    ForeignKeyConstraint(["observation_id", "security_id"],
        [f"{SCHEMA}.market_cap_observations.id", f"{SCHEMA}.market_cap_observations.security_id"]),
)

current_publication = Table(
    "current_publication", metadata,
    Column("channel", String(32), primary_key=True),
    Column("publication_id", BigInteger, ForeignKey(f"{SCHEMA}.publications.id"), nullable=False),
    Column("promoted_at", DateTime(timezone=True), nullable=False),
)

# Supporting indexes for referencing FKs and per-source health lookups.
Index("ix_publication_sources_run_batch_source", publication_sources.c.run_id,
      publication_sources.c.batch_id, publication_sources.c.source)
Index("ix_run_dependencies_batch_source", run_dependencies.c.batch_id, run_dependencies.c.source)
Index("ix_publication_market_caps_observation_security", publication_market_caps.c.observation_id,
      publication_market_caps.c.security_id)
Index("ix_ingestion_runs_source_started", ingestion_runs.c.source,
      ingestion_runs.c.started_at.desc(), ingestion_runs.c.id.desc())
