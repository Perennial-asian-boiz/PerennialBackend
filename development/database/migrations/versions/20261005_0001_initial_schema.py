"""Initial schema: securities, batches, runs and four source tables.

Revision ID: 0001
Revises:
Create Date: 2026-10-05
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

SCHEMA = "perennial"
SOURCES = "('congress_trades', 'ark_holdings', 'insider_trades', 'short_interest')"


def _id():
    return sa.Column("id", sa.BigInteger, sa.Identity(always=True), nullable=False)


def _batch_fk(table):
    return sa.ForeignKeyConstraint(
        ["batch_id"], [f"{SCHEMA}.source_batches.id"],
        name=f"fk_{table}_batch_id_source_batches", ondelete="CASCADE",
    )


def _security_fk(table):
    return sa.ForeignKeyConstraint(
        ["security_id"], [f"{SCHEMA}.securities.id"], name=f"fk_{table}_security_id_securities"
    )


def upgrade() -> None:
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

    op.create_table(
        "securities",
        _id(),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("exchange", sa.String(20)),
        sa.Column("currency", sa.String(3)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id", name="pk_securities"),
        sa.UniqueConstraint("symbol", name="uq_securities_symbol"),
        sa.CheckConstraint(
            "symbol = upper(symbol) AND symbol ~ '^[A-Z0-9][A-Z0-9./-]*$'",
            name="ck_securities_symbol_format",
        ),
        sa.CheckConstraint("exchange IS NULL OR exchange = upper(exchange)", name="ck_securities_exchange_upper"),
        sa.CheckConstraint("currency IS NULL OR currency ~ '^[A-Z]{3}$'", name="ck_securities_currency_format"),
        schema=SCHEMA,
    )

    op.create_table(
        "source_batches",
        _id(),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("hash_version", sa.String(16), nullable=False),
        sa.Column("imported_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("source_as_of", sa.Date),
        sa.Column("record_count", sa.Integer, nullable=False),
        sa.Column("payload", postgresql.JSONB, nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_source_batches"),
        sa.UniqueConstraint("source", "content_hash", name="uq_source_batches_source_content_hash"),
        sa.UniqueConstraint("id", "source", name="uq_source_batches_id_source"),
        sa.CheckConstraint(f"source IN {SOURCES}", name="ck_source_batches_source"),
        sa.CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="ck_source_batches_content_hash_format"),
        sa.CheckConstraint("record_count >= 0", name="ck_source_batches_record_count"),
        schema=SCHEMA,
    )

    op.create_table(
        "ingestion_runs",
        _id(),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("collection_mode", sa.String(16), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("input_count", sa.Integer),
        sa.Column("accepted_count", sa.Integer),
        sa.Column("batch_id", sa.BigInteger),
        sa.Column("reused_batch", sa.Boolean),
        sa.Column("hash_version", sa.String(16)),
        sa.Column("error_code", sa.String(64)),
        sa.Column("error_summary", sa.Text),
        sa.Column("diagnostics", postgresql.JSONB),
        sa.PrimaryKeyConstraint("id", name="pk_ingestion_runs"),
        sa.ForeignKeyConstraint(
            ["batch_id", "source"],
            [f"{SCHEMA}.source_batches.id", f"{SCHEMA}.source_batches.source"],
            name="fk_ingestion_runs_batch_source",
        ),
        sa.CheckConstraint(f"source IN {SOURCES}", name="ck_ingestion_runs_source"),
        sa.CheckConstraint("status IN ('succeeded', 'failed')", name="ck_ingestion_runs_status"),
        sa.CheckConstraint(
            "collection_mode IN ('fixture', 'file', 'live')", name="ck_ingestion_runs_collection_mode"
        ),
        sa.CheckConstraint(
            "(status = 'succeeded' AND batch_id IS NOT NULL AND reused_batch IS NOT NULL "
            "AND error_code IS NULL) OR (status = 'failed' AND batch_id IS NULL AND error_code IS NOT NULL)",
            name="ck_ingestion_runs_status_consistency",
        ),
        sa.CheckConstraint("char_length(error_summary) <= 1000", name="ck_ingestion_runs_error_summary_length"),
        sa.CheckConstraint(
            "octet_length(diagnostics::text) <= 16384", name="ck_ingestion_runs_diagnostics_size"
        ),
        sa.CheckConstraint("finished_at >= started_at", name="ck_ingestion_runs_finished_after_started"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_ingestion_runs_latest_success", "ingestion_runs",
        ["source", sa.text("finished_at DESC"), sa.text("id DESC")],
        schema=SCHEMA, postgresql_where=sa.text("status = 'succeeded'"),
    )
    op.create_index("ix_ingestion_runs_batch_id", "ingestion_runs", ["batch_id"], schema=SCHEMA)

    op.create_table(
        "congress_trades",
        _id(),
        sa.Column("batch_id", sa.BigInteger, nullable=False),
        sa.Column("security_id", sa.BigInteger, nullable=False),
        sa.Column("row_number", sa.Integer, nullable=False),
        sa.Column("politician_name", sa.String(200), nullable=False),
        sa.Column("chamber", sa.String(16)),
        sa.Column("trade_type", sa.String(100), nullable=False),
        sa.Column("transaction_date", sa.Date, nullable=False),
        sa.Column("disclosure_date", sa.Date),
        sa.Column("amount_label", sa.String(100)),
        sa.Column("asset_description", sa.String(500)),
        sa.Column("asset_type", sa.String(100)),
        sa.Column("district", sa.String(20)),
        sa.Column("source_link", sa.String(2048)),
        sa.Column("data_source", sa.String(32)),
        sa.PrimaryKeyConstraint("id", name="pk_congress_trades"),
        _batch_fk("congress_trades"),
        _security_fk("congress_trades"),
        sa.UniqueConstraint("batch_id", "row_number", name="uq_congress_trades_batch_id_row_number"),
        sa.CheckConstraint("row_number >= 1", name="ck_congress_trades_row_number"),
        schema=SCHEMA,
    )
    op.create_index("ix_congress_trades_security_id", "congress_trades", ["security_id"], schema=SCHEMA)

    op.create_table(
        "ark_holdings",
        _id(),
        sa.Column("batch_id", sa.BigInteger, nullable=False),
        sa.Column("security_id", sa.BigInteger, nullable=False),
        sa.Column("company", sa.String(300)),
        sa.Column("funds", postgresql.ARRAY(sa.String(10)), nullable=False),
        sa.Column("fund_count", sa.Integer, nullable=False),
        sa.Column("total_weight", sa.Numeric(20, 8), nullable=False),
        sa.Column("share_price", sa.Numeric(20, 6)),
        sa.PrimaryKeyConstraint("id", name="pk_ark_holdings"),
        _batch_fk("ark_holdings"),
        _security_fk("ark_holdings"),
        sa.UniqueConstraint("batch_id", "security_id", name="uq_ark_holdings_batch_id_security_id"),
        sa.CheckConstraint("fund_count >= 1 AND fund_count = cardinality(funds)", name="ck_ark_holdings_fund_count"),
        sa.CheckConstraint("total_weight >= 0", name="ck_ark_holdings_total_weight"),
        sa.CheckConstraint("share_price IS NULL OR share_price > 0", name="ck_ark_holdings_share_price"),
        schema=SCHEMA,
    )
    op.create_index("ix_ark_holdings_security_id", "ark_holdings", ["security_id"], schema=SCHEMA)

    op.create_table(
        "insider_trades",
        _id(),
        sa.Column("batch_id", sa.BigInteger, nullable=False),
        sa.Column("security_id", sa.BigInteger, nullable=False),
        sa.Column("row_number", sa.Integer, nullable=False),
        sa.Column("insider_name", sa.String(200), nullable=False),
        sa.Column("transaction_type", sa.String(50), nullable=False),
        sa.Column("transaction_date", sa.Date, nullable=False),
        sa.Column("shares", sa.Numeric(24, 6)),
        sa.Column("value", sa.Numeric(20, 2)),
        sa.Column("bucket", sa.String(32)),
        sa.Column("data_source", sa.String(32)),
        sa.PrimaryKeyConstraint("id", name="pk_insider_trades"),
        _batch_fk("insider_trades"),
        _security_fk("insider_trades"),
        sa.UniqueConstraint("batch_id", "row_number", name="uq_insider_trades_batch_id_row_number"),
        sa.CheckConstraint("row_number >= 1", name="ck_insider_trades_row_number"),
        schema=SCHEMA,
    )
    op.create_index("ix_insider_trades_security_id", "insider_trades", ["security_id"], schema=SCHEMA)

    op.create_table(
        "short_interest",
        _id(),
        sa.Column("batch_id", sa.BigInteger, nullable=False),
        sa.Column("security_id", sa.BigInteger, nullable=False),
        sa.Column("settlement_date", sa.Date, nullable=False),
        sa.Column("short_interest_shares", sa.BigInteger),
        sa.Column("average_daily_volume", sa.BigInteger),
        sa.Column("days_to_cover", sa.Numeric(12, 4)),
        sa.PrimaryKeyConstraint("id", name="pk_short_interest"),
        _batch_fk("short_interest"),
        _security_fk("short_interest"),
        sa.UniqueConstraint("batch_id", "security_id", name="uq_short_interest_batch_id_security_id"),
        sa.CheckConstraint(
            "short_interest_shares IS NULL OR short_interest_shares >= 0", name="ck_short_interest_shares"
        ),
        sa.CheckConstraint(
            "average_daily_volume IS NULL OR average_daily_volume >= 0", name="ck_short_interest_volume"
        ),
        sa.CheckConstraint("days_to_cover IS NULL OR days_to_cover >= 0", name="ck_short_interest_days_to_cover"),
        schema=SCHEMA,
    )
    op.create_index("ix_short_interest_security_id", "short_interest", ["security_id"], schema=SCHEMA)


def downgrade() -> None:
    for table in ("short_interest", "insider_trades", "ark_holdings", "congress_trades",
                  "ingestion_runs", "source_batches", "securities"):
        op.drop_table(table, schema=SCHEMA)
    op.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} RESTRICT")
