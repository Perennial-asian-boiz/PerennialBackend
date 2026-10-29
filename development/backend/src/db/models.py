"""
Table definitions for the `perennial` application schema.

These mirror development/database/migrations; a test asserts that the
migrated database and this metadata do not drift apart.

Batches are immutable snapshots of one source's fetcher output. Source rows
belong to exactly one batch. Operational queries can read the latest successful ingestion run per source.
Production readers use current_publication and its pinned source batches.
Neither read path aggregates rows across snapshots.
"""

from typing import Any, Iterable

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
    SmallInteger,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

SCHEMA = "perennial"

# The watchlist publication pins exactly these sources; nothing else can block it.
WATCHLIST_SOURCES = (
    "congress_trades",
    "ark_holdings",
    "insider_trades",
    "short_interest",
)
# Sources whose rows are not about one security: no ticker, no securities FK.
SUBJECTLESS_SOURCES: tuple[str, ...] = (
    "gdelt_theme_volume",
    "gdelt_theme_articles",
    "gdelt_spike_results",  # derived from gdelt_theme_volume (collection_mode derived)
)
# Sources a publication pins row by row (publication_market_cap_rows), not by
# one batch in publication_sources. Not watchlist sources (M4 MC-A).
PINNED_ROW_SOURCES: tuple[str, ...] = ("market_caps",)
# Pattern evaluation (src/services/fundamentals): SEC EDGAR annual figures and the
# derived fundamentals score. Per security, never part of the watchlist publication.
FUNDAMENTALS_SOURCES: tuple[str, ...] = ("fundamentals", "fundamental_scores")
# S1-03 news ingestion (src/services/news): articles per security from Finnhub.
NEWS_SOURCES: tuple[str, ...] = ("news_articles",)
# Sprint 2 scores (src/services/scores): the ticker sentiment contract (#76), the
# derived score-v1 blend and its explanations. Per security, never in the watchlist.
SCORE_SOURCES: tuple[str, ...] = (
    "ticker_sentiment",
    "company_scores",
    "score_explanations",
)
# The closed source vocabulary (CHECKs, CLI, locks). Append only; lifecycle lock
# keys are explicit per source (SOURCE_LOCK_KEYS).
SOURCES = (
    WATCHLIST_SOURCES
    + SUBJECTLESS_SOURCES
    + PINNED_ROW_SOURCES
    + FUNDAMENTALS_SOURCES
    + NEWS_SOURCES
    + SCORE_SOURCES
)
RUN_STATUSES = ("running", "succeeded", "failed", "abandoned")
COLLECTION_MODES = ("fixture", "file", "live", "derived")
PUBLICATION_EVENT_KINDS = ("quality_passed", "quality_override", "degraded", "rollback")
SECURITY_TYPES = ("common", "adr", "etf")
IDENTIFIER_SCHEMES = ("ticker", "figi", "cusip", "cik")

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


def _in(column: str, values: Iterable[str]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def _id() -> Column[Any]:
    return Column("id", BigInteger, Identity(always=True), primary_key=True)


# One row per issuer (M4). Share classes such as GOOG and GOOGL stay distinct
# securities that point at one profile. sector/industry are expanded here from
# market_cap_observations; GDELT reads switch over in a later change.
company_profiles = Table(
    "company_profiles",
    metadata,
    _id(),
    Column("legal_name", Text),
    Column("sector", String(64)),
    Column("industry", String(128)),
    Column("alias_names", ARRAY(Text), nullable=False, server_default=text("'{}'")),
    # Issuer-level: one CIK can cover several share classes.
    Column("cik", String(10)),
    # The share class whose provider listing carries the issuer's insider filings.
    Column(
        "insider_reporting_security_id",
        BigInteger,
        ForeignKey(
            f"{SCHEMA}.securities.id",
            use_alter=True,
            name="fk_company_profiles_insider_reporting_security_id_securities",
        ),
    ),
    # S1-12: the stored excerpt, its source, and the SHA-256 of the description it
    # came from. The description itself is never stored (migration 0012).
    Column("summary", Text),
    Column("summary_grade", Numeric(5, 2)),
    Column("summary_status", String(16)),
    Column("summary_source_name", String(128)),
    Column("summary_source_url", Text),
    Column("summary_as_of", DateTime(timezone=True)),
    Column("description_digest", String(64)),
    UniqueConstraint("cik"),
    CheckConstraint("cik IS NULL OR cik ~ '^[0-9]{10}$'", name="cik_format"),
    CheckConstraint(
        "legal_name IS NULL OR btrim(legal_name) <> ''", name="legal_name_not_blank"
    ),
    CheckConstraint(
        "summary IS NULL OR (btrim(summary) <> '' AND char_length(summary) <= 4000)",
        name="summary_not_blank",
    ),
    CheckConstraint(
        "summary_status IS NULL OR summary_status IN ('ok', 'unavailable')",
        name="summary_status",
    ),
    CheckConstraint(
        "summary_source_name IS NULL OR btrim(summary_source_name) <> ''",
        name="summary_source_name_not_blank",
    ),
    # https with a host only: the API never serves another scheme (CS-5).
    CheckConstraint(
        "summary_source_url IS NULL OR (char_length(summary_source_url) <= 2048"
        " AND summary_source_url"
        " ~ '^https://[A-Za-z0-9.-]+(:[0-9]{1,5})?([/?#][^[:space:]]*)?$')",
        name="summary_source_url_https",
    ),
    CheckConstraint(
        "description_digest IS NULL OR description_digest ~ '^[0-9a-f]{64}$'",
        name="description_digest_sha256",
    ),
    # An ok summary is served, so every part of it must be stored (0013).
    CheckConstraint(
        "summary_status IS DISTINCT FROM 'ok' OR (summary IS NOT NULL"
        " AND summary_grade IS NOT NULL AND summary_source_name IS NOT NULL"
        " AND summary_source_url IS NOT NULL AND summary_as_of IS NOT NULL"
        " AND description_digest IS NOT NULL)",
        name="summary_ok_complete",
    ),
)

securities = Table(
    "securities",
    metadata,
    _id(),
    Column("symbol", String(20), nullable=False),
    Column("exchange", String(20)),
    Column("currency", String(3)),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column("name", Text),
    Column("security_type", String(16)),
    Column("country", String(2)),
    Column("figi", String(12)),
    Column("is_active", Boolean, nullable=False, server_default=text("true")),
    Column(
        "company_profile_id", BigInteger, ForeignKey(f"{SCHEMA}.company_profiles.id")
    ),
    UniqueConstraint("symbol"),
    UniqueConstraint("figi"),
    CheckConstraint(
        f"security_type IS NULL OR {_in('security_type', SECURITY_TYPES)}",
        name="security_type",
    ),
    CheckConstraint("country IS NULL OR country ~ '^[A-Z]{2}$'", name="country_format"),
    CheckConstraint("figi IS NULL OR figi ~ '^[A-Z0-9]{12}$'", name="figi_format"),
    CheckConstraint(
        "symbol = upper(symbol) AND symbol ~ '^[A-Z0-9][A-Z0-9./-]*$'",
        name="symbol_format",
    ),
    CheckConstraint(
        "exchange IS NULL OR exchange = upper(exchange)", name="exchange_upper"
    ),
    CheckConstraint(
        "currency IS NULL OR currency ~ '^[A-Z]{3}$'", name="currency_format"
    ),
)

Index("ix_securities_company_profile_id", securities.c.company_profile_id)

# Lookup-only aliases (M4). Provider spellings are encoded in value with a fixed
# provider prefix ("FINNHUB:BRK-B"); a bare value is the canonical ticker.
security_identifiers = Table(
    "security_identifiers",
    metadata,
    Column("scheme", String(16), primary_key=True),
    Column("value", String(64), primary_key=True),
    Column(
        "security_id", BigInteger, ForeignKey(f"{SCHEMA}.securities.id"), nullable=False
    ),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    CheckConstraint(_in("scheme", IDENTIFIER_SCHEMES), name="scheme"),
    CheckConstraint(
        "value = btrim(value) AND value <> '' AND value = upper(value)",
        name="value_format",
    ),
)
Index("ix_security_identifiers_security_id", security_identifiers.c.security_id)

source_batches = Table(
    "source_batches",
    metadata,
    _id(),
    Column("source", String(32), nullable=False),
    Column("content_hash", String(64), nullable=False),
    Column("hash_version", String(16), nullable=False),
    Column(
        "imported_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
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
    CheckConstraint(
        f"char_length(error_summary) <= {ERROR_SUMMARY_MAX}",
        name="error_summary_length",
    ),
    CheckConstraint(
        f"octet_length(diagnostics::text) <= {DIAGNOSTICS_MAX_BYTES}",
        name="diagnostics_size",
    ),
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


def _batch_fk() -> Column[Any]:
    return Column(
        "batch_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.source_batches.id", ondelete="CASCADE"),
        nullable=False,
    )


def _security_fk() -> Column[Any]:
    return Column(
        "security_id", BigInteger, ForeignKey(f"{SCHEMA}.securities.id"), nullable=False
    )


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
    CheckConstraint(
        "fund_count >= 1 AND fund_count = cardinality(funds)", name="fund_count"
    ),
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
    CheckConstraint(
        "short_interest_shares IS NULL OR short_interest_shares >= 0", name="shares"
    ),
    CheckConstraint(
        "average_daily_volume IS NULL OR average_daily_volume >= 0", name="volume"
    ),
    CheckConstraint(
        "days_to_cover IS NULL OR days_to_cover >= 0", name="days_to_cover"
    ),
)
Index("ix_short_interest_security_id", short_interest.c.security_id)

_THEME_FORMAT = "theme ~ '^[A-Z0-9_]{1,64}$'"

# GDELT through the ingestion contract: immutable batches with run lineage.
gdelt_theme_volume = Table(
    "gdelt_theme_volume",
    metadata,
    _id(),
    _batch_fk(),
    Column("row_number", Integer, nullable=False),
    Column("theme", String(64), nullable=False),
    Column("day", Date, nullable=False),
    Column("article_count", Integer, nullable=False),
    Column("total_articles", BigInteger),
    Column("avg_tone", Numeric(8, 4)),
    UniqueConstraint("batch_id", "row_number"),
    UniqueConstraint("batch_id", "theme", "day"),
    CheckConstraint("row_number >= 1", name="row_number"),
    CheckConstraint(_THEME_FORMAT, name="theme_format"),
    CheckConstraint("article_count >= 0", name="article_count"),
    CheckConstraint(
        "total_articles IS NULL OR total_articles >= 0", name="total_articles"
    ),
)

gdelt_theme_articles = Table(
    "gdelt_theme_articles",
    metadata,
    _id(),
    _batch_fk(),
    Column("row_number", Integer, nullable=False),
    Column("theme", String(64), nullable=False),
    Column("day", Date, nullable=False),
    Column("url", String(2048), nullable=False),
    Column("title", String(500)),
    Column("domain", String(255)),
    Column("seen_at", DateTime(timezone=True)),
    UniqueConstraint("batch_id", "row_number"),
    UniqueConstraint("batch_id", "theme", "url"),
    CheckConstraint("row_number >= 1", name="row_number"),
    CheckConstraint(_THEME_FORMAT, name="theme_format"),
    CheckConstraint("url ~ '^https?://'", name="url_scheme"),
)

gdelt_spike_results = Table(
    "gdelt_spike_results",
    metadata,
    _id(),
    _batch_fk(),
    Column("row_number", Integer, nullable=False),
    Column("theme", String(64), nullable=False),
    Column("day", Date, nullable=False),
    Column("article_count", Integer, nullable=False),
    Column("baseline_median", Numeric(12, 2)),
    Column("observed_baseline_days", SmallInteger, nullable=False),
    Column("ratio", Numeric(10, 2)),
    Column("is_spike", Boolean),
    Column("rule_version", String(16), nullable=False),
    UniqueConstraint("batch_id", "row_number"),
    UniqueConstraint("batch_id", "theme", "day"),
    CheckConstraint("row_number >= 1", name="row_number"),
    CheckConstraint(_THEME_FORMAT, name="theme_format"),
    CheckConstraint("article_count >= 0", name="article_count"),
    CheckConstraint("observed_baseline_days >= 0", name="observed_baseline_days"),
)

# M4 market caps through the ingestion contract (MC-A): one row per security
# per batch, from one provider adapter. retrieved_at is the run's collected_at.
market_caps = Table(
    "market_caps",
    metadata,
    _id(),
    _batch_fk(),
    _security_fk(),
    Column("value", Numeric(24, 2)),  # NULL: the provider returned no value
    Column("currency", String(3), nullable=False),
    # In the row, not only the envelope: the envelope is not part of batch identity.
    Column("provider", String(32), nullable=False),
    Column("issuer_level", Boolean, nullable=False),  # never summed across classes
    Column("observed_at", DateTime(timezone=True)),
    Column("sector", String(64)),
    Column("industry", String(128)),
    UniqueConstraint("batch_id", "security_id"),
    UniqueConstraint("id", "batch_id", "security_id"),  # target of the pin FK
    CheckConstraint("value IS NULL OR value > 0", name="positive_value"),
    CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency"),
    CheckConstraint("provider ~ '^[a-z0-9_]{1,32}$'", name="provider"),
)
Index("ix_market_caps_security_id", market_caps.c.security_id)

SOURCE_TABLES = {
    "congress_trades": congress_trades,
    "ark_holdings": ark_holdings,
    "insider_trades": insider_trades,
    "short_interest": short_interest,
    "gdelt_theme_volume": gdelt_theme_volume,
    "gdelt_theme_articles": gdelt_theme_articles,
    "gdelt_spike_results": gdelt_spike_results,
    "market_caps": market_caps,
}

# These tables make collection lineage and publication explicit. Historical
# imports never change the single current-publication pointer by themselves.
run_dependencies = Table(
    "run_dependencies",
    metadata,
    Column(
        "run_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.ingestion_runs.id"),
        primary_key=True,
    ),
    Column("source", String(32), primary_key=True),
    Column("batch_id", BigInteger, nullable=False),
    # For a downstream run's Congress dependency: the accumulated history it
    # was planned from (congress_trade_history ids <= this). Publication pins it.
    Column("history_through_id", BigInteger),
    ForeignKeyConstraint(
        ["batch_id", "source"],
        [f"{SCHEMA}.source_batches.id", f"{SCHEMA}.source_batches.source"],
    ),
    CheckConstraint(
        "history_through_id IS NULL "
        "OR (source = 'congress_trades' AND history_through_id >= 0)",
        name="history_through_id",
    ),
)

publications = Table(
    "publications",
    metadata,
    _id(),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("ranking_version", String(32), nullable=False),
    Column("ranking_config", JSONB, nullable=False),
    Column("output", JSONB, nullable=False),
)

# Append-only audit beside publications. ranking_config is a replay input, so
# quality verdicts, degraded markers and rollbacks are recorded here instead.
publication_events = Table(
    "publication_events",
    metadata,
    _id(),
    Column(
        "publication_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.publications.id"),
        nullable=False,
    ),
    Column("kind", String(32), nullable=False),
    Column("actor", String(200)),
    Column("reason", String(500)),
    Column("checks", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(_in("kind", PUBLICATION_EVENT_KINDS), name="kind"),
    CheckConstraint("actor IS NULL OR btrim(actor) <> ''", name="actor_not_blank"),
    CheckConstraint("reason IS NULL OR btrim(reason) <> ''", name="reason_not_blank"),
    CheckConstraint(
        "kind NOT IN ('quality_override', 'rollback') "
        "OR (actor IS NOT NULL AND reason IS NOT NULL)",
        name="decision_attributed",
    ),
    CheckConstraint("jsonb_typeof(checks) = 'object'", name="checks_object"),
)
Index("ix_publication_events_publication_id", publication_events.c.publication_id)
Index(
    "uq_publication_events_quality",
    publication_events.c.publication_id,
    unique=True,
    postgresql_where=text("kind IN ('quality_passed', 'quality_override')"),
)
Index(
    "uq_publication_events_degraded",
    publication_events.c.publication_id,
    unique=True,
    postgresql_where=text("kind = 'degraded'"),
)

publication_sources = Table(
    "publication_sources",
    metadata,
    Column(
        "publication_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.publications.id"),
        primary_key=True,
    ),
    Column("source", String(32), primary_key=True),
    Column("run_id", BigInteger, nullable=False),
    Column("batch_id", BigInteger, nullable=False),
    ForeignKeyConstraint(
        ["run_id", "batch_id", "source"],
        [
            f"{SCHEMA}.ingestion_runs.id",
            f"{SCHEMA}.ingestion_runs.batch_id",
            f"{SCHEMA}.ingestion_runs.source",
        ],
    ),
)

market_cap_observations = Table(
    "market_cap_observations",
    metadata,
    _id(),
    _security_fk(),
    Column("value", Numeric(24, 2)),
    Column("currency", String(3), nullable=False),
    Column("provider", String(32), nullable=False),
    Column("observed_at", DateTime(timezone=True)),
    Column("retrieved_at", DateTime(timezone=True), nullable=False),
    # Classification from the same profile response, so publications replay it too.
    Column("sector", String(64)),
    Column("industry", String(128)),
    CheckConstraint("value IS NULL OR value > 0", name="positive_value"),
    CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency"),
    UniqueConstraint("id", "security_id"),
)
Index(
    "ix_market_cap_observations_security_time",
    market_cap_observations.c.security_id,
    market_cap_observations.c.retrieved_at.desc(),
)

publication_market_caps = Table(
    "publication_market_caps",
    metadata,
    Column(
        "publication_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.publications.id"),
        primary_key=True,
    ),
    Column("security_id", BigInteger, primary_key=True),
    Column("observation_id", BigInteger, nullable=False),
    ForeignKeyConstraint(
        ["observation_id", "security_id"],
        [
            f"{SCHEMA}.market_cap_observations.id",
            f"{SCHEMA}.market_cap_observations.security_id",
        ],
    ),
)

# The exact market_caps row a publication used for each candidate (M4 MC-A).
# Every candidate gets a pin; all-NULL lineage means it was unavailable. The
# run records which retrieval made the row eligible: a reused batch can belong
# to several runs.
publication_market_cap_rows = Table(
    "publication_market_cap_rows",
    metadata,
    Column(
        "publication_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.publications.id"),
        primary_key=True,
    ),
    Column(
        "security_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.securities.id"),
        primary_key=True,
    ),
    Column("source", String(32), nullable=False),
    Column("run_id", BigInteger),
    Column("batch_id", BigInteger),
    Column("market_cap_id", BigInteger),
    ForeignKeyConstraint(
        ["run_id", "batch_id", "source"],
        [
            f"{SCHEMA}.ingestion_runs.id",
            f"{SCHEMA}.ingestion_runs.batch_id",
            f"{SCHEMA}.ingestion_runs.source",
        ],
    ),
    ForeignKeyConstraint(
        ["market_cap_id", "batch_id", "security_id"],
        [
            f"{SCHEMA}.market_caps.id",
            f"{SCHEMA}.market_caps.batch_id",
            f"{SCHEMA}.market_caps.security_id",
        ],
    ),
    CheckConstraint("source = 'market_caps'", name="source"),
    # The composite FKs only apply when every column is set.
    CheckConstraint(
        "(run_id IS NULL AND batch_id IS NULL AND market_cap_id IS NULL) OR "
        "(run_id IS NOT NULL AND batch_id IS NOT NULL AND market_cap_id IS NOT NULL)",
        name="lineage_all_or_none",
    ),
)
Index(
    "ix_publication_market_cap_rows_run_batch_source",
    publication_market_cap_rows.c.run_id,
    publication_market_cap_rows.c.batch_id,
    publication_market_cap_rows.c.source,
)
Index(
    "ix_publication_market_cap_rows_market_cap_batch_security",
    publication_market_cap_rows.c.market_cap_id,
    publication_market_cap_rows.c.batch_id,
    publication_market_cap_rows.c.security_id,
)

current_publication = Table(
    "current_publication",
    metadata,
    Column("channel", String(32), primary_key=True),
    Column(
        "publication_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.publications.id"),
        nullable=False,
    ),
    Column("promoted_at", DateTime(timezone=True), nullable=False),
)

# Supporting indexes for referencing FKs and per-source health lookups.
Index(
    "ix_publication_sources_run_batch_source",
    publication_sources.c.run_id,
    publication_sources.c.batch_id,
    publication_sources.c.source,
)
Index(
    "ix_run_dependencies_batch_source",
    run_dependencies.c.batch_id,
    run_dependencies.c.source,
)
Index(
    "ix_publication_market_caps_observation_security",
    publication_market_caps.c.observation_id,
    publication_market_caps.c.security_id,
)
Index(
    "ix_ingestion_runs_source_started",
    ingestion_runs.c.source,
    ingestion_runs.c.started_at.desc(),
    ingestion_runs.c.id.desc(),
)

# GDELT macro events (src/services/gdelt). Independent of SOURCES and publications.
gdelt_theme_days = Table(
    "gdelt_theme_days",
    metadata,
    _id(),
    Column("theme", String(64), nullable=False),
    Column("day", Date, nullable=False),
    Column("article_count", Integer, nullable=False),
    Column("total_articles", BigInteger),
    Column("avg_tone", Numeric(8, 4)),
    Column("retrieved_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("theme", "day"),
    CheckConstraint("article_count >= 0", name="article_count"),
)

gdelt_articles = Table(
    "gdelt_articles",
    metadata,
    _id(),
    Column("theme", String(64), nullable=False),
    Column("day", Date, nullable=False),
    Column("url", Text, nullable=False),
    Column("title", Text),
    Column("domain", String(255)),
    Column("seen_at", DateTime(timezone=True)),
    UniqueConstraint("theme", "day", "url"),
)

gdelt_spike_checks = Table(
    "gdelt_spike_checks",
    metadata,
    _id(),
    Column("theme", String(64), nullable=False),
    Column("day", Date, nullable=False),
    Column("article_count", Integer, nullable=False),
    Column("baseline_median", Numeric(12, 2), nullable=False),
    Column("ratio", Numeric(10, 2)),
    Column("is_spike", Boolean, nullable=False),
    Column("checked_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("theme", "day"),
)

# Accumulated Congress history (src/pipeline/congress_history.py). Append-only:
# each row is one distinct trade first seen in a live Congress batch.
congress_trade_history = Table(
    "congress_trade_history",
    metadata,
    Column("history_id", BigInteger, Identity(always=True), primary_key=True),
    Column("trade_key", String(64), nullable=False),
    Column("occurrence", Integer, nullable=False),
    _security_fk(),
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
    Column(
        "first_seen_batch_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.source_batches.id"),
        nullable=False,
    ),
    UniqueConstraint("trade_key", "occurrence"),
    CheckConstraint("trade_key ~ '^[0-9a-f]{64}$'", name="trade_key_format"),
    CheckConstraint("occurrence >= 1", name="occurrence"),
)
Index("ix_congress_trade_history_security_id", congress_trade_history.c.security_id)
Index(
    "ix_congress_trade_history_chamber_disclosure",
    congress_trade_history.c.chamber,
    congress_trade_history.c.disclosure_date,
)
Index(
    "ix_congress_trade_history_first_seen_batch_id",
    congress_trade_history.c.first_seen_batch_id,
)

# A full free page that shared no row with stored history: trades may be missing.
congress_history_gaps = Table(
    "congress_history_gaps",
    metadata,
    _id(),
    Column("chamber", String(16), nullable=False),
    Column("newest_stored_date", Date, nullable=False),
    Column("oldest_seen_date", Date, nullable=False),
    Column("gap_days", Integer, nullable=False),
    Column(
        "detected_batch_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.source_batches.id"),
        nullable=False,
    ),
    Column(
        "detected_run_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.ingestion_runs.id"),
        nullable=False,
    ),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    UniqueConstraint("detected_batch_id", "chamber"),
    CheckConstraint("gap_days >= 0", name="gap_days"),
)
Index(
    "ix_congress_history_gaps_detected_run_id", congress_history_gaps.c.detected_run_id
)

# The history a publication ranked: every row with history_id <= through.
publication_history_watermarks = Table(
    "publication_history_watermarks",
    metadata,
    Column(
        "publication_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.publications.id"),
        primary_key=True,
    ),
    Column("source", String(32), primary_key=True),
    Column("through_history_id", BigInteger, nullable=False),
    CheckConstraint("source = 'congress_trades'", name="source"),
    CheckConstraint("through_history_id >= 0", name="through_history_id"),
)

# Pattern evaluation (docs/work-items/pattern-evaluation-design.md): SEC EDGAR
# annual line items per security, and the derived fundamentals score.
FUNDAMENTAL_LINE_ITEMS = (
    "revenue",
    "revenue_net_of_interest",
    "gross_profit",
    "cost_of_revenue",
    "operating_income",
    "pretax_income",
    "net_income",
    "operating_cash_flow",
    "assets",
    "current_assets",
    "current_liabilities",
    "liabilities",
    "long_term_debt",
    "equity",
    "diluted_shares",
    "noninterest_expense",
)
FUNDAMENTAL_SCORE_MODELS = ("general", "bank")
FUNDAMENTAL_MISSING_CODES = (
    "no_revenue",
    "not_enough_data",
    "insufficient_history",
)

fundamentals = Table(
    "fundamentals",
    metadata,
    _id(),
    _batch_fk(),
    _security_fk(),
    Column("row_number", Integer, nullable=False),
    Column("fiscal_year_end", Date, nullable=False),
    Column("line_item", String(32), nullable=False),
    Column("value", Numeric(24, 2), nullable=False),  # signed: losses are negative
    Column("unit", String(8), nullable=False),  # ISO currency, or "shares"
    Column("concept", String(160), nullable=False),  # the XBRL concept it came from
    Column("form", String(8), nullable=False),
    Column("filed", Date, nullable=False),
    UniqueConstraint("batch_id", "row_number"),
    UniqueConstraint("batch_id", "security_id", "fiscal_year_end", "line_item"),
    CheckConstraint("row_number >= 1", name="row_number"),
    CheckConstraint(_in("line_item", FUNDAMENTAL_LINE_ITEMS), name="line_item"),
    CheckConstraint("unit ~ '^([A-Z]{3}|shares)$'", name="unit"),
)
Index("ix_fundamentals_security_id", fundamentals.c.security_id)

fundamental_scores = Table(
    "fundamental_scores",
    metadata,
    _id(),
    _batch_fk(),
    _security_fk(),
    Column("fiscal_year_end", Date),
    Column("model", String(8)),
    Column("score", Numeric(4, 1)),  # NULL: not scored; see missing
    Column("missing", ARRAY(String(32)), nullable=False),
    Column("revenue_growth_3y", Numeric(16, 6)),
    Column("operating_margin", Numeric(16, 6)),
    Column("operating_margin_trend", Numeric(16, 6)),
    Column("return_on_equity", Numeric(16, 6)),
    Column("efficiency_ratio", Numeric(16, 6)),
    Column("profit_source", String(24)),
    Column("gross_margin", Numeric(16, 6)),
    Column("piotroski_score", SmallInteger),
    Column("piotroski_signals", SmallInteger, nullable=False),
    Column("score_version", String(32), nullable=False),
    UniqueConstraint("batch_id", "security_id"),
    CheckConstraint(
        f"model IS NULL OR {_in('model', FUNDAMENTAL_SCORE_MODELS)}", name="model"
    ),
    CheckConstraint("score IS NULL OR (score >= 0 AND score <= 100)", name="score"),
    CheckConstraint(
        "missing <@ ARRAY["
        + ", ".join(f"'{c}'" for c in FUNDAMENTAL_MISSING_CODES)
        + "]::varchar[]",
        name="missing_codes",
    ),
    CheckConstraint(
        "piotroski_signals BETWEEN 0 AND 9 AND (piotroski_score IS NULL OR "
        "piotroski_score BETWEEN 0 AND piotroski_signals)",
        name="piotroski",
    ),
)
Index("ix_fundamental_scores_security_id", fundamental_scores.c.security_id)

SOURCE_TABLES.update(fundamentals=fundamentals, fundamental_scores=fundamental_scores)

# S1-03 news ingestion (docs/work-items/news-ingestion-design-note.md). A batch holds
# only the (security, article) pairs first seen in that run, so the unique
# (security_id, finnhub_id) constraint makes a duplicate impossible across runs.
NEWS_CONTENT_RETRIEVED = ("headline_and_summary", "headline_only")
NEWS_MATCH_RULES = ("ticker", "name")
NEWS_COVERAGE_STATUSES = ("covered", "no_coverage", "error")

news_articles = Table(
    "news_articles",
    metadata,
    _id(),
    _batch_fk(),
    _security_fk(),
    Column("row_number", Integer, nullable=False),
    Column("finnhub_id", BigInteger, nullable=False),
    Column("url", String(2048), nullable=False),
    Column("headline", String(500), nullable=False),
    Column("outlet", String(100), nullable=False),
    Column("published_at", DateTime(timezone=True), nullable=False),
    Column("content_retrieved", String(24), nullable=False),
    Column("match_rule", String(8), nullable=False),
    Column("content_hash", String(64), nullable=False),
    UniqueConstraint("batch_id", "row_number"),
    UniqueConstraint("security_id", "finnhub_id"),
    CheckConstraint("row_number >= 1", name="row_number"),
    CheckConstraint("finnhub_id > 0", name="finnhub_id"),
    CheckConstraint("url ~ '^https?://'", name="url_scheme"),
    CheckConstraint(
        _in("content_retrieved", NEWS_CONTENT_RETRIEVED), name="content_retrieved"
    ),
    CheckConstraint(_in("match_rule", NEWS_MATCH_RULES), name="match_rule"),
    CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash"),
)
Index("ix_news_articles_published_at", news_articles.c.published_at)

# One row per (run, security): the article counts, or why there were none.
news_coverage = Table(
    "news_coverage",
    metadata,
    _id(),
    Column(
        "run_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.ingestion_runs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    _security_fk(),
    Column("status", String(16), nullable=False),
    Column("articles_found", Integer, nullable=False),
    Column("articles_new", Integer, nullable=False),
    Column("error_code", String(64)),
    UniqueConstraint("run_id", "security_id"),
    CheckConstraint(_in("status", NEWS_COVERAGE_STATUSES), name="status"),
    CheckConstraint(
        "articles_found >= 0 AND articles_new >= 0 AND articles_new <= articles_found",
        name="counts",
    ),
    CheckConstraint("(status = 'error') = (error_code IS NOT NULL)", name="error_code"),
    CheckConstraint(
        "status <> 'no_coverage' OR articles_found = 0", name="no_coverage_empty"
    ),
)
Index("ix_news_coverage_security_id", news_coverage.c.security_id)

SOURCE_TABLES.update(news_articles=news_articles)

# Sprint 2 scores (output/sprint-2/s2-stories-plan.md §3.1, §4). The API reads a
# batch only through current_scores, like current_publication.
SCORE_COMPONENTS = ("fundamentals", "sentiment", "event_exposure")  # fixed order
SCORE_STATUSES = ("ok", "unavailable")
SCORE_UNAVAILABLE_REASONS = ("no_fundamentals", "only_fundamentals")
EXPLANATION_STATUSES = ("ok", "not_recommended", "unavailable")
EXPLANATION_REASONS = ("insufficient_supporting_data", "score_unavailable")
EXPLANATION_MAX_CLAIMS = 3

# The sentiment contract (issue #76): one row per (batch, security); no row, no coverage.
ticker_sentiment = Table(
    "ticker_sentiment",
    metadata,
    _id(),
    _batch_fk(),
    _security_fk(),
    Column("row_number", Integer, nullable=False),
    Column("value", Numeric(5, 4), nullable=False),  # -1..1
    Column("article_count", Integer, nullable=False),
    Column("window_start", Date, nullable=False),
    Column("window_end", Date, nullable=False),  # the component's as-of
    Column("model_version", String(64), nullable=False),
    Column("source_weights_version", String(32), nullable=False),
    Column("method", String(32), nullable=False),
    Column("rollup_version", String(32), nullable=False),
    UniqueConstraint("batch_id", "row_number"),
    UniqueConstraint("batch_id", "security_id"),
    CheckConstraint("row_number >= 1", name="row_number"),
    CheckConstraint("value BETWEEN -1 AND 1", name="value"),
    CheckConstraint("article_count >= 1", name="article_count"),
    CheckConstraint("window_start <= window_end", name="window"),
)
Index("ix_ticker_sentiment_security_id", ticker_sentiment.c.security_id)


def _score_component(name: str) -> list[Any]:
    """One component's breakdown; record_id has no FK (its table follows from the component)."""
    return [
        Column(f"{name}_value", Numeric(4, 1)),
        Column(f"{name}_nominal_weight", Numeric(5, 4), nullable=False),
        Column(f"{name}_weight", Numeric(5, 4)),  # effective, after redistribution
        Column(f"{name}_contribution", Numeric(5, 2)),
        Column(f"{name}_input_as_of", Date),
        Column(f"{name}_input_version", String(32)),
        Column(f"{name}_record_id", BigInteger),
        CheckConstraint(
            f"({name}_value IS NULL OR {name}_value BETWEEN 0 AND 100)"
            f" AND {name}_nominal_weight BETWEEN 0 AND 1"
            f" AND ({name}_weight IS NULL OR {name}_weight BETWEEN 0 AND 1)"
            f" AND ({name}_contribution IS NULL OR {name}_contribution BETWEEN 0 AND 100)",
            name=name,
        ),
        # A component is listed as missing exactly when it has no value.
        CheckConstraint(
            f"({name}_value IS NULL) = ('{name}' = ANY (missing))",
            name=f"{name}_missing",
        ),
    ]


company_scores = Table(
    "company_scores",
    metadata,
    _id(),
    _batch_fk(),
    _security_fk(),
    Column("status", String(16), nullable=False),
    Column("unavailable_reason", String(32)),
    Column("score", Numeric(4, 1)),  # NULL: unavailable; see unavailable_reason
    Column("score_version", String(32), nullable=False),
    Column("missing", ARRAY(String(32)), nullable=False),
    Column("recommended", Boolean, nullable=False),
    # The watchlist publication `recommended` was judged against.
    Column(
        "watchlist_publication_id", BigInteger, ForeignKey(f"{SCHEMA}.publications.id")
    ),
    *(part for name in SCORE_COMPONENTS for part in _score_component(name)),
    UniqueConstraint("batch_id", "security_id"),
    CheckConstraint(_in("status", SCORE_STATUSES), name="status"),
    CheckConstraint("score IS NULL OR score BETWEEN 0 AND 100", name="score"),
    CheckConstraint("(status = 'ok') = (score IS NOT NULL)", name="status_score"),
    CheckConstraint(
        "(status = 'unavailable') = (unavailable_reason IS NOT NULL)",
        name="status_reason",
    ),
    CheckConstraint(
        "unavailable_reason IS NULL OR "
        + _in("unavailable_reason", SCORE_UNAVAILABLE_REASONS),
        name="unavailable_reason",
    ),
    CheckConstraint(
        "missing <@ ARRAY["
        + ", ".join(f"'{c}'" for c in SCORE_COMPONENTS)
        + "]::varchar[]",
        name="missing_components",
    ),
    CheckConstraint(
        "NOT recommended OR (status = 'ok' AND score >= 60"
        " AND watchlist_publication_id IS NOT NULL)",
        name="recommended",
    ),
)
Index("ix_company_scores_security_id", company_scores.c.security_id)

score_explanations = Table(
    "score_explanations",
    metadata,
    _id(),
    _batch_fk(),
    _security_fk(),
    Column(
        "company_score_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.company_scores.id"),
        nullable=False,
    ),
    Column("status", String(16), nullable=False),
    Column("reason", String(32)),
    Column("template_version", String(32), nullable=False),
    Column("claims", JSONB, nullable=False),
    Column("input_digest", String(64), nullable=False),
    UniqueConstraint("batch_id", "security_id"),
    CheckConstraint(_in("status", EXPLANATION_STATUSES), name="status"),
    CheckConstraint(
        "(status = 'unavailable') = (reason IS NOT NULL)", name="status_reason"
    ),
    CheckConstraint(
        "reason IS NULL OR " + _in("reason", EXPLANATION_REASONS), name="reason"
    ),
    # CASE, not AND: jsonb_array_length raises on a non-array instead of failing the check.
    CheckConstraint(
        "CASE WHEN jsonb_typeof(claims) = 'array' THEN jsonb_array_length(claims) <= "
        f"{EXPLANATION_MAX_CLAIMS} ELSE false END",
        name="claims",
    ),
    CheckConstraint("(status = 'ok') = (claims <> '[]'::jsonb)", name="status_claims"),
    CheckConstraint("input_digest ~ '^[0-9a-f]{64}$'", name="input_digest"),
)
Index("ix_score_explanations_security_id", score_explanations.c.security_id)
Index("ix_score_explanations_company_score_id", score_explanations.c.company_score_id)

SOURCE_TABLES.update(
    ticker_sentiment=ticker_sentiment,
    company_scores=company_scores,
    score_explanations=score_explanations,
)

# The scores batch each channel serves; the API reads company_scores only through it.
current_scores = Table(
    "current_scores",
    metadata,
    Column("channel", String(32), primary_key=True),
    Column(
        "batch_id",
        BigInteger,
        ForeignKey(f"{SCHEMA}.source_batches.id"),
        nullable=False,
    ),
    Column("promoted_at", DateTime(timezone=True), nullable=False),
)
