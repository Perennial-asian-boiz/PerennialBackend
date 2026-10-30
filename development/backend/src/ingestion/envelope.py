"""
Fetcher envelope metadata kept in the batch payload.

Each fetcher wraps its records in a top-level object (source name, endpoints,
fund list, counts, fetched_at, ...). The payload keeps an allowlisted, sanitized
copy of it for inspection. It is NOT part of batch identity: a reused batch
keeps the envelope of the run that first imported it.

Sanitization:
  * Only the keys listed in ENVELOPE_KEYS are kept; everything else, including
    free-text notes and derived per-ticker summaries, is dropped.
  * Strings pass through redact() (URL credentials and query strings removed,
    secret-looking pairs masked) and are capped at 300 characters.
  * Values must be scalars or lists of up to 20 scalars; anything else is dropped.
  * short_interest `errors` text is never stored; only its count (`errors_count`).
"""

from typing import Any, Dict, Optional

from src.ingestion.redaction import redact

ENVELOPE_KEYS = {
    "congress_trades": (
        "source",
        "endpoints",
        "fetched_at",
        "total_trades",
        "chambers",
        "house_source",
        "house_reports",
        "house_reports_scanned",
    ),
    "ark_holdings": (
        "source",
        "funds_tracked",
        "fetched_at",
        "total_tickers",
        "multi_fund_tickers",
        "single_fund_tickers",
        # Funds left out for being older than publish's limit ("FUND:YYYY-MM-DD").
        "excluded_stale_funds",
    ),
    "insider_trades": (
        "source",
        "fetched_at",
        "lookback_days",
        "min_buy_value",
        "total_buys",
        "tickers_with_buys",
        "unsupported_tickers",
    ),
    "short_interest": (
        "schema_version",
        "source",
        "fetched_at",
        "total_records",
        "unsupported_tickers",
    ),
    # GDELT: missing timeline days are coverage gaps ("THEME:YYYY-MM-DD"), never
    # zero rows; lists keep the first MAX_LIST_ITEMS, the count is exact.
    "gdelt_theme_volume": (
        "source",
        "target_day",
        "window_days",
        "themes",
        "missing_days",
        "missing_days_count",
    ),
    "gdelt_theme_articles": ("source", "target_day", "themes", "skipped_articles"),
    "gdelt_spike_results": (
        "rule_version",
        "spike_ratio",
        "baseline_days",
        "min_baseline_days",
        "min_articles",
        "input_batch_id",
    ),
    # provider is the adapter name; it is also in every row, which is hashed.
    "market_caps": ("provider", "fetched_at", "total_symbols", "symbols_with_value"),
}
MAX_LIST_ITEMS = 20


def _scalar(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if value == value and abs(value) != float("inf") else None
    if isinstance(value, str):
        return redact(value, 300)
    raise TypeError


def sanitize_envelope(
    source: str, envelope: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    if not isinstance(envelope, dict):
        return {}
    clean: Dict[str, Any] = {}
    for key in ENVELOPE_KEYS[source]:
        if key not in envelope:
            continue
        value = envelope[key]
        try:
            if isinstance(value, list):
                clean[key] = [_scalar(v) for v in value[:MAX_LIST_ITEMS]]
            else:
                clean[key] = _scalar(value)
        except TypeError:
            continue
    if source == "short_interest" and isinstance(envelope.get("errors"), list):
        clean["errors_count"] = len(envelope["errors"])
    return clean


# Pattern evaluation (src/services/fundamentals). Lists keep the first
# MAX_LIST_ITEMS; the *_count keys are exact.
ENVELOPE_KEYS.update(
    fundamentals=(
        "source",
        "fiscal_years",
        "universe",
        "with_data",
        "no_cik",
        "no_cik_count",
        "no_annual_data",
        "no_annual_data_count",
    ),
    fundamental_scores=(
        "score_version",
        "general_limits",
        "bank_limits",
        "min_measures",
        "max_margin",
        "input_batch_id",
    ),
)

# S1-03 news ingestion. Lists keep the first MAX_LIST_ITEMS; counts are exact.
ENVELOPE_KEYS.update(
    news_articles=(
        "source",
        "window_days",
        "universe",
        "covered",
        "no_coverage",
        "no_coverage_count",
        "errors",
        "errors_count",
        "articles_found",
        "articles_new",
        "match_rule_version",
    ),
)
# S1-05 source credibility weighting (ticker_sentiment's keys are below).
ENVELOPE_KEYS.update(
    social_posts=(
        "source",
        "window_days",
        "fresh",
        "stale",
        "missing",
        "posts",
        "skipped_tickers",
        "skipped_tickers_count",
    ),
    article_sentiment=("model", "input_batch_id", "articles_scored"),
)

# Sprint 2 scores (derived runs). company_scores keeps score-v1's envelope
# (score_version and weights as "component:weight" strings) and the input batches.
ENVELOPE_KEYS.update(
    ticker_sentiment=(  # PR #85's nine keys (decision 108)
        "weights_version",
        "window_days",
        "universe",
        "scored",
        "no_data_count",
        "news_used",
        "social_used",
        "social_batch_fresh",
        "publishers_unknown",
    ),
    company_scores=(
        "score_version",
        "weights",
        "fundamental_scores_batch_id",
        "ticker_sentiment_batch_id",
        "watchlist_publication_id",
        "universe",
        "scored",
        "unavailable_count",
    ),
    score_explanations=(
        "template_version",
        "company_scores_batch_id",
        "universe",
        "explained",
        "carried_forward_count",
    ),
)
