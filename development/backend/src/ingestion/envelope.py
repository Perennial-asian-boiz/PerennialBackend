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
    "congress_trades": ("source", "endpoints", "fetched_at", "total_trades"),
    "ark_holdings": (
        "source", "funds_tracked", "fetched_at", "total_tickers",
        "multi_fund_tickers", "single_fund_tickers",
    ),
    "insider_trades": (
        "source", "fetched_at", "lookback_days", "min_buy_value", "total_buys", "tickers_with_buys",
    ),
    "short_interest": ("schema_version", "source", "fetched_at", "total_records"),
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


def sanitize_envelope(source: str, envelope: Optional[Dict[str, Any]]) -> Dict[str, Any]:
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
