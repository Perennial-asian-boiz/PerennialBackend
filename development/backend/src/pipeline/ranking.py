"""Versioned, deterministic watchlist calculation. No I/O or current clock.

Keep released implementations addressable: a change to ranking semantics gets
a new version, not an edit to v1. Input snapshots are selected by the caller.
"""

import json
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence, cast

# v3 ranks accumulated Congress history (2-year window) instead of one batch;
# the scoring itself is unchanged from v2.
# v4 is v3 with Congress candidates capped at max_congress_candidates. With
# complete House Clerk data every purchased ticker would otherwise be a
# candidate (~650 a year), and each candidate costs one FMP market-cap request
# against a 250-a-day free plan. v4 also ignores purchases dated after their
# own disclosure: a trade cannot be reported before it happens, so the date is
# a filer's typo (e.g. a 2025 trade written as 2026), and it would otherwise
# become the ticker's "most recent" buy. Stored rows keep the date as filed.
VERSION = "watchlist-v4"
VERSIONS = ("watchlist-v1", "watchlist-v2", "watchlist-v3", "watchlist-v4")
DEFAULT_CONFIG = {
    "market_cap_threshold": "10000000000",
    "max_congress_candidates": 100,
}


def candidate_symbols(
    rows: Mapping[str, Sequence[dict[str, Any]]],
    *,
    version: str = VERSION,
    config: Mapping[str, Any] | None = None,
) -> list[str]:
    """
    Symbols a publication evaluates (and fetches market caps for).

    v1-v3: every Congress purchase symbol plus every ARK holding.
    v4: the top `max_congress_candidates` Congress purchase symbols, by
    distinct buyers, then buy count, then most recent transaction, then
    symbol, counting only purchases with a plausible date; plus every ARK
    holding.
    """
    ark = {r["symbol"] for r in rows["ark_holdings"]}
    if version != "watchlist-v4":
        return sorted(
            {
                r["symbol"]
                for r in rows["congress_trades"]
                if "purchase" in r["trade_type"].lower()
            }
            | ark
        )
    settings = dict(DEFAULT_CONFIG if config is None else config)
    limit = int(
        settings.get(
            "max_congress_candidates", DEFAULT_CONFIG["max_congress_candidates"]
        )
    )
    purchases: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _cross_provider_purchases(rows["congress_trades"]):
        if not dated_after_disclosure(row):
            purchases[row["symbol"]].append(row)

    def strength(symbol: str) -> tuple[Any, ...]:
        trades = purchases[symbol]
        latest = max(r["transaction_date"] for r in trades)
        return (
            -len({r["politician_name"] for r in trades}),
            -len(trades),
            -latest.toordinal(),
            symbol,
        )

    return sorted(set(sorted(purchases, key=strength)[:limit]) | ark)


def dated_after_disclosure(row: Mapping[str, Any]) -> bool:
    """True when a trade's own date is later than the date it was disclosed."""
    traded, disclosed = row.get("transaction_date"), row.get("disclosure_date")
    return traded is not None and disclosed is not None and traded > disclosed


def json_ready(value: Any) -> Any:
    def encode(obj: Any) -> float | str:
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.astimezone(timezone.utc).isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        raise TypeError("unsupported output value")

    return json.loads(json.dumps(value, default=encode, allow_nan=False))


def _cross_provider_purchases(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the largest provider group per trade key; raw snapshots are untouched."""
    groups: dict[tuple[Any, ...], dict[str | None, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        if "purchase" not in row["trade_type"].lower():
            continue
        key = (
            row["politician_name"].strip().casefold(),
            row["symbol"],
            row["transaction_date"],
            row["trade_type"].strip().casefold(),
        )
        groups[key][row.get("data_source")].append(row)
    kept = []
    for providers in groups.values():
        # NULL is its own provider. Canonical row_number breaks equal-size ties.
        chosen = min(
            providers.values(),
            key=lambda group: (-len(group), min(r["row_number"] for r in group)),
        )
        kept.extend(chosen)
    return kept


def calculate(
    rows: Mapping[str, Sequence[dict[str, Any]]],
    market_caps: Mapping[str, Decimal | None],
    generated_at: datetime,
    config: Mapping[str, Any] | None = None,
    *,
    version: str = VERSION,
) -> dict[str, Any]:
    if version not in VERSIONS:
        raise ValueError("unsupported ranking version")
    config = dict(DEFAULT_CONFIG if config is None else config)
    threshold = Decimal(config["market_cap_threshold"])
    congress: dict[str, list[dict[str, Any]]] = defaultdict(list)
    insiders: dict[str, list[dict[str, Any]]] = defaultdict(list)
    congress_rows = rows["congress_trades"]
    if version != "watchlist-v1":
        congress_rows = _cross_provider_purchases(congress_rows)
    if version == "watchlist-v4":
        congress_rows = [r for r in congress_rows if not dated_after_disclosure(r)]
    for row in congress_rows:
        if "purchase" in row["trade_type"].lower():
            congress[row["symbol"]].append(row)
    for row in rows["insider_trades"]:
        if row["transaction_type"].lower() == "purchase":
            insiders[row["symbol"]].append(row)
    ark = {r["symbol"]: r for r in rows["ark_holdings"]}
    short = {r["symbol"]: r for r in rows["short_interest"]}
    buckets: dict[str, list[dict[str, Any]]] = {
        "popular_stable": [],
        "affordable_growing": [],
        "unresolved": [],
    }
    for symbol in candidate_symbols(rows, version=version, config=config):
        c, i, a, si = (
            congress.get(symbol),
            insiders.get(symbol),
            ark.get(symbol),
            short.get(symbol),
        )
        cs = ins = ars = sis = None
        if c:
            most_recent = max(
                c, key=lambda r: (r["transaction_date"], -r["row_number"])
            )
            disclosures = [r["disclosure_date"] for r in c if r.get("disclosure_date")]
            cs = {
                "buy_count": len(c),
                "distinct_buyer_count": len({r["politician_name"] for r in c}),
                "most_recent_transaction_date": most_recent["transaction_date"],
                "most_recent_disclosure_date": max(disclosures)
                if disclosures
                else most_recent["transaction_date"],
                "amount_range": most_recent.get("amount_label"),
            }
        if i:
            ins = {
                "buy_count": len(i),
                "distinct_insider_count": len({r["insider_name"] for r in i}),
                "total_value": sum((r["value"] or Decimal(0) for r in i), Decimal(0)),
                "most_recent_buy": max(r["transaction_date"] for r in i),
            }
        if a:
            ars = {
                k: a[k] for k in ("funds", "fund_count", "total_weight", "share_price")
            }
        if si:
            sis = {
                "short_position_shares": si["short_interest_shares"],
                "average_daily_volume": si["average_daily_volume"],
                "days_to_cover": si["days_to_cover"],
                "settlement_date": si["settlement_date"],
            }
        cap = market_caps[symbol]
        bucket = (
            "unresolved"
            if cap is None
            else ("popular_stable" if cap > threshold else "affordable_growing")
        )
        buckets[bucket].append(
            {
                "ticker": symbol,
                "bucket": bucket,
                "rank": None,
                "market_cap": cap,
                "signals": {
                    "congress": cs,
                    "insider": ins,
                    "ark": ars,
                    "short_interest": sis,
                },
                "source_dates": {
                    "congress": cs["most_recent_disclosure_date"] if cs else None,
                    "insider": ins["most_recent_buy"] if ins else None,
                    "ark": config.get("source_as_of", {}).get("ark_holdings")
                    if a
                    else None,
                    "short_interest": sis["settlement_date"] if sis else None,
                },
                "warnings": ["market_cap_unavailable"] if cap is None else [],
            }
        )

    def popular(r: dict[str, Any]) -> tuple[Any, ...]:
        c, i = r["signals"]["congress"] or {}, r["signals"]["insider"] or {}
        day = c.get("most_recent_transaction_date")
        return (
            -c.get("distinct_buyer_count", 0),
            -c.get("buy_count", 0),
            -day.toordinal() if day else 0,
            -i.get("distinct_insider_count", 0),
            -i.get("total_value", 0),
            r["ticker"],
        )

    def growing(r: dict[str, Any]) -> tuple[Any, ...]:
        a, i = r["signals"]["ark"] or {}, r["signals"]["insider"] or {}
        return (
            -a.get("fund_count", 0),
            -a.get("total_weight", 0),
            -i.get("distinct_insider_count", 0),
            -i.get("total_value", 0),
            r["ticker"],
        )

    buckets["popular_stable"].sort(key=popular)
    buckets["affordable_growing"].sort(key=growing)
    for name in ("popular_stable", "affordable_growing"):
        for position, record in enumerate(buckets[name], 1):
            record["rank"] = position
    return cast(
        dict[str, Any],
        json_ready(
            {
                "schema_version": "1.0",
                "ranking_version": version,
                "generated_at": generated_at,
                **buckets,
                "errors": [],
            }
        ),
    )
