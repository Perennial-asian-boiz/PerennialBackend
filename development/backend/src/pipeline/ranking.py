"""Versioned, deterministic watchlist calculation. No I/O or current clock.

Keep released implementations addressable: a change to ranking semantics gets
a new version, not an edit to v1. Input snapshots are selected by the caller.
"""

import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
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
# v5 is v4 without share_price in the ARK signal: no source has a licence to
# display a price (decision 95, L5 a).
# v6 is v5 labelled for SEC's public float (decision 112): a record carries
# public_float and its public_float_as_of instead of market_cap, weak values
# go in size_flags (any warnings entry makes the dashboard row "Watch"), and
# the output names its size_basis. Buckets and order are v5's.
VERSION = "watchlist-v5"
VERSIONS = (
    "watchlist-v1",
    "watchlist-v2",
    "watchlist-v3",
    "watchlist-v4",
    "watchlist-v5",
    "watchlist-v6",
)
# Versions with v4's candidate cap and its after-disclosure filter.
_CAPPED = ("watchlist-v4", "watchlist-v5", "watchlist-v6")
_ARK_KEYS = ("funds", "fund_count", "total_weight")
# The size provider each labelled version ranks, and so the version a pinned
# publication of that provider's values must use. Any other provider's values
# are market caps, ranked under VERSION.
SIZE_BASIS = {"watchlist-v6": "sec_public_float"}
# A public float older than this is flagged size_estimate_stale (B2).
SIZE_STALE_AGE = timedelta(days=548)
DEFAULT_CONFIG = {
    "market_cap_threshold": "10000000000",
    "max_congress_candidates": 100,
}


def version_for(provider: str | None) -> str:
    """The ranking version that labels a size provider's values correctly."""
    return next((v for v, p in SIZE_BASIS.items() if p == provider), VERSION)


def candidate_symbols(
    rows: Mapping[str, Sequence[dict[str, Any]]],
    *,
    version: str = VERSION,
    config: Mapping[str, Any] | None = None,
) -> list[str]:
    """
    Symbols a publication evaluates (and fetches market caps for).

    v1-v3: every Congress purchase symbol plus every ARK holding.
    v4-v6: the top `max_congress_candidates` Congress purchase symbols, by
    distinct buyers, then buy count, then most recent transaction, then
    symbol, counting only purchases with a plausible date; plus every ARK
    holding.
    """
    ark = {r["symbol"] for r in rows["ark_holdings"]}
    if version not in _CAPPED:
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
    size_as_of: Mapping[str, datetime | None] | None = None,
) -> dict[str, Any]:
    """
    size_as_of is each value's measurement time (market_caps.observed_at). v6
    publishes it as public_float_as_of and requires it for every value.
    """
    if version not in VERSIONS:
        raise ValueError("unsupported ranking version")
    basis = SIZE_BASIS.get(version)
    config = dict(DEFAULT_CONFIG if config is None else config)
    threshold = Decimal(config["market_cap_threshold"])
    congress: dict[str, list[dict[str, Any]]] = defaultdict(list)
    insiders: dict[str, list[dict[str, Any]]] = defaultdict(list)
    congress_rows = rows["congress_trades"]
    if version != "watchlist-v1":
        congress_rows = _cross_provider_purchases(congress_rows)
    if version in _CAPPED:
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
            keys = (
                _ARK_KEYS
                if version in ("watchlist-v5", "watchlist-v6")
                else (*_ARK_KEYS, "share_price")
            )
            ars = {k: a[k] for k in keys}
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
        if basis is None:
            size: dict[str, Any] = {"market_cap": cap}
            warnings = ["market_cap_unavailable"] if cap is None else []
        else:
            size = _labelled_size(cap, (size_as_of or {}).get(symbol), generated_at)
            warnings = []  # size_flags carries v6's size problems
        buckets[bucket].append(
            {
                "ticker": symbol,
                "bucket": bucket,
                "rank": None,
                **size,
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
                "warnings": warnings,
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
                **({"size_basis": basis} if basis else {}),
                **buckets,
                "errors": [],
            }
        ),
    )


def _labelled_size(
    value: Decimal | None, observed_at: datetime | None, generated_at: datetime
) -> dict[str, Any]:
    """v6's size fields for one record."""
    if value is None:
        flags = ["size_unavailable"]
        day = None
    else:
        if observed_at is None:
            raise ValueError("a public float needs its as-of date")
        day = observed_at.astimezone(timezone.utc).date()
        age = generated_at.astimezone(timezone.utc).date() - day
        flags = ["size_estimate_stale"] if age > SIZE_STALE_AGE else []
    return {"public_float": value, "public_float_as_of": day, "size_flags": flags}
