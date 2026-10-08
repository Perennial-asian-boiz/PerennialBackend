"""Versioned, deterministic watchlist calculation. No I/O or current clock.

Keep released implementations addressable: a change to ranking semantics gets
a new version, not an edit to v1. Input snapshots are selected by the caller.
"""
import json
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal

VERSION = "watchlist-v2"
DEFAULT_CONFIG = {"market_cap_threshold": "10000000000"}


def candidate_symbols(rows):
    return sorted({r["symbol"] for r in rows["congress_trades"]
                   if "purchase" in r["trade_type"].lower()} |
                  {r["symbol"] for r in rows["ark_holdings"]})


def json_ready(value):
    def encode(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.astimezone(timezone.utc).isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        raise TypeError("unsupported output value")
    return json.loads(json.dumps(value, default=encode, allow_nan=False))


def _cross_provider_purchases(rows):
    """Keep the largest provider group per trade key; raw snapshots are untouched."""
    groups = defaultdict(lambda: defaultdict(list))
    for row in rows:
        if "purchase" not in row["trade_type"].lower():
            continue
        key = (row["politician_name"].strip().casefold(), row["symbol"],
               row["transaction_date"], row["trade_type"].strip().casefold())
        groups[key][row.get("data_source")].append(row)
    kept = []
    for providers in groups.values():
        # NULL is its own provider. Canonical row_number breaks equal-size ties.
        chosen = min(providers.values(),
                     key=lambda group: (-len(group), min(r["row_number"] for r in group)))
        kept.extend(chosen)
    return kept


def calculate(rows, market_caps, generated_at, config=None, *, version=VERSION):
    if version not in ("watchlist-v1", "watchlist-v2"):
        raise ValueError("unsupported ranking version")
    config = dict(DEFAULT_CONFIG if config is None else config)
    threshold = Decimal(config["market_cap_threshold"])
    congress, insiders = defaultdict(list), defaultdict(list)
    congress_rows = rows["congress_trades"]
    if version == "watchlist-v2":
        congress_rows = _cross_provider_purchases(congress_rows)
    for row in congress_rows:
        if "purchase" in row["trade_type"].lower():
            congress[row["symbol"]].append(row)
    for row in rows["insider_trades"]:
        if row["transaction_type"].lower() == "purchase":
            insiders[row["symbol"]].append(row)
    ark = {r["symbol"]: r for r in rows["ark_holdings"]}
    short = {r["symbol"]: r for r in rows["short_interest"]}
    buckets = {"popular_stable": [], "affordable_growing": [], "unresolved": []}
    for symbol in candidate_symbols(rows):
        c, i, a, si = congress.get(symbol), insiders.get(symbol), ark.get(symbol), short.get(symbol)
        cs = ins = ars = sis = None
        if c:
            most_recent = max(c, key=lambda r: (r["transaction_date"], -r["row_number"]))
            disclosures = [r["disclosure_date"] for r in c if r.get("disclosure_date")]
            cs = {"buy_count": len(c), "distinct_buyer_count": len({r["politician_name"] for r in c}),
                  "most_recent_transaction_date": most_recent["transaction_date"],
                  "most_recent_disclosure_date": max(disclosures) if disclosures else most_recent["transaction_date"],
                  "amount_range": most_recent.get("amount_label")}
        if i:
            ins = {"buy_count": len(i), "distinct_insider_count": len({r["insider_name"] for r in i}),
                   "total_value": sum((r["value"] or Decimal(0) for r in i), Decimal(0)),
                   "most_recent_buy": max(r["transaction_date"] for r in i)}
        if a:
            ars = {k: a[k] for k in ("funds", "fund_count", "total_weight", "share_price")}
        if si:
            sis = {"short_position_shares": si["short_interest_shares"],
                   "average_daily_volume": si["average_daily_volume"],
                   "days_to_cover": si["days_to_cover"], "settlement_date": si["settlement_date"]}
        cap = market_caps[symbol]
        bucket = "unresolved" if cap is None else ("popular_stable" if cap > threshold else "affordable_growing")
        buckets[bucket].append({"ticker": symbol, "bucket": bucket, "rank": None, "market_cap": cap,
            "signals": {"congress": cs, "insider": ins, "ark": ars, "short_interest": sis},
            "source_dates": {"congress": cs["most_recent_disclosure_date"] if cs else None,
                             "insider": ins["most_recent_buy"] if ins else None,
                             "ark": config.get("source_as_of", {}).get("ark_holdings") if a else None,
                             "short_interest": sis["settlement_date"] if sis else None},
            "warnings": ["market_cap_unavailable"] if cap is None else []})
    def popular(r):
        c, i = r["signals"]["congress"] or {}, r["signals"]["insider"] or {}
        day = c.get("most_recent_transaction_date")
        return (-c.get("distinct_buyer_count", 0), -c.get("buy_count", 0),
                -day.toordinal() if day else 0, -i.get("distinct_insider_count", 0),
                -i.get("total_value", 0), r["ticker"])
    def growing(r):
        a, i = r["signals"]["ark"] or {}, r["signals"]["insider"] or {}
        return (-a.get("fund_count", 0), -a.get("total_weight", 0),
                -i.get("distinct_insider_count", 0), -i.get("total_value", 0), r["ticker"])
    buckets["popular_stable"].sort(key=popular)
    buckets["affordable_growing"].sort(key=growing)
    for name in ("popular_stable", "affordable_growing"):
        for position, record in enumerate(buckets[name], 1):
            record["rank"] = position
    return json_ready({"schema_version": "1.0", "ranking_version": version,
                       "generated_at": generated_at, **buckets, "errors": []})
