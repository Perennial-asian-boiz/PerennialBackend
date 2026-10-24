"""
Accumulated Congress history on the free FMP plan
(docs/work-items/congress-incremental-design-note.md).

The free plan serves only the 20 latest disclosures per chamber, so each live
Congress batch is an observation of "what was latest in this run". derive()
folds a batch into congress_trade_history, an append-only set of distinct
trades:

  * trade_key is a SHA-256 over the stored trade fields. FMP's latest
    endpoints carry no transaction id, so full content is the only safe identity.
  * occurrence keeps genuine multiplicity. A batch with k identical rows
    guarantees k occurrences of that key; overlapping batches never add more.
  * Appends hold HISTORY_LOCK, and publications read their watermark under it,
    so `history_id <= watermark` is a fixed set and replays byte-identically.

A full page that shares no row with stored history for its chamber is a
possible gap: trades may have scrolled past unseen. It is recorded durably,
never fails collection and never blocks publishing (it degrades instead).

The 2-year ranking window (D13) is applied when reading, measured from the
publication's created_at, so stored history is never trimmed.
"""

import hashlib
import json
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence

from sqlalchemy import func, insert, select, text
from sqlalchemy.engine import Connection

from src.db import models as m
from src.pipeline.demo_guard import LIVE_MODES

HISTORY_LOCK = 170100
WINDOW_YEARS = 2
PAGE_SIZE = 20  # rows the free plan serves per chamber
CHAMBERS = ("Senate", "House")
SOURCE = "congress_trades"

# Stored trade fields that define a distinct trade (everything but per-run ids).
KEY_FIELDS = (
    "security_id",
    "politician_name",
    "chamber",
    "trade_type",
    "transaction_date",
    "disclosure_date",
    "amount_label",
    "asset_description",
    "asset_type",
    "district",
    "source_link",
    "data_source",
)


def trade_key(row: Mapping[str, Any]) -> str:
    canonical = {
        k: (row[k].isoformat() if isinstance(row[k], date) else row[k])
        for k in KEY_FIELDS
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def window_start(generated_at: datetime, years: int = WINDOW_YEARS) -> date:
    """Same calendar day `years` earlier; Feb 29 falls back to Feb 28."""
    day = generated_at.date()
    try:
        return day.replace(year=day.year - years)
    except ValueError:
        return day.replace(year=day.year - years, day=28)


def _lock(conn: Connection) -> None:
    conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": HISTORY_LOCK})


def _batch_rows(conn: Connection, batch_id: int) -> List[Dict[str, Any]]:
    t = m.congress_trades
    return [
        dict(r)
        for r in conn.execute(
            select(*(t.c[k] for k in KEY_FIELDS))
            .where(t.c.batch_id == batch_id)
            .order_by(t.c.row_number)
        ).mappings()
    ]


def derive(
    conn: Connection, run_id: int, modes: Sequence[str] = LIVE_MODES
) -> Dict[str, int]:
    """
    Fold one succeeded Congress run's batch into history. Idempotent:
    deriving the same batch again adds nothing and records no new gap.
    Runs inside the caller's transaction. `modes` follows publication: live
    only, except demo mode (DEMO_MODES) on the guarded demo database.
    """
    run = (
        conn.execute(select(m.ingestion_runs).where(m.ingestion_runs.c.id == run_id))
        .mappings()
        .one()
    )
    if (
        run["source"] != SOURCE
        or run["status"] != "succeeded"
        or run["collection_mode"] not in modes
    ):
        raise ValueError(
            "history derives only from succeeded publishable Congress runs"
        )
    _lock(conn)
    batch_id = run["batch_id"]
    rows = _batch_rows(conn, batch_id)
    keyed: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        keyed.setdefault(trade_key(row), []).append(row)
    h = m.congress_trade_history
    stored = dict(
        conn.execute(
            select(h.c.trade_key, func.max(h.c.occurrence))
            .where(h.c.trade_key.in_(list(keyed)))
            .group_by(h.c.trade_key)
        )
        .tuples()
        .all()
    )
    counts = {"history_added": 0, "possible_gaps": 0, "gap_days": 0}
    served = run["diagnostics"] or {}
    for chamber in CHAMBERS:
        gap = _gap(conn, chamber, served, keyed, stored, batch_id, run_id)
        if gap is not None:
            counts["possible_gaps"] += 1
            counts["gap_days"] = max(counts["gap_days"], gap)
    # One pass in page order, so history ids follow what the provider listed.
    new_rows = []
    emitted: Dict[str, int] = {}
    for row in rows:
        key = trade_key(row)
        occurrence = emitted[key] = emitted.get(key, 0) + 1
        if occurrence > stored.get(key, 0):
            new_rows.append(
                {
                    **row,
                    "trade_key": key,
                    "occurrence": occurrence,
                    "first_seen_batch_id": batch_id,
                }
            )
    if new_rows:
        conn.execute(insert(h), new_rows)
    counts["history_added"] = len(new_rows)
    return counts


def _gap(
    conn: Connection,
    chamber: str,
    served: Mapping[str, Any],
    keyed: Mapping[str, List[Dict[str, Any]]],
    stored: Mapping[str, int],
    batch_id: int,
    run_id: int,
) -> Optional[int]:
    """Record a possible gap for one chamber; return its size in days, or None."""
    rows_served = served.get(f"{chamber.lower()}_rows_served")
    if not isinstance(rows_served, int) or rows_served < PAGE_SIZE:
        return None  # a short page is the whole served list
    h = m.congress_trade_history
    newest = conn.execute(
        select(
            func.max(func.coalesce(h.c.disclosure_date, h.c.transaction_date))
        ).where(h.c.chamber == chamber)
    ).scalar_one()
    if newest is None:
        return None  # cold start, not a gap
    seen = [
        (key, r)
        for key, group in keyed.items()
        for r in group
        if r["chamber"] == chamber
    ]
    if not seen or any(key in stored for key, _ in seen):
        return None
    oldest = min(r["disclosure_date"] or r["transaction_date"] for _, r in seen)
    days: int = max(0, (oldest - newest).days)
    g = m.congress_history_gaps
    exists = conn.execute(
        select(g.c.id).where(g.c.detected_batch_id == batch_id, g.c.chamber == chamber)
    ).scalar_one_or_none()
    if exists is None:
        conn.execute(
            insert(g).values(
                chamber=chamber,
                newest_stored_date=newest,
                oldest_seen_date=oldest,
                gap_days=days,
                detected_batch_id=batch_id,
                detected_run_id=run_id,
            )
        )
    return days


def watermark(conn: Connection) -> int:
    """The newest history id visible under HISTORY_LOCK (0 when empty)."""
    _lock(conn)
    h = m.congress_trade_history
    return int(
        conn.execute(select(func.coalesce(func.max(h.c.history_id), 0))).scalar_one()
    )


def ranking_rows(
    conn: Connection, through: int, generated_at: datetime, years: int = WINDOW_YEARS
) -> List[Dict[str, Any]]:
    """History up to `through`, inside the ranking window, shaped like batch rows."""
    h = m.congress_trade_history
    rows = conn.execute(
        select(m.securities.c.symbol, h)
        .join(m.securities, m.securities.c.id == h.c.security_id)
        .where(
            h.c.history_id <= through,
            h.c.transaction_date >= window_start(generated_at, years),
        )
        .order_by(h.c.history_id)
    ).mappings()
    # history_id is unique and ordered, so it serves as ranking's row_number.
    return [{**dict(r), "row_number": r["history_id"]} for r in rows]


def summary(
    conn: Connection, through: int, generated_at: datetime, years: int = WINDOW_YEARS
) -> Dict[str, Any]:
    """Counts and dates for the audit record; no trade content."""
    h, g = m.congress_trade_history, m.congress_history_gaps
    since = dict(
        conn.execute(
            select(h.c.chamber, func.min(h.c.disclosure_date))
            .where(h.c.history_id <= through)
            .group_by(h.c.chamber)
        )
        .tuples()
        .all()
    )
    start = window_start(generated_at, years)
    count, max_days = conn.execute(
        select(func.count(), func.coalesce(func.max(g.c.gap_days), 0)).where(
            g.c.oldest_seen_date >= start
        )
    ).one()
    return {
        "through_history_id": through,
        "window_years": years,
        "since": {
            c: (since[c].isoformat() if since.get(c) else None) for c in CHAMBERS
        },
        "gaps_in_window": {"count": int(count), "max_days": int(max_days)},
    }


def fold_batch(
    conn: Connection, batch_id: int, modes: Sequence[str] = LIVE_MODES
) -> None:
    """Make sure a selected Congress batch is in history before planning from it."""
    r = m.ingestion_runs
    run_id = conn.execute(
        select(r.c.id)
        .where(
            r.c.source == SOURCE,
            r.c.batch_id == batch_id,
            r.c.status == "succeeded",
            r.c.collection_mode.in_(modes),
        )
        .order_by(r.c.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    if run_id is not None:
        derive(conn, run_id, modes)


def planned_watermark(conn: Connection, run_ids: Mapping[str, int]) -> Optional[int]:
    """
    The history watermark both downstream runs were planned from, or None when
    neither recorded one (runs planned before CI-1). Disagreement is refused:
    the publication must rank exactly the history its downstream plans covered.
    """
    d = m.run_dependencies
    recorded = {
        source: conn.execute(
            select(d.c.history_through_id).where(
                d.c.run_id == run_ids[source], d.c.source == SOURCE
            )
        ).scalar_one_or_none()
        for source in ("insider_trades", "short_interest")
    }
    values = set(recorded.values())
    if len(values) != 1:
        raise ValueError("downstream runs were planned from different Congress history")
    (value,) = values
    return None if value is None else int(value)


def covers_batch(conn: Connection, through: int, batch_id: int) -> bool:
    """True when every history row first seen in batch_id is within `through`."""
    h = m.congress_trade_history
    return not conn.execute(
        select(func.count()).where(
            h.c.first_seen_batch_id == batch_id, h.c.history_id > through
        )
    ).scalar_one()
