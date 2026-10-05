"""
PURPOSE:
    Aggregates candidate signals from:
      - database/local_data/trades_congress.json (fmp.py output)
      - database/local_data/ark_holdings.json (ark.py output)
      - database/local_data/trades_insider.json (insider.py output)
      - database/local_data/short_interest.json (short_interest.py output)

    Groups candidate tickers into:
      - popular_stable: Market Cap > $10B
      - affordable_growing: Market Cap <= $10B
      - unresolved: Market Cap missing, invalid, or lookup error

    Outputs deterministic Version 1 candidate watchlist:
      database/local_data/consensus_watchlist.json

    MISSING vs NEGATIVE:
      An absent signal is not evidence of absence. Each candidate's insider
      signal carries an explicit status — has_buys, no_qualifying_buys,
      unknown, not_evaluated — and anything inconclusive is flagged with the
      warning `insider_data_unavailable`. Only `no_qualifying_buys` means
      "we checked and there was no insider buying". Input health is
      summarized at the top level under `source_health`.
"""

import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
import requests
from dotenv import load_dotenv
# ── shared path resolution (see paths.py) ──
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(next(
    p for p in _Path(__file__).resolve().parents if p.name == "consensus_watchlist"
)))
from paths import ENV_PATH, LOCAL_DATA_DIR, local_data_dir


# ─────────────────────────────────────────────────────────
# CONFIG & LOGGING
# ─────────────────────────────────────────────────────────

logger = logging.getLogger("consensus")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[consensus] %(levelname)s: %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

load_dotenv(dotenv_path=ENV_PATH)

FMP_API_KEY = os.getenv("FMP_API_KEY", "")
FMP_PROFILE_URL = "https://financialmodelingprep.com/stable/profile"

MARKET_CAP_THRESHOLD = 10_000_000_000  # $10 Billion


# ─────────────────────────────────────────────────────────
# PATH HELPERS & ATOMIC SAVER
# ─────────────────────────────────────────────────────────

def resolve_data_dirs() -> List[Path]:
    """
    Returns candidate database/local_data directories.
    Ensures directories exist.
    """
    candidates = [local_data_dir()]
    existing = []
    for d in candidates:
        d.mkdir(parents=True, exist_ok=True)
        if d not in existing:
            existing.append(d)
    return existing


def get_input_file_path(filename: str) -> Optional[Path]:
    dirs = resolve_data_dirs()
    for d in dirs:
        p = d / filename
        if p.exists():
            return p
    return None


def normalize_ticker(ticker: str) -> Optional[str]:
    if not ticker or not isinstance(ticker, str):
        return None
    cleaned = ticker.strip().upper()
    if not cleaned or cleaned in ("N/A", "NONE", "NULL", "--", "NAN"):
        return None
    return cleaned


def save_atomic_json(data: Dict[str, Any], target_paths: List[Path]) -> None:
    for target in target_paths:
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", dir=target.parent, delete=False, encoding="utf-8") as tf:
            json.dump(data, tf, indent=2, ensure_ascii=False)
            temp_path = Path(tf.name)

        try:
            temp_path.replace(target)
            logger.info(f"Saved atomic output to {target}")
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            raise IOError(f"Failed atomic write to {target}: {e}") from e


# ─────────────────────────────────────────────────────────
# SIGNAL LOADERS & AGGREGATORS
# ─────────────────────────────────────────────────────────

def load_congress_signals(file_path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    path = file_path or get_input_file_path("trades_congress.json")
    if not path or not path.exists():
        logger.warning("trades_congress.json not found — Congress signal will be empty")
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        trades = data.get("trades", [])
        signals: Dict[str, Dict[str, Any]] = {}

        for t in trades:
            trade_type = (t.get("trade_type") or "").lower()
            if "purchase" not in trade_type:
                continue

            norm_ticker = normalize_ticker(t.get("ticker"))
            if not norm_ticker:
                continue

            pol_name = (t.get("politician_name") or "").strip()
            txn_date = t.get("transaction_date") or ""
            disc_date = t.get("disclosure_date") or ""
            amt_range = t.get("amount_range") or ""

            if norm_ticker not in signals:
                signals[norm_ticker] = {
                    "buy_count": 0,
                    "buyers": set(),
                    "most_recent_transaction_date": "",
                    "most_recent_disclosure_date": "",
                    "amount_range": amt_range,
                }

            s = signals[norm_ticker]
            s["buy_count"] += 1
            if pol_name:
                s["buyers"].add(pol_name)

            if txn_date and (not s["most_recent_transaction_date"] or txn_date > s["most_recent_transaction_date"]):
                s["most_recent_transaction_date"] = txn_date
                s["amount_range"] = amt_range

            if disc_date and (not s["most_recent_disclosure_date"] or disc_date > s["most_recent_disclosure_date"]):
                s["most_recent_disclosure_date"] = disc_date

        out: Dict[str, Dict[str, Any]] = {}
        for ticker, s in signals.items():
            out[ticker] = {
                "buy_count": s["buy_count"],
                "distinct_buyer_count": len(s["buyers"]),
                "most_recent_transaction_date": s["most_recent_transaction_date"] or None,
                "most_recent_disclosure_date": s["most_recent_disclosure_date"] or s["most_recent_transaction_date"] or None,
                "amount_range": s["amount_range"] or None,
            }
        logger.info(f"Loaded Congress purchase signals for {len(out)} tickers")
        return out
    except Exception as e:
        logger.error(f"Error loading Congress signals: {e}")
        return {}


def load_ark_signals(file_path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    path = file_path or get_input_file_path("ark_holdings.json")
    if not path or not path.exists():
        logger.warning("ark_holdings.json not found — ARK signal will be empty")
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        holdings = data.get("holdings", [])
        fetched_at = data.get("fetched_at")
        signals: Dict[str, Dict[str, Any]] = {}

        for h in holdings:
            norm_ticker = normalize_ticker(h.get("ticker"))
            if not norm_ticker:
                continue

            signals[norm_ticker] = {
                "fund_count": h.get("fund_count", len(h.get("funds", []))),
                "funds": h.get("funds", []),
                "total_weight": float(h.get("total_weight", 0.0)),
                "share_price": float(h.get("share_price", 0.0)) if h.get("share_price") is not None else None,
                "fetched_at": fetched_at,
            }

        logger.info(f"Loaded ARK holdings signals for {len(signals)} tickers")
        return signals
    except Exception as e:
        logger.error(f"Error loading ARK signals: {e}")
        return {}


def _absent_insider_meta(reason: str) -> Dict[str, Any]:
    """Provenance for 'we have no usable insider file at all'."""
    return {
        "run_status": "failed",
        "reason": reason,
        "data_as_of": None,
        "scanned": set(),
        "failed": set(),
        "last_attempt_status": None,
        "retained_previous_data": False,
        "errors_by_code": {},
        "degraded_inputs": [],
    }


def load_insider_signals(
    file_path: Optional[Path] = None
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """
    Returns (signals, meta). `meta` is what makes an absent ticker
    interpretable: which tickers insider.py actually scanned, which ones it
    failed to get an answer for, and how healthy the run was. Without it a
    missing ticker is indistinguishable from a collection failure.
    """
    path = file_path or get_input_file_path("trades_insider.json")
    if not path or not path.exists():
        logger.warning("trades_insider.json not found — Insider signal unavailable")
        return {}, _absent_insider_meta("file_missing")

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        coverage = data.get("coverage") or {}
        attempt = data.get("last_attempt") or {}
        meta = {
            "run_status": coverage.get("run_status", "unknown"),
            "reason": None,
            "data_as_of": data.get("data_as_of") or data.get("fetched_at"),
            "scanned": set(coverage.get("scanned_tickers") or []),
            "failed": set(coverage.get("failed_tickers") or []),
            "last_attempt_status": attempt.get("run_status"),
            "retained_previous_data": bool(attempt.get("retained_previous_data")),
            # Why collection degraded, grouped by cause — carried through so a
            # dev can diagnose from the watchlist without opening the fetcher
            # output or the console log.
            "errors_by_code": coverage.get("errors_by_code") or {},
            "degraded_inputs": coverage.get("degraded_inputs") or [],
        }

        # Pre-contract files have no coverage block. Treat the scan scope as
        # unknown rather than inventing one — every absence becomes
        # inconclusive, which is the safe reading.
        if not coverage:
            meta["run_status"] = "unknown"
            meta["reason"] = "no_coverage_block"
            logger.warning(
                "trades_insider.json has no coverage block (pre-status format) "
                "— insider absences will be reported as inconclusive"
            )

        transactions = data.get("transactions", [])
        signals: Dict[str, Dict[str, Any]] = {}

        for txn in transactions:
            if (txn.get("transaction_type") or "").lower() != "purchase":
                continue

            norm_ticker = normalize_ticker(txn.get("ticker"))
            if not norm_ticker:
                continue

            insider_name = (txn.get("insider_name") or "").strip()
            val = float(txn.get("value", 0.0) or 0.0)
            txn_date = txn.get("transaction_date") or ""

            if norm_ticker not in signals:
                signals[norm_ticker] = {
                    "buy_count": 0,
                    "insiders": set(),
                    "total_value": 0.0,
                    "most_recent_buy": "",
                }

            s = signals[norm_ticker]
            s["buy_count"] += 1
            if insider_name:
                s["insiders"].add(insider_name)
            s["total_value"] += val
            if txn_date and (not s["most_recent_buy"] or txn_date > s["most_recent_buy"]):
                s["most_recent_buy"] = txn_date

        out: Dict[str, Dict[str, Any]] = {}
        for ticker, s in signals.items():
            out[ticker] = {
                "status": "has_buys",
                "buy_count": s["buy_count"],
                "distinct_insider_count": len(s["insiders"]),
                "total_value": s["total_value"],
                "most_recent_buy": s["most_recent_buy"] or None,
            }

        logger.info(
            f"Loaded Insider purchase signals for {len(out)} tickers "
            f"(run_status={meta['run_status']}, scanned={len(meta['scanned'])}, "
            f"unknown={len(meta['failed'])})"
        )
        if meta["retained_previous_data"]:
            logger.warning(
                f"Insider data is held over — last attempt "
                f"({meta['last_attempt_status']}) did not replace it; "
                f"data_as_of={meta['data_as_of']}"
            )
        return out, meta
    except Exception as e:
        logger.error(f"Error loading Insider signals: {e}")
        return {}, _absent_insider_meta(f"unreadable: {type(e).__name__}")


def load_short_interest_signals(file_path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    path = file_path or get_input_file_path("short_interest.json")
    if not path or not path.exists():
        logger.warning("short_interest.json not found — Short Interest signal will be empty")
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        records = data.get("records", [])
        signals: Dict[str, Dict[str, Any]] = {}

        for r in records:
            norm_ticker = normalize_ticker(r.get("ticker"))
            if not norm_ticker:
                continue

            signals[norm_ticker] = {
                "short_position_shares": r.get("short_position_shares"),
                "average_daily_volume": r.get("average_daily_volume"),
                "days_to_cover": r.get("days_to_cover"),
                "settlement_date": r.get("settlement_date"),
            }

        logger.info(f"Loaded Short Interest signals for {len(signals)} tickers")
        return signals
    except Exception as e:
        logger.error(f"Error loading Short Interest signals: {e}")
        return {}


# ─────────────────────────────────────────────────────────
# MARKET CAP LOOKUP
# ─────────────────────────────────────────────────────────

def fetch_market_cap(
    ticker: str,
    cache: Dict[str, Optional[float]],
    session: Optional[requests.Session] = None
) -> Optional[float]:
    """
    Looks up market capitalization using cached values or FMP stable profile endpoint.
    Endpoint: GET https://financialmodelingprep.com/stable/profile?symbol={ticker}&apikey={KEY}
    """
    if ticker in cache:
        return cache[ticker]

    if not FMP_API_KEY or FMP_API_KEY == "YOUR_FMP_KEY_HERE":
        cache[ticker] = None
        return None

    req_session = session or requests.Session()

    try:
        resp = req_session.get(
            FMP_PROFILE_URL,
            params={"symbol": ticker, "apikey": FMP_API_KEY},
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and data:
                item = data[0]
                mcap = item.get("mktCap") or item.get("marketCap")
                if mcap is not None:
                    mcap_val = float(mcap)
                    cache[ticker] = mcap_val
                    return mcap_val
        cache[ticker] = None
        return None
    except Exception as e:
        logger.warning(f"Market cap lookup failed for {ticker}: {e}")
        cache[ticker] = None
        return None


# ─────────────────────────────────────────────────────────
# SIGNAL INTERPRETATION
# ─────────────────────────────────────────────────────────

def resolve_insider_signal(
    ticker: str,
    signal: Optional[Dict[str, Any]],
    meta: Dict[str, Any]
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Decides what an absent insider signal actually means, and returns
    (signal, warning). Four outcomes, only one of which is a finding:

        has_buys           — qualifying purchases found
        no_qualifying_buys — scanned and answered; nothing met the filters
        unknown            — scanned but we never got an answer
        not_evaluated      — outside insider.py's scan scope

    Only `no_qualifying_buys` licenses the claim "no insider buying". The
    other two absences are missing data and must not read as a negative.
    """
    if signal is not None:
        return signal, None

    if meta["run_status"] == "failed":
        return {
            "status": "unknown",
            "reason": meta.get("reason") or "collection_failed",
            "data_as_of": meta["data_as_of"],
        }, "insider_data_unavailable"

    if ticker in meta["failed"]:
        return {
            "status": "unknown",
            "reason": "request_failed",
            "data_as_of": meta["data_as_of"],
        }, "insider_data_unavailable"

    if ticker in meta["scanned"]:
        return {
            "status": "no_qualifying_buys",
            "buy_count": 0,
            "distinct_insider_count": 0,
            "total_value": 0.0,
            "most_recent_buy": None,
            "data_as_of": meta["data_as_of"],
        }, None

    # Not in the scanned set — either outside scan scope, or the file predates
    # the coverage contract so the scope is unknowable.
    return {
        "status": "not_evaluated",
        "reason": meta.get("reason") or "outside_scan_scope",
        "data_as_of": meta["data_as_of"],
    }, "insider_data_unavailable"


# ─────────────────────────────────────────────────────────
# DETERMINISTIC RANKING HEURISTICS
# ─────────────────────────────────────────────────────────

def popular_stable_sort_key(record: Dict[str, Any]) -> Tuple[int, int, str, int, float, str]:
    c = record["signals"].get("congress") or {}
    i = record["signals"].get("insider") or {}

    c_distinct = c.get("distinct_buyer_count", 0)
    c_buys = c.get("buy_count", 0)
    c_date = c.get("most_recent_transaction_date") or ""

    i_distinct = i.get("distinct_insider_count", 0)
    i_val = i.get("total_value", 0.0)

    # Invert date for descending string comparison
    date_key = "".join(chr(255 - ord(char)) for char in c_date)

    return (
        -c_distinct,
        -c_buys,
        date_key,
        -i_distinct,
        -i_val,
        record["ticker"]
    )


def affordable_growing_sort_key(record: Dict[str, Any]) -> Tuple[int, float, int, float, str]:
    a = record["signals"].get("ark") or {}
    i = record["signals"].get("insider") or {}

    a_funds = a.get("fund_count", 0)
    a_weight = a.get("total_weight", 0.0)

    i_distinct = i.get("distinct_insider_count", 0)
    i_val = i.get("total_value", 0.0)

    return (
        -a_funds,
        -a_weight,
        -i_distinct,
        -i_val,
        record["ticker"]
    )


# ─────────────────────────────────────────────────────────
# MAIN AGGREGATION & PIPELINE
# ─────────────────────────────────────────────────────────

def run(
    congress_file: Optional[Path] = None,
    ark_file: Optional[Path] = None,
    insider_file: Optional[Path] = None,
    short_interest_file: Optional[Path] = None,
    profile_cache: Optional[Dict[str, Optional[float]]] = None
) -> Dict[str, Any]:
    """
    Main consensus watchlist aggregation pipeline.
    Reads all signal sources, computes market caps, groups candidates,
    applies V1 deterministic ranking, and writes output atomically.
    """
    logger.info("Starting Consensus Watchlist Aggregation...")

    congress_signals = load_congress_signals(congress_file)
    ark_signals = load_ark_signals(ark_file)
    insider_signals, insider_meta = load_insider_signals(insider_file)
    short_interest_signals = load_short_interest_signals(short_interest_file)

    # Union of candidates from Congress (purchases) and ARK
    candidate_tickers = sorted(set(congress_signals.keys()) | set(ark_signals.keys()))
    logger.info(f"Total consensus candidate tickers to evaluate: {len(candidate_tickers)}")

    market_cap_cache: Dict[str, Optional[float]] = profile_cache if profile_cache is not None else {}
    session = requests.Session()

    popular_stable: List[Dict[str, Any]] = []
    affordable_growing: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []
    errors: List[str] = []

    for ticker in candidate_tickers:
        mcap = fetch_market_cap(ticker, market_cap_cache, session=session)

        warnings: List[str] = []

        c_sig = congress_signals.get(ticker)
        si_sig = short_interest_signals.get(ticker)
        a_sig = ark_signals.get(ticker)

        # An absent insider signal is ambiguous on its own — resolve it
        # against what insider.py reported it actually established.
        i_sig, insider_warning = resolve_insider_signal(
            ticker, insider_signals.get(ticker), insider_meta
        )
        if insider_warning:
            warnings.append(insider_warning)

        # Build source dates dict
        source_dates = {
            "congress": c_sig.get("most_recent_disclosure_date") if c_sig else None,
            "insider": i_sig.get("most_recent_buy") if i_sig else None,
            "short_interest": si_sig.get("settlement_date") if si_sig else None,
            "ark": a_sig.get("fetched_at") if a_sig else None,
        }

        # Build ark signal without internal fetched_at field
        a_sig_clean = dict(a_sig) if a_sig else None
        if a_sig_clean and "fetched_at" in a_sig_clean:
            del a_sig_clean["fetched_at"]

        signals_payload = {
            "congress": c_sig,
            "insider": i_sig,
            "short_interest": si_sig,
            "ark": a_sig_clean,
        }

        if mcap is None or mcap <= 0:
            bucket = "unresolved"
            warnings.append("market_cap_unavailable")
            item = {
                "ticker": ticker,
                "bucket": bucket,
                "rank": None,
                "market_cap": None,
                "signals": signals_payload,
                "source_dates": source_dates,
                "warnings": warnings,
            }
            unresolved.append(item)
        elif mcap > MARKET_CAP_THRESHOLD:
            bucket = "popular_stable"
            item = {
                "ticker": ticker,
                "bucket": bucket,
                "rank": None,
                "market_cap": mcap,
                "signals": signals_payload,
                "source_dates": source_dates,
                "warnings": warnings,
            }
            popular_stable.append(item)
        else:
            bucket = "affordable_growing"
            item = {
                "ticker": ticker,
                "bucket": bucket,
                "rank": None,
                "market_cap": mcap,
                "signals": signals_payload,
                "source_dates": source_dates,
                "warnings": warnings,
            }
            affordable_growing.append(item)

    # Sort & Rank Popular & Stable
    popular_stable.sort(key=popular_stable_sort_key)
    for idx, record in enumerate(popular_stable, start=1):
        record["rank"] = idx

    # Sort & Rank Affordable & Growing
    affordable_growing.sort(key=affordable_growing_sort_key)
    for idx, record in enumerate(affordable_growing, start=1):
        record["rank"] = idx

    # Sort Unresolved by Ticker A-Z
    unresolved.sort(key=lambda r: r["ticker"])

    insider_unknown = sum(
        1 for r in popular_stable + affordable_growing + unresolved
        if "insider_data_unavailable" in r["warnings"]
    )

    output_payload = {
        "schema_version": "1.1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        # Health of the inputs this watchlist was built from. generated_at
        # says when we aggregated; this says whether the data was any good.
        "source_health": {
            "insider": {
                "run_status": insider_meta["run_status"],
                "data_as_of": insider_meta["data_as_of"],
                "tickers_scanned": len(insider_meta["scanned"]),
                "tickers_unknown": len(insider_meta["failed"]),
                "last_attempt_status": insider_meta["last_attempt_status"],
                "retained_previous_data": insider_meta["retained_previous_data"],
                "reason": insider_meta.get("reason"),
                "candidates_without_insider_data": insider_unknown,
                "errors_by_code": insider_meta["errors_by_code"],
                "degraded_inputs": insider_meta["degraded_inputs"],
            },
        },
        "popular_stable": popular_stable,
        "affordable_growing": affordable_growing,
        "unresolved": unresolved,
        "errors": errors,
    }

    target_paths = [d / "consensus_watchlist.json" for d in resolve_data_dirs()]
    save_atomic_json(output_payload, target_paths)

    logger.info(
        f"Consensus aggregation complete. "
        f"Popular & Stable: {len(popular_stable)}, "
        f"Affordable & Growing: {len(affordable_growing)}, "
        f"Unresolved: {len(unresolved)}"
    )
    if insider_unknown:
        logger.warning(
            f"{insider_unknown} candidate(s) have no usable insider data — "
            f"their absence of insider buying is inconclusive "
            f"(insider run_status={insider_meta['run_status']})"
        )
        for code, g in (insider_meta["errors_by_code"] or {}).items():
            logger.warning(
                f"  insider {code} [{g.get('category')}] x{g.get('count')} — "
                f"e.g. {g.get('sample_detail')}"
            )
    return output_payload


if __name__ == "__main__":
    run()
