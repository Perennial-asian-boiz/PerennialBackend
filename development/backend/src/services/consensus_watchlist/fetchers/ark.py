"""
PURPOSE:
    Fetches current holdings from ARK Invest ETFs and saves them
    as the "Affordable & Growing" candidate list for Perennial.

    ARK specifically invests in disruptive/early-stage companies —
    exactly the profile Perennial's "Affordable & Growing" persona
    is looking for. These tickers feed into:
        - insider.py  (checks insider buying on these tickers)
        - consensus.py (routes to Affordable & Growing bucket)
        - Cohen's sentiment pipeline (checks social/news sentiment)

SOURCE:
    arkfunds.io — Free third party ARK holdings API
    No key needed, updated daily
    Endpoint: https://arkfunds.io/api/v2/etf/holdings?symbol={FUND}

FUNDS TRACKED:
    ARKK — ARK Innovation ETF (broad disruptive tech)
    ARKQ — ARK Autonomous Tech & Robotics ETF
    ARKG — ARK Genomic Revolution ETF
    ARKW — ARK Next Generation Internet ETF

SCHEDULE:
    Runs daily at 9:02am via scheduler/cron.py
    (after fmp.py, before insider.py)

OUTPUT:
    database/local_data/ark_holdings.json

UPGRADE PATH:
    When school funding is available, supplement with
    FMP paid screener for broader small/mid cap coverage:
    GET https://financialmodelingprep.com/stable/company-screener
    params: marketCapLowerThan=5B, priceLowerThan=50
"""

import logging

# ── shared path resolution (see paths.py) ──
import sys as _sys
from pathlib import Path as _Path

import requests

_sys.path.insert(
    0,
    str(
        next(
            p
            for p in _Path(__file__).resolve().parents
            if p.name == "consensus_watchlist"
        )
    ),
)
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Support direct script execution as well as package imports.
_sys.path.insert(
    0, str(next(p for p in _Path(__file__).resolve().parents if p.name == "backend"))
)
from paths import (  # noqa: F401 -- legacy module exports
    ENV_PATH,
    LOCAL_DATA_DIR,
    local_data_dir,
)
from src.utils.logger import configure_logging, log_exception

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────

ARK_API_URL = "https://arkfunds.io/api/v2/etf/holdings"

# Funds to pull — each targets a different growth sector
ARK_FUNDS = {
    "ARKK": "ARK Innovation ETF",
    "ARKQ": "ARK Autonomous Tech & Robotics ETF",
    "ARKG": "ARK Genomic Revolution ETF",
    "ARKW": "ARK Next Generation Internet ETF",
}

REQUEST_DELAY = 0.5  # seconds between API calls


# ─────────────────────────────────────────────────────────
# FETCH — Holdings for one ARK fund
# ─────────────────────────────────────────────────────────


def legacy_fetch_fund_holdings(symbol: str) -> List[Dict[str, Any]]:
    """
    Fetches current holdings for one ARK ETF fund.
    Returns list of holding dicts or empty list on failure.
    """
    try:
        resp = requests.get(ARK_API_URL, params={"symbol": symbol}, timeout=15)

        if resp.status_code == 429:
            logger.warning("ark_rate_limited wait_seconds=5")
            time.sleep(5)
            return []
        if resp.status_code != 200:
            logger.info("ark_response_rejected status=%d", resp.status_code)
            return []

        data = resp.json()
        holdings: List[Dict[str, Any]] = data.get("holdings", [])
        logger.info("ark_collected holdings=%d", len(holdings))
        return holdings

    except Exception as e:
        log_exception(logger, "ark_fetch_fund_holdings_failed", e)
        return []


# ─────────────────────────────────────────────────────────
# PARSE — Combine holdings across all funds
# ─────────────────────────────────────────────────────────


def parse_holdings(all_raw: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Combines holdings from all ARK funds.
    If a ticker appears in multiple funds, merges them.
    Tags each ticker with which fund(s) hold it —
    appearing in multiple funds = stronger ARK conviction.

    Returns list of clean holding dicts.
    """
    ticker_map: defaultdict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "ticker": "",
            "company": "",
            "funds": [],
            "fund_count": 0,
            "share_price": 0.0,
            "total_weight": 0.0,
            "bucket": "affordable_growing",
            "data_source": "ark_etf",
        }
    )

    for symbol, holdings in all_raw.items():
        for h in holdings:
            ticker = (h.get("ticker") or "").strip().upper()
            if not ticker:
                continue

            entry = ticker_map[ticker]
            entry["ticker"] = ticker
            entry["company"] = h.get("company", "").strip()
            entry["share_price"] = h.get("share_price", 0.0)
            entry["total_weight"] += h.get("weight", 0.0)

            if symbol not in entry["funds"]:
                entry["funds"].append(symbol)
                entry["fund_count"] += 1

    # Sort by fund count (tickers in multiple funds = stronger signal)
    # then by total weight
    holdings_list = sorted(
        ticker_map.values(),
        key=lambda x: (x["fund_count"], x["total_weight"]),
        reverse=True,
    )

    return holdings_list


# ─────────────────────────────────────────────────────────
# SAVE
# ─────────────────────────────────────────────────────────


def legacy_save_to_json(
    holdings: List[Dict[str, Any]], filename: Optional[str | _Path] = None
) -> None:
    """
    Saves ARK holdings to local JSON file.
    Replace with write_to_db() once DB is set up.
    """
    if filename is None:
        output_dir = LOCAL_DATA_DIR
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = output_dir / "ark_holdings.json"

    # Separate multi-fund vs single-fund tickers
    multi_fund = [h for h in holdings if h["fund_count"] > 1]
    single_fund = [h for h in holdings if h["fund_count"] == 1]

    output = {
        "source": "ARK Invest ETF Holdings (arkfunds.io)",
        "funds_tracked": list(ARK_FUNDS.keys()),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "total_tickers": len(holdings),
        "multi_fund_tickers": len(multi_fund),
        "single_fund_tickers": len(single_fund),
        "note": (
            "multi_fund_tickers appear in 2+ ARK funds = stronger conviction signal"
        ),
        "holdings": holdings,
    }

    with open(filename, "w") as f:
        json.dump(output, f, indent=2)

    logger.info("ark_saved records=%d", len(holdings))


# ─────────────────────────────────────────────────────────
# PRINT
# ─────────────────────────────────────────────────────────


def legacy_print_summary(holdings: List[Dict[str, Any]]) -> None:
    logger.info(
        "ark_summary holdings=%d multi_fund=%d",
        len(holdings),
        sum(1 for h in holdings if h["fund_count"] > 1),
    )


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────


def legacy_run() -> List[Dict[str, Any]]:
    """
    Main function called by scheduler/cron.py daily at 9:02am.
    Can also be run manually: py ark.py
    Runs BEFORE insider.py which reads this output.
    """
    logger.info("Starting ARK ETF holdings fetch...")
    logger.info("ark_funds_planned")

    # Fetch all funds
    all_raw: Dict[str, List[Dict[str, Any]]] = {}
    for symbol in ARK_FUNDS:
        holdings = legacy_fetch_fund_holdings(symbol)
        if holdings:
            all_raw[symbol] = holdings
        time.sleep(REQUEST_DELAY)

    if not all_raw:
        logger.warning("No data returned from any ARK fund")
        return []

    logger.info("Parsing and merging holdings...")

    # Parse and merge
    holdings = parse_holdings(all_raw)
    logger.info("%s unique tickers across all funds", len(holdings))

    # Print summary
    legacy_print_summary(holdings)

    # Save
    legacy_save_to_json(holdings)

    logger.info("Done. ark_holdings.json ready for insider.py")
    return holdings


# scheduler/cron.py calls run() and cannot change; delete with cron.py (M7).
run = legacy_run


if __name__ == "__main__":
    configure_logging()
    legacy_run()
