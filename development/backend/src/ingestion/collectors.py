"""
Live collection wrappers with explicit outcomes.

The existing fetchers swallow request and parse errors and return [] / None /
partial lists, which cannot be told apart from a genuinely empty result. These
wrappers make the HTTP calls themselves, reusing each fetcher's endpoints,
constants and parse functions, and record every unit of work (fund, chamber
page, ticker) as completed or failed. Policy:

  * Any failed unit fails the whole collection (no partial snapshots); the
    importer records a failed run and the previous good batch stays current.
    This includes FMP 402 (free-tier page limit): truncation is never accepted.
  * Provider responses are structurally validated *before* the fetcher's parse
    function runs, because those parse functions swallow exceptions and return
    partial results. A unit whose data would be dropped by such a swallowed
    error fails instead.
  * A collection that completed every unit with zero records is a successful
    empty result (e.g. no insider buys in the window).
  * Insider and short-interest plans are built here from both upstream files
    (trades_congress.json, ark_holdings.json), read with a byte cap and
    validated record by record, using the same selection rules as the
    fetchers. A missing, oversized, unreadable or malformed file is
    `missing_upstream`; an empty plan is `no_input_tickers`; a plan over
    MAX_PLANNED_REQUESTS tickers is `plan_too_large` (never truncated).
  * Responses are streamed and capped at MAX_RESPONSE_BYTES. No retries except
    one for HTTP 429; a timeout or 5xx fails the unit.
  * Failure details are unit names and fixed codes. URLs, query strings (the
    FMP key travels in one) and exception messages are never recorded or printed.

Congress: any parsed trade without an ISO transaction date fails the
collection (parse_error) before fmp.filter_recent, which would otherwise drop
it silently. filter_recent's two-year window itself is kept as is.

The fetchers' own run() functions, JSON files and scheduler are unchanged.
"""

import importlib
import json
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

from src.ingestion.files import read_json_bounded
from src.services.consensus_watchlist.paths import PACKAGE_DIR
from src.ingestion.importer import CollectionOutcome

FETCHERS_DIR = PACKAGE_DIR / "fetchers"
MAX_RESPONSE_BYTES = 50 * 1024 * 1024
MAX_REPORTED_FAILURES = 20
# Upper bound on per-ticker provider requests in one collection. Real plans are
# a few hundred tickers; a larger plan fails (`plan_too_large`) instead of
# being truncated or fanning out thousands of external requests.
MAX_PLANNED_REQUESTS = 2000
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _fetcher(name: str):
    """Import a fetcher module the same way scheduler/cron.py does."""
    if str(FETCHERS_DIR) not in sys.path:
        sys.path.insert(0, str(FETCHERS_DIR))
    return importlib.import_module(name)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_iso_date(value: Any) -> bool:
    if not isinstance(value, str) or not _ISO_DATE.match(value):
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


# ── HTTP ─────────────────────────────────────────────────

@dataclass
class _Response:
    ok: bool
    status: Optional[int] = None
    data: Any = None
    code: Optional[str] = None


def _read_capped(resp) -> Optional[bytes]:
    declared = resp.headers.get("Content-Length") if resp.headers is not None else None
    if declared and declared.isdigit() and int(declared) > MAX_RESPONSE_BYTES:
        return None
    body = bytearray()
    for chunk in resp.iter_content(chunk_size=65536):
        body.extend(chunk)
        if len(body) > MAX_RESPONSE_BYTES:
            return None
    return bytes(body)


def _get_json(
    session: requests.Session,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 15,
    allowed_status: Tuple[int, ...] = (),
    sleep: Callable[[float], None] = time.sleep,
    retries_on_429: int = 1,
) -> _Response:
    for attempt in range(retries_on_429 + 1):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=timeout, stream=True)
        except requests.Timeout:
            return _Response(False, code="timeout")
        except requests.RequestException as exc:
            return _Response(False, code=f"request_error:{type(exc).__name__}")
        try:
            status = resp.status_code
            if status == 429 and attempt < retries_on_429:
                sleep(2.0 * (attempt + 1))
                continue
            if status in allowed_status:
                return _Response(True, status=status)
            if status != 200:
                return _Response(False, status=status, code=f"http_{status}")
            try:
                body = _read_capped(resp)
            except requests.Timeout:
                return _Response(False, status=status, code="timeout")
            except requests.RequestException as exc:
                return _Response(False, status=status, code=f"request_error:{type(exc).__name__}")
            if body is None:
                return _Response(False, status=status, code="response_too_large")
            try:
                return _Response(True, status=status, data=json.loads(body))
            except (ValueError, RecursionError):
                return _Response(False, status=status, code="invalid_json")
        finally:
            resp.close()
    return _Response(False, code="http_429")


# ── Outcome tracking ─────────────────────────────────────

@dataclass
class _Tracker:
    unit_kind: str
    attempted: int = 0
    completed: int = 0
    failures: List[Dict[str, str]] = field(default_factory=list)
    failure_count: int = 0

    def ok(self) -> None:
        self.attempted += 1
        self.completed += 1

    def fail(self, unit: str, code: str) -> None:
        self.attempted += 1
        self.failure_count += 1
        if len(self.failures) < MAX_REPORTED_FAILURES:
            self.failures.append({"unit": unit, "code": code})

    def diagnostics(self) -> Dict[str, Any]:
        return {
            "unit_kind": self.unit_kind,
            "attempted": self.attempted,
            "completed": self.completed,
            "failed": self.failure_count,
            "failures": self.failures,
        }

    def outcome(self, source: str, records: List[Dict[str, Any]], **kw) -> CollectionOutcome:
        if self.failure_count:
            code = "partial_collection" if self.completed else "collection_failed"
            return CollectionOutcome.failure(source, "live", code, diagnostics=self.diagnostics())
        return CollectionOutcome.success(source, "live", records, diagnostics=self.diagnostics(), **kw)

    def parse_failure(self, source: str) -> CollectionOutcome:
        return CollectionOutcome.failure(source, "live", "parse_error", diagnostics=self.diagnostics())


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _single_iso_date(values: List[Any]) -> Optional[date]:
    """The provider date if every value is the same ISO date; otherwise unknown."""
    if not values or not all(_is_iso_date(v) for v in values) or len(set(values)) != 1:
        return None
    return date.fromisoformat(values[0])


def _read_upstream(source: str, data_dir: Path):
    """
    Read and validate both upstream fetcher files with bounded reads.
    Returns ({"trades": [...], "holdings": [...]}, None) or (None, failure outcome).
    Plans are built from this data, not from the legacy loaders, which swallow
    errors and silently skip malformed records.
    """
    tracker = _Tracker("upstream files")
    data: Dict[str, List[Dict[str, Any]]] = {}
    for name, key in (("trades_congress.json", "trades"), ("ark_holdings.json", "holdings")):
        path = data_dir / name
        if not path.exists():
            tracker.fail(name, "missing_file")
            continue
        parsed, problem = read_json_bounded(path)
        if problem:
            tracker.fail(name, "file_too_large" if problem == "input_too_large" else "unreadable_file")
            continue
        rows = parsed.get(key) if isinstance(parsed, dict) else None
        if not isinstance(rows, list) or not all(_upstream_row_ok(key, r) for r in rows):
            tracker.fail(name, "malformed_file")
            continue
        data[key] = rows
        tracker.ok()
    if tracker.failure_count:
        return None, CollectionOutcome.failure(
            source, "live", "missing_upstream", diagnostics=tracker.diagnostics()
        )
    return data, None


def _upstream_row_ok(key: str, row: Any) -> bool:
    if not isinstance(row, dict) or not isinstance(row.get("ticker"), str) or not row["ticker"].strip():
        return False
    if key == "trades":
        return isinstance(row.get("trade_type"), str) and bool(row["trade_type"].strip())
    fund_count = row.get("fund_count")
    return isinstance(fund_count, int) and not isinstance(fund_count, bool) and fund_count >= 1


def _insider_plan(insider, upstream) -> List[Tuple[str, str]]:
    """Same selection as insider.load_congress_tickers / load_ark_tickers / run()."""
    purchases = [t["ticker"] for t in upstream["trades"] if "purchase" in t.get("trade_type", "").lower()]
    congress = [t for t, _ in Counter(purchases).most_common(insider.MAX_CONGRESS_TICKERS)]
    holdings = upstream["holdings"]
    ark = [h["ticker"] for h in holdings if h["fund_count"] > 1] + [
        h["ticker"] for h in holdings if h["fund_count"] == 1]
    congress_set = set(congress)
    return [(t, "popular_stable") for t in congress] + [
        (t, "affordable_growing") for t in ark if t not in congress_set]


def _short_interest_plan(si, upstream) -> List[str]:
    """Same universe as short_interest.get_candidate_tickers()."""
    tickers = {si.normalize_ticker(r["ticker"]) for r in upstream["trades"] + upstream["holdings"]}
    return sorted(t for t in tickers if t)


# ── ARK ──────────────────────────────────────────────────

def _ark_fund_ok(holdings: Any) -> Optional[str]:
    """None if usable, else a failure code. Rows without a ticker (cash etc.) are skipped upstream."""
    if not isinstance(holdings, list) or not holdings:
        return "empty_or_malformed_holdings"
    tickered = 0
    for h in holdings:
        if not isinstance(h, dict):
            return "malformed_holding"
        ticker = h.get("ticker")
        if ticker is None or (isinstance(ticker, str) and not ticker.strip()):
            continue
        if not isinstance(ticker, str):
            return "malformed_holding"
        if not _is_number(h.get("weight", 0.0)):
            return "malformed_holding"
        price = h.get("share_price", 0.0)
        if price is not None and not _is_number(price):
            return "malformed_holding"
        if not isinstance(h.get("company", ""), str):
            return "malformed_holding"
        tickered += 1
    return None if tickered else "empty_or_malformed_holdings"


def collect_ark(session: Optional[requests.Session] = None, sleep=time.sleep) -> CollectionOutcome:
    ark = _fetcher("ark")
    session = session or requests.Session()
    tracker = _Tracker("ARK funds")
    all_raw: Dict[str, List[Dict[str, Any]]] = {}
    for symbol in ark.ARK_FUNDS:
        resp = _get_json(session, ark.ARK_API_URL, params={"symbol": symbol}, sleep=sleep)
        if not resp.ok:
            tracker.fail(symbol, resp.code)
        else:
            holdings = resp.data.get("holdings") if isinstance(resp.data, dict) else None
            problem = _ark_fund_ok(holdings)
            if problem:
                tracker.fail(symbol, problem)
            else:
                all_raw[symbol] = holdings
                tracker.ok()
        sleep(ark.REQUEST_DELAY)
    if tracker.failure_count:
        return tracker.outcome("ark_holdings", [])
    try:
        holdings = ark.parse_holdings(all_raw)
    except (TypeError, ValueError, AttributeError):
        return tracker.parse_failure("ark_holdings")
    dates = [h.get("date") for rows in all_raw.values() for h in rows if h.get("ticker")]
    envelope = {
        "source": "ARK Invest ETF Holdings (arkfunds.io)",
        "funds_tracked": list(ark.ARK_FUNDS),
        "fetched_at": _now_iso(),
        "total_tickers": len(holdings),
        "multi_fund_tickers": sum(1 for h in holdings if h["fund_count"] > 1),
        "single_fund_tickers": sum(1 for h in holdings if h["fund_count"] == 1),
    }
    return tracker.outcome(
        "ark_holdings", holdings, envelope=envelope, source_as_of=_single_iso_date(dates)
    )


# ── Congress (FMP + Senate Stock Watcher) ────────────────

def _string_fields_ok(item: Dict[str, Any], keys: Tuple[str, ...]) -> bool:
    return all(item.get(k) is None or isinstance(item.get(k), str) for k in keys)


_FMP_KEYS = ("symbol", "transactionDate", "disclosureDate", "firstName", "lastName",
             "assetDescription", "assetType", "type", "amount", "district", "link")
_WATCHER_KEYS = ("ticker", "transaction_date", "senator", "asset_description", "asset_type",
                 "type", "amount", "ptr_link")


def collect_congress(
    session: Optional[requests.Session] = None, api_key: Optional[str] = None, sleep=time.sleep
) -> CollectionOutcome:
    fmp = _fetcher("fmp")
    key = api_key if api_key is not None else fmp.FMP_API_KEY
    if not key or key == "YOUR_FMP_KEY_HERE":
        return CollectionOutcome.failure("congress_trades", "live", "missing_credential")
    session = session or requests.Session()
    tracker = _Tracker("Congress requests")
    raw_fmp: List[Dict[str, Any]] = []
    for chamber in ("senate", "house"):
        for page in range(fmp.MAX_PAGES):
            unit = f"{chamber} page {page}"
            resp = _get_json(
                session, f"{fmp.BASE_URL}/{chamber}-latest",
                params={"page": page, "limit": 20, "apikey": key}, sleep=sleep,
            )
            if not resp.ok:
                tracker.fail(unit, resp.code)
                break
            items = resp.data
            if not isinstance(items, list) or not all(
                isinstance(i, dict) and _string_fields_ok(i, _FMP_KEYS) for i in items
            ):
                tracker.fail(unit, "malformed_response")
                break
            tracker.ok()
            if not items:
                break
            for item in items:
                item["_chamber"] = chamber.capitalize()
            raw_fmp.extend(items)
    resp = _get_json(session, fmp.SENATE_WATCHER_URL, timeout=30, sleep=sleep)
    watcher: List[Dict[str, Any]] = []
    if not resp.ok:
        tracker.fail("senate watcher", resp.code)
    elif not isinstance(resp.data, list) or not all(
        isinstance(i, dict) and _string_fields_ok(i, _WATCHER_KEYS) for i in resp.data
    ):
        tracker.fail("senate watcher", "malformed_response")
    else:
        watcher = resp.data
        tracker.ok()
    if tracker.failure_count:
        return tracker.outcome("congress_trades", [])
    try:
        trades = fmp.parse_trades(raw_fmp)
        trades += [t for t in (fmp.parse_watcher_trade(i) for i in watcher) if t]
        # filter_recent silently drops rows whose date does not parse, so a
        # malformed required date must fail the collection before it runs.
        if not all(_is_iso_date(t.get("transaction_date")) for t in trades):
            return tracker.parse_failure("congress_trades")
        trades = fmp.filter_recent(fmp.deduplicate(trades), years=2)
    except (TypeError, ValueError, AttributeError, KeyError):
        return tracker.parse_failure("congress_trades")
    envelope = {
        "source": "Financial Modeling Prep (FMP)",
        "endpoints": [f"{fmp.BASE_URL}/senate-latest", f"{fmp.BASE_URL}/house-latest"],
        "fetched_at": _now_iso(),
        "total_trades": len(trades),
    }
    return tracker.outcome("congress_trades", trades, envelope=envelope)


# ── Insider (SecuritiesDB) ───────────────────────────────

def _insider_payload_ok(raw: Any) -> bool:
    """Every field parse_insider_buys touches on a Purchase must be well-formed."""
    if not isinstance(raw, dict) or not isinstance(raw.get("data"), dict):
        return False
    txns = raw["data"].get("insider_transactions")
    if not isinstance(txns, dict) or not isinstance(txns.get("recent"), list):
        return False
    for txn in txns["recent"]:
        if not isinstance(txn, dict) or not isinstance(txn.get("type"), str):
            return False
        if txn["type"] != "Purchase":
            continue
        if not _is_iso_date(txn.get("date")):
            return False
        value = txn.get("value")
        if value is not None and not _is_number(value):
            return False
        shares = txn.get("shares")
        if shares is not None and not _is_number(shares):
            return False
        if not isinstance(txn.get("insider"), str):
            return False
    return True


def collect_insider(session: Optional[requests.Session] = None, sleep=time.sleep) -> CollectionOutcome:
    insider = _fetcher("insider")
    upstream, failure = _read_upstream("insider_trades", insider.LOCAL_DATA_DIR)
    if failure:
        return failure
    plan = _insider_plan(insider, upstream)
    if not plan:
        return CollectionOutcome.failure("insider_trades", "live", "no_input_tickers")
    if len(plan) > MAX_PLANNED_REQUESTS:
        return CollectionOutcome.failure("insider_trades", "live", "plan_too_large")

    session = session or requests.Session()
    tracker = _Tracker("insider tickers")
    buys: List[Dict[str, Any]] = []
    for ticker, bucket in plan:
        resp = _get_json(
            session, f"{insider.SECURITIES_URL}/{ticker}/insider-activity",
            allowed_status=(404,), sleep=sleep,
        )
        if not resp.ok:
            tracker.fail(ticker, resp.code)
        elif resp.status == 404:
            tracker.ok()  # provider has no Form 4 record for this ticker
        elif not _insider_payload_ok(resp.data):
            tracker.fail(ticker, "malformed_response")
        else:
            buys.extend(insider.parse_insider_buys(ticker, resp.data, bucket))
            tracker.ok()
        sleep(insider.REQUEST_DELAY)
    envelope = {
        "source": "SecuritiesDB (SEC Form 4)",
        "fetched_at": _now_iso(),
        "lookback_days": insider.LOOKBACK_DAYS,
        "min_buy_value": insider.MIN_BUY_VALUE,
        "total_buys": len(buys),
        "tickers_with_buys": len({b["ticker"] for b in buys}),
    }
    return tracker.outcome("insider_trades", buys, envelope=envelope)


# ── Short interest (Nasdaq) ──────────────────────────────

def _short_interest_kind(raw: Any) -> Optional[str]:
    """'none' = explicit no record, 'rows' = data to parse, None = malformed."""
    if not isinstance(raw, dict) or "data" not in raw:
        return None
    data = raw["data"]
    if data is None:
        return "none"
    if not isinstance(data, dict) or "shortInterestTable" not in data:
        return None
    table = data["shortInterestTable"]
    if table is None:
        return "none"
    if not isinstance(table, dict) or not isinstance(table.get("rows"), list):
        return None
    rows = table["rows"]
    if not rows:
        return "none"
    for row in rows:
        if not isinstance(row, dict):
            return None
        if not all(isinstance(row.get(k), (str, int, float, type(None)))
                   for k in ("interest", "avgDailyShareVolume", "daysToCover")):
            return None
        if not isinstance(row.get("settlementDate", ""), str):
            return None
    return "rows"


def collect_short_interest(
    session: Optional[requests.Session] = None, sleep=time.sleep
) -> CollectionOutcome:
    si = _fetcher("short_interest")
    upstream, failure = _read_upstream("short_interest", si.LOCAL_DATA_DIR)
    if failure:
        return failure
    tickers = _short_interest_plan(si, upstream)
    if not tickers:
        return CollectionOutcome.failure("short_interest", "live", "no_input_tickers")
    if len(tickers) > MAX_PLANNED_REQUESTS:
        return CollectionOutcome.failure("short_interest", "live", "plan_too_large")
    session = session or requests.Session()
    tracker = _Tracker("short-interest tickers")
    records: List[Dict[str, Any]] = []
    for ticker in tickers:
        resp = _get_json(
            session, si.NASDAQ_URL.format(symbol=ticker), headers=si.HEADERS,
            timeout=si.REQUEST_TIMEOUT, sleep=sleep,
        )
        kind = _short_interest_kind(resp.data) if resp.ok else None
        if not resp.ok:
            tracker.fail(ticker, resp.code)
        elif kind is None:
            tracker.fail(ticker, "malformed_response")
        elif kind == "none":
            tracker.ok()  # explicit "no short-interest record" from the provider
        else:
            record = si.parse_short_interest(ticker, resp.data)
            if record is None:
                tracker.fail(ticker, "parse_error")
            else:
                records.append(record)
                tracker.ok()
        sleep(si.REQUEST_DELAY)
    records.sort(key=lambda r: r["ticker"])
    envelope = {
        "schema_version": "1.0",
        "source": "nasdaq",
        "fetched_at": _now_iso(),
        "total_records": len(records),
    }
    return tracker.outcome("short_interest", records, envelope=envelope)


COLLECTORS = {
    "congress_trades": collect_congress,
    "ark_holdings": collect_ark,
    "insider_trades": collect_insider,
    "short_interest": collect_short_interest,
}
