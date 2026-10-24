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
    Stop at the first failed provider unit, with planned/not_attempted counts;
    do not delay or request the rest of a collection already known to fail.
  * Provider responses are structurally validated *before* the fetcher's parse
    function runs, because those parse functions swallow exceptions and return
    partial results. A unit whose data would be dropped by such a swallowed
    error fails instead.
  * A collection that completed every unit with zero records is a successful
    empty result (e.g. no insider buys in the window), except short interest:
    an entirely empty Nasdaq universe fails with empty_short_interest.
  * Insider and short-interest plans use explicit upstream={trades, holdings}
    rows from pinned database batches. Only callers omitting upstream use the
    legacy bounded file fallback. Malformed supplied rows or missing/malformed
    files fail with `missing_upstream`; an empty plan is `no_input_tickers`; a plan over
    MAX_PLANNED_REQUESTS tickers is `plan_too_large` (never truncated).
  * Responses are streamed and capped at MAX_RESPONSE_BYTES. HTTP 429, timeouts,
    non-SSL connection errors and selected 5xx retry at most twice within a
    per-unit 60-second deadline, with bounded exponential backoff and jitter.
    Retry-After is never shortened: a delay beyond the remaining budget fails
    the unit without another request. Without Retry-After, X-Ratelimit-Reset
    (Finnhub: epoch seconds when the window resets) is honored the same way. Request timeouts
    and stream-chunk deadline checks bound normal provider behavior; requests'
    socket timeout measures inactivity, so a blocked read is not preempted by
    the monotonic deadline. Auth, parse and size failures never retry.
  * Failure details are unit names and fixed codes. URLs, query strings (the
    FMP key travels in one) and exception messages are never recorded or printed.

Congress: the free FMP plan serves only page 0 (20 rows) per chamber, so a
run reads at most settings.fmp_max_pages pages (default 1) and a full final
page ends the chamber instead of failing. Stored history accumulates across
runs and possible gaps are detected there (src/pipeline/congress_history.py).
Trades of every age are stored (D13); the 2-year window is applied in ranking.
Any parsed trade without an ISO transaction date fails the collection
(parse_error).

The production database scheduler calls this COLLECTORS interface through
src.pipeline.runner. The legacy JSON scheduler and fetchers' file run() entry
points remain separate; they must not be used as production database adapters.
"""

import importlib
import json
import math
import random
import re
import sys
import time
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

from src.ingestion.adapters import (
    ArkAdapter,
    Collector,
    FmpAdapter,
    InsiderAdapter,
    ShortInterestAdapter,
)
from src.ingestion.files import read_json_bounded
from src.ingestion.importer import CollectionOutcome
from src.services.consensus_watchlist.paths import PACKAGE_DIR
from src.settings import Settings

FETCHERS_DIR = PACKAGE_DIR / "fetchers"
MAX_RESPONSE_BYTES = 50 * 1024 * 1024
MAX_REPORTED_FAILURES = 20
# Upper bound on per-ticker provider requests in one collection. Real plans are
# a few hundred tickers; a larger plan fails (`plan_too_large`) instead of
# being truncated or fanning out thousands of external requests.
MAX_PLANNED_REQUESTS = 2000
MAX_HTTP_RETRIES = 2
HTTP_DEADLINE_SECONDS = 60.0
MAX_RETRY_DELAY_SECONDS = 10.0
RETRYABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
UpstreamRows = Dict[str, List[Dict[str, Any]]]


def _fetcher(name: str) -> Any:
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


class _DeadlineExceeded(Exception):
    pass


def _read_capped(
    resp: requests.Response,
    *,
    deadline: Optional[float] = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> Optional[bytes]:
    declared = resp.headers.get("Content-Length") if resp.headers is not None else None
    if declared and declared.isdigit() and int(declared) > MAX_RESPONSE_BYTES:
        return None
    body = bytearray()
    for chunk in resp.iter_content(chunk_size=65536):
        if deadline is not None and monotonic() >= deadline:
            raise _DeadlineExceeded
        body.extend(chunk)
        if len(body) > MAX_RESPONSE_BYTES:
            return None
    return bytes(body)


def _ratelimit_reset(
    headers: Optional[Mapping[str, str]], wall_time: Callable[[], float]
) -> float:
    """Seconds until X-Ratelimit-Reset: an epoch timestamp, or a delta if small."""
    value = (headers or {}).get("X-Ratelimit-Reset")
    if not isinstance(value, str) or len(value) > 20:
        return 0.0
    try:
        reset = float(value)
    except ValueError:
        return 0.0
    if not math.isfinite(reset):
        return 0.0
    delay = reset - wall_time() if reset > 1_000_000_000 else reset
    return max(0.0, delay)


def _retry_after(
    headers: Optional[Mapping[str, str]], wall_time: Callable[[], float]
) -> float:
    value = (headers or {}).get("Retry-After")
    if value is None:
        return _ratelimit_reset(headers, wall_time)
    if not isinstance(value, str) or len(value) > 100:
        return 0.0
    try:
        delay = float(value)
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
            if when.tzinfo is None:
                return 0.0
            delay = when.timestamp() - wall_time()
        except (ValueError, TypeError, OverflowError):
            return 0.0
    if not math.isfinite(delay):
        return 0.0
    return max(0.0, delay)


def _get_json(
    session: requests.Session,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 15,
    allowed_status: Tuple[int, ...] = (),
    sleep: Callable[[float], None] = time.sleep,
    max_retries: int = MAX_HTTP_RETRIES,
    jitter: Callable[[], float] = random.random,
    monotonic: Callable[[], float] = time.monotonic,
    wall_time: Callable[[], float] = time.time,
    deadline_seconds: float = HTTP_DEADLINE_SECONDS,
    raw: bool = False,
) -> _Response:
    """Bound one HTTP unit, close every response before waiting, never expose input text.

    With raw=True the capped body is returned as bytes instead of parsed JSON
    (House Clerk ZIP indexes and PTR PDFs).
    """
    deadline = monotonic() + min(deadline_seconds, HTTP_DEADLINE_SECONDS)
    retries = min(MAX_HTTP_RETRIES, max(0, max_retries))
    for attempt in range(retries + 1):
        remaining = deadline - monotonic()
        if remaining <= 0:
            return _Response(False, code="deadline_exceeded")
        retry_after = 0.0
        resp = None
        try:
            resp = session.get(
                url,
                params=params,
                headers=headers,
                timeout=min(timeout, remaining),
                stream=True,
            )
            if monotonic() >= deadline:
                return _Response(False, code="deadline_exceeded")
            status = resp.status_code
            if status in allowed_status:
                return _Response(True, status=status)
            if status in RETRYABLE_HTTP_STATUSES:
                failure = _Response(False, status=status, code=f"http_{status}")
                retry_after = _retry_after(resp.headers, wall_time)
            elif status != 200:
                return _Response(False, status=status, code=f"http_{status}")
            else:
                body = _read_capped(resp, deadline=deadline, monotonic=monotonic)
                if monotonic() >= deadline:
                    return _Response(False, status=status, code="deadline_exceeded")
                if body is None:
                    return _Response(False, status=status, code="response_too_large")
                if raw:
                    return _Response(True, status=status, data=body)
                try:
                    data = json.loads(body)
                except (ValueError, RecursionError):
                    return _Response(False, status=status, code="invalid_json")
                if monotonic() >= deadline:
                    return _Response(False, status=status, code="deadline_exceeded")
                return _Response(True, status=status, data=data)
        except _DeadlineExceeded:
            return _Response(False, code="deadline_exceeded")
        except requests.Timeout:
            failure = _Response(False, code="timeout")
        except requests.ConnectionError as exc:
            failure = _Response(False, code=f"request_error:{type(exc).__name__}")
            if isinstance(exc, requests.exceptions.SSLError):
                return failure
        except requests.RequestException as exc:
            return _Response(False, code=f"request_error:{type(exc).__name__}")
        finally:
            if resp is not None:
                resp.close()
        if attempt == retries:
            return failure
        backoff = min(
            MAX_RETRY_DELAY_SECONDS, 2.0**attempt + min(1.0, max(0.0, jitter()))
        )
        delay = max(backoff, retry_after)
        if delay >= deadline - monotonic():
            return _Response(False, code="deadline_exceeded")
        sleep(delay)
    raise AssertionError("unreachable")


# ── Outcome tracking ─────────────────────────────────────


@dataclass
class _Tracker:
    unit_kind: str
    planned: Optional[int] = None
    attempted: int = 0
    completed: int = 0
    failures: List[Dict[str, str]] = field(default_factory=list)
    failure_count: int = 0
    extra_counts: Dict[str, int] = field(default_factory=dict)

    def ok(self) -> None:
        self.attempted += 1
        self.completed += 1

    def fail(self, unit: str, code: Optional[str]) -> None:
        self.attempted += 1
        self.failure_count += 1
        if len(self.failures) < MAX_REPORTED_FAILURES:
            self.failures.append({"unit": unit, "code": code or "failed"})

    def diagnostics(self) -> Dict[str, Any]:
        counts = {
            "unit_kind": self.unit_kind,
            "attempted": self.attempted,
            "completed": self.completed,
            "failed": self.failure_count,
            "failures": self.failures,
        }
        if self.planned is not None:
            counts.update(
                planned=self.planned,
                not_attempted=max(0, self.planned - self.attempted),
            )
        counts.update(self.extra_counts)
        return counts

    def outcome(
        self, source: str, records: List[Dict[str, Any]], **kw: Any
    ) -> CollectionOutcome:
        if self.failure_count:
            code = "partial_collection" if self.completed else "collection_failed"
            return CollectionOutcome.failure(
                source, "live", code, diagnostics=self.diagnostics()
            )
        kw.setdefault("coverage", {"complete": True, "scope": "snapshot"})
        return CollectionOutcome.success(
            source, "live", records, diagnostics=self.diagnostics(), **kw
        )

    def parse_failure(self, source: str) -> CollectionOutcome:
        return CollectionOutcome.failure(
            source, "live", "parse_error", diagnostics=self.diagnostics()
        )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _single_iso_date(values: List[Any]) -> Optional[date]:
    """The provider date if every value is the same ISO date; otherwise unknown."""
    if not values or not all(_is_iso_date(v) for v in values) or len(set(values)) != 1:
        return None
    return date.fromisoformat(values[0])


def _latest_iso_date(values: List[Any]) -> Optional[date]:
    """Latest retained content date; empty or incomplete dates stay unknown.

    Never substitute collection time: the date participates in batch identity.
    Mixed settlement dates use the latest, retaining each row's own date.
    """
    if not values or not all(_is_iso_date(value) for value in values):
        return None
    return max(date.fromisoformat(value) for value in values)


def _read_upstream(
    source: str, data_dir: Path
) -> Tuple[Optional[UpstreamRows], Optional[CollectionOutcome]]:
    """
    Read and validate both upstream fetcher files with bounded reads.
    Returns ({"trades": [...], "holdings": [...]}, None) or (None, failure outcome).
    Plans are built from this data, not from the legacy loaders, which swallow
    errors and silently skip malformed records.
    """
    tracker = _Tracker("upstream files", planned=2)
    data: Dict[str, List[Dict[str, Any]]] = {}
    for name, key in (
        ("trades_congress.json", "trades"),
        ("ark_holdings.json", "holdings"),
    ):
        path = data_dir / name
        if not path.exists():
            tracker.fail(name, "missing_file")
            continue
        parsed, problem = read_json_bounded(path)
        if problem:
            tracker.fail(
                name,
                "file_too_large" if problem == "input_too_large" else "unreadable_file",
            )
            continue
        rows = parsed.get(key) if isinstance(parsed, dict) else None
        if not isinstance(rows, list) or not all(
            _upstream_row_ok(key, r) for r in rows
        ):
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
    if (
        not isinstance(row, dict)
        or not isinstance(row.get("ticker"), str)
        or not row["ticker"].strip()
    ):
        return False
    if key == "trades":
        return isinstance(row.get("trade_type"), str) and bool(
            row["trade_type"].strip()
        )
    fund_count = row.get("fund_count")
    return (
        isinstance(fund_count, int)
        and not isinstance(fund_count, bool)
        and fund_count >= 1
    )


def _insider_plan(
    insider: InsiderAdapter, upstream: UpstreamRows
) -> List[Tuple[str, str]]:
    """Same selection as insider.legacy_load_congress_tickers / legacy_load_ark_tickers / legacy_run()."""
    purchases = [
        t["ticker"]
        for t in upstream["trades"]
        if "purchase" in t.get("trade_type", "").lower()
    ]
    congress = [
        t for t, _ in Counter(purchases).most_common(insider.MAX_CONGRESS_TICKERS)
    ]
    holdings = upstream["holdings"]
    ark = [h["ticker"] for h in holdings if h["fund_count"] > 1] + [
        h["ticker"] for h in holdings if h["fund_count"] == 1
    ]
    congress_set = set(congress)
    return [(t, "popular_stable") for t in congress] + [
        (t, "affordable_growing") for t in ark if t not in congress_set
    ]


def _short_interest_plan(si: ShortInterestAdapter, upstream: UpstreamRows) -> List[str]:
    """Same universe as short_interest.legacy_get_candidate_tickers()."""
    tickers = {
        si.normalize_ticker(r["ticker"])
        for r in upstream["trades"] + upstream["holdings"]
    }
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


def collect_ark(
    session: Optional[requests.Session] = None,
    sleep: Callable[[float], None] = time.sleep,
) -> CollectionOutcome:
    ark: ArkAdapter = _fetcher("ark")
    session = session or requests.Session()
    tracker = _Tracker("ARK funds", planned=len(ark.ARK_FUNDS))
    all_raw: Dict[str, List[Dict[str, Any]]] = {}
    for symbol in ark.ARK_FUNDS:
        resp = _get_json(
            session, ark.ARK_API_URL, params={"symbol": symbol}, sleep=sleep
        )
        if not resp.ok:
            tracker.fail(symbol, resp.code)
        else:
            holdings = (
                resp.data.get("holdings") if isinstance(resp.data, dict) else None
            )
            problem = _ark_fund_ok(holdings)
            if problem:
                tracker.fail(symbol, problem)
            else:
                assert isinstance(holdings, list)
                all_raw[symbol] = holdings
                tracker.ok()
        if tracker.failure_count:
            break
        sleep(ark.REQUEST_DELAY)
    if tracker.failure_count:
        return tracker.outcome("ark_holdings", [])
    try:
        holdings = ark.parse_holdings(all_raw)
    except (TypeError, ValueError, AttributeError):
        return tracker.parse_failure("ark_holdings")
    dates = [
        h.get("date") for rows in all_raw.values() for h in rows if h.get("ticker")
    ]
    envelope = {
        "source": "ARK Invest ETF Holdings (arkfunds.io)",
        "funds_tracked": list(ark.ARK_FUNDS),
        "fetched_at": _now_iso(),
        "total_tickers": len(holdings),
        "multi_fund_tickers": sum(1 for h in holdings if h["fund_count"] > 1),
        "single_fund_tickers": sum(1 for h in holdings if h["fund_count"] == 1),
    }
    return tracker.outcome(
        "ark_holdings",
        holdings,
        envelope=envelope,
        source_as_of=_single_iso_date(dates),
    )


# ── Congress (House Clerk + FMP) ─────────────────────────

FREE_PLAN_PAGES = 1  # the free plan answers deeper pages with HTTP 402


def _string_fields_ok(item: Dict[str, Any], keys: Tuple[str, ...]) -> bool:
    return all(item.get(k) is None or isinstance(item.get(k), str) for k in keys)


_FMP_KEYS = (
    "symbol",
    "transactionDate",
    "disclosureDate",
    "firstName",
    "lastName",
    "assetDescription",
    "assetType",
    "type",
    "amount",
    "district",
    "link",
)
_CLERK_TERMINAL = ("parsed", "scanned")


def _collect_fmp_chambers(
    session: requests.Session,
    fmp: FmpAdapter,
    key: str,
    chambers: Tuple[str, ...],
    page_limit: int,
    tracker: _Tracker,
    sleep: Callable[[float], None],
) -> Optional[List[Dict[str, Any]]]:
    """
    FMP '-latest' pages per chamber, up to page_limit; None on any failed unit.
    A short page is the whole served list; a full final page is the end of
    what this run can see (the history derivation checks it for gaps).
    """
    counts = tracker.extra_counts
    raw_fmp: List[Dict[str, Any]] = []
    for chamber in chambers:
        for page in range(page_limit):
            unit = f"{chamber} page {page}"
            counts["fmp_calls"] += 1
            resp = _get_json(
                session,
                f"{fmp.BASE_URL}/{chamber}-latest",
                params={"page": page, "limit": 20, "apikey": key},
                sleep=sleep,
            )
            if not resp.ok:
                tracker.fail(unit, resp.code)
                return None
            items = resp.data
            if not isinstance(items, list) or not all(
                isinstance(i, dict) and _string_fields_ok(i, _FMP_KEYS) for i in items
            ):
                tracker.fail(unit, "malformed_response")
                return None
            if len(items) > 20:
                tracker.fail(unit, "malformed_response")
                return None
            counts[f"{chamber}_pages_fetched"] += 1
            counts[f"{chamber}_rows_served"] += len(items)
            tracker.ok()
            for item in items:
                item["_chamber"] = chamber.capitalize()
            raw_fmp.extend(items)
            if len(items) < 20 or page == page_limit - 1:
                break
            if tracker.planned is not None:
                tracker.planned += 1
    return raw_fmp


def _cached_clerk_report(entry: Any) -> bool:
    """A cache entry the collector may reuse: a final outcome with well-formed trades."""
    return (
        isinstance(entry, dict)
        and entry.get("status") in _CLERK_TERMINAL
        and isinstance(entry.get("trades"), list)
        and all(isinstance(t, dict) for t in entry["trades"])
    )


def _collect_house_clerk(
    session: requests.Session,
    tracker: _Tracker,
    sleep: Callable[[float], None],
    cache_path: Optional[Path],
) -> Optional[List[Dict[str, Any]]]:
    """
    House trades from every PTR filed in house_clerk.LOOKBACK_DAYS; None on
    any failed unit. Units are each yearly index, then each report. A scanned
    report (no text layer) completes with no records and is counted; a report
    with stock rows the parser could not read fails, since its trades would
    otherwise be dropped. Parsed reports are reused from the House Clerk
    cache, which holds the same outcome a fresh parse produces.

    house_rows_served stays 0: the Clerk lists every filing, so there is no
    page edge for the history derivation's gap check to measure.
    """
    hc = _fetcher("house_clerk")
    today = datetime.now(timezone.utc).date()
    cutoff = date.fromordinal(today.toordinal() - hc.LOOKBACK_DAYS).isoformat()
    years = sorted({today.year, int(cutoff[:4])})
    if tracker.planned is not None:
        tracker.planned += len(years)

    filings: Dict[str, Dict[str, Any]] = {}
    for year in years:
        unit = f"house index {year}"
        resp = _get_json(
            session,
            hc.INDEX_URL.format(year=year),
            headers=hc.HEADERS,
            timeout=hc.REQUEST_TIMEOUT,
            sleep=sleep,
            raw=True,
        )
        if not resp.ok:
            tracker.fail(unit, resp.code)
            return None
        try:
            listed = hc.parse_index(resp.data)
        except ValueError:
            tracker.fail(unit, "malformed_response")
            return None
        tracker.ok()
        for f in listed:
            if f["filing_date"] >= cutoff:
                filings[f["doc_id"]] = f

    cache = hc.load_cache(cache_path)
    if tracker.planned is not None:
        tracker.planned += len(filings)
    tracker.extra_counts.update(
        house_reports=len(filings),
        house_reports_scanned=0,
        house_reports_downloaded=0,
        house_stock_rows_skipped=0,
    )
    trades: List[Dict[str, Any]] = []
    outcomes: Dict[str, Dict[str, Any]] = {}
    for doc_id, f in sorted(filings.items()):
        unit = f"house report {doc_id}"
        report = cache.get(doc_id)
        if not _cached_clerk_report(report):
            resp = _get_json(
                session,
                hc.PTR_URL.format(year=f["year"], doc_id=doc_id),
                headers=hc.HEADERS,
                timeout=hc.REQUEST_TIMEOUT,
                sleep=sleep,
                raw=True,
            )
            if not resp.ok:
                tracker.fail(unit, resp.code)
                break
            report = hc.parse_report(resp.data, f)
            tracker.extra_counts["house_reports_downloaded"] += 1
            sleep(hc.REQUEST_DELAY)
        if report["status"] == hc.STATUS_FAILED:
            tracker.fail(unit, "malformed_response")
            break
        if report["status"] == hc.STATUS_PARTIAL:
            tracker.fail(unit, "parse_error")
            break
        outcomes[doc_id] = report
        if report["status"] == hc.STATUS_SCANNED:
            tracker.extra_counts["house_reports_scanned"] += 1
        tracker.extra_counts["house_stock_rows_skipped"] += report.get(
            "skipped_rows", 0
        )
        trades.extend(report["trades"])
        tracker.ok()
    # Even a failed collection keeps what it parsed, so the retry is cheaper.
    hc.save_cache(
        {**{d: o for d, o in cache.items() if _cached_clerk_report(o)}, **outcomes},
        cache_path,
    )
    return None if tracker.failure_count else trades


def collect_congress(
    session: Optional[requests.Session] = None,
    api_key: Optional[str] = None,
    sleep: Callable[[float], None] = time.sleep,
    *,
    settings: Optional[Settings] = None,
    max_pages: Optional[int] = None,
    house_source: Optional[str] = None,
    include_fmp_senate: Optional[bool] = None,
    house_cache_path: Optional[Path] = None,
) -> CollectionOutcome:
    """
    House trades from the Clerk of the House (or FMP), plus FMP's Senate page
    when enabled. Explicit arguments win, then settings. A caller with neither
    gets the FMP-only collection of both chambers on the free plan's one page.
    """
    fmp: FmpAdapter = _fetcher("fmp")
    if house_source is None:
        house_source = settings.congress_house_source if settings is not None else "fmp"
    if include_fmp_senate is None:
        include_fmp_senate = (
            settings.congress_include_fmp_senate if settings is not None else True
        )
    if house_source not in ("house_clerk", "fmp"):
        raise ValueError("house_source must be 'house_clerk' or 'fmp'")
    chambers = tuple(
        c
        for c, wanted in (
            ("senate", include_fmp_senate),
            ("house", house_source == "fmp"),
        )
        if wanted
    )

    key: Optional[str] = ""
    if chambers:
        if api_key is not None:
            key = api_key
        elif settings is not None and settings.fmp_api_key is not None:
            key = settings.fmp_api_key.get_secret_value()
        else:
            key = fmp.FMP_API_KEY
        if not key or key.lower().startswith("your_"):
            return CollectionOutcome.failure(
                "congress_trades", "live", "missing_credential"
            )
    if max_pages is not None:
        page_limit = max_pages
    elif settings is not None:
        page_limit = settings.fmp_max_pages
    else:
        page_limit = FREE_PLAN_PAGES
    session = session or requests.Session()
    # Initially one request per FMP chamber. Full pages discover further units
    # up to page_limit, and the House Clerk indexes add one unit per report.
    page_counts = {
        "senate_pages_fetched": 0,
        "house_pages_fetched": 0,
        "senate_rows_served": 0,
        "house_rows_served": 0,
        "fmp_calls": 0,
    }
    tracker = _Tracker(
        "Congress requests", planned=len(chambers), extra_counts=page_counts
    )
    raw_fmp = _collect_fmp_chambers(
        session, fmp, key or "", chambers, page_limit, tracker, sleep
    )
    if raw_fmp is None:
        return tracker.outcome("congress_trades", [])
    clerk: List[Dict[str, Any]] = []
    if house_source == "house_clerk":
        collected = _collect_house_clerk(session, tracker, sleep, house_cache_path)
        if collected is None:
            return tracker.outcome("congress_trades", [])
        clerk = collected
    try:
        trades = fmp.parse_trades(raw_fmp) + clerk
        if not all(_is_iso_date(t.get("transaction_date")) for t in trades):
            return tracker.parse_failure("congress_trades")
        trades = fmp.deduplicate_proven_repeats(trades)
    except (TypeError, ValueError, AttributeError, KeyError):
        return tracker.parse_failure("congress_trades")
    endpoints = [f"{fmp.BASE_URL}/{c}-latest" for c in chambers]
    if house_source == "house_clerk":
        endpoints.append(_fetcher("house_clerk").INDEX_URL)
    envelope = {
        "source": " + ".join(
            (["Financial Modeling Prep (FMP)"] if chambers else [])
            + (["Clerk of the House"] if house_source == "house_clerk" else [])
        ),
        "endpoints": endpoints,
        "fetched_at": _now_iso(),
        "total_trades": len(trades),
        "chambers": (["Senate"] if include_fmp_senate else []) + ["House"],
        "house_source": house_source,
        "house_reports": tracker.extra_counts.get("house_reports"),
        "house_reports_scanned": tracker.extra_counts.get("house_reports_scanned"),
    }
    return tracker.outcome(
        "congress_trades",
        trades,
        envelope=envelope,
        coverage={"complete": True, "scope": "recent_window"},
        source_as_of=_latest_iso_date([t.get("disclosure_date") for t in trades]),
    )


# ── Insider (Finnhub) ────────────────────────────────────


def _finnhub_payload_ok(raw: Any) -> bool:
    """Every field parse_finnhub_buys touches on a share purchase must be well-formed."""
    if not isinstance(raw, dict) or not isinstance(raw.get("data"), list):
        return False
    for txn in raw["data"]:
        if not isinstance(txn, dict):
            return False
        code = txn.get("transactionCode")
        if not isinstance(code, str):
            return False
        derivative = txn.get("isDerivative")
        if derivative is not None and not isinstance(derivative, bool):
            return False
        if code != "P" or derivative:
            continue
        if not _is_iso_date(txn.get("transactionDate")):
            return False
        for key in ("change", "transactionPrice"):
            value = txn.get(key)
            if value is not None and not _is_number(value):
                return False
        if not isinstance(txn.get("name"), str):
            return False
    return True


def _resolve_upstream(
    source: str, data_dir: Path, upstream: Any
) -> Tuple[Optional[UpstreamRows], Optional[CollectionOutcome]]:
    if upstream is None:
        return _read_upstream(source, data_dir)
    if not isinstance(upstream, dict) or not all(
        isinstance(upstream.get(key), list)
        and all(_upstream_row_ok(key, row) for row in upstream[key])
        for key in ("trades", "holdings")
    ):
        return None, CollectionOutcome.failure(source, "live", "missing_upstream")
    return upstream, None


def collect_insider(
    session: Optional[requests.Session] = None,
    sleep: Callable[[float], None] = time.sleep,
    *,
    upstream: Any = None,
    api_key: Optional[str] = None,
    settings: Optional[Settings] = None,
) -> CollectionOutcome:
    """Collect from explicit {trades, holdings} rows; None retains the legacy file fallback."""
    insider: InsiderAdapter = _fetcher("insider")
    upstream, failure = _resolve_upstream(
        "insider_trades", insider.LOCAL_DATA_DIR, upstream
    )
    if failure:
        return failure
    assert upstream is not None
    plan = _insider_plan(insider, upstream)
    if not plan:
        return CollectionOutcome.failure("insider_trades", "live", "no_input_tickers")
    if len(plan) > MAX_PLANNED_REQUESTS:
        return CollectionOutcome.failure("insider_trades", "live", "plan_too_large")
    if api_key is not None:
        key = api_key
    elif settings is not None and settings.finnhub_api_key is not None:
        key = settings.finnhub_api_key.get_secret_value()
    else:
        key = insider.FINNHUB_API_KEY
    if not key or key.lower().startswith("your_"):
        return CollectionOutcome.failure("insider_trades", "live", "missing_credential")

    session = session or requests.Session()
    tracker = _Tracker("insider tickers", planned=len(plan))
    buys: List[Dict[str, Any]] = []
    unsupported: List[str] = []
    cutoff = insider.lookback_cutoff().isoformat()
    today = datetime.now(timezone.utc).date().isoformat()
    for ticker, bucket in plan:
        symbol = insider.provider_symbol(ticker)
        if symbol is None:
            # No SEC Form 4 listing (e.g. a Canadian "/U" unit): nothing to ask.
            unsupported.append(ticker)
            tracker.ok()
            continue
        # The key travels in a header, never the query string.
        resp = _get_json(
            session,
            insider.FINNHUB_URL,
            params={"symbol": symbol, "from": cutoff, "to": today},
            headers={"X-Finnhub-Token": key},
            sleep=sleep,
        )
        if not resp.ok:
            tracker.fail(ticker, resp.code)
            break
        elif not _finnhub_payload_ok(resp.data):
            tracker.fail(ticker, "malformed_response")
            break
        else:
            parsed_buys, parse_code, _parse_detail = insider.parse_finnhub_buys(
                ticker, resp.data, bucket
            )
            if parse_code is not None or not isinstance(parsed_buys, list):
                tracker.fail(ticker, "parse_error")
                break
            buys.extend(parsed_buys)
            tracker.ok()
        sleep(insider.REQUEST_DELAY)
    envelope = {
        "source": insider.SOURCE_NAME,
        "fetched_at": _now_iso(),
        "lookback_days": insider.LOOKBACK_DAYS,
        "min_buy_value": insider.MIN_BUY_VALUE,
        "total_buys": len(buys),
        "tickers_with_buys": len({b["ticker"] for b in buys}),
        "unsupported_tickers": unsupported,
    }
    return tracker.outcome(
        "insider_trades",
        buys,
        envelope=envelope,
        # Coverage through the exact UTC date sent in every request, not the
        # newest buy in a changing universe. Pinned by the importer in batch
        # identity; same content on a later query day intentionally differs.
        source_as_of=date.fromisoformat(today),
        coverage={"complete": True, "scope": "selected_universe"},
    )


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
        if not all(
            isinstance(row.get(k), (str, int, float, type(None)))
            for k in ("interest", "avgDailyShareVolume", "daysToCover")
        ):
            return None
        if not isinstance(row.get("settlementDate", ""), str):
            return None
    return "rows"


def collect_short_interest(
    session: Optional[requests.Session] = None,
    sleep: Callable[[float], None] = time.sleep,
    *,
    upstream: Any = None,
) -> CollectionOutcome:
    """Collect from explicit {trades, holdings} rows; supplied data never reads files."""
    si: ShortInterestAdapter = _fetcher("short_interest")
    upstream, failure = _resolve_upstream("short_interest", si.LOCAL_DATA_DIR, upstream)
    if failure:
        return failure
    assert upstream is not None
    tickers = _short_interest_plan(si, upstream)
    if not tickers:
        return CollectionOutcome.failure("short_interest", "live", "no_input_tickers")
    if len(tickers) > MAX_PLANNED_REQUESTS:
        return CollectionOutcome.failure("short_interest", "live", "plan_too_large")
    session = session or requests.Session()
    tracker = _Tracker("short-interest tickers", planned=len(tickers))
    records: List[Dict[str, Any]] = []
    for ticker in tickers:
        resp = _get_json(
            session,
            si.NASDAQ_URL.format(symbol=ticker),
            headers=si.HEADERS,
            timeout=si.REQUEST_TIMEOUT,
            sleep=sleep,
        )
        kind = _short_interest_kind(resp.data) if resp.ok else None
        if not resp.ok:
            tracker.fail(ticker, resp.code)
            break
        elif kind is None:
            tracker.fail(ticker, "malformed_response")
            break
        elif kind == "none":
            tracker.ok()  # explicit "no short-interest record" from the provider
        else:
            record = si.parse_short_interest_strict(ticker, resp.data)
            if record is None:
                tracker.fail(ticker, "parse_error")
                break
            else:
                records.append(record)
                tracker.ok()
        sleep(si.REQUEST_DELAY)
    if not records and not tracker.failure_count:
        return CollectionOutcome.failure(
            "short_interest",
            "live",
            "empty_short_interest",
            diagnostics=tracker.diagnostics(),
        )
    records.sort(key=lambda r: r["ticker"])
    envelope = {
        "schema_version": "1.0",
        "source": "nasdaq",
        "fetched_at": _now_iso(),
        "total_records": len(records),
    }
    return tracker.outcome(
        "short_interest",
        records,
        envelope=envelope,
        source_as_of=_latest_iso_date([r.get("settlement_date") for r in records]),
        coverage={"complete": True, "scope": "selected_universe"},
    )


COLLECTORS: Dict[str, Collector] = {
    "congress_trades": collect_congress,
    "ark_holdings": collect_ark,
    "insider_trades": collect_insider,
    "short_interest": collect_short_interest,
}
