"""
Fetch recent Yahoo Finance news for a list of tickers.

Each ticker is queried once; the lookback window is applied locally because
yfinance.Search has no server-side date filtering.

Usage:
    python yahoo_news.py
"""
import asyncio
import hashlib
import logging
import random
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import yfinance as yf

try:  # present in newer yfinance releases
    from yfinance.exceptions import YFRateLimitError
except ImportError:  # older versions: define a stand-in so `except` clauses still work
    class YFRateLimitError(Exception):
        pass

# YfData contains session information and must be reset to fetch new results from the API
try:  # internals used by the spy and the session reset; both degrade gracefully if missing
    from yfinance.data import YfData, SingletonMeta
except ImportError:
    YfData = SingletonMeta = None

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # insert src dir in path so utils dir can be accessed
from utils.fetch_tickers import fetch_tickers

MAX_CONCURRENCY = 5        # simultaneous in-flight requests
MIN_INTERVAL = 2.0        # minimum seconds between request *starts* (global)
JITTER = 1.0               # extra random 0..JITTER seconds added to each interval
MAX_RETRIES = 3            # retries per ticker after the first attempt
BACKOFF_BASE = 5.0         # seconds; doubles each retry on rate limiting
BACKOFF_CAP = 30.0        # never wait longer than this for one backoff
TRANSIENT_BACKOFF = 1.5    # base seconds for network/timeouts (non rate-limit)
REQUEST_TIMEOUT = 30.0     # seconds before we stop waiting on a single request
NEWS_COUNT = 100
LOOKBACK_DAYS = 7

# Diagnostics / recovery switches
SPY_SEARCH_RESPONSES = True     # log status, size, latency, headers and body shape of every search response
SPY_ALL_HEADERS = False         # True: dump every response header instead of a short whitelist
SPY_LOG_FILE = Path(__file__).with_name("yahoo_spy.log")  # spy output goes here (appended), not to the terminal
RESET_SESSION_ON_EMPTY = True   # when a response has 0 articles, retry on a brand-new yfinance session (up to MAX_RETRIES times); False = retry on the same session (diagnostics only)



# Wraps YfData.get so every call to Yahoo's search endpoint prints what came
# back. Because yfinance's lru_cache sits ABOVE get(), a retry that is served
# from that cache prints no line at all, which is itself diagnostic.
_SPY_HEADERS = ("date", "age", "via", "server", "cache-control", "x-cache",
                "x-yahoo-request-id", "content-type", "content-encoding")
_tls = threading.local()  # tells the spy which ticker/attempt/generation this worker thread is serving


def _describe_body(resp) -> str:
    try:
        body = resp.json()
    except Exception:
        text = (getattr(resp, "text", "") or "")[:160].replace("\n", " ")
        return f"non-JSON body: {text!r}"
    if not isinstance(body, dict):
        return f"JSON {type(body).__name__}"
    news, quotes = body.get("news"), body.get("quotes")
    fin = body.get("finance")
    err = fin.get("error") if isinstance(fin, dict) else None
    return (f"keys={sorted(body)} "
            f"news={len(news) if isinstance(news, list) else news!r} "
            f"quotes={len(quotes) if isinstance(quotes, list) else quotes!r}"
            + (f" error={err}" if err else ""))


_spy_logger = logging.getLogger("yahoo_news.spy")


def _setup_spy_logger() -> None:
    """Route spy output to SPY_LOG_FILE only (idempotent; thread-safe because logging handlers lock)."""
    if _spy_logger.handlers:
        return
    handler = logging.FileHandler(SPY_LOG_FILE, encoding="utf-8")  # append mode; flushes on every record
    formatter = logging.Formatter("%(asctime)sZ %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    formatter.converter = time.gmtime  # UTC timestamps
    handler.setFormatter(formatter)
    _spy_logger.addHandler(handler)
    _spy_logger.setLevel(logging.INFO)
    _spy_logger.propagate = False  # keep it out of the root logger / terminal


def _spy_log(message: str) -> None:
    _spy_logger.info(f"[{getattr(_tls, 'ctx', '?')}] {message}")


def _install_search_spy() -> None:
    if YfData is None:
        print("[spy] yfinance.data.YfData not available; spy disabled", file=sys.stderr)
        return
    if getattr(YfData, "_yn_spy_installed", False):
        return
    _setup_spy_logger()
    print(f"[spy] logging search responses to {SPY_LOG_FILE}", file=sys.stderr)
    original_get = YfData.get

    def spy_get(self, url, *args, **kwargs):
        if "finance/search" not in str(url):
            return original_get(self, url, *args, **kwargs)
        t0 = time.perf_counter()
        try:
            resp = original_get(self, url, *args, **kwargs)
        except Exception as e:
            _spy_log(f"raised {type(e).__name__}: {e} after {time.perf_counter() - t0:.2f}s")
            raise
        try:  # logging must never break fetching
            hdrs = resp.headers
            shown = dict(hdrs) if SPY_ALL_HEADERS else {k: hdrs.get(k) for k in _SPY_HEADERS if hdrs.get(k) is not None}
            _spy_log(f"status={resp.status_code} bytes={len(resp.content)} "
                     f"t={time.perf_counter() - t0:.2f}s {_describe_body(resp)} headers={shown}")
        except Exception as e:
            _spy_log(f"(could not describe response: {type(e).__name__}: {e})")
        return resp

    YfData.get = spy_get
    YfData._yn_spy_installed = True


# yfinance keeps ONE YfData singleton: one curl_cffi session (connection pool,
# cookies) plus an in-memory crumb. Popping it from the singleton registry
# makes the next yf.Search() build a fresh one. The old session is NOT closed
# immediately because another worker thread may still be using it; it is
# retired and closed at shutdown. These are private yfinance internals and may
# change between versions, so every step is guarded.
_state_lock = threading.Lock()
_generation = 0                # incremented on every successful reset
_retired_sessions: list = []


def _clear_yf_cache() -> bool:
    """Drop yfinance's in-process response cache so the next identical request really hits Yahoo."""
    if YfData is None:
        return False
    try:
        YfData.cache_get.cache_clear()
        return True
    except AttributeError:
        return False


def _reset_yf_state(seen_generation: int) -> str:
    """
    Reset yfinance's singleton session so the retry runs on a brand-new one.
    `seen_generation` is the generation the failing request ran under: if another
    task already reset since then, the retry will use that fresh session anyway,
    so this call does nothing. There is deliberately NO minimum interval between
    resets: skipping one would force the retry onto the same session that just
    returned the empty (cached) response.
    Returns a short human-readable outcome for logging.
    """
    global _generation
    if YfData is None or SingletonMeta is None:
        return "reset unsupported by this yfinance version"
    _clear_yf_cache()  # always, so the retry is a real request even if the reset below is skipped
    with _state_lock:
        if _generation != seen_generation:
            return f"already reset to gen {_generation} by another task"
        try:
            with SingletonMeta._lock:
                old = SingletonMeta._instances.pop(YfData, None)
            session = getattr(old, "_session", None)
            if session is not None:
                _retired_sessions.append(session)
        except Exception as e:
            return f"reset failed: {type(e).__name__}: {e}"
        _generation += 1
        return f"reset -> gen {_generation}"


def _close_retired_sessions() -> None:
    while _retired_sessions:
        session = _retired_sessions.pop()
        try:
            session.close()
        except Exception as e:
            print(f"[yfinance] error closing retired session: {type(e).__name__}: {e}", file=sys.stderr)


class RateLimiter:
    """
    Global async rate limiter shared by all workers.
    Spaces request starts at least MIN_INTERVAL (+ jitter) apart.
    Supports a shared cooldown: when any worker is rate limited, *all*
    workers pause, instead of each one hammering Yahoo independently.
    """

    def __init__(self, min_interval: float, jitter: float):
        self._min_interval = min_interval
        self._jitter = jitter
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0
        self._cooldown_until = 0.0

    async def acquire(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            async with self._lock:
                now = loop.time()
                wait = max(self._next_allowed, self._cooldown_until) - now
                if wait <= 0:
                    self._next_allowed = now + self._min_interval + random.uniform(0, self._jitter)
                    return
            # sleep outside the lock, then re-check
            await asyncio.sleep(wait)

    def penalize(self, seconds: float) -> None:
        """Pause every worker for <seconds> (extends, never shortens, an existing cooldown)."""
        loop = asyncio.get_running_loop()
        self._cooldown_until = max(self._cooldown_until, loop.time() + seconds)


def _is_rate_limited(exc: BaseException) -> bool:
    if isinstance(exc, YFRateLimitError):
        return True
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg or "rate limit" in msg


def _to_utc(value) -> datetime | None:
    """
    Convert an ISO-8601 string, epoch seconds/milliseconds or datetime into a timezone-aware UTC datetime.
    Returns None if the value is missing or unparseable. Naive values are assumed to already be UTC.
    """
    if value is None or value == "":
        return None
    try:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, (int, float)):
            ts = float(value)
            if ts > 1e11:  # looks like milliseconds
                ts /= 1000
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        else:
            s = str(value).strip()
            if s.isdigit():
                return _to_utc(int(s))
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, OverflowError, OSError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _canonical_url(url) -> str | None:
    """trim, lowercase scheme/host, drop fragment and trailing slash."""
    if not url or not isinstance(url, str) or not url.strip():
        return None
    parts = urlsplit(url.strip())
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, parts.query, ""))


def _norm_text(value) -> str:
    return " ".join(str(value or "").split()).casefold()


def make_dedupe_key(yahoo_id, url, title, publisher, published: datetime) -> str:
    """
    Strongest available identity, in priority order:
      1. Yahoo article ID                       -> "id:<id>"
      2. Canonical URL                          -> "url:<url>"
      3. Deterministic hash of title + publisher + UTC publication timestamp -> "hash:<sha256>"
    Prefixes keep the three namespaces from ever colliding with each other.
    """
    if yahoo_id is not None and str(yahoo_id).strip():
        return f"id:{str(yahoo_id).strip()}"
    canon = _canonical_url(url)
    if canon:
        return f"url:{canon}"
    basis = "\x1f".join((
        _norm_text(title),
        _norm_text(publisher),
        published.astimezone(timezone.utc).isoformat(),
    ))
    return "hash:" + hashlib.sha256(basis.encode("utf-8")).hexdigest()


def normalize(item: dict) -> dict | None:
    """
    yfinance has returned news in two shapes over time:
      new: {"id": ..., "content": {"title", "pubDate", "canonicalUrl": {"url"}, "provider": {"displayName"}, ...}}
      old: {"uuid": ..., "title", "link", "publisher", "providerPublishTime"}
    This flattens both into one structure. Fields are looked up under item["content"]
    first and then at the top level, so Search results in either shape work.
    """
    if not isinstance(item, dict):
        return None
    content = item.get("content")
    if not isinstance(content, dict):
        content = item

    published = (
        _to_utc(content.get("pubDate"))
        or _to_utc(content.get("displayTime"))
        or _to_utc(content.get("providerPublishTime"))
        or _to_utc(item.get("providerPublishTime"))
    )
    if published is None:
        return None  # can't apply the date filter without a timestamp

    url = (
        (content.get("canonicalUrl") or {}).get("url")
        or (content.get("clickThroughUrl") or {}).get("url")
        or content.get("link")
        or item.get("link")
    )
    provider = content.get("provider")
    publisher = (
        (provider.get("displayName") if isinstance(provider, dict) else None)
        or content.get("publisher")
        or item.get("publisher")
    )
    title = content.get("title") or item.get("title")
    if not title:
        return None

    yahoo_id = item.get("id") or item.get("uuid") or content.get("id") or content.get("uuid")

    return {
        "id": yahoo_id or url or title,
        "title": title,
        "publisher": publisher,
        "url": url,
        "published": published,
        "summary": content.get("summary") or "",
        "dedupe_key": make_dedupe_key(yahoo_id, url, title, publisher, published),
    }


def dedupe_and_sort(articles: list[dict]) -> list[dict]:
    """First occurrence of each dedupe_key wins; result is newest first """
    unique: dict[str, dict] = {}
    for art in articles:
        unique.setdefault(art["dedupe_key"], art)
    return sorted(unique.values(), key=lambda a: (a["published"], a["dedupe_key"]), reverse=True)



# yfinance.Search has NO server-side start/end date parameters,
# so every call returns the latest NEWS_COUNT (yahoo limits internally to 50) articles regardless
# of date. We query once per ticker and apply the lookback window locally in
# fetch_news().
def _search_news_blocking(ticker: str, count: int, attempt: int = 0) -> tuple[list[dict], int]:
    """
    yfinance is synchronous (it does the HTTP call inside the constructor), so this runs in a worker thread.
    Returns (raw articles, session generation the request ran under).
    """
    gen = _generation
    _tls.ctx = f"{ticker} a{attempt} gen{gen}"  # read by the response spy
    search = yf.Search(
        ticker,
        news_count=count,
        max_results=1,
        raise_errors=True,
    )
    return (search.news or []), gen


async def fetch_news(
    ticker: str,
    since: datetime,
    count: int,
    limiter: RateLimiter,
    sem: asyncio.Semaphore,
) -> list[dict]:
    last_exc: BaseException | None = None

    for attempt in range(MAX_RETRIES + 1):
        await limiter.acquire()  # global spacing + shared cooldown
        try:
            async with sem:  # cap in-flight requests
                raw, gen = await asyncio.wait_for(
                    asyncio.to_thread(_search_news_blocking, ticker, count, attempt), # wrap synch search call in a thread for concurrency
                    timeout=REQUEST_TIMEOUT,
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            last_exc = e
            if attempt == MAX_RETRIES:
                break
            if _is_rate_limited(e):
                # exponential backoff + jitter, applied to ALL workers via the limiter
                delay = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** attempt)) + random.uniform(0, 2)
                limiter.penalize(delay)
                print(f"[{ticker}] rate limited, pausing {delay:.1f}s "
                      f"(retry {attempt + 1}/{MAX_RETRIES})", file=sys.stderr)
            else:
                # timeouts / connection hiccups: shorter, per-ticker backoff
                delay = min(BACKOFF_CAP, TRANSIENT_BACKOFF * (2 ** attempt)) + random.uniform(0, 1)
                print(f"[{ticker}] {type(e).__name__}: {e}; retrying in {delay:.1f}s "
                      f"(retry {attempt + 1}/{MAX_RETRIES})", file=sys.stderr)
                await asyncio.sleep(delay)
            continue

        if not raw:  # 0 raw articles: retry (at most MAX_RETRIES times) on a NEW session, never the same one
            # Retrying on the same session just replays Yahoo's cached empty response.
            if attempt == MAX_RETRIES:
                print(f"[{ticker}] Yahoo returned empty news after all retries", file=sys.stderr)
                return []

            delay = min(BACKOFF_CAP, TRANSIENT_BACKOFF * (2 ** attempt))
            note = f"; session {_reset_yf_state(gen)}" if RESET_SESSION_ON_EMPTY else ""
            print(f"[{ticker}] Yahoo returned empty news (gen {gen}){note}; "
                  f"retrying in {delay:.1f}s (retry {attempt + 1}/{MAX_RETRIES})", file=sys.stderr)
            await asyncio.sleep(delay)
            continue

        print(f"[{ticker}] raw articles: {len(raw)}", file=sys.stderr)  # diagnostic
        articles = []
        for item in raw:
            try:
                art = normalize(item)
            except Exception as e:  # one malformed item shouldn't discard the whole response
                print(f"[{ticker}] skipped malformed article: {e}", file=sys.stderr)
                continue
            if art and art["published"] >= since:  # local date filter
                articles.append(art)
        return dedupe_and_sort(articles)

    raise RuntimeError(f"giving up after {MAX_RETRIES + 1} attempts") from last_exc


def print_report(by_ticker: dict[str, list[dict]]) -> None:
    for tk, arts in by_ticker.items():
        print(f"=== {tk} ({len(arts)} article{'s' if len(arts) != 1 else ''}) ===")
        for a in arts:
            print(f"- [{a['published']:%Y-%m-%d %H:%M} UTC] {a['title']}")
            print(f"  {a['publisher'] or 'Unknown source'} | {a['url'] or 'no link'}")
        print()


async def main() -> dict[str, list[dict]]:
    tickers = await fetch_tickers()
    if not tickers:
        print("No tickers found")
        return {}

    if SPY_SEARCH_RESPONSES:
        _install_search_spy()

    # de-duplicate (preserving order) so we never spend requests twice on the same symbol
    tickers = list(dict.fromkeys(tickers))
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    limiter = RateLimiter(MIN_INTERVAL, JITTER)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    try:
        # all tickers run concurrently, but the limiter + semaphore pace the actual requests
        results = await asyncio.gather(
            *(fetch_news(tk, since, NEWS_COUNT, limiter, sem) for tk in tickers),
            return_exceptions=True,  # one bad ticker shouldn't kill the run
        )
    finally:
        _close_retired_sessions()

    by_ticker: dict[str, list[dict]] = {}
    for tk, res in zip(tickers, results):
        if isinstance(res, BaseException):
            print(f"[{tk}] fetch failed: {res}", file=sys.stderr)
            by_ticker[tk] = []
        else:
            by_ticker[tk] = res

    if "--print" in sys.argv:
        print_report(by_ticker)
    return by_ticker


if __name__ == "__main__":
    asyncio.run(main())