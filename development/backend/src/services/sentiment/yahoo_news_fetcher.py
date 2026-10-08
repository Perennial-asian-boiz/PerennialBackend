"""
Fetch recent Yahoo Finance news for a list of tickers.

Each ticker is queried once; the lookback window is applied locally because
yfinance.Search has no server-side date filtering.

dependencies: yfinance

Usage:
    python yahoo_news.py [--print]
"""
import asyncio
import hashlib
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import yfinance as yf

try:  # present in newer yfinance releases
    from yfinance.exceptions import YFRateLimitError
except ImportError:  # older versions: define a stand-in so `except` clauses still work
    class YFRateLimitError(Exception):
        pass

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # insert src dir in path so utils dir can be accessed
from utils.fetch_tickers import fetch_tickers

MAX_CONCURRENCY = 1        # simultaneous in-flight requests
MIN_INTERVAL = 2.0        # minimum seconds between request *starts* (global)
JITTER = 1.0               # extra random 0..JITTER seconds added to each interval
MAX_RETRIES = 3            # retries per ticker after the first attempt
BACKOFF_BASE = 5.0         # seconds; doubles each retry on rate limiting
BACKOFF_CAP = 60.0        # never wait longer than this for one backoff
TRANSIENT_BACKOFF = 1.5    # base seconds for network/timeouts (non rate-limit)
REQUEST_TIMEOUT = 30.0     # seconds before we stop waiting on a single request
NEWS_COUNT = 100
LOOKBACK_DAYS = 7


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------
class RateLimiter:
    """
    Global async rate limiter shared by all workers.

    - Spaces request starts at least MIN_INTERVAL (+ jitter) apart.
    - Supports a shared cooldown: when any worker is rate limited, *all*
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
      3. Deterministic hash of title + publisher + UTC publication timestamp
                                                -> "hash:<sha256>"
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


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------
# NOTE ON DATE FILTERING: yfinance.Search has NO server-side start/end date
# parameters, so every call returns the latest NEWS_COUNT articles regardless
# of date. We query once per ticker and apply the lookback window locally in
# fetch_news(). For very busy tickers, NEWS_COUNT may not reach back the full
# LOOKBACK_DAYS.
def _search_news_blocking(ticker: str, count: int) -> list[dict]:
    """yfinance is synchronous (it does the HTTP call inside the constructor), so this runs in a worker thread."""
    search = yf.Search(
        ticker,
        news_count=count,
        max_results=1,
        raise_errors=True,
    )
    return search.news or []


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
                raw = await asyncio.wait_for(
                    asyncio.to_thread(_search_news_blocking, ticker, count),
                    timeout=REQUEST_TIMEOUT,
                )
                if not raw: # retry if 0 raw articles to make sure there are actually 0
                    if attempt == MAX_RETRIES:
                        print(f"[{ticker}] Yahoo returned empty news after all retries", file=sys.stderr)
                        return []

                    delay = min(BACKOFF_CAP, TRANSIENT_BACKOFF * (2 ** attempt))
                    print(
                        f"[{ticker}] Yahoo returned empty news; retrying in {delay:.1f}s",
                        file=sys.stderr,
                    )
                    await asyncio.sleep(delay)
                    continue
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

    # de-duplicate (preserving order) so we never spend requests twice on the same symbol
    tickers = list(dict.fromkeys(tickers))
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    limiter = RateLimiter(MIN_INTERVAL, JITTER)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    # all tickers run concurrently, but the limiter + semaphore pace the actual requests
    results = await asyncio.gather(
        *(fetch_news(tk, since, NEWS_COUNT, limiter, sem) for tk in tickers),
        return_exceptions=True,  # one bad ticker shouldn't kill the run
    )

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