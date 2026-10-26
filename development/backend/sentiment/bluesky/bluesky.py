"""
Bluesky historical query: pulls past posts that mention ticker cashtags
using app.bsky.feed.searchPosts, instead of listening to the live Jetstream.

Each page of results returned by the API is filtered, then passed as one batch
to the sentiment scorer. Post text is not written to disk (the scorer needs a
file for its output, so a temporary one is used and deleted right after).

At the end of the run (or if it is interrupted or fails part-way) the scores are
written to a JSON file (default: sentiment_results.json next to this script,
override with --output; it is overwritten each run). Format: a list with one
object per ticker:
    [{"ticker": "NVDA", "average_sentiment": 6.12, "post_count": 2,
      "posts": {"did:plc:xxx/rkey1": 7.4, "did:plc:yyy/rkey2": 4.84}}, ...]
A post that mentions several tickers appears under each of them.

Only cashtags ($NVDA) count as a hit; company names alone do not.

Logging: API errors, retries and a brief line per successful request are written
to a rotating log file (default: bluesky_query.log next to this script, override
with --log-file). Timestamps are UTC. Post text and credentials are never logged.

Setup:
    pip install httpx python-dotenv
    Create a .env file next to this script (and add it to .gitignore):
        BSKY_HANDLE=you.bsky.social
        BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx   # create at bsky.app/settings/app-passwords

Run from the backend/ directory, as a module (so `src.utils` resolves):
    python -m sentiment.bluesky.bluesky_query                      # last 7 days
    python -m sentiment.bluesky.bluesky_query --since 2026-10-01 --until 2026-10-08
    python -m sentiment.bluesky.bluesky_query --tickers NVDA Apple --since 2026-10-20 --window-hours 6
    python -m sentiment.bluesky.bluesky_query --days 3 --time 30      # last 3 days, stop fetching after 30 min

--days N  looks back N days from --until (ignored when --since is given).
--time T  stops making API requests after T minutes; posts already fetched are
          still scored and the results file is written as usual.

--tickers accepts ticker symbols or company names (case-insensitive); names are
resolved to their symbol using the ticker file.
"""

import argparse
import asyncio
import json
import logging
import logging.handlers
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Make `src.utils` importable no matter where/how this file is launched.
# File lives at backend/sentiment/bluesky/, so backend/ is parents[2].
BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import httpx  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

# NOTE: adjust this import path to wherever sentiment_scorer.py lives in your project.
from sentiment.sentiment_scorer import (  # noqa: E402
    load_scores,
    score_texts,
)
from src.utils.console import stdout_logger  # noqa: E402
from src.utils.fetch_tickers import fetch_ticker_map  # noqa: E402

_out = stdout_logger()  # console lines (print() is banned in src/)

load_dotenv()  # reads BSKY_HANDLE / BSKY_APP_PASSWORD from a .env file next to the script

# ---------- Config ----------
PDS_URL = (
    "https://bsky.social/xrpc"  # authenticated; proxies searchPosts to the AppView
)
# symbol -> company name (same pairs as the ticker file, which is name -> symbol)
FALLBACK_COMPANIES = {
    "NVDA": "NVIDIA",
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "AMZN": "Amazon",
    "GOOGL": "Alphabet",
}
ENGLISH_ONLY = True
PAGE_LIMIT = 100  # API max per page
DEFAULT_LOOKBACK_DAYS = 7  # used when --since is not given
DEFAULT_WINDOW_HOURS = 24  # slice big ranges so deep pagination never caps out

OUTPUT_FILE = Path(__file__).with_name("bluesky_sentiments.json")

LOG_FILE = Path(__file__).with_name("bluesky_query.log")
LOG_MAX_BYTES = 5_000_000  # rotate at ~5 MB, keep 3 old files
LOG_BACKUPS = 3
BODY_SNIPPET = 200  # max chars of an error response body to log

# Same regex as the live listener: $TSLA, $nvda, but not "$5" or "$100".
CASHTAG_RE = re.compile(r"(?<![\w$])\$([A-Za-z]{1,5})\b")

log = logging.getLogger("bluesky_query")


class TimeUp(Exception):
    """The --time limit was reached: no more API requests are made."""


# ---------- Logging ----------
def setup_logging(path):
    """Log to a rotating file with UTC timestamps. Safe to call more than once."""
    if log.handlers:
        return
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(
        path, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUPS, encoding="utf-8"
    )
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ"
    )
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.propagate = False


def short_window(start, end):
    """Compact window label for log lines, e.g. 10-17T20:11..10-18T20:11."""
    f = "%m-%dT%H:%M"
    return f"{start.astimezone(timezone.utc).strftime(f)}..{end.astimezone(timezone.utc).strftime(f)}"


# ---------- Helpers ----------
def extract_tickers(text, allowed):
    return {m.upper() for m in CASHTAG_RE.findall(text)} & allowed


def uri_to_id(uri):
    """at://did:plc:xxx/app.bsky.feed.post/rkey -> did:plc:xxx/rkey (matches live IDs)."""
    did, _collection, rkey = uri.removeprefix("at://").split("/")
    return f"{did}/{rkey}"


def iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def parse_date(s):
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def windows(since, until, hours):
    step = timedelta(hours=hours)
    cur = since
    while cur < until:
        yield cur, min(cur + step, until)
        cur += step


# ---------- Scoring ----------
def _score_texts_temp(texts):
    """Run score_texts on `texts` and return {text: score}.

    score_texts only reports results by writing a file, so the output goes to a
    temporary directory that is deleted as soon as the scores are read back
    (also on error).
    """
    with tempfile.TemporaryDirectory(prefix="bsky_scores_") as tmp:
        path = os.path.join(tmp, "scores.jsonl")
        score_texts(texts, path, resume=False)
        return load_scores(path)


async def score_batch(posts, results):
    """Score one batch of fetched posts, record the scores in `results` and hand
    each scored post to handle_post.

    `results` is {ticker: {post_id: score}} and is updated in place.
    Scoring is blocking (model inference), so it runs in a worker thread to keep
    the event loop responsive. Returns the number of posts processed.
    """
    if not posts:
        return 0
    start = time.monotonic()
    try:
        scores = await asyncio.to_thread(_score_texts_temp, [p["text"] for p in posts])
    except Exception:
        log.exception("scoring failed for a batch of %d posts", len(posts))
        raise
    for post in posts:
        post["score"] = scores.get(post["text"])
        if post["score"] is not None:
            for ticker in post["tickers"]:
                results.setdefault(ticker, {})[post["id"]] = post["score"]
        await handle_post(post)
    log.info(
        "scored batch posts=%d elapsed=%.1fs", len(posts), time.monotonic() - start
    )
    return len(posts)


# ---------- Results file ----------
def build_report(results, tickers):
    """List of {ticker, average_sentiment, post_count, posts} for every queried ticker.

    Tickers with no posts are included with average_sentiment null.
    """
    report = []
    for ticker in sorted(tickers):
        posts = results.get(ticker, {})
        average = round(sum(posts.values()) / len(posts), 2) if posts else None
        report.append(
            {
                "ticker": ticker,
                "average_sentiment": average,
                "post_count": len(posts),
                "posts": posts,
            }
        )
    return report


def write_results(path, results, tickers):
    """Write the report as JSON. The file is replaced atomically, so a crash
    mid-write never leaves a half-written file. Never raises."""
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp")
    try:
        report = build_report(results, tickers)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
        log.info(
            "results written path=%s tickers=%d posts=%d",
            path,
            len(report),
            sum(r["post_count"] for r in report),
        )
        _out.info(f"Results saved to {path}")
    except Exception:
        log.exception("failed to write results to %s", path)
        _out.info(f"Failed to write results to {path} (see log).")
        try:
            tmp.unlink()
        except OSError:
            pass


# ---------- Pipeline ----------
async def handle_post(post):
    """
    post = {
        "id": "did:plc:xxx/rkey",   # unique, use for dedupe
        "tickers": ["NVDA"],
        "text": "...",
        "created_at": "2026-09-29T12:00:00.000Z",
        "ingested_at": 1234567890.0,
        "score": 7.42,              # 1 (most negative) .. 10 (most positive)
    }
    TODO: write to your database.
    """


# ---------- Bluesky client ----------
class BlueskyClient:
    def __init__(self, handle, app_password, deadline=None):
        self.handle = handle
        self.app_password = app_password
        self.http = httpx.AsyncClient(timeout=30)
        self.token = None
        self.deadline = deadline  # time.monotonic() value from --time, or None

    def time_up(self):
        return self.deadline is not None and time.monotonic() >= self.deadline

    async def _sleep(self, seconds):
        """Back-off sleep that never runs past the --time deadline."""
        if self.deadline is not None:
            seconds = min(seconds, max(0.0, self.deadline - time.monotonic()))
        if seconds > 0:
            await asyncio.sleep(seconds)

    async def close(self):
        await self.http.aclose()

    async def login(self):
        try:
            r = await self.http.post(
                f"{PDS_URL}/com.atproto.server.createSession",
                json={"identifier": self.handle, "password": self.app_password},
            )
        except httpx.TransportError as e:
            log.error("login failed (network): %s: %s", type(e).__name__, e)
            raise
        if r.status_code != 200:
            log.error(
                "login failed status=%s body=%s", r.status_code, r.text[:BODY_SNIPPET]
            )
            r.raise_for_status()
        self.token = r.json()["accessJwt"]
        log.info("login ok handle=%s", self.handle)

    async def get(self, method, params, retries=5):
        """GET with auto re-login on expired token and backoff on rate limits / 5xx / network errors."""
        for attempt in range(1, retries + 1):
            if self.time_up():
                raise TimeUp()
            try:
                r = await self.http.get(
                    f"{PDS_URL}/{method}",
                    params=params,
                    headers={"Authorization": f"Bearer {self.token}"},
                )
            except httpx.TransportError as e:
                log.warning(
                    "%s network error attempt=%d/%d: %s: %s",
                    method,
                    attempt,
                    retries,
                    type(e).__name__,
                    e,
                )
                await self._sleep(2 ** (attempt - 1))
                continue

            if r.status_code == 200:
                if attempt > 1:
                    log.info("%s succeeded after %d attempts", method, attempt)
                return r.json()
            if r.status_code in (400, 401) and "ExpiredToken" in r.text:
                log.warning(
                    "%s token expired, logging in again (attempt=%d/%d)",
                    method,
                    attempt,
                    retries,
                )
                await self.login()
                continue
            if r.status_code == 429:
                reset = r.headers.get("ratelimit-reset")
                wait = (
                    max(1, int(reset) - int(time.time()))
                    if reset and reset.isdigit()
                    else 2 ** (attempt - 1)
                )
                wait = min(wait, 300)
                log.warning(
                    "%s rate limited status=429 sleeping=%ss attempt=%d/%d",
                    method,
                    wait,
                    attempt,
                    retries,
                )
                _out.info(f"Rate limited, sleeping {wait}s")
                await self._sleep(wait)
                continue
            if r.status_code >= 500:
                log.warning(
                    "%s server error status=%s attempt=%d/%d body=%s",
                    method,
                    r.status_code,
                    attempt,
                    retries,
                    r.text[:BODY_SNIPPET],
                )
                await self._sleep(2 ** (attempt - 1))
                continue

            log.error(
                "%s request rejected status=%s body=%s",
                method,
                r.status_code,
                r.text[:BODY_SNIPPET],
            )
            r.raise_for_status()

        log.error("%s failed after %d attempts", method, retries)
        raise RuntimeError(f"{method} failed after {retries} attempts")

    async def search_posts(self, query, since, until):
        """Yield one list of posts per API page matching `query` within [since, until), following cursors."""
        cursor = None
        page_no = 0
        while True:
            params = {
                "q": query,
                "since": iso(since),
                "until": iso(until),
                "sort": "latest",
                "limit": PAGE_LIMIT,
            }
            if ENGLISH_ONLY:
                params["lang"] = "en"
            if cursor:
                params["cursor"] = cursor

            start = time.monotonic()
            data = await self.get("app.bsky.feed.searchPosts", params)
            elapsed = time.monotonic() - start

            posts = data.get("posts", [])
            cursor = data.get("cursor")
            page_no += 1
            log.info(
                "search ok q=%s window=%s page=%d posts=%d more=%s elapsed=%.2fs",
                query,
                short_window(since, until),
                page_no,
                len(posts),
                "yes" if cursor else "no",
                elapsed,
            )

            if posts:
                yield posts
            if not cursor or not posts:
                break


# ---------- Query runner ----------
async def run_query(tickers, since, until, window_hours, output_path, deadline=None):
    """tickers: iterable of ticker symbols, e.g. {"NVDA", "AAPL"}

    deadline: time.monotonic() value after which no more API requests are made
    (from --time); posts fetched before it are still scored and saved."""
    handle = os.environ.get("BSKY_HANDLE")
    password = os.environ.get("BSKY_APP_PASSWORD")
    if not handle or not password:
        log.error("missing BSKY_HANDLE / BSKY_APP_PASSWORD, cannot start")
        raise SystemExit("Set BSKY_HANDLE and BSKY_APP_PASSWORD in your .env file.")

    client = BlueskyClient(handle, password, deadline)

    allowed = frozenset(tickers)
    seen = (
        set()
    )  # a post mentioning $TSLA and $NVDA shows up in both searches; handle once
    count = 0
    results = {}  # {ticker: {post_id: score}}, written to the JSON file at the end
    run_start = time.monotonic()

    log.info(
        "run start tickers=%s since=%s until=%s window_hours=%d",
        sorted(allowed),
        iso(since),
        iso(until),
        window_hours,
    )

    try:
        await client.login()
        for ticker in sorted(allowed):
            for w_start, w_end in windows(since, until, window_hours):
                fetched = 0
                kept = 0
                async for page in client.search_posts(f"${ticker}", w_start, w_end):
                    fetched += len(page)
                    batch = []
                    for p in page:
                        post_id = uri_to_id(p["uri"])
                        if post_id in seen:
                            continue

                        record = p.get("record", {})
                        text = record.get("text", "")
                        if not text:
                            continue
                        if ENGLISH_ONLY and "en" not in record.get("langs", ["en"]):
                            continue

                        # Search is fuzzy (may match plain "TSLA"), so re-apply the strict cashtag filter.
                        found = extract_tickers(text, allowed)
                        if not found:
                            continue

                        seen.add(post_id)
                        batch.append(
                            {
                                "id": post_id,
                                "tickers": sorted(found),
                                "text": text,
                                "created_at": record.get("createdAt"),
                                "ingested_at": time.time(),
                            }
                        )

                    # One scoring call per fetched API page.
                    kept += len(batch)
                    count += await score_batch(batch, results)
                log.info(
                    "window done ticker=%s window=%s fetched=%d new=%d total=%d",
                    ticker,
                    short_window(w_start, w_end),
                    fetched,
                    kept,
                    count,
                )
                _out.info(
                    f"  {ticker} {iso(w_start)} -> {iso(w_end)} done (total {count})"
                )
    except TimeUp:
        log.info("time limit reached, no more API requests (%d posts scored)", count)
        _out.info(f"Time limit reached: stopped fetching ({count} posts scored).")
    except Exception:
        log.exception("run aborted after %d posts", count)
        raise
    finally:
        await client.close()
        write_results(output_path, results, allowed)

    log.info(
        "run finished unique_posts=%d elapsed=%.1fs",
        count,
        time.monotonic() - run_start,
    )
    _out.info(f"Finished: {count} unique posts.")


# ---------- Ticker loading ----------
SYMBOL_RE = re.compile(r"^[A-Z]{1,5}$")  # what CASHTAG_RE can actually match
NAME_TO_SYMBOL = {n.lower(): s for s, n in FALLBACK_COMPANIES.items()}


def parse_companies(raw):
    """Normalise the ticker data into {SYMBOL: company name}.

    Structure is {"NVIDIA": "NVDA", "Apple": "AAPL", ...}: the key is the
    company name and the value is the ticker symbol. A value that is not a valid
    1-5 letter symbol is skipped with a warning, since a cashtag could never
    match it.
    """
    if not isinstance(raw, dict):
        raise TypeError(
            f"Expected a {{company name: ticker}} dict, got {type(raw).__name__}."
        )

    out = {}
    for name, sym in raw.items():
        name = str(name).strip()
        sym = str(sym).upper().lstrip("$").strip()
        if SYMBOL_RE.match(sym):
            out[sym] = name
        else:
            log.warning(
                "skipping ticker entry %r: %r is not a valid symbol (1-5 letters)",
                name,
                sym,
            )
            _out.info(
                f"Skipping {name!r}: {sym!r} is not a valid ticker symbol (1-5 letters)."
            )
    return out


async def load_tickers(cli_tickers):
    """Return the set of ticker symbols to query.

    The ticker file is always loaded so --tickers can be given as either a
    symbol (NVDA) or a company name (Nvidia). Unknown values are treated as
    plain symbols.
    """
    companies = {}
    source = "file"
    try:
        fetched = await fetch_ticker_map()
        if fetched:
            companies = parse_companies(fetched)
    except Exception as e:
        log.warning("ticker fetch failed, using fallback: %s: %s", type(e).__name__, e)
        _out.info(f"Ticker fetch failed, using fallback: {e}")
    if not companies:
        companies = dict(FALLBACK_COMPANIES)
        source = "fallback"
    log.info("tickers loaded source=%s count=%d", source, len(companies))

    if not cli_tickers:
        return set(companies)

    by_name = {n.lower(): s for s, n in companies.items() if n}
    selected = set()
    for t in cli_tickers:
        key = t.strip().lstrip("$")
        sym = key.upper()
        if sym in companies:
            selected.add(sym)
        elif key.lower() in by_name:
            selected.add(by_name[key.lower()])
        else:
            selected.add(sym)
    return selected


def main():
    ap = argparse.ArgumentParser(
        description="Query past Bluesky posts for ticker cashtags and score their sentiment."
    )
    ap.add_argument(
        "--tickers",
        nargs="+",
        help="Tickers or company names to query (default: everything in the ticker file, else fallback list)",
    )
    ap.add_argument(
        "--since",
        help="Start, e.g. 2026-10-01 or 2026-10-01T12:00:00 "
        f"(default: {DEFAULT_LOOKBACK_DAYS} days ago)",
    )
    ap.add_argument("--until", help="End (default: now)")
    ap.add_argument(
        "--days",
        type=float,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Look back this many days from --until when --since is not given "
        f"(default: {DEFAULT_LOOKBACK_DAYS})",
    )
    ap.add_argument(
        "--time",
        type=float,
        default=None,
        help="Stop making API requests after this many minutes; posts already "
        "fetched are scored and saved (default: no limit)",
    )
    ap.add_argument(
        "--window-hours",
        type=int,
        default=DEFAULT_WINDOW_HOURS,
        help="Slice the range into windows of this size",
    )
    ap.add_argument(
        "--output",
        default=str(OUTPUT_FILE),
        help=f"JSON file for the sentiment results (default: {OUTPUT_FILE.name} next to this script; overwritten each run)",
    )
    ap.add_argument(
        "--log-file",
        default=str(LOG_FILE),
        help=f"Where to write the log (default: {LOG_FILE.name} next to this script)",
    )
    args = ap.parse_args()
    if args.days <= 0:
        ap.error("--days must be greater than 0")
    if args.time is not None and args.time <= 0:
        ap.error("--time must be greater than 0")
    deadline = time.monotonic() + args.time * 60 if args.time is not None else None

    setup_logging(args.log_file)

    until = parse_date(args.until) if args.until else datetime.now(timezone.utc)
    since = parse_date(args.since) if args.since else until - timedelta(days=args.days)

    async def go():
        tickers = await load_tickers(args.tickers)
        _out.info(f"Querying {sorted(tickers)} from {iso(since)} to {iso(until)}")
        await run_query(tickers, since, until, args.window_hours, args.output, deadline)

    try:
        asyncio.run(go())
    except KeyboardInterrupt:
        log.info("run stopped by user")
        _out.info("Stopped.")


if __name__ == "__main__":
    main()
