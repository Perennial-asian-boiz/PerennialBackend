"""
Async Reddit scraper (no Reddit API) - posts + comments from the last N days.

Design
------
* All subreddits are listed concurrently (one producer task per subreddit).
* Each post found goes on a queue; a worker pool fetches comment pages while
  the listings are still being crawled.
* A semaphore caps in-flight requests; any throttling triggers a global
  cooldown so every task pauses together.
* Circuit breaker: N consecutive blocked responses (403/429, or a 200 that is
  not real old-Reddit HTML) stops the run early; everything collected is saved.
* Data is checkpointed periodically, on early stop, on Ctrl+C and at the end.
* The first few unexpected responses are saved to output/debug_response_N.html.
* Collection modes: crawl every subreddit's /new, search per ticker, or both
  (auto = search when the ticker list is small). /hot and /rising are also read,
  so stickied daily threads and older posts with fresh comments are not missed.
  Daily / very large threads get their comments fetched under two sorts.
* After scraping, process_data() drops noise, finds which tickers each post /
  comment is about (reddit_processing.py), scores the relevant text with
  score_items() and writes per-ticker rows plus daily / summary aggregates.

Cookie file (required)
----------------------
Reddit redirects logged-out visitors to a login page, so the scraper REQUIRES a
cookie file from a logged-in browser session and refuses to start without it

Run
---
  python reddit.py --max-posts 3
  python reddit.py --days 7
  python reddit.py --backend playwright --headed --max-posts 3
  python reddit.py --no-comments
  python reddit.py --tickers-file backend/data/tickers.json
  python reddit.py --mode search --search-pages 3
  python reddit.py --no-sentiment

Output (./output): posts.csv, comments.csv, reddit_data.json, run_summary.json,
                   sentiment_item_scores.jsonl (raw model output, resumable),
                   sentiment_rows.csv (one row per post/comment x ticker),
                   ticker_sentiment_daily.csv, ticker_sentiment_summary.csv
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, NamedTuple
from urllib.parse import urlencode, urlparse

from bs4 import BeautifulSoup

try:
    from ..sentiment_scorer import load_score_records, score_items   # as a package
    from .reddit_processing import (
        DAILY_FIELDS, ROW_FIELDS, SUMMARY_FIELDS, TickerMatcher, aggregate_rows,
        build_scoring_items, is_discussion_title, load_json_file, load_tickers,
        write_csv_atomic)
except ImportError:                                                  # run as a script
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from sentiment_scorer import load_score_records, score_items
    from reddit_processing import (
        DAILY_FIELDS, ROW_FIELDS, SUMMARY_FIELDS, TickerMatcher, aggregate_rows,
        build_scoring_items, is_discussion_title, load_json_file, load_tickers,
        write_csv_atomic)

SUBREDDITS = [
    "stocks",
    "investing",
    "StockMarket",
    "wallstreetbets",
    "options",
    "Daytrading",
    "pennystocks",
    "ValueInvesting",
    "SecurityAnalysis",
]

BASE = "https://old.reddit.com"
CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)

POST_FIELDS = [
    "subreddit", "post_id", "title", "author", "created_utc", "score",
    "num_comments", "flair", "domain", "url", "permalink", "selftext",
    "comments_scraped",
]
COMMENT_FIELDS = [
    "subreddit", "post_id", "comment_id", "parent_id", "author",
    "created_utc", "score", "body", "permalink",
]


# --------------------------------------------------------------------------- #
# Backends: all they do is turn a URL into (status, headers info, final url, html)
# --------------------------------------------------------------------------- #
class Resp(NamedTuple):
    status: int
    retry_after: str | None
    final_url: str
    redirected: bool
    text: str


REDDIT_SESSION_COOKIES = ("reddit_session", "token_v2")

class CookieFileError(Exception):
    pass


def parse_cookie_header(header: str):
    """'a=1; b=2' -> [{'name': 'a', 'value': '1'}, ...]"""
    out = []
    for part in header.split(";"):
        if "=" in part:
            name, value = part.strip().split("=", 1)
            if name:
                out.append({"name": name, "value": value})
    return out


def _to_expiry(v):
    """Normalise an expiry (seconds, ms, or missing/0/-1 = session) to seconds or None."""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    return v / 1000 if v > 1e11 else v


def load_cookie_file(path: str) -> list[dict]:
    """
    Read a browser cookie export (Netscape / JSON / raw Cookie header line) and
    return the reddit.com cookies as dicts: name, value, domain, path, secure, expires.
    Raises CookieFileError if the file is missing, empty or has no usable cookies.
    """
    if not os.path.isfile(path):
        raise CookieFileError(f"cookie file not found: {path}\n")
    with open(path, encoding="utf-8-sig") as f:
        text = f.read().strip()
    if not text:
        raise CookieFileError(f"cookie file is empty: {path}\n")

    raw: list[dict] = []
    if text[0] in "[{":                                    # ---- JSON
        try:
            obj = json.loads(text)
        except json.JSONDecodeError as e:
            raise CookieFileError(f"{path} looks like JSON but can't be parsed: {e}")
        items = obj.get("cookies", []) if isinstance(obj, dict) else obj
        for c in items:
            if isinstance(c, dict) and c.get("name"):
                raw.append({
                    "name": c["name"], "value": str(c.get("value", "")),
                    "domain": c.get("domain") or ".reddit.com",
                    "path": c.get("path") or "/",
                    "secure": bool(c.get("secure", True)),
                    "expires": _to_expiry(c.get("expirationDate", c.get("expires",
                                                                      c.get("expiry")))),
                })
    else:
        lines = [ln.rstrip("\r") for ln in text.splitlines()]
        if any(len(ln.split("\t")) >= 7 for ln in lines):  # ---- Netscape cookies.txt
            for ln in lines:
                ln = ln.strip()
                if ln.startswith("#HttpOnly_"):
                    ln = ln[len("#HttpOnly_"):]
                elif not ln or ln.startswith("#"):
                    continue
                parts = ln.split("\t")
                if len(parts) >= 7:
                    domain, _flag, cpath, secure, expiry, name, value = parts[:7]
                    raw.append({
                        "name": name, "value": value, "domain": domain,
                        "path": cpath or "/", "secure": secure.strip().upper() == "TRUE",
                        "expires": _to_expiry(expiry),
                    })
        else:                                              # ---- raw Cookie header
            header = text.splitlines()[0] if "\n" not in text.strip() else text.replace("\n", ";")
            if header.lower().startswith("cookie:"):
                header = header[len("cookie:"):]
            for c in parse_cookie_header(header):
                raw.append({**c, "domain": ".reddit.com", "path": "/",
                            "secure": True, "expires": None})

    if not raw:
        raise CookieFileError(f"no cookies could be parsed from {path}\n")

    now = time.time()
    kept: dict[tuple, dict] = {}
    expired = 0
    for c in raw:
        if not c["domain"].lstrip(".").endswith("reddit.com"):
            continue                                       # ignore other sites' cookies
        if c["expires"] and c["expires"] < now:
            expired += 1
            continue
        kept[(c["name"], c["domain"], c["path"])] = c      # later entries win
    if expired:
        print(f"note: ignored {expired} expired cookie(s) in {path}")
    if not kept:
        raise CookieFileError(
            f"{path} has no (unexpired) reddit.com cookies. Were you on a reddit.com "
            f"tab, logged in, when you exported?\n")
    return list(kept.values())


class CurlBackend:
    """curl_cffi: libcurl with a Chrome TLS/JA3 + HTTP/2 fingerprint."""

    def __init__(self, args):
        try:
            from curl_cffi.requests import AsyncSession
        except ImportError:
            sys.exit("curl_cffi is not installed. Run:  pip install curl_cffi")
        proxies = {"http": args.proxy, "https": args.proxy} if args.proxy else None
        # impersonate= sets browser-matching TLS, HTTP/2 settings and header order
        self.session = AsyncSession(impersonate=args.impersonate, proxies=proxies,
                                    timeout=30)
        cookies = getattr(args, "cookies", None) or []
        try:
            # jar cookies, so refreshed tokens from Set-Cookie replace the originals
            for c in cookies:
                self.session.cookies.set(c["name"], c["value"],
                                         domain=c["domain"], path=c["path"])
        except Exception:
            self.session.headers["Cookie"] = "; ".join(
                f"{c['name']}={c['value']}" for c in cookies)

    async def start(self):
        pass

    async def get(self, url, params):
        r = await self.session.get(url, params=params, allow_redirects=True)
        return Resp(
            status=r.status_code,
            retry_after=r.headers.get("Retry-After"),
            final_url=str(r.url),
            redirected=(getattr(r, "redirect_count", 0) or 0) > 0,
            text=r.text,
        )

    async def close(self):
        await self.session.close()


class PlaywrightBackend:
    """A real Chromium browser. Pages are opened per request, then closed."""

    def __init__(self, args):
        self.args = args
        self.pw = self.browser = self.context = None

    async def start(self):
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            sys.exit("playwright is not installed. Run:  pip install playwright "
                     "&& playwright install chromium")
        self.pw = await async_playwright().start()
        launch = {
            "headless": not self.args.headed,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        if self.args.proxy:
            launch["proxy"] = {"server": self.args.proxy}
        self.browser = await self.pw.chromium.launch(**launch)
        self.context = await self.browser.new_context(
            user_agent=CHROME_UA,
            viewport={"width": 1366, "height": 900},
            locale="en-US",
            timezone_id="America/Los_Angeles",
        )
        pw_cookies = []
        for c in getattr(self.args, "cookies", None) or []:
            d = {"name": c["name"], "value": c["value"], "domain": c["domain"],
                 "path": c["path"], "secure": c["secure"]}
            if c["expires"]:
                d["expires"] = c["expires"]
            pw_cookies.append(d)
        if pw_cookies:
            await self.context.add_cookies(pw_cookies)
        await self.context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        # We only need the HTML: skip images/media/fonts to go faster
        async def block_heavy(route):
            if route.request.resource_type in ("image", "media", "font"):
                await route.abort()
            else:
                await route.continue_()

        await self.context.route("**/*", block_heavy)

    async def get(self, url, params):
        if params:
            url += ("&" if "?" in url else "?") + urlencode(params)
        page = await self.context.new_page()
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            if resp is None:
                raise RuntimeError("no response")
            return Resp(
                status=resp.status,
                retry_after=(await resp.all_headers()).get("retry-after"),
                final_url=page.url,
                redirected=resp.request.redirected_from is not None,
                text=await page.content(),
            )
        finally:
            await page.close()

    async def close(self):
        for obj in (self.context, self.browser):
            if obj:
                try:
                    await obj.close()
                except Exception:
                    pass
        if self.pw:
            await self.pw.stop()


def make_backend(args):
    return PlaywrightBackend(args) if args.backend == "playwright" else CurlBackend(args)


# --------------------------------------------------------------------------- #
# Shared run state
# --------------------------------------------------------------------------- #
@dataclass
class Ctx:
    cutoff: datetime
    out_dir: str
    delay: float
    max_blocks: int
    max_posts: int | None
    no_comments: bool
    retries: int = 6
    stale_min_comments: int = 50   # older posts (from /hot) need this many comments

    backend: Any = None
    sem: asyncio.Semaphore = None
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    stop_reason: str = ""

    consecutive_blocks: int = 0
    total_blocks: int = 0
    total_requests: int = 0
    cooldown_until: float = 0.0  # time.monotonic() value
    debug_dumps: int = 0
    login_walls: int = 0
    empty_comment_posts: int = 0    # posts that report comments but parsed 0
    parsed_comment_posts: int = 0   # posts where at least one comment parsed
    debug_comment_dumps: int = 0
    debug_search_dumps: int = 0
    seen_ids: set = field(default_factory=set)   # post ids already registered

    posts: list = field(default_factory=list)
    comments: list = field(default_factory=list)
    sub_status: dict = field(default_factory=dict)
    posts_queued: int = 0
    posts_done: int = 0


def trip(ctx: Ctx, reason: str):
    """Trip the circuit breaker: tell every task to wind down."""
    if not ctx.stop.is_set():
        ctx.stop_reason = reason
        ctx.stop.set()
        print(f"\n!!! STOPPING EARLY: {reason}\n    Cancelling in-flight requests and saving...\n")


# --------------------------------------------------------------------------- #
# Fetch with block detection
# --------------------------------------------------------------------------- #
async def nap(ctx: Ctx, seconds: float):
    """Sleep, but wake immediately if the circuit breaker trips."""
    try:
        await asyncio.wait_for(ctx.stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


def dump_debug(ctx: Ctx, url: str, final_url: str, status, text: str):
    """Save the first few unexpected responses so you can see what Reddit sent."""
    if ctx.debug_dumps >= 3:
        return
    ctx.debug_dumps += 1
    os.makedirs(ctx.out_dir, exist_ok=True)
    path = os.path.join(ctx.out_dir, f"debug_response_{ctx.debug_dumps}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"<!-- requested={url} final_url={final_url} status={status} -->\n{text}")
    print(f"    saved raw response to {path}")


async def fetch(ctx: Ctx, url: str, params: dict | None = None,
                expect: str | None = "#siteTable"):
    """
    GET a page and return a BeautifulSoup, or None on failure / stop.

    A response only counts as success if it is real old.reddit HTML (contains
    `expect`). A 200 that is really a login wall / block page / redirect to the
    new site is a soft block, treated like 403/429.

    Every wait here wakes immediately if the circuit breaker trips, and tasks
    that were queued for a request slot re-check the global cooldown, so one
    block really does pause everybody.
    """
    attempt = 0
    while attempt < ctx.retries:
        if ctx.stop.is_set():
            return None

        wait = ctx.cooldown_until - time.monotonic()
        if wait > 0:
            await nap(ctx, wait)
            continue

        throttled = False
        async with ctx.sem:
            if ctx.stop.is_set():
                return None
            if ctx.cooldown_until > time.monotonic():
                throttled = True  # a block happened while we waited for a slot
            else:
                await nap(ctx, random.uniform(0.5, 1.5) * ctx.delay)  # jitter
                if ctx.stop.is_set():
                    return None
                try:
                    ctx.total_requests += 1
                    # Race the request against the stop signal so a hung
                    # request can never delay shutdown
                    req = asyncio.ensure_future(ctx.backend.get(url, params))
                    stopper = asyncio.ensure_future(ctx.stop.wait())
                    try:
                        await asyncio.wait({req, stopper},
                                           return_when=asyncio.FIRST_COMPLETED)
                    finally:
                        stopper.cancel()
                        if not req.done():
                            req.cancel()
                    if req.cancelled():
                        return None
                    r = req.result()
                except Exception as e:  # network error, timeout, browser error...
                    print(f"    network error ({type(e).__name__}: {str(e)[:80]}); "
                          f"retry {attempt + 1}")
                    r = Resp(0, None, url, False, "")
        if throttled:
            continue  # doesn't use up a retry

        if ctx.stop.is_set():
            return None  # breaker tripped while this request was in flight

        soft = False
        if r.status == 200:
            soup = await asyncio.to_thread(BeautifulSoup, r.text, "lxml")
            if expect is None or soup.select_one(expect) is not None:
                ctx.consecutive_blocks = 0
                ctx.login_walls = 0
                return soup
            title = soup.title.get_text(strip=True) if soup.title else "(no title)"
            moved = f" (redirected to {r.final_url})" if r.redirected else ""
            print(f"    unexpected page{moved}; title: {title!r}")
            dump_debug(ctx, url, r.final_url, r.status, r.text)
            soft = True
            if "/login" in urlparse(r.final_url).path or title.startswith("Welcome to Reddit"):
                # A login wall is deterministic: retrying and cooling down won't help
                ctx.login_walls += 1
                if ctx.login_walls >= 3:
                    trip(ctx, "Reddit is redirecting requests to its login page, so "
                              "the session in your cookie file is expired, logged out, "
                              "or was rejected. Log in again in your browser, re-export "
                              "the cookie file and re-run")
                    return None

        if soft or r.status in (403, 429):
            attempt += 1
            ctx.consecutive_blocks += 1
            ctx.total_blocks += 1
            label = "unexpected non-Reddit page" if soft else f"HTTP {r.status}"
            print(f"    {label} (consecutive blocks: "
                  f"{ctx.consecutive_blocks}/{ctx.max_blocks})")
            if not soft:
                dump_debug(ctx, url, r.final_url, r.status, r.text)
            if ctx.consecutive_blocks >= ctx.max_blocks:
                trip(ctx, f"{ctx.consecutive_blocks} consecutive blocked responses "
                          f"(403/429 or non-Reddit page)")
                return None
            try:
                cooldown = float(r.retry_after) if r.retry_after else None
            except ValueError:
                cooldown = None
            if cooldown is None:
                cooldown = min(120, 10 * ctx.consecutive_blocks)
            ctx.cooldown_until = max(ctx.cooldown_until, time.monotonic() + cooldown)
            continue

        if r.status == 0 or r.status >= 500:
            await nap(ctx, min(60, 2 ** attempt * 3))
            attempt += 1
            continue

        print(f"    HTTP {r.status} for {url}; skipping")  # 404, 410, ...
        return None

    return None


# --------------------------------------------------------------------------- #
# Parsing (sync, cheap)
# --------------------------------------------------------------------------- #
def ts_to_dt(ms):
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


def parse_listing(soup, subreddit, cutoff, keep_stale=False):
    """
    Return (posts, reached_cutoff, next_url).

    keep_stale=False (/new): stop at the first post older than the cutoff.
    keep_stale=True (/hot, /rising): keep older posts too, flagged stale=True,
    because an old or stickied thread can still hold fresh comments.
    """
    posts, reached_cutoff = [], False
    for t in soup.select("div#siteTable > div.thing.link"):
        classes = t.get("class", [])
        if "promoted" in classes or t.get("data-promoted") == "true":
            continue
        try:
            created = ts_to_dt(t["data-timestamp"])
        except (KeyError, ValueError):
            continue
        stickied = "stickied" in classes or t.get("data-stickied") == "true"
        stale = created < cutoff
        if stale and not keep_stale:
            if stickied:
                continue
            reached_cutoff = True  # /new is newest-first: the rest is older
            break

        title_a = t.select_one("a.title")
        flair_tag = t.select_one("span.linkflairlabel")
        posts.append({
            "subreddit": subreddit,
            "post_id": t.get("data-fullname", "").replace("t3_", ""),
            "title": title_a.get_text(strip=True) if title_a else "",
            "author": t.get("data-author", ""),
            "created_utc": created.isoformat(),
            "score": t.get("data-score", ""),
            "num_comments": t.get("data-comments-count", ""),
            "flair": flair_tag.get_text(strip=True) if flair_tag else "",
            "domain": t.get("data-domain", ""),
            "url": t.get("data-url", ""),
            "permalink": BASE + t.get("data-permalink", ""),
            "selftext": "",
            "comments_scraped": False,
            "stickied": stickied,
            "stale": stale,
        })

    nxt = soup.select_one("span.next-button a")
    return posts, reached_cutoff, (nxt["href"] if nxt else None)


_POST_HREF_RE = re.compile(r"/r/([^/]+)/comments/([a-z0-9]+)", re.I)


def _first_int(tag):
    if tag is None:
        return ""
    m = re.search(r"-?\d[\d,]*", tag.get_text(" ", strip=True))
    return m.group().replace(",", "") if m else ""


def parse_search(soup, cutoff):
    """
    Parse an old.reddit search-results page (sorted by new).
    Return (posts, reached_cutoff, next_url). Search results carry no selftext;
    it is filled in from the post page when the comments are fetched.
    """
    posts, reached_cutoff = [], False
    for t in soup.select("div.search-result-link"):
        a = t.select_one("a.search-title")
        m = _POST_HREF_RE.search(a["href"]) if a is not None and a.get("href") else None
        if not m:
            continue
        tm = t.select_one("time[datetime]")
        created = _parse_iso(tm["datetime"]) if tm is not None else None
        if created is None:
            continue
        if created < cutoff:
            reached_cutoff = True       # sort=new: everything after this is older
            break
        author = t.select_one("a.author, span.author")
        flair = t.select_one("span.linkflairlabel")
        posts.append({
            "subreddit": m.group(1),
            "post_id": m.group(2),
            "title": a.get_text(strip=True),
            "author": author.get_text(strip=True) if author is not None else "",
            "created_utc": created.isoformat(),
            "score": _first_int(t.select_one("span.search-score")),
            "num_comments": _first_int(t.select_one("a.search-comments")),
            "flair": flair.get_text(strip=True) if flair is not None else "",
            "domain": "",
            "url": "",
            "permalink": BASE + urlparse(a["href"]).path,
            "selftext": "",
            "comments_scraped": False,
            "stickied": False,
            "stale": False,
        })

    nxt = soup.select_one("span.next-button a")
    return posts, reached_cutoff, (nxt["href"] if nxt else None)


def _parse_iso(value):
    try:
        d = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def comment_created(node, entry):
    """
    Comment time. Post listings carry a data-timestamp attribute, but comments
    usually don't: they have a <time datetime="..."> element in their tagline.
    `entry` excludes the replies (they live in the sibling div.child), so this
    never picks up a child comment's time.
    """
    ts = node.get("data-timestamp")
    if ts:
        try:
            return ts_to_dt(ts)
        except (ValueError, OverflowError):
            pass
    t = entry.select_one("time.live-timestamp[datetime]") or entry.select_one("time[datetime]")
    return _parse_iso(t["datetime"]) if t else None


def parse_post_page(soup, post, cutoff):
    """
    Return (selftext, comments, n_unexpanded_stubs, diag).
    A comment is never dropped just because its timestamp can't be read: all
    comments on an in-window post are newer than the window anyway.
    """
    body = soup.select_one("div.thing.link div.usertext-body div.md")
    selftext = body.get_text("\n", strip=True) if body else ""

    diag = {"candidates": 0, "no_entry": 0, "no_time": 0, "too_old": 0}
    comments = []
    for c in soup.select("div.commentarea div.thing.comment"):
        diag["candidates"] += 1
        entry = c.find("div", class_="entry", recursive=False)
        if entry is None:
            diag["no_entry"] += 1
            continue
        created = comment_created(c, entry)
        if created is None:
            diag["no_time"] += 1
        elif created < cutoff:
            diag["too_old"] += 1
            continue

        md = entry.select_one("div.usertext-body div.md")
        score_tag = entry.select_one("span.score.unvoted")
        score = ""
        if score_tag:
            score = score_tag.get("title") or score_tag.get_text(strip=True)
        parent = c.find_parent("div", class_="comment")
        parent_id = parent.get("data-fullname", "").replace("t1_", "") if parent else ""

        comments.append({
            "subreddit": post["subreddit"],
            "post_id": post["post_id"],
            "comment_id": c.get("data-fullname", "").replace("t1_", ""),
            "parent_id": parent_id or post["post_id"],  # top-level -> post id
            "author": c.get("data-author", ""),
            "created_utc": created.isoformat() if created else "",
            "score": score,
            "body": md.get_text("\n", strip=True) if md else "",
            "permalink": BASE + c.get("data-permalink", ""),
        })

    stubs = len(soup.select("div.commentarea div.thing.morechildren"))
    return selftext, comments, stubs, diag


# --------------------------------------------------------------------------- #
# Tasks
# --------------------------------------------------------------------------- #
BIG_THREAD = 400   # posts with at least this many comments get a second sort


def register_post(ctx: Ctx, p: dict, queue: asyncio.Queue, discussion_only=False) -> bool:
    """
    Add a post to the run (and the comment queue) unless it is a duplicate or
    not worth fetching. One shared `seen_ids` set keeps the crawl, the search and
    the /hot listing from fetching the same thread twice.
    """
    pid = p["post_id"]
    if not pid or pid in ctx.seen_ids:
        return False
    discussion = is_discussion_title(p["title"])
    if discussion_only and not (p.get("stickied") or discussion):
        return False
    if p.get("stale"):
        # an old post is only worth a request if it is a pinned/daily thread or busy
        try:
            n = int(p.get("num_comments") or 0)
        except ValueError:
            n = 0
        if not (p.get("stickied") or discussion or n >= ctx.stale_min_comments):
            return False
    ctx.seen_ids.add(pid)
    ctx.posts.append(p)
    if not ctx.no_comments:
        ctx.posts_queued += 1
        queue.put_nowait(p)
    return True


def comment_sorts(post: dict) -> list[str]:
    """Daily threads and huge threads exceed one page of comments: also take the top ones."""
    try:
        n = int(post.get("num_comments") or 0)
    except ValueError:
        n = 0
    if post.get("stickied") or is_discussion_title(post.get("title", "")) or n >= BIG_THREAD:
        return ["new", "top"]
    return ["new"]


async def produce_posts(ctx: Ctx, sub: str, queue: asyncio.Queue):
    """Crawl /r/<sub>/new/ until the cutoff; feed posts to the comment queue."""
    url, params = f"{BASE}/r/{sub}/new/", {"limit": 100}
    n, page = 0, 1
    ctx.sub_status[sub] = "interrupted"

    while url and not ctx.stop.is_set():
        soup = await fetch(ctx, url, params)
        if soup is None:
            return
        posts, reached_cutoff, nxt = parse_listing(soup, sub, ctx.cutoff)

        for p in posts:
            if ctx.max_posts and n >= ctx.max_posts:
                reached_cutoff = True
                break
            if register_post(ctx, p, queue):
                n += 1

        print(f"  [r/{sub}] page {page}: {n} posts so far")
        if reached_cutoff or not nxt:
            ctx.sub_status[sub] = "complete"
            return
        url, params = nxt, None
        page += 1


async def produce_listing(ctx: Ctx, sub: str, kind: str, pages: int,
                          queue: asyncio.Queue, discussion_only: bool):
    """
    Read /r/<sub>/hot or /rising. Unlike /new this keeps posts older than the
    cutoff (pinned daily threads, long-running discussions): their comments are
    filtered by their own timestamps later, so only fresh ones survive.
    """
    url, params = f"{BASE}/r/{sub}/{kind}/", {"limit": 100}
    n, page = 0, 1
    while url and page <= pages and not ctx.stop.is_set():
        soup = await fetch(ctx, url, params)
        if soup is None:
            return
        posts, _, nxt = parse_listing(soup, sub, ctx.cutoff, keep_stale=True)
        for p in posts:
            if ctx.max_posts and n >= ctx.max_posts:
                nxt = None
                break
            if register_post(ctx, p, queue, discussion_only):
                n += 1
        print(f"  [r/{sub}/{kind}] page {page}: {n} new posts")
        if not nxt:
            return
        url, params = nxt, None
        page += 1


def search_time_filter(cutoff: datetime) -> str:
    days = (datetime.now(timezone.utc) - cutoff).days + 1
    for limit, name in ((1, "day"), (7, "week"), (31, "month"), (365, "year")):
        if days <= limit:
            return name
    return "all"


def dump_search_debug(ctx: Ctx, query: str, soup):
    """A search page that parsed to zero posts: save it so the selectors can be checked."""
    if ctx.debug_search_dumps >= 2:
        return
    ctx.debug_search_dumps += 1
    os.makedirs(ctx.out_dir, exist_ok=True)
    path = os.path.join(ctx.out_dir, f"debug_search_{ctx.debug_search_dumps}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"<!-- query={query} -->\n{soup}")
    print(f"    search {query!r}: 0 posts parsed (normal for a quiet ticker; page saved "
          f"to {path} in case the result selectors need checking)")


async def produce_search(ctx: Ctx, query: str, queue: asyncio.Queue, pages: int):
    """Search the tracked subreddits for one ticker/company query, newest first."""
    url = f"{BASE}/r/{'+'.join(SUBREDDITS)}/search"
    params = {"q": query, "restrict_sr": "on", "sort": "new",
              "t": search_time_filter(ctx.cutoff), "limit": 100}
    n, page = 0, 1
    while url and page <= pages and not ctx.stop.is_set():
        # "#header" exists on every real old.reddit page, including zero-result
        # searches, so a ticker nobody mentions is not mistaken for a block
        soup = await fetch(ctx, url, params, expect="#header")
        if soup is None:
            return
        posts, reached_cutoff, nxt = parse_search(soup, ctx.cutoff)
        if not posts and page == 1:
            dump_search_debug(ctx, query, soup)
        for p in posts:
            if ctx.max_posts and n >= ctx.max_posts:
                reached_cutoff = True
                break
            if register_post(ctx, p, queue):
                n += 1
        print(f"  [search {query}] page {page}: {n} new posts")
        if reached_cutoff or not nxt:
            return
        url, params = nxt, None
        page += 1


def dump_comment_debug(ctx: Ctx, post: dict, soup, diag: dict):
    """Save a comment page that parsed to nothing so the layout can be inspected."""
    if ctx.debug_comment_dumps >= 2:
        return
    ctx.debug_comment_dumps += 1
    os.makedirs(ctx.out_dir, exist_ok=True)
    path = os.path.join(ctx.out_dir, f"debug_comments_{ctx.debug_comment_dumps}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"<!-- post={post['permalink']} reported_comments={post.get('num_comments')} "
                f"diag={diag} -->\n{soup}")
    print(f"    saved the comment page to {path}")


async def comment_worker(ctx: Ctx, queue: asyncio.Queue):
    """Pull posts off the queue and fetch their comment pages."""
    while True:
        post = await queue.get()
        try:
            if ctx.stop.is_set():
                continue  # drain the queue so queue.join() can finish

            comments, seen_cids = [], set()
            first_soup = first_diag = None
            stubs = 0
            for sort in comment_sorts(post):
                soup = await fetch(ctx, post["permalink"], {"limit": 500, "sort": sort})
                if soup is None:
                    if ctx.stop.is_set():
                        break
                    continue
                selftext, found, n_stubs, diag = parse_post_page(soup, post, ctx.cutoff)
                if first_soup is None:
                    first_soup, first_diag, stubs = soup, diag, n_stubs
                    post["selftext"] = selftext
                    post["comments_scraped"] = True
                for c in found:                      # merge the sorts, no duplicates
                    if c["comment_id"] not in seen_cids:
                        seen_cids.add(c["comment_id"])
                        comments.append(c)
            if first_soup is None:
                continue

            ctx.comments.extend(comments)
            ctx.posts_done += 1
            if stubs:
                print(f"    note: {stubs} 'load more comments' stub(s) not expanded "
                      f"({post['post_id']})")

            # Health check: the post says it has comments, but we parsed none
            try:
                expected = int(post.get("num_comments") or 0)
            except ValueError:
                expected = 0
            if comments:
                ctx.parsed_comment_posts += 1
            elif expected > 0:
                ctx.empty_comment_posts += 1
                if ctx.empty_comment_posts <= 5:
                    print(f"    warning: post {post['post_id']} reports {expected} "
                          f"comments but 0 were parsed {first_diag}")
                dump_comment_debug(ctx, post, first_soup, first_diag)
                if ctx.empty_comment_posts >= 10 and ctx.parsed_comment_posts == 0:
                    trip(ctx, f"comment pages load, but no comments could be parsed from "
                              f"{ctx.empty_comment_posts} posts that have comments. The "
                              f"page layout probably differs from what the parser expects: "
                              f"inspect {ctx.out_dir}/debug_comments_1.html")

            if ctx.posts_done % 20 == 0:
                print(f"  progress: {ctx.posts_done}/{ctx.posts_queued} posts' comments "
                      f"fetched, {len(ctx.comments)} comments total")
        finally:
            queue.task_done()


async def checkpoint_loop(ctx: Ctx, every: float):
    while True:
        await asyncio.sleep(every)
        save_all(ctx)


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
def _atomic_write(path, writer_fn, **open_kwargs):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", **open_kwargs) as f:
        writer_fn(f)
    os.replace(tmp, path)  # never leaves a half-written file behind


def _write_csv(path, fields, rows):
    def w(f):
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    _atomic_write(path, w, newline="")


def save_all(ctx: Ctx):
    os.makedirs(ctx.out_dir, exist_ok=True)
    posts, comments = list(ctx.posts), list(ctx.comments)
    _write_csv(os.path.join(ctx.out_dir, "posts.csv"), POST_FIELDS, posts)
    _write_csv(os.path.join(ctx.out_dir, "comments.csv"), COMMENT_FIELDS, comments)
    _atomic_write(
        os.path.join(ctx.out_dir, "reddit_data.json"),
        lambda f: json.dump({"posts": posts, "comments": comments}, f,
                            ensure_ascii=False, indent=2),
    )


def write_summary(ctx: Ctx, mode: str = "crawl"):
    summary = {
        "mode": mode,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "window_start": ctx.cutoff.isoformat(),
        "stopped_early": ctx.stop.is_set(),
        "stop_reason": ctx.stop_reason or None,
        "total_requests": ctx.total_requests,
        "total_blocked_responses": ctx.total_blocks,
        "posts": len(ctx.posts),
        "posts_with_comments_scraped": sum(1 for p in ctx.posts if p["comments_scraped"]),
        "comments": len(ctx.comments),
        "subreddit_listing_status": ctx.sub_status,
    }
    os.makedirs(ctx.out_dir, exist_ok=True)
    _atomic_write(os.path.join(ctx.out_dir, "run_summary.json"),
                  lambda f: json.dump(summary, f, indent=2))
    return summary


# --------------------------------------------------------------------------- #
# Filtering + sentiment scoring
# --------------------------------------------------------------------------- #
def process_data(posts: list, comments: list,
                 tickers_file: str = "tickers.json",
                 out_dir: str = "output",
                 scores_file: str | None = None,
                 cutoff: datetime | None = None,
                 matcher: TickerMatcher | None = None,
                 aliases_file: str | None = None,
                 ambiguous_file: str | None = None,
                 min_items: int = 20):
    """
    1. Load {company name: ticker} from tickers_file (unless a matcher is given).
    2. Drop noise: deleted/removed, bots, link-only, too short/long, duplicates.
    3. Attribute each post/comment to the tickers it is about (its own text; a
       comment with none inherits the tickers in its post's title, except in
       daily threads) and cut multi-ticker / long texts down to the relevant
       sentences. Short replies get their parent comment / post title as context.
    4. Score every (post|comment, ticker) item with score_items().
    5. Write one row per item plus per-ticker daily and summary aggregates.
    Returns the file name of the per-item rows CSV (None if nothing was kept).
    """
    if matcher is None:
        matcher = TickerMatcher(load_tickers(tickers_file),
                                aliases=load_json_file(aliases_file, {}),
                                extra_ambiguous=load_json_file(ambiguous_file, []))

    items, stats = build_scoring_items(posts, comments, matcher, cutoff=cutoff)
    dropped = {k[8:]: v for k, v in stats.items() if k.startswith("dropped_")}
    print(f"Kept {len(items)} (post|comment, ticker) items "
          f"from {stats['posts_in']} posts / {stats['comments_in']} comments")
    if dropped:
        print("  dropped: " + ", ".join(f"{k}={v}" for k, v in sorted(dropped.items())))
    if not items:
        return None

    scores_file = scores_file or os.path.join(out_dir, "sentiment_item_scores.jsonl")
    score_items(items, scores_file)
    records = load_score_records(scores_file)

    rows = []
    for it in items:
        rec = records.get(it["id"])
        if rec is None:                      # scoring was interrupted for this item
            continue
        rows.append({**it, "score": rec["score"], "bullish": rec["bullish"],
                     "bearish": rec["bearish"], "neutral": rec["neutral"]})

    rows_file = os.path.join(out_dir, "sentiment_rows.csv")
    write_csv_atomic(rows_file, ROW_FIELDS, rows)

    daily, summary = aggregate_rows(rows, min_items=min_items)
    write_csv_atomic(os.path.join(out_dir, "ticker_sentiment_daily.csv"), DAILY_FIELDS, daily)
    write_csv_atomic(os.path.join(out_dir, "ticker_sentiment_summary.csv"),
                     SUMMARY_FIELDS, summary)

    print(f"Scored {len(rows)} items across {len(summary)} tickers "
          f"({sum(1 for r in summary if r['reliable'])} with >= {min_items} voters)")
    for r in summary[:10]:
        shown = r["reported_score"] if r["reliable"] else f"({r['mean_score_weighted']}, low n)"
        print(f"  {r['ticker']:8s} voters={r['n_voters']:<5d} items={r['n_items']:<5d} "
              f"score={shown}  bullish={r['pct_bullish']:.0%} bearish={r['pct_bearish']:.0%}")
    return rows_file


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
async def run(ctx: Ctx, args) -> None:
    ctx.sem = asyncio.Semaphore(args.concurrency)
    print(f"Collecting items newer than {ctx.cutoff.isoformat()}")
    n_cookies = len(getattr(args, "cookies", None) or [])
    if n_cookies:
        print(f"Using {n_cookies} reddit.com cookies from the cookie file (values not shown)")
    print(f"backend={args.backend}  concurrency={args.concurrency}  "
          f"delay~{args.delay}s  stop after {args.max_blocks} consecutive "
          f"blocked responses (403/429/non-Reddit page)")
    print(f"mode={args.mode}"
          + (f"  search queries={len(args.search_queries)}" if args.search_queries else "")
          + (f"  extra listings={','.join(args.listings)}" if args.listings else "")
          + "\n")

    backend = make_backend(args)
    ctx.backend = backend
    queue: asyncio.Queue = asyncio.Queue()
    workers, ckpt, work = [], None, None
    try:
        await backend.start()
        workers = [asyncio.create_task(comment_worker(ctx, queue))
                   for _ in range(args.concurrency)]
        ckpt = asyncio.create_task(checkpoint_loop(ctx, args.checkpoint_secs))

        async def crawl():
            jobs = []
            if args.mode in ("crawl", "both"):
                jobs += [produce_posts(ctx, s, queue) for s in SUBREDDITS]
            if args.mode in ("search", "both"):
                jobs += [produce_search(ctx, q, queue, args.search_pages)
                         for q in args.search_queries]
            for s in SUBREDDITS:                 # pinned daily threads, old-but-active posts
                for kind in args.listings:
                    pages = args.hot_pages if kind == "hot" else 1
                    jobs.append(produce_listing(ctx, s, kind, pages, queue,
                                                discussion_only=(args.mode == "search")))
            await asyncio.gather(*jobs)
            await queue.join()

        work = asyncio.create_task(crawl())
        stopper = asyncio.create_task(ctx.stop.wait())
        # Finish when the crawl completes OR the breaker trips, whichever is first
        await asyncio.wait({work, stopper}, return_when=asyncio.FIRST_COMPLETED)
        stopper.cancel()
        if work.done() and not work.cancelled():
            work.result()  # surface any exception from the crawl
    finally:
        # 1) save immediately - nothing below can delay or lose the data
        save_all(ctx)
        print(f"Saved {len(ctx.posts)} posts and {len(ctx.comments)} comments "
              f"to {ctx.out_dir}/")
        # 2) cancel everything still running; never wait on stragglers for long
        tasks = [t for t in workers + [ckpt, work] if t is not None]
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=5)
        # 3) close the backend with a hard time limit
        try:
            await asyncio.wait_for(backend.close(), timeout=5)
        except BaseException:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--max-posts", type=int, default=None,
                    help="cap posts per subreddit (for testing)")
    ap.add_argument("--no-comments", action="store_true")
    ap.add_argument("--out", default="output")
    ap.add_argument("--backend", choices=["curl", "playwright"], default="curl")
    ap.add_argument("--impersonate", default="chrome",
                    help="curl_cffi browser to impersonate, e.g. chrome, chrome131, "
                         "safari, edge (default: chrome)")
    ap.add_argument("--headed", action="store_true",
                    help="playwright: show the browser window (harder to detect)")
    ap.add_argument("--concurrency", type=int, default=3,
                    help="max simultaneous requests/tabs (default 3)")
    ap.add_argument("--delay", type=float, default=2.0,
                    help="avg delay per request slot in seconds (default 2.0)")
    ap.add_argument("--max-blocks", type=int, default=8,
                    help="stop after this many consecutive blocked responses (default 8)")
    ap.add_argument("--checkpoint-secs", type=float, default=60)
    ap.add_argument("--cookie-file", default="reddit_cookies.txt",
                    help="REQUIRED: cookie export from a logged-in reddit.com session "
                         "(Netscape / JSON / raw Cookie line). Default: ./reddit_cookies.txt")
    ap.add_argument("--list-cookies", action="store_true",
                    help="print the names/domains/expiry of the cookies in the file "
                         "(never the values) and exit")
    ap.add_argument("--skip-cookie-check", action="store_true",
                    help="start even if no reddit_session/token_v2 cookie is present")
    ap.add_argument("--proxy", default=None,
                    help="proxy URL, e.g. http://user:pass@host:port")
    ap.add_argument("--tickers-file", default="tickers.json",
                    help="JSON dict of {company name: ticker}")
    ap.add_argument("--aliases-file", default=None,
                    help="optional JSON {alias: ticker or [tickers]}, e.g. "
                         '{"the big tech": ["AAPL", "MSFT"]}')
    ap.add_argument("--ambiguous-file", default=None,
                    help="optional JSON list of extra symbols/names that need a finance "
                         "word nearby to count as a ticker mention")
    ap.add_argument("--scores-file", default=None,
                    help="raw model output, resumable "
                         "(default: <out>/sentiment_item_scores.jsonl)")
    ap.add_argument("--min-items", type=int, default=20,
                    help="distinct voters needed before a ticker's score counts as "
                         "reliable (default 20)")
    ap.add_argument("--no-sentiment", action="store_true",
                    help="scrape only; skip filtering and scoring")
    ap.add_argument("--mode", choices=["auto", "crawl", "search", "both"], default="auto",
                    help="crawl = every subreddit's /new; search = one search per ticker; "
                         "auto = search if the ticker list is small, else crawl (default)")
    ap.add_argument("--search-threshold", type=int, default=40,
                    help="auto mode: search when there are at most this many tickers")
    ap.add_argument("--search-pages", type=int, default=2,
                    help="result pages per search query (default 2)")
    ap.add_argument("--extra-listings", default="hot,rising",
                    help="comma list of extra listings to read for pinned/old-but-active "
                         "threads: hot, rising (default both; empty to disable)")
    ap.add_argument("--hot-pages", type=int, default=2,
                    help="pages of /hot to read per subreddit (default 2)")
    ap.add_argument("--stale-min-comments", type=int, default=50,
                    help="posts older than the window are only fetched if pinned, a "
                         "daily thread, or at least this many comments (default 50)")
    args = ap.parse_args()

    args.listings = [x.strip() for x in args.extra_listings.split(",") if x.strip()]
    bad = set(args.listings) - {"hot", "rising"}
    if bad:
        sys.exit(f"ERROR: unknown --extra-listings value(s): {', '.join(sorted(bad))}")

    # Mandatory: refuse to start without a usable cookie file
    try:
        args.cookies = load_cookie_file(args.cookie_file)
    except CookieFileError as e:
        sys.exit(f"ERROR: {e}")
    print(f"Loaded {len(args.cookies)} reddit.com cookies from {args.cookie_file}")
    cookie_names = sorted({c["name"] for c in args.cookies})
    if args.list_cookies:
        print(f"{'name':28s} {'domain':20s} {'secure':7s} expires")
        for c in sorted(args.cookies, key=lambda c: c["name"]):
            exp = (datetime.fromtimestamp(c["expires"], timezone.utc).strftime("%Y-%m-%d")
                   if c["expires"] else "session")
            print(f"{c['name']:28s} {c['domain']:20s} {str(c['secure']):7s} {exp}")
        sys.exit(0)
    if not set(cookie_names) & set(REDDIT_SESSION_COOKIES):
        msg = (
            "your cookie file has no Reddit login session (no 'reddit_session' or "
            "'token_v2' cookie), so Reddit would send every request to the login page.\n"
            f"Cookies found (names only): {', '.join(cookie_names)}\n\n"
            "Usual causes:\n"
            "  1. You were not logged in to reddit.com when you exported "
            "(these look like logged-out cookies).\n"
            "  2. The export tool skipped HttpOnly cookies, and reddit_session is "
            "HttpOnly. Use an extension that includes them (Cookie-Editor, or "
            "'Get cookies.txt LOCALLY'), not a document.cookie/console snippet.\n"
            "  3. You exported from a different site or browser profile.\n\n"
            "Check: DevTools -> Application -> Cookies -> https://www.reddit.com and "
            "look for a 'reddit_session' row. If it isn't there, you aren't logged in "
            "in that browser profile.\n"
            "(If you ARE logged in and Reddit has renamed its cookies, re-run with "
            "--skip-cookie-check.)"
        )
        if args.skip_cookie_check:
            print(f"WARNING: {msg}\n")
        else:
            sys.exit(f"ERROR: {msg}")

    # Load the tickers BEFORE scraping, so a bad path fails now, not after an hour
    matcher = None
    try:
        matcher = TickerMatcher(load_tickers(args.tickers_file),
                                aliases=load_json_file(args.aliases_file, {}),
                                extra_ambiguous=load_json_file(args.ambiguous_file, []))
    except (OSError, ValueError) as e:
        if args.no_sentiment and args.mode in ("auto", "crawl"):
            print(f"note: tickers not loaded ({e}); crawling without ticker search")
        else:
            sys.exit(f"ERROR: could not load tickers/aliases: {e}")
    if args.mode == "auto":
        args.mode = ("search" if matcher and matcher.n_tickers <= args.search_threshold
                     else "crawl")
        why = (f"{matcher.n_tickers} tickers" if matcher else "no tickers")
        print(f"auto mode -> {args.mode} ({why}, search threshold "
              f"{args.search_threshold})")
    args.search_queries = (matcher.search_queries()
                           if matcher and args.mode in ("search", "both") else [])

    ctx = Ctx(
        stale_min_comments=args.stale_min_comments,
        cutoff=datetime.now(timezone.utc) - timedelta(days=args.days),
        out_dir=args.out,
        delay=args.delay,
        max_blocks=args.max_blocks,
        max_posts=args.max_posts,
        no_comments=args.no_comments,
    )
    try:
        asyncio.run(run(ctx, args))
    except KeyboardInterrupt:
        trip(ctx, "interrupted by user (Ctrl+C)")

    s = write_summary(ctx, args.mode)
    print(f"\nDone. {s['posts']} posts "
          f"({s['posts_with_comments_scraped']} with comments scraped), "
          f"{s['comments']} comments -> {args.out}/")

    if not args.no_sentiment and not args.no_comments and ctx.posts:
        rows_path = process_data(
            ctx.posts, ctx.comments, args.tickers_file, out_dir=args.out,
            scores_file=args.scores_file, cutoff=ctx.cutoff, matcher=matcher,
            min_items=args.min_items)
        print(f"Sentiment rows: {rows_path}")

    if s["stopped_early"]:
        incomplete = [k for k, v in s["subreddit_listing_status"].items()
                      if v != "complete"]
        print(f"Stopped early: {s['stop_reason']}")
        if incomplete:
            print(f"Subreddit listings not fully crawled: {', '.join(incomplete)}")
        sys.exit(2)


if __name__ == "__main__":
    main()