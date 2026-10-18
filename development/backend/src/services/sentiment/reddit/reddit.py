"""
Async Reddit scraper (no Reddit API) - posts + comments from the last N days.

Scrapes old.reddit.com HTML. Plain Python HTTP clients (requests/aiohttp) get
flagged because their TLS/HTTP2 fingerprint doesn't look like a browser, so
this version fetches pages through one of two browser-like backends:

  --backend curl         (default) curl_cffi impersonating Chrome's TLS + HTTP/2
                         fingerprint. Fast and light; same async design.
  --backend playwright   a real Chromium browser. Slower, but is a real browser.
                         Add --headed if headless gets flagged.

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

Cookie file (required)
----------------------
Reddit redirects logged-out visitors to a login page, so the scraper REQUIRES a
cookie file from a logged-in browser session and refuses to start without it
(default: ./cookies.txt, or --cookie-file PATH). Accepted formats (auto-detected):

  * Netscape cookies.txt      (browser extensions such as "Get cookies.txt LOCALLY")
  * JSON                      (Cookie-Editor / EditThisCookie export)
  * a single raw Cookie line  (DevTools -> Network -> any reddit.com request ->
                               Request Headers -> copy the value of "Cookie")

Log in to reddit.com first, then export while on a reddit.com tab. Only
reddit.com cookies are used; everything else in the file is ignored.

Check what the file contains (names only, never values) with --list-cookies. A
logged-in export must contain 'reddit_session' (an HttpOnly cookie) or 'token_v2'.

Treat the file like a password: never share it, never commit it to git. Automated
access with an account can get that account rate-limited or suspended, so use a
throwaway account. If the session expires mid-run you'll hit the login page, the
scraper stops and saves what it has: log in again, re-export, re-run.

Run: python reddit_scraper.py 


Output (./output): posts.csv, comments.csv, reddit_data.json, run_summary.json
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple
from urllib.parse import urlencode, urlparse

from bs4 import BeautifulSoup

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


def parse_listing(soup, subreddit, cutoff):
    """Return (posts, reached_cutoff, next_url)."""
    posts, reached_cutoff = [], False
    for t in soup.select("div#siteTable > div.thing.link"):
        classes = t.get("class", [])
        if "promoted" in classes or t.get("data-promoted") == "true":
            continue
        try:
            created = ts_to_dt(t["data-timestamp"])
        except (KeyError, ValueError):
            continue
        if created < cutoff:
            if "stickied" in classes or t.get("data-stickied") == "true":
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
        })

    nxt = soup.select_one("span.next-button a")
    return posts, reached_cutoff, (nxt["href"] if nxt else None)


def parse_post_page(soup, post, cutoff):
    """Return (selftext, comments, n_unexpanded_stubs)."""
    body = soup.select_one("div.thing.link div.usertext-body div.md")
    selftext = body.get_text("\n", strip=True) if body else ""

    comments = []
    for c in soup.select("div.commentarea div.thing.comment"):
        try:
            created = ts_to_dt(c["data-timestamp"])
        except (KeyError, ValueError):
            continue
        if created < cutoff:
            continue

        entry = c.find("div", class_="entry", recursive=False)
        if entry is None:
            continue
        md = entry.select_one("div.usertext-body div.md")
        score_tag = entry.select_one("span.score.unvoted")
        parent = c.find_parent("div", class_="comment")
        parent_id = parent.get("data-fullname", "").replace("t1_", "") if parent else ""

        comments.append({
            "subreddit": post["subreddit"],
            "post_id": post["post_id"],
            "comment_id": c.get("data-fullname", "").replace("t1_", ""),
            "parent_id": parent_id or post["post_id"],  # top-level -> post id
            "author": c.get("data-author", ""),
            "created_utc": created.isoformat(),
            "score": score_tag.get("title", "") if score_tag else "",
            "body": md.get_text("\n", strip=True) if md else "",
            "permalink": BASE + c.get("data-permalink", ""),
        })

    stubs = len(soup.select("div.commentarea div.thing.morechildren"))
    return selftext, comments, stubs


# --------------------------------------------------------------------------- #
# Tasks
# --------------------------------------------------------------------------- #
async def produce_posts(ctx: Ctx, sub: str, queue: asyncio.Queue):
    """Crawl /r/<sub>/new/ until the cutoff; feed posts to the comment queue."""
    url, params = f"{BASE}/r/{sub}/new/", {"limit": 100}
    seen, page = set(), 1
    ctx.sub_status[sub] = "interrupted"

    while url and not ctx.stop.is_set():
        soup = await fetch(ctx, url, params)
        if soup is None:
            return
        posts, reached_cutoff, nxt = parse_listing(soup, sub, ctx.cutoff)

        for p in posts:
            if p["post_id"] in seen:
                continue
            if ctx.max_posts and len(seen) >= ctx.max_posts:
                reached_cutoff = True
                break
            seen.add(p["post_id"])
            ctx.posts.append(p)
            if not ctx.no_comments:
                ctx.posts_queued += 1
                queue.put_nowait(p)

        print(f"  [r/{sub}] page {page}: {len(seen)} posts so far")
        if reached_cutoff or not nxt:
            ctx.sub_status[sub] = "complete"
            return
        url, params = nxt, None
        page += 1


async def comment_worker(ctx: Ctx, queue: asyncio.Queue):
    """Pull posts off the queue and fetch their comment pages."""
    while True:
        post = await queue.get()
        try:
            if ctx.stop.is_set():
                continue  # drain the queue so queue.join() can finish
            soup = await fetch(ctx, post["permalink"], {"limit": 500, "sort": "new"})
            if soup is None:
                continue
            selftext, comments, stubs = parse_post_page(soup, post, ctx.cutoff)
            post["selftext"] = selftext
            post["comments_scraped"] = True
            ctx.comments.extend(comments)
            ctx.posts_done += 1
            if stubs:
                print(f"    note: {stubs} 'load more comments' stub(s) not expanded "
                      f"({post['post_id']})")
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


def write_summary(ctx: Ctx):
    summary = {
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
          f"blocked responses (403/429/non-Reddit page)\n")

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
            await asyncio.gather(*(produce_posts(ctx, s, queue) for s in SUBREDDITS))
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
    ap.add_argument("--cookie-file", default="cookies.txt",
                    help="REQUIRED: cookie export from a logged-in reddit.com session "
                         "(Netscape / JSON / raw Cookie line). Default: ./cookies.txt")
    ap.add_argument("--list-cookies", action="store_true",
                    help="print the names/domains/expiry of the cookies in the file "
                         "(never the values) and exit")
    ap.add_argument("--skip-cookie-check", action="store_true",
                    help="start even if no reddit_session/token_v2 cookie is present")
    ap.add_argument("--proxy", default=None,
                    help="proxy URL, e.g. http://user:pass@host:port")
    args = ap.parse_args()

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

    ctx = Ctx(
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

    s = write_summary(ctx)
    print(f"\nDone. {s['posts']} posts "
          f"({s['posts_with_comments_scraped']} with comments scraped), "
          f"{s['comments']} comments -> {args.out}/")
    if s["stopped_early"]:
        incomplete = [k for k, v in s["subreddit_listing_status"].items()
                      if v != "complete"]
        print(f"Stopped early: {s['stop_reason']}")
        if incomplete:
            print(f"Subreddit listings not fully crawled: {', '.join(incomplete)}")
        sys.exit(2)


if __name__ == "__main__":
    main()