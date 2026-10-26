"""
Async Reddit scraper (no Reddit API) - scores the sentiment of posts + comments
that mention the tracked tickers, from the last N days.

Design
------
* All subreddits are listed concurrently (one producer task per subreddit).
* Each post found goes on a queue; a worker pool fetches comment pages while
  the listings are still being crawled.
* A semaphore caps in-flight requests; any throttling triggers a global
  cooldown so every task pauses together.
* Circuit breaker: N consecutive blocked responses (403/429, or a 200 that is
  not real old-Reddit HTML) stops the run early; everything scored so far is saved.
* Collection modes: crawl every subreddit's /new, search per ticker, or both
  (auto = search when the ticker list is small). /hot and /rising are also read,
  so stickied daily threads and older posts with fresh comments are not missed.
  Daily / very large threads get their comments fetched under two sorts.
* Scoring: each fetched thread (the post plus its comments) is one batch. The
  items in it are filtered and attributed to tickers (reddit_processing.py) and
  passed to the shared sentiment scorer (score_texts, 1..10). score_texts only
  reports through a file, so a temporary directory is used and always deleted.
  A short reply is scored together with the text it answers (parent comment /
  post title, kept short) so "yes, get out now" is read in context.
  No post / comment text is written to disk.

Results
-------
At the end of the run (or if it is interrupted or crashes part-way) the scores
are written to one JSON file (default: reddit_sentiments.json next to this
script, override with --output; overwritten each run, replaced atomically).
The file is also refreshed every --checkpoint-secs (default 60) while the run is
going, so a hard kill loses at most that much work. If the run fails before any
scraping starts (missing credentials, no tickers, login failure) the existing
file is left untouched.
Format: a list with one object per ticker, sorted by ticker:
    [{"ticker": "NVDA", "average_sentiment": 6.12, "post_count": 2,
      "posts": {"t3_abc123": 7.4, "t1_def456": 4.84}}, ...]
"posts" / "post_count" hold every scored item (posts AND comments), keyed by
Reddit's own fullname (t3_ = post, t1_ = comment). An item that mentions several
tickers appears under each of them. A ticker with no items has
average_sentiment null, post_count 0 and an empty posts object.

Logging
-------
Requests, retries, blocks, parse problems and scoring are written to a rotating
log file (default: reddit.log next to this script, override with --log-file).
Timestamps are UTC. The log never contains credentials, cookies or item text.

Tickers
-------
Loaded with fetch_ticker_map() from fetch_tickers.py ({company name: ticker}).
The script stops with an error if no tickers can be loaded.

Getting a Reddit session (two ways)
-----------------------------------
Reddit redirects logged-out visitors to a login page, so every run needs a
logged-in session. By default it is read from a cookie file (as before):
  Default : cookie export from your normal, logged-in browser -> ./reddit_cookies.txt
            (Netscape / JSON / raw Cookie line)
  --login : log in with Playwright instead (REDDIT_USERNAME / REDDIT_PASSWORD from
            env vars or a .env file). Reddit may reject automated browsers, which is
            why the cookie file is the default. Cookies stay in memory, never saved.

Run
---
  python reddit.py --max-posts 3
  python reddit.py --days 7
  python reddit.py --backend playwright --headed --max-posts 3
  python reddit.py --login --headed  (automatic Playwright login, visible window)
  python reddit.py --mode search --search-pages 3
  python reddit.py --output results.json --log-file logs/reddit.log
  python reddit.py --days 3 --time 30  (stop making requests after 30 min)

--time T stops making new requests after T minutes (counted from start, login
included). Threads already fetched are still scored and the results saved;
this is not counted as stopping early (exit code 0).
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import logging.handlers
import os
import random
import re
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, NamedTuple
from urllib.parse import urlencode, urlparse

from bs4 import BeautifulSoup

# Make `src.utils` importable no matter where/how this file is launched.
# File lives at backend/src/services/sentiment/reddit/, so backend/ is parents[4].
BACKEND_DIR = Path(__file__).resolve().parents[4]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from src.utils.console import stdout_logger  # noqa: E402
from src.utils.fetch_tickers import fetch_ticker_map  # noqa: E402

try:
    from ..sentiment_scorer import load_scores, score_texts  # as a package
    from .reddit_processing import (
        TickerMatcher,
        build_scoring_items,
        is_discussion_title,
        load_json_file,
    )
except ImportError:  # run as a script
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from reddit_processing import (
        TickerMatcher,
        build_scoring_items,
        is_discussion_title,
        load_json_file,
    )
    from sentiment_scorer import load_scores, score_texts

_out = stdout_logger()  # console lines (print() is banned in src/)

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
    "subreddit",
    "post_id",
    "title",
    "author",
    "created_utc",
    "score",
    "num_comments",
    "flair",
    "domain",
    "url",
    "permalink",
    "selftext",
    "comments_scraped",
]
COMMENT_FIELDS = [
    "subreddit",
    "post_id",
    "comment_id",
    "parent_id",
    "author",
    "created_utc",
    "score",
    "body",
    "permalink",
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


# --------------------------------------------------------------------------- #
# Automated Reddit login (Playwright, SYNC API)
#
# This runs ONCE at the start of main(), before the async scraper starts. It
# logs in with a throw-away in-memory browser, then returns the session cookies
# as a plain list of dicts. The scraper backends (curl / playwright) take that
# list from args.cookies exactly as they used to take it from the cookie file.
# Nothing is written to disk by the login (no cookie file, no debug dumps).
# --------------------------------------------------------------------------- #
log = logging.getLogger("reddit_scraper")

LOGIN_URL = "https://www.reddit.com/login"
COOKIE_FILE = "reddit_cookies.txt"  # default session source (relative to where you run the script)
VERIFY_URL = "https://old.reddit.com/"  # the site we actually scrape
OUTPUT_FILE = Path(__file__).with_name("reddit_sentiments.json")
LOG_FILE = Path(__file__).with_name("reddit.log")
LOG_MAX_BYTES = 5_000_000  # rotate at ~5 MB, keep 3 old files
LOG_BACKUPS = 3
BODY_SNIPPET = 200  # max chars of a response body to log
CONTEXT_CHARS = 200  # max chars of parent text joined in front of a reply
LOGIN_TIMEOUT_S = 45  # how long an automatic login may take
MANUAL_TIMEOUT_S = 600  # how long we wait for YOU to finish a manual login

REDDIT_SESSION_COOKIES = ("reddit_session", "token_v2")


class LoginFailed(Exception):
    """Login definitely did not work (wrong password, blocked, timed out...)."""


class ManualStepNeeded(Exception):
    """Captcha / 2FA / unknown page: a human has to look at it."""


_SECRETS: set[str] = set()


def register_secrets(*values):
    """Values (passwords, cookies, proxy credentials) that must never reach the log."""
    for v in values:
        if v and len(v) >= 4:
            _SECRETS.add(v)


class _LogFormatter(logging.Formatter):
    """UTC timestamps; blanks out any registered secret, also inside tracebacks."""

    converter = time.gmtime

    def format(self, record):
        text = super().format(record)
        for secret in sorted(_SECRETS, key=len, reverse=True):
            text = text.replace(secret, "***")
        return text


def setup_logging(path):
    """Log to a rotating file only (not the console), UTC timestamps. Safe to call twice."""
    if log.handlers:
        return
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(
        path, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUPS, encoding="utf-8"
    )
    handler.setFormatter(
        _LogFormatter(
            "%(asctime)s %(levelname)-7s %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ"
        )
    )
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.propagate = False


def die(message: str):
    """Log the problem as an ERROR, then exit with the same message as before."""
    log.error("%s", message.removeprefix("ERROR: "))
    sys.exit(message)


# Thread URLs end in a slug made from the post title (= item text): never log it.
_THREAD_SLUG_RE = re.compile(r"(/comments/[a-z0-9]+)/[^/?#\s]+", re.I)


def _scrub(text: str) -> str:
    return _THREAD_SLUG_RE.sub(r"\1", text)


def _snippet(text: str) -> str:
    """First BODY_SNIPPET chars of a response body, single line, <title> blanked."""
    text = re.sub(r"<title>.*?</title>", "<title/>", text or "", flags=re.S | re.I)
    return _scrub(" ".join(text.split()))[:BODY_SNIPPET]


def _req_label(url: str, params: dict | None = None) -> str:
    """Short, log-safe description of a request (path + query, no host, no slug)."""
    label = _scrub(url.replace(BASE, "", 1))
    if params:
        label += ("&" if "?" in label else "?") + urlencode(params)
    return label


def get_credentials() -> tuple[str, str]:
    """Read REDDIT_USERNAME / REDDIT_PASSWORD from the environment or a .env file."""
    try:
        from dotenv import load_dotenv  # optional: pip install python-dotenv

        env_file = Path(__file__).resolve().parent / ".env"  # .env next to this script
        # override=True: the .env file wins over any old REDDIT_* variable already set in
        # your shell. interpolate=False: a '$' in your password is kept literally.
        if env_file.exists():
            load_dotenv(env_file, override=True, interpolate=False)
            log.info("Credentials loaded from %s", env_file)
        else:
            load_dotenv(override=True, interpolate=False)  # .env in current folder
    except ImportError:
        log.info("python-dotenv not installed; using real environment variables only")
    username = os.environ.get("REDDIT_USERNAME", "").strip()
    password = os.environ.get("REDDIT_PASSWORD", "")
    register_secrets(username, password)  # redacted from the log if they ever leak in
    log.info(
        "Loaded REDDIT_USERNAME (%d chars) and REDDIT_PASSWORD (%d chars)",
        len(username),
        len(password),
    )  # lengths only; a '#' can silently cut a password
    missing = [
        n
        for n, v in (("REDDIT_USERNAME", username), ("REDDIT_PASSWORD", password))
        if not v
    ]
    if missing:
        die(
            f"ERROR: missing {' and '.join(missing)}. Set them as environment "
            f"variables or put them in a .env file (see .env.example)."
        )
    return username, password


# ---- cookie file (default way to get a session) ---------------------------- #
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
    if text[0] in "[{":  # ---- JSON
        try:
            obj = json.loads(text)
        except json.JSONDecodeError as e:
            raise CookieFileError(f"{path} looks like JSON but can't be parsed: {e}")
        items = obj.get("cookies", []) if isinstance(obj, dict) else obj
        for c in items:
            if isinstance(c, dict) and c.get("name"):
                raw.append(
                    {
                        "name": c["name"],
                        "value": str(c.get("value", "")),
                        "domain": c.get("domain") or ".reddit.com",
                        "path": c.get("path") or "/",
                        "secure": bool(c.get("secure", True)),
                        "expires": _to_expiry(
                            c.get("expirationDate", c.get("expires", c.get("expiry")))
                        ),
                    }
                )
    else:
        lines = [ln.rstrip("\r") for ln in text.splitlines()]
        if any(len(ln.split("\t")) >= 7 for ln in lines):  # ---- Netscape cookies.txt
            for ln in lines:
                ln = ln.strip()
                if ln.startswith("#HttpOnly_"):
                    ln = ln[len("#HttpOnly_") :]
                elif not ln or ln.startswith("#"):
                    continue
                parts = ln.split("\t")
                if len(parts) >= 7:
                    domain, _flag, cpath, secure, expiry, name, value = parts[:7]
                    raw.append(
                        {
                            "name": name,
                            "value": value,
                            "domain": domain,
                            "path": cpath or "/",
                            "secure": secure.strip().upper() == "TRUE",
                            "expires": _to_expiry(expiry),
                        }
                    )
        else:  # ---- raw Cookie header
            header = (
                text.splitlines()[0]
                if "\n" not in text.strip()
                else text.replace("\n", ";")
            )
            if header.lower().startswith("cookie:"):
                header = header[len("cookie:") :]
            for c in parse_cookie_header(header):
                raw.append(
                    {
                        **c,
                        "domain": ".reddit.com",
                        "path": "/",
                        "secure": True,
                        "expires": None,
                    }
                )

    if not raw:
        raise CookieFileError(f"no cookies could be parsed from {path}\n")

    now = time.time()
    kept: dict[tuple, dict] = {}
    expired = 0
    for c in raw:
        if not c["domain"].lstrip(".").endswith("reddit.com"):
            continue  # ignore other sites' cookies
        if c["expires"] and c["expires"] < now:
            expired += 1
            continue
        kept[(c["name"], c["domain"], c["path"])] = c  # later entries win
    if expired:
        _out.info(f"note: ignored {expired} expired cookie(s) in {path}")
    if not kept:
        raise CookieFileError(
            f"{path} has no (unexpired) reddit.com cookies. Were you on a reddit.com "
            f"tab, logged in, when you exported?\n"
        )
    return list(kept.values())


def prepare_cookie_file(args) -> list[dict]:
    """Default mode: load the cookie export and make sure it holds a Reddit login session."""
    try:
        cookies = load_cookie_file(COOKIE_FILE)
    except CookieFileError as e:
        die(f"ERROR: {e}")
    register_secrets(
        *(c["value"] for c in cookies)
    )  # never let cookie values reach the log
    _out.info(f"Loaded {len(cookies)} reddit.com cookies from {COOKIE_FILE}")
    cookie_names = sorted({c["name"] for c in cookies})
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
            "in that browser profile."
        )
        die(f"ERROR: {msg}")
    return cookies


# ---- small Playwright helpers ---------------------------------------------- #
def _visible_text(page, pattern) -> bool:
    """True if some VISIBLE text on the page matches the regex (pierces shadow DOM)."""
    try:
        loc = page.get_by_text(pattern)
        return loc.count() > 0 and loc.first.is_visible()
    except Exception:
        return False


def _visible(locator) -> bool:
    try:
        return locator.count() > 0 and locator.first.is_visible()
    except Exception:
        return False


def _find_first(page, candidates, what, timeout_s=15):
    """Return the first candidate locator that becomes visible, else ask for a human."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for loc in candidates:
            if _visible(loc):
                return loc.first
        page.wait_for_timeout(250)
    raise ManualStepNeeded(f"could not find the {what} on the login page")


def _on_login_page(page) -> bool:
    return urlparse(page.url).path.rstrip("/").startswith("/login")


def _dismiss_consent_banner(page):
    """Reddit's cookie-consent popup can cover the form. Click it away if present."""
    try:
        # Targets a button whose text is 'Reject non-essential' or 'Accept all'
        btn = page.get_by_role(
            "button", name=re.compile(r"reject non-essential|accept all", re.I)
        )
        if _visible(btn):
            btn.first.click(timeout=3000)
    except Exception:
        pass


# Text Reddit shows for each situation. If Reddit rewords them, fix them here.
BAD_CREDENTIALS = re.compile(
    r"incorrect username or password|invalid username or password|"
    r"wrong username or password|couldn.t find an account",
    re.I,
)
RATE_LIMITED = re.compile(
    r"too many (attempts|requests|tries)|try again later|rate.?limit", re.I
)
BLOCKED = re.compile(r"blocked by network security|whoa there, pardner", re.I)
CAPTCHA_TEXT = re.compile(
    r"verify you are (a )?human|prove your humanity|"
    r"i.m not a robot|complete the captcha|security check",
    re.I,
)
TWO_FACTOR_TEXT = re.compile(
    r"two-factor|2fa|verification code|authentication code|"
    r"6-digit|enter the code|we sent you a code",
    re.I,
)


def _matched_text(page, pattern) -> str:
    """The on-page text that matched (Reddit's own wording, never your input), shortened."""
    try:
        return " ".join(page.get_by_text(pattern).first.inner_text().split())[:150]
    except Exception:
        return "?"


def _check_hard_failure(page):
    """Raise LoginFailed if the page clearly says the login was refused."""
    if _visible_text(page, BAD_CREDENTIALS):
        raise LoginFailed(
            "Reddit rejected the username or password. Reddit's page said: "
            f'"{_matched_text(page, BAD_CREDENTIALS)}". Check REDDIT_USERNAME / '
            "REDDIT_PASSWORD in your .env (use single quotes around a password "
            "with $ # or \\ in it). Re-run with --debug-login to save a screenshot."
        )
    if _visible_text(page, BLOCKED):
        raise LoginFailed(
            "Reddit blocked this connection ('network security' page). "
            "Wait a while, try another network, or use a --proxy."
        )
    if _visible_text(page, RATE_LIMITED):
        raise LoginFailed(
            "Reddit says there were too many login attempts. "
            "Wait 15-30 minutes and run again."
        )


def _detect_challenge(page) -> str | None:
    """Return a short description if a captcha or 2FA prompt is showing, else None."""
    # A visible captcha iframe (ignoring Google's tiny invisible reCAPTCHA badge)
    frames = page.locator(
        "iframe[src*='captcha' i], iframe[title*='captcha' i], "
        "iframe[src*='arkoselabs' i]"
    )
    try:
        for i in range(frames.count()):
            fr = frames.nth(i)
            src = (fr.get_attribute("src") or "").lower()
            box = fr.bounding_box() if fr.is_visible() else None
            if (
                box
                and "size=invisible" not in src
                and box["width"] >= 200
                and box["height"] >= 50
            ):
                return "a captcha"
    except Exception:
        pass
    if _visible_text(page, CAPTCHA_TEXT):
        return "a captcha / human check"
    # 2FA: a one-time-code input or the usual wording
    code_box = page.locator("input[autocomplete='one-time-code'], input[name='otp']")
    code_label = page.get_by_label(
        re.compile(r"verification code|authentication code|one-time", re.I)
    )
    if (
        _visible(code_box)
        or _visible(code_label)
        or _visible_text(page, TWO_FACTOR_TEXT)
    ):
        return "a two-factor (2FA) code prompt"
    return None


def _wait_for_login(page, manual_ok: bool):
    """
    Wait (no fixed sleep) until we have left the login page or something needs a human.
    manual_ok=True means a visible window is open, so we may wait for the user.
    """
    deadline = time.monotonic() + LOGIN_TIMEOUT_S
    waiting_for_human = False
    while True:
        challenge = None
        if not waiting_for_human:
            try:
                _check_hard_failure(page)
            except LoginFailed:
                # With a visible window a rejected password is not the end: you can type
                # it by hand. (Headless: stop here with the clear error message.)
                if not (manual_ok and _visible_text(page, BAD_CREDENTIALS)):
                    raise
                challenge = (
                    "Reddit rejected the username/password the script typed "
                    "(retype them yourself in the window)"
                )
        if challenge is None:
            challenge = _detect_challenge(page)
        # success = we left /login AND no password box is still showing
        if (
            challenge is None
            and not _on_login_page(page)
            and not _visible(page.locator("input[type='password']"))
        ):
            return
        timed_out = time.monotonic() > deadline
        if (challenge or timed_out) and not waiting_for_human:
            reason = challenge or "the page did not finish logging in"
            if not manual_ok:
                raise ManualStepNeeded(
                    f"Reddit is showing {reason}" if challenge else reason
                )
            waiting_for_human = True
            deadline = time.monotonic() + MANUAL_TIMEOUT_S
            log.warning(
                "ACTION NEEDED: %s. In the browser window that is open, finish "
                "logging in by hand (solve the captcha / type the 2FA code / click "
                "Log In). The script continues by itself once you are in. "
                "Waiting up to %d minutes...",
                reason,
                MANUAL_TIMEOUT_S // 60,
            )
            _out.info(
                f"ACTION NEEDED: {reason}. Finish logging in by hand in the browser "
                f"window that is open; the script continues by itself afterwards."
            )
        elif timed_out and waiting_for_human:
            raise LoginFailed(
                f"Timed out after {MANUAL_TIMEOUT_S // 60} minutes waiting "
                f"for the manual login to finish."
            )
        page.wait_for_timeout(500)


def _verify_logged_in(page) -> bool:
    """Load old.reddit.com (the site we scrape) and confirm the header shows a logged-in user."""
    page.goto(VERIFY_URL, wait_until="domcontentloaded", timeout=45_000)
    if _on_login_page(page):
        return False
    # Targets old Reddit's header 'logout' form. It only exists when logged in
    # (logged-out visitors see 'want to join? log in or sign up' instead).
    try:
        page.wait_for_selector(
            "#header-bottom-right form.logout", state="attached", timeout=10_000
        )
        return True
    except Exception:
        return False


def _cookies_for_scraper(context) -> list[dict]:
    """Playwright cookies -> the simple dict format the scraper backends expect."""
    out = []
    for c in context.cookies():
        if not c["domain"].lstrip(".").endswith("reddit.com"):
            continue
        exp = c.get("expires", -1)
        out.append(
            {
                "name": c["name"],
                "value": c["value"],
                "domain": c["domain"],
                "path": c.get("path") or "/",
                "secure": bool(c.get("secure", True)),
                "expires": exp if exp and exp > 0 else None,
            }
        )  # -1 = session cookie
    return out


def _save_debug_screenshot(page, debug: bool, tag: str):
    """Only with --debug-login: save a screenshot of the failed login to debug/.
    It can show your username (the password box is masked by the browser)."""
    if not (debug and page):
        return
    try:
        Path("debug").mkdir(exist_ok=True)
        path = Path("debug") / f"login_{time.strftime('%Y%m%d_%H%M%S')}_{tag}.png"
        page.screenshot(path=str(path), full_page=True)
        _out.info(f"Saved login screenshot: {path}")
    except Exception as e:
        _out.info(f"Could not save login screenshot ({type(e).__name__})")


def _describe(box) -> str:
    """What kind of element we found, e.g. 'input type=password name=password'."""
    try:
        return box.evaluate(
            "e => e.tagName.toLowerCase() + ' type=' + (e.type || '-')"
            " + ' name=' + (e.name || '-')"
        )
    except Exception:
        return "unknown element"


def _type_into(box, value: str, what: str, debug: bool):
    """Click the box and type key-by-key at a human-ish speed. If the box does not keep
    EXACTLY what we typed, clear it and fill it instantly instead.
    Values are only compared in memory; never printed or logged."""

    def kept() -> bool:
        try:
            return box.input_value() == value
        except Exception:
            return False

    box.click()
    box.press_sequentially(value, delay=random.randint(55, 120))  # ms between keys
    method = "typed"
    if not kept():
        box.fill("")
        box.fill(value)
        method = "filled"
    ok = kept()
    if debug:
        try:
            n = len(box.input_value())
        except Exception:
            n = -1
        _out.info(
            f"[debug-login] {what} box = {_describe(box)}; it holds {n} characters, "
            f"script typed {len(value)} ({method}): {'match' if ok else 'MISMATCH'}"
        )
    if not ok:
        raise ManualStepNeeded(f"the {what} box did not keep the text the script typed")


def _login_once(
    username, password, headed: bool, proxy, manual_ok: bool, debug: bool = False
) -> list[dict]:
    """One login attempt in a fresh, in-memory browser. Returns the session cookies."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        die(
            "playwright is not installed. Run:  pip install playwright "
            "&& playwright install chromium"
        )
    tag = "headed" if headed else "headless"
    log.info("Logging in to Reddit (%s browser)...", tag)
    with sync_playwright() as pw:
        launch = {
            "headless": not headed,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        if proxy:
            launch["proxy"] = {"server": proxy}
        # Prefer the Chrome (or Edge) already installed on this computer: Reddit treats it
        # like a normal browser. Fall back to Playwright's bundled Chromium if neither exists.
        browser, used, last_err = None, None, None
        for channel in ("chrome", "msedge", None):
            try:
                extra = {"channel": channel} if channel else {}
                browser = pw.chromium.launch(
                    ignore_default_args=[
                        "--enable-automation"
                    ],  # drops the 'automated' banner flag
                    **launch,
                    **extra,
                )
                used = channel or "bundled-chromium"
                break
            except Exception as e:
                last_err = e
        if browser is None:
            die(
                f"ERROR: could not start a browser ({str(last_err)[:150]}). "
                f"Did you run:  playwright install chromium ?"
            )
        log.info("Login browser: %s", used)
        if debug:
            _out.info(f"[debug-login] browser = {used}")
        page = None
        try:
            # new_context() is in-memory only: nothing is saved to disk
            ctx_opts = dict(
                viewport={"width": 1366, "height": 900},
                locale="en-US",
                timezone_id="America/Los_Angeles",
            )
            if used == "bundled-chromium" and not headed:
                ctx_opts["user_agent"] = (
                    CHROME_UA  # only needed to hide 'HeadlessChrome'
                )
            context = browser.new_context(**ctx_opts)
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            )
            page = context.new_page()
            page.set_default_timeout(20_000)

            page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=45_000)
            _dismiss_consent_banner(page)

            # --- username box: the field labelled 'Email or username' ---
            user_box = _find_first(
                page,
                [
                    page.get_by_label(re.compile(r"email or username|username", re.I)),
                    page.get_by_placeholder(re.compile(r"username", re.I)),
                    page.locator("input[name='username']"),  # last resort
                ],
                "username box",
            )
            _type_into(user_box, username, "username", debug)

            # --- password box: the field labelled 'Password' ---
            pass_box = _find_first(
                page,
                [
                    page.get_by_label(re.compile(r"password", re.I)),
                    page.get_by_placeholder(re.compile(r"password", re.I)),
                    page.locator("input[type='password']"),  # last resort
                ],
                "password box",
            )
            _type_into(pass_box, password, "password", debug)

            page.wait_for_timeout(
                random.randint(500, 1100)
            )  # brief pause, like a person
            # --- submit: the button whose text is 'Log In' (else press Enter) ---
            submit = page.get_by_role(
                "button", name=re.compile(r"^\s*log in\s*$", re.I)
            )
            if _visible(submit):
                submit.first.click()
            else:
                pass_box.press("Enter")

            _wait_for_login(page, manual_ok)

            if not _verify_logged_in(page):
                msg = (
                    "login looked finished, but old.reddit.com does not show a "
                    "logged-in user"
                )
                if manual_ok:
                    raise LoginFailed(msg.capitalize() + ".")
                raise ManualStepNeeded(msg)
            cookies = _cookies_for_scraper(context)
            register_secrets(*(c["value"] for c in cookies))
            log.info(
                "Login confirmed on old.reddit.com; keeping %d reddit.com cookies "
                "in memory only (values never logged).",
                len(cookies),
            )
            return cookies
        except (LoginFailed, ManualStepNeeded):
            _save_debug_screenshot(page, debug, tag)
            raise
        except Exception as e:  # timeouts, navigation errors, closed window...
            _save_debug_screenshot(page, debug, tag)
            detail = f"unexpected {type(e).__name__}: {str(e)[:150]}"
            raise (LoginFailed(detail) if manual_ok else ManualStepNeeded(detail))
        finally:
            browser.close()


def login_to_reddit(
    username, password, headed=False, proxy=None, debug=False
) -> list[dict]:
    """
    Log in and return the session cookies (in memory only).
    Headless first; if a captcha / 2FA / unknown step appears, relaunch with a
    visible window and wait for the user. With --headed it starts visible.
    """
    try:
        return _login_once(
            username, password, headed, proxy, manual_ok=headed, debug=debug
        )
    except ManualStepNeeded as e:
        if headed:  # already visible: nothing more to relaunch
            raise LoginFailed(str(e))
        log.warning(
            "Automatic login could not finish: %s. Relaunching with a VISIBLE "
            "browser window so you can complete it.",
            e,
        )
        try:
            return _login_once(
                username, password, True, proxy, manual_ok=True, debug=debug
            )
        except ManualStepNeeded as e2:
            raise LoginFailed(str(e2))


class CurlBackend:
    """curl_cffi: libcurl with a Chrome TLS/JA3 + HTTP/2 fingerprint."""

    def __init__(self, args):
        try:
            from curl_cffi.requests import AsyncSession
        except ImportError:
            die("curl_cffi is not installed. Run:  pip install curl_cffi")
        proxies = {"http": args.proxy, "https": args.proxy} if args.proxy else None
        # impersonate= sets browser-matching TLS, HTTP/2 settings and header order
        self.session = AsyncSession(
            impersonate=args.impersonate, proxies=proxies, timeout=30
        )
        cookies = getattr(args, "cookies", None) or []
        try:
            # jar cookies, so refreshed tokens from Set-Cookie replace the originals
            for c in cookies:
                self.session.cookies.set(
                    c["name"], c["value"], domain=c["domain"], path=c["path"]
                )
        except Exception:
            self.session.headers["Cookie"] = "; ".join(
                f"{c['name']}={c['value']}" for c in cookies
            )

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
            die(
                "playwright is not installed. Run:  pip install playwright "
                "&& playwright install chromium"
            )
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
            d = {
                "name": c["name"],
                "value": c["value"],
                "domain": c["domain"],
                "path": c["path"],
                "secure": c["secure"],
            }
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
    return (
        PlaywrightBackend(args) if args.backend == "playwright" else CurlBackend(args)
    )


# --------------------------------------------------------------------------- #
# Shared run state
# --------------------------------------------------------------------------- #
@dataclass
class Ctx:
    cutoff: datetime
    delay: float
    max_blocks: int
    max_posts: int | None
    retries: int = 6
    stale_min_comments: int = 50  # older posts (from /hot) need this many comments

    backend: Any = None
    sem: asyncio.Semaphore = None
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    stop_reason: str = ""
    time_up: asyncio.Event = field(default_factory=asyncio.Event)  # --time reached
    deadline: float | None = None  # time.monotonic() value from --time

    consecutive_blocks: int = 0
    total_blocks: int = 0
    total_requests: int = 0
    cooldown_until: float = 0.0  # time.monotonic() value
    login_walls: int = 0
    empty_comment_posts: int = 0  # posts that report comments but parsed 0
    parsed_comment_posts: int = 0  # posts where at least one comment parsed
    seen_ids: set = field(default_factory=set)  # post ids already registered

    posts: list = field(default_factory=list)
    comments_total: int = 0  # comments parsed (their text is not kept)
    sub_status: dict = field(default_factory=dict)
    posts_queued: int = 0
    posts_done: int = 0

    # scoring + results
    matcher: Any = None
    score_lock: asyncio.Lock = None
    results: dict = field(default_factory=dict)  # {ticker: {item_id: score}}
    item_keys: set = field(default_factory=set)  # (ticker, item_id) already handled
    scored_ids: set = field(default_factory=set)  # distinct item ids that got a score
    scoring_failures: int = 0  # consecutive failed batches
    text_seen: set = field(default_factory=set)  # (ticker, hash of text), no text kept
    output_path: str = ""
    tickers: set = field(default_factory=set)  # every queried ticker
    saved_count: int = 0  # items in the last checkpoint


def trip(ctx: Ctx, reason: str, level: int = logging.ERROR):
    """Trip the circuit breaker: tell every task to wind down."""
    if not ctx.stop.is_set():
        ctx.stop_reason = reason
        ctx.stop.set()
        log.log(level, "stopping early: %s", reason)
        _out.info(
            f"\n!!! STOPPING EARLY: {reason}\n    Cancelling in-flight requests and saving...\n"
        )


# --------------------------------------------------------------------------- #
# Fetch with block detection
# --------------------------------------------------------------------------- #
def halted(ctx: Ctx) -> bool:
    """No new requests: the circuit breaker tripped or the --time limit was reached."""
    return ctx.stop.is_set() or ctx.time_up.is_set()


async def nap(ctx: Ctx, seconds: float):
    """Sleep, but wake immediately if the circuit breaker trips or time is up."""
    if seconds <= 0:
        return
    waiters = {
        asyncio.ensure_future(ctx.stop.wait()),
        asyncio.ensure_future(ctx.time_up.wait()),
    }
    try:
        await asyncio.wait(
            waiters, timeout=seconds, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        for w in waiters:
            w.cancel()


async def time_limit(ctx: Ctx):
    """Set ctx.time_up at the --time deadline. In-flight requests finish, no new
    ones start, and every thread already fetched is still scored."""
    await asyncio.sleep(max(0.0, ctx.deadline - time.monotonic()))
    if not ctx.stop.is_set():
        log.info("time limit reached, no more requests; scoring what was fetched")
        _out.info(
            "\nTime limit reached: no more requests, scoring what was fetched...\n"
        )
    ctx.time_up.set()


async def fetch(
    ctx: Ctx,
    url: str,
    params: dict | None = None,
    expect: str | None = "#siteTable",
    info: dict | None = None,
):
    """
    GET a page and return a BeautifulSoup, or None on failure / stop.

    A response only counts as success if it is real old.reddit HTML (contains
    `expect`). A 200 that is really a login wall / block page / redirect to the
    new site is a soft block, treated like 403/429.

    Every wait here wakes immediately if the circuit breaker trips, and tasks
    that were queued for a request slot re-check the global cooldown, so one
    block really does pause everybody.

    If `info` is given, the latency of the successful request (seconds) is
    stored in info["latency"] so the caller can log it.
    """
    what = _req_label(url, params)
    attempt = 0
    while attempt < ctx.retries:
        if halted(ctx):
            return None

        wait = ctx.cooldown_until - time.monotonic()
        if wait > 0:
            await nap(ctx, wait)
            continue

        throttled = False
        latency = None
        async with ctx.sem:
            if halted(ctx):
                return None
            if ctx.cooldown_until > time.monotonic():
                throttled = True  # a block happened while we waited for a slot
            else:
                await nap(ctx, random.uniform(0.5, 1.5) * ctx.delay)  # jitter
                if halted(ctx):
                    return None
                try:
                    ctx.total_requests += 1
                    started = time.monotonic()
                    # Race the request against the stop signal so a hung
                    # request can never delay shutdown
                    req = asyncio.ensure_future(ctx.backend.get(url, params))
                    stopper = asyncio.ensure_future(ctx.stop.wait())
                    try:
                        await asyncio.wait(
                            {req, stopper}, return_when=asyncio.FIRST_COMPLETED
                        )
                    finally:
                        stopper.cancel()
                        if not req.done():
                            req.cancel()
                    if req.cancelled():
                        return None
                    r = req.result()
                    latency = time.monotonic() - started
                except Exception as e:  # network error, timeout, browser error...
                    _out.info(
                        f"    network error ({type(e).__name__}: {str(e)[:80]}); "
                        f"retry {attempt + 1}"
                    )
                    log.warning(
                        "%s network error attempt=%d/%d: %s: %s",
                        what,
                        attempt + 1,
                        ctx.retries,
                        type(e).__name__,
                        _scrub(str(e))[:BODY_SNIPPET],
                    )
                    r = Resp(0, None, url, False, "")
        if throttled:
            continue  # doesn't use up a retry

        if ctx.stop.is_set():
            return None  # breaker tripped while this request was in flight

        soft = False
        moved = ""
        if r.status == 200:
            soup = await asyncio.to_thread(BeautifulSoup, r.text, "lxml")
            if expect is None or soup.select_one(expect) is not None:
                ctx.consecutive_blocks = 0
                ctx.login_walls = 0
                if attempt > 0:
                    log.warning("%s succeeded after %d attempts", what, attempt + 1)
                if info is not None:
                    info["latency"] = latency
                return soup
            title = soup.title.get_text(strip=True) if soup.title else "(no title)"
            moved = f" (redirected to {r.final_url})" if r.redirected else ""
            _out.info(f"    unexpected page{moved}; title: {title!r}")
            soft = True
            if "/login" in urlparse(r.final_url).path or title.startswith(
                "Welcome to Reddit"
            ):
                # A login wall is deterministic: retrying and cooling down won't help
                ctx.login_walls += 1
                if ctx.login_walls >= 3:
                    trip(
                        ctx,
                        "Reddit is redirecting requests to its login page, so "
                        "your session (cookie file or --login) was rejected, logged "
                        "out or expired. Re-export a fresh cookie file from a "
                        "logged-in browser (or re-run with --login) and try again",
                    )
                    return None

        if soft or r.status in (403, 429):
            attempt += 1
            ctx.consecutive_blocks += 1
            ctx.total_blocks += 1
            label = "unexpected non-Reddit page" if soft else f"HTTP {r.status}"
            _out.info(
                f"    {label} (consecutive blocks: "
                f"{ctx.consecutive_blocks}/{ctx.max_blocks})"
            )
            try:
                cooldown = float(r.retry_after) if r.retry_after else None
            except ValueError:
                cooldown = None
            if cooldown is None:
                cooldown = min(120, 10 * ctx.consecutive_blocks)
            if soft:
                reason = f"page loaded but selector {expect!r} not found{_scrub(moved)}"
            elif r.status == 429:
                reason = "rate limited (HTTP 429)"
            else:
                reason = f"blocked (HTTP {r.status})"
            log.warning(
                "%s %s attempt=%d/%d sleeping=%.0fs consecutive_blocks=%d/%d body=%s",
                what,
                reason,
                attempt,
                ctx.retries,
                cooldown,
                ctx.consecutive_blocks,
                ctx.max_blocks,
                _snippet(r.text),
            )
            if ctx.consecutive_blocks >= ctx.max_blocks:
                trip(
                    ctx,
                    f"{ctx.consecutive_blocks} consecutive blocked responses "
                    f"(403/429 or non-Reddit page)",
                )
                return None
            ctx.cooldown_until = max(ctx.cooldown_until, time.monotonic() + cooldown)
            continue

        if r.status == 0 or r.status >= 500:
            pause = min(60, 2**attempt * 3)
            if r.status >= 500:  # status 0 (network error) was logged above
                log.warning(
                    "%s server error status=%d attempt=%d/%d sleeping=%ds body=%s",
                    what,
                    r.status,
                    attempt + 1,
                    ctx.retries,
                    pause,
                    _snippet(r.text),
                )
            await nap(ctx, pause)
            attempt += 1
            continue

        _out.info(f"    HTTP {r.status} for {url}; skipping")  # 404, 410, ...
        log.error(
            "%s request rejected status=%d body=%s", what, r.status, _snippet(r.text)
        )
        return None

    log.error("%s failed after %d attempts", what, ctx.retries)
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
        posts.append(
            {
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
            }
        )

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
            reached_cutoff = True  # sort=new: everything after this is older
            break
        author = t.select_one("a.author, span.author")
        flair = t.select_one("span.linkflairlabel")
        posts.append(
            {
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
            }
        )

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
    t = entry.select_one("time.live-timestamp[datetime]") or entry.select_one(
        "time[datetime]"
    )
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

        comments.append(
            {
                "subreddit": post["subreddit"],
                "post_id": post["post_id"],
                "comment_id": c.get("data-fullname", "").replace("t1_", ""),
                "parent_id": parent_id or post["post_id"],  # top-level -> post id
                "author": c.get("data-author", ""),
                "created_utc": created.isoformat() if created else "",
                "score": score,
                "body": md.get_text("\n", strip=True) if md else "",
                "permalink": BASE + c.get("data-permalink", ""),
            }
        )

    stubs = len(soup.select("div.commentarea div.thing.morechildren"))
    return selftext, comments, stubs, diag


# --------------------------------------------------------------------------- #
# Tasks
# --------------------------------------------------------------------------- #
BIG_THREAD = 400  # posts with at least this many comments get a second sort


def register_post(
    ctx: Ctx, p: dict, queue: asyncio.Queue, discussion_only=False
) -> bool:
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
    ctx.posts_queued += 1
    queue.put_nowait(p)
    return True


def comment_sorts(post: dict) -> list[str]:
    """Daily threads and huge threads exceed one page of comments: also take the top ones."""
    try:
        n = int(post.get("num_comments") or 0)
    except ValueError:
        n = 0
    if (
        post.get("stickied")
        or is_discussion_title(post.get("title", ""))
        or n >= BIG_THREAD
    ):
        return ["new", "top"]
    return ["new"]


def _check_listing(soup, posts, reached_cutoff, what, page):
    """Scraper health check: the page loaded fine, but did the selectors find anything?"""
    raw = len(soup.select("div#siteTable > div.thing.link"))
    if raw == 0:
        log.warning(
            "%s page=%d loaded but selector 'div#siteTable > div.thing.link' "
            "matched nothing",
            what,
            page,
        )
    elif not posts and not reached_cutoff:
        log.warning(
            "%s page=%d has %d post blocks but none could be parsed "
            "(data-timestamp missing?)",
            what,
            page,
            raw,
        )


def _check_search(soup, posts, reached_cutoff, what, page):
    raw = len(soup.select("div.search-result-link"))
    if raw == 0:
        log.warning(
            "%s page=%d loaded but selector 'div.search-result-link' found no "
            "results (normal for a quiet query, otherwise the selector may be "
            "stale)",
            what,
            page,
        )
    elif not posts and not reached_cutoff:
        log.warning(
            "%s page=%d has %d results but none could be parsed "
            "(a.search-title / time[datetime] changed?)",
            what,
            page,
            raw,
        )


async def produce_posts(ctx: Ctx, sub: str, queue: asyncio.Queue):
    """Crawl /r/<sub>/new/ until the cutoff; feed posts to the comment queue."""
    url, params = f"{BASE}/r/{sub}/new/", {"limit": 100}
    n, page, fetched = 0, 1, 0
    ctx.sub_status[sub] = "interrupted"

    try:
        while url and not halted(ctx):
            what, info = _req_label(url, params), {}
            soup = await fetch(ctx, url, params, info=info)
            if soup is None:
                if not halted(ctx):
                    log.error(
                        "listing r/%s/new page=%d failed, giving up on this listing",
                        sub,
                        page,
                    )
                return
            posts, reached_cutoff, nxt = parse_listing(soup, sub, ctx.cutoff)
            _check_listing(soup, posts, reached_cutoff, what, page)
            fetched += len(posts)

            for p in posts:
                if ctx.max_posts and n >= ctx.max_posts:
                    reached_cutoff = True
                    break
                if register_post(ctx, p, queue):
                    n += 1

            log.info(
                "listing ok r/%s/new page=%d posts=%d more=%s latency=%.2fs",
                sub,
                page,
                len(posts),
                "no" if reached_cutoff or not nxt else "yes",
                info["latency"],
            )
            _out.info(f"  [r/{sub}] page {page}: {n} posts so far")
            if reached_cutoff or not nxt:
                ctx.sub_status[sub] = "complete"
                return
            url, params = nxt, None
            page += 1
    finally:
        log.info(
            "listing done r/%s/new fetched=%d new=%d total=%d",
            sub,
            fetched,
            n,
            len(ctx.posts),
        )


async def produce_listing(
    ctx: Ctx,
    sub: str,
    kind: str,
    pages: int,
    queue: asyncio.Queue,
    discussion_only: bool,
):
    """
    Read /r/<sub>/hot or /rising. Unlike /new this keeps posts older than the
    cutoff (pinned daily threads, long-running discussions): their comments are
    filtered by their own timestamps later, so only fresh ones survive.
    """
    url, params = f"{BASE}/r/{sub}/{kind}/", {"limit": 100}
    n, page, fetched = 0, 1, 0
    try:
        while url and page <= pages and not halted(ctx):
            what, info = _req_label(url, params), {}
            soup = await fetch(ctx, url, params, info=info)
            if soup is None:
                if not halted(ctx):
                    log.error(
                        "listing r/%s/%s page=%d failed, giving up on this listing",
                        sub,
                        kind,
                        page,
                    )
                return
            posts, _, nxt = parse_listing(soup, sub, ctx.cutoff, keep_stale=True)
            _check_listing(soup, posts, False, what, page)
            fetched += len(posts)
            for p in posts:
                if ctx.max_posts and n >= ctx.max_posts:
                    nxt = None
                    break
                if register_post(ctx, p, queue, discussion_only):
                    n += 1
            log.info(
                "listing ok r/%s/%s page=%d posts=%d more=%s latency=%.2fs",
                sub,
                kind,
                page,
                len(posts),
                "yes" if nxt and page < pages else "no",
                info["latency"],
            )
            _out.info(f"  [r/{sub}/{kind}] page {page}: {n} new posts")
            if not nxt:
                return
            url, params = nxt, None
            page += 1
    finally:
        log.info(
            "listing done r/%s/%s fetched=%d new=%d total=%d",
            sub,
            kind,
            fetched,
            n,
            len(ctx.posts),
        )


def search_time_filter(cutoff: datetime) -> str:
    days = (datetime.now(timezone.utc) - cutoff).days + 1
    for limit, name in ((1, "day"), (7, "week"), (31, "month"), (365, "year")):
        if days <= limit:
            return name
    return "all"


async def produce_search(ctx: Ctx, query: str, queue: asyncio.Queue, pages: int):
    """Search the tracked subreddits for one ticker/company query, newest first."""
    url = f"{BASE}/r/{'+'.join(SUBREDDITS)}/search"
    params = {
        "q": query,
        "restrict_sr": "on",
        "sort": "new",
        "t": search_time_filter(ctx.cutoff),
        "limit": 100,
    }
    n, page, fetched = 0, 1, 0
    try:
        while url and page <= pages and not halted(ctx):
            # "#header" exists on every real old.reddit page, including zero-result
            # searches, so a ticker nobody mentions is not mistaken for a block
            what, info = _req_label(url, params), {}
            soup = await fetch(ctx, url, params, expect="#header", info=info)
            if soup is None:
                if not halted(ctx):
                    log.error(
                        "search q=%s page=%d failed, giving up on this query",
                        query,
                        page,
                    )
                return
            posts, reached_cutoff, nxt = parse_search(soup, ctx.cutoff)
            _check_search(soup, posts, reached_cutoff, what, page)
            fetched += len(posts)
            for p in posts:
                if ctx.max_posts and n >= ctx.max_posts:
                    reached_cutoff = True
                    break
                if register_post(ctx, p, queue):
                    n += 1
            log.info(
                "search ok q=%s page=%d posts=%d more=%s latency=%.2fs",
                query,
                page,
                len(posts),
                "no" if reached_cutoff or not nxt or page >= pages else "yes",
                info["latency"],
            )
            _out.info(f"  [search {query}] page {page}: {n} new posts")
            if reached_cutoff or not nxt:
                return
            url, params = nxt, None
            page += 1
    finally:
        log.info(
            "search done q=%s fetched=%d new=%d total=%d",
            query,
            fetched,
            n,
            len(ctx.posts),
        )


async def comment_worker(ctx: Ctx, queue: asyncio.Queue):
    """Pull posts off the queue, fetch their comment pages and score the thread."""
    while True:
        post = await queue.get()
        try:
            if halted(ctx):
                continue  # drain the queue so queue.join() can finish

            comments, seen_cids = [], set()
            first_soup = first_diag = None
            stubs = 0
            for sort in comment_sorts(post):
                info = {}
                soup = await fetch(
                    ctx, post["permalink"], {"limit": 500, "sort": sort}, info=info
                )
                if soup is None:
                    if halted(ctx):
                        break
                    log.error(
                        "thread page post=%s sort=%s failed, skipping it",
                        post["post_id"],
                        sort,
                    )
                    continue
                selftext, found, n_stubs, diag = parse_post_page(soup, post, ctx.cutoff)
                log.info(
                    "thread page ok post=%s sort=%s comments=%d more=%s latency=%.2fs",
                    post["post_id"],
                    sort,
                    len(found),
                    "yes" if n_stubs else "no",
                    info["latency"],
                )
                if first_soup is None:
                    first_soup, first_diag, stubs = soup, diag, n_stubs
                    post["selftext"] = selftext
                    post["comments_scraped"] = True
                for c in found:  # merge the sorts, no duplicates
                    if c["comment_id"] not in seen_cids:
                        seen_cids.add(c["comment_id"])
                        comments.append(c)
            if first_soup is None:
                continue

            ctx.comments_total += len(comments)
            ctx.posts_done += 1
            await score_thread(ctx, post, comments)  # one thread = one scoring batch
            post["selftext"] = ""  # text is not kept after scoring
            if stubs:
                _out.info(
                    f"    note: {stubs} 'load more comments' stub(s) not expanded "
                    f"({post['post_id']})"
                )

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
                    _out.info(
                        f"    warning: post {post['post_id']} reports {expected} "
                        f"comments but 0 were parsed {first_diag}"
                    )
                log.warning(
                    "thread page loaded but no comments parsed post=%s "
                    "reported_comments=%d diag=%s url=%s",
                    post["post_id"],
                    expected,
                    first_diag,
                    _scrub(post["permalink"]),
                )
                if ctx.empty_comment_posts >= 10 and ctx.parsed_comment_posts == 0:
                    trip(
                        ctx,
                        f"comment pages load, but no comments could be parsed from "
                        f"{ctx.empty_comment_posts} posts that have comments. The "
                        f"page layout probably differs from what the parser expects: "
                        f"see the 'no comments parsed' warnings in the log",
                    )

            if ctx.posts_done % 20 == 0:
                _out.info(
                    f"  progress: {ctx.posts_done}/{ctx.posts_queued} posts' comments "
                    f"fetched, {ctx.comments_total} comments total"
                )
        finally:
            queue.task_done()


# --------------------------------------------------------------------------- #
# Scoring + results
# --------------------------------------------------------------------------- #
def _score_texts_temp(texts):
    """Run score_texts on `texts` and return {text: score}.

    score_texts only reports results by writing a file, so the output goes to a
    temporary directory that is deleted as soon as the scores are read back
    (also on error).
    """
    with tempfile.TemporaryDirectory(prefix="reddit_scores_") as tmp:
        path = os.path.join(tmp, "scores.jsonl")
        score_texts(texts, path, resume=False)
        return load_scores(path)


def item_ref(item: dict) -> tuple[str, str]:
    """(ticker, item id) for one item produced by build_scoring_items().

    The id is Reddit's own fullname: t1_<comment id> for a comment, t3_<post id>
    for a post. ASSUMPTION (reddit_processing.py was not provided): every item
    dict has "ticker", "post_id" and, for comment items, a non-empty "comment_id".
    """
    ticker = item["ticker"]
    comment_id = item.get("comment_id")
    if comment_id:
        return ticker, "t1_" + str(comment_id).removeprefix("t1_")
    return ticker, "t3_" + str(item["post_id"]).removeprefix("t3_")


def _build_items(ctx: Ctx, post: dict, comments: list) -> list:
    items, _stats = build_scoring_items(
        [post], comments, ctx.matcher, cutoff=ctx.cutoff
    )
    return items


async def score_thread(ctx: Ctx, post: dict, comments: list) -> None:
    """Score one fetched thread (the post + its comments) as a single batch.

    Items are filtered / attributed to tickers, de-duplicated against everything
    already handled in this run, scored, and recorded in ctx.results. Never
    raises (a worker that died here would stall the queue); repeated failures
    trip the circuit breaker instead.
    """
    fetched = 1 + len(comments)
    new = []
    try:
        # One batch at a time: the first call loads the model, and the matcher /
        # model are not meant to be driven from several threads at once.
        async with ctx.score_lock:
            items = await asyncio.to_thread(_build_items, ctx, post, comments)
            for it in items:
                ticker, item_id = item_ref(it)
                if (ticker, item_id) in ctx.item_keys:
                    continue
                ctx.item_keys.add((ticker, item_id))
                # Same text already seen for this ticker (e.g. copy-pasted spam in
                # other threads): skip it. Only a short hash is kept, never the text.
                digest = hashlib.blake2b(
                    it["text"].encode("utf-8", "replace"), digest_size=8
                ).digest()
                if (ticker, digest) in ctx.text_seen:
                    continue
                ctx.text_seen.add((ticker, digest))
                # score_texts takes plain strings, so the text being answered
                # (parent comment / post title) is joined in front of the reply.
                context = (it.get("context") or "").strip()[:CONTEXT_CHARS]
                new.append(
                    (
                        ticker,
                        item_id,
                        f"{context} {it['text']}" if context else it["text"],
                    )
                )
            if new:
                start = time.monotonic()
                scores = await asyncio.to_thread(
                    _score_texts_temp, [text for _, _, text in new]
                )
                missing = 0
                for ticker, item_id, text in new:
                    score = scores.get(text)
                    if score is None:
                        missing += 1
                        continue
                    ctx.results.setdefault(ticker, {})[item_id] = score
                    ctx.scored_ids.add(item_id)
                if missing:
                    log.warning(
                        "scorer returned no score for %d of %d items", missing, len(new)
                    )
                log.info(
                    "scored batch items=%d elapsed=%.1fs",
                    len(new),
                    time.monotonic() - start,
                )
        ctx.scoring_failures = 0
    except Exception:
        ctx.scoring_failures += 1
        log.exception(
            "scoring failed for thread post=%s (%d unique items scored so far)",
            post["post_id"],
            len(ctx.scored_ids),
        )
        if ctx.scoring_failures >= 3:
            trip(
                ctx,
                "scoring failed for 3 threads in a row (see the traceback in the log)",
            )
        return
    log.info(
        "thread done post=%s fetched=%d new=%d total=%d",
        post["post_id"],
        fetched,
        len(new),
        len(ctx.scored_ids),
    )


def build_report(results, tickers):
    """List of {ticker, average_sentiment, post_count, posts} for every queried ticker.

    Tickers with no items are included with average_sentiment null.
    """
    report = []
    for ticker in sorted(tickers):
        items = results.get(ticker, {})
        average = round(sum(items.values()) / len(items), 2) if items else None
        report.append(
            {
                "ticker": ticker,
                "average_sentiment": average,
                "post_count": len(items),
                "posts": items,
            }
        )
    return report


def write_results(path, results, tickers, final=True):
    """Write the report as JSON. The file is replaced atomically, so a crash
    mid-write never leaves a half-written file. Never raises.
    final=False is a mid-run checkpoint (logged, but nothing printed)."""
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp")
    try:
        report = build_report(results, tickers)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
        log.info(
            "%s written path=%s tickers=%d items=%d",
            "results" if final else "checkpoint",
            path,
            len(report),
            sum(r["post_count"] for r in report),
        )
        if final:
            _out.info(f"Results saved to {path}")
    except Exception:
        log.exception("failed to write results to %s", path)
        _out.info(f"Failed to write results to {path} (see log).")
        try:
            tmp.unlink()
        except OSError:
            pass


def save_checkpoint(ctx: Ctx) -> None:
    """Refresh the results file mid-run, but only when there is something new
    (so an early checkpoint can never replace a good file with an empty one)."""
    n = sum(len(items) for items in ctx.results.values())
    if n == 0 or n == ctx.saved_count:
        return
    write_results(
        ctx.output_path, ctx.results, ctx.tickers | set(ctx.results), final=False
    )
    ctx.saved_count = n


async def checkpoint_loop(ctx: Ctx, every: float):
    while True:
        await asyncio.sleep(every)
        save_checkpoint(ctx)


def run_summary(ctx: Ctx, mode: str = "crawl") -> dict:
    return {
        "mode": mode,
        "stopped_early": ctx.stop.is_set(),
        "time_limit_reached": ctx.time_up.is_set(),
        "stop_reason": ctx.stop_reason or None,
        "total_requests": ctx.total_requests,
        "total_blocked_responses": ctx.total_blocks,
        "posts": len(ctx.posts),
        "posts_with_comments_scraped": sum(
            1 for p in ctx.posts if p["comments_scraped"]
        ),
        "comments": ctx.comments_total,
        "subreddit_listing_status": ctx.sub_status,
    }


# --------------------------------------------------------------------------- #
# Tickers
# --------------------------------------------------------------------------- #
async def load_ticker_map():
    """Return {company name: ticker} from fetch_ticker_map() (fetch_tickers.py).

    Raises RuntimeError if the ticker source is unavailable or has no entries.
    """
    try:
        companies = await fetch_ticker_map()
    except Exception as e:
        raise RuntimeError(f"ticker fetch failed: {type(e).__name__}: {e}") from e
    if not companies:
        raise RuntimeError(
            "the ticker file is missing, unreadable or has no usable entries"
        )
    log.info("tickers loaded source=file count=%d", len(set(companies.values())))
    return dict(companies)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
async def run(ctx: Ctx, args) -> None:
    ctx.sem = asyncio.Semaphore(args.concurrency)
    ctx.score_lock = asyncio.Lock()
    _out.info(f"Collecting items newer than {ctx.cutoff.isoformat()}")
    n_cookies = len(getattr(args, "cookies", None) or [])
    if n_cookies:
        _out.info(
            f"Using {n_cookies} reddit.com cookies "
            f"({'automatic login' if args.login else 'cookie file'}; values not shown)"
        )
    _out.info(
        f"backend={args.backend}  concurrency={args.concurrency}  "
        f"delay~{args.delay}s  stop after {args.max_blocks} consecutive "
        f"blocked responses (403/429/non-Reddit page)"
    )
    _out.info(
        f"mode={args.mode}"
        + (
            f"  search queries={len(args.search_queries)}"
            if args.search_queries
            else ""
        )
        + (f"  extra listings={','.join(args.listings)}" if args.listings else "")
        + "\n"
    )
    backend = make_backend(args)
    ctx.backend = backend
    queue: asyncio.Queue = asyncio.Queue()
    workers, ckpt, work, timer = [], None, None, None
    try:
        await backend.start()
        workers = [
            asyncio.create_task(comment_worker(ctx, queue))
            for _ in range(args.concurrency)
        ]
        ckpt = asyncio.create_task(checkpoint_loop(ctx, args.checkpoint_secs))
        timer = (
            asyncio.create_task(time_limit(ctx)) if ctx.deadline is not None else None
        )

        async def crawl():
            jobs = []
            if args.mode in ("crawl", "both"):
                jobs += [produce_posts(ctx, s, queue) for s in SUBREDDITS]
            if args.mode in ("search", "both"):
                jobs += [
                    produce_search(ctx, q, queue, args.search_pages)
                    for q in args.search_queries
                ]
            for s in SUBREDDITS:  # pinned daily threads, old-but-active posts
                for kind in args.listings:
                    pages = args.hot_pages if kind == "hot" else 1
                    jobs.append(
                        produce_listing(
                            ctx,
                            s,
                            kind,
                            pages,
                            queue,
                            discussion_only=(args.mode == "search"),
                        )
                    )
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
        # cancel everything still running; never wait on stragglers for long
        # (the results file is written by main(), in its own finally block)
        tasks = [t for t in workers + [ckpt, work, timer] if t is not None]
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=5)
        # close the backend with a hard time limit
        try:
            await asyncio.wait_for(backend.close(), timeout=5)
        except BaseException:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--days",
        type=float,
        default=7,
        help="only keep posts / comments from the last N days (default 7)",
    )
    ap.add_argument(
        "--time",
        type=float,
        default=None,
        help="stop making requests after this many minutes; threads already "
        "fetched are still scored and saved (default: no limit)",
    )
    ap.add_argument(
        "--max-posts",
        type=int,
        default=None,
        help="cap posts per subreddit (for testing)",
    )
    ap.add_argument(
        "--output",
        default=str(OUTPUT_FILE),
        help=f"JSON file for the sentiment results (default: {OUTPUT_FILE.name} "
        f"next to this script; overwritten each run)",
    )
    ap.add_argument(
        "--checkpoint-secs",
        type=float,
        default=60,
        help="refresh the results file this often while running (default 60)",
    )
    ap.add_argument(
        "--log-file",
        default=str(LOG_FILE),
        help=f"where to write the log (default: {LOG_FILE.name} next to this script)",
    )
    ap.add_argument("--backend", choices=["curl", "playwright"], default="curl")
    ap.add_argument(
        "--impersonate",
        default="chrome",
        help="curl_cffi browser to impersonate, e.g. chrome, chrome131, "
        "safari, edge (default: chrome)",
    )
    ap.add_argument(
        "--login",
        action="store_true",
        help="log in with Playwright (REDDIT_USERNAME / REDDIT_PASSWORD) "
        "instead of reading the cookie file",
    )
    ap.add_argument(
        "--headed",
        action="store_true",
        help="show the browser window, for --login and for "
        "--backend playwright (with --login a visible window also "
        "opens automatically if the login needs a captcha / 2FA)",
    )
    ap.add_argument(
        "--debug-login",
        action="store_true",
        help="with --login: if the login fails, save a screenshot to debug/ "
        "(may show your username)",
    )
    ap.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="max simultaneous requests/tabs (default 3)",
    )
    ap.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="avg delay per request slot in seconds (default 2.0)",
    )
    ap.add_argument(
        "--max-blocks",
        type=int,
        default=8,
        help="stop after this many consecutive blocked responses (default 8)",
    )
    ap.add_argument(
        "--proxy", default=None, help="proxy URL, e.g. http://user:pass@host:port"
    )
    ap.add_argument(
        "--aliases-file",
        default=None,
        help="optional JSON {alias: ticker or [tickers]}, e.g. "
        '{"the big tech": ["AAPL", "MSFT"]}',
    )
    ap.add_argument(
        "--ambiguous-file",
        default=None,
        help="optional JSON list of extra symbols/names that need a finance "
        "word nearby to count as a ticker mention",
    )
    ap.add_argument(
        "--mode",
        choices=["auto", "crawl", "search", "both"],
        default="auto",
        help="crawl = every subreddit's /new; search = one search per ticker; "
        "auto = search if the ticker list is small, else crawl (default)",
    )
    ap.add_argument(
        "--search-threshold",
        type=int,
        default=40,
        help="auto mode: search when there are at most this many tickers",
    )
    ap.add_argument(
        "--search-pages",
        type=int,
        default=2,
        help="result pages per search query (default 2)",
    )
    ap.add_argument(
        "--extra-listings",
        default="hot,rising",
        help="comma list of extra listings to read for pinned/old-but-active "
        "threads: hot, rising (default both; empty to disable)",
    )
    ap.add_argument(
        "--hot-pages",
        type=int,
        default=2,
        help="pages of /hot to read per subreddit (default 2)",
    )
    ap.add_argument(
        "--stale-min-comments",
        type=int,
        default=50,
        help="posts older than the window are only fetched if pinned, a "
        "daily thread, or at least this many comments (default 50)",
    )
    args = ap.parse_args()
    if args.days <= 0:
        ap.error("--days must be greater than 0")
    if args.time is not None and args.time <= 0:
        ap.error("--time must be greater than 0")
    # counted from process start (login included), so the run stays inside the limit
    deadline = time.monotonic() + args.time * 60 if args.time is not None else None

    args.listings = [x.strip() for x in args.extra_listings.split(",") if x.strip()]
    bad = set(args.listings) - {"hot", "rising"}
    if bad:
        die(f"ERROR: unknown --extra-listings value(s): {', '.join(sorted(bad))}")

    # The session must exist before we do anything slow (never printed or logged)
    setup_logging(args.log_file)
    if args.login:
        username, password = get_credentials()
    else:
        args.cookies = prepare_cookie_file(
            args
        )  # exits with a clear message if unusable
    if args.proxy:  # proxy URLs can carry user:pass and show up in error messages
        register_secrets(args.proxy, urlparse(args.proxy).password)

    # Load the tickers BEFORE scraping, so a bad path fails now, not after an hour
    try:
        companies = asyncio.run(load_ticker_map())
    except RuntimeError as e:
        die(f"ERROR: could not load tickers: {e}")
    try:
        matcher = TickerMatcher(
            companies,
            aliases=load_json_file(args.aliases_file, {}),
            extra_ambiguous=load_json_file(args.ambiguous_file, []),
        )
    except (OSError, ValueError) as e:
        die(f"ERROR: could not load tickers/aliases: {e}")
    if args.mode == "auto":
        args.mode = "search" if matcher.n_tickers <= args.search_threshold else "crawl"
        _out.info(
            f"auto mode -> {args.mode} ({matcher.n_tickers} tickers, search threshold "
            f"{args.search_threshold})"
        )
    args.search_queries = (
        matcher.search_queries() if args.mode in ("search", "both") else []
    )

    # --login: log in with Playwright. Cookies stay in memory and go to the backend.
    if args.login:
        try:
            args.cookies = login_to_reddit(
                username,
                password,
                headed=args.headed,
                proxy=args.proxy,
                debug=args.debug_login,
            )
        except LoginFailed as e:
            die(f"ERROR: Reddit login failed: {e}")
        except KeyboardInterrupt:
            die("Interrupted during login.")
        del password  # don't keep it around any longer than needed

    ctx = Ctx(
        stale_min_comments=args.stale_min_comments,
        cutoff=datetime.now(timezone.utc) - timedelta(days=args.days),
        delay=args.delay,
        max_blocks=args.max_blocks,
        max_posts=args.max_posts,
        matcher=matcher,
        deadline=deadline,
    )
    tickers = set(companies.values())
    ctx.tickers, ctx.output_path = tickers, args.output
    run_start = time.monotonic()
    log.info(
        "run start tickers=%s days=%g since=%s mode=%s backend=%s concurrency=%d "
        "delay=%.1fs max_posts=%s extra_listings=%s checkpoint=%ss",
        sorted(tickers),
        args.days,
        ctx.cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
        args.mode,
        args.backend,
        args.concurrency,
        args.delay,
        args.max_posts,
        ",".join(args.listings) or "-",
        args.checkpoint_secs,
    )
    try:
        asyncio.run(run(ctx, args))
    except KeyboardInterrupt:
        trip(ctx, "interrupted by user (Ctrl+C)", logging.WARNING)
    except Exception:
        log.exception("run aborted after %d unique items scored", len(ctx.scored_ids))
        raise
    finally:
        # Always save what was scored, also after Ctrl+C or a crash.
        write_results(args.output, ctx.results, tickers | set(ctx.results))

    s = run_summary(ctx, args.mode)
    log.info(
        "run finished unique_items=%d elapsed=%.1fs requests=%d blocked=%d",
        len(ctx.scored_ids),
        time.monotonic() - run_start,
        s["total_requests"],
        s["total_blocked_responses"],
    )
    _out.info(
        f"\nDone. {s['posts']} posts "
        f"({s['posts_with_comments_scraped']} with comments scraped), "
        f"{s['comments']} comments, {len(ctx.scored_ids)} unique items scored "
        f"-> {args.output}"
    )

    if s["stopped_early"]:
        incomplete = [
            k for k, v in s["subreddit_listing_status"].items() if v != "complete"
        ]
        _out.info(f"Stopped early: {s['stop_reason']}")
        if incomplete:
            _out.info(f"Subreddit listings not fully crawled: {', '.join(incomplete)}")
        sys.exit(2)
    if s["time_limit_reached"]:
        _out.info(
            "Time limit reached: stopped making requests; everything fetched was scored."
        )


if __name__ == "__main__":
    main()
