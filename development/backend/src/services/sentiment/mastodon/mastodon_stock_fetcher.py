"""
Fetch Mastodon posts (and their comments) from the last N days that mention
stock tickers, score the sentiment of each, and save the per-ticker sentiment.

Flow:
    1. load_tickers()           -> tickers to look for, from the shared ticker file
                                   (src/utils/fetch_tickers.py); a small fallback
                                   list is used if that file is unavailable
    2. Read the server list     -> Mastodon server domains (JSON file)
    3. For every server, query the hashtag timeline for each ticker (#AAPL, ...)
       in batches, AND crawl the broad finance "discovery" hashtags (#stocks,
       #investing, ...), then keep only posts that really mention a ticker and
       don't come from excluded authors (news feeds, bots, spam, ...).
    4. fetch_all_comments()     -> for every kept post, fetch its replies from the
                                   server the post came from (statuses/:id/context)
    5. score_batch()            -> per source server, that server's posts + their
                                   comments go through sentiment_scorer.score_items
                                   as one batch (temporary file, deleted right after)
    6. aggregate_batch()        -> combine post and comment sentiment, equal weight;
                                   the combined score is the post's item score
    7. write_results()          -> JSON results file (written once, at the end)

Results file (default: mastodon_sentiments.json next to this script, override
with --output; overwritten each run, also written if the run is interrupted or
fails part-way). Format: a list with one object per queried ticker:
    [{"ticker": "NVDA", "average_sentiment": 6.12, "post_count": 2,
      "posts": {"https://mastodon.social/users/x/statuses/1": 7.4, ...}}, ...]
Item ids are the post's canonical ActivityPub URI (globally unique; Mastodon's
numeric ids are local to each server). A post that mentions several tickers
appears under each of them. Tickers with no posts have average_sentiment null.

No post/comment text is written to disk: the scorer needs a file for its
output, so a temporary one is used and deleted as soon as the scores are read.

Logging: requests, retries, API errors, scoring batches and run summaries go to
a rotating log file (default: mastodon.log next to this script, override with
--log-file). Timestamps are UTC. Post text is never logged.

Usage (run from the backend/ directory, as a module, so `src.*` resolves):
    python -m src.services.sentiment.mastodon.mastodon_stock_fetcher
    python -m src.services.sentiment.mastodon.mastodon_stock_fetcher --workers 10
    python -m src.services.sentiment.mastodon.mastodon_stock_fetcher --output out.json --log-file run.log
    python -m src.services.sentiment.mastodon.mastodon_stock_fetcher --days 3 --time 30

--days N  only keeps posts from the last N days (default: DAYS).
--time T  stops making requests (timelines and comments) after T minutes; what
          was fetched by then is still scored and the results file is written.

Ticker file format (read by src/utils/fetch_tickers.py) -- company name: ticker:
    {"NVIDIA": "NVDA", "Apple": "AAPL", "Berkshire Hathaway": "BRK.B"}

How a post counts as mentioning a ticker:
    * cashtag  ($AAPL in the text)                     -> always counts
    * hashtag  (#AAPL on the post)                     -> counts, unless the ticker
      is ambiguous (<= AMBIGUOUS_MAX_LEN letters, or listed in AMBIGUOUS_TICKERS).
      Ambiguous tickers also need a finance clue (a finance hashtag like
      #investing or a keyword like "earnings"). This keeps tickers that are also
      normal words or other things (#ALL, #IT, #ON, #A, #NVDA the screen reader)
      from matching unrelated posts.
    * company name ("Nvidia" for NVDA, taken from the name in the ticker file)
      or bare uppercase symbol ("NVDA" in the text, unambiguous tickers only)
                                                       -> counts, but only when the
      post also has a finance clue (this is how discovery-tag posts are matched).

Comments:
    Comments are the replies to a kept post. They do NOT need to mention a
    ticker themselves (they are part of the conversation about the post).
    Bots and near-empty comments are dropped.

How sentiment is aggregated (see COMMENT_AGGREGATION):
    "post_vs_comments" (default): post score and the average comment score each
        count 50%. A post with no comments just uses its own score.
    "per_item": the post and every comment are one equal vote each.
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
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from urllib.parse import urlparse

# Make `src.*` importable no matter where/how this file is launched.
# File lives at backend/src/services/sentiment/mastodon/, so backend/ is parents[4].
BACKEND_DIR = Path(__file__).resolve().parents[4]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import requests

from src.utils.fetch_tickers import fetch_ticker_map

# ---- Settings (edit here) ----
SERVERS_FILE = "mastodon_servers.json"       # Mastodon server list JSON
OUTPUT_FILE = Path(__file__).with_name("mastodon_sentiments.json")  # results, overwritten each run
LOG_FILE = Path(__file__).with_name("mastodon.log")
LOG_MAX_BYTES = 5_000_000           # rotate at ~5 MB, keep 3 old files
LOG_BACKUPS = 3
BODY_SNIPPET = 200                  # max chars of an error response body to log

# Used when the ticker file is missing/unreadable. Same shape as the file:
# company name -> ticker.
FALLBACK_TICKERS = {
    "NVIDIA": "NVDA",
    "Apple": "AAPL",
    "Microsoft": "MSFT",
    "Amazon": "AMZN",
    "Alphabet": "GOOGL",
}

DAYS = 7                            # how many days back to fetch
LOCAL_ONLY = False                  # True = only posts authored on each server
OPEN_ONLY = False                   # True = only servers with open_registrations
MASTODON_ONLY = False               # True = only servers where software == "mastodon"

# ---- Post filters ----
EXCLUDE_BOTS = True                 # drop accounts flagged as bots (account.bot)
MIN_WORDS = 5                       # min words of real text (URLs, #tags, @mentions
                                    # excluded). Set to 0 to disable.
REQUIRE_CONTEXT_FOR_HASHTAG_MATCH = True  # hashtag-only ticker matches need a finance clue
                                    # ...but only for AMBIGUOUS tickers (see below);
                                    # other tickers match on the hashtag alone.
AMBIGUOUS_MAX_LEN = 3               # tickers with <= this many letters count as ambiguous
                                    # (#ALL, #IT, #ON, #A). Set to 0 to disable the rule.
AMBIGUOUS_TICKERS = {"NVDA"}        # extra tickers that always need a finance clue, e.g.
                                    # NVDA (screen reader), COST, NOW, CASH, PLAY, SNAP

# ---- Comment settings ----
FETCH_COMMENTS = True               # False = score posts only
DIRECT_REPLIES_ONLY = False         # True = only direct replies, False = whole thread
MAX_COMMENTS_PER_POST = 200         # cap per post (0 = no cap)
MIN_COMMENT_WORDS = 2               # min words of real text in a comment (0 = disable)
SKIP_CONTEXT_WHEN_NO_REPLIES = True # don't call the API for posts with replies_count == 0
                                    # (set False if you suspect stale counts on remote posts)

# ---- Discovery tags ----
# Besides the #<ticker> timelines, also crawl these broad finance hashtags and pick
# out posts that mention a ticker by cashtag ($NVDA), company name ("Nvidia") or an
# unambiguous bare symbol ("NVDA"). Set DISCOVERY_TAGS = [] to turn discovery off.
DISCOVERY_TAGS = ["stocks", "stockmarket", "investing", "trading",
                  "wallstreet", "nasdaq", "sp500", "earnings"]
DISCOVERY_MAX_PAGES = 50            # max pages (40 posts each) per request batch per server
MATCH_COMPANY_NAMES = True          # match "Nvidia" etc. (name from the ticker file)
MATCH_BARE_SYMBOLS = True           # match bare uppercase "NVDA" (non-ambiguous tickers only)

# ---- Author exclusion (we want opinions from people, not feeds/bots/spam) ----
EXCLUDE_AUTHORS = True              # master switch for everything in this block

# Whole instances that host news feeds / bots. Best-effort list: a domain that
# doesn't exist or never appears in your data does no harm. Extend it after
# reviewing the "excluded authors" and "prolific authors" lines in the log.
EXCLUDED_AUTHOR_DOMAINS = {
    "pubeurope.com",     # per-country news-feed accounts (seen in your results)
    "botsin.space",      # bot-hosting instances
    "mastodon.bot",
    "newsbots.eu",
}

# Exact accounts to exclude, lowercase "username@domain", e.g. {"somebot@example.social"}
EXCLUDED_AUTHORS = set()

# Words in the username or display name that mark feeds/bots/alert services.
# Matches whole tokens only ("stock_news", "Market Alerts"), so "newsom" is fine.
EXCLUDED_NAME_RE = re.compile(
    r"(?<![a-z0-9])(?:bots?|rss|feeds?|alerts?|news|headlines?|wire|updates?|digest|"
    r"tickers?|quotes?|signals?|screener|scanner|picks)(?![a-z0-9])"
)
# CamelCase endings in the username: "StockBot", "MarketNews", "TickerFeed"
EXCLUDED_CAMEL_RE = re.compile(r"[a-z0-9](?:Bot|News|Feed|Alerts?|Wire|Ticker)s?$")

# Phrases in the account bio that give away automation, aggregation or promo spam
EXCLUDED_BIO_RE = re.compile(
    r"\b(?:this is a bot|i(?:['’]m| am) a bot|bot account|automated (?:account|posts?|feed)|"
    r"posts? automatically|automatically posts?|auto-?posted|rss feed|news feed|"
    r"news aggregator|aggregator|not a real person|"
    r"join (?:my|our|the) (?:telegram|discord|whatsapp|channel)|dm (?:me )?for (?:signals|picks))\b",
    re.IGNORECASE,
)

# Accounts posting more than this many matching posts in the window are treated as
# automated/spammy and ALL their posts are dropped (0 = disable). Dropped accounts
# are listed in the log so you can review them.
MAX_POSTS_PER_AUTHOR = 25

# ---- Sentiment aggregation ----
COMMENT_AGGREGATION = "post_vs_comments"   # or "per_item"

# Finance clue 1: co-occurring hashtags (lowercase, no #)
FINANCE_TAGS = {
    "stock", "stocks", "stockmarket", "stockmarketnews", "investing",
    "investment", "investor", "investors", "trading", "trader", "daytrading",
    "swingtrading", "finance", "markets", "wallstreet", "nasdaq", "nyse",
    "sp500", "dowjones", "earnings", "dividend", "dividends", "etf", "ipo",
    "options", "equities", "bullmarket", "bearmarket", "shares", "fintech",
    "economy",
}

# Discovery tags are finance hashtags by definition, so they count as a finance clue.
DISCOVERY_TAGS = list(dict.fromkeys(
    re.sub(r"[^a-z0-9]", "", t.lower()) for t in DISCOVERY_TAGS if t))
FINANCE_TAGS |= set(DISCOVERY_TAGS)

# Finance clue 2: keywords/phrases in the post text or media alt text
FINANCE_KEYWORDS = [
    "stock market", "stocks", "share price", "earnings", "nasdaq", "nyse",
    "s&p", "s&p 500", "dow jones", "dividend", "portfolio", "investor",
    "investing", "ipo", "etf", "bull market", "bear market", "trading",
    "equity", "equities", "ticker", "market cap", "hedge fund", "wall street",
]

KEYWORD_RE = re.compile(
    r"(?<!\w)(?:" + "|".join(re.escape(k) for k in FINANCE_KEYWORDS) + r")(?!\w)",
    re.IGNORECASE,
)

# Cashtags like $AAPL, $TSLA, $BRK.B (letters only, so "$5" never matches)
CASHTAG_RE = re.compile(r"(?<![\w$])\$([A-Za-z]{1,5}(?:\.[A-Za-z])?)\b")

# Accepted ticker symbols: AAPL, BRK.B, BF-B ...
SYMBOL_RE = re.compile(r"^[A-Z0-9]{1,6}(?:[.\-][A-Z])?$")

# ---- Request settings ----
# Mastodon's hashtag timeline accepts extra tags via any[] but servers cap the
# total at ~4 by default, so we send up to 4 tickers per request.
# Set to 1 to send one ticker per request (slower, but no batching assumptions).
TICKERS_PER_REQUEST = 4
DEFAULT_WORKERS = 10
PAGE_SIZE = 40          # Mastodon's max for timelines
REQUEST_TIMEOUT = 15    # seconds
MAX_RETRIES = 3
USER_AGENT = "mastodon-ticker-fetcher/1.0 (research script)"
# Status codes on a tag-timeline request that mean "public tag timelines are
# disabled on this server" (login required) rather than a real error.
REFUSAL_STATUSES = (401, 403)

SENTIMENT_FIELDS = ("score", "bullish", "bearish", "neutral")
DROP_REASONS = ("bot", "excluded_author", "too_few_words",
                "ticker_needs_context", "no_ticker_match")

log = logging.getLogger("mastodon_stock_fetcher")

# --time support: run() sets the deadline (a time.monotonic() value). Once it has
# passed, no new requests are made; fetched posts are still scored and saved.
_deadline = None
_time_up_lock = threading.Lock()
_time_up_noted = False


def time_up():
    """True once the --time deadline has passed (logged and printed once)."""
    global _time_up_noted
    if _deadline is None or time.monotonic() < _deadline:
        return False
    with _time_up_lock:
        if not _time_up_noted:
            _time_up_noted = True
            log.info("time limit reached, no more requests; scoring what was fetched")
            print("Time limit reached: no more requests, scoring what was fetched...")
    return True


def polite_sleep(seconds):
    """time.sleep that never runs past the --time deadline."""
    if _deadline is not None:
        seconds = min(seconds, max(0.0, _deadline - time.monotonic()))
    if seconds > 0:
        time.sleep(seconds)


# ------------------------------------------------------------- logging ---
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


def _url_label(url):
    """'https://host/api/v1/timelines/tag/aapl?max_id=..' -> 'host/api/v1/timelines/tag/aapl'."""
    p = urlparse(url or "")
    return f"{p.netloc}{p.path}"


def _snippet(resp):
    """First BODY_SNIPPET chars of a response body, whitespace collapsed to one line."""
    try:
        return " ".join(resp.text[:BODY_SNIPPET].split())
    except Exception:  # never let logging break a request
        return "<unreadable body>"


# ------------------------------------------------------- data source I/O ---
def parse_tickers(data):
    """
    Return the tickers to search for as a list of dicts:
        {"symbol": "AAPL", "name": "Apple Inc." or None, "tag": "aapl"}
    `tag` is the hashtag form of the symbol (lowercase, no punctuation).

    `data` is the ticker data, normally {company name: ticker} as returned by
    fetch_ticker_map(). The older shapes {"tickers": [...]} and ["AAPL", ...]
    are still accepted here.
    """
    if isinstance(data, dict) and isinstance(data.get("tickers"), list):
        raw = data["tickers"]                    # older format: {"tickers": [...]}
    elif isinstance(data, dict):
        # Main format: {"NVIDIA": "NVDA"} -> company name: ticker
        raw = [{"symbol": sym, "name": name} for name, sym in data.items()]
    else:
        raw = data                               # older format: ["AAPL", ...]

    tickers, seen = [], set()
    for item in raw:
        if isinstance(item, str):
            symbol, name = item, None
        elif isinstance(item, dict):
            symbol, name = item.get("symbol") or item.get("ticker"), item.get("name")
        else:
            continue
        if not symbol or not isinstance(symbol, str):
            continue

        symbol = symbol.strip().upper().lstrip("$")
        if not SYMBOL_RE.match(symbol) or not any(c.isalpha() for c in symbol):
            log.warning("skipping invalid ticker %r", symbol)
            print(f"  ! skipping invalid ticker: {symbol!r}")
            continue
        if symbol in seen:
            continue
        seen.add(symbol)

        tickers.append(
            {"symbol": symbol, "name": name, "tag": re.sub(r"[^a-z0-9]", "", symbol.lower())}
        )
    return tickers


def load_tickers():
    """Tickers from the shared ticker file, or FALLBACK_TICKERS if it is unavailable."""
    tickers, source = [], "file"
    try:
        raw = asyncio.run(fetch_ticker_map())
        if raw:
            tickers = parse_tickers(raw)
        else:
            log.warning("ticker file missing, unreadable or empty; using fallback")
    except Exception as e:
        log.warning("ticker fetch failed, using fallback: %s: %s", type(e).__name__, e)
        print(f"Ticker fetch failed, using fallback: {e}")
    if not tickers:
        tickers = parse_tickers(FALLBACK_TICKERS)
        source = "fallback"
    log.info("tickers loaded source=%s count=%d tickers=%s",
             source, len(tickers), [t["symbol"] for t in tickers])
    return tickers


def load_domains(path, open_only=False, mastodon_only=False):
    """Return a list of Mastodon server domains from the JSON file."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    domains = []
    for s in data.get("servers", []):
        domain = s.get("domain")
        if not domain:
            continue
        if open_only and not s.get("open_registrations"):
            continue
        if mastodon_only and s.get("software", "").lower() != "mastodon":
            continue
        domains.append(domain.strip().lower())

    # de-duplicate, keep order
    return list(dict.fromkeys(domains))


# --------------------------------------------------------- results file ---
def build_report(results, tickers):
    """List of {ticker, average_sentiment, post_count, posts} for every queried ticker.

    Tickers with no posts are included with average_sentiment null.
    """
    report = []
    for ticker in sorted(tickers):
        posts = results.get(ticker, {})
        average = round(sum(posts.values()) / len(posts), 2) if posts else None
        report.append({
            "ticker": ticker,
            "average_sentiment": average,
            "post_count": len(posts),
            "posts": posts,
        })
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
        log.info("results written path=%s tickers=%d posts=%d",
                 path, len(report), sum(r["post_count"] for r in report))
        print(f"Results saved to {path}")
    except Exception:
        log.exception("failed to write results to %s", path)
        print(f"Failed to write results to {path} (see log).")
        try:
            tmp.unlink()
        except OSError:
            pass


# ------------------------------------------------------------- helpers ---
def parse_time(ts):
    """Parse Mastodon's ISO 8601 timestamp into an aware datetime."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def strip_html(html):
    text = re.sub(r"<br\s*/?>|</p>", "\n", html or "")
    text = re.sub(r"<[^>]+>", "", text)
    return unescape(text).strip()


def count_real_words(text):
    """Count words after removing URLs, #hashtags and @mentions."""
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"(?<!\w)[#@][\w.-]+", " ", text)
    return len(re.findall(r"\w+", text))


def finance_context(text, alt_text, tag_names):
    """Finance clues (finance hashtags / keywords) found in a post. Empty = none."""
    signals = []

    tag_hits = sorted(set(tag_names) & FINANCE_TAGS)
    if tag_hits:
        signals.append("tags:" + ",".join(tag_hits))

    keywords = sorted({m.lower() for m in KEYWORD_RE.findall(f"{text}\n{alt_text}")})
    if keywords:
        signals.append("keywords:" + ",".join(keywords))

    return signals


_LEGAL_SUFFIX_RE = re.compile(
    r"[,\s]+(?:inc|incorporated|corp|corporation|co|company|ltd|limited|plc|llc|"
    r"holdings?|group|sa|nv|ag|class [a-c])\.?$",
    re.IGNORECASE,
)


def clean_company_name(name):
    """'Apple Inc.' -> 'Apple', 'Meta Platforms, Inc.' -> 'Meta Platforms'."""
    if not name or not isinstance(name, str):
        return ""
    name, prev = name.strip(), None
    while prev != name:
        prev = name
        name = _LEGAL_SUFFIX_RE.sub("", name).strip(" ,.")
    return name


class TickerIndex:
    """Fast lookups for matching posts against the ticker list."""

    def __init__(self, tickers):
        self.symbols = {t["symbol"] for t in tickers}
        self.tag_to_symbol = {t["tag"]: t["symbol"] for t in tickers}

        # Company names -> symbol (for discovery posts like "Nvidia beat earnings")
        self.name_to_symbol = {}
        if MATCH_COMPANY_NAMES:
            for t in tickers:
                name = clean_company_name(t.get("name"))
                if len(name) >= 3:
                    self.name_to_symbol.setdefault(name.lower(), t["symbol"])
        self.name_re = None
        if self.name_to_symbol:
            names = sorted(self.name_to_symbol, key=len, reverse=True)
            self.name_re = re.compile(
                r"(?<!\w)(?:" + "|".join(re.escape(n) for n in names) + r")(?!\w)",
                re.IGNORECASE,
            )

        # Bare uppercase symbols ("NVDA"), only for tickers that aren't ambiguous
        self.bare_re = None
        bare = [t["symbol"] for t in tickers
                if MATCH_BARE_SYMBOLS and not is_ambiguous(t["symbol"])]
        if bare:
            bare.sort(key=len, reverse=True)
            self.bare_re = re.compile(
                r"(?<![\w$#@.])(?:" + "|".join(re.escape(s) for s in bare) + r")(?!\w)"
            )

    def text_hits(self, text):
        """Symbols mentioned by company name / bare symbol in `text`: (names, bare)."""
        names = ({self.name_to_symbol[m.lower()] for m in self.name_re.findall(text)}
                 if self.name_re else set())
        bare = set(self.bare_re.findall(text)) if self.bare_re else set()
        return names, bare


# ------------------------------------------------------ author exclusion ---
def author_check(account):
    """
    Decide whether an account should be excluded.
    Returns (handle, reason_or_None); handle is 'username@domain' (lowercase).
    Reasons: listed | domain | group | name | bio
    """
    host = (urlparse(account.get("url") or "").hostname or "").lower()
    username = account.get("username") or ""
    display = account.get("display_name") or ""
    handle = f"{username.lower()}@{host}"
    if not EXCLUDE_AUTHORS:
        return handle, None

    if handle in EXCLUDED_AUTHORS:
        return handle, "listed"
    if any(host == d or host.endswith("." + d) for d in EXCLUDED_AUTHOR_DOMAINS):
        return handle, "domain"
    if account.get("group"):
        return handle, "group"
    if (EXCLUDED_NAME_RE.search(f"{username} {display}".lower())
            or EXCLUDED_CAMEL_RE.search(username)):
        return handle, "name"
    note = strip_html(account.get("note"))
    if note and EXCLUDED_BIO_RE.search(note):
        return handle, "bio"
    return handle, None


def note_author_exclusion(stats, handle, reason):
    """Record an exclusion so the run summary can show why and who."""
    stats[f"author_reason:{reason}"] += 1
    stats[f"author_handle:{handle}"] += 1


def summarize_author_exclusions(stats, top=100):
    """Excluded-author counts by reason and the most-excluded handles."""
    reasons = {k.split(":", 1)[1]: v for k, v in stats.items()
               if k.startswith("author_reason:")}
    handles = sorted(((k.split(":", 1)[1], v) for k, v in stats.items()
                      if k.startswith("author_handle:")), key=lambda kv: -kv[1])[:top]
    return {"by_reason": reasons, "top_handles": dict(handles)}


def drop_prolific_authors(posts):
    """
    Drop every post from authors with more than MAX_POSTS_PER_AUTHOR posts in
    this run (almost always feeds, bots or spam). Returns (kept, {handle: count}).
    """
    if not (EXCLUDE_AUTHORS and MAX_POSTS_PER_AUTHOR):
        return posts, {}
    counts = Counter(p["author_handle"] for p in posts)
    heavy = {h: c for h, c in counts.most_common() if c > MAX_POSTS_PER_AUTHOR}
    return [p for p in posts if p["author_handle"] not in heavy], heavy


def is_ambiguous(symbol):
    """True if a hashtag-only match on this ticker needs a finance clue."""
    letters = re.sub(r"[^A-Z]", "", symbol)
    return len(letters) <= AMBIGUOUS_MAX_LEN or symbol in AMBIGUOUS_TICKERS


def evaluate_status(status, index, stats):
    """
    Decide whether to keep a post.
    Returns (record_or_None, drop_reason_or_None).
    """
    account = status.get("account", {})
    text = strip_html(status.get("content"))
    media = status.get("media_attachments", []) or []
    alt_texts = [m.get("description") for m in media if m.get("description")]
    alt_joined = " ".join(alt_texts)
    tag_names = [t["name"].lower() for t in status.get("tags", [])]

    # 1. Bots
    if EXCLUDE_BOTS and account.get("bot"):
        return None, "bot"

    # 1b. Excluded authors (news feeds, bots, spam, ...)
    author_handle, author_reason = author_check(account)
    if author_reason:
        note_author_exclusion(stats, author_handle, author_reason)
        return None, "excluded_author"

    # 2. Too little real text (image-only / link-only / hashtag-only posts)
    word_count = count_real_words(text)
    if MIN_WORDS and word_count < MIN_WORDS:
        return None, "too_few_words"

    # 3. Ticker match
    cashtag_hits = {m.upper() for m in CASHTAG_RE.findall(f"{text}\n{alt_joined}")} & index.symbols
    hashtag_hits = {index.tag_to_symbol[t] for t in tag_names if t in index.tag_to_symbol}
    context = finance_context(text, alt_joined, tag_names)

    matches = {}
    if context:  # name / bare-symbol matches are only trusted with a finance clue
        name_hits, bare_hits = index.text_hits(f"{text}\n{alt_joined}")
        for symbol in name_hits:
            matches[symbol] = "name"
        for symbol in bare_hits:
            matches[symbol] = "symbol"
    for symbol in hashtag_hits:
        if (context or not REQUIRE_CONTEXT_FOR_HASHTAG_MATCH
                or not is_ambiguous(symbol)):
            matches[symbol] = "hashtag"
    for symbol in cashtag_hits:  # cashtag is the stronger match, so it wins
        matches[symbol] = "cashtag"

    if not matches:
        return None, ("ticker_needs_context" if hashtag_hits else "no_ticker_match")

    record = {
        "id": status.get("id"),
        "uri": status.get("uri"),
        "url": status.get("url"),
        "created_at": status.get("created_at"),
        "author": account.get("acct"),
        "author_handle": author_handle,
        "author_url": account.get("url"),
        "author_is_bot": bool(account.get("bot")),
        "content": text,
        "word_count": word_count,
        "language": status.get("language"),
        "tags": tag_names,
        "matched_tickers": sorted(matches),
        "ticker_matches": dict(sorted(matches.items())),
        "finance_context": context,
        "media_count": len(media),
        "media_types": [m.get("type") for m in media],
        "media_alt_texts": alt_texts,
        "image_only": bool(media) and word_count == 0,
        "replies": status.get("replies_count"),
        "reblogs": status.get("reblogs_count"),
        "favourites": status.get("favourites_count"),
    }
    return record, None


def get_with_retry(session, url, params=None, tag_timeline=False, failure=None):
    """GET with simple retry/backoff and 429 handling. Returns Response or None.

    Every failed attempt is logged as a WARNING with its attempt number; giving
    up (rejected request or retries exhausted) is logged as an ERROR.

    tag_timeline=True marks a hashtag-timeline request: a 401/403 there means
    the server refuses public (logged-out) tag timelines, and is logged as such.
    If `failure` is a dict, the final status code of a rejected request is
    stored in failure["status"] and failure["refused"] is set for refusals.
    """
    label = _url_label(url)
    for attempt in range(1, MAX_RETRIES + 1):
        if time_up():
            if failure is not None:
                failure["time_up"] = True
            return None
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            log.warning("GET %s network error attempt=%d/%d: %s: %s",
                        label, attempt, MAX_RETRIES, type(e).__name__, e)
            if attempt == MAX_RETRIES:
                log.error("GET %s failed after %d attempts", label, MAX_RETRIES)
                print(f"  ! {url}: {e}")
                return None
            polite_sleep(2 ** attempt)
            continue

        if resp.status_code == 200:
            if attempt > 1:
                log.info("GET %s succeeded after %d attempts", label, attempt)
            return resp

        if resp.status_code == 429:
            # Respect rate limit reset if provided
            wait = 5
            reset = resp.headers.get("X-RateLimit-Reset")
            if reset:
                try:
                    delta = (parse_time(reset) - datetime.now(timezone.utc)).total_seconds()
                    wait = min(max(delta, 1), 60)
                except ValueError:
                    pass
            log.warning("GET %s rate limited status=429 sleeping=%.0fs attempt=%d/%d body=%s",
                        label, wait, attempt, MAX_RETRIES, _snippet(resp))
            polite_sleep(wait)
            continue

        if resp.status_code >= 500 and attempt < MAX_RETRIES:
            log.warning("GET %s server error status=%s attempt=%d/%d body=%s",
                        label, resp.status_code, attempt, MAX_RETRIES, _snippet(resp))
            polite_sleep(2 ** attempt)
            continue

        if resp.status_code >= 500:
            log.warning("GET %s server error status=%s attempt=%d/%d body=%s",
                        label, resp.status_code, attempt, MAX_RETRIES, _snippet(resp))
            log.error("GET %s failed after %d attempts status=%s",
                      label, MAX_RETRIES, resp.status_code)
        elif tag_timeline and resp.status_code in REFUSAL_STATUSES:
            # Server requires login for hashtag timelines (Mastodon's
            # "timeline_preview" setting is off). Expected policy, not a fault.
            log.warning("GET %s server refused public tag timelines status=%s body=%s",
                        label, resp.status_code, _snippet(resp))
            if failure is not None:
                failure["refused"] = True
        else:
            # 404/410/422 etc
            log.error("GET %s request rejected status=%s body=%s",
                      label, resp.status_code, _snippet(resp))
        if failure is not None:
            failure["status"] = resp.status_code
        print(f"  ! {url}: HTTP {resp.status_code}")
        return None

    log.error("GET %s failed after %d attempts (rate limited)", label, MAX_RETRIES)
    return None


def item_key(record, domain=None):
    """Stable, globally unique key for a post/comment (ids are server-local, URIs are not)."""
    return (record.get("uri") or record.get("url")
            or f"{domain or record.get('source_server')}:{record.get('id')}")


# --------------------------------------------------------------- fetch ---
def fetch_batch(session, domain, batch, index, cutoff, local_only,
                seen_uris, posts, stats, max_pages=500):
    """
    Page through the hashtag timeline for one batch of tickers on one server.
    Appends kept posts to `posts`. Returns True if the first request succeeded.
    """
    primary = batch[0]["tag"]
    extras = [t["tag"] for t in batch[1:]]
    tags_label = ",".join([primary] + extras)

    url = f"https://{domain}/api/v1/timelines/tag/{primary}"
    params = {"limit": PAGE_SIZE}
    if extras:
        params["any[]"] = extras
    if local_only:
        params["local"] = "true"

    pages = 0
    first_ok = False
    fetched = 0
    kept_before = len(posts)

    while url and pages < max_pages and not time_up():
        start = time.monotonic()
        failure = {}
        resp = get_with_retry(session, url, params, tag_timeline=True, failure=failure)
        elapsed = time.monotonic() - start
        if resp is None:
            if failure.get("refused"):
                stats["tag_timeline_refused"] = failure["status"]
            break
        pages += 1
        first_ok = True

        try:
            page = resp.json()
        except ValueError:
            log.warning("timeline bad JSON server=%s tags=%s page=%d body=%s",
                        domain, tags_label, pages, _snippet(resp))
            break
        if not isinstance(page, list):
            log.warning("timeline unexpected response server=%s tags=%s page=%d type=%s body=%s",
                        domain, tags_label, pages, type(page).__name__, _snippet(resp))
            break
        if not page:
            log.info("timeline ok server=%s tags=%s page=%d posts=0 kept=0 more=no elapsed=%.2fs",
                     domain, tags_label, pages, elapsed)
            break
        fetched += len(page)
        kept_on_page = len(posts)

        reached_cutoff = False
        for status in page:
            created = parse_time(status["created_at"])
            if created < cutoff:
                reached_cutoff = True
                continue  # skip but finish scanning this page

            key = status.get("uri") or status.get("url")
            if key in seen_uris:  # already seen via another ticker batch
                continue
            seen_uris.add(key)

            stats["seen"] += 1
            record, reason = evaluate_status(status, index, stats)
            if record is None:
                stats[reason] += 1
                continue
            record["source_server"] = domain
            posts.append(record)

        # Follow pagination via the Link header (rel="next")
        next_link = resp.links.get("next", {}).get("url")
        more = bool(next_link) and not reached_cutoff and pages < max_pages
        log.info("timeline ok server=%s tags=%s page=%d posts=%d kept=%d more=%s elapsed=%.2fs",
                 domain, tags_label, pages, len(page), len(posts) - kept_on_page,
                 "yes" if more else "no", elapsed)

        # Timeline is newest-first, so once we see a post older than the cutoff
        # everything after it is older too.
        if reached_cutoff:
            break

        url, params = next_link, None  # next URL already contains its params

        polite_sleep(0.3)  # be polite

    log.info("tags done server=%s tags=%s pages=%d fetched=%d new=%d total=%d",
             domain, tags_label, pages, fetched, len(posts) - kept_before, len(posts))
    return first_ok


def fetch_server(domain, tickers, index, cutoff, local_only=False):
    """
    Fetch ticker posts from one server. Servers are processed one per worker
    so each server only ever sees sequential requests from us.
    Returns (domain, kept_posts, stats_counter).
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

    posts, seen_uris, stats = [], set(), Counter()
    any_ok = False

    # Jobs: ticker hashtag batches first, then the broad discovery hashtag batches
    # (capped at DISCOVERY_MAX_PAGES pages since those timelines can be huge).
    n = TICKERS_PER_REQUEST
    jobs = [(tickers[i:i + n], 500) for i in range(0, len(tickers), n)]
    jobs += [([{"tag": t} for t in DISCOVERY_TAGS[i:i + n]], DISCOVERY_MAX_PAGES)
             for i in range(0, len(DISCOVERY_TAGS), n)]

    for batch, max_pages in jobs:
        if time_up():
            break
        ok = fetch_batch(session, domain, batch, index, cutoff, local_only,
                         seen_uris, posts, stats, max_pages=max_pages)
        any_ok = any_ok or ok
        if time_up():
            break   # the time limit, not the server, ended this batch
        if not ok and not any_ok:
            # Nothing has worked on this server yet (down, or tag timelines
            # require login) -- don't hammer it with every remaining batch.
            stats["server_unavailable"] = 1
            break

    return domain, posts, stats


# ------------------------------------------------------------ comments ---
def evaluate_comment(status, texts, post, stats):
    """
    Decide whether to keep a comment (a reply to `post`).
    `texts` maps status id -> text for the whole thread, used to find the text
    of the comment's parent (the sentiment model reads it as context).
    Returns (record_or_None, drop_reason_or_None).
    """
    account = status.get("account", {})

    if EXCLUDE_BOTS and account.get("bot"):
        return None, "comment_bot"

    author_handle, author_reason = author_check(account)
    if author_reason:
        note_author_exclusion(stats, author_handle, author_reason)
        return None, "comment_excluded_author"

    text = strip_html(status.get("content"))
    word_count = count_real_words(text)
    if word_count == 0 or (MIN_COMMENT_WORDS and word_count < MIN_COMMENT_WORDS):
        return None, "comment_too_few_words"

    parent_id = status.get("in_reply_to_id")
    record = {
        "id": status.get("id"),
        "uri": status.get("uri"),
        "url": status.get("url"),
        "created_at": status.get("created_at"),
        "author": account.get("acct"),
        "in_reply_to_id": parent_id,
        "text": text,
        # Parent text (the post, or the comment being replied to) as model context
        "context": texts.get(parent_id) or post["content"],
    }
    return record, None


def fetch_comments_for_post(session, domain, post, stats):
    """
    Fetch the replies to one post from the server the post came from.
    Returns a list of comment records, or None if the request failed.
    """
    url = f"https://{domain}/api/v1/statuses/{post['id']}/context"
    start = time.monotonic()
    resp = get_with_retry(session, url)
    elapsed = time.monotonic() - start
    if resp is None:
        return None
    try:
        descendants = (resp.json() or {}).get("descendants") or []
    except ValueError:
        log.warning("context bad JSON server=%s post_id=%s body=%s",
                    domain, post["id"], _snippet(resp))
        return None

    texts = {post["id"]: post["content"]}
    for d in descendants:
        texts[d.get("id")] = strip_html(d.get("content"))

    comments = []
    for d in descendants:
        stats["comments_found"] += 1
        if DIRECT_REPLIES_ONLY and d.get("in_reply_to_id") != post["id"]:
            stats["comment_not_direct"] += 1
            continue
        record, reason = evaluate_comment(d, texts, post, stats)
        if record is None:
            stats[reason] += 1
            continue
        record["source_server"] = domain
        comments.append(record)
        if MAX_COMMENTS_PER_POST and len(comments) >= MAX_COMMENTS_PER_POST:
            break
    log.info("context ok server=%s post_id=%s replies=%d kept=%d elapsed=%.2fs",
             domain, post["id"], len(descendants), len(comments), elapsed)
    return comments


def fetch_comments_for_server(domain, posts):
    """
    Fetch comments for all posts that came from one server (sequential, so the
    server only ever sees one request at a time from us).
    Returns (domain, {post_key: [comments]}, stats_counter).
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

    result, stats = {}, Counter()
    queried = 0
    for post in posts:
        if time_up():
            break   # posts left without comments are scored on their own
        if SKIP_CONTEXT_WHEN_NO_REPLIES and not post.get("replies"):
            result[item_key(post)] = []
            continue
        queried += 1
        comments = fetch_comments_for_post(session, domain, post, stats)
        if comments is None:
            stats["context_failed"] += 1
            result[item_key(post)] = []
        else:
            result[item_key(post)] = comments
        polite_sleep(0.3)  # be polite
    log.info("comments done server=%s posts=%d requests=%d failed=%d kept=%d",
             domain, len(posts), queried, stats["context_failed"],
             sum(len(v) for v in result.values()))
    return domain, result, stats


def fetch_all_comments(posts, workers):
    """
    Fetch comments for every unique post, grouped by source server.
    Returns ({post_key: [comments]}, stats_counter).
    """
    by_server = defaultdict(list)
    for p in posts:
        by_server[p["source_server"]].append(p)

    comments_by_post, total = {}, Counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_comments_for_server, d, ps): d
                   for d, ps in by_server.items()}
        for fut in as_completed(futures):
            domain = futures[fut]
            try:
                _, result, stats = fut.result()
            except Exception as e:  # never let one server kill the run
                log.exception("comment fetch crashed server=%s", domain)
                print(f"[{domain}] comment fetch failed: {e}")
                continue
            comments_by_post.update(result)
            total.update(stats)
            print(f"[{domain}] comments kept: {sum(len(v) for v in result.values())}")
    return comments_by_post, total


# ----------------------------------------------------------- sentiment ---
def build_score_items(posts, comments_by_post):
    """
    Build the list of dicts sentiment_scorer.score_items() expects:
        {"id": <unique key>, "text": <text to score>, "context": <optional>}
    Posts and comments get different id prefixes, so a post that is also a
    reply inside another post's thread is scored separately in each role.
    """
    items = []
    for p in posts:
        items.append({"id": f"post::{item_key(p)}", "text": p["content"], "context": ""})
    for comments in comments_by_post.values():
        for c in comments:
            items.append({"id": f"comment::{item_key(c)}",
                          "text": c["text"], "context": c["context"]})
    return items


def _score_items_temp(items):
    """Run score_items on `items` and return {id: record}.

    score_items only reports results by writing a file, so the output goes to a
    temporary directory that is deleted as soon as the scores are read back
    (also on error). Imported here so fetching doesn't need torch/transformers.
    """
    from src.services.sentiment.sentiment_scorer import load_score_records, score_items

    with tempfile.TemporaryDirectory(prefix="mastodon_scores_") as tmp:
        path = os.path.join(tmp, "scores.jsonl")
        score_items(items, path, resume=False)
        return load_score_records(path)


def score_batch(domain, posts, comments_by_post):
    """
    Score one batch (one source server's posts + their comments) and return
    {id: {"id", "score", "bullish", "bearish", "neutral"}}.
    """
    items = build_score_items(posts, comments_by_post)
    if not items:
        return {}
    start = time.monotonic()
    try:
        records = _score_items_temp(items)
    except Exception:
        log.exception("scoring failed server=%s items=%d", domain, len(items))
        raise
    log.info("scored batch server=%s items=%d posts=%d comments=%d elapsed=%.1fs",
             domain, len(items), len(posts), len(items) - len(posts),
             time.monotonic() - start)
    return records


def _pick(record):
    """Just the sentiment numbers from a scorer record."""
    return {f: record[f] for f in SENTIMENT_FIELDS}


def _mean(sentiments):
    n = len(sentiments)
    return {f: round(sum(s[f] for s in sentiments) / n, 4) for f in SENTIMENT_FIELDS}


def combine_sentiment(post_sent, comment_sents):
    """
    Combine a post's sentiment with its comments' sentiment, equal weight.
    Returns (comment_average_or_None, combined).
    """
    if not comment_sents:
        return None, dict(post_sent)
    comment_avg = _mean(comment_sents)
    if COMMENT_AGGREGATION == "per_item":
        return comment_avg, _mean([post_sent] + comment_sents)
    # "post_vs_comments": post = 50%, average of comments = 50%
    return comment_avg, _mean([post_sent, comment_avg])


def aggregate_batch(posts, comments_by_post, records, results):
    """
    Combine each post's score with its comments' scores and record the result
    under every ticker the post matched: results[ticker][post_key] = score.
    `results` is updated in place. Returns (posts_scored, comments_scored, missing).
    """
    posts_scored = comments_scored = missing = 0
    for p in posts:
        pkey = item_key(p)
        post_rec = records.get(f"post::{pkey}")
        if post_rec is None:
            missing += 1
            continue
        post_sent = _pick(post_rec)

        comment_sents = []
        for c in comments_by_post.get(pkey, []):
            c_rec = records.get(f"comment::{item_key(c)}")
            if c_rec is not None:
                comment_sents.append(_pick(c_rec))

        _, combined = combine_sentiment(post_sent, comment_sents)
        score = round(combined["score"], 2)
        for sym in p["matched_tickers"]:
            results.setdefault(sym, {})[pkey] = score
        posts_scored += 1
        comments_scored += len(comment_sents)
    return posts_scored, comments_scored, missing


# ---------------------------------------------------------------- main ---
def _pipeline(workers, tickers, index, cutoff, domains, results, progress):
    """Fetch, filter, fetch comments, score. Fills `results` and `progress` in
    place. Returns True if it ran to the end, False if there was nothing to score."""
    fetched = {}
    total_stats = Counter()
    refused_servers = {}    # {domain: status} for servers refusing public tag timelines
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(fetch_server, d, tickers, index, cutoff, LOCAL_ONLY): d
            for d in domains
        }
        for fut in as_completed(futures):
            domain = futures[fut]
            try:
                _, posts, stats = fut.result()
            except Exception as e:  # never let one server kill the run
                log.exception("server fetch crashed server=%s", domain)
                print(f"[{domain}] failed: {e}")
                continue
            fetched[domain] = posts
            refused_status = stats.pop("tag_timeline_refused", None)
            total_stats.update(stats)
            if stats["server_unavailable"] and refused_status:
                refused_servers[domain] = refused_status
                log.warning("server refused public tag timelines, skipped server=%s status=%s",
                            domain, refused_status)
                print(f"[{domain}] unavailable, skipped")
            elif stats["server_unavailable"]:
                log.warning("server unavailable, skipped server=%s", domain)
                print(f"[{domain}] unavailable, skipped")
            else:
                dropped = ", ".join(f"{r}={stats[r]}" for r in DROP_REASONS if stats[r])
                log.info("server done server=%s seen=%d kept=%d dropped=%s",
                         domain, stats["seen"], len(posts), dropped or "none")
                print(f"[{domain}] kept {len(posts)} of {stats['seen']} posts"
                      + (f" (dropped: {dropped})" if dropped else ""))

    # Federated servers often see the same post; de-duplicate by canonical URI.
    seen, unique = set(), []
    kept_total = 0
    for posts in fetched.values():
        kept_total += len(posts)
        for p in posts:
            key = p.get("uri") or p.get("url")
            if key and key in seen:
                continue
            seen.add(key)
            unique.append(p)

    unique.sort(key=lambda p: p["created_at"], reverse=True)
    print(f"\nSeen {total_stats['seen']} posts; dropped "
          + ", ".join(f"{r}={total_stats[r]}" for r in DROP_REASONS))
    print(f"{len(unique)} unique posts kept after de-duplication.")
    log.info("fetch done seen=%d kept=%d unique=%d dropped=%s servers_unavailable=%d "
             "servers_refused_tag_timelines=%d",
             total_stats["seen"], kept_total, len(unique),
             ",".join(f"{r}={total_stats[r]}" for r in DROP_REASONS),
             total_stats["server_unavailable"], len(refused_servers))
    if refused_servers:
        log.info("servers that refused public tag timelines: %s", sorted(refused_servers))

    excluded = summarize_author_exclusions(total_stats)
    if excluded["by_reason"]:
        print("Excluded authors by reason: "
              + ", ".join(f"{r}={n}" for r, n in excluded["by_reason"].items()))
        log.info("excluded authors (posts) by_reason=%s", excluded["by_reason"])

    unique, prolific = drop_prolific_authors(unique)
    if prolific:
        print(f"Dropped posts from {len(prolific)} prolific authors "
              f"(> {MAX_POSTS_PER_AUTHOR} posts each): "
              + ", ".join(f"{h} ({c})" for h, c in list(prolific.items())[:5])
              + (" ..." if len(prolific) > 5 else ""))
        print(f"{len(unique)} posts remain.")
        log.info("prolific authors dropped count=%d limit=%d authors=%s remaining_posts=%d",
                 len(prolific), MAX_POSTS_PER_AUTHOR, prolific, len(unique))

    if not unique:
        print("Nothing to score.")
        log.info("nothing to score")
        return False

    # ---- comments ----
    comments_by_post, comment_stats = {}, Counter()
    if FETCH_COMMENTS:
        print("Fetching comments...")
        comments_by_post, comment_stats = fetch_all_comments(unique, workers)
        comment_excluded = summarize_author_exclusions(comment_stats)
        log.info("comments fetched found=%d kept=%d dropped bot=%d too_few_words=%d "
                 "not_direct_reply=%d excluded_author=%d context_failed=%d",
                 comment_stats["comments_found"],
                 sum(len(v) for v in comments_by_post.values()),
                 comment_stats["comment_bot"], comment_stats["comment_too_few_words"],
                 comment_stats["comment_not_direct"],
                 comment_stats["comment_excluded_author"],
                 comment_stats["context_failed"])
        if comment_excluded["by_reason"]:
            log.info("excluded authors (comments) by_reason=%s", comment_excluded["by_reason"])
    total_comments = sum(len(v) for v in comments_by_post.values())

    # ---- sentiment: one batch per source server ----
    print(f"Scoring {len(unique) + total_comments} items ({len(unique)} posts + "
          f"{total_comments} comments)...")
    by_server = defaultdict(list)
    for p in unique:
        by_server[p["source_server"]].append(p)

    missing = 0
    for domain, server_posts in by_server.items():
        keys = [item_key(p) for p in server_posts]
        server_comments = {k: comments_by_post[k] for k in keys if k in comments_by_post}
        records = score_batch(domain, server_posts, server_comments)
        n_posts, n_comments, n_missing = aggregate_batch(
            server_posts, server_comments, records, results)
        progress["posts_scored"] += n_posts
        progress["comments_scored"] += n_comments
        missing += n_missing

    if missing:
        log.warning("%d posts had no score record and were skipped", missing)
        print(f"  ! {missing} posts had no score record and were skipped")
    return True


def run(workers, output_path, days=DAYS, time_limit_minutes=None, deadline=None):
    """deadline (a time.monotonic() value) wins over time_limit_minutes if both are given."""
    global _deadline
    run_start = time.monotonic()
    if deadline is None and time_limit_minutes is not None:
        deadline = run_start + time_limit_minutes * 60
    _deadline = deadline

    tickers = load_tickers()
    if not tickers:
        log.error("no tickers loaded; nothing to do")
        print("No tickers loaded; nothing to do.")
        return
    index = TickerIndex(tickers)
    symbols = [t["symbol"] for t in tickers]

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        domains = load_domains(SERVERS_FILE, OPEN_ONLY, MASTODON_ONLY)
    except (OSError, ValueError) as e:
        log.error("cannot read server list %s: %s: %s", SERVERS_FILE, type(e).__name__, e)
        raise
    if not domains:
        log.warning("server list %s has no usable servers", SERVERS_FILE)
    print(f"Loaded {len(tickers)} tickers and {len(domains)} servers; "
          f"fetching posts since {cutoff.isoformat()}")

    log.info("run start tickers=%s since=%s days=%g servers=%d workers=%d local_only=%s "
             "fetch_comments=%s discovery_tags=%s time_limit_min=%s",
             sorted(symbols), cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"), days, len(domains),
             workers, LOCAL_ONLY, FETCH_COMMENTS, DISCOVERY_TAGS,
             None if deadline is None else round((deadline - run_start) / 60, 2))

    results = {}            # {ticker: {post_key: score}}, written once at the end
    progress = Counter()    # posts_scored, comments_scored
    completed = False
    try:
        completed = _pipeline(workers, tickers, index, cutoff, domains, results, progress)
    except Exception:
        log.exception("run aborted after %d posts scored", progress["posts_scored"])
        raise
    finally:
        write_results(output_path, results, symbols)

    log.info("run finished unique_posts=%d comments=%d elapsed=%.1fs",
             progress["posts_scored"], progress["comments_scored"],
             time.monotonic() - run_start)
    if completed:
        print(f"\nDone. {progress['posts_scored']} posts and "
              f"{progress['comments_scored']} comments scored.")


def main():
    ap = argparse.ArgumentParser(description="Fetch Mastodon posts mentioning stock tickers "
                                             "and score their sentiment.")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                    help=f"Parallel servers (default: {DEFAULT_WORKERS})")
    ap.add_argument("--output", default=str(OUTPUT_FILE),
                    help=f"JSON file for the sentiment results (default: {OUTPUT_FILE.name} "
                         "next to this script; overwritten each run)")
    ap.add_argument("--log-file", default=str(LOG_FILE),
                    help=f"Where to write the log (default: {LOG_FILE.name} next to this script)")
    ap.add_argument("--days", type=float, default=DAYS,
                    help=f"Only keep posts from the last N days (default: {DAYS})")
    ap.add_argument("--time", type=float, default=None,
                    help="Stop making requests after this many minutes; what was fetched is "
                         "still scored and saved (default: no limit)")
    args = ap.parse_args()
    if args.days <= 0:
        ap.error("--days must be greater than 0")
    if args.time is not None and args.time <= 0:
        ap.error("--time must be greater than 0")
    # counted from process start, so slow imports are inside the limit
    deadline = time.monotonic() + args.time * 60 if args.time is not None else None

    setup_logging(args.log_file)

    try:
        run(args.workers, args.output, days=args.days, deadline=deadline)
    except KeyboardInterrupt:
        log.info("run stopped by user")
        print("Stopped.")


if __name__ == "__main__":
    main()