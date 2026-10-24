"""
Fetch Mastodon posts (and their comments) from the last N days that mention
stock tickers, score the sentiment of each, and store the aggregated sentiment.

Flow:
    1. fetch_tickers()          -> list of tickers to look for (JSON file for now)
    2. Read the server list     -> Mastodon server domains (JSON file)
    3. For every server, query the hashtag timeline for each ticker (#AAPL, ...)
       in batches, AND crawl the broad finance "discovery" hashtags (#stocks,
       #investing, ...), then keep only posts that really mention a ticker and
       don't come from excluded authors (news feeds, bots, spam, ...).
    4. fetch_all_comments()     -> for every kept post, fetch its replies from the
                                   server the post came from (statuses/:id/context)
    5. score_sentiment()        -> pass posts + comments through sentiment_scorer
                                   (score_items) and read the scores back
    6. aggregate_sentiment()    -> combine post and comment sentiment, equal weight
    7. write_sentiment_to_database() -> store the sentiment data (JSON file for now)

Usage:
    python fetch_tag_posts.py
    python fetch_tag_posts.py --workers 10

Requires sentiment_scorer.py (unchanged) in the same folder.

Tickers file format (TICKERS_FILE) -- company name: ticker:
    {"NVIDIA": "NVDA", "Apple": "AAPL", "Berkshire Hathaway": "BRK.B"}
(The older formats {"tickers": [...]} and ["AAPL", "TSLA"] still work too.)

How a post counts as mentioning a ticker:
    * cashtag  ($AAPL in the text)                     -> always counts
    * hashtag  (#AAPL on the post)                     -> counts, unless the ticker
      is ambiguous (<= AMBIGUOUS_MAX_LEN letters, or listed in AMBIGUOUS_TICKERS).
      Ambiguous tickers also need a finance clue (a finance hashtag like
      #investing or a keyword like "earnings"). This keeps tickers that are also
      normal words or other things (#ALL, #IT, #ON, #A, #NVDA the screen reader)
      from matching unrelated posts.
    * company name ("Nvidia" for NVDA, taken from the name in tickers.json)
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
import json
import re
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html import unescape
from urllib.parse import urlparse

import requests

# ---- Settings (edit here) ----
SERVERS_FILE = "mastodon_servers.json"       # Mastodon server list JSON
TICKERS_FILE = "tickers.json"       # ticker list JSON (stand-in for a data source)
SENTIMENT_OUTPUT_FILE = "mastodon_ticker_sentiment.json"  # stand-in for the database
SCORES_FILE = "mastodon_item_scores.jsonl"   # raw per-item scores written by the scorer
SCORE_RESUME = True                 # True = reuse scores already in SCORES_FILE
                                    # (items are keyed by post/comment URI)
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
MATCH_COMPANY_NAMES = True          # match "Nvidia" etc. (name from tickers.json)
MATCH_BARE_SYMBOLS = True           # match bare uppercase "NVDA" (non-ambiguous tickers only)

# ---- Author exclusion (we want opinions from people, not feeds/bots/spam) ----
EXCLUDE_AUTHORS = True              # master switch for everything in this block

# Whole instances that host news feeds / bots. Best-effort list: a domain that
# doesn't exist or never appears in your data does no harm. Extend it after
# reviewing "author_exclusions" and "prolific_authors_dropped" in the output.
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
# are listed in the output so you can review them.
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

SENTIMENT_FIELDS = ("score", "bullish", "bearish", "neutral")
DROP_REASONS = ("bot", "excluded_author", "too_few_words",
                "ticker_needs_context", "no_ticker_match")


# ------------------------------------------------------- data source I/O ---
def fetch_tickers(path=TICKERS_FILE):
    """
    Return the tickers to search for as a list of dicts:
        {"symbol": "AAPL", "name": "Apple Inc." or None, "tag": "aapl"}
    `tag` is the hashtag form of the symbol (lowercase, no punctuation).

    TODO: replace the JSON read below with the real data source (API, DB query,
    etc.) -- as long as this function keeps returning the same structure,
    nothing else in the script needs to change.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

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
            print(f"  ! skipping invalid ticker: {symbol!r}")
            continue
        if symbol in seen:
            continue
        seen.add(symbol)

        tickers.append(
            {"symbol": symbol, "name": name, "tag": re.sub(r"[^a-z0-9]", "", symbol.lower())}
        )
    return tickers


def write_sentiment_to_database(output, path=SENTIMENT_OUTPUT_FILE):
    """
    Persist the sentiment results. `output` is a dict with run metadata, a
    "sentiment_per_ticker" summary and a "posts" list (each post has a unique
    "uri" and a nested "comments" list, all carrying sentiment only).

    TODO: the database isn't ready yet, so this writes a JSON file. When it is,
    replace the body with an insert/upsert (e.g. one row per post and one per
    comment keyed on "uri", plus a run-metadata row). The signature can stay
    the same.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)


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
    """Record an exclusion so the output can show why and who."""
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


def get_with_retry(session, url, params=None):
    """GET with simple retry/backoff and 429 handling. Returns Response or None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                print(f"  ! {url}: {e}")
                return None
            time.sleep(2 ** attempt)
            continue

        if resp.status_code == 200:
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
            time.sleep(wait)
            continue

        if resp.status_code >= 500 and attempt < MAX_RETRIES:
            time.sleep(2 ** attempt)
            continue

        # 401/403/404/410 etc: server doesn't allow public tag timeline
        print(f"  ! {url}: HTTP {resp.status_code}")
        return None
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

    url = f"https://{domain}/api/v1/timelines/tag/{primary}"
    params = {"limit": PAGE_SIZE}
    if extras:
        params["any[]"] = extras
    if local_only:
        params["local"] = "true"

    pages = 0
    first_ok = False

    while url and pages < max_pages:
        resp = get_with_retry(session, url, params)
        if resp is None:
            break
        pages += 1
        first_ok = True

        try:
            page = resp.json()
        except ValueError:
            break
        if not isinstance(page, list) or not page:
            break

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

        # Timeline is newest-first, so once we see a post older than the cutoff
        # everything after it is older too.
        if reached_cutoff:
            break

        # Follow pagination via the Link header (rel="next")
        next_link = resp.links.get("next", {}).get("url")
        url, params = next_link, None  # next URL already contains its params

        time.sleep(0.3)  # be polite

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
        ok = fetch_batch(session, domain, batch, index, cutoff, local_only,
                         seen_uris, posts, stats, max_pages=max_pages)
        any_ok = any_ok or ok
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
    resp = get_with_retry(session, url)
    if resp is None:
        return None
    try:
        descendants = (resp.json() or {}).get("descendants") or []
    except ValueError:
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
    for post in posts:
        if SKIP_CONTEXT_WHEN_NO_REPLIES and not post.get("replies"):
            result[item_key(post)] = []
            continue
        comments = fetch_comments_for_post(session, domain, post, stats)
        if comments is None:
            stats["context_failed"] += 1
            result[item_key(post)] = []
        else:
            result[item_key(post)] = comments
        time.sleep(0.3)  # be polite
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


def score_sentiment(posts, comments_by_post):
    """
    Run all posts and comments through sentiment_scorer.score_items() and
    return {id: {"id", "score", "bullish", "bearish", "neutral"}}.
    Imported here so fetching doesn't need torch/transformers until this point.
    """
    from ..sentiment_scorer import load_score_records, score_items

    items = build_score_items(posts, comments_by_post)
    print(f"Scoring {len(items)} items ({len(posts)} posts + "
          f"{len(items) - len(posts)} comments)...")
    score_items(items, SCORES_FILE, resume=SCORE_RESUME)
    return load_score_records(SCORES_FILE)


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


def aggregate_sentiment(posts, comments_by_post, records):
    """
    Build the sentiment-only output rows (no post/comment text) and the
    per-ticker summary. Returns (rows, per_ticker).
    """
    rows, per_ticker_scores = [], defaultdict(list)
    missing = 0

    for p in posts:
        pkey = item_key(p)
        post_rec = records.get(f"post::{pkey}")
        if post_rec is None:
            missing += 1
            continue
        post_sent = _pick(post_rec)

        comment_rows, comment_sents = [], []
        for c in comments_by_post.get(pkey, []):
            c_rec = records.get(f"comment::{item_key(c)}")
            if c_rec is None:
                continue
            c_sent = _pick(c_rec)
            comment_sents.append(c_sent)
            comment_rows.append({
                "uri": c.get("uri"),
                "url": c.get("url"),
                "created_at": c.get("created_at"),
                "author": c.get("author"),
                "sentiment": c_sent,
            })

        comment_avg, combined = combine_sentiment(post_sent, comment_sents)
        rows.append({
            "uri": p.get("uri"),
            "url": p.get("url"),
            "created_at": p.get("created_at"),
            "author": p.get("author"),
            "source_server": p.get("source_server"),
            "matched_tickers": p["matched_tickers"],
            "post_sentiment": post_sent,
            "comments_scored": len(comment_rows),
            "comment_sentiment": comment_avg,   # None when there are no comments
            "combined_sentiment": combined,
            "comments": comment_rows,
        })
        for sym in p["matched_tickers"]:
            per_ticker_scores[sym].append(combined["score"])

    if missing:
        print(f"  ! {missing} posts had no score record and were skipped")

    per_ticker = {
        sym: {"posts": len(s), "mean_combined_score": round(sum(s) / len(s), 2)}
        for sym, s in sorted(per_ticker_scores.items(), key=lambda kv: -len(kv[1]))
    }
    return rows, per_ticker


# ---------------------------------------------------------------- main ---
def main():
    ap = argparse.ArgumentParser(description="Fetch Mastodon posts mentioning stock tickers "
                                             "and score their sentiment.")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                    help=f"Parallel servers (default: {DEFAULT_WORKERS})")
    args = ap.parse_args()

    tickers = fetch_tickers()
    if not tickers:
        print("No tickers loaded; nothing to do.")
        return
    index = TickerIndex(tickers)

    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS)
    domains = load_domains(SERVERS_FILE, OPEN_ONLY, MASTODON_ONLY)
    print(f"Loaded {len(tickers)} tickers and {len(domains)} servers; "
          f"fetching posts since {cutoff.isoformat()}")

    results = {}
    total_stats = Counter()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(fetch_server, d, tickers, index, cutoff, LOCAL_ONLY): d
            for d in domains
        }
        for fut in as_completed(futures):
            domain = futures[fut]
            try:
                _, posts, stats = fut.result()
            except Exception as e:  # never let one server kill the run
                print(f"[{domain}] failed: {e}")
                continue
            results[domain] = posts
            total_stats.update(stats)
            if stats["server_unavailable"]:
                print(f"[{domain}] unavailable, skipped")
            else:
                dropped = ", ".join(f"{r}={stats[r]}" for r in DROP_REASONS if stats[r])
                print(f"[{domain}] kept {len(posts)} of {stats['seen']} posts"
                      + (f" (dropped: {dropped})" if dropped else ""))

    # Federated servers often see the same post; de-duplicate by canonical URI.
    seen, unique = set(), []
    for posts in results.values():
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

    excluded = summarize_author_exclusions(total_stats)
    if excluded["by_reason"]:
        print("Excluded authors by reason: "
              + ", ".join(f"{r}={n}" for r, n in excluded["by_reason"].items()))

    unique, prolific = drop_prolific_authors(unique)
    if prolific:
        print(f"Dropped posts from {len(prolific)} prolific authors "
              f"(> {MAX_POSTS_PER_AUTHOR} posts each): "
              + ", ".join(f"{h} ({c})" for h, c in list(prolific.items())[:5])
              + (" ..." if len(prolific) > 5 else ""))
        print(f"{len(unique)} posts remain.")

    if not unique:
        print("Nothing to score.")
        return

    # ---- comments ----
    comments_by_post, comment_stats = {}, Counter()
    if FETCH_COMMENTS:
        print("Fetching comments...")
        comments_by_post, comment_stats = fetch_all_comments(unique, args.workers)
    total_comments = sum(len(v) for v in comments_by_post.values())

    # ---- sentiment ----
    records = score_sentiment(unique, comments_by_post)
    rows, per_ticker = aggregate_sentiment(unique, comments_by_post, records)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "since": cutoff.isoformat(),
        "settings": {
            "comment_aggregation": COMMENT_AGGREGATION,
            "fetch_comments": FETCH_COMMENTS,
            "direct_replies_only": DIRECT_REPLIES_ONLY,
            "max_comments_per_post": MAX_COMMENTS_PER_POST,
            "exclude_bots": EXCLUDE_BOTS,
            "min_words": MIN_WORDS,
            "min_comment_words": MIN_COMMENT_WORDS,
            "score_scale": "1 (most negative) .. 10 (most positive)",
        },
        "tickers_searched": len(tickers),
        "servers_queried": len(domains),
        "servers_unavailable": total_stats["server_unavailable"],
        "posts_seen": total_stats["seen"],
        "posts_dropped": {r: total_stats[r] for r in DROP_REASONS},
        "discovery_tags": DISCOVERY_TAGS,
        "author_exclusions": {
            "posts": excluded,
            "comments": summarize_author_exclusions(comment_stats),
        },
        "prolific_authors_dropped": prolific,
        "posts_scored": len(rows),
        "comments_found": comment_stats["comments_found"],
        "comments_scored": total_comments,
        "comments_dropped": {
            "bot": comment_stats["comment_bot"],
            "too_few_words": comment_stats["comment_too_few_words"],
            "not_direct_reply": comment_stats["comment_not_direct"],
            "excluded_author": comment_stats["comment_excluded_author"],
        },
        "comment_fetch_failures": comment_stats["context_failed"],
        "sentiment_per_ticker": per_ticker,
        "posts": rows,
    }

    write_sentiment_to_database(output)

    print(f"\nDone. {len(rows)} posts and {total_comments} comments scored; "
          f"sentiment saved to {SENTIMENT_OUTPUT_FILE}")


if __name__ == "__main__":
    main()