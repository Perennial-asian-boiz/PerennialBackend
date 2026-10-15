"""
Fetch Mastodon posts from the last N days that mention stock tickers.

Flow:
    1. fetch_tickers()      -> list of tickers to look for (JSON file for now)
    2. Read the server list -> Mastodon server domains (JSON file)
    3. For every server, query the hashtag timeline for each ticker (#AAPL, ...)
       in batches, then keep only posts that really mention a ticker.
    4. write_to_database()  -> store the results (JSON file for now)

Usage:
    python fetch_tag_posts.py
    python fetch_tag_posts.py --workers 10

Tickers file format (TICKERS_FILE) -- either of these works:
    {"tickers": [{"symbol": "AAPL", "name": "Apple Inc."}, {"symbol": "TSLA"}]}
    ["AAPL", "TSLA", "BRK.B"]

How a post counts as mentioning a ticker:
    * cashtag  ($AAPL in the text)                     -> always counts
    * hashtag  (#AAPL on the post) AND a finance clue  -> counts
      (a finance clue = a finance hashtag like #investing or a finance
       keyword like "earnings"). This keeps tickers that are also normal
       words (#ALL, #IT, #ON, #A) from matching unrelated posts.

"""

import argparse
import json
import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html import unescape

import requests

# ---- Settings (edit here) ----
SERVERS_FILE = "mastodon_servers.json"       # Mastodon server list JSON
TICKERS_FILE = "tickers.json"       # ticker list JSON (stand-in for a data source)
OUTPUT_FILE = "mastodon_ticker_posts.json"   # stand-in for the database
DAYS = 7                            # how many days back to fetch
LOCAL_ONLY = False                  # True = only posts authored on each server
OPEN_ONLY = False                   # True = only servers with open_registrations
MASTODON_ONLY = False               # True = only servers where software == "mastodon"

# ---- Post filters ----
EXCLUDE_BOTS = True                 # drop accounts flagged as bots (account.bot)
MIN_WORDS = 5                       # min words of real text (URLs, #tags, @mentions
                                    # excluded). Set to 0 to disable.
REQUIRE_CONTEXT_FOR_HASHTAG_MATCH = True  # hashtag-only ticker matches need a finance clue

# Finance clue 1: co-occurring hashtags (lowercase, no #)
FINANCE_TAGS = {
    "stock", "stocks", "stockmarket", "stockmarketnews", "investing",
    "investment", "investor", "investors", "trading", "trader", "daytrading",
    "swingtrading", "finance", "markets", "wallstreet", "nasdaq", "nyse",
    "sp500", "dowjones", "earnings", "dividend", "dividends", "etf", "ipo",
    "options", "equities", "bullmarket", "bearmarket", "shares", "fintech",
    "economy",
}

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

    raw = data.get("tickers", []) if isinstance(data, dict) else data

    tickers, seen = [], set()
    for item in raw:
        if isinstance(item, str):
            symbol, name = item, None
        elif isinstance(item, dict):
            symbol, name = item.get("symbol") or item.get("ticker"), item.get("name")
        else:
            continue
        if not symbol:
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


def write_to_database(output, path=OUTPUT_FILE):
    """
    Persist the results. `output` is a dict with run metadata plus a "posts"
    list (each post has a unique "uri").

    TODO: the database isn't ready yet, so this writes a JSON file. When it is,
    replace the body with an insert/upsert (e.g. one row per post keyed on
    "uri", plus a run-metadata row). The signature can stay the same.
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


class TickerIndex:
    """Fast lookups for matching posts against the ticker list."""

    def __init__(self, tickers):
        self.symbols = {t["symbol"] for t in tickers}
        self.tag_to_symbol = {t["tag"]: t["symbol"] for t in tickers}


def evaluate_status(status, index):
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

    # 2. Too little real text (image-only / link-only / hashtag-only posts)
    word_count = count_real_words(text)
    if MIN_WORDS and word_count < MIN_WORDS:
        return None, "too_few_words"

    # 3. Ticker match
    cashtag_hits = {m.upper() for m in CASHTAG_RE.findall(f"{text}\n{alt_joined}")} & index.symbols
    hashtag_hits = {index.tag_to_symbol[t] for t in tag_names if t in index.tag_to_symbol}
    context = finance_context(text, alt_joined, tag_names)

    matches = {}
    for symbol in hashtag_hits:
        if context or not REQUIRE_CONTEXT_FOR_HASHTAG_MATCH:
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
            record, reason = evaluate_status(status, index)
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

    for i in range(0, len(tickers), TICKERS_PER_REQUEST):
        batch = tickers[i:i + TICKERS_PER_REQUEST]
        ok = fetch_batch(session, domain, batch, index, cutoff, local_only,
                         seen_uris, posts, stats)
        any_ok = any_ok or ok
        if not ok and not any_ok:
            # Nothing has worked on this server yet (down, or tag timelines
            # require login) -- don't hammer it with every remaining batch.
            stats["server_unavailable"] = 1
            break

    return domain, posts, stats


# ---------------------------------------------------------------- main ---
def main():
    ap = argparse.ArgumentParser(description="Fetch Mastodon posts mentioning stock tickers.")
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
                print(f"[{domain}] kept {len(posts)} of {stats['seen']} posts")

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

    ticker_counts = Counter(sym for p in unique for sym in p["matched_tickers"])

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "since": cutoff.isoformat(),
        "filters": {
            "exclude_bots": EXCLUDE_BOTS,
            "min_words": MIN_WORDS,
            "require_context_for_hashtag_match": REQUIRE_CONTEXT_FOR_HASHTAG_MATCH,
        },
        "tickers_searched": len(tickers),
        "servers_queried": len(domains),
        "servers_unavailable": total_stats["server_unavailable"],
        "servers_with_posts": sum(1 for v in results.values() if v),
        "posts_seen": total_stats["seen"],
        "dropped": {
            "bot": total_stats["bot"],
            "too_few_words": total_stats["too_few_words"],
            "ticker_needs_context": total_stats["ticker_needs_context"],
            "no_ticker_match": total_stats["no_ticker_match"],
        },
        "total_kept_before_dedupe": sum(len(v) for v in results.values()),
        "count": len(unique),
        "posts_per_ticker": dict(ticker_counts.most_common()),
        "posts": unique,
    }

    write_to_database(output)

    print(
        f"\nDone. Seen {total_stats['seen']} posts; dropped "
        f"{total_stats['bot']} bot, {total_stats['too_few_words']} too short, "
        f"{total_stats['ticker_needs_context']} ticker-hashtag without finance context, "
        f"{total_stats['no_ticker_match']} no ticker."
    )
    print(f"{len(unique)} unique posts saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()