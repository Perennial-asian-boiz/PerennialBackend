"""
Bluesky Jetstream listener: streams public posts and filters for ticker cashtags.

Run: python bluesky.py
"""

import asyncio
import json
import re
import time
from pathlib import Path
import websockets

# ---------- Config ----------
JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe"
# Other public instances: jetstream1.us-east, jetstream1.us-west, jetstream2.us-west

FALLBACK_TICKERS = {"TSLA", "NVDA", "AAPL", "AMD", "MSFT", "GME", "AMC", "SPY"}
TICKER_FILE = Path("tickers.json")   # format: {"tickers": ["TSLA", "NVDA"]}
TICKER_POLL_SECONDS = 5              # how often to check the ticker source

CURSOR_FILE = Path("cursor.txt")   # remembers where we left off so a restart doesn't lose data
ENGLISH_ONLY = True

# Matches $TSLA, $nvda, etc. Requires letters, so "$5" or "$100" won't match.
CASHTAG_RE = re.compile(r"(?<![\w$])\$([A-Za-z]{1,5})\b")

async def fetch_tickers() -> set[str] | None:
    """
    Return the current ticker set from database.
    Return None if the source is unavailable or unreadable right now 
    the watcher will then keep the current set instead of wiping it

    returns a set of UPPERCASE tickers without "$", or None.
    """
    # ---- JSON file implementation (current) ----
    try:
        data = json.loads(TICKER_FILE.read_text())
        return {t.strip().upper().lstrip("$") for t in data["tickers"] if t and t.strip()}
    except (FileNotFoundError, json.JSONDecodeError, KeyError, TypeError):
        return None

    # ---- Supabase implementation (later) ----
    # Needs a client created once at startup (e.g. a module-level `sb`).
    # rows = (await sb.table("ticker_config").select("tickers").eq("id", 1).execute()).data
    # if not rows:
    #     return None
    # return {t.strip().upper().lstrip("$") for t in rows[0]["tickers"] if t}


class TickerWatcher:
    """
    Holds the current ticker set in memory and hot-swaps it when
    fetch_tickers() returns something different.
    The Jetstream websocket is never touched.
    """

    def __init__(self):
        self.tickers: frozenset[str] = frozenset(FALLBACK_TICKERS)

    async def refresh(self):
        try:
            fetched = await fetch_tickers()
        except Exception as e:
            print(f"Ticker fetch failed (keeping current set): {e}")
            return
        if fetched is None:
            return  # source unavailable -> keep current set
        new = frozenset(fetched)
        if new != self.tickers:
            added, removed = new - self.tickers, self.tickers - new
            self.tickers = new  # atomic reference swap; in-flight events are safe
            print(f"Tickers updated: +{sorted(added)} -{sorted(removed)}")
        # same set -> do nothing

    async def run(self):
        """Check the source on an interval. Swap this loop for push-based
        updates (e.g. Supabase Realtime) later if you want instant reloads."""
        while True:
            await self.refresh()
            await asyncio.sleep(TICKER_POLL_SECONDS)


watcher = TickerWatcher()


# ---------- Helpers ----------
def load_cursor():
    try:
        return int(CURSOR_FILE.read_text().strip())
    except (FileNotFoundError, ValueError):
        return None

def save_cursor(time_us):
    CURSOR_FILE.write_text(str(time_us))

def build_url(cursor=None):
    url = f"{JETSTREAM_URL}?wantedCollections=app.bsky.feed.post"
    if cursor:
        # Rewind a few seconds to avoid gaps; dedupe downstream using did + rkey
        url += f"&cursor={cursor - 5_000_000}"
    return url

def extract_tickers(text):
    return {m.upper() for m in CASHTAG_RE.findall(text)} & watcher.tickers

# pipeline
async def handle_post(post):
    """
    post = {
        "id": "did:plc:xxx/rkey",   # unique, use for dedupe
        "tickers": ["TSLA"],
        "text": "...",
        "created_at": "2026-09-29T12:00:00.000Z",
        "ingested_at": 1234567890.0,
    }
    TODO: score with FinBERT/VADER and write to your database.
    """
    print(f"[{','.join(post['tickers'])}] {post['text'][:120]!r}")

# Stream loop
async def process_event(raw):
    event = json.loads(raw)

    # Save cursor on every event
    if "time_us" in event:
        save_cursor(event["time_us"])

    if event.get("kind") != "commit":
        return
    commit = event.get("commit", {})
    if commit.get("operation") != "create" or commit.get("collection") != "app.bsky.feed.post":
        return

    record = commit.get("record", {})
    text = record.get("text", "")
    if not text:
        return
    if ENGLISH_ONLY and "en" not in record.get("langs", ["en"]):
        return

    tickers = extract_tickers(text)
    if not tickers:
        return

    await handle_post({
        "id": f"{event['did']}/{commit['rkey']}",
        "tickers": sorted(tickers),
        "text": text,
        "created_at": record.get("createdAt"),
        "ingested_at": time.time(),
    })

async def listen():
    backoff = 1
    while True:
        url = build_url(load_cursor())
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                print(f"Connected: {url}")
                backoff = 1
                async for raw in ws:
                    try:
                        await process_event(raw)
                    except Exception as e:  # prevent one bad event from ending stream
                        print(f"Event error: {e}")
        except (websockets.ConnectionClosed, OSError) as e:
            print(f"Disconnected ({e}). Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def main():
    await asyncio.gather(listen(), watcher.run())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")