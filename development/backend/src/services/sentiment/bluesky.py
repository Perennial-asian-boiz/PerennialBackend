"""
Bluesky Jetstream listener: streams public posts and filters for ticker cashtags.

dependencies: websockets
Run: python jetstream_listener.py
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
TICKERS = {"TSLA", "NVDA", "AAPL", "AMD", "MSFT", "GME", "AMC", "SPY"}  # replace with ticker list from ARC
CURSOR_FILE = Path("cursor.txt")   # remembers where we left off so a restart doesn't lose data
ENGLISH_ONLY = True

# Matches $TSLA, $nvda, etc. Requires letters, so "$5" or "$100" won't match.
CASHTAG_RE = re.compile(r"(?<![\w$])\$([A-Za-z]{1,5})\b")

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
    return {m.upper() for m in CASHTAG_RE.findall(text)} & TICKERS

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

if __name__ == "__main__":
    try:
        asyncio.run(listen())
    except KeyboardInterrupt:
        print("Stopped.")