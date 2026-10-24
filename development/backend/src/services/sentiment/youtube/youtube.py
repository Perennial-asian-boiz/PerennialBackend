import os
import sys
import json
import html
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import yt_dlp
from faster_whisper import WhisperModel
import glob
import random
import re
from dotenv import load_dotenv
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from collections import deque
from pathlib import Path
import logging
import asyncio
import threading
from dataclasses import dataclass, field
import shutil
import argparse
import time


BACKEND_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent
DATA_DIR = BACKEND_DIR/"data"
DATA_DIR.mkdir(exist_ok=True)
UTILS_DIR = BACKEND_DIR/"src"/"utils"
LOG_DIR = BACKEND_DIR/"logs"
LOG_DIR.mkdir(exist_ok=True)
COOKIES_FILE = "youtube_cookies.txt"
# scratch space for downloaded subtitle/audio files, one sub-folder per ticker.
TMP_DIR = DATA_DIR/"tmp"

# ---- storage -------------------------------------------------------------------------
# Everything that changes once per VIDEO is stored in append-only JSON Lines files: one
# complete line per record, flushed + fsynced, so saving one video costs O(1) no matter how
# much has been collected, and a crash can only ever damage the last (partial) line, which
# is repaired on the next start. (Same approach the sentiment scorer uses.)
STATUS_LOG = DATA_DIR/"status.jsonl"            # {"id": ..., "status": "<attempts>" | "skipped"}
TRANSCRIPT_LOG = DATA_DIR/"transcripts.jsonl"   # {"id", "title", "source", "transcript"}
LEGACY_STATUS_FILE = DATA_DIR/"status.json"     # old format, imported once (see migrate_legacy)
# where the sentiment scores (JSON Lines, written by score_texts) are stored.
SCORES_FILE = DATA_DIR/"youtube_sentiment_scores.jsonl"
# per-ticker / per-video averaged scores (JSON). The chunk-level scores live in SCORES_FILE.
VIDEO_SCORES_FILE = DATA_DIR/"youtube_video_sentiment.json"

API_WORKERS = 3          # tickers hitting the YouTube Data API at once
TRANSCRIPT_WORKERS = 1   # videos being downloaded/transcribed at once; keep low to avoid 429s
MAX_TRIES = 5            # genuine failed attempts before a video is given up on
SCORE_BATCH_VIDEOS = 10  # transcribed videos collected before one score_texts() call

# The scorer truncates every text to 256 tokens, so long transcripts are split into chunks,
# each chunk is scored, and the chunk scores are averaged per video.
# Chunking is by words (the scorer's tokenizer isn't touched). 100 words is roughly
# 130-200 tokens, which leaves a safety margin under the 256 limit even for number/ticker-heavy text.
CHUNK_WORDS = 100
MIN_TAIL_WORDS = 25      # a final chunk shorter than this is merged into the previous one

# sentiment_scorer.py lives in UTILS_DIR (not edited here, only imported).
# Adjust this path if the scorer lives somewhere else.
if str(UTILS_DIR) not in sys.path:
    sys.path.insert(0, str(UTILS_DIR))
from ..sentiment_scorer import score_texts, load_scores
from ....utils.fetch_tickers import fetch_ticker_list

def safe_name(text):
    """ make a ticker safe to use inside a filename (e.g. 'BRK/B' -> 'BRK_B') """
    return re.sub(r"[^\w.\-]", "_", text)


def create_logger(ticker):
    """ creates a logger for a single ticker """
    logger = logging.getLogger(f"youtube.{safe_name(ticker)}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Prevent duplicate handlers if called multiple times
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"
    )

    handler = logging.FileHandler(LOG_DIR/f"{safe_name(ticker)}.log", encoding="utf-8")
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger

# takes the number of days to look back
# The default of 7 keeps the original behaviour when --days is not given.
def get_published_after(days=7):
    """ compute the date `days` days ago for video timeframes """
    now = datetime.now(timezone.utc)
    return (
        now - relativedelta(days=days)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


load_dotenv()

# main options for YT-DLP calls attached to other options in get_transcripts
COMMON_YDL_OPTS = {
    "quiet": True,
    "retries": 2,
    "fragment_retries": 2,
    "sleep_interval_requests": 2,
}

@dataclass
class BackoffConfig:
    start_minutes: float = 5     # first sleep after being rate limited
    step_minutes: float = 5      # extra minutes added for each further sleep in a row
    max_minutes: float = 30      # a sleep is never longer than this
    threshold: int = 2           # consecutive rate-limit errors before we sleep at all

    def sleep_seconds(self, level):
        """ sleep length for the given backoff level (0 = first sleep): start + step * level, capped """
        minutes = min(self.start_minutes + self.step_minutes * level, self.max_minutes)
        return minutes * 60


# decides whether a yt_dlp error is YouTube throttling/blocking us.
# Treating every failed download as "a failure" when deciding to sleep is bad, so
# ordinary failures triggered long sleeps even though
# YouTube never rate limited us. Now only genuine rate-limit/bot-check errors count.
def is_rate_limited(error):
    msg = str(error)
    return "429" in msg or "Too Many Requests" in msg or "not a bot" in msg


# yt_dlp errors that will never go away by retrying (the video itself is the problem).
PERMANENT_ERROR_PHRASES = (
    "private video",
    "video unavailable",
    "this video is unavailable",
    "this video is not available",
    "has been removed",
    "members-only",
    "members only",
    "account associated with this video has been terminated",
    "no longer available",
)

def is_permanent_failure(error):
    msg = str(error).lower()
    return any(phrase in msg for phrase in PERMANENT_ERROR_PHRASES)


# YouTube Data API: the DAILY quota is used up (as opposed to a short-lived per-second
# rate limit, which is a different reason and not a reason to stop discovery).
def is_quota_exceeded(error):
    if not isinstance(error, HttpError):
        return False
    if getattr(getattr(error, "resp", None), "status", None) != 403:
        return False
    text = str(error)
    return "quotaExceeded" in text or "dailyLimitExceeded" in text or "exceeded your quota" in text


# rate-limit state that is SHARED by every ticker (it used to be local variables inside
# get_transcripts, one set per ticker).
class RateLimitState:
    def __init__(self, config):
        self.config = config
        self.consecutive_429s = 0
        self.backoff_level = 0
        self.pause_until = 0.0     # time.monotonic() value; workers wait until it has passed

    def seconds_remaining(self):
        return max(0.0, self.pause_until - time.monotonic())

    def record_rate_limit(self, logger):
        self.consecutive_429s += 1
        # only pause once we have been rate limited several times IN A ROW
        if self.consecutive_429s >= self.config.threshold:
            wait = self.config.sleep_seconds(self.backoff_level)
            self.pause_until = time.monotonic() + wait
            logger.info(
                f"pausing ALL workers for {wait/60:.1f} minutes after {self.consecutive_429s} "
                f"consecutive rate limits (backoff level {self.backoff_level})"
            )
            self.consecutive_429s = 0
            self.backoff_level += 1

    def record_success(self):
        # a success means YouTube is serving us again, so reset the backoff
        self.consecutive_429s = 0
        self.backoff_level = 0


async def interruptible_sleep(stop_event, seconds):
    if seconds <= 0:
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def wait_if_paused(rate, stop_event):
    while not stop_event.is_set():
        remaining = rate.seconds_remaining()
        if remaining <= 0:
            return
        await interruptible_sleep(stop_event, remaining)


def load_json(path, default):
    """helper function to load json files"""

    #if filepath is bad return default
    if not os.path.exists(path):
        return default

    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()

        #if json is empty return default
        if not content:
            return default

        return json.loads(content)


def load_json_safe(path, default):
    """ like load_json, but a corrupt file gives a warning and the default instead of a crash """
    try:
        return load_json(path, default)
    except (ValueError, OSError) as e:
        print(f"warning: could not read {path} ({e}); using defaults")
        return default


def save_json_atomic(path, obj):
    """ write to a temp file, then swap it in, so a crash can't leave a half-written file """
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=4, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def load_tickers(path):
    """
    Reads tickers from a JSON file. Accepted shapes:
      ["AAPL", "MSFT"]
      {"tickers": ["AAPL", "MSFT"]}
      {"AAPL": {...}, "MSFT": {...}}              (keys are used as tickers)
      [{"ticker": "AAPL"}, {"symbol": "MSFT"}]
    Duplicates are removed (order preserved).
    """
    raw = load_json(path, None)
    if raw is None:
        raise SystemExit(f"Could not find or read tickers file: {path}")

    if isinstance(raw, dict):
        raw = raw["tickers"] if "tickers" in raw else list(raw.keys())

    tickers = []
    for entry in raw:
        if isinstance(entry, dict):
            entry = entry.get("ticker") or entry.get("symbol")
        if isinstance(entry, str) and entry.strip():
            tickers.append(entry.strip())

    tickers = list(dict.fromkeys(tickers))
    if not tickers:
        raise SystemExit(f"No tickers found in {path}")
    return tickers


# ---- append-only JSON Lines helpers ----------------------------------------------------

def repair_tail(path):
    """ drop a trailing partial line left behind by a crash (every record ends in '\\n') """
    if not os.path.exists(path):
        return
    with open(path, "rb+") as f:
        data = f.read()
        keep = data.rfind(b"\n") + 1      # 0 if there is no newline at all
        if keep != len(data):
            f.truncate(keep)


def append_jsonl(path, record):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def read_jsonl(path):
    """ all well-formed records of a JSON Lines file, in file order (malformed lines skipped) """
    records = []
    if not os.path.exists(path):
        return records
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(record, dict):
                records.append(record)
    return records


class Store:
    """
    The ONE shared record of what has been done, keyed by video ID (not by ticker).

    * status:      id -> "<failed attempts so far>" or "skipped" (permanently given up on)
    * transcripts: id -> {"id", "title", "source", "transcript"}
    * in_progress: ids a worker is handling right now (so two tickers that matched the same
                   video can't both download it)
    * titles:      id -> title (from the search results)

    Every change is appended to its own log and only for the video that changed, so one
    ticker can never overwrite another ticker's newer values with a stale copy.
    """

    def __init__(self):
        repair_tail(STATUS_LOG)
        repair_tail(TRANSCRIPT_LOG)
        self.status = {}
        self.transcripts = {}
        self.in_progress = set()
        self.titles = {}

        fresh_install = not (STATUS_LOG.exists() or TRANSCRIPT_LOG.exists())

        status_records = read_jsonl(STATUS_LOG)
        for rec in status_records:
            if "id" in rec and "status" in rec:
                self.status[str(rec["id"])] = str(rec["status"])
        for rec in read_jsonl(TRANSCRIPT_LOG):
            if rec.get("id"):
                self.transcripts[rec["id"]] = rec     # later record wins

        if fresh_install:
            self.migrate_legacy()
        else:
            self._compact_status_log(len(status_records))

    def migrate_legacy(self):
        """ one-time import of the old <ticker>_transcripts.json files and status.json """
        for path in sorted(DATA_DIR.glob("*_transcripts.json")):
            for id, entry in load_json_safe(path, {}).items():
                if id not in self.transcripts and isinstance(entry, dict) and entry.get("transcript"):
                    self.save_transcript(id, {
                        "title": entry.get("title", ""),
                        "source": entry.get("source", ""),
                        "transcript": entry["transcript"],
                    })
        for id, value in load_json_safe(LEGACY_STATUS_FILE, {}).items():
            # old "done" entries without a transcript are not trusted: that video is redone.
            # Old attempt counters are kept.
            if value != "done" and id not in self.status and id not in self.transcripts:
                self.set_status(id, value)

    def _compact_status_log(self, lines_read):
        """ the status log gets one line per change; rewrite it once it is mostly stale lines """
        live = {id: s for id, s in self.status.items() if id not in self.transcripts}
        if lines_read <= 2 * len(live) + 100:
            return
        tmp_path = f"{STATUS_LOG}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            for id, s in live.items():
                f.write(json.dumps({"id": id, "status": s}) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, STATUS_LOG)
        self.status = live

    def attempts(self, id):
        s = self.status.get(id, "0")
        return int(s) if s.isdigit() else 0

    def is_finished(self, id):
        """ done (has a transcript), permanently skipped, or out of tries """
        if id in self.transcripts:
            return True
        s = self.status.get(id, "0")
        return s == "skipped" or (s.isdigit() and int(s) >= MAX_TRIES)

    def set_status(self, id, value):
        value = str(value)
        if self.status.get(id) == value:
            return
        self.status[id] = value
        append_jsonl(STATUS_LOG, {"id": id, "status": value})

    def save_transcript(self, id, entry):
        # The transcript line is the proof that a video is done (see is_finished), so there
        # is no separate "done" status that could get out of step after a crash.
        record = {"id": id, **entry}
        append_jsonl(TRANSCRIPT_LOG, record)
        self.transcripts[id] = record


# googleapiclient clients (httplib2 underneath) are NOT thread-safe, so every API worker
# gets its own client instead of sharing one.
def build_youtube_client():
    return googleapiclient.discovery.build(
        "youtube",
        "v3",
        developerKey=os.getenv("YOUTUBE_API_KEY")
    )


# ---- YouTube Data API: one page at a time ---------------------------------------------
# These two functions are synchronous/blocking; the discovery worker runs them in a thread
# with asyncio.to_thread() so several tickers can query the API at the same time.

def search_page(youtube, query, published_after, page_token):
    """ ONE search page (50 results, 100 quota units). Returns (ids, {id: title}, next_page_token) """
    response = youtube.search().list(
        part="snippet",
        maxResults=50,
        order="date",
        publishedAfter=published_after,
        q=query,
        type="video",
        fields="nextPageToken, items(id/videoId, snippet/title)",
        pageToken=page_token
    ).execute()

    ids, titles = [], {}
    for item in response.get("items", []):
        video_id = item.get("id", {}).get("videoId")
        if video_id:
            ids.append(video_id)
            titles[video_id] = item.get("snippet", {}).get("title", "")
    return ids, titles, response.get("nextPageToken")


def fetch_details(youtube, ids):
    """ video details (1 quota unit per call of up to 50 ids). Returns {id: item} """
    details = {}
    for i in range(0, len(ids), 50):
        response = youtube.videos().list(
            part="statistics,snippet,contentDetails",
            fields="items(id,snippet/title,snippet/channelTitle,snippet/publishedAt,snippet/tags,statistics/viewCount,statistics/likeCount,contentDetails/duration)",
            id=",".join(ids[i:i+50])
        ).execute()
        for item in response.get("items", []):
            details[item["id"]] = item
    return details


def metadata_entry(item):
    snippet = item.get("snippet", {})
    statistics = item.get("statistics", {})
    contentDetails = item.get("contentDetails", {})
    return {
        "title": snippet.get("title"),
        "channel": snippet.get("channelTitle"),
        "publishedDate": snippet.get("publishedAt"),
        "tags": snippet.get("tags", []),
        "viewCount": int(statistics.get("viewCount", 0)),
        "likeCount": int(statistics.get("likeCount", 0)),
        "duration": contentDetails.get("duration", "PT0S")
    }


def parse_vtt(path):
    """
    Turns a WebVTT file written by yt-dlp into plain text.

    * Works block by block: only text AFTER a cue's timing line is kept, so the header
      (WEBVTT / Kind: / Language:), NOTE/STYLE blocks and cue numbers are dropped.
    * YouTube's rolling auto-captions repeat the previous line in the next cue, so a line
      identical to the one just kept is skipped (otherwise the text comes out ~2x).
    * Inline timing tags, HTML entities (&gt;&gt;), speaker-change markers (>>) and sound
      annotations such as [Music] are removed.
    """
    with open(path, encoding="utf-8") as f:
        content = f.read()

    text = []
    previous = None
    for block in re.split(r"\n\s*\n", content):
        lines = block.splitlines()
        timing = next((i for i, l in enumerate(lines) if "-->" in l), None)
        if timing is None:
            continue
        for line in lines[timing + 1:]:
            line = re.sub(r"<[^>]+>", "", line)
            line = html.unescape(line)
            line = re.sub(r"\[[^\]]*\]", "", line)
            line = " ".join(line.replace(">>", " ").split())
            if not line or line == previous:
                continue
            text.append(line)
            previous = line
    return " ".join(text)

# initialize whisper model for audio transcription
model = WhisperModel(
    "tiny",
    device="cpu",
    compute_type="int8"
)
# added lock so only one thread runs the shared Whisper model at a time.
# Transcription is run in a worker thread, and one model instance shouldn't be used
# by several threads at once
whisper_lock = threading.Lock()

def transcribe(audio_path):
    """
    Uses the whisper model to turn audio into text when no captions are available.
    Returns (text, language, language_probability).

    faster-whisper detects the language when transcribe() is called, but only decodes the
    audio while the returned `segments` generator is consumed. So a non-English video is
    rejected here BEFORE the expensive decoding: text is None and the caller drops the video.
    """
    with whisper_lock:
        segments, info = model.transcribe(audio_path)
        if info.language != "en":
            return None, info.language, info.language_probability
        text = " ".join(seg.text for seg in segments)
        return text, info.language, info.language_probability


# small blocking helpers that process_video() runs via asyncio.to_thread().
def _extract_info(url, ydl_opts):
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        return ydl.extract_info(url, download=False)

def _download(opts, url):
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([url])


# everything the pipeline needs to remember about one ticker between turns.

@dataclass
class TickerState:
    ticker: str
    logger: logging.Logger
    metadata_file: str
    transcripts_file: str       # per-ticker export, rewritten once at the end of a run
    metadata: dict              # video metadata from the YouTube API (saved after every page)
    tmp_dir: Path
    tmp: str                    # glob-escaped tmp_dir
    ydl_common: dict
    video_ids: list = field(default_factory=list)               # every video found this run
    seen: set = field(default_factory=set)
    pending: deque = field(default_factory=deque)               # video IDs still to process
    transcript_metadata: dict = field(default_factory=dict)     # id -> (kind, language)
    page_token: str = None      # next search page to fetch
    pages: int = 0              # search pages fetched this run
    discovery_done: bool = False
    scheduled: bool = False     # True while the ticker is in ready_queue or held by a worker
    completed: int = 0          # transcripts finished during this run


@dataclass
class Context:
    """ everything the workers share """
    store: Store
    rate: RateLimitState
    published_after: str
    stop_event: asyncio.Event            # time limit: shut everything down
    quota_event: asyncio.Event           # API quota used up: stop discovery only
    discovery_queue: asyncio.Queue       # round-robin line of tickers waiting for their next search page
    ready_queue: asyncio.Queue           # round-robin line of tickers with videos to transcribe
    score_queue: asyncio.Queue           # ids of transcribed videos waiting to be scored
    scored_chunks: set = field(default_factory=set)     # chunk texts already in the scores file
    score_enqueued: set = field(default_factory=set)    # video ids already sent to score_queue


def build_ticker_state(ticker):
    tmp_dir = TMP_DIR/safe_name(ticker)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # per-ticker copy of the cookies file (yt_dlp writes cookies back to the file it is given)
    ticker_cookies = tmp_dir/"_cookies.txt"
    shutil.copyfile(COOKIES_FILE, ticker_cookies)

    metadata_file = f"{DATA_DIR}/{safe_name(ticker)}_metadata.json"
    return TickerState(
        ticker=ticker,
        logger=create_logger(ticker),
        metadata_file=metadata_file,
        transcripts_file=f"{DATA_DIR}/{safe_name(ticker)}_transcripts.json",
        metadata=load_json_safe(metadata_file, {}),
        tmp_dir=tmp_dir,
        tmp=glob.escape(str(tmp_dir)),   # escaped so glob() treats the path literally
        ydl_common={**COMMON_YDL_OPTS, "cookiefile": str(ticker_cookies)},
    )


def cleanup_ticker(state):
    """ remove this ticker's private copy of the cookies file """
    (state.tmp_dir/"_cookies.txt").unlink(missing_ok=True)


# ---- chunking + scoring helpers ---------------------------------------------------------

def chunk_text(text, chunk_words=CHUNK_WORDS, min_tail=MIN_TAIL_WORDS):
    """
    Splits a transcript into chunks of about `chunk_words` words so every chunk fits inside
    the scorer's 256-token window. A very short last chunk is merged into the one before it
    (so it can't produce a noisy score from a handful of words).
    """
    words = text.split()
    if not words:
        return []
    chunks = [words[i:i + chunk_words] for i in range(0, len(words), chunk_words)]
    if len(chunks) > 1 and len(chunks[-1]) < min_tail:
        chunks[-2].extend(chunks.pop())
    return [" ".join(c) for c in chunks]


def transcript_chunks(store, id):
    record = store.transcripts.get(id)
    text = record.get("transcript") if record else None
    return chunk_text(text) if isinstance(text, str) else []


def schedule_scoring(ctx, id):
    if id not in ctx.score_enqueued:
        ctx.score_enqueued.add(id)
        ctx.score_queue.put_nowait(id)


def schedule_ticker(ctx, state):
    """ put the ticker in the transcript round-robin line unless it is already in it / being served """
    if state.pending and not state.scheduled:
        state.scheduled = True
        ctx.ready_queue.put_nowait(state)


def register_videos(ctx, state, ids):
    """ add newly discovered video ids to the ticker, skipping work that is already finished """
    store = ctx.store
    for id in ids:
        if id in state.seen:
            continue
        state.seen.add(id)
        state.video_ids.append(id)
        if store.is_finished(id):
            state.logger.info(f"{id}: skipping, done or given up on")
            # transcribed in an earlier run: make sure it was actually scored (a crash may have
            # stopped that), but only if some of its chunks are missing from the scores file
            if any(c not in ctx.scored_chunks for c in transcript_chunks(store, id)):
                schedule_scoring(ctx, id)
            continue
        state.pending.append(id)
    schedule_ticker(ctx, state)


# ---- stage 1: video discovery (round robin, one search page per turn) -------------------
# Every ticker gets ONE search page, then goes to the back of discovery_queue, so discovery
# is spread evenly over all tickers. Videos found on a page go to the transcript workers
# straight away. When the daily API quota runs out, quota_event stops discovery at once and the
# transcript workers carry on with everything that was already discovered.

async def discover_one_page(name, state, youtube, ctx):
    logger = state.logger

    def stop_discovery(reason):
        state.discovery_done = True
        logger.info(f"{state.ticker}: discovery finished ({reason})")

    def quota_hit():
        if not ctx.quota_event.is_set():
            ctx.quota_event.set()
            print(f"[{name}] YouTube API quota exhausted: stopping discovery, "
                  f"continuing with the videos already found")
        logger.warning(f"{state.ticker}: API quota exhausted")

    try:
        # quoted so YouTube searches for the exact phrase (same as the original script)
        ids, titles, next_token = await asyncio.to_thread(
            search_page, youtube, f'"{state.ticker}"', ctx.published_after, state.page_token
        )
    except Exception as e:
        if is_quota_exceeded(e):
            quota_hit()
            stop_discovery("quota")
        else:
            # one failing ticker (bad query, network error...) is logged and skipped instead of
            # crashing the whole run; videos it already found are still processed.
            logger.error(f"{state.ticker}: API search failed: {e}")
            print(f"[{name}] {state.ticker}: API search failed ({e})")
            stop_discovery("error")
        return

    state.page_token = next_token
    state.pages += 1
    ctx.store.titles.update(titles)
    # the videos go to the transcript workers BEFORE the details call, so a quota error there
    # can't cost us the videos we just found
    register_videos(ctx, state, ids)
    print(f"[{name}] {state.ticker}: page {state.pages}, {len(ids)} videos "
          f"({len(state.video_ids)} total)")

    new_ids = [id for id in ids if id not in state.metadata]
    if new_ids:
        try:
            details = await asyncio.to_thread(fetch_details, youtube, new_ids)
            for id in new_ids:
                item = details.get(id)
                if item is not None:           # None = video removed in between
                    state.metadata[id] = metadata_entry(item)
            save_json_atomic(state.metadata_file, state.metadata)
        except Exception as e:
            if is_quota_exceeded(e):
                quota_hit()
            else:
                logger.error(f"{state.ticker}: video details failed: {e}")

    if next_token and not ctx.quota_event.is_set() and not ctx.stop_event.is_set():
        ctx.discovery_queue.put_nowait(state)      # back of the line
    else:
        stop_discovery("no more pages" if not next_token else "stopped")


async def api_worker(name, ctx):
    youtube = await asyncio.to_thread(build_youtube_client)

    while True:
        state = await ctx.discovery_queue.get()
        try:
            if state is None:
                break
            if ctx.stop_event.is_set() or ctx.quota_event.is_set():
                state.discovery_done = True
                continue    # discovery is over: the remaining tickers are simply dropped
            await discover_one_page(name, state, youtube, ctx)
        finally:
            ctx.discovery_queue.task_done()


# ---- stage 2: transcripts ---------------------------------------------------------------

def refund_attempt(store, id):
    store.set_status(id, max(0, store.attempts(id) - 1))


def handle_ytdlp_error(state, id, error, ctx, stage):
    """
    What a failed yt_dlp call means for the video. The attempt was already counted when
    processing started, so:
      * rate limited -> YouTube's fault: give the attempt back and retry later
      * permanent    -> the video is gone/private/...: never try it again
      * anything else-> the counted attempt stands (after MAX_TRIES it is given up on)
    """
    logger = state.logger
    store = ctx.store
    if is_rate_limited(error):
        ctx.rate.record_rate_limit(logger)
        refund_attempt(store, id)
        state.pending.append(id)
        logger.warning(f"{id}: {stage} rate limited by YouTube, retrying later")
    elif is_permanent_failure(error):
        store.set_status(id, "skipped")
        logger.warning(f"{id}: {stage} failed permanently, skipping video: {error}")
    else:
        logger.error(f"{id}: {stage} failed (attempt {store.attempts(id)}/{MAX_TRIES}): {error}")


# processes exactly ONE video and returns; transcript_worker() puts the ticker at the back
# of the queue afterwards, so tickers take turns, one video each (round robin).
# The yt_dlp requests inside ONE ticker are still done one at a time on purpose
async def process_next_video(state, ctx):
    store = ctx.store
    id = state.pending.popleft()

    # the check uses the shared store, so a video that matched several tickers is only done once
    if store.is_finished(id):
        state.logger.info(f"{id}: skipping, already done or given up on")
        return
    if id in store.in_progress:
        state.logger.info(f"{id}: skipping, another worker is already processing it")
        return

    store.in_progress.add(id)
    try:
        await process_video(state, id, ctx)
    finally:
        store.in_progress.discard(id)
        # whatever happened, no half-downloaded files are left behind
        for old_path in glob.glob(f"{state.tmp}/{id}*"):
            try:
                os.remove(old_path)
            except OSError:
                pass


async def process_video(state, id, ctx):
    logger = state.logger
    store = ctx.store
    url = f"https://www.youtube.com/watch?v={id}"

    # Count this attempt NOW, so a video that crashes the whole process is still given up on
    # eventually. It is handed back if the attempt turns out to be YouTube's fault.
    store.set_status(id, store.attempts(id) + 1)

    # retrieve transcript metadata (cached in state so a retry after a 429 doesn't repeat it)
    if id not in state.transcript_metadata:
        logger.info(f"{id}: processing metadata")
        try:
            info = await asyncio.to_thread(_extract_info, url, state.ydl_common)
            logger.info(f"{id}: yt_dlp metadata success")
        except Exception as e:
            handle_ytdlp_error(state, id, e, ctx, "metadata")
            return

        manual = info.get("subtitles", {})
        english_manual = next(
            (lang for lang in manual if lang.lower().startswith("en")),
            None
        )
        automatic = info.get("automatic_captions", {})
        english_automatic = next(
            (lang for lang in automatic if lang.lower().startswith("en")),
            None
        )
        if english_manual:
            state.transcript_metadata[id] = ("manual", english_manual)
        elif english_automatic:
            state.transcript_metadata[id] = ("automatic", english_automatic)
        else:
            state.transcript_metadata[id] = ("audio", None)

    await interruptible_sleep(ctx.stop_event, random.uniform(4,7))
    if ctx.stop_event.is_set():
        refund_attempt(store, id)
        state.pending.appendleft(id)
        return

    title = store.titles.get(id) or state.metadata.get(id, {}).get("title") or ""
    tmp_dir = state.tmp_dir
    tmp = state.tmp
    opts = None
    src_type = None

    # parameters for yt_dlp to try and get any transcripts from youtube API
    if state.transcript_metadata[id][0] == "manual":
        opts = {
            **state.ydl_common,
            "skip_download": True,
            "subtitlesformat": "vtt",
            "writesubtitles": True,
            "writeautomaticsub": False,
            "subtitleslangs": [state.transcript_metadata[id][1]],
            "outtmpl": f"{tmp_dir}/{id}.%(ext)s",
            "quiet": True,
            "no_warnings": True
        }
        src_type = "text"
    elif state.transcript_metadata[id][0] == "automatic":
        opts = {
            **state.ydl_common,
            "skip_download": True,
            "subtitlesformat": "vtt",
            "writesubtitles": False,
            "writeautomaticsub": True,
            "subtitleslangs": [state.transcript_metadata[id][1]],
            "outtmpl": f"{tmp_dir}/{id}.%(ext)s",
            "quiet": True,
        }
        src_type = "text"
    else:
        opts = {
            **state.ydl_common,
            "format": "bestaudio/best",
            "outtmpl": f"{tmp_dir}/%(id)s.%(ext)s",
            "quiet": True,
            "sleep_interval": 10,
            "max_sleep_interval": 30,
        }
        src_type = "audio"

    for old_path in glob.glob(f"{tmp}/{id}*"):
        try:
            os.remove(old_path)
        except OSError:
            logger.warning(f"{id}: could not remove stale file {old_path}")

    try:
        await asyncio.to_thread(_download, opts, url)
    except Exception as e:
        handle_ytdlp_error(state, id, e, ctx, "download")
        return

    if src_type == "text":
        matches = sorted(glob.glob(f"{tmp}/{id}*.vtt"))
        if not matches:
            logger.error(f"{id}: No text matches found")
            return
        result = parse_vtt(matches[0])
        logger.info(f"{id}: Success, yt_dlp downloaded text transcript")
    else:
        matches = sorted(glob.glob(f"{tmp}/{id}.*"))
        matches = [
            path for path in matches
            if path.endswith((".mp3", ".m4a", ".webm", ".opus", ".wav"))
        ]
        if not matches:
            logger.error(f"{id}: No audio matches found")
            return
        try:
            result, language, probability = await asyncio.to_thread(transcribe, matches[0])
        except Exception as e:
            logger.error(f"{id}: whisper transcription failed: {e}")
            return
        if result is None:
            # not English: drop the video for good (no transcript saved, never retried)
            store.set_status(id, "skipped")
            logger.info(f"{id}: dropped, whisper detected language '{language}' "
                        f"(p={probability:.2f}), not English")
            return
        logger.info(f"{id}: Success, yt_dlp downloaded video audio and was transcibed "
                    f"(language {language}, p={probability:.2f})")

    if not result or not result.strip():
        # deterministic (no speech / empty captions), so retrying would not help
        store.set_status(id, "skipped")
        logger.warning(f"{id}: empty transcript, skipping video")
        return

    # ONE appended line: the finished transcript is safe on disk from here on
    store.save_transcript(id, {"title": title, "source": src_type, "transcript": result})
    logger.info(f"{id}: Done")
    ctx.rate.record_success()
    state.completed += 1
    schedule_scoring(ctx, id)


# transcript_worker takes ONE ticker from the queue, processes ONE of its videos,
# and puts the ticker back at the END of the queue if it still has videos left. Because the
# queue is first-in-first-out, every ticker gets a turn before any ticker gets a second one.
# A ticker is only ever in the queue (or held by one worker), never both (state.scheduled),
# so two workers can't process the same ticker at once.
async def transcript_worker(name, ctx):
    while True:
        state = await ctx.ready_queue.get()
        try:
            if state is None or ctx.stop_event.is_set():
                break   # sentinel, or shutdown requested: leave remaining videos for the next run

            await wait_if_paused(ctx.rate, ctx.stop_event)
            if ctx.stop_event.is_set():
                break

            try:
                await process_next_video(state, ctx)
            except Exception as e:
                state.logger.error(f"{state.ticker}: transcript gathering failed: {e}")
                print(f"[{name}] {state.ticker}: transcript gathering failed ({e})")

            if state.pending and not ctx.stop_event.is_set():
                ctx.ready_queue.put_nowait(state)      # back of the line
            elif not state.pending:
                # nothing left right now. If discovery is still adding videos, it puts the
                # ticker back in the line itself (schedule_ticker)
                state.scheduled = False
                if state.discovery_done:
                    print(f"[{name}] {state.ticker}: transcripts finished "
                          f"({state.completed} new this run)")
        finally:
            ctx.ready_queue.task_done()


# ---- stage 3: scoring, one batch of finished videos at a time ---------------------------
# Runs next to the transcript workers. Finished videos are collected, and every
# SCORE_BATCH_VIDEOS videos (and once more at the very end) their chunks go to ONE
# score_texts() call. Only one call ever runs at a time: score_texts appends to a single file
# and repairs its tail on entry, so two concurrent calls could damage each other's lines.
# Returns whatever score_texts() returned last (the scores file path).
async def scoring_worker(ctx, scores_file):
    batch = []
    last_result = None

    async def flush():
        nonlocal batch, last_result
        ids, batch = batch, []
        chunks = []
        for id in ids:
            chunks.extend(transcript_chunks(ctx.store, id))
        chunks = [c for c in dict.fromkeys(chunks) if c not in ctx.scored_chunks]
        if not chunks:
            return
        print(f"[scoring] {len(chunks)} chunk(s) from {len(ids)} video(s)...")
        try:
            last_result = await asyncio.to_thread(score_texts, chunks, str(scores_file))
            ctx.scored_chunks.update(chunks)
        except Exception as e:
            # nothing is lost: the transcripts are saved, and the next run scores whatever
            # is missing from the scores file
            print(f"[scoring] batch failed ({e}); it will be retried on the next run")

    while True:
        id = await ctx.score_queue.get()
        if id is None:
            await flush()
            break
        batch.append(id)
        if len(batch) >= SCORE_BATCH_VIDEOS:
            await flush()

    return last_result


def collect_video_chunks(states, store):
    """
    Returns {ticker: {video_id: {"title": ..., "chunks": [str, ...]}}} for every video found
    in this run's time window that has a finished, non-empty transcript (including ones
    transcribed in earlier runs, or under another ticker).
    """
    by_ticker = {}
    for s in states:
        videos = {}
        for id in s.video_ids:
            chunks = transcript_chunks(store, id)
            if chunks:
                videos[id] = {"title": store.transcripts[id].get("title", ""), "chunks": chunks}
        if videos:
            by_ticker[s.ticker] = videos
    return by_ticker


def aggregate_scores(by_ticker, scores):
    """
    Turns chunk scores ({chunk_text: score}, from load_scores) into per-video and per-ticker
    scores. A video's score is the average of its chunk scores weighted by chunk length in
    words; a ticker's score is the plain average of its video scores.
    """
    out = {}
    for ticker, videos in by_ticker.items():
        scored = {}
        for id, v in videos.items():
            total, weight, n = 0.0, 0, 0
            for chunk in v["chunks"]:
                score = scores.get(chunk)
                if score is None:        # e.g. a malformed line was skipped when reading the file
                    continue
                w = len(chunk.split())
                total += score * w
                weight += w
                n += 1
            if n == 0:
                continue
            scored[id] = {
                "title": v["title"],
                "score": round(total / weight, 2),
                "chunks_scored": n,
                "chunks_total": len(v["chunks"]),
            }
        if scored:
            out[ticker] = {
                "score": round(sum(x["score"] for x in scored.values()) / len(scored), 2),
                "videos": scored,
            }
    return out


def export_ticker_transcripts(state, store):
    """
    Keeps the per-ticker <ticker>_transcripts.json files (same format as before) up to date for
    anything else that reads them. Written ONCE per ticker at the end of a run, atomically; the
    append-only transcripts.jsonl is the real record, so a crash before this loses nothing.
    """
    existing = load_json_safe(state.transcripts_file, {})
    changed = False
    for id in state.video_ids:
        record = store.transcripts.get(id)
        if record is None:
            continue
        entry = {k: v for k, v in record.items() if k != "id"}
        if existing.get(id) != entry:
            existing[id] = entry
            changed = True
    if changed:
        save_json_atomic(state.transcripts_file, existing)


# sets stop_event after the time limit. Everything else reacts to the event.
async def stop_after(minutes, stop_event):
    await asyncio.sleep(minutes * 60)
    print(f"\nTime limit of {minutes:g} minute(s) reached. Finishing the videos currently "
          f"in progress, saving, then shutting down...")
    stop_event.set()


# end-of-run report. Also makes it easy to see how evenly the work was shared.
def print_summary(states, stopped_early, quota_hit):
    notes = []
    if stopped_early:
        notes.append("stopped by time limit")
    if quota_hit:
        notes.append("API quota exhausted")
    print("\nSummary" + (f" ({', '.join(notes)})" if notes else "") + ":")
    for s in states:
        print(f"  {s.ticker}: {len(s.video_ids)} videos found over {s.pages} page(s), "
              f"{s.completed} transcribed this run, {len(s.pending)} video(s) left for next run")


async def async_main(time_limit_minutes=None, days=7, scores_file=SCORES_FILE):
    """
    Discovers videos (round robin over the tickers), transcribes them, and scores the
    transcripts in batches while the run is still going. Returns whatever score_texts()
    returns (the path of the chunk-level scores file), or None if no scores exist. Per-video and
    per-ticker averages are additionally written to VIDEO_SCORES_FILE.
    """
    tickers = await fetch_ticker_list()
    if not tickers:
        raise SystemExit("Could not find, read, or parse any tickers from the ticker source")
    print(f"Searching for {len(tickers)} tickers: {', '.join(tickers)} "
          f"(videos from the last {days:g} day(s))")

    states = []
    for ticker in tickers:
        try:
            states.append(build_ticker_state(ticker))
        except Exception as e:
            print(f"{ticker}: setup failed ({e}); skipping this ticker")
    if not states:
        return None

    store = Store()
    ctx = Context(
        store=store,
        rate=RateLimitState(BackoffConfig()),
        published_after=get_published_after(days),
        stop_event=asyncio.Event(),
        quota_event=asyncio.Event(),
        discovery_queue=asyncio.Queue(),
        ready_queue=asyncio.Queue(),
        score_queue=asyncio.Queue(),
    )
    # chunks that already have a score, so earlier runs' videos aren't scored again
    if os.path.exists(scores_file):
        ctx.scored_chunks = set(load_scores(scores_file))

    for state in states:
        ctx.discovery_queue.put_nowait(state)

    # never start more API workers than there are tickers
    n_api = max(1, min(API_WORKERS, len(states)))
    n_transcript = max(1, TRANSCRIPT_WORKERS)

    timer_task = None
    if time_limit_minutes is not None:
        timer_task = asyncio.create_task(stop_after(time_limit_minutes, ctx.stop_event))

    api_tasks = [
        asyncio.create_task(api_worker(f"api-{i+1}", ctx))
        for i in range(n_api)
    ]
    transcript_tasks = [
        asyncio.create_task(transcript_worker(f"transcript-{i+1}", ctx))
        for i in range(n_transcript)
    ]
    scoring_task = asyncio.create_task(scoring_worker(ctx, scores_file))

    result = None
    try:
        # discovery: every ticker has had all its pages, or the quota / time limit ended it
        await ctx.discovery_queue.join()
        for _ in range(n_api):
            ctx.discovery_queue.put_nowait(None)
        await asyncio.gather(*api_tasks)
        if ctx.quota_event.is_set():
            print("\nDiscovery stopped (API quota). Transcribing everything found so far...")

        # wait until every ticker's videos are finished (queue.join()) OR the time
        # limit fires, whichever comes first, then tell the transcript workers to stop.
        join_task = asyncio.ensure_future(ctx.ready_queue.join())
        stop_task = asyncio.ensure_future(ctx.stop_event.wait())
        await asyncio.wait({join_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        join_task.cancel()
        stop_task.cancel()

        for _ in range(n_transcript):
            ctx.ready_queue.put_nowait(None)
        # workers finish the video they are on (it can't be interrupted mid-download), save, exit
        await asyncio.gather(*transcript_tasks)

        # everything is transcribed: score the last (partial) batch
        ctx.score_queue.put_nowait(None)
        result = await scoring_task
    finally:
        if timer_task is not None:
            timer_task.cancel()
        for s in states:
            cleanup_ticker(s)
            try:
                export_ticker_transcripts(s, store)
            except Exception as e:
                print(f"{s.ticker}: could not export transcripts file ({e})")
        print_summary(states, ctx.stop_event.is_set(), ctx.quota_event.is_set())

    # score_texts() was not needed this run if everything had been scored before; the scores file
    # still exists then, and is what the averages are built from.
    scores_path = result or (str(scores_file) if os.path.exists(scores_file) else None)
    if scores_path is None:
        print("\nNo transcripts available to score.")
        return None

    by_ticker = collect_video_chunks(states, store)
    chunk_scores = await asyncio.to_thread(load_scores, scores_path)
    aggregate = aggregate_scores(by_ticker, chunk_scores)
    save_json_atomic(VIDEO_SCORES_FILE, aggregate)
    print(f"\nChunk-level sentiment scores: {scores_path}")
    print(f"Per-video/per-ticker averages written to: {VIDEO_SCORES_FILE}")
    for ticker, info in aggregate.items():
        print(f"  {ticker}: {info['score']} (avg of {len(info['videos'])} video(s))")

    return scores_path


def run(time_limit_minutes=None, days=7, scores_file=SCORES_FILE):
    """
    Synchronous entry point for other scripts: fetch YouTube transcripts for every ticker
    in tickers.json, score them, and return what score_texts() returns (the scores file path).
    Read the results back with sentiment_scorer.load_scores(<returned path>).
    """
    return asyncio.run(async_main(time_limit_minutes, days, scores_file))


def main():
    parser = argparse.ArgumentParser(
        description="Download YouTube transcripts for every ticker in tickers.json and score their sentiment."
    )
    parser.add_argument(
        "--time",
        type=float,
        default=None,
        help="Stop automatically after this many minutes: in-progress videos are finished "
             "and saved, then the script exits (default: run until everything is done)"
    )

    parser.add_argument(
        "--days",
        type=float,
        default=7,
        help="Only consider videos published within this many days (default: 7)"
    )
    parser.add_argument(
        "--scores-file",
        type=str,
        default=str(SCORES_FILE),
        help=f"Where to write the sentiment scores (default: {SCORES_FILE})"
    )
    args = parser.parse_args()
    if args.time is not None and args.time <= 0:
        parser.error("--time must be greater than 0")
    if args.days <= 0:
        parser.error("--days must be greater than 0")

    # Fail early with a clear message instead of
    # starting a long run and crashing on the first ticker.
    if not Path(COOKIES_FILE).is_file():
        raise SystemExit(f"Cookies file not found: {COOKIES_FILE}")

    return run(args.time, args.days, args.scores_file)

if __name__ == "__main__":
    main()