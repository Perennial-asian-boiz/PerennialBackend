"""
YouTube sentiment: finds recent videos that match each ticker (YouTube Data API), gets a
transcript for each video (captions via yt_dlp, or audio via yt_dlp + Whisper), splits the
transcript into chunks, and scores the chunks with the shared sentiment scorer.

Items are videos. A video's id is its native YouTube video id. Its score is the average of
its chunk scores, weighted by chunk length in words. Videos that were transcribed are scored
in batches of SCORE_BATCH_VIDEOS videos (all their chunks go to ONE score_texts call).

Transcripts, metadata and scores are kept in memory only. Besides the results file and the
log file, the only thing kept on disk is status.json, which maps a video id to "done" (it was
scored) or "skipped" (it can never be transcribed: private, removed, not English, no speech),
so a later run does not process the same video again. The results file only covers the videos
processed in the current run. The scorer needs a file for its output, so a temporary one is
used and deleted right after. yt_dlp needs scratch files for subtitles/audio and its cookie
jar; they live in a temporary directory that is deleted at the end of the run (also after a
crash or Ctrl+C).

At the end of the run (or if it is interrupted or fails part-way) the scores are written
to a JSON file (default: youtube_sentiment_results.json next to this script, override with
--output; it is overwritten each run). Format: a list with one object per ticker:
    [{"ticker": "NVDA", "average_sentiment": 6.12, "post_count": 2,
      "posts": {"<video id>": 7.4, "<video id>": 4.84}}, ...]
("posts"/"post_count" hold videos here, so every source script has the same schema.)
A video that matches several tickers appears under each of them.

Logging: API errors, retries and a brief line per successful request are written to a
rotating log file (default: youtube.log next to this script, override with --log-file).
Timestamps are UTC. Item text, credentials and cookies are never logged.
"""

import argparse
import asyncio
import glob
import html
import json
import logging
import logging.handlers
import os
import random
import re
import shutil
import sys
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import googleapiclient.discovery
import httplib2
import yt_dlp
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from faster_whisper import WhisperModel
from googleapiclient.errors import HttpError

BACKEND_DIR = Path(__file__).resolve().parents[2]
UTILS_DIR = BACKEND_DIR / "src" / "utils"
COOKIES_FILE = "youtube_cookies.txt"
DATA_DIR = BACKEND_DIR / "data"

# the files this script leaves behind
STATUS_FILE = (
    DATA_DIR / "status.json"
)  # {video id: "done" | "skipped"}, kept across runs
OUTPUT_FILE = Path(__file__).with_name("youtube_sentiments.json")
LOG_FILE = Path(__file__).with_name("youtube.log")
LOG_MAX_BYTES = 5_000_000  # rotate at ~5 MB, keep 3 old files
LOG_BACKUPS = 3
BODY_SNIPPET = 200  # max chars of an error response body to log

API_WORKERS = 3  # tickers hitting the YouTube Data API at once
TRANSCRIPT_WORKERS = (
    1  # videos being downloaded/transcribed at once; keep low to avoid 429s
)
MAX_TRIES = 5  # genuine failed attempts before a video is given up on
API_RETRIES = (
    5  # attempts per YouTube Data API request (rate limits / 5xx / network errors)
)
SCORE_BATCH_VIDEOS = 10  # transcribed videos collected before one score_texts() call

# The scorer truncates every text to 256 tokens, so long transcripts are split into chunks,
# each chunk is scored, and the chunk scores are averaged per video.
# Chunking is by words (the scorer's tokenizer isn't touched). 100 words is roughly
# 130-200 tokens, which leaves a safety margin under the 256 limit even for number/ticker-heavy text.
CHUNK_WORDS = 100
MIN_TAIL_WORDS = 25  # a final chunk shorter than this is merged into the previous one

# sentiment_scorer.py lives in UTILS_DIR (not edited here, only imported).
# Adjust this path if the scorer lives somewhere else.
if str(UTILS_DIR) not in sys.path:
    sys.path.insert(0, str(UTILS_DIR))
from src.utils.console import stdout_logger  # noqa: E402
from src.utils.fetch_tickers import fetch_ticker_map  # noqa: E402

from ..sentiment_scorer import load_scores, score_texts  # noqa: E402

_out = stdout_logger()  # console lines (print() is banned in src/)

log = logging.getLogger("youtube")


# ---- logging -------------------------------------------------------------------------
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


def describe_error(error):
    """'ExceptionType: message' cut to BODY_SNIPPET chars, for a single log line"""
    return f"{type(error).__name__}: {' '.join(str(error).split())[:BODY_SNIPPET]}"


def http_error_info(error):
    """
    (status, first BODY_SNIPPET chars of the response body) of a googleapiclient HttpError.
    str(HttpError) contains the request URL, and that URL contains the API key, so it is
    never logged: only the status and the response body are.
    """
    status = getattr(getattr(error, "resp", None), "status", None)
    content = getattr(error, "content", b"")
    body = (
        content.decode("utf-8", "replace")
        if isinstance(content, bytes)
        else str(content)
    )
    return status, " ".join(body.split())[:BODY_SNIPPET]


def api_error_reason(error):
    """
    Short, console-safe reason for a failed API call: 'HTTP <status>: <message>' for an
    HttpError, never its URL (the URL contains the API key).
    """
    if isinstance(error, HttpError):
        status, body = http_error_info(error)
        try:
            content = error.content
            raw = (
                content.decode("utf-8", "replace")
                if isinstance(content, bytes)
                else str(content)
            )
            body = json.loads(raw)["error"]["message"]
        except (ValueError, KeyError, TypeError, AttributeError):
            pass
        return f"HTTP {status}: {' '.join(str(body).split())[:100]}"
    return describe_error(error)


def safe_name(text):
    """make a ticker safe to use inside a filename (e.g. 'BRK/B' -> 'BRK_B')"""
    return re.sub(r"[^\w.\-]", "_", text)


# takes the number of days to look back
# The default of 7 keeps the original behaviour when --days is not given.
def get_published_after(days=7):
    """compute the date `days` days ago for video timeframes"""
    now = datetime.now(timezone.utc)
    return (now - relativedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


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
    start_minutes: float = 5  # first sleep after being rate limited
    step_minutes: float = 5  # extra minutes added for each further sleep in a row
    max_minutes: float = 30  # a sleep is never longer than this
    threshold: int = 2  # consecutive rate-limit errors before we sleep at all

    def sleep_seconds(self, level):
        """sleep length for the given backoff level (0 = first sleep): start + step * level, capped"""
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
    return (
        "quotaExceeded" in text
        or "dailyLimitExceeded" in text
        or "exceeded your quota" in text
    )


# YouTube Data API: a short-lived rate limit (HTTP 429, or a 403 "rateLimitExceeded" /
# "userRateLimitExceeded"). These are worth waiting out and retrying.
def is_api_rate_limited(error):
    if not isinstance(error, HttpError):
        return False
    status = getattr(getattr(error, "resp", None), "status", None)
    if status == 429:
        return True
    return status == 403 and "ratelimitexceeded" in str(error).lower()


# rate-limit state that is SHARED by every ticker (it used to be local variables inside
# get_transcripts, one set per ticker).
class RateLimitState:
    def __init__(self, config):
        self.config = config
        self.consecutive_429s = 0
        self.backoff_level = 0
        self.pause_until = (
            0.0  # time.monotonic() value; workers wait until it has passed
        )

    def seconds_remaining(self):
        return max(0.0, self.pause_until - time.monotonic())

    def record_rate_limit(self):
        self.consecutive_429s += 1
        # only pause once we have been rate limited several times IN A ROW
        if self.consecutive_429s >= self.config.threshold:
            wait = self.config.sleep_seconds(self.backoff_level)
            self.pause_until = time.monotonic() + wait
            log.warning(
                "rate limited by YouTube: pausing ALL workers for %.1f minutes after %d "
                "consecutive rate limits (backoff level %d)",
                wait / 60,
                self.consecutive_429s,
                self.backoff_level,
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

    # if filepath is bad return default
    if not os.path.exists(path):
        return default

    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()

        # if json is empty return default
        if not content:
            return default

        return json.loads(content)


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


class Store:
    """
    The ONE shared record of what has been done, keyed by video ID (not by ticker).

    In memory only (nothing about a video's content is ever written to disk):
    * tries:       id -> failed/started attempts during THIS run (retried in-run, up to MAX_TRIES)
    * transcripts: id -> {"id", "title", "source", "transcript"}
    * in_progress: ids a worker is handling right now (so two tickers that matched the same
                   video can't both download it)
    * titles:      id -> title (from the search results)
    * retries:     id -> how many times the video had to be retried (for the log)

    Persisted in status.json, across runs, so a video is never processed twice:
    * persisted:   id -> "done" (transcribed AND scored) or "skipped" (permanently unusable).
                   Partial attempts are never saved: a video that failed all its tries this
                   run is simply tried again by the next run.
    """

    def __init__(self, status_file):
        self.status_file = Path(status_file)
        self.tries = {}
        self.transcripts = {}
        self.in_progress = set()
        self.titles = {}
        self.retries = {}
        self.persisted = {}
        self._load()

    def _load(self):
        try:
            raw = load_json(self.status_file, {})
        except (ValueError, OSError):
            log.exception(
                "could not read status file %s, starting with an empty one",
                self.status_file,
            )
            return
        if not isinstance(raw, dict):
            log.error(
                "status file %s is not a JSON object, starting with an empty one",
                self.status_file,
            )
            return
        ignored = 0
        for id, value in raw.items():
            if value in ("done", "skipped"):
                self.persisted[str(id)] = value
            else:
                ignored += 1  # e.g. attempt counters from the old format
        log.info(
            "status file loaded path=%s done=%d skipped=%d ignored=%d",
            self.status_file,
            sum(v == "done" for v in self.persisted.values()),
            sum(v == "skipped" for v in self.persisted.values()),
            ignored,
        )

    def _save(self):
        """atomic write (temp file in the same folder, then os.replace); never raises"""
        path = self.status_file
        tmp = path.with_name(path.name + ".tmp")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.persisted, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except Exception:
            log.exception("failed to write status file %s", path)
            try:
                tmp.unlink()
            except OSError:
                pass

    def attempts(self, id):
        return self.tries.get(id, 0)

    def set_attempts(self, id, n):
        self.tries[id] = n

    def is_finished(self, id):
        """done / skipped (now or in an earlier run), has a transcript, or out of tries"""
        return (
            id in self.transcripts
            or id in self.persisted
            or self.tries.get(id, 0) >= MAX_TRIES
        )

    def mark_done(self, id):
        if self.persisted.get(id) != "done":
            self.persisted[id] = "done"
            self._save()

    def mark_skipped(self, id):
        if self.persisted.get(id) != "skipped":
            self.persisted[id] = "skipped"
            self._save()

    def note_retry(self, id):
        self.retries[id] = self.retries.get(id, 0) + 1

    def save_transcript(self, id, entry):
        self.transcripts[id] = {"id": id, **entry}


# googleapiclient clients (httplib2 underneath) are NOT thread-safe, so every API worker
# gets its own client instead of sharing one.
def build_youtube_client():
    return googleapiclient.discovery.build(
        "youtube", "v3", developerKey=os.getenv("YOUTUBE_API_KEY")
    )


# ---- YouTube Data API: one page at a time ---------------------------------------------
# These two functions are synchronous/blocking; the discovery worker runs them in a thread
# (through api_call) so several tickers can query the API at the same time.


def search_page(youtube, query, published_after, page_token):
    """ONE search page (50 results, 100 quota units). Returns (ids, {id: title}, next_page_token)"""
    response = (
        youtube.search()
        .list(
            part="snippet",
            maxResults=50,
            order="date",
            publishedAfter=published_after,
            q=query,
            type="video",
            fields="nextPageToken, items(id/videoId, snippet/title)",
            pageToken=page_token,
        )
        .execute()
    )

    ids, titles = [], {}
    for item in response.get("items", []):
        video_id = item.get("id", {}).get("videoId")
        if video_id:
            ids.append(video_id)
            titles[video_id] = item.get("snippet", {}).get("title", "")
    return ids, titles, response.get("nextPageToken")


def fetch_details(youtube, ids):
    """video details (1 quota unit per call of up to 50 ids). Returns {id: item}"""
    details = {}
    for i in range(0, len(ids), 50):
        response = (
            youtube.videos()
            .list(
                part="statistics,snippet,contentDetails",
                fields="items(id,snippet/title,snippet/channelTitle,snippet/publishedAt,snippet/tags,statistics/viewCount,statistics/likeCount,contentDetails/duration)",
                id=",".join(ids[i : i + 50]),
            )
            .execute()
        )
        for item in response.get("items", []):
            details[item["id"]] = item
    return details


class ApiRetriesExhausted(RuntimeError):
    """a YouTube Data API request still failed after API_RETRIES attempts (already logged)"""


async def api_call(label, fn, *args, retries=API_RETRIES):
    """
    Runs a blocking YouTube Data API function in a worker thread, with logging and retries:
    short-lived rate limits, 5xx errors and network errors/timeouts are logged and retried
    with backoff. Rejected requests (other 4xx, and the daily quota) are logged and re-raised
    at once. Returns (result, seconds the successful attempt took).
    """
    for attempt in range(1, retries + 1):
        start = time.monotonic()
        try:
            result = await asyncio.to_thread(fn, *args)
        except HttpError as e:
            status, body = http_error_info(e)
            if is_quota_exceeded(e):
                log.error("%s request rejected status=%s body=%s", label, status, body)
                raise
            if is_api_rate_limited(e):
                wait = min(2 ** (attempt - 1), 300)
                log.warning(
                    "%s rate limited status=%s sleeping=%ss attempt=%d/%d body=%s",
                    label,
                    status,
                    wait,
                    attempt,
                    retries,
                    body,
                )
                await asyncio.sleep(wait)
                continue
            if status is not None and status >= 500:
                log.warning(
                    "%s server error status=%s attempt=%d/%d body=%s",
                    label,
                    status,
                    attempt,
                    retries,
                    body,
                )
                await asyncio.sleep(2 ** (attempt - 1))
                continue
            log.error("%s request rejected status=%s body=%s", label, status, body)
            raise
        except (OSError, httplib2.HttpLib2Error) as e:
            # OSError covers timeouts, connection errors and TLS errors
            log.warning(
                "%s network error attempt=%d/%d: %s",
                label,
                attempt,
                retries,
                describe_error(e),
            )
            await asyncio.sleep(2 ** (attempt - 1))
            continue

        if attempt > 1:
            log.warning("%s succeeded after %d attempts", label, attempt)
        return result, time.monotonic() - start

    log.error("%s failed after %d attempts", label, retries)
    raise ApiRetriesExhausted(f"{label} failed after {retries} attempts")


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
        "duration": contentDetails.get("duration", "PT0S"),
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
        timing = next((i for i, line in enumerate(lines) if "-->" in line), None)
        if timing is None:
            continue
        for line in lines[timing + 1 :]:
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
model = WhisperModel("tiny", device="cpu", compute_type="int8")
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
    metadata: dict  # video metadata from the YouTube API (in memory only)
    tmp_dir: Path
    tmp: str  # glob-escaped tmp_dir
    ydl_common: dict
    video_ids: list = field(default_factory=list)  # every video found this run
    seen: set = field(default_factory=set)
    pending: deque = field(default_factory=deque)  # video IDs still to process
    transcript_metadata: dict = field(default_factory=dict)  # id -> (kind, language)
    page_token: str = None  # next search page to fetch
    pages: int = 0  # search pages fetched this run
    discovery_done: bool = False
    scheduled: bool = (
        False  # True while the ticker is in ready_queue or held by a worker
    )
    completed: int = 0  # transcripts finished during this run


@dataclass
class Context:
    """everything the workers share"""

    store: Store
    rate: RateLimitState
    published_after: str
    stop_event: asyncio.Event  # time limit: shut everything down
    quota_event: asyncio.Event  # API quota used up: stop discovery only
    discovery_queue: (
        asyncio.Queue
    )  # round-robin line of tickers waiting for their next search page
    ready_queue: asyncio.Queue  # round-robin line of tickers with videos to transcribe
    score_queue: asyncio.Queue  # ids of transcribed videos waiting to be scored
    results: dict  # {ticker: {video_id: score}}, written to the JSON file at the end
    video_scores: dict = field(
        default_factory=dict
    )  # video id -> score, for every scored video
    video_tickers: dict = field(
        default_factory=dict
    )  # video id -> tickers it was found under
    score_enqueued: set = field(
        default_factory=set
    )  # video ids already sent to score_queue


def build_ticker_state(ticker, tmp_root):
    tmp_dir = tmp_root / safe_name(ticker)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # per-ticker copy of the cookies file (yt_dlp writes cookies back to the file it is given)
    ticker_cookies = tmp_dir / "_cookies.txt"
    shutil.copyfile(COOKIES_FILE, ticker_cookies)

    return TickerState(
        ticker=ticker,
        metadata={},
        tmp_dir=tmp_dir,
        tmp=glob.escape(str(tmp_dir)),  # escaped so glob() treats the path literally
        ydl_common={**COMMON_YDL_OPTS, "cookiefile": str(ticker_cookies)},
    )


def cleanup_ticker(state):
    """remove this ticker's private copy of the cookies file"""
    (state.tmp_dir / "_cookies.txt").unlink(missing_ok=True)


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
    chunks = [words[i : i + chunk_words] for i in range(0, len(words), chunk_words)]
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
    """put the ticker in the transcript round-robin line unless it is already in it / being served"""
    if state.pending and not state.scheduled:
        state.scheduled = True
        ctx.ready_queue.put_nowait(state)


def register_videos(ctx, state, ids):
    """
    Add newly discovered video ids to the ticker, skipping work that is already finished.
    Returns how many of the ids were new for this ticker.
    """
    store = ctx.store
    new = 0
    for id in ids:
        if id in state.seen:
            continue
        state.seen.add(id)
        state.video_ids.append(id)
        ctx.video_tickers.setdefault(id, set()).add(state.ticker)
        new += 1
        if store.is_finished(id):
            log.info(
                "%s: skipping, already done/skipped or out of tries (ticker=%s)",
                id,
                state.ticker,
            )
            if id in ctx.video_scores:
                # already scored under another ticker this run: it counts under this one too
                ctx.results.setdefault(state.ticker, {})[id] = ctx.video_scores[id]
            elif id in store.transcripts:
                schedule_scoring(ctx, id)
            continue
        state.pending.append(id)
    schedule_ticker(ctx, state)
    return new


# ---- stage 1: video discovery (round robin, one search page per turn) -------------------
# Every ticker gets ONE search page, then goes to the back of discovery_queue, so discovery
# is spread evenly over all tickers. Videos found on a page go to the transcript workers
# straight away. When the daily API quota runs out, quota_event stops discovery at once and the
# transcript workers carry on with everything that was already discovered.


async def discover_one_page(name, state, youtube, ctx):
    ticker = state.ticker

    def stop_discovery(reason):
        state.discovery_done = True
        log.info(
            "discovery finished ticker=%s reason=%s pages=%d videos=%d",
            ticker,
            reason,
            state.pages,
            len(state.video_ids),
        )

    def quota_hit():
        if not ctx.quota_event.is_set():
            ctx.quota_event.set()
            _out.info(
                f"[{name}] YouTube API quota exhausted: stopping discovery, "
                f"continuing with the videos already found"
            )
        log.warning("%s: API quota exhausted", ticker)

    # quoted so YouTube searches for the exact phrase (same as the original script)
    query = f'"{ticker}"'
    try:
        (ids, titles, next_token), elapsed = await api_call(
            f"search.list ticker={ticker} page={state.pages + 1}",
            search_page,
            youtube,
            query,
            ctx.published_after,
            state.page_token,
        )
    except Exception as e:
        if is_quota_exceeded(e):
            quota_hit()
            stop_discovery("quota")
        else:
            # one failing ticker (bad query, network error...) is logged and skipped instead of
            # crashing the whole run; videos it already found are still processed.
            if not isinstance(e, (HttpError, ApiRetriesExhausted)):
                log.exception(
                    "%s: unexpected error during API search", ticker
                )  # others were logged by api_call
            _out.info(f"[{name}] {ticker}: API search failed ({api_error_reason(e)})")
            stop_discovery("error")
        return

    state.page_token = next_token
    state.pages += 1
    log.info(
        "search ok q=%s page=%d videos=%d more=%s elapsed=%.2fs",
        query,
        state.pages,
        len(ids),
        "yes" if next_token else "no",
        elapsed,
    )
    ctx.store.titles.update(titles)
    # the videos go to the transcript workers BEFORE the details call, so a quota error there
    # can't cost us the videos we just found
    new = register_videos(ctx, state, ids)
    log.info(
        "page done ticker=%s page=%d fetched=%d new=%d total=%d",
        ticker,
        state.pages,
        len(ids),
        new,
        len(ctx.video_tickers),
    )
    _out.info(
        f"[{name}] {ticker}: page {state.pages}, {len(ids)} videos "
        f"({len(state.video_ids)} total)"
    )

    new_ids = [id for id in ids if id not in state.metadata]
    if new_ids:
        try:
            details, elapsed = await api_call(
                f"videos.list ticker={ticker} ids={len(new_ids)}",
                fetch_details,
                youtube,
                new_ids,
            )
            for id in new_ids:
                item = details.get(id)
                if item is not None:  # None = video removed in between
                    state.metadata[id] = metadata_entry(item)
            log.info(
                "details ok ticker=%s ids=%d returned=%d elapsed=%.2fs",
                ticker,
                len(new_ids),
                len(details),
                elapsed,
            )
        except Exception as e:
            if is_quota_exceeded(e):
                quota_hit()
            elif not isinstance(e, (HttpError, ApiRetriesExhausted)):
                log.exception("%s: unexpected error fetching video details", ticker)

    if next_token and not ctx.quota_event.is_set() and not ctx.stop_event.is_set():
        ctx.discovery_queue.put_nowait(state)  # back of the line
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
                continue  # discovery is over: the remaining tickers are simply dropped
            await discover_one_page(name, state, youtube, ctx)
        finally:
            ctx.discovery_queue.task_done()


# ---- stage 2: transcripts ---------------------------------------------------------------


def refund_attempt(store, id):
    store.set_attempts(id, max(0, store.attempts(id) - 1))


def retry_later(state, id, ctx, reason):
    """
    A genuine failed attempt for this video (it was already counted when processing started).
    While tries are left the video goes to the back of the ticker's line; after MAX_TRIES
    it is given up on (is_finished() then returns True).
    """
    store = ctx.store
    attempt = store.attempts(id)
    if attempt < MAX_TRIES:
        store.note_retry(id)
        state.pending.append(id)
        log.warning("%s: %s attempt=%d/%d, will retry", id, reason, attempt, MAX_TRIES)
    else:
        log.error(
            "%s: %s; failed after all %d attempts, giving up", id, reason, MAX_TRIES
        )


def handle_ytdlp_error(state, id, error, ctx, stage):
    """
    What a failed yt_dlp call means for the video. The attempt was already counted when
    processing started, so:
      * rate limited -> YouTube's fault: give the attempt back and retry later
      * permanent    -> the video is gone/private/...: never try it again
      * anything else-> the counted attempt stands (retried while tries are left)
    """
    store = ctx.store
    attempt = store.attempts(id)
    detail = describe_error(error)
    if is_rate_limited(error):
        kind = (
            "bot check (cookies may be expired)"
            if "not a bot" in str(error)
            else "rate limited (429)"
        )
        ctx.rate.record_rate_limit()
        refund_attempt(store, id)
        store.note_retry(id)
        state.pending.append(id)
        log.warning(
            "%s: ytdlp %s %s attempt=%d/%d, retrying later: %s",
            id,
            stage,
            kind,
            attempt,
            MAX_TRIES,
            detail,
        )
    elif is_permanent_failure(error):
        store.mark_skipped(id)
        log.warning(
            "%s: ytdlp %s failed permanently, skipping video: %s", id, stage, detail
        )
    else:
        retry_later(state, id, ctx, f"ytdlp {stage} failed: {detail}")


# processes exactly ONE video and returns; transcript_worker() puts the ticker at the back
# of the queue afterwards, so tickers take turns, one video each (round robin).
# The yt_dlp requests inside ONE ticker are still done one at a time on purpose
async def process_next_video(state, ctx):
    store = ctx.store
    id = state.pending.popleft()

    # the check uses the shared store, so a video that matched several tickers is only done once
    if store.is_finished(id):
        log.info(
            "%s: skipping, already done or given up on (ticker=%s)", id, state.ticker
        )
        return
    if id in store.in_progress:
        log.info(
            "%s: skipping, another worker is already processing it (ticker=%s)",
            id,
            state.ticker,
        )
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
    store = ctx.store
    url = f"https://www.youtube.com/watch?v={id}"

    # Count this attempt NOW, so a video that crashes the whole process is still given up on
    # eventually. It is handed back if the attempt turns out to be YouTube's fault.
    store.set_attempts(id, store.attempts(id) + 1)

    # retrieve transcript metadata (cached in state so a retry after a 429 doesn't repeat it)
    if id not in state.transcript_metadata:
        start = time.monotonic()
        try:
            info = await asyncio.to_thread(_extract_info, url, state.ydl_common)
        except Exception as e:
            handle_ytdlp_error(state, id, e, ctx, "metadata")
            return
        if not info:
            retry_later(
                state,
                id,
                ctx,
                f"metadata lookup returned no info (unexpected structure) url={url}",
            )
            return

        manual = info.get("subtitles", {})
        english_manual = next(
            (lang for lang in manual if lang.lower().startswith("en")), None
        )
        automatic = info.get("automatic_captions", {})
        english_automatic = next(
            (lang for lang in automatic if lang.lower().startswith("en")), None
        )
        if english_manual:
            state.transcript_metadata[id] = ("manual", english_manual)
        elif english_automatic:
            state.transcript_metadata[id] = ("automatic", english_automatic)
        else:
            state.transcript_metadata[id] = ("audio", None)
        log.info(
            "ytdlp metadata ok video=%s ticker=%s captions=%s elapsed=%.1fs",
            id,
            state.ticker,
            state.transcript_metadata[id][0],
            time.monotonic() - start,
        )

    await interruptible_sleep(ctx.stop_event, random.uniform(4, 7))
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
            "no_warnings": True,
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
            log.warning("%s: could not remove stale file %s", id, old_path)

    start = time.monotonic()
    try:
        await asyncio.to_thread(_download, opts, url)
    except Exception as e:
        handle_ytdlp_error(state, id, e, ctx, "download")
        return
    log.info(
        "ytdlp download ok video=%s ticker=%s source=%s elapsed=%.1fs",
        id,
        state.ticker,
        src_type,
        time.monotonic() - start,
    )

    if src_type == "text":
        matches = sorted(glob.glob(f"{tmp}/{id}*.vtt"))
        if not matches:
            retry_later(
                state,
                id,
                ctx,
                f"download ok but no subtitle (.vtt) file was written url={url}",
            )
            return
        result = parse_vtt(matches[0])
    else:
        matches = sorted(glob.glob(f"{tmp}/{id}.*"))
        matches = [
            path
            for path in matches
            if path.endswith((".mp3", ".m4a", ".webm", ".opus", ".wav"))
        ]
        if not matches:
            retry_later(
                state, id, ctx, f"download ok but no audio file was written url={url}"
            )
            return
        start = time.monotonic()
        try:
            result, language, probability = await asyncio.to_thread(
                transcribe, matches[0]
            )
        except Exception as e:
            retry_later(
                state, id, ctx, f"whisper transcription failed: {describe_error(e)}"
            )
            return
        if result is None:
            # not English: drop the video for good (no transcript saved, never retried)
            store.mark_skipped(id)
            log.info(
                "%s: dropped, whisper detected language '%s' (p=%.2f), not English",
                id,
                language,
                probability,
            )
            return
        log.info(
            "whisper ok video=%s language=%s p=%.2f elapsed=%.1fs",
            id,
            language,
            probability,
            time.monotonic() - start,
        )

    if not result or not result.strip():
        # deterministic (no speech / empty captions), so retrying would not help
        store.mark_skipped(id)
        log.warning(
            "%s: transcript is empty after parsing (source=%s), skipping video url=%s",
            id,
            src_type,
            url,
        )
        return

    store.save_transcript(
        id, {"title": title, "source": src_type, "transcript": result}
    )
    if store.retries.get(id):
        log.warning("%s: succeeded after %d attempts", id, store.retries[id] + 1)
    log.info(
        "video done video=%s ticker=%s source=%s words=%d",
        id,
        state.ticker,
        src_type,
        len(result.split()),
    )
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
                break  # sentinel, or shutdown requested: leave remaining videos unprocessed

            await wait_if_paused(ctx.rate, ctx.stop_event)
            if ctx.stop_event.is_set():
                break

            try:
                await process_next_video(state, ctx)
            except Exception as e:
                log.exception(
                    "%s: transcript gathering failed (%d videos scored so far)",
                    state.ticker,
                    len(ctx.video_scores),
                )
                _out.info(f"[{name}] {state.ticker}: transcript gathering failed ({e})")

            if state.pending and not ctx.stop_event.is_set():
                ctx.ready_queue.put_nowait(state)  # back of the line
            elif not state.pending:
                # nothing left right now. If discovery is still adding videos, it puts the
                # ticker back in the line itself (schedule_ticker)
                state.scheduled = False
                if state.discovery_done:
                    log.info(
                        "transcripts finished ticker=%s new_this_run=%d",
                        state.ticker,
                        state.completed,
                    )
                    _out.info(
                        f"[{name}] {state.ticker}: transcripts finished "
                        f"({state.completed} new this run)"
                    )
        finally:
            ctx.ready_queue.task_done()


# ---- stage 3: scoring, one batch of finished videos at a time ---------------------------


def _score_texts_temp(texts):
    """Run score_texts on `texts` and return {text: score}.

    score_texts only reports results by writing a file, so the output goes to a
    temporary directory that is deleted as soon as the scores are read back
    (also on error).
    """
    with tempfile.TemporaryDirectory(prefix="yt_scores_") as tmp:
        path = os.path.join(tmp, "scores.jsonl")
        score_texts(texts, path, resume=False)
        return load_scores(path)


def video_score(chunks, scores):
    """
    A video's score: the average of its chunk scores weighted by chunk length in words.
    `scores` is {chunk_text: score}. Returns (score, chunks_scored), or (None, 0) if none of
    the chunks has a score.
    """
    total, weight, n = 0.0, 0, 0
    for chunk in chunks:
        score = scores.get(chunk)
        if score is None:  # e.g. a malformed line was skipped when reading the file
            continue
        w = len(chunk.split())
        total += score * w
        weight += w
        n += 1
    if n == 0:
        return None, 0
    return round(total / weight, 2), n


# Runs next to the transcript workers. Finished videos are collected, and every
# SCORE_BATCH_VIDEOS videos (and once more at the very end) their chunks go to ONE
# score_texts() call. Only one call ever runs at a time. The scores are recorded in
# ctx.results under every ticker the video was found under.
async def scoring_worker(ctx):
    batch = []

    async def flush():
        nonlocal batch
        ids, batch = batch, []
        by_video = {id: transcript_chunks(ctx.store, id) for id in ids}
        chunks = list(dict.fromkeys(c for cs in by_video.values() for c in cs))
        if not chunks:
            return
        _out.info(f"[scoring] {len(chunks)} chunk(s) from {len(ids)} video(s)...")
        start = time.monotonic()
        try:
            scores = await asyncio.to_thread(_score_texts_temp, chunks)
        except Exception as e:
            # nothing is kept on disk, so these videos simply get no score in this run
            log.exception(
                "scoring failed for a batch of %d videos (%d chunks); they get no score",
                len(ids),
                len(chunks),
            )
            _out.info(f"[scoring] batch failed ({e}); these videos will not be scored")
            return
        for id, vid_chunks in by_video.items():
            score, _n = video_score(vid_chunks, scores)
            if score is None:
                log.warning(
                    "%s: no chunk scores came back (%d chunks)", id, len(vid_chunks)
                )
                continue
            ctx.video_scores[id] = score
            for ticker in ctx.video_tickers.get(id, ()):
                ctx.results.setdefault(ticker, {})[id] = score
            ctx.store.mark_done(id)  # only now: transcribed AND scored
        log.info(
            "scored batch videos=%d chunks=%d elapsed=%.1fs",
            len(ids),
            len(chunks),
            time.monotonic() - start,
        )

    while True:
        id = await ctx.score_queue.get()
        if id is None:
            await flush()
            break
        batch.append(id)
        if len(batch) >= SCORE_BATCH_VIDEOS:
            await flush()


# ---- results file -----------------------------------------------------------------------
def build_report(results, tickers):
    """List of {ticker, average_sentiment, post_count, posts} for every queried ticker.

    Tickers with no videos are included with average_sentiment null. The keys are called
    "posts"/"post_count" (they hold videos here) so every source script has the same schema.
    """
    report = []
    for ticker in sorted(set(tickers)):
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
            "results written path=%s tickers=%d videos=%d",
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


# sets stop_event after the time limit. Everything else reacts to the event.
async def stop_after(minutes, stop_event):
    await asyncio.sleep(minutes * 60)
    log.info(
        "time limit of %g minute(s) reached, finishing videos in progress", minutes
    )
    _out.info(
        f"\nTime limit of {minutes:g} minute(s) reached. Finishing the videos currently "
        f"in progress, saving, then shutting down..."
    )
    stop_event.set()


# end-of-run report. Also makes it easy to see how evenly the work was shared.
def print_summary(states, stopped_early, quota_hit):
    notes = []
    if stopped_early:
        notes.append("stopped by time limit")
    if quota_hit:
        notes.append("API quota exhausted")
    _out.info("\nSummary" + (f" ({', '.join(notes)})" if notes else "") + ":")
    for s in states:
        _out.info(
            f"  {s.ticker}: {len(s.video_ids)} videos found over {s.pages} page(s), "
            f"{s.completed} transcribed this run, {len(s.pending)} video(s) left for next run"
        )


async def async_main(
    time_limit_minutes=None, days=7, output_path=OUTPUT_FILE, status_file=STATUS_FILE
):
    """
    Discovers videos (round robin over the tickers), transcribes them, and scores the
    transcripts in batches while the run is still going. At the end (also after an error or
    Ctrl+C) the per-ticker results are written to `output_path`. Returns that path, or None if
    no video could be scored.
    """
    if not os.getenv("YOUTUBE_API_KEY"):
        log.error("missing YOUTUBE_API_KEY, cannot start")
        raise SystemExit("Set YOUTUBE_API_KEY in your .env file.")

    ticker_map = (
        await fetch_ticker_map()
    )  # {company name: SYMBOL}; only the symbols are used
    tickers = list(dict.fromkeys(ticker_map.values())) if ticker_map else []
    if not tickers:
        log.error(
            "no tickers loaded: the ticker file is missing, unreadable, not a "
            "{company name: ticker} object, or empty"
        )
        raise SystemExit(
            "Could not find, read, or parse any tickers from the ticker source"
        )
    log.info("tickers loaded source=file count=%d", len(tickers))
    log.info("auth: YouTube API key and cookies file are present (values not logged)")
    _out.info(
        f"Searching for {len(tickers)} tickers: {', '.join(tickers)} "
        f"(videos from the last {days:g} day(s))"
    )

    results = {}  # {ticker: {video_id: score}}, written to the JSON file at the end
    video_scores = {}  # video id -> score, one entry per unique scored video
    run_start = time.monotonic()
    published_after = get_published_after(days)
    log.info(
        "run start tickers=%s days=%g published_after=%s time_limit_min=%s",
        tickers,
        days,
        published_after,
        time_limit_minutes,
    )

    store = Store(status_file)
    ctx = Context(
        store=store,
        rate=RateLimitState(BackoffConfig()),
        published_after=published_after,
        stop_event=asyncio.Event(),
        quota_event=asyncio.Event(),
        discovery_queue=asyncio.Queue(),
        ready_queue=asyncio.Queue(),
        score_queue=asyncio.Queue(),
        results=results,
        video_scores=video_scores,
    )

    # scratch space for yt_dlp (subtitle/audio files, cookie copies); always deleted at the end
    tmp_root = Path(tempfile.mkdtemp(prefix="yt_tmp_"))
    states = []
    timer_task = None

    try:
        for ticker in tickers:
            try:
                states.append(build_ticker_state(ticker, tmp_root))
            except Exception as e:
                log.exception("%s: setup failed, skipping this ticker", ticker)
                _out.info(f"{ticker}: setup failed ({e}); skipping this ticker")
        if not states:
            log.error("no ticker could be set up, nothing to do")
            return None

        for state in states:
            ctx.discovery_queue.put_nowait(state)

        # never start more API workers than there are tickers
        n_api = max(1, min(API_WORKERS, len(states)))
        n_transcript = max(1, TRANSCRIPT_WORKERS)

        if time_limit_minutes is not None:
            timer_task = asyncio.create_task(
                stop_after(time_limit_minutes, ctx.stop_event)
            )

        api_tasks = [
            asyncio.create_task(api_worker(f"api-{i + 1}", ctx)) for i in range(n_api)
        ]
        transcript_tasks = [
            asyncio.create_task(transcript_worker(f"transcript-{i + 1}", ctx))
            for i in range(n_transcript)
        ]
        scoring_task = asyncio.create_task(scoring_worker(ctx))

        # discovery: every ticker has had all its pages, or the quota / time limit ended it
        await ctx.discovery_queue.join()
        for _ in range(n_api):
            ctx.discovery_queue.put_nowait(None)
        await asyncio.gather(*api_tasks)
        if ctx.quota_event.is_set():
            _out.info(
                "\nDiscovery stopped (API quota). Transcribing everything found so far..."
            )

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
        await scoring_task
    except asyncio.CancelledError:
        # Ctrl+C: asyncio cancels the main task; the finally block below still saves the results
        log.warning("run interrupted after %d videos scored", len(video_scores))
        raise
    except Exception:
        log.exception("run aborted after %d videos scored", len(video_scores))
        raise
    finally:
        # results first, so a problem in the cleanup below can never cost us the scores
        write_results(output_path, results, tickers)
        if timer_task is not None:
            timer_task.cancel()
        for s in states:
            cleanup_ticker(s)
        if states:
            print_summary(states, ctx.stop_event.is_set(), ctx.quota_event.is_set())
        shutil.rmtree(tmp_root, ignore_errors=True)

    log.info(
        "run finished unique_videos=%d elapsed=%.1fs",
        len(video_scores),
        time.monotonic() - run_start,
    )
    if not video_scores:
        _out.info("\nNo transcripts available to score.")
        return None

    _out.info("")
    for ticker in sorted(results):
        scores = results[ticker]
        _out.info(
            f"  {ticker}: {round(sum(scores.values()) / len(scores), 2)} "
            f"(avg of {len(scores)} video(s))"
        )

    return str(output_path)


def run(
    time_limit_minutes=None,
    days=7,
    output_path=OUTPUT_FILE,
    log_file=LOG_FILE,
    status_file=STATUS_FILE,
):
    """
    Synchronous entry point for other scripts: fetch YouTube transcripts for every ticker
    in tickers.json, score them, write the results JSON to `output_path` and return that
    path (None if no video could be scored).
    """
    setup_logging(log_file)
    return asyncio.run(async_main(time_limit_minutes, days, output_path, status_file))


def main():
    parser = argparse.ArgumentParser(
        description="Download YouTube transcripts for every ticker in tickers.json and score their sentiment."
    )
    parser.add_argument(
        "--time",
        type=float,
        default=None,
        help="Stop automatically after this many minutes: in-progress videos are finished "
        "and saved, then the script exits (default: run until everything is done)",
    )

    parser.add_argument(
        "--days",
        type=float,
        default=7,
        help="Only consider videos published within this many days (default: 7)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(OUTPUT_FILE),
        help=f"JSON file for the sentiment results (default: {OUTPUT_FILE.name} next to this script; "
        f"overwritten each run)",
    )
    parser.add_argument(
        "--status-file",
        type=str,
        default=str(STATUS_FILE),
        help=f"JSON file that remembers which videos are done/skipped across runs (default: {STATUS_FILE})",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=str(LOG_FILE),
        help=f"Where to write the log (default: {LOG_FILE.name} next to this script)",
    )
    args = parser.parse_args()
    if args.time is not None and args.time <= 0:
        parser.error("--time must be greater than 0")
    if args.days <= 0:
        parser.error("--days must be greater than 0")

    setup_logging(args.log_file)

    # Fail early with a clear message instead of
    # starting a long run and crashing on the first ticker.
    if not Path(COOKIES_FILE).is_file():
        log.error("cookies file not found: %s", COOKIES_FILE)
        raise SystemExit(f"Cookies file not found: {COOKIES_FILE}")

    try:
        return run(args.time, args.days, args.output, args.log_file, args.status_file)
    except KeyboardInterrupt:
        log.info("run stopped by user")
        _out.info("Stopped.")


if __name__ == "__main__":
    main()
