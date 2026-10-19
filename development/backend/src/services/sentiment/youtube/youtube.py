import os
import json
import googleapiclient.discovery
import yt_dlp
from faster_whisper import WhisperModel
import glob
import random
import re
from dotenv import load_dotenv
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from yt_dlp.utils import DownloadError
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
STATUS_FILE = DATA_DIR/"status.json"
UTILS_DIR = BACKEND_DIR/"src"/"utils"
LOG_DIR = BACKEND_DIR/"logs"
LOG_DIR.mkdir(exist_ok=True)
TICKERS_FILE = "tickers.json"
COOKIES_FILE = "youtube_cookies.txt"
# scratch space for downloaded subtitle/audio files, one sub-folder per ticker.
TMP_DIR = DATA_DIR/"tmp"
API_WORKERS = 3          # tickers hitting the YouTube Data API at once
TRANSCRIPT_WORKERS = 1   # videos being downloaded/transcribed at once; keep low to avoid 429s


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



# googleapiclient clients (httplib2 underneath) are NOT thread-safe, so every API worker
# gets its own client instead of sharing one.
def build_youtube_client():
    return googleapiclient.discovery.build(
        "youtube",
        "v3",
        developerKey=os.getenv("YOUTUBE_API_KEY")
    )

class StopRequested(Exception):
    pass


# This function is synchronous/blocking; api_worker() runs it in a thread with
# asyncio.to_thread() so several tickers can query the API at the same time.
# takes stop_event and checks it before EVERY YouTube Data API request (each search page
# and each batch of video details).
def get_video_ids_and_metadata(keyword, logger, metadata_file, youtube, published_after, stop_event):
    """ given a keyword return list of objects from Youtube API that are related to the keyword """
    page_token = None
    results = []
    videoIds = []
    logger.info("started metadata gathering")

    while True:
        if stop_event.is_set():
            raise StopRequested()
        request = youtube.search().list(
            part="snippet",
            maxResults=50,
            order="date",
            publishedAfter=published_after,
            q=keyword,
            type="video",
            fields="nextPageToken, items(id/videoId)",
            pageToken=page_token
        )
        response = request.execute()
        results.extend(response.get("items",[]))

        page_token = response.get("nextPageToken")

        if not page_token:
            break

    for item in results:
        videoIds.append(item["id"]["videoId"])

    # get metadata from video IDs above
    metadata = load_json(metadata_file, {}) # dictionary with previously fetched metadata

    details: dict[str, dict] = {}
    for i in range(0, len(videoIds), 50):
        if stop_event.is_set():
            raise StopRequested()
        batchIds = videoIds[i:i+50]
        request = youtube.videos().list(
            part="statistics,snippet,contentDetails",
            fields="items(id,snippet/title,snippet/channelTitle,snippet/publishedAt,snippet/tags,statistics/viewCount,statistics/likeCount,contentDetails/duration)",
            id = ",".join(batchIds)
        )
        response = request.execute()

        for item in response.get("items",[]):
            details[item['id']] = item

    for videoId in videoIds:
        if videoId in metadata:
            continue

        item = details.get(videoId)
        # if video is removed between execution
        if item is None:
            continue

        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        contentDetails = item.get("contentDetails", {})

        metadata[videoId]={
            "title": snippet.get("title"),
            "channel": snippet.get("channelTitle"),
            "publishedDate": snippet.get("publishedAt"),
            "tags": snippet.get("tags",[]),
            "viewCount": int(statistics.get("viewCount",0)),
            "likeCount": int(statistics.get("likeCount",0)),
            "duration": contentDetails.get("duration", "PT0S")
        }
        logger.info(f"{videoId}: metadata success")

    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent = 4)

    return videoIds

def parse_vtt(path):
    """ function to parse the transcript files returned by yt-dlp """
    text = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.isdigit():
                continue
            line = line.strip()
            if not line or "-->" in line or "WEBVTT" in line:
                continue
            line = re.sub(r"<[^>]+>", "", line)
            text.append(line)
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
    """ uses whisper model to turn audio to text when no transcripts are available """
    with whisper_lock:
        segments, _ = model.transcribe(audio_path)
        return " ".join(seg.text for seg in segments)

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


# small blocking helpers that process_next_video() runs via asyncio.to_thread().
def _extract_info(url, ydl_opts):
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        return ydl.extract_info(url, download=False)

def _download(opts, url):
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([url])


# everything the transcript stage needs to remember about one ticker between turns.

@dataclass
class TickerState:
    ticker: str
    logger: logging.Logger
    transcripts_file: str
    data: dict                  # video metadata from the YouTube API
    transcripts: dict           # transcripts collected so far (saved after every video)
    tmp_dir: Path
    tmp: str                    # glob-escaped tmp_dir
    ydl_common: dict
    status: dict
    pending: deque = field(default_factory=deque)               # video IDs still to process
    transcript_metadata: dict = field(default_factory=dict)     # id -> (kind, language)
    completed: int = 0          # transcripts finished during this run


def build_ticker_state(ticker, videoIds, logger, transcripts_file, metadata_file):
    tmp_dir = TMP_DIR/safe_name(ticker)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # per-ticker copy of the cookies file (yt_dlp writes cookies back to the file it is given)
    ticker_cookies = tmp_dir/"_cookies.txt"
    shutil.copyfile(COOKIES_FILE, ticker_cookies)

    status = check_status(videoIds)
    pending = deque()
    for id in videoIds:
        current_status = status[id]
        if current_status == "done" or int(current_status) >= 5:
            logger.info(f"{id}: skipping, done or tries exceeded")
            continue
        pending.append(id)

    return TickerState(
        ticker=ticker,
        logger=logger,
        transcripts_file=transcripts_file,
        data=load_json(metadata_file, {}),
        transcripts=load_json(transcripts_file, {}),
        tmp_dir=tmp_dir,
        tmp=glob.escape(str(tmp_dir)),   # escaped so glob() treats the path literally
        ydl_common={**COMMON_YDL_OPTS, "cookiefile": str(ticker_cookies)},
        status=status,
        pending=pending,
    )


def cleanup_ticker(state):
    """ remove this ticker's private copy of the cookies file """
    (state.tmp_dir/"_cookies.txt").unlink(missing_ok=True)


# processes exactly ONE video and returns; transcript_worker() puts the ticker at the back
# of the queue afterwards, so tickers take turns, one video each (round robin).
# The yt_dlp requests inside ONE ticker are still done one at a time on purpose
async def process_next_video(state, rate, stop_event):
    logger = state.logger
    id = state.pending.popleft()
    url = f"https://www.youtube.com/watch?v={id}"
    result = None

    current_status = state.status[id]
    if current_status == "done" or int(current_status) >= 5:
        logger.info(f"{id}: skipping transcript, done or tries exceeded")
        return

    # retrieve transcript metadata (cached in state so a retry after a 429 doesn't repeat it)
    if id not in state.transcript_metadata:
        logger.info(f"{id}: processing metadata")
        try:
            info = await asyncio.to_thread(_extract_info, url, state.ydl_common)
            logger.info(f"{id}: yt_dlp metadata success")
        except Exception as e:
            logger.error(f"{id}: yt_dlp metadata failed")
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

    await interruptible_sleep(stop_event, random.uniform(4,7))
    if stop_event.is_set():
        state.pending.appendleft(id)
        return

    current_status = int(current_status) + 1
    state.status[id] = str(current_status)
    update_status(state.status)
    title = state.data.get(id,{}).get("title", "")
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
    except DownloadError as e:
        # only rate-limit errors (is_rate_limited) move us toward a pause. Other
        # download errors are just logged and the video is skipped.
        if is_rate_limited(e):
            rate.record_rate_limit(logger)
            logger.warning(f"{id}: Failed, Rate limited by API, retrying later")
            state.pending.append(id)
            return
        else:
            logger.error(f"{id}: Failed, download error: {e}")
            return
    except Exception as e:
        logger.error(f"{id}: Failed, unknown error: {e}")
        return

    if src_type == "text":
        matches = sorted(glob.glob(f"{tmp}/{id}*.vtt"))
        if not matches:
            logger.error(f"{id}: No text matches found")
            return
        path = matches[0]
        result = parse_vtt(path) if path else None
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
        path = matches[0]
        result = await asyncio.to_thread(transcribe, path) if path else None
        logger.info(f"{id}: Success, yt_dlp downloaded video audio and was transcibed")

    state.transcripts[id] = {
        "title": title,
        "source": src_type,
        "transcript": result
    }
    state.status[id] = "done"
    os.remove(path)
    logger.info(f"{id}: Done")
    rate.record_success()
    state.completed += 1

    # saved after EVERY video, so a shutdown never loses finished work
    update_status(state.status)
    with open(state.transcripts_file, "w", encoding="utf-8") as f:
        json.dump(state.transcripts, f, indent = 4, ensure_ascii=False)


# status.json is now shared by several tickers running concurrently. These two functions are
# plain synchronous functions that are called directly from coroutines (never from threads),
# and update_status() re-reads the file and writes it back with no `await` in between. Because
# asyncio only switches tasks at `await`, that read-modify-write can't be interleaved with
# another ticker's update, so no extra lock is needed. If you ever move them into
# asyncio.to_thread(), add a threading.Lock around update_status().
def check_status(video_ids):
    """ returns status state for each video ID to prevent unnecessary work """

    #TODO: this function works together with update_status and is currently being saved and fetched via JSON
    #      in the future probably migrate to a SQL or some other database

    results = {}
    cache = load_json(STATUS_FILE, {})

    for id in video_ids:
        results[id] = cache.get(id, "0")

    return results

def update_status(states):
    """ stores status state for each video ID to prevent unnecessary work """

    #TODO: this function works together with check_status and is currently being saved and fetched via JSON
    #      in the future probably migrate to a SQL or some other database
    status = load_json(STATUS_FILE, {})

    for state in states.keys():
        if state not in status.keys() or status[state] != states[state]:
            status[state] = states[state]

    with open(STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(status, f, indent = 4)


# Each worker owns one YouTube API client and pulls tickers off `ticker_queue` until it sees the
# None sentinel. The blocking API calls run in a thread, so N workers = N tickers hitting the
# YouTube Data API at once. Finished tickers are handed to the transcript workers through
# `ready_queue`, so transcripts for ticker A can download while ticker B is still fetching.
async def api_worker(name, ticker_queue, ready_queue, published_after, stop_event, states):
    youtube = await asyncio.to_thread(build_youtube_client)

    while True:
        if stop_event.is_set():
            break
        ticker = await ticker_queue.get()
        if ticker is None:
            ticker_queue.task_done()
            break
        if stop_event.is_set():
            ticker_queue.task_done()
            break

        logger = create_logger(ticker)
        metadata_file = f"{DATA_DIR}/{safe_name(ticker)}_metadata.json"
        transcripts_file = f"{DATA_DIR}/{safe_name(ticker)}_transcripts.json"
        try:
            print(f"[{name}] fetching API metadata for {ticker}")
            # quoted so YouTube searches for the exact phrase (same as the original script)
            videoIds = await asyncio.to_thread(
                get_video_ids_and_metadata,
                f'"{ticker}"', logger, metadata_file, youtube, published_after, stop_event
            )
            print(f"[{name}] {ticker}: {len(videoIds)} videos found")
            # instead of handing the whole ticker to a transcript worker to finish,
            # we register it in the shared round-robin queue.
            state = build_ticker_state(ticker, videoIds, logger, transcripts_file, metadata_file)
            states.append(state)
            if state.pending:
                ready_queue.put_nowait(state)
            else:
                print(f"[{name}] {ticker}: nothing new to transcribe")
                cleanup_ticker(state)
        except StopRequested:
            # time limit hit while this ticker's API fetch was running. Not an error;
            # the ticker is just left for the next run.
            logger.info(f"{ticker}: API metadata gathering stopped by time limit")
            print(f"[{name}] {ticker}: API gathering stopped by time limit")
        except Exception as e:
            # one failing ticker (bad query, API quota hit, network error...) is logged and
            # skipped instead of crashing the whole run and every other ticker with it.
            logger.error(f"{ticker}: API metadata gathering failed: {e}")
            print(f"[{name}] {ticker}: API gathering failed ({e})")
        finally:
            ticker_queue.task_done()


# transcript_worker takes ONE ticker from the queue, processes ONE of its videos,
# and puts the ticker back at the END of the queue if it still has videos left. Because the
# queue is first-in-first-out, every ticker gets a turn before any ticker gets a second one.
# A ticker is only ever in the queue (or held by one worker), never both, so two workers can't
# process the same ticker at once.
async def transcript_worker(name, ready_queue, rate, stop_event):
    while True:
        state = await ready_queue.get()
        try:
            if state is None or stop_event.is_set():
                break   # sentinel, or shutdown requested: leave remaining videos for the next run

            await wait_if_paused(rate, stop_event)
            if stop_event.is_set():
                break

            try:
                await process_next_video(state, rate, stop_event)
            except Exception as e:
                state.logger.error(f"{state.ticker}: transcript gathering failed: {e}")
                print(f"[{name}] {state.ticker}: transcript gathering failed ({e})")

            if state.pending and not stop_event.is_set():
                ready_queue.put_nowait(state)      # back of the line
            elif not state.pending:
                print(f"[{name}] {state.ticker}: transcripts finished "
                      f"({state.completed} new this run)")
                cleanup_ticker(state)
        finally:
            ready_queue.task_done()


# sets stop_event after the time limit. Everything else reacts to the event.
async def stop_after(minutes, stop_event):
    await asyncio.sleep(minutes * 60)
    print(f"\nTime limit of {minutes:g} minute(s) reached. Finishing the videos currently "
          f"in progress, saving, then shutting down...")
    stop_event.set()


# end-of-run report. Also makes it easy to see how evenly the work was shared.
def print_summary(states, stopped_early):
    print("\nSummary" + (" (stopped by time limit)" if stopped_early else "") + ":")
    for s in states:
        print(f"  {s.ticker}: {s.completed} transcribed this run, "
              f"{len(s.pending)} video(s) left for next run")


async def async_main(time_limit_minutes=None, days=7):
    tickers = load_tickers(TICKERS_FILE)
    print(f"Searching for {len(tickers)} tickers: {', '.join(tickers)} "
          f"(videos from the last {days:g} day(s))")

    published_after = get_published_after(days)
    ticker_queue = asyncio.Queue()
    ready_queue = asyncio.Queue()     # tickers that have videos waiting (the round-robin line)
    stop_event = asyncio.Event()
    rate = RateLimitState(BackoffConfig())
    states = []

    for ticker in tickers:
        ticker_queue.put_nowait(ticker)

    # never start more API workers than there are tickers
    n_api = max(1, min(API_WORKERS, len(tickers)))
    n_transcript = max(1, TRANSCRIPT_WORKERS)

    # one stop sentinel per worker
    for _ in range(n_api):
        ticker_queue.put_nowait(None)

    timer_task = None
    if time_limit_minutes is not None:
        timer_task = asyncio.create_task(stop_after(time_limit_minutes, stop_event))

    api_tasks = [
        asyncio.create_task(
            api_worker(f"api-{i+1}", ticker_queue, ready_queue, published_after, stop_event, states)
        )
        for i in range(n_api)
    ]
    transcript_tasks = [
        asyncio.create_task(
            transcript_worker(f"transcript-{i+1}", ready_queue, rate, stop_event)
        )
        for i in range(n_transcript)
    ]

    try:
        # when every API worker is done, nothing new can enter ready_queue
        await asyncio.gather(*api_tasks)

        # wait until every ticker's videos are finished (queue.join()) OR the time
        # limit fires, whichever comes first, then tell the transcript workers to stop.
        join_task = asyncio.ensure_future(ready_queue.join())
        stop_task = asyncio.ensure_future(stop_event.wait())
        await asyncio.wait({join_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        join_task.cancel()
        stop_task.cancel()

        for _ in range(n_transcript):
            ready_queue.put_nowait(None)
        # workers finish the video they are on (it can't be interrupted mid-download), save, exit
        await asyncio.gather(*transcript_tasks)
    finally:
        if timer_task is not None:
            timer_task.cancel()
        for s in states:
            cleanup_ticker(s)
        print_summary(states, stop_event.is_set())


def main():
    parser = argparse.ArgumentParser(
        description="Download YouTube transcripts for every ticker in tickers.json."
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
    args = parser.parse_args()
    if args.time is not None and args.time <= 0:
        parser.error("--time must be greater than 0")
    if args.days <= 0:
        parser.error("--days must be greater than 0")

    # Fail early with a clear message instead of
    # starting a long run and crashing on the first ticker.
    if not Path(COOKIES_FILE).is_file():
        raise SystemExit(f"Cookies file not found: {COOKIES_FILE}")

    asyncio.run(async_main(args.time, args.days))

    return

if __name__ == "__main__":
    main()