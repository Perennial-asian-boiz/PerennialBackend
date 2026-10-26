"""
Single entry point for the sentiment pipeline.

Order:
    1. mastodon_server_crawler   runs alone first (refreshes mastodon/mastodon_servers.json)
    2. bluesky, mastodon_stock_fetcher, reddit, youtube
                                 run at the same time, each in its own process

Every script runs as a separate subprocess (`python -m sentiment.<folder>.<file>`)
with its own folder as the working directory, so files it reads/writes by relative path
(cookie files, mastodon_servers.json) live next to it. backend/ is put on PYTHONPATH so the
shared `src.utils.fetch_tickers` and `sentiment.sentiment_scorer` imports work.
Each script keeps writing its own results JSON and its own log file, exactly as when it is
run on its own.

Options (both optional; passed unchanged to the 4 source scripts, not to the crawler):
    --days N   only keep data from the last N days         (default: each script's own, 7)
    --time T   minutes; after T minutes the 4 scripts stop making requests, then finish
               scoring and saving what they already have.  (default: no limit)
               The clock starts when the 4 scripts start, i.e. after the crawler is done.

Failures:
    One script failing never stops the others. If the crawler fails, the Mastodon fetcher
    uses the existing mastodon_servers.json from an earlier run; if there is none, it is
    skipped. A summary is printed at the end and the exit code is 1 if anything failed.

Console output of all scripts is streamed live, each line prefixed with the script name.
Ctrl+C reaches every script (they save their partial results) and the runner waits for
them to exit.

Run (from backend/):
    python -m sentiment.sentiment_manager (default)
    python -m sentiment.sentiment_manager --days 2 --time 7 (quick test)

NOTE:
 - make sure tickers.json is in the sentiment directory
 - before running ensure fresh reddit_cookies and youtube_cookies
"""

import argparse
import asyncio
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from src.utils.console import stdout_logger

_out = stdout_logger()  # console lines (print() is banned in src/)

SENTIMENT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = SENTIMENT_DIR.parent  # sentiment -> backend
PACKAGE = "sentiment"

GRACE_SECONDS = (
    60  # after Ctrl+C: how long a script may take to save before it is terminated
)


@dataclass(frozen=True)
class Script:
    name: str  # label used in the console prefix and the summary
    folder: str  # sub-folder of sentiment/ (also the working directory)
    module: (
        str  # file name without .py; rename here if your files are named differently
    )

    @property
    def dotted(self):
        return f"{PACKAGE}.{self.folder}.{self.module}"

    @property
    def cwd(self):
        return SENTIMENT_DIR / self.folder


CRAWLER = Script("mastodon-crawler", "mastodon", "mastodon_server_crawler")
MASTODON = Script("mastodon", "mastodon", "mastodon_stock_fetcher")
SOURCES = (
    Script("bluesky", "bluesky", "bluesky"),
    MASTODON,
    # Script("reddit", "reddit", "reddit"),
    Script("youtube", "youtube", "youtube"),
)
SERVERS_FILE = (
    MASTODON.cwd / "mastodon_servers.json"
)  # written by the crawler, read by the fetcher

WIDTH = max(len(s.name) for s in (CRAWLER, *SOURCES))


@dataclass
class Result:
    name: str
    status: str  # "ok", "failed", "skipped", "stopped" (Ctrl+C)
    seconds: float = 0.0
    note: str = ""


# ---- console ------------------------------------------------------------------------------
def say(name, text):
    _out.info(f"[{name:<{WIDTH}}] {text}")


def fmt_secs(seconds):
    m, s = divmod(int(round(seconds)), 60)
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m {s:02d}s" if h else f"{m}m {s:02d}s"


def fmt_num(x):
    """1.0 -> '1', 2.5 -> '2.5' (what the child's argparse float() gets)."""
    return f"{x:g}"


async def pump(stream, name):
    """Forward a child's output line by line with a name prefix. Splits on \\r as well as
    \\n, so progress bars that redraw a line still show up and can't grow without bound."""
    buf = b""
    while True:
        chunk = await stream.read(65536)
        if not chunk:
            break
        buf += chunk
        *lines, buf = re.split(rb"\r\n|\r|\n", buf)
        for line in lines:
            text = line.decode("utf-8", "replace").rstrip()
            if text:
                say(name, text)
    text = buf.decode("utf-8", "replace").rstrip()
    if text:
        say(name, text)


# ---- running one script -------------------------------------------------------------------
def child_env():
    env = os.environ.copy()
    paths = [str(BACKEND_DIR)] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])
    env["PYTHONPATH"] = os.pathsep.join(paths)
    env["PYTHONUNBUFFERED"] = "1"  # stream output live instead of in 8 KB blocks
    env["PYTHONIOENCODING"] = (
        "utf-8"  # so the runner can decode every line the same way
    )
    return env


async def stop_child(proc, name):
    """After Ctrl+C: the child got the interrupt too and is saving. Give it time, then force."""
    if proc.returncode is not None:
        return
    say(name, f"waiting up to {GRACE_SECONDS}s for it to save and exit...")
    try:
        await asyncio.wait_for(proc.wait(), GRACE_SECONDS)
        return
    except asyncio.TimeoutError:
        pass
    say(name, "still running, terminating")
    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), 10)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()


async def run_script(script, args=()):
    cmd = [sys.executable, "-m", script.dotted, *args]
    start = time.monotonic()
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=script.cwd,
            env=child_env(),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except OSError as e:
        say(script.name, f"could not start: {e}")
        return Result(script.name, "failed", note=f"could not start: {e}")

    say(script.name, "started" + (f" ({' '.join(args)})" if args else ""))
    pump_task = asyncio.create_task(pump(proc.stdout, script.name))
    try:
        code = await proc.wait()
    except asyncio.CancelledError:
        # Ctrl+C. Not re-raised: the other scripts must get the same chance to save,
        # and asyncio.gather would stop waiting for them at the first cancelled task.
        await stop_child(proc, script.name)
        try:
            await asyncio.wait_for(pump_task, 5)
        except asyncio.TimeoutError:
            pump_task.cancel()
        return Result(
            script.name,
            "stopped",
            time.monotonic() - start,
            f"interrupted, exit code {proc.returncode}",
        )
    try:
        await asyncio.wait_for(pump_task, 10)  # flush the last lines
    except asyncio.TimeoutError:
        pump_task.cancel()

    elapsed = time.monotonic() - start
    if code == 0:
        say(script.name, f"finished in {fmt_secs(elapsed)}")
        return Result(script.name, "ok", elapsed)
    say(script.name, f"FAILED with exit code {code} after {fmt_secs(elapsed)}")
    return Result(script.name, "failed", elapsed, f"exit code {code}")


# ---- whole pipeline -----------------------------------------------------------------------
async def run_all(days, minutes):
    run_start = time.monotonic()
    results = []

    # 1. the crawler, alone
    crawl = await run_script(CRAWLER)
    results.append(crawl)
    if crawl.status == "stopped":
        print_summary(results, time.monotonic() - run_start)
        return 130

    sources = list(SOURCES)
    if crawl.status != "ok":
        if SERVERS_FILE.is_file():
            when = datetime.fromtimestamp(SERVERS_FILE.stat().st_mtime).strftime(
                "%Y-%m-%d %H:%M"
            )
            say(
                MASTODON.name,
                f"crawler failed; using the existing server list from {when}",
            )
        else:
            say(
                MASTODON.name,
                "crawler failed and there is no existing server list; skipping",
            )
            sources.remove(MASTODON)
            results.append(
                Result(MASTODON.name, "skipped", note="no mastodon_servers.json")
            )

    # 2. the source scripts, in parallel, all with the same --days / --time
    args = []
    if days is not None:
        args += ["--days", fmt_num(days)]
    if minutes is not None:
        args += ["--time", fmt_num(minutes)]
    tasks = [asyncio.create_task(run_script(s, args)) for s in sources]
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        # runs after every script has exited, also after Ctrl+C
        for s, t in zip(sources, tasks):
            if t.done() and not t.cancelled() and isinstance(t.result(), Result):
                results.append(t.result())
            elif t.done() and not t.cancelled():
                results.append(
                    Result(s.name, "failed", note=f"runner error: {t.result()!r}")
                )
        print_summary(results, time.monotonic() - run_start)

    if any(r.status == "stopped" for r in results):
        return 130
    return 0 if all(r.status == "ok" for r in results) else 1


def print_summary(results, total):
    order = [CRAWLER.name] + [s.name for s in SOURCES]
    results = sorted(results, key=lambda r: order.index(r.name))
    _out.info(f"\nSummary ({fmt_secs(total)} total)")
    for r in results:
        took = fmt_secs(r.seconds) if r.seconds else "-"
        _out.info(f"  {r.name:<{WIDTH}}  {r.status:<7}  {took:>11}  {r.note}".rstrip())


def positive(value):
    x = float(value)
    if x <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return x


def main():
    ap = argparse.ArgumentParser(
        description="Run the Mastodon server crawler, then the Bluesky, Mastodon, Reddit and "
        "YouTube sentiment scripts in parallel."
    )
    ap.add_argument(
        "--days",
        type=positive,
        default=None,
        help="Only keep data from the last N days (default: each script's own, 7)",
    )
    ap.add_argument(
        "--time",
        type=positive,
        default=None,
        metavar="T",
        help="Minutes. After T minutes the scripts stop making requests, then finish "
        "scoring and saving. Starts after the crawler. (default: no limit)",
    )
    args = ap.parse_args()

    try:
        sys.stdout.reconfigure(
            errors="replace"
        )  # never crash on a character the console can't show
    except AttributeError:
        pass

    try:
        code = asyncio.run(run_all(args.days, args.time))
    except KeyboardInterrupt:
        _out.info("\nStopped.")
        code = 130
    sys.exit(code)


if __name__ == "__main__":
    main()
