"""
Discover Mastodon servers (e.g. mastodon.social) and write them to mastodon_servers.json.

How it works
------------
1. Seeds: the joinmastodon.org server directory plus a few well-known servers.
2. Crawl:  every server exposes the domains it federates with at /api/v1/instance/peers.
           Those peers are enqueued (breadth-first, up to --depth hops).
3. Verify: each candidate domain is checked with ONE request to /nodeinfo/2.0
           (a fixed path in Mastodon and its forks). Only domains whose software
           name is "mastodon" are kept, which filters out Pleroma, Misskey,
           Lemmy, PeerTube, dead domains, etc.

Everything runs on a single asyncio worker pool sharing one aiohttp connection pool.

Discovery is hard-stopped after TIME_LIMIT seconds (60). Whatever has been found by
then is written to mastodon_servers.json. The script takes no arguments; edit the
settings block below to change its behaviour.

Usage:         python find_mastodon_servers.py
"""

import asyncio
import json
import os
import re
import signal
import sys
import tempfile
import time
from datetime import datetime, timezone
import aiohttp

# ---------------------------------------------------------------------------
# Settings (edit here; the script takes no command-line arguments)
# ---------------------------------------------------------------------------
OUTPUT_FILE = "mastodon_servers.json"
CONCURRENCY = 300        # simultaneous workers / connections
MAX_DEPTH = 1            # peer-crawl hops: 0 = seeds only, 1 = seeds' peers, 2+ = deeper
MAX_SERVERS = 0          # stop after this many servers (0 = no limit)
TIME_LIMIT = 60          # hard stop in seconds (0 = no limit)
SOFTWARE = {"mastodon"}  # nodeinfo software names to accept
EXTRA_SEEDS = ()         # additional seed domains, e.g. ("example.social",)
# ---------------------------------------------------------------------------

SEED_API_URLS = (
    "https://api.joinmastodon.org/v1/servers",
    "https://api.joinmastodon.org/servers",
)
FALLBACK_SEEDS = (
    "mastodon.social",
    "mastodon.online",
    "mstdn.social",
    "fosstodon.org",
    "hachyderm.io",
    "infosec.exchange",
)

USER_AGENT = "mastodon-server-discovery/1.0 (+async crawler)"
MAX_NODEINFO_BYTES = 1 << 20      # 1 MiB
MAX_PEERS_BYTES = 64 << 20        # 64 MiB (peer lists of big servers are large)

DOMAIN_RE = re.compile(
    r"^(?=.{4,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9-]{2,63}$"
)
BAD_SUFFIXES = (
    ".onion", ".i2p", ".local", ".localhost", ".internal",
    ".test", ".invalid", ".example", ".lan", ".home.arpa",
)


def normalize_domain(raw):
    """Return a clean lowercase ASCII domain, or None if it isn't a usable hostname."""
    if not isinstance(raw, str):
        return None
    d = raw.strip().lower()
    d = re.sub(r"^[a-z]+://", "", d).split("/")[0].split("?")[0].rstrip(".")
    if not d or "*" in d or ":" in d or "@" in d or " " in d:
        return None
    try:
        d = d.encode("idna").decode("ascii")  # unicode domains -> punycode
    except UnicodeError:
        return None
    if not DOMAIN_RE.match(d) or d.endswith(BAD_SUFFIXES) or d.rsplit(".", 1)[-1].isdigit():
        return None
    return d


async def get_json(session, url, max_bytes, expect_host=None):
    """GET url and parse JSON. Returns None on any failure (dead host, non-200, too big...)."""
    try:
        async with session.get(url, allow_redirects=True, max_redirects=3) as resp:
            if resp.status != 200:
                return None
            # A redirect to another host means this domain isn't the one answering.
            if expect_host and resp.url.host and resp.url.host.lower() != expect_host:
                return None
            if resp.content_length and resp.content_length > max_bytes:
                return None
            chunks, size = [], 0
            async for chunk in resp.content.iter_chunked(65536):
                size += len(chunk)
                if size > max_bytes:
                    return None
                chunks.append(chunk)
            return json.loads(b"".join(chunks))
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, OSError):
        return None


async def fetch_seeds(session):
    seeds = set(EXTRA_SEEDS) | set(FALLBACK_SEEDS)
    for url in SEED_API_URLS:
        data = await get_json(session, url, 16 << 20)
        if isinstance(data, list):
            seeds.update(s.get("domain") for s in data if isinstance(s, dict))
            break
    return seeds


class Crawler:
    def __init__(self, session, max_depth, max_servers, software):
        self.session = session
        self.max_depth = max_depth
        self.max_servers = max_servers
        self.software = software
        self.queue = asyncio.Queue()
        self.seen = set()
        self.results = {}
        self.checked = 0
        self.stop = asyncio.Event()

    def enqueue(self, raw, depth):
        domain = normalize_domain(raw)
        if domain and domain not in self.seen:
            self.seen.add(domain)
            self.queue.put_nowait((domain, depth))

    async def check(self, domain):
        """One request: is this domain running Mastodon? Returns info dict or None."""
        data = await get_json(
            self.session, f"https://{domain}/nodeinfo/2.0", MAX_NODEINFO_BYTES, domain
        )
        if not isinstance(data, dict):
            return None
        sw = data.get("software")
        if not isinstance(sw, dict) or str(sw.get("name", "")).lower() not in self.software:
            return None
        users = (data.get("usage") or {}).get("users") or {}
        return {
            "domain": domain,
            "software": str(sw.get("name")).lower(),
            "version": sw.get("version"),
            "open_registrations": data.get("openRegistrations"),
            "total_users": users.get("total"),
            "active_month": users.get("activeMonth"),
        }

    async def worker(self):
        while True:
            domain, depth = await self.queue.get()
            try:
                if self.stop.is_set():
                    continue  # drain the queue quickly after stop/limit
                info = await self.check(domain)
                self.checked += 1
                if info:
                    self.results[domain] = info
                    if self.max_servers and len(self.results) >= self.max_servers:
                        self.stop.set()
                        continue
                    if depth < self.max_depth:
                        peers = await get_json(
                            self.session,
                            f"https://{domain}/api/v1/instance/peers",
                            MAX_PEERS_BYTES,
                            domain,
                        )
                        if isinstance(peers, list):
                            for p in peers:
                                self.enqueue(p, depth + 1)
            except Exception:
                pass  # one bad domain must never kill a worker
            finally:
                self.queue.task_done()

    async def progress(self, interval=2.0):
        start = time.monotonic()
        while True:
            await asyncio.sleep(interval)
            print(
                f"\r[{time.monotonic() - start:6.0f}s] found={len(self.results):<6} "
                f"checked={self.checked:<7} queued={self.queue.qsize():<7}",
                end="", file=sys.stderr, flush=True,
            )


def write_json_atomic(path, results):
    servers = sorted(results.values(), key=lambda s: s["domain"])
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "count": len(servers),
        "servers": servers,
    }
    directory = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)  # never leaves a half-written file


async def main():
    loop = asyncio.get_running_loop()
    deadline = loop.time() + TIME_LIMIT if TIME_LIMIT else None

    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY,
        limit_per_host=4,
        ttl_dns_cache=600,
        use_dns_cache=True,
        enable_cleanup_closed=True,
    )
    timeout = aiohttp.ClientTimeout(total=30, connect=5, sock_connect=5, sock_read=10)
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout, headers=headers
    ) as session:
        crawler = Crawler(session, MAX_DEPTH, MAX_SERVERS, SOFTWARE)

        # Ctrl-C stops gracefully and still writes what has been found so far.
        try:
            loop.add_signal_handler(signal.SIGINT, crawler.stop.set)
        except (NotImplementedError, RuntimeError):
            pass  # e.g. Windows

        print("Fetching seed servers...", file=sys.stderr)
        for seed in await fetch_seeds(session):
            crawler.enqueue(seed, 0)
        print(f"Seeded {crawler.queue.qsize()} domains; crawling with "
              f"{CONCURRENCY} workers (depth={MAX_DEPTH})", file=sys.stderr)

        workers = [asyncio.create_task(crawler.worker()) for _ in range(CONCURRENCY)]
        ticker = asyncio.create_task(crawler.progress())
        remaining = max(0.0, deadline - loop.time()) if deadline else None
        try:
            await asyncio.wait_for(crawler.queue.join(), timeout=remaining)
        except asyncio.TimeoutError:
            print(f"\nTime limit of {TIME_LIMIT}s reached; stopping discovery.",
                  file=sys.stderr)
        finally:
            crawler.stop.set()
            ticker.cancel()
            for w in workers:
                w.cancel()
            await asyncio.gather(ticker, *workers, return_exceptions=True)

    print(file=sys.stderr)
    write_json_atomic(OUTPUT_FILE, crawler.results)
    print(f"Wrote {len(crawler.results)} Mastodon servers to {OUTPUT_FILE} "
          f"({crawler.checked} domains checked)", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())