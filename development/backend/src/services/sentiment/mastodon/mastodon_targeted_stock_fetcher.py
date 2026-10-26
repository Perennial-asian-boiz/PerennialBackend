"""
Pull posts from a Mastodon account using the Mastodon REST API.

Default account: @stocks@mastodon.social

Flow:
  1. GET /api/v1/accounts/lookup?acct=<user>   -> resolve the numeric account ID
  2. GET /api/v1/accounts/:id/statuses         -> page backwards with max_id

Usage:
    python pull_mastodon_posts.py
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import requests

# Make `src.utils` importable no matter where/how this file is launched.
# File lives at backend/src/services/sentiment/mastodon/, so backend/ is parents[4].
BACKEND_DIR = Path(__file__).resolve().parents[4]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from src.utils.console import stderr_logger  # noqa: E402

_err = stderr_logger()  # console lines (print() is banned in src/)

PAGE_SIZE = 40  # Mastodon's maximum per request


class _HTMLToText(HTMLParser):
    """Minimal HTML -> plain text (keeps paragraph and line breaks)."""

    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag == "br":
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag == "p":
            self.parts.append("\n\n")

    def handle_data(self, data):
        self.parts.append(data)


def html_to_text(html):
    parser = _HTMLToText()
    parser.feed(html or "")
    return "".join(parser.parts).strip()


def parse_acct(acct):
    """'@stocks@mastodon.social' -> ('stocks', 'mastodon.social')"""
    parts = acct.lstrip("@").split("@")
    if len(parts) != 2 or not all(parts):
        sys.exit(f"Invalid account '{acct}'. Expected the form @user@server.tld")
    return parts[0], parts[1]


def parse_since(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        sys.exit(f"Invalid --since date '{value}'. Use YYYY-MM-DD or ISO 8601.")
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def api_get(session, url, params=None, max_retries=5):
    """GET with rate-limit handling. Returns parsed JSON."""
    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=20)
        except requests.RequestException as exc:
            wait = 2**attempt
            _err.info(f"Network error ({exc}); retrying in {wait}s...")
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            reset = resp.headers.get("X-RateLimit-Reset")
            wait = 60
            if reset:
                try:
                    reset_dt = datetime.fromisoformat(reset.replace("Z", "+00:00"))
                    wait = max(
                        1, (reset_dt - datetime.now(timezone.utc)).total_seconds() + 1
                    )
                except ValueError:
                    pass
            _err.info(f"Rate limited; sleeping {wait:.0f}s...")
            time.sleep(min(wait, 900))
            continue

        if resp.status_code >= 500:
            time.sleep(2**attempt)
            continue

        if resp.status_code == 404:
            sys.exit(f"Not found: {url}")
        if resp.status_code in (401, 403):
            sys.exit(
                f"Access denied ({resp.status_code}). This server may require a token: "
                "set MASTODON_TOKEN."
            )
        resp.raise_for_status()
        return resp.json()

    sys.exit(f"Giving up on {url} after {max_retries} attempts.")


def simplify(status):
    """Flatten a status into the fields most people actually want."""
    reblog = status.get("reblog")
    source = reblog or status
    return {
        "id": status["id"],
        "created_at": status["created_at"],
        "url": status.get("url"),
        "is_reblog": reblog is not None,
        "reblogged_from": source["account"]["acct"] if reblog else None,
        "text": html_to_text(source.get("content")),
        "html": source.get("content"),
        "spoiler_text": source.get("spoiler_text") or None,
        "language": source.get("language"),
        "sensitive": source.get("sensitive"),
        "in_reply_to_id": status.get("in_reply_to_id"),
        "replies_count": source.get("replies_count"),
        "reblogs_count": source.get("reblogs_count"),
        "favourites_count": source.get("favourites_count"),
        "tags": [t["name"] for t in source.get("tags", [])],
        "media": [
            {
                "type": m.get("type"),
                "url": m.get("url"),
                "description": m.get("description"),
            }
            for m in source.get("media_attachments", [])
        ],
    }


def fetch_posts(
    session, base, account_id, max_posts, since, exclude_replies, exclude_reblogs
):
    url = f"{base}/api/v1/accounts/{account_id}/statuses"
    params = {
        "limit": PAGE_SIZE,
        "exclude_replies": str(exclude_replies).lower(),
        "exclude_reblogs": str(exclude_reblogs).lower(),
    }
    posts = []

    while True:
        page = api_get(session, url, params)
        if not page:
            break

        for status in page:
            created = datetime.fromisoformat(
                status["created_at"].replace("Z", "+00:00")
            )
            # Pinned posts can appear out of order, but the main timeline is newest-first,
            # so once we pass the cutoff we can stop.
            if since and created < since:
                return posts
            posts.append(simplify(status))
            if max_posts and len(posts) >= max_posts:
                return posts

        params["max_id"] = page[-1]["id"]  # page backwards through older posts
        _err.info(f"\rFetched {len(posts)} posts...", extra={"end": ""})

    return posts


def main():
    ap = argparse.ArgumentParser(description="Pull posts from a Mastodon account.")
    ap.add_argument("-a", "--account", default="@stocks@mastodon.social")
    ap.add_argument(
        "-n",
        "--max-posts",
        type=int,
        default=100,
        help="max posts to fetch (0 = everything available; default: 100)",
    )
    ap.add_argument("--since", help="only posts on/after this date (YYYY-MM-DD)")
    ap.add_argument("--no-replies", action="store_true", help="exclude replies")
    ap.add_argument("--no-reblogs", action="store_true", help="exclude boosts")
    ap.add_argument("-o", "--output", default="mastodon_targeted_posts.json")
    args = ap.parse_args()

    user, server = parse_acct(args.account)
    base = f"https://{server}"
    since = parse_since(args.since)

    session = requests.Session()
    session.headers["User-Agent"] = "mastodon-post-puller/1.0"
    token = os.environ.get("MASTODON_TOKEN")
    if token:
        session.headers["Authorization"] = f"Bearer {token}"

    account = api_get(session, f"{base}/api/v1/accounts/lookup", {"acct": user})
    _err.info(
        f"Found {account['acct']}@{server} (id={account['id']}, "
        f"{account['statuses_count']} total posts)"
    )

    posts = fetch_posts(
        session,
        base,
        account["id"],
        args.max_posts,
        since,
        args.no_replies,
        args.no_reblogs,
    )
    _err.info("")

    output = {
        "account": f"@{user}@{server}",
        "account_id": account["id"],
        "display_name": account.get("display_name"),
        "followers_count": account.get("followers_count"),
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "count": len(posts),
        "posts": posts,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    _err.info(f"Wrote {len(posts)} posts to {args.output}")


if __name__ == "__main__":
    main()
