"""
Fetch recent Yahoo Finance news for a list of tickers.

dependencies: yfinance

Usage:
    python yahoo_news.py 
"""
import sys
from datetime import datetime, timedelta, timezone
import asyncio
import yfinance as yf
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2])) # insert src dir in path so utils dir can be accessed
from utils.fetch_tickers import fetch_tickers


def _to_utc(value) -> datetime | None:
    """
    Convert an ISO-8601 string, epoch seconds/milliseconds or datetime into a timezone-aware UTC datetime. 
    Returns None if the value is missing or unparseable. Naive values are assumed to already be UTC.
    """
    if value is None or value == "":
        return None
    try:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, (int, float)):
            ts = float(value)
            if ts > 1e11:  # looks like milliseconds
                ts /= 1000
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        else:
            s = str(value).strip()
            if s.isdigit():
                return _to_utc(int(s))
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, OverflowError, OSError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize(item: dict) -> dict | None:
    """
    yfinance has returned news in two shapes over time:
      new: {"id": ..., "content": {"title", "pubDate", "canonicalUrl": {"url"}, "provider": {"displayName"}, ...}}
      old: {"uuid": ..., "title", "link", "publisher", "providerPublishTime"}
    This flattens both into one structure. Fields are looked up under item["content"]
    first and then at the top level, so Search results in either shape work.
    """
    if not isinstance(item, dict):
        return None
    content = item.get("content")
    if not isinstance(content, dict):
        content = item

    published = (
        _to_utc(content.get("pubDate"))
        or _to_utc(content.get("displayTime"))
        or _to_utc(content.get("providerPublishTime"))
        or _to_utc(item.get("providerPublishTime"))
    )
    if published is None:
        return None  # can't apply the date filter without a timestamp

    url = (
        (content.get("canonicalUrl") or {}).get("url")
        or (content.get("clickThroughUrl") or {}).get("url")
        or content.get("link")
        or item.get("link")
    )
    provider = content.get("provider")
    publisher = (
        (provider.get("displayName") if isinstance(provider, dict) else None)
        or content.get("publisher")
        or item.get("publisher")
    )
    title = content.get("title") or item.get("title")
    if not title:
        return None

    return {
        "id": item.get("id") or item.get("uuid") or url or title,
        "title": title,
        "publisher": publisher,
        "url": url,
        "published": published,
        "summary": content.get("summary") or "",
    }


async def fetch_news(ticker: str, since: datetime, count: int) -> list[dict]:
    search = yf.Search(
        ticker,
        news_count=count,
        max_results=1,
        raise_errors=True,
    )
    raw = search.news or []
    print(f"[{ticker}] raw articles: {len(raw)}", file=sys.stderr)  # diagnostic
    articles = []
    for item in raw:
        art = normalize(item)
        if art and art["published"] >= since:
            articles.append(art)
    return sorted(articles, key=lambda a: a["published"], reverse=True)


async def main():
    tickers = await fetch_tickers()
    if tickers is None or not tickers:
        print("No tickers found")
        return
    since = datetime.now(timezone.utc) - timedelta(days=7)

    by_ticker: dict[str, list[dict]] = {}
    for tk in tickers:
        try:
            by_ticker[tk] = await fetch_news(tk, since, 100)
        except Exception as e:  # one bad ticker shouldn't kill the run
            print(f"[{tk}] fetch failed: {e}", file=sys.stderr)
            by_ticker[tk] = []

    for tk, arts in by_ticker.items():
        print(f"=== {tk} ({len(arts)} article{'s' if len(arts) != 1 else ''}) ===")
        for a in arts:
            print(f"- [{a['published']:%Y-%m-%d %H:%M} UTC] {a['title']}")
            print(f"  {a['publisher'] or 'Unknown source'} | {a['url'] or 'no link'}")
        print()


if __name__ == "__main__":
    asyncio.run(main())
