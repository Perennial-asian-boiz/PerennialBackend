"""
Fetch recent Yahoo Finance news for a list of tickers.

dependencies: yfinance

Usage:
    python yahoo_news.py 
"""
import sys
from datetime import datetime, timedelta, timezone
from utils.fetch_tickers import fetch_tickers
import asyncio
import yfinance as yf


def normalize(item: dict) -> dict | None:
    """
    yfinance has returned news in two shapes over time:
      new: {"id": ..., "content": {"title", "pubDate", "canonicalUrl": {"url"}, "provider": {"displayName"}, ...}}
      old: {"uuid": ..., "title", "link", "publisher", "providerPublishTime"}
    This flattens both into one structure.
    """
    content = item.get("content") or item

    published = None
    pub = content.get("pubDate") or content.get("displayTime")
    if pub:
        try:
            published = datetime.fromisoformat(pub.replace("Z", "+00:00"))
        except ValueError:
            pass
    if published is None and item.get("providerPublishTime"):
        published = datetime.fromtimestamp(item["providerPublishTime"], tz=timezone.utc)
    if published is None:
        return None  # can't apply the date filter without a timestamp

    url = (
        (content.get("canonicalUrl") or {}).get("url")
        or (content.get("clickThroughUrl") or {}).get("url")
        or item.get("link")
    )
    publisher = (content.get("provider") or {}).get("displayName") or item.get("publisher")
    title = content.get("title")
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
    t = yf.Ticker(ticker)
    try:
        raw = t.get_news(count=count, tab="news")
    except TypeError:  # older yfinance without get_news(count=, tab=)
        raw = t.news
    articles = []
    for item in raw or []:
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

    print(f"News from the past 7 day(s) since {since:%Y-%m-%d %H:%M} UTC\n")
    for tk, arts in by_ticker.items():
        print(f"=== {tk} ({len(arts)} article{'s' if len(arts) != 1 else ''}) ===")
        for a in arts:
            print(f"- [{a['published']:%Y-%m-%d %H:%M} UTC] {a['title']}")
            print(f"  {a['publisher'] or 'Unknown source'} | {a['url'] or 'no link'}")
        print()


if __name__ == "__main__":
    asyncio.run(main())
