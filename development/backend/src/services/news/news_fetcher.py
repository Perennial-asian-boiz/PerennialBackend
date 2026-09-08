import requests
import feedparser
import collections
import trafilatura
import json
from pathlib import Path
from utils.logger import logger

RSS_FEEDS = {
    "bbc": {
        "outlet": "https://www.bbc.com/news",
        "feed": "https://feeds.bbci.co.uk/news/rss.xml",
    },
    "guardian": {
        "outlet": "https://www.theguardian.com/international",
        "feed": "https://www.theguardian.com/international/rss",
    },
    "france24": {
        "outlet": "https://www.france24.com/en/",
        "feed": "https://www.france24.com/en/rss/",
    },
    "deutsche_welle": {
        "outlet": "https://www.dw.com/en/top-stories/s-9097",
        "feed": "https://rss.dw.com/rdf/rss-en-top",
    },
    "al_jazeera": {
        "outlet": "https://www.aljazeera.com/",
        "feed": "https://www.aljazeera.com/xml/rss/all.xml",
    },
    "the_hindu": {
        "outlet": "https://www.thehindu.com/",
        "feed": "https://www.thehindu.com/feeder/default.rss",
    },
    "japan_times": {
        "outlet": "https://www.japantimes.co.jp/",
        "feed": "https://www.japantimes.co.jp/feed/",
    },
    "straits_times": {
        "outlet": "https://www.straitstimes.com/world",
        "feed": "https://www.straitstimes.com/news/world/rss.xml",
    },
    "abc_australia": {
        "outlet": "https://www.abc.net.au/news/",
        "feed": "https://www.abc.net.au/news/feed/51120/rss.xml",
    },
    "cbc": {
        "outlet": "https://www.cbc.ca/news",
        "feed": "https://rss.cbc.ca/lineup/topstories.xml",
    },
    "news24": {
        "outlet": "https://www.news24.com/",
        "feed": "https://www.news24.com/rss",
    },
    "financial_times": {
        "outlet": "https://www.ft.com/world",
        "feed": "https://www.ft.com/rss/home",
    },
}

BACKEND_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = BACKEND_DIR/"data"
DATA_DIR.mkdir(exist_ok=True)

def collect_article_urls():
    articles = collections.defaultdict(list)

    for feed in RSS_FEEDS:
        outlet = RSS_FEEDS[feed]["outlet"]
        feed_url = RSS_FEEDS[feed]["feed"]

        response = requests.get(
            feed_url,
            headers={
                "User-Agent": (
                    "PerennialNewsBot/1.0 "
                    "(contact: your-email@example.com)"
                ),
                "Accept": "application/rss+xml, application/xml, text/xml, */*",
            },
            timeout=20,
        )
        response.raise_for_status()

        feed_content = feedparser.parse(response.content)

        # feedparser uses `bozo` to indicate malformed/unusual feed XML.
        # Log it, but entries can still often be usable.
        if feed_content.bozo:
            logger.warning(
                "Feed may have malformed XML: %s",
                getattr(feed_content, "bozo_exception", "Unknown error"),
            )

        for entry in feed_content.entries:
            article_url = entry.get("link")
            if not article_url:
                continue

            articles[outlet].append({
                "url": article_url,
                "title": entry.get("title"),
                "published": entry.get("published"),
            })

    return articles

def parse_html(url):
    response = requests.get(
        url,
        headers={
            "User-Agent": (
                "PerennialNewsBot/1.0 "
                "(contact: your-email@example.com)"
            ),
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        },
        timeout=20,
    )
    response.raise_for_status()

    return trafilatura.extract(
        response.text,
        include_comments=False,
        include_links=False,
        favor_precision=True,
    )

def main():
    article_objects = collect_article_urls()
    content = []

    for key in article_objects:
        article_obj = article_objects[key]

        for obj in article_obj:
            article_text = parse_html(obj["url"])

            if not article_text:
                logger.warning("Could not extract article text: %s", obj["url"])
                continue
            
            content.append({
                "url": obj["url"],
                "title": obj["title"],
                "published": obj["published"],
                "article": article_text
            })

    with open(f"{DATA_DIR}/articles.json", "w") as f:
        json.dump(content, f, indent=4)


if __name__ == "__main__":
    main()