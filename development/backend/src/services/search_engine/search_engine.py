import argparse
from src.utils.logger import create_logger
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
import base64
import binascii
import trafilatura
import json

BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BACKEND_DIR/"data"
DATA_DIR.mkdir(exist_ok=True)
URL_PREFIX = "https://www.bing.com/search"
SEARCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:154.0) "
        "Gecko/20100101 Firefox/154.0"
    )
}

def clean_url(href: str) -> str | None:
    """ extract redirect link from Bing links """
    if not href:
        return None

    parsed = urlparse(href)

    # Detect a Bing tracking URL
    if "bing.com" in parsed.netloc.lower() and parsed.path == "/ck/a":
        parameters = parse_qs(parsed.query)
        encoded_url = parameters.get("u", [None])[0]

        if not encoded_url:
            return None

        # Bing's Base64 destination begins with an "a1" marker
        if encoded_url.startswith("a1"):
            encoded_url = encoded_url[2:]

        # Base64 strings sometimes need trailing padding
        encoded_url += "=" * (-len(encoded_url) % 4)

        try:
            href = base64.urlsafe_b64decode(
                encoded_url
            ).decode("UTF-8")
        except (binascii.Error, UnicodeDecodeError, ValueError):
            return None

        parsed = urlparse(href)

    if parsed.scheme not in {"http", "https"}:
        return None

    # Reject any Bing URL that was not successfully decoded
    if "bing.com" in parsed.netloc.lower():
        return None

    return href

def get_top_urls(phrase: str, logger):
    """ return top 10 urls from Bing search engine for phrase """
    res = requests.get(
        URL_PREFIX,
        params={
            "q": phrase,
            "count": 10,
            "setlang": "en",
            "cc": "us",
        },
        headers=SEARCH_HEADERS,
        timeout=10,
    )

    logger.info(f"Web Engine status code for \"{phrase}\": {res.status_code}")
    res.raise_for_status()

    soup = BeautifulSoup(res.text, "html.parser")
    results = []
    seen = set()

    for result in soup.select("li.b_algo"):
        link = result.select_one("h2 a")

        if link is None:
            continue

        href = link.get("href")

        cleaned_url = clean_url(href or "")

        if cleaned_url is None or cleaned_url in seen:
            continue

        seen.add(cleaned_url)
        results.append(cleaned_url)
        logger.info(f"Added URL: {cleaned_url}")

        if len(results) >= 10:
            break
    return results

def extract_content(url):
    """ Use trafilature library to extract content from html from redirect links from Bing """
    response = requests.get(
        url,
        timeout=15,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (compatible; PerennialBot/1.0; "
                "+https://your-domain.com/bot)"
            )
        },
    )
    response.raise_for_status()

    return trafilatura.extract(
        response.text,
        url=url,
        include_comments=False,
        include_tables=True,
        favor_precision=True,
    )

def handle_urls(url_list, filepath, logger):
    """ given list of Urls clean, fetch and write data to JSON """
    data = {}
    for url in url_list:
        try:
            logger.info(f"extracted {url}")
            content = extract_content(url)
            data[url] = content
        except requests.HTTPError as error:
            status = error.response.status_code if error.response else "unknown"
            logger.warning(
                f"Skipping URL after HTTP {status}: {url}"
            )

    with open(filepath, 'w') as f:
        json.dump(data, f, indent = 4)

def main():
    """ main body of script to coordinate searches """
    parser = argparse.ArgumentParser(description="Return top data from search results from a web engine given search term.")
    parser.add_argument(
        "phrase",
        help="phrase to search for"
    )
    args = parser.parse_args()
    phrase = args.phrase
    print(f"Searching for: {phrase}")

    search_result_file = f"{DATA_DIR}/{phrase}_search_results.json"
    
    logger = create_logger(f"{phrase}_search.log")
    url_list = get_top_urls(phrase, logger)

    if not url_list:
        logger.warning("No valid URLs found")
        return
    
    handle_urls(url_list, search_result_file, logger)
    
    return

if __name__ == "__main__":
    main()