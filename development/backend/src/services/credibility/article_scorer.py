"""JOB-7: the S1-04 article scorer for #85's score_articles slot, FinBERT over each
article's full text, falling back to its headline.

score_articles is a derived article_sentiment run. Its input is every stored
news article published in derive_sentiment's window that has no score from
MODEL_VERSION yet, so a failed run is caught up by the next one and nothing
outside the window is read. For each article the page at its url is downloaded
and its main text extracted; when that fails (paywall, block, timeout, too
little text, or disallowed by the site's robots.txt) the article is scored
on its headline alone, never skipped. The
scorer reads the texts in news_article_id order, in chunks of at most
BATCH_SIZE articles; a text longer than FinBERT's 512-token window is split
into overlapping windows whose probabilities are averaged, weighted by length.
Each article's score is P(positive) - P(negative). With nothing new to score
there is no run at all.

The run fails closed: the model failing to load (torch or transformers missing,
or the weights unavailable) or the scorer raising or answering badly fails it
"scoring_failed" with no batch, and no headline, url or article text reaches a
log or the run record. A failed download is not a failure: it falls back.

Downloading is done openly as a bot: requests carry a user agent that names
this crawler and a contact address (CONTACT; SCRAPER_CONTACT overrides it),
every site's robots.txt is checked before each request (including every
redirect hop), and a site that blocks us (401/403/429) is not retried. We never
pose as a browser.

torch, transformers, requests and trafilatura are imported only on the scoring
path, so importing this module (and CI, which installs none of them) never
needs them. On first load, transformers fetches the pinned weights from the
Hugging Face hub.
"""

import logging
import math
import os
import threading
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import urljoin, urlsplit
from urllib.robotparser import RobotFileParser

from sqlalchemy import exists, select
from sqlalchemy.engine import Connection, Engine
from src.db import models as m
from src.db.queries import latest_successful_run
from src.ingestion.diagnostics import exception_diagnostics
from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection
from src.pipeline.lifecycle import begin_run, heartbeat_worker, source_lock
from src.services.credibility.job import WINDOW_DAYS

logger = logging.getLogger(__name__)

SOURCE, NEWS = "article_sentiment", "news_articles"
MODEL_NAME = "ProsusAI/finbert"
MODEL_REVISION = "0bc2ac10dda6929bcb1f77ccc43c7ec5c424f031"  # main as of 2026-11-01
MODEL_VERSION = f"{MODEL_NAME}@{MODEL_REVISION}"
MAX_TOKENS = 512  # FinBERT's window, special tokens included
STRIDE = 64  # tokens shared by neighbouring windows of one long text
BATCH_SIZE = 32  # articles per scorer call
WINDOW_BATCH = 16  # 512-token windows per forward pass
LABELS = ("positive", "negative", "neutral")

DOWNLOAD_WORKERS = 8
DOWNLOAD_TIMEOUT = 20  # seconds per request
MIN_BODY_CHARS = 200  # a shorter extraction counts as a failed download
MAX_REDIRECTS = 5
# Honest bot identity. Sites can see who we are, read our contact address, and
# block us through robots.txt. SCRAPER_CONTACT overrides the contact address.
BOT_NAME = "PerennialSentimentBot"
BOT_VERSION = "1.0"
CONTACT = os.environ.get("SCRAPER_CONTACT", "").strip() or "perennial491@gmail.com"
USER_AGENT = f"{BOT_NAME}/{BOT_VERSION} (+mailto:{CONTACT})"

# texts -> one {"positive", "negative", "neutral"} probability mapping per text.
Scorer = Callable[[Sequence[str]], Sequence[Mapping[str, float]]]
# urls -> one article body (or None when unavailable) per url, in order.
Fetcher = Callable[[Sequence[str | None]], Sequence[str | None]]


def article_text(article: Mapping[str, Any], body: str | None = None) -> str:
    """What the model reads: the headline followed by the downloaded body, or
    the headline alone when there is no body. The scorer handles any length."""
    headline = str(article["headline"] or "").strip()
    body = (body or "").strip()
    if not body:
        return headline
    return f"{headline}.\n\n{body}" if headline else body


@dataclass
class _SiteRobots:
    """One site's robots.txt, fetched at most once; lock serialises the fetch."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    parser: RobotFileParser | None = None


_robots: dict[str, _SiteRobots] = {}  # "scheme://host" -> its robots.txt
_robots_lock = threading.Lock()
_session_local = threading.local()


def _session() -> Any:
    """One requests.Session per download thread, carrying the bot's headers."""
    import requests

    session = getattr(_session_local, "s", None)
    if session is None:
        session = _session_local.s = requests.Session()
        session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "From": CONTACT,  # the HTTP header meant for a bot's contact
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
    return session


def _robots_for(url: str) -> RobotFileParser:
    """Parsed robots.txt for url's site, fetched once per site per process."""
    parts = urlsplit(url)
    site = f"{parts.scheme}://{parts.netloc}"
    with _robots_lock:
        entry = _robots.setdefault(site, _SiteRobots())
    with entry.lock:  # other threads for this site wait for one fetch
        if entry.parser is None:
            entry.parser = _fetch_robots(site)
        return entry.parser


def _fetch_robots(site: str) -> RobotFileParser:
    import requests

    parser = RobotFileParser()
    try:
        response = _session().get(site + "/robots.txt", timeout=DOWNLOAD_TIMEOUT)
    except requests.RequestException:
        response = None
    if response is None or response.status_code in (401, 403):
        # Can't tell, or refused (the rule urllib's own read() uses): crawl nothing.
        parser.parse(["User-agent: *", "Disallow: /"])
    elif response.status_code >= 400:
        parser.parse([])  # no robots.txt: everything allowed
    else:
        parser.parse(response.text.splitlines())
    return parser


def allowed_by_robots(url: str) -> bool:
    """True if url's robots.txt lets our user agent fetch it."""
    return _robots_for(url).can_fetch(USER_AGENT, url)


def _download(url: str | None) -> str | None:
    """The main text of the page at url, or None if it can't be had.

    Redirects (e.g. Finnhub's redirect links) are followed one hop at a time so
    robots.txt is checked on every URL we request. Pages robots.txt disallows,
    and sites that refuse us (401/403/429), give None; we don't work around
    them. Nothing here logs: a url must not reach a log.
    """
    import requests
    import trafilatura

    if not url:
        return None
    try:
        for _ in range(MAX_REDIRECTS + 1):
            if urlsplit(url).scheme not in ("http", "https"):
                return None
            if not allowed_by_robots(url):
                return None
            response = _session().get(
                url, timeout=DOWNLOAD_TIMEOUT, allow_redirects=False
            )
            if response.is_redirect and "location" in response.headers:
                url = urljoin(url, response.headers["location"])
                continue
            break
        else:
            return None  # too many redirects
        if response.status_code in (401, 403, 429):
            return None
        response.raise_for_status()
    except requests.RequestException:
        return None
    body = trafilatura.extract(
        response.text,
        url=url,
        include_comments=False,
        include_tables=False,
        favor_precision=True,
    )
    return body if body and len(body) >= MIN_BODY_CHARS else None


def download_bodies(urls: Sequence[str | None]) -> list[str | None]:
    """_download for every url, in parallel; results in input order. Without
    requests or trafilatura installed every article falls back to its headline."""
    try:
        import requests  # noqa: F401
        import trafilatura  # noqa: F401
    except ImportError:
        logger.warning("article scores: requests/trafilatura missing, headlines only")
        return [None] * len(urls)
    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
        return list(pool.map(_download, urls))


def polarity(probabilities: Mapping[str, float]) -> Decimal:
    """P(positive) - P(negative), to 4 places."""
    value = probabilities["positive"] - probabilities["negative"]
    return Decimal(f"{value:.4f}") + 0  # + 0 turns "-0.0000" into "0.0000"


def _probability(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("a probability is missing or not a number")
    if not 0 <= value <= 1:  # NaN fails this too
        raise ValueError("a probability is outside [0, 1]")
    return float(value)


def _checked(answers: Sequence[Mapping[str, float]], count: int) -> None:
    """One probability distribution over LABELS per text, else ValueError."""
    if len(answers) != count:
        raise ValueError("the scorer answered a different number of texts")
    for answer in answers:
        total = sum(_probability(answer.get(label)) for label in LABELS)
        if not math.isclose(total, 1, abs_tol=1e-3):
            raise ValueError("the probabilities don't sum to 1")


def finbert() -> Scorer:
    """FinBERT at the pinned revision. Raises if torch, transformers or the
    weights are unavailable."""
    import torch
    from transformers import AutoModelForSequenceClassification, AutoTokenizer

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, revision=MODEL_REVISION)
    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_NAME, revision=MODEL_REVISION
    )
    model.to(device).eval()
    labels = [model.config.id2label[i].lower() for i in range(model.config.num_labels)]

    def score(texts: Sequence[str]) -> list[dict[str, float]]:
        # Every text becomes one or more overlapping MAX_TOKENS windows;
        # owners[k] is the index of the text window k came from.
        encoded = tokenizer(
            list(texts),
            truncation=True,
            max_length=MAX_TOKENS,
            stride=STRIDE,
            return_overflowing_tokens=True,
        )
        windows = encoded["input_ids"]
        owners = list(encoded["overflow_to_sample_mapping"])
        sums = [torch.zeros(len(labels)) for _ in texts]
        weights = [0] * len(texts)
        for i in range(0, len(windows), WINDOW_BATCH):
            padded = tokenizer.pad(
                {"input_ids": windows[i : i + WINDOW_BATCH]}, return_tensors="pt"
            )
            inputs = {k: v.to(device) for k, v in padded.items()}
            with torch.no_grad():
                rows = torch.softmax(model(**inputs).logits.float(), dim=-1).cpu()
            for k, row in enumerate(rows, start=i):
                size = len(windows[k])  # longer windows count for more
                sums[owners[k]] += row * size
                weights[owners[k]] += size
        return [
            dict(zip(labels, (total / weight).tolist()))
            for total, weight in zip(sums, weights)
        ]

    return score


def _unscored(conn: Connection, start: datetime, end: datetime) -> list[Any]:
    """The window's articles with no score from MODEL_VERSION, by id."""
    na, a = m.news_articles, m.article_sentiment
    scored = exists().where(a.c.news_article_id == na.c.id, a.c.model == MODEL_VERSION)
    return list(
        conn.execute(
            select(na.c.id, m.securities.c.symbol, na.c.headline, na.c.url)
            .join(m.securities, m.securities.c.id == na.c.security_id)
            .where(na.c.published_at >= start, na.c.published_at <= end, ~scored)
            .order_by(na.c.id)
        ).all()
    )


def _scores_outcome(
    articles: Sequence[Any],
    scorer: Scorer | None,
    news_batch: int,
    fetcher: Fetcher | None = None,
) -> CollectionOutcome:
    score = scorer or finbert()
    fetch = fetcher or download_bodies
    records = []
    full_text = 0
    for i in range(0, len(articles), BATCH_SIZE):
        chunk = articles[i : i + BATCH_SIZE]
        bodies = fetch([a.url for a in chunk])
        if len(bodies) != len(chunk):
            raise ValueError("the fetcher answered a different number of urls")
        full_text += sum(1 for body in bodies if body)
        answers = score(
            [article_text(a._mapping, body) for a, body in zip(chunk, bodies)]
        )
        _checked(answers, len(chunk))
        records += [
            {
                "ticker": article.symbol,
                "news_article_id": article.id,
                "score": str(polarity(answer)),
                "model": MODEL_VERSION,
            }
            for article, answer in zip(chunk, answers)
        ]
    return CollectionOutcome.success(
        SOURCE,
        "derived",
        records,
        coverage={"complete": True, "scope": "derivation"},
        envelope={
            "model": MODEL_VERSION,
            "input_batch_id": news_batch,
            "articles_scored": len(records),
            "articles_full_text": full_text,
            "articles_headline_only": len(records) - full_text,
        },
    )


def score_articles(
    engine: Engine,
    *,
    scorer: Scorer | None = None,
    fetcher: Fetcher | None = None,
    now: datetime | None = None,
) -> ImportResult | None:
    """Score the window's new articles as a derived run; None when there were none.
    scorer=None means FinBERT; fetcher=None means downloading each article's url
    (tests pass a fetcher that returns None per url to stay offline)."""
    now = now or datetime.now(timezone.utc)
    start = now - timedelta(days=WINDOW_DAYS)
    with source_lock(engine, SOURCE):
        with engine.connect() as conn:
            news = latest_successful_run(conn, NEWS)
            articles = _unscored(conn, start, now) if news is not None else []
        if news is not None and not articles:
            logger.info("article scores: nothing new to score")
            return None
        dependencies = {NEWS: news.batch_id} if news is not None else {}
        run_id = begin_run(engine, SOURCE, "derived", dependencies=dependencies)
        with heartbeat_worker(engine, run_id):
            if news is None:
                outcome = CollectionOutcome.failure(
                    SOURCE, "derived", "missing_upstream"
                )
            else:
                try:
                    outcome = _scores_outcome(
                        articles, scorer, news.batch_id, fetcher=fetcher
                    )
                except Exception as exc:  # the class name only: never a headline
                    outcome = CollectionOutcome.failure(
                        SOURCE,
                        "derived",
                        "scoring_failed",
                        diagnostics=exception_diagnostics(exc),
                    )
            result = import_collection(engine, outcome, run_id=run_id)
    logger.info(
        "article scores status=%s code=%s run_id=%s batch_id=%s scored=%s",
        result.status,
        result.error_code,
        result.run_id,
        result.batch_id,
        result.accepted_count,
    )
    return result
