# article_scoring.py
"""Score news articles from Supabase with FinBERT and store the scores.

Steps (run_scoring(engine) does all of them):
    1. iter_unscored_articles    news_articles rows with no article_sentiment row
                                 for this model, filtered in SQL and read one
                                 page (BATCH_ARTICLES) at a time
    2. download_article_texts    article text from each row's url (in parallel)
    3. score_texts_finbert       FinBERT -> one number per article, positive - negative
    4. upload_scores             hands every new score, as one collection, to the
                                 backend's import_collection (src/ingestion/importer.py),
                                 the only write path: it creates the source_batches
                                 and ingestion_runs rows and fills in security_id
                                 and row_number

Reads use psycopg directly. The connection string comes from the engine you
pass in (or from DATABASE_URL in the environment / .env).

Hook into the credibility job (between news ingestion and weighting):
    from src.services.articles_scorer.article_scoring import run_scoring
    import_result = run_scoring(engine)

The importer does not skip duplicates: if one article already has a score from
this model, the whole import fails. That is why step 1 filters out articles
already in article_sentiment. The NOT EXISTS filter is fast when
article_sentiment has an index on (news_article_id, model) (a unique
constraint on those columns counts).

Articles whose page can't be downloaded (paywall, 403, timeout, or disallowed
by the site's robots.txt) are scored on their headline alone; set
HEADLINE_FALLBACK = False to skip them instead, so a later run tries the
download again.

Downloading is done openly as a bot: requests carry a user agent that names
this crawler and a contact address (CONTACT; SCRAPER_CONTACT overrides it),
every site's robots.txt is checked before each request (including every
redirect hop), and a site that blocks us (401/403/429) is not retried. We never
pose as a browser.
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
import json
import logging
import os
import threading
from typing import Optional
from urllib.parse import urljoin, urlsplit
from urllib.robotparser import RobotFileParser

import psycopg
import requests
import trafilatura
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection

try:
    from .finbert import MODEL_ID, score_articles     # as a package
except ImportError:
    from finbert import MODEL_ID, score_articles      # run from this folder

log = logging.getLogger(__name__)

SCORES_TABLE = "perennial.article_sentiment"  # where existing scores are read from
SOURCE = "article_sentiment"  # importer source name (migration 0019 vocabulary)
COLLECTION_MODE = "live"      # must be one of src.db.models.COLLECTION_MODES
BATCH_ARTICLES = 50          # articles downloaded + scored per round
DOWNLOAD_WORKERS = 8
DOWNLOAD_TIMEOUT = 20        # seconds per request
MIN_TEXT_CHARS = 200         # shorter extractions count as a failed download
HEADLINE_FALLBACK = True
MAX_REDIRECTS = 5
# Honest bot identity. Sites can see who we are, read our contact address, and
# block us through robots.txt. SCRAPER_CONTACT overrides the contact address.
BOT_NAME = "PerennialSentimentBot"
BOT_VERSION = "1.0"
CONTACT = os.environ.get("SCRAPER_CONTACT", "").strip() or "perennial491@gmail.com"
USER_AGENT = f"{BOT_NAME}/{BOT_VERSION} (+mailto:{CONTACT})"


@dataclass
class ScoringResult:
    model: str
    candidates: int = 0          # unscored articles found
    downloaded: int = 0          # full text extracted
    headline_only: int = 0       # download failed, scored on the headline
    skipped: int = 0             # nothing to score (no text and no headline)
    scored: int = 0              # articles given a score this run
    errors: list = field(default_factory=list)

    def as_dict(self):
        return asdict(self)


# ---- database -------------------------------------------------------------------------------
def get_db_url():
    load_dotenv()
    return os.environ["DATABASE_URL"]


def get_engine(db_url=None):
    """SQLAlchemy engine for DATABASE_URL, using the psycopg (v3) driver."""
    url = db_url or get_db_url()
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            url = "postgresql+psycopg://" + url[len(prefix):]
    return create_engine(url, pool_pre_ping=True)


def engine_db_url(engine):
    """Plain postgresql:// connection string for psycopg, taken from the engine."""
    return engine.url.set(drivername="postgresql").render_as_string(hide_password=False)


_UNSCORED_SQL = f"""
    SELECT na.id, s.symbol, na.row_number, na.finnhub_id, na.url, na.headline
    FROM perennial.news_articles AS na
    JOIN perennial.securities    AS s ON s.id = na.security_id
    WHERE na.id > %(after_id)s
      AND NOT EXISTS (SELECT 1 FROM {SCORES_TABLE} AS sc
                      WHERE sc.news_article_id = na.id AND sc.model = %(model)s)
    ORDER BY na.id
    LIMIT %(page)s;
"""


def fetch_unscored_page(conn, model=MODEL_ID, after_id=0, page=BATCH_ARTICLES):
    """Up to `page` articles `model` hasn't scored, with id > after_id, as dicts:
    {id, symbol, row_number, finnhub_id, url, headline}, ordered by id.

    The filtering happens in Postgres, so only new articles leave the database,
    and only one page of them at a time.
    """
    cur = conn.execute(_UNSCORED_SQL, {"after_id": after_id, "model": model, "page": page})
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def iter_unscored_articles(db_url=None, model=MODEL_ID, limit=None, page=BATCH_ARTICLES):
    """Yield pages (lists) of unscored articles, oldest id first, up to `limit` total.

    Keyset pagination (id > last id seen) keeps memory at one page and never
    returns the same article twice in a run, even if its page fails to score.
    """
    remaining = limit
    after_id = 0
    with psycopg.connect(db_url or get_db_url()) as conn:
        while remaining is None or remaining > 0:
            size = page if remaining is None else min(page, remaining)
            rows = fetch_unscored_page(conn, model, after_id, size)
            if not rows:
                return
            yield rows
            after_id = rows[-1]["id"]
            if remaining is not None:
                remaining -= len(rows)
            conn.commit()       # end the read transaction between pages


# ---- upload ---------------------------------------------------------------------------------
def _record(row):
    """One importer record: the four fields of the article_sentiment handoff."""
    return {
        "ticker": row["ticker"],
        "news_article_id": int(row["news_article_id"]),
        # numeric(5,4) in [-1, 1]
        "score": round(max(-1.0, min(1.0, float(row["score"]))), 4),
        "model": row["model"],
    }


def upload_scores(rows, engine: Engine, *, mode=COLLECTION_MODE,
                  run_id: Optional[int] = None) -> ImportResult:
    """Import [{ticker, news_article_id, score, model}, ...] through import_collection.

    All rows go in as one collection, so one call writes one source_batches row
    (rows numbered 1..n) and one ingestion_runs row. import_collection resolves
    each ticker to its security_id, checks every record, and writes all rows
    or none of them. It does not raise for bad input. A failed import comes
    back as an ImportResult with status "failed" and an error_code.

    Args:
        rows: the scores. Extra keys (e.g. security_id) are ignored.
        engine: SQLAlchemy engine for the database.
        mode: collection mode recorded on the run.
        run_id: an ingestion run the job already started; None starts a new one.

    An empty `rows` is still imported, so the run is recorded as a succeeded
    run with 0 records.

    Returns:
        The ImportResult.
    """
    if not rows:
        log.info("no new scores; recording an empty run")
    outcome = CollectionOutcome.success(SOURCE, mode, [_record(r) for r in rows])
    result = import_collection(engine, outcome, run_id=run_id)
    if result.succeeded:
        log.info("imported %s article scores (batch %s%s)", result.accepted_count,
                 result.batch_id, ", reused" if result.reused_batch else "")
    else:
        log.error("article score import failed: %s %s", result.error_code,
                  result.error_summary)
    return result


# ---- downloading ----------------------------------------------------------------------------
_robots: dict = {}              # "scheme://host" -> [lock, RobotFileParser or None]
_robots_lock = threading.Lock()
_session_local = threading.local()


def _session():
    """One requests.Session per download thread, carrying the bot's headers."""
    s = getattr(_session_local, "s", None)
    if s is None:
        s = _session_local.s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT,
                          "From": CONTACT,      # the HTTP header meant for a bot's contact
                          "Accept-Language": "en-US,en;q=0.9"})
    return s


def _robots_for(url):
    """Parsed robots.txt for url's site, fetched once per site per process."""
    parts = urlsplit(url)
    site = f"{parts.scheme}://{parts.netloc}"
    with _robots_lock:
        entry = _robots.get(site)
        if entry is None:
            entry = _robots[site] = [threading.Lock(), None]
    lock, rp = entry
    if rp is not None:
        return rp
    with lock:                  # other threads for this site wait for one fetch
        if entry[1] is None:
            entry[1] = _fetch_robots(site)
        return entry[1]


def _fetch_robots(site):
    rp = RobotFileParser()
    try:
        resp = _session().get(site + "/robots.txt", timeout=DOWNLOAD_TIMEOUT)
        if resp.status_code in (401, 403):
            rp.disallow_all = True          # same rule urllib's own read() uses
        elif resp.status_code >= 400:
            rp.allow_all = True             # no robots.txt: everything allowed
        else:
            rp.parse(resp.text.splitlines())
    except requests.RequestException as e:
        log.debug("robots.txt unavailable for %s: %s", site, e)
        rp.disallow_all = True              # can't tell, so don't crawl the site
    return rp


def allowed_by_robots(url):
    """True if url's robots.txt lets our user agent fetch it."""
    return _robots_for(url).can_fetch(USER_AGENT, url)


def download_article_text(url):
    """Main text of the article at `url`, or None if it can't be fetched/extracted.

    Redirects (e.g. Finnhub's redirect links) are followed one hop at a time so
    robots.txt is checked on every URL we request. Pages robots.txt disallows,
    and sites that refuse us (401/403/429), give None; we don't work around them.
    """
    if not url:
        return None
    try:
        for _ in range(MAX_REDIRECTS + 1):
            if urlsplit(url).scheme not in ("http", "https"):
                return None
            if not allowed_by_robots(url):
                log.debug("robots.txt disallows %s", url)
                return None
            resp = _session().get(url, timeout=DOWNLOAD_TIMEOUT, allow_redirects=False)
            if resp.is_redirect and "location" in resp.headers:
                url = urljoin(url, resp.headers["location"])
                continue
            break
        else:
            log.debug("too many redirects for %s", url)
            return None
        if resp.status_code in (401, 403, 429):
            log.debug("site refused %s (%s)", url, resp.status_code)
            return None
        resp.raise_for_status()
    except requests.RequestException as e:
        log.debug("download failed for %s: %s", url, e)
        return None
    body = trafilatura.extract(resp.text, url=url, include_comments=False,
                               include_tables=False, favor_precision=True)
    if not body or len(body) < MIN_TEXT_CHARS:
        return None
    return body


def download_article_texts(urls, workers=DOWNLOAD_WORKERS):
    """download_article_text for every url, in parallel; results in input order."""
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(download_article_text, urls))


# ---- scoring --------------------------------------------------------------------------------
def score_texts_finbert(texts):
    """One FinBERT score per text, in input order: positive - negative, -1..1."""
    return [r["sentiment"] for r in score_articles(texts)]


def _article_input(article, body):
    """Text FinBERT sees for one article: headline + body, or the headline alone."""
    headline = (article.get("headline") or "").strip()
    if body:
        return f"{headline}.\n\n{body}" if headline else body
    return headline if HEADLINE_FALLBACK else ""


# ---- entry point ----------------------------------------------------------------------------
def run_scoring(engine: Optional[Engine] = None, model=MODEL_ID, limit=None, *,
                mode=COLLECTION_MODE, run_id: Optional[int] = None) -> ImportResult:
    """Score every article `model` hasn't scored yet and import the scores.

    Articles are downloaded and scored in rounds of BATCH_ARTICLES. A round
    that fails is logged and left out, and the next run picks it up again.
    Then every score is imported in one import_collection call.

    Args:
        engine: SQLAlchemy engine (default: built from DATABASE_URL). Reads
            use psycopg on the same database.
        model: model name written to the `model` field.
        limit: optional max number of articles to process this run.
        mode, run_id: passed to import_collection (see upload_scores).

    Returns:
        The ImportResult from import_collection (a run is recorded even when
        there were no new scores). Scoring counts are logged, and `last_scoring` holds
        them as a ScoringResult.
    """
    global last_scoring
    engine = engine or get_engine()
    db_url = engine_db_url(engine)
    result = last_scoring = ScoringResult(model=model)

    log.info("scoring unscored articles with %s", model)

    rows = []
    for batch in iter_unscored_articles(db_url, model, limit):
        result.candidates += len(batch)
        try:
            bodies = download_article_texts([a["url"] for a in batch])
            inputs = []
            for a, body in zip(batch, bodies):
                inputs.append(_article_input(a, body))
                if body:
                    result.downloaded += 1
                elif inputs[-1]:
                    result.headline_only += 1

            todo = [(a, t) for a, t in zip(batch, inputs) if t]
            result.skipped += len(batch) - len(todo)
            scores = score_texts_finbert([t for _, t in todo])

            new = [{"ticker": a["symbol"], "news_article_id": a["id"],
                    "score": float(s), "model": model}
                   for (a, _), s in zip(todo, scores)]
            rows += new
            result.scored += len(new)
        except Exception as e:      # keep going; the next run retries this round
            log.exception("batch starting at article id %s failed", batch[0]["id"])
            result.errors.append(f"articles {batch[0]['id']}..{batch[-1]['id']}: {e!r}")

        log.info("%d articles processed", result.candidates)

    log.info("scoring summary: %s", result.as_dict())
    return upload_scores(rows, engine, mode=mode, run_id=run_id)


last_scoring: Optional[ScoringResult] = None   # counts from the latest run_scoring call


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Score unscored news articles with FinBERT.")
    ap.add_argument("--limit", type=int, default=None, help="max articles this run")
    args = ap.parse_args()
    imported = run_scoring(limit=args.limit)
    print(json.dumps({"scoring": last_scoring.as_dict(),
                      "import": asdict(imported)}, indent=2))
