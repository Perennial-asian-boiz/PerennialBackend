"""S1-05 credibility-weighted sentiment, and the scheduled sentiment job. Run once with:

    python -m src.pipeline.scheduler --once --job sentiment

The job runs, in order: news ingestion (S1-03, A), article scoring (S1-04, B,
plugged in through score_articles), the publisher lookup, the social import,
and the weighting (C). One step failing never stops the next.

The weighting is a derived run (ticker_sentiment). For every security in the
current production watchlist publication it takes, over the last WINDOW_DAYS:

* each scored news article whose headline names the company (the table's
  relevance rule): S1-04's latest score for it from the current model (the model
  of the newest article score), weighted by its publisher's entry in
  source_weights.json;
* each social post of the latest social batch, if that batch is inside the
  window: its 1-10 score mapped to [-1, 1] by (score - 5.5) / 4.5, weighted by
  its platform's entry.

Each weight then fades with the item's age at the time of the run, halving every
half_life_days. A news article's age runs from its publication; a social post's
from the end of its file's day, since posts carry no dates.

value is the weighted average, plain_average the unweighted one (AC 1). Every
security gets a row with the counts, the date range and the weights version
(AC 3); one with nothing to score has a count of 0 and no value. model_version
names the news model, and is NULL when no news was counted.
"""

import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from decimal import Decimal
from typing import Any, Callable

import requests
from sqlalchemy import select
from sqlalchemy.engine import Connection, Engine
from src.db import models as m
from src.db.queries import batch_rows, latest_successful_run
from src.ingestion.identity import resolve_tickers
from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection
from src.pipeline.lifecycle import begin_run, heartbeat_worker, source_lock
from src.services.credibility.publishers import PublisherResolver, resolve_publishers
from src.services.credibility.social import run_social
from src.services.credibility.weights import WeightTable, load_weights
from src.services.fundamentals.job import current_universe
from src.services.news.finnhub import FinnhubNewsClient
from src.services.news.job import company_names, run_news
from src.services.news.matching import match_rule, name_variants

logger = logging.getLogger(__name__)

SOURCE = "ticker_sentiment"
WINDOW_DAYS = 7


def social_to_unit(score: float) -> float:
    """The social pipeline's 1-10 scale mapped onto S1-04's [-1, 1]."""
    return (score - 5.5) / 4.5


def decay(at: datetime, now: datetime, half_life_days: float) -> float:
    """The share of its weight an item dated `at` keeps; a future date keeps it all."""
    age_days = max((now - at).total_seconds(), 0.0) / 86400
    return float(0.5 ** (age_days / half_life_days))


def about_company(headline: str, symbol: str, variants: set[str]) -> bool:
    """headline-match-v1: S1-03's match-v1 on the headline alone."""
    return match_rule(headline, "", symbol, variants) is not None


@dataclass(frozen=True)
class Item:
    score: float  # in [-1, 1]
    weight: float
    first_at: datetime
    last_at: datetime
    social: bool


def weigh(
    symbol: str, items: list[Item], version: str, model: str | None
) -> dict[str, Any]:
    """One ticker_sentiment record: the weighted and plain averages of its items."""
    news_count = sum(not i.social for i in items)
    record: dict[str, Any] = {
        "ticker": symbol,
        "article_count": len(items),
        "news_count": news_count,
        "social_count": sum(i.social for i in items),
        "weights_version": version,
        "model_version": model if news_count else None,
        "value": None,
        "plain_average": None,
        "first_at": None,
        "last_at": None,
    }
    if items:
        total = sum(i.weight for i in items)
        weighted = sum(i.weight * i.score for i in items) / total
        plain = sum(i.score for i in items) / len(items)
        record.update(
            value=Decimal(f"{weighted:.4f}"),
            plain_average=Decimal(f"{plain:.4f}"),
            first_at=min(i.first_at for i in items),
            last_at=max(i.last_at for i in items),
        )
    return record


@dataclass(frozen=True)
class News:
    items: dict[str, list[Item]]
    off_topic: int  # scored articles left out because the headline names no company
    publishers_unknown: int  # counted articles with no resolved publisher
    model: str | None


def news_items(
    conn: Connection,
    symbols: list[str],
    start: datetime,
    end: datetime,
    table: WeightTable,
) -> News:
    """The window's scored articles about each symbol, weighted and decayed."""
    na, s, np_, a = (
        m.news_articles,
        m.securities,
        m.news_publishers,
        m.article_sentiment,
    )
    model = conn.execute(
        select(a.c.model).order_by(a.c.batch_id.desc(), a.c.id.desc()).limit(1)
    ).scalar()
    if model is None:
        return News({}, 0, 0, None)
    latest = (
        select(a.c.news_article_id, a.c.security_id, a.c.score)
        .where(a.c.model == model)
        .distinct(a.c.news_article_id)
        .order_by(a.c.news_article_id, a.c.batch_id.desc())
        .subquery()
    )
    rows = conn.execute(
        select(
            s.c.symbol, na.c.headline, na.c.published_at, latest.c.score, np_.c.domain
        )
        .join(s, s.c.id == na.c.security_id)
        .join(
            latest,
            (latest.c.news_article_id == na.c.id)
            & (latest.c.security_id == na.c.security_id),
        )
        .outerjoin(np_, np_.c.finnhub_id == na.c.finnhub_id)
        .where(
            s.c.symbol.in_(symbols),
            na.c.published_at >= start,
            na.c.published_at <= end,
        )
    ).all()
    names = company_names(conn, symbols)
    variants = {symbol: name_variants(names.get(symbol, [])) for symbol in symbols}
    out: dict[str, list[Item]] = {}
    off_topic = unknown = 0
    for symbol, headline, published_at, score, domain in rows:
        if not about_company(headline, symbol, variants[symbol]):
            off_topic += 1
            continue
        unknown += domain is None
        weight = table.weight_for(domain) * decay(
            published_at, end, table.half_life_days
        )
        out.setdefault(symbol, []).append(
            Item(float(score), weight, published_at, published_at, False)
        )
    return News(out, off_topic, unknown, model)


def social_items(
    rows: list[dict[str, Any]], as_of: date, now: datetime, table: WeightTable
) -> dict[str, list[Item]]:
    """Posts carry no dates: each spans the collection window ending on as_of."""
    last = datetime.combine(as_of, dtime(23, 59, 59), tzinfo=timezone.utc)
    first = last - timedelta(days=WINDOW_DAYS)
    fade = decay(last, now, table.half_life_days)
    out: dict[str, list[Item]] = {}
    for row in rows:
        out.setdefault(row["symbol"], []).append(
            Item(
                social_to_unit(float(row["score"])),
                table.weight_for(row["platform"]) * fade,
                first,
                last,
                True,
            )
        )
    return out


def derive_sentiment(
    engine: Engine, table: WeightTable | None = None, now: datetime | None = None
) -> ImportResult:
    """Weight the window's scored news and fresh social posts, as a derived run."""
    table = table or load_weights()
    now = now or datetime.now(timezone.utc)
    start = now - timedelta(days=WINDOW_DAYS)
    with source_lock(engine, SOURCE):
        with engine.connect() as conn:
            universe = current_universe(conn)
            symbols: list[str] = []
            if universe is not None and universe.symbols:
                ids, _failures = resolve_tickers(conn, SOURCE, universe.symbols)
                symbols = sorted(ids)
            inputs = {
                source: latest_successful_run(conn, source)
                for source in ("news_articles", "article_sentiment", "social_posts")
            }
        social = inputs["social_posts"]
        social_fresh = (
            social is not None
            and social.source_as_of is not None
            and social.source_as_of >= start.date()
        )
        dependencies = dict(universe.dependencies) if universe else {}
        dependencies.update(
            {
                source: run.batch_id
                for source, run in inputs.items()
                if run is not None and (source != "social_posts" or social_fresh)
            }
        )
        run_id = begin_run(engine, SOURCE, "derived", dependencies=dependencies)
        with heartbeat_worker(engine, run_id):
            if not symbols:
                outcome = CollectionOutcome.failure(
                    SOURCE, "derived", "missing_upstream"
                )
                return import_collection(engine, outcome, run_id=run_id)
            with engine.connect() as conn:
                news = news_items(conn, symbols, start, now, table)
                posts: dict[str, list[Item]] = {}
                if social_fresh and social is not None:
                    rows = batch_rows(conn, "social_posts", social.batch_id)
                    posts = social_items(rows, social.source_as_of, now, table)
            records = [
                weigh(
                    symbol,
                    news.items.get(symbol, []) + posts.get(symbol, []),
                    table.version,
                    news.model,
                )
                for symbol in symbols
            ]
            scored = sum(r["article_count"] > 0 for r in records)
            outcome = CollectionOutcome.success(
                SOURCE,
                "derived",
                records,
                coverage={"complete": True, "scope": "derivation"},
                envelope={
                    "weights_version": table.version,
                    "window_days": WINDOW_DAYS,
                    "universe": len(symbols),
                    "scored": scored,
                    "no_data_count": len(symbols) - scored,
                    "news_used": sum(r["news_count"] for r in records),
                    "social_used": sum(r["social_count"] for r in records),
                    "social_batch_fresh": social_fresh,
                    "publishers_unknown": news.publishers_unknown,
                    "news_off_topic": news.off_topic,
                    "half_life_days": table.half_life_days,
                },
            )
            return import_collection(engine, outcome, run_id=run_id)


@dataclass
class SentimentRun:
    results: dict[str, ImportResult] = field(default_factory=dict)
    seconds: dict[str, float] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        """The weighting stored its batch; a missing social file is not a failure."""
        result = self.results.get(SOURCE)
        return result is not None and result.succeeded


def run_sentiment(
    engine: Engine,
    news_client: FinnhubNewsClient,
    resolver: PublisherResolver,
    *,
    score_articles: Callable[[Engine], ImportResult] | None = None,
    now: datetime | None = None,
) -> SentimentRun:
    """A, B, the publisher lookup, the social import and C, timed (AC 4)."""
    now = now or datetime.now(timezone.utc)
    run = SentimentRun()
    steps: list[tuple[str, Callable[[], Any]]] = [
        ("news_articles", lambda: run_news(engine, news_client, now.date())),
    ]
    if score_articles is not None:
        steps.append(("article_sentiment", lambda: score_articles(engine)))
    steps += [
        (
            "news_publishers",
            lambda: resolve_publishers(engine, resolver, now, window_days=WINDOW_DAYS),
        ),
        ("social_posts", lambda: run_social(engine, now, window_days=WINDOW_DAYS)),
        (SOURCE, lambda: derive_sentiment(engine, now=now)),
    ]
    for name, step in steps:
        began = time.monotonic()
        try:
            result = step()
        except Exception as exc:  # one step failing never stops the next
            logger.error("sentiment step=%s failed: %s", name, type(exc).__name__)
            result = None
        run.seconds[name] = round(time.monotonic() - began, 1)
        if isinstance(result, ImportResult):
            run.results[name] = result
    logger.info(
        "sentiment total_seconds=%.1f steps=%s statuses=%s",
        sum(run.seconds.values()),
        run.seconds,
        {k: v.status for k, v in run.results.items()},
    )
    return run


def run_once(settings: Any = None) -> int:
    """Scheduler entry point: exit code 0 when the weighting stored its batch."""
    from src.db.session import make_engine
    from src.ingestion.redaction import describe_exception
    from src.settings import load_settings

    engine = None
    try:
        settings = load_settings() if settings is None else settings
        settings.require_provider_keys("finnhub")
        engine = make_engine(settings.database_url.get_secret_value())
        news_session, lookup_session = requests.Session(), requests.Session()
        try:
            run = run_sentiment(
                engine,
                FinnhubNewsClient(
                    news_session, settings.finnhub_api_key.get_secret_value()
                ),
                PublisherResolver(lookup_session),
                # S1-04 (B) plugs in here: score_articles=<its run function>.
            )
        finally:
            news_session.close()
            lookup_session.close()
        return 0 if run.succeeded else 1
    except Exception as exc:
        logger.error("sentiment run failed: %s", describe_exception(exc))
        return 1
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    from src.utils.logger import configure_logging

    configure_logging()
    sys.exit(run_once())
