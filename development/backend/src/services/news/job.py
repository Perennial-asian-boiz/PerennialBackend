"""S1-03 news ingestion through the ingestion contract. Run once with:

    python -m src.pipeline.scheduler --once --job news

For every ticker in the current production watchlist publication (the consensus
list), fetch the last WINDOW_DAYS of Finnhub company news, keep the articles that
name the company (matching.py, match-v1), and store each (security, article)
pair once: a run's batch holds only the pairs not stored before, and the table's
UNIQUE (security_id, finnhub_id) rejects any duplicate (AC 4).

Each ticker also gets a news_coverage row for the run: 'covered' with counts,
'no_coverage' when nothing matched, or 'error' with a fixed code when Finnhub
failed for it, for example rate limiting (http_429). A failing ticker never
stops the run (AC 5); the batch is then marked incomplete.
"""

import hashlib
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from sqlalchemy import insert, select
from sqlalchemy.engine import Connection, Engine
from src.db import models as m
from src.ingestion.diagnostics import exception_diagnostics, failure_code
from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection
from src.pipeline.lifecycle import begin_run, heartbeat_worker, source_lock
from src.services.fundamentals.job import current_universe
from src.services.news.finnhub import FinnhubError, FinnhubNewsClient, RawArticle
from src.services.news.matching import MATCH_RULE_VERSION, match_rule, name_variants
from src.utils.logger import log_exception

logger = logging.getLogger(__name__)

SOURCE = "news_articles"
WINDOW_DAYS = 7


@dataclass
class Coverage:
    symbol: str
    status: str
    articles_found: int = 0
    articles_new: int = 0
    error_code: str | None = None


@dataclass
class Collected:
    outcome: CollectionOutcome
    coverage: list[Coverage] = field(default_factory=list)


def company_names(conn: Connection, symbols: list[str]) -> dict[str, list[str]]:
    """Security name, legal name and alias names per symbol, for match-v1."""
    s, p = m.securities, m.company_profiles
    rows = conn.execute(
        select(s.c.symbol, s.c.name, p.c.legal_name, p.c.alias_names)
        .select_from(s.outerjoin(p, p.c.id == s.c.company_profile_id))
        .where(s.c.symbol.in_(symbols))
    ).all()
    names: dict[str, list[str]] = {}
    for row in rows:
        names[row.symbol] = [
            n for n in (row.name, row.legal_name, *(row.alias_names or [])) if n
        ]
    return names


def stored_ids(conn: Connection, symbols: list[str]) -> dict[str, set[int]]:
    """Finnhub ids already stored per symbol, across every earlier batch."""
    rows = conn.execute(
        select(m.securities.c.symbol, m.news_articles.c.finnhub_id)
        .join(m.securities, m.securities.c.id == m.news_articles.c.security_id)
        .where(m.securities.c.symbol.in_(symbols))
    ).all()
    out: dict[str, set[int]] = {}
    for symbol, fid in rows:
        out.setdefault(symbol, set()).add(fid)
    return out


def _record(symbol: str, article: RawArticle, rule: str) -> dict[str, Any]:
    digest = hashlib.sha256(
        f"{article.headline}\n{article.summary}".encode()
    ).hexdigest()
    return {
        "ticker": symbol,
        "finnhub_id": article.finnhub_id,
        "url": article.url,
        "headline": article.headline,
        "outlet": article.outlet,
        "published_at": article.published_at,
        "content_retrieved": "headline_and_summary"
        if article.summary
        else "headline_only",
        "match_rule": rule,
        "content_hash": digest,
    }


def collect_news(
    client: FinnhubNewsClient,
    symbols: list[str],
    names: dict[str, list[str]],
    known: dict[str, set[int]],
    today: date,
) -> Collected:
    start = today - timedelta(days=WINDOW_DAYS)
    records: list[dict[str, Any]] = []
    coverage: list[Coverage] = []
    for symbol in symbols:
        try:
            articles, _skipped = client.company_news(symbol, start, today)
        except FinnhubError as exc:
            coverage.append(
                Coverage(symbol, "error", error_code=failure_code(str(exc)))
            )
            log_exception(logger, "news_ticker_failed", exc)  # class name only
            continue
        variants = name_variants(names.get(symbol, []))
        seen: set[int] = set()
        found = new = 0
        for article in articles:
            if article.finnhub_id in seen:
                continue
            seen.add(article.finnhub_id)
            rule = match_rule(article.headline, article.summary, symbol, variants)
            if rule is None:
                continue
            found += 1
            if article.finnhub_id in known.get(symbol, set()):
                continue
            new += 1
            records.append(_record(symbol, article, rule))
        coverage.append(
            Coverage(symbol, "covered" if found else "no_coverage", found, new)
        )
    errors = [c.symbol for c in coverage if c.status == "error"]
    no_coverage = [c.symbol for c in coverage if c.status == "no_coverage"]
    if symbols and len(errors) == len(symbols):
        outcome = CollectionOutcome.failure(
            SOURCE,
            "live",
            "collection_failed",
            diagnostics={
                "unit_kind": "news tickers",
                "planned": len(symbols),
                "attempted": len(symbols),
                "completed": 0,
                "failed": len(errors),
                "failures": [
                    {"unit": c.symbol, "code": c.error_code}
                    for c in coverage
                    if c.error_code
                ],
            },
        )
        return Collected(outcome, coverage)
    records.sort(key=lambda r: (r["ticker"], r["published_at"], r["finnhub_id"]))
    outcome = CollectionOutcome.success(
        SOURCE,
        "live",
        records,
        source_as_of=today,
        coverage={"complete": not errors, "scope": "selected_universe"},
        envelope={
            "source": "finnhub_company_news",
            "window_days": WINDOW_DAYS,
            "universe": len(symbols),
            "covered": sum(c.status == "covered" for c in coverage),
            "no_coverage": no_coverage,
            "no_coverage_count": len(no_coverage),
            "errors": errors,
            "errors_count": len(errors),
            "articles_found": sum(c.articles_found for c in coverage),
            "articles_new": len(records),
            "match_rule_version": MATCH_RULE_VERSION,
        },
    )
    return Collected(outcome, coverage)


def _write_coverage(engine: Engine, run_id: int, coverage: list[Coverage]) -> None:
    if not coverage:
        return
    with engine.begin() as conn:
        ids = dict(
            conn.execute(
                select(m.securities.c.symbol, m.securities.c.id).where(
                    m.securities.c.symbol.in_([c.symbol for c in coverage])
                )
            )
            .tuples()
            .all()
        )
        rows = [
            {
                "run_id": run_id,
                "security_id": ids[c.symbol],
                "status": c.status,
                "articles_found": c.articles_found,
                "articles_new": c.articles_new,
                "error_code": c.error_code,
            }
            for c in coverage
            if c.symbol in ids
        ]
        if rows:
            conn.execute(insert(m.news_coverage), rows)


def run_news(
    engine: Engine, client: FinnhubNewsClient, today: date | None = None
) -> ImportResult:
    """One news ingestion run: collect, import the new pairs, record coverage."""
    today = today or datetime.now(timezone.utc).date()
    with source_lock(engine, SOURCE):
        with engine.connect() as conn:
            universe = current_universe(conn)
            symbols = universe.symbols if universe else []
            names = company_names(conn, symbols)
            known = stored_ids(conn, symbols)
        dependencies = dict(universe.dependencies) if universe else {}
        run_id = begin_run(engine, SOURCE, "live", dependencies=dependencies)
        collected = Collected(
            CollectionOutcome.failure(SOURCE, "live", "missing_upstream")
        )
        with heartbeat_worker(engine, run_id):
            if symbols:
                try:
                    collected = collect_news(client, symbols, names, known, today)
                except (
                    Exception
                ) as exc:  # a bug in the collector: record it, write nothing
                    code = (
                        "collection_failed"
                        if isinstance(exc, requests.RequestException)
                        else "internal_error"
                    )
                    collected = Collected(
                        CollectionOutcome.failure(
                            SOURCE, "live", code, diagnostics=exception_diagnostics(exc)
                        )
                    )
            result = import_collection(engine, collected.outcome, run_id=run_id)
        _write_coverage(engine, run_id, collected.coverage)
    statuses = [c.status for c in collected.coverage]
    logger.info(
        "news run_id=%s status=%s code=%s covered=%d no_coverage=%d errors=%d new=%s",
        result.run_id,
        result.status,
        result.error_code,
        statuses.count("covered"),
        statuses.count("no_coverage"),
        statuses.count("error"),
        result.accepted_count,
    )
    return result


def run_once(settings: Any = None) -> int:
    """Scheduler entry point: exit code 0 when the run's batch was stored."""
    from src.db.session import make_engine
    from src.ingestion.redaction import describe_exception
    from src.settings import load_settings

    engine = None
    try:
        settings = load_settings() if settings is None else settings
        settings.require_provider_keys("finnhub")
        engine = make_engine(settings.database_url.get_secret_value())
        client = FinnhubNewsClient(
            requests.Session(), settings.finnhub_api_key.get_secret_value()
        )
        try:
            result = run_news(engine, client)
        finally:
            client.session.close()
        return 0 if result.succeeded else 1
    except Exception as exc:
        logger.error("news run failed: %s", describe_exception(exc))
        return 1
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    from src.utils.logger import configure_logging

    configure_logging()
    sys.exit(run_once())
