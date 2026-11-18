"""S3-01 price snapshots through the ingestion contract. Run once with:

    python -m src.pipeline.scheduler --once --job prices

For every ticker in the current production watchlist publication (the consensus
list), store one daily_prices row: Finnhub's latest price as the close, its
trading date in New York, the previous close, and the 52-week high and low.

Run it after the US close. While the NYSE session is open (market_status, with
its holidays and early closes) the job starts no run and exits 1, since the
latest price would not be a close.

A ticker Finnhub has no price for is listed under no_quote; one without a usable
52-week range is stored without it and listed under no_range. A failing ticker
never stops the run; the batch is then marked incomplete. Every ticker failing
fails the run.
"""

import logging
import sys
from datetime import datetime, timezone
from typing import Any

import requests
from pydantic import ValidationError
from src.api.market_status import ET
from src.api.market_status import status as market_status
from src.ingestion.diagnostics import exception_diagnostics, failure_code
from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection
from src.ingestion.schemas import DailyPrice
from src.pipeline.lifecycle import begin_run, heartbeat_worker, source_lock
from src.services.fundamentals.job import current_universe
from src.services.news.finnhub import FinnhubError
from src.services.prices.finnhub import FinnhubPriceClient
from src.utils.logger import log_exception

logger = logging.getLogger(__name__)

SOURCE = "daily_prices"
PROVIDER = "finnhub"
CURRENCY = "USD"  # Finnhub's free plan quotes US listings only


def collect_prices(client: FinnhubPriceClient, symbols: list[str]) -> CollectionOutcome:
    records: list[dict[str, Any]] = []
    no_quote: list[str] = []
    no_range: list[str] = []
    failures: list[tuple[str, str]] = []
    for symbol in symbols:
        try:
            quote = client.quote(symbol)
            if quote is None:
                no_quote.append(symbol)
                continue
            span = client.range_52w(symbol)
        except FinnhubError as exc:
            failures.append((symbol, failure_code(str(exc))))
            log_exception(logger, "prices_ticker_failed", exc)  # class name only
            continue
        record: dict[str, Any] = {
            "ticker": symbol,
            "trading_date": quote.quoted_at.astimezone(ET).date(),
            "quoted_at": quote.quoted_at,
            "close": quote.price,
            "previous_close": quote.previous_close,
            "price_currency": CURRENCY,
            "provider": PROVIDER,
        }
        if span is None:
            no_range.append(symbol)
        else:
            record.update(
                high_52w=span.high,
                high_52w_date=span.high_date,
                low_52w=span.low,
                low_52w_date=span.low_date,
            )
        try:
            DailyPrice.model_validate(
                record
            )  # one bad answer must not fail the whole batch
        except ValidationError:
            failures.append((symbol, "malformed_response"))
            continue
        records.append(record)
    errors = [symbol for symbol, _ in failures]
    if symbols and len(errors) == len(symbols):
        return CollectionOutcome.failure(
            SOURCE,
            "live",
            "collection_failed",
            diagnostics={
                "unit_kind": "price tickers",
                "planned": len(symbols),
                "attempted": len(symbols),
                "completed": 0,
                "failed": len(errors),
                "failures": [{"unit": s, "code": c} for s, c in failures],
            },
        )
    return CollectionOutcome.success(
        SOURCE,
        "live",
        records,
        source_as_of=max((r["trading_date"] for r in records), default=None),
        coverage={"complete": not errors, "scope": "selected_universe"},
        envelope={
            "source": "finnhub_quote",
            "universe": len(symbols),
            "priced": len(records),
            "no_quote": no_quote,
            "no_quote_count": len(no_quote),
            "no_range": no_range,
            "no_range_count": len(no_range),
            "errors": errors,
            "errors_count": len(errors),
        },
    )


def run_prices(
    engine: Any, client: FinnhubPriceClient, now: datetime | None = None
) -> ImportResult | None:
    """One snapshot run, or None (no run started) while the market is open."""
    now = now or datetime.now(timezone.utc)
    if market_status(now)[0]["is_open"]:
        logger.warning("prices skipped: market_open")
        return None
    with source_lock(engine, SOURCE):
        with engine.connect() as conn:
            universe = current_universe(conn)
        symbols = universe.symbols if universe else []
        dependencies = dict(universe.dependencies) if universe else {}
        run_id = begin_run(engine, SOURCE, "live", dependencies=dependencies)
        outcome = CollectionOutcome.failure(SOURCE, "live", "missing_upstream")
        with heartbeat_worker(engine, run_id):
            if symbols:
                try:
                    outcome = collect_prices(client, symbols)
                except (
                    Exception
                ) as exc:  # a bug in the collector: record it, write nothing
                    code = (
                        "collection_failed"
                        if isinstance(exc, requests.RequestException)
                        else "internal_error"
                    )
                    outcome = CollectionOutcome.failure(
                        SOURCE, "live", code, diagnostics=exception_diagnostics(exc)
                    )
            result = import_collection(engine, outcome, run_id=run_id)
    logger.info(
        "prices run_id=%s status=%s code=%s priced=%s",
        result.run_id,
        result.status,
        result.error_code,
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
        client = FinnhubPriceClient(
            requests.Session(), settings.finnhub_api_key.get_secret_value()
        )
        try:
            result = run_prices(engine, client)
        finally:
            client.session.close()
        return 0 if result is not None and result.succeeded else 1
    except Exception as exc:
        logger.error("prices run failed: %s", describe_exception(exc))
        return 1
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    from src.utils.logger import configure_logging

    configure_logging()
    sys.exit(run_once())
