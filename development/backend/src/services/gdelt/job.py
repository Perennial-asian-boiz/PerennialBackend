"""Daily GDELT macro-event run. Run once with: python -m src.services.gdelt.job

Through the ingestion contract (services/gdelt/contract.py): theme volume and
evidence articles are collected as immutable batches with run lineage, then the
spike rule runs as a derived run over the latest good volume batch.
"""

import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

import requests
from sqlalchemy.engine import Engine
from src.services.gdelt.client import DocApiClient
from src.services.gdelt.contract import run_contract
from src.services.gdelt.themes import ThemeMap, load_theme_map
from src.utils.logger import configure_logging

logger = logging.getLogger(__name__)


@dataclass
class RunSummary:
    """Per contract source: which succeeded, which failed with a fixed code."""

    day: date
    succeeded: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)
    events: list[str] = field(default_factory=list)


def run_gdelt(
    engine: Engine,
    *,
    client: DocApiClient | None = None,
    theme_map: ThemeMap | None = None,
    now: datetime | None = None,
) -> RunSummary:
    now = now or datetime.now(timezone.utc)
    theme_map = theme_map or load_theme_map()
    owned = client is None
    client = client or DocApiClient(requests.Session())
    try:
        run = run_contract(engine, client, theme_map, now)
    finally:
        if owned:
            client.session.close()
    summary = RunSummary(day=run.day, events=run.events)
    for source, result in run.results.items():
        if result.succeeded:
            summary.succeeded.append(source)
        else:
            summary.failed[source] = result.error_code or "collection_failed"
    logger.info(
        "gdelt run day=%s ok=%d failed=%d events=%s",
        summary.day,
        len(summary.succeeded),
        len(summary.failed),
        summary.events,
    )
    return summary


def run_scheduled() -> None:
    """Scheduler entry point: own the engine, log failures, never take the scheduler down."""
    from src.db.session import make_engine
    from src.ingestion.redaction import describe_exception

    engine = None
    try:
        engine = make_engine()
        run_gdelt(engine)
    except Exception as exc:
        logger.error("gdelt run failed: %s", describe_exception(exc))
    finally:
        if engine is not None:
            engine.dispose()


def main() -> int:
    from src.db.session import make_engine

    configure_logging()
    engine = make_engine()
    try:
        summary = run_gdelt(engine)
    finally:
        engine.dispose()
    # Sources and fixed codes only; any failed source fails the run.
    logger.info(
        "gdelt_complete day=%s succeeded=%s failed=%s events=%s",
        summary.day,
        summary.succeeded,
        summary.failed,
        summary.events,
    )
    return 1 if summary.failed else 0


if __name__ == "__main__":
    sys.exit(main())
