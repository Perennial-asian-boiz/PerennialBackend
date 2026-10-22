"""Daily database pipeline entry points.

Production (roadmap M3): a platform scheduler runs one job per invocation,

    python -m src.pipeline.scheduler --once            # the database pipeline
    python -m src.pipeline.scheduler --once --job gdelt

at 09:00 America/Los_Angeles (pipeline) and 09:30 (GDELT) on trading days. The
exit code is the success signal: 0 when the job completed, 1 otherwise,
including when another run already holds the pipeline lock, so a skipped
overlapping run alerts instead of passing as success. Missed runs and missed
publications are reported by `cli health` (pipeline:missed_run,
publication:missed).

Without --once this starts the legacy supervised APScheduler daemon, kept until
the platform scheduler is cut over. --test executes one complete LIVE run, not
an offline test.

Every stage uses the shared collectors and importer; publication occurs only
when all required sources meet coverage, lineage and freshness rules. JSON-only
experimentation remains available via consensus.py, outside this production path.
"""

import logging
import sys
from functools import partial

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from src.db.session import make_engine
from src.ingestion.redaction import describe_exception
from src.pipeline.runner import run_pipeline
from src.services.gdelt.job import run_scheduled as run_gdelt_job
from src.settings import Settings, load_settings

logger = logging.getLogger(__name__)


class PipelineRunFailed(RuntimeError):
    """Safe failure passed to scheduler executors and their event listeners."""

    def __init__(self) -> None:
        super().__init__("database pipeline run failed")


def run_full_pipeline(settings: Settings | None = None) -> int | None:
    # Always refresh the selected universe, even when settlement observations
    # themselves have not changed. An old short-interest plan can miss new tickers.
    engine, failed = None, False
    try:
        settings = load_settings() if settings is None else settings
        settings.require_provider_keys("fmp")
        settings.require_provider_keys("finnhub")
        engine = make_engine(
            settings.database_url.get_secret_value(), workload="pipeline"
        )
        return run_pipeline(
            engine, quality_thresholds=settings.quality_thresholds, settings=settings
        )
    except Exception as exc:
        logger.error("pipeline failed: %s", describe_exception(exc))
        failed = True
    finally:
        if engine is not None:
            try:
                engine.dispose()
            except Exception as exc:
                logger.error("engine dispose failed: %s", describe_exception(exc))
    if failed:
        # Outside the handler: listeners cannot traverse a raw exception context.
        raise PipelineRunFailed()
    return None


def run_once(job: str = "pipeline", settings: Settings | None = None) -> int:
    """Run one scheduled job and return its process exit code."""
    if job == "pipeline":
        try:
            publication_id = run_full_pipeline(settings)
        except PipelineRunFailed:
            return 1
        logger.info(
            "scheduled pipeline run published publication_id=%s", publication_id
        )
        return 0
    if job == "gdelt":
        return _run_gdelt_once(settings)
    raise ValueError("unknown scheduled job")


GDELT_INCOMPLETE = "gdelt_incomplete"


def _run_gdelt_once(settings: Settings | None) -> int:
    """Any failed theme fails the scheduled job; only counts and codes are logged."""
    from src.services.gdelt.job import run_gdelt

    engine = None
    try:
        engine = (
            make_engine(settings.database_url.get_secret_value())
            if settings is not None
            else make_engine()
        )
        summary = run_gdelt(engine)
    except Exception as exc:
        logger.error("gdelt run failed: %s", describe_exception(exc))
        return 1
    finally:
        if engine is not None:
            engine.dispose()
    if summary.failed:
        logger.error(
            "gdelt run incomplete code=%s ok=%d failed=%d",
            GDELT_INCOMPLETE,
            len(summary.succeeded),
            len(summary.failed),
        )
        return 1
    return 0


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    settings = load_settings()
    job = partial(run_full_pipeline, settings)
    scheduler = BlockingScheduler(timezone="America/Los_Angeles")
    scheduler.add_job(
        job,
        CronTrigger(hour=9, minute=0, timezone="America/Los_Angeles"),
        id="daily_pipeline",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        run_gdelt_job,
        CronTrigger(hour=9, minute=30, timezone="America/Los_Angeles"),
        id="gdelt_daily",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    # Perform one recovery/catch-up run whenever a supervisor starts this worker.
    # PostgreSQL locks prevent overlapping source work across worker processes.
    try:
        job()
    except Exception:
        pass  # Logged above; remain supervised and retry at the next schedule.
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    if "--once" in sys.argv:
        import argparse

        parser = argparse.ArgumentParser(prog="python -m src.pipeline.scheduler")
        parser.add_argument("--once", action="store_true", required=True)
        parser.add_argument("--job", choices=("pipeline", "gdelt"), default="pipeline")
        args = parser.parse_args()  # exits 2 on a missing or unknown --job
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
        )
        sys.exit(run_once(args.job))
    if "--test" in sys.argv:
        try:
            run_full_pipeline()
        except Exception:
            sys.exit(1)
    else:
        main()
