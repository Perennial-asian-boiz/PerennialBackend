"""Supervised daily database pipeline. --test executes one complete live run.

Every stage uses the shared collectors and importer; publication occurs only
when all required sources meet coverage, lineage and freshness rules. JSON-only
experimentation remains available via consensus.py, outside this production path.
"""
import logging
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[4]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from src.db.session import make_engine
from src.ingestion.redaction import describe_exception
from src.pipeline.runner import run_pipeline

logger = logging.getLogger(__name__)


def run_full_pipeline(include_short_interest=True):
    # Always refresh the selected universe, even when settlement observations
    # themselves have not changed. An old short-interest plan can miss new tickers.
    engine = make_engine()
    try:
        return run_pipeline(engine)
    except Exception as exc:
        logger.error("pipeline failed: %s", describe_exception(exc))
        raise
    finally:
        engine.dispose()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    scheduler = BlockingScheduler(timezone="America/Los_Angeles")
    scheduler.add_job(run_full_pipeline,
        CronTrigger(hour=9, minute=0, timezone="America/Los_Angeles"),
        id="daily_pipeline", replace_existing=True, max_instances=1,
        coalesce=True, misfire_grace_time=3600)
    # Perform one recovery/catch-up run whenever a supervisor starts this worker.
    # PostgreSQL locks prevent overlapping source work across worker processes.
    try:
        run_full_pipeline()
    except Exception:
        pass  # Logged above; remain supervised and retry at the next schedule.
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    if "--test" in sys.argv:
        try:
            run_full_pipeline()
        except Exception:
            sys.exit(1)
    else:
        main()
