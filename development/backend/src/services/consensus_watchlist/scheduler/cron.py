"""
Perennial — Pipeline Scheduler
File: services/consensus_watchlist/scheduler/cron.py

PURPOSE:
    Single entry point to run the entire Consensus Watchlist pipeline.
    Either runs on an automatic schedule OR runs everything now (--test).

PIPELINE ORDER:
    1. fmp.py            → Congress trades       → trades_congress.json
    2. ark.py            → ARK ETF holdings      → ark_holdings.json
    3. insider.py        → Insider buys          → trades_insider.json
    4. short_interest.py → FINRA short interest  → short_interest.json
    5. consensus.py      → Cross-reference/rank  → consensus_watchlist.json

SCHEDULE (automatic mode):
    9:00am daily     → fmp → ark → insider → consensus
    9:10am 1st/15th  → short_interest (FINRA publishes bi-weekly)

HOW TO RUN:
    Test everything now:
        py scheduler/cron.py --test

    Run on schedule (leave running in background):
        py scheduler/cron.py

    Ctrl+C to stop.

DEPENDENCIES:
    pip install apscheduler
"""

import sys
from pathlib import Path
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# ─────────────────────────────────────────────────────────
# PATH SETUP
# Add fetchers/ and consensus_watchlist/ to import path
# ─────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parents[1]  # consensus_watchlist/
FETCHERS_DIR = BASE_DIR / "fetchers"

sys.path.insert(0, str(FETCHERS_DIR))  # so we can: import fmp, ark, ...
sys.path.insert(0, str(BASE_DIR))  # so we can: import consensus

# ─────────────────────────────────────────────────────────
# IMPORT PIPELINE MODULES
# ─────────────────────────────────────────────────────────

import fmp
import ark
import insider

# Aalind's files — import defensively in case names differ
try:
    import short_interest
except ImportError:
    short_interest = None
    print("[cron] ⚠️  short_interest.py not found — will skip")

try:
    import consensus
except ImportError:
    consensus = None
    print("[cron] ⚠️  consensus.py not found — will skip")


# ─────────────────────────────────────────────────────────
# JOB WRAPPERS
# ─────────────────────────────────────────────────────────

def _banner(name):
    print(f"\n[cron] ─────────────────────────────────────")
    print(f"[cron] {name} — {datetime.now()}")
    print(f"[cron] ─────────────────────────────────────")


def run_fmp():
    _banner("Running fmp.py")
    try:
        fmp.run()
    except Exception as e:
        print(f"[cron] ❌ fmp.py failed: {e}")


def run_ark():
    _banner("Running ark.py")
    try:
        ark.run()
    except Exception as e:
        print(f"[cron] ❌ ark.py failed: {e}")


def run_insider():
    _banner("Running insider.py")
    try:
        insider.run()
    except Exception as e:
        print(f"[cron] ❌ insider.py failed: {e}")


def run_short_interest():
    if short_interest is None:
        print("[cron] ⚠️  short_interest.py not available — skipping")
        return
    _banner("Running short_interest.py")
    try:
        short_interest.run()
    except Exception as e:
        print(f"[cron] ❌ short_interest.py failed: {e}")


def run_consensus():
    if consensus is None:
        print("[cron] ⚠️  consensus.py not available — skipping")
        return
    _banner("Running consensus.py")
    try:
        consensus.run()
    except Exception as e:
        print(f"[cron] ❌ consensus.py failed: {e}")


# ─────────────────────────────────────────────────────────
# FULL PIPELINE
# ─────────────────────────────────────────────────────────

def run_full_pipeline(include_short_interest=False):
    """
    Runs the complete pipeline in order.
    short_interest only runs bi-weekly so it's optional here.
    """
    print(f"\n[cron] ═════════════════════════════════════")
    print(f"[cron] FULL PIPELINE START — {datetime.now()}")
    print(f"[cron] ═════════════════════════════════════")

    run_fmp()
    run_ark()
    run_insider()
    if include_short_interest:
        run_short_interest()
    run_consensus()

    print(f"\n[cron] ═════════════════════════════════════")
    print(f"[cron] FULL PIPELINE DONE — {datetime.now()}")
    print(f"[cron] ═════════════════════════════════════\n")


# ─────────────────────────────────────────────────────────
# SCHEDULER (automatic mode)
# ─────────────────────────────────────────────────────────

def main():
    scheduler = BlockingScheduler(timezone="America/Los_Angeles")

    # Daily pipeline at 9:00am PT
    scheduler.add_job(
        run_full_pipeline,
        CronTrigger(hour=9, minute=0),
        id="daily_pipeline",
        name="Daily pipeline (fmp→ark→insider→consensus)",
        replace_existing=True,
    )

    # Short interest at 9:10am on 1st and 15th
    scheduler.add_job(
        run_short_interest,
        CronTrigger(day="1,15", hour=9, minute=10),
        id="short_interest",
        name="Short interest (FINRA bi-weekly)",
        replace_existing=True,
    )

    print("\n" + "=" * 55)
    print("  PERENNIAL — Pipeline Scheduler")
    print("=" * 55)
    print("  Daily 9:00am PT   → fmp → ark → insider → consensus")
    print("  1st+15th 9:10am   → short_interest")
    print("  Ctrl+C to stop")
    print("=" * 55 + "\n")

    for job in scheduler.get_jobs():
        print(f"  [{job.name}]")
    print()

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\n[cron] Scheduler stopped.")
        scheduler.shutdown()


# ─────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        print("[cron] TEST MODE — running full pipeline now")
        # In test mode, include short_interest so we test everything
        run_full_pipeline(include_short_interest=True)
    else:
        main()