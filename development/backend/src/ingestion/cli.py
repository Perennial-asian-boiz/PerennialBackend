"""
Command line for imports and inspection. Run from development/backend:

    python -m src.ingestion.cli import-file ark_holdings path/to/ark_holdings.json --fixture
    python -m src.ingestion.cli import-file ark_holdings ../database/local_data/ark_holdings.json --attest-complete
    python -m src.ingestion.cli fetch ark_holdings
    python -m src.ingestion.cli latest ark_holdings --limit 5

Exit codes: 0 succeeded, 1 import recorded as failed or unhealthy report,
2 command/configuration/database failure. Earlier pipeline stages may already
have recorded runs when a later stage fails. Output never includes the
database URL, request URLs or exception messages.
"""

import argparse
import getpass
import json
import logging
import sys
from dataclasses import asdict
from typing import Any, List, Optional

from src.db.models import SOURCES, WATCHLIST_SOURCES
from src.db.queries import batch_rows, latest_successful_run
from src.db.session import make_engine
from src.ingestion.redaction import redact
from src.settings import SettingsError, load_settings
from src.utils.logger import configure_logging, log_context, log_exception

logger = logging.getLogger(__name__)


def emit(obj: Any) -> None:
    """Write command results to stdout; diagnostics belong on stderr."""
    sys.stdout.write(json.dumps(obj, indent=2, default=str) + "\n")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.ingestion.cli")
    sub = parser.add_subparsers(dest="command", required=True)

    imp = sub.add_parser("import-file", help="import an existing fetcher JSON file")
    imp.add_argument("source", choices=WATCHLIST_SOURCES)
    imp.add_argument("path")
    imp.add_argument(
        "--fixture",
        action="store_true",
        help="the file is a synthetic/sanitized fixture (recorded as mode=fixture)",
    )
    imp.add_argument(
        "--attest-complete",
        action="store_true",
        help="assert the collection that wrote this file finished without errors "
        "(required for fetcher output files and for empty inputs)",
    )

    fetch = sub.add_parser(
        "fetch", help="collect live through the explicit-outcome wrapper and import"
    )
    fetch.add_argument("source", choices=WATCHLIST_SOURCES)

    latest = sub.add_parser(
        "latest", help="show the latest successful batch for a source"
    )
    latest.add_argument("source", choices=SOURCES)
    latest.add_argument("--limit", type=int, default=10)
    latest.add_argument("--offset", type=int, default=0)
    latest.add_argument("--symbol")
    sub.add_parser(
        "run-pipeline",
        help="collect all sources from database inputs and publish atomically",
    )
    manual = sub.add_parser(
        "publish", help="publish the latest successful live batches"
    )
    manual.add_argument(
        "--override", help="reason for overriding the named failed quality checks"
    )
    manual.add_argument(
        "--override-check",
        dest="override_checks",
        action="append",
        metavar="SOURCE:CHECK",
        help="a failed check to override, as reported (repeatable; needs --override)",
    )
    manual.add_argument("--actor", help="operator identity for the override audit")
    back = sub.add_parser(
        "rollback", help="serve an earlier publication again, with an audited reason"
    )
    back.add_argument("--to", dest="to_publication_id", type=int, required=True)
    back.add_argument("--reason", required=True, help="why production goes back")
    back.add_argument("--actor", help="operator identity for the rollback audit")
    sub.add_parser("current", help="show the published watchlist")
    sub.add_parser("health", help="machine-readable health; exits 1 when unhealthy")
    replay = sub.add_parser(
        "replay", help="recompute a saved publication without network access"
    )
    replay.add_argument("publication_id", type=int)
    recover = sub.add_parser(
        "recover-stale", help="mark workers with expired heartbeats abandoned"
    )
    recover.add_argument("--minutes", type=int, default=10)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    configure_logging()
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command == "publish" and bool(args.override) != bool(args.override_checks):
        parser.error("--override and --override-check must be given together")
    with log_context(
        source=getattr(args, "source", None),
        publication_id=getattr(args, "publication_id", None),
    ):
        return _execute(args)


def _execute(args: argparse.Namespace) -> int:
    try:
        settings = load_settings()
        if args.command == "run-pipeline":
            settings.require_provider_keys("fmp")
            settings.require_provider_keys("finnhub")
        elif args.command == "publish":
            settings.require_provider_keys("fmp")
        elif args.command == "fetch":
            if args.source == "congress_trades" and (
                settings.congress_house_source == "fmp"
                or settings.congress_include_fmp_senate
            ):
                # House Clerk alone needs no key; FMP chambers do.
                settings.require_provider_keys("fmp")
            elif args.source == "insider_trades":
                settings.require_provider_keys("finnhub")
        engine = make_engine(
            settings.database_url.get_secret_value(),
            workload="pipeline" if args.command == "run-pipeline" else "cli",
        )
    except SettingsError as exc:
        # SettingsError contains only setting names, never rejected values.
        logger.error("configuration error: %s", redact(exc))
        return 2
    except Exception as exc:
        log_exception(logger, "database engine could not be created", exc)
        return 2

    try:
        # Imported here so `--help` and config errors do not import the fetchers.
        from src.ingestion.importer import import_collection

        if args.command == "run-pipeline":
            from src.pipeline.runner import run_pipeline

            emit(
                {
                    "publication_id": run_pipeline(
                        engine,
                        quality_thresholds=settings.quality_thresholds,
                        settings=settings,
                    )
                }
            )
            return 0
        if args.command == "publish":
            from src.pipeline.publication import PublicationError, publish
            from src.pipeline.ranking import candidate_symbols
            from src.pipeline.runner import collect_market_caps

            with engine.connect() as conn:
                latest = {
                    source: latest_successful_run(conn, source)
                    for source in WATCHLIST_SOURCES
                }
                if any(run is None for run in latest.values()):
                    raise PublicationError("all sources need a successful batch")
                run_ids = {
                    source: run.run_id
                    for source, run in latest.items()
                    if run is not None
                }
                publish_rows = {
                    source: batch_rows(conn, source, run.batch_id)
                    for source, run in latest.items()
                    if run is not None
                }
            observations = collect_market_caps(
                candidate_symbols(publish_rows),
                api_key=settings.fmp_api_key.get_secret_value()
                if settings.fmp_api_key is not None
                else None,
            )
            actor = (args.actor or getpass.getuser()) if args.override else None
            emit(
                {
                    "publication_id": publish(
                        engine,
                        run_ids,
                        observations,
                        quality_thresholds=settings.quality_thresholds,
                        override_reason=args.override,
                        override_actor=actor,
                        override_checks=args.override_checks,
                    )
                }
            )
            return 0
        if args.command == "rollback":
            from src.pipeline.publication import rollback

            previous = rollback(
                engine,
                args.to_publication_id,
                actor=args.actor or getpass.getuser(),
                reason=args.reason,
            )
            emit(
                {"publication_id": args.to_publication_id, "rolled_back_from": previous}
            )
            return 0
        if args.command == "current":
            from src.pipeline.publication import current

            emit(current(engine))
            return 0
        if args.command == "replay":
            from src.pipeline.publication import replay

            emit(replay(engine, args.publication_id))
            return 0
        if args.command == "health":
            from src.pipeline.health import health

            report = health(engine, settings=settings)
            emit(report)
            return 0 if report["healthy"] else 1
        if args.command == "recover-stale":
            from datetime import timedelta

            from src.pipeline.lifecycle import abandon_stale_runs

            emit(
                {
                    "abandoned": abandon_stale_runs(
                        engine, timedelta(minutes=args.minutes)
                    )
                }
            )
            return 0

        if args.command == "latest":
            with engine.connect() as conn:
                run = latest_successful_run(conn, args.source)
                if run is None:
                    emit({"source": args.source, "latest": None})
                    return 0
                rows = batch_rows(
                    conn,
                    args.source,
                    run.batch_id,
                    limit=max(args.limit, 0),
                    offset=args.offset,
                    symbol=args.symbol,
                )
            emit(
                {
                    "source": args.source,
                    "latest": dict(run._mapping),
                    "row_count": run.record_count,
                    "rows": rows,
                }
            )
            return 0

        if args.command == "import-file":
            from src.ingestion.files import read_fetcher_file

            outcome = read_fetcher_file(
                args.source,
                args.path,
                mode="fixture" if args.fixture else "file",
                attest_complete=args.attest_complete,
            )
        else:
            from src.pipeline.runner import collect_source

            result = collect_source(engine, args.source, settings=settings)
            emit(asdict(result))
            return 0 if result.succeeded else 1
        result = import_collection(engine, outcome)
    except Exception as exc:
        from src.pipeline.publication import (
            OverrideInputError,
            PublicationInProgressError,
            PublicationQualityError,
            RollbackError,
        )
        from src.pipeline.runner import RequiredSourceUnavailableError

        if isinstance(exc, PublicationQualityError):
            emit({"error_code": exc.code, "checks": list(exc.checks)})
            return 1
        if isinstance(exc, (PublicationInProgressError, RollbackError)):
            emit({"error_code": exc.code})
            return 1
        if isinstance(exc, RequiredSourceUnavailableError):
            emit({"error_code": exc.code, "source": exc.source})
            return 1
        if isinstance(exc, OverrideInputError):
            log_exception(logger, exc.code, exc)
            return 2
        log_exception(logger, "database operation failed", exc)
        return 2
    finally:
        engine.dispose()

    emit(asdict(result))
    return 0 if result.succeeded else 1


if __name__ == "__main__":
    sys.exit(main())
