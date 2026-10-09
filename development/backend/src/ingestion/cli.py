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
import json
import sys
from dataclasses import asdict
from typing import Any, List, Optional

from src.db.config import DatabaseConfigError
from src.db.models import SOURCES
from src.db.queries import batch_rows, latest_successful_run
from src.db.session import make_engine
from src.ingestion.redaction import describe_exception


def _print(obj: Any) -> None:
    print(json.dumps(obj, indent=2, default=str))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.ingestion.cli")
    sub = parser.add_subparsers(dest="command", required=True)

    imp = sub.add_parser("import-file", help="import an existing fetcher JSON file")
    imp.add_argument("source", choices=SOURCES)
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
    fetch.add_argument("source", choices=SOURCES)

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
    args = _parser().parse_args(argv)
    try:
        engine = make_engine()
    except DatabaseConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(
            f"error: database engine could not be created: {describe_exception(exc)}",
            file=sys.stderr,
        )
        return 2

    try:
        # Imported here so `--help` and config errors do not import the fetchers.
        from src.ingestion.importer import import_collection

        if args.command == "run-pipeline":
            from src.pipeline.runner import run_pipeline

            _print({"publication_id": run_pipeline(engine)})
            return 0
        if args.command == "current":
            from src.pipeline.publication import current

            _print(current(engine))
            return 0
        if args.command == "replay":
            from src.pipeline.publication import replay

            _print(replay(engine, args.publication_id))
            return 0
        if args.command == "health":
            from src.pipeline.health import health

            report = health(engine)
            _print(report)
            return 0 if report["healthy"] else 1
        if args.command == "recover-stale":
            from datetime import timedelta

            from src.pipeline.lifecycle import abandon_stale_runs

            _print(
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
                    _print({"source": args.source, "latest": None})
                    return 0
                rows = batch_rows(
                    conn,
                    args.source,
                    run.batch_id,
                    limit=max(args.limit, 0),
                    offset=args.offset,
                    symbol=args.symbol,
                )
            _print(
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

            result = collect_source(engine, args.source)
            _print(asdict(result))
            return 0 if result.succeeded else 1
        result = import_collection(engine, outcome)
    except Exception as exc:
        print(
            f"error: database operation failed: {describe_exception(exc)}",
            file=sys.stderr,
        )
        return 2
    finally:
        engine.dispose()

    _print(asdict(result))
    return 0 if result.succeeded else 1


if __name__ == "__main__":
    sys.exit(main())
