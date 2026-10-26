"""Pattern evaluation through the ingestion contract. Run once with:

    python -m src.services.fundamentals.job

Two ordinary ingestion runs, each writing an immutable batch:

* fundamentals (live): SEC EDGAR annual line items for every candidate in the
  current production watchlist publication. run_dependencies names the Congress
  and ARK batches that publication pinned, so the plan is traceable.
* fundamental_scores (derived): the score rule applied to one recorded
  fundamentals batch, named in run_dependencies.

A company with no SEC annual data has no rows and no score; the envelope counts
it. A transport failure for any company fails the fundamentals batch (readers
keep the last good one); the derivation then re-derives from that last good
batch, and an identical result reuses its batch.
"""

import logging
import sys
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable

import requests
from sqlalchemy import select
from sqlalchemy.engine import Connection, Engine
from src.db import models as m
from src.db.queries import batch_rows, latest_successful_run
from src.ingestion.diagnostics import exception_diagnostics, failure_code
from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection
from src.pipeline.lifecycle import begin_run, heartbeat_worker, source_lock
from src.services.fundamentals.edgar import (
    FISCAL_YEARS_KEPT,
    EdgarClient,
    EdgarError,
    annual_values,
)
from src.services.fundamentals.scoring import ScoreRule, score_security
from src.utils.logger import log_exception

logger = logging.getLogger(__name__)

FUNDAMENTALS, SCORES = "fundamentals", "fundamental_scores"
UNIT_KIND = "SEC filers"
UPSTREAM = ("congress_trades", "ark_holdings")


@dataclass(frozen=True)
class Universe:
    symbols: list[str]
    dependencies: dict[str, int]
    ciks: dict[str, int]  # seeded company_profiles.cik, preferred over SEC's ticker map


def current_universe(conn: Connection) -> Universe | None:
    """Candidates of the current production watchlist publication, with its pins."""
    pub = conn.execute(
        select(m.publications.c.id, m.publications.c.output)
        .join(
            m.current_publication,
            m.current_publication.c.publication_id == m.publications.c.id,
        )
        .where(m.current_publication.c.channel == "production")
    ).one_or_none()
    if pub is None:
        return None
    output = pub.output or {}
    symbols = sorted(
        {
            item["ticker"]
            for bucket in ("popular_stable", "affordable_growing", "unresolved")
            for item in output.get(bucket, [])
            if isinstance(item, dict) and isinstance(item.get("ticker"), str)
        }
    )
    pins = dict(
        conn.execute(
            select(
                m.publication_sources.c.source, m.publication_sources.c.batch_id
            ).where(
                m.publication_sources.c.publication_id == pub.id,
                m.publication_sources.c.source.in_(UPSTREAM),
            )
        )
        .tuples()
        .all()
    )
    ciks: dict[str, int] = {}
    if symbols:
        rows = conn.execute(
            select(m.securities.c.symbol, m.company_profiles.c.cik)
            .join(
                m.company_profiles,
                m.securities.c.company_profile_id == m.company_profiles.c.id,
            )
            .where(
                m.securities.c.symbol.in_(symbols),
                m.company_profiles.c.cik.is_not(None),
            )
        ).tuples()
        ciks = {s: int(c) for s, c in rows if str(c).isdigit()}
    return Universe(symbols, pins, ciks)


def _sec_ticker(symbol: str) -> str:
    return symbol.replace(".", "-")  # SEC writes share classes as BRK-B


def collect_fundamentals(client: EdgarClient, universe: Universe) -> CollectionOutcome:
    """Every candidate's annual line items, or a failed outcome naming the failures."""
    try:
        ticker_map = client.ticker_map()
    except EdgarError as exc:
        return CollectionOutcome.failure(
            FUNDAMENTALS,
            "live",
            "collection_failed",
            diagnostics=exception_diagnostics(exc),
        )
    records: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    no_cik: list[str] = []
    no_data: list[str] = []
    for symbol in universe.symbols:
        cik = universe.ciks.get(symbol) or ticker_map.get(_sec_ticker(symbol))
        if cik is None:
            no_cik.append(symbol)
            continue
        try:
            facts = client.company_facts(cik)
        except EdgarError as exc:
            failures.append({"unit": symbol, "code": failure_code(str(exc))})
            log_exception(logger, "fundamentals_company_failed", exc)  # class name only
            continue
        values = annual_values(facts) if facts is not None else []
        if not values:
            no_data.append(symbol)
            continue
        records.extend(
            {
                "ticker": symbol,
                "fiscal_year_end": v.fiscal_year_end.isoformat(),
                "line_item": v.line_item,
                "value": v.value,
                "unit": v.unit,
                "concept": v.concept,
                "form": v.form,
                "filed": v.filed.isoformat(),
            }
            for v in values
        )
    if failures:
        planned = len(universe.symbols)
        return CollectionOutcome.failure(
            FUNDAMENTALS,
            "live",
            "partial_collection" if len(failures) < planned else "collection_failed",
            diagnostics={
                "unit_kind": UNIT_KIND,
                "planned": planned,
                "attempted": planned,
                "completed": planned - len(failures),
                "failed": len(failures),
                "failures": failures,
            },
        )
    return CollectionOutcome.success(
        FUNDAMENTALS,
        "live",
        records,
        coverage={"complete": True, "scope": "selected_universe"},
        envelope={
            "source": "sec_edgar_companyfacts",
            "fiscal_years": FISCAL_YEARS_KEPT,
            "universe": len(universe.symbols),
            "with_data": len(universe.symbols) - len(no_cik) - len(no_data),
            "no_cik": no_cik,
            "no_cik_count": len(no_cik),
            "no_annual_data": no_data,
            "no_annual_data_count": len(no_data),
        },
    )


def run_collection(engine: Engine, client: EdgarClient) -> ImportResult:
    with source_lock(engine, FUNDAMENTALS):
        with engine.connect() as conn:
            universe = current_universe(conn)
        dependencies = dict(universe.dependencies) if universe else {}
        run_id = begin_run(engine, FUNDAMENTALS, "live", dependencies=dependencies)
        with heartbeat_worker(engine, run_id):
            if universe is None or not universe.symbols:
                outcome = CollectionOutcome.failure(
                    FUNDAMENTALS, "live", "missing_upstream"
                )
            else:
                try:
                    outcome = collect_fundamentals(client, universe)
                except (
                    Exception
                ) as exc:  # a bug in the collector: record it, write nothing
                    code = (
                        "collection_failed"
                        if isinstance(exc, requests.RequestException)
                        else "internal_error"
                    )
                    outcome = CollectionOutcome.failure(
                        FUNDAMENTALS,
                        "live",
                        code,
                        diagnostics=exception_diagnostics(exc),
                    )
            return import_collection(engine, outcome, run_id=run_id)


def _lines_by_security(
    rows: list[dict[str, Any]],
) -> dict[str, dict[str, dict[date, float]]]:
    out: dict[str, dict[str, dict[date, float]]] = {}
    for row in rows:
        out.setdefault(row["symbol"], {}).setdefault(row["line_item"], {})[
            row["fiscal_year_end"]
        ] = float(row["value"])
    return out


def derive_scores(engine: Engine, rule: ScoreRule = ScoreRule()) -> ImportResult:
    """Apply the score rule to the latest good fundamentals batch, as a derived run."""
    with source_lock(engine, SCORES):
        with engine.connect() as conn:
            latest = latest_successful_run(conn, FUNDAMENTALS)
        dependencies = {FUNDAMENTALS: latest.batch_id} if latest is not None else {}
        run_id = begin_run(engine, SCORES, "derived", dependencies=dependencies)
        with heartbeat_worker(engine, run_id):
            if latest is None:
                outcome = CollectionOutcome.failure(
                    SCORES, "derived", "missing_upstream"
                )
                return import_collection(engine, outcome, run_id=run_id)
            with engine.connect() as conn:
                rows = batch_rows(conn, FUNDAMENTALS, latest.batch_id)
            records = []
            for symbol, lines in sorted(_lines_by_security(rows).items()):
                record = score_security(symbol, lines, rule)
                if record is not None:
                    records.append(record)
            outcome = CollectionOutcome.success(
                SCORES,
                "derived",
                records,
                coverage={"complete": True, "scope": "derivation"},
                envelope={**rule.envelope(), "input_batch_id": latest.batch_id},
            )
            return import_collection(engine, outcome, run_id=run_id)


@dataclass
class FundamentalsRun:
    results: dict[str, ImportResult] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return len(self.results) == 2 and all(
            r.succeeded for r in self.results.values()
        )


def run_fundamentals(
    engine: Engine,
    client: EdgarClient,
    collect: Callable[[Engine, EdgarClient], ImportResult] = run_collection,
) -> FundamentalsRun:
    run = FundamentalsRun()
    run.results[FUNDAMENTALS] = collect(engine, client)
    run.results[SCORES] = derive_scores(engine)
    for source, result in run.results.items():
        logger.info(
            "fundamentals source=%s status=%s code=%s run_id=%s",
            source,
            result.status,
            result.error_code,
            result.run_id,
        )
    return run


def run_once(settings: Any = None) -> int:
    """Scheduler entry point: exit code 0 when both runs succeeded, 1 otherwise."""
    from src.db.session import make_engine
    from src.ingestion.redaction import describe_exception
    from src.settings import load_settings

    engine = None
    try:
        settings = load_settings() if settings is None else settings
        if not settings.sec_user_agent:
            logger.error("fundamentals run failed code=missing_credential")
            return 1
        engine = make_engine(settings.database_url.get_secret_value())
        client = EdgarClient(requests.Session(), settings.sec_user_agent)
        try:
            run = run_fundamentals(engine, client)
        finally:
            client.session.close()
        return 0 if run.succeeded else 1
    except Exception as exc:
        logger.error("fundamentals run failed: %s", describe_exception(exc))
        return 1
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    from src.utils.logger import configure_logging

    configure_logging()
    sys.exit(run_once())
