"""Database-first collection and publication. No source JSON files required."""
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import requests
from sqlalchemy import select

from src.db import models as m
from src.db.queries import batch_rows, latest_successful_run
from src.ingestion.importer import CollectionOutcome, import_collection
from src.pipeline.lifecycle import begin_run, heartbeat_worker, source_lock, abandon_stale_runs
from src.pipeline.publication import publish, PublicationError
from src.pipeline.ranking import candidate_symbols

logger = logging.getLogger(__name__)
UPSTREAM = ("congress_trades", "ark_holdings")


def _upstream(conn, batches):
    result = {}
    for source, key in (("congress_trades", "trades"), ("ark_holdings", "holdings")):
        rows = batch_rows(conn, source, batches[source])
        result[key] = [dict(r, ticker=r["symbol"]) for r in rows]
    return result


def collect_source(engine, source, *, upstream_batches=None, collector=None):
    from src.ingestion.collectors import COLLECTORS
    collector = COLLECTORS[source] if collector is None else collector
    with source_lock(engine, source):
        dependencies = {}
        missing = False
        downstream = source in ("insider_trades", "short_interest")
        with engine.connect() as conn:
            if downstream:
                if upstream_batches is None:
                    selected = {s: latest_successful_run(conn, s) for s in UPSTREAM}
                    missing = any(r is None or r.collection_mode != "live" for r in selected.values())
                    if not missing:
                        dependencies = {s: r.batch_id for s, r in selected.items()}
                else:
                    if set(upstream_batches) != set(UPSTREAM):
                        raise ValueError("both upstream batch IDs are required")
                    dependencies = dict(upstream_batches)
            upstream = _upstream(conn, dependencies) if dependencies else None
        run_id = begin_run(engine, source, "live", dependencies=dependencies)
        with heartbeat_worker(engine, run_id):
            if missing:
                outcome = CollectionOutcome.failure(source, "live", "missing_upstream")
            else:
                try:
                    outcome = collector(upstream=upstream) if downstream else collector()
                    if not isinstance(outcome, CollectionOutcome) or outcome.source != source or outcome.mode != "live":
                        raise ValueError("collector returned mismatched outcome")
                except Exception:
                    # Provider exception strings may contain credentials or response data.
                    outcome = CollectionOutcome.failure(source, "live", "collection_failed")
            result = import_collection(engine, outcome, run_id=run_id)
        logger.info("collection source=%s run_id=%s status=%s code=%s", source, run_id, result.status, result.error_code)
        return result


def collect_market_caps(symbols, *, session=None, api_key=None, max_seconds=300):
    """A missing profile is an explicit null; transport/parse failures abort publication."""
    from src.ingestion.collectors import _get_json, _fetcher
    key = api_key if api_key is not None else _fetcher("fmp").FMP_API_KEY
    if not key or key.lower().startswith("your_"):
        raise PublicationError("market-cap credential is missing")
    owned = session is None
    session = session or requests.Session()
    deadline = time.monotonic() + max_seconds
    observations = {}
    try:
        for symbol in symbols:
            if time.monotonic() >= deadline:
                raise PublicationError("market-cap collection deadline exceeded")
            response = _get_json(session, "https://financialmodelingprep.com/stable/profile",
                params={"symbol": symbol, "apikey": key}, timeout=min(10, max(0.1, deadline-time.monotonic())))
            if not response.ok:
                raise PublicationError("market-cap collection failed")
            payload = response.data
            if not isinstance(payload, list) or len(payload) > 1:
                raise PublicationError("market-cap response is malformed")
            value = None
            if payload:
                item = payload[0]
                if not isinstance(item, dict) or item.get("symbol") != symbol:
                    raise PublicationError("market-cap response has mismatched security")
                if item.get("currency", "USD") != "USD":
                    raise PublicationError("market-cap currency is unsupported")
                raw = item.get("marketCap", item.get("mktCap"))
                if raw is not None:
                    try:
                        if isinstance(raw, bool):
                            raise InvalidOperation
                        value = Decimal(str(raw))
                        if not value.is_finite() or value <= 0:
                            raise InvalidOperation
                    except (InvalidOperation, ValueError):
                        raise PublicationError("market-cap value is malformed") from None
            observations[symbol] = {"value": value, "currency": "USD", "provider": "fmp",
                                    "retrieved_at": datetime.now(timezone.utc), "observed_at": None}
        return observations
    finally:
        if owned:
            session.close()


def run_pipeline(engine, *, collectors=None, market_cap_collector=None):
    """Stop on a failed stage; all publication writes commit together at the end."""
    with source_lock(engine, "pipeline"):
        return _run_pipeline(engine, collectors=collectors, market_cap_collector=market_cap_collector)


def _run_pipeline(engine, *, collectors=None, market_cap_collector=None):
    from src.ingestion.collectors import COLLECTORS
    collectors = COLLECTORS if collectors is None else collectors
    market_cap_collector = collect_market_caps if market_cap_collector is None else market_cap_collector
    abandon_stale_runs(engine)
    run_ids, batches = {}, {}
    for source in m.SOURCES:
        upstream = {s: batches[s] for s in UPSTREAM} if source not in UPSTREAM else None
        result = collect_source(engine, source, upstream_batches=upstream, collector=collectors[source])
        if not result.succeeded:
            raise RuntimeError(f"collection failed for {source}: {result.error_code}")
        run_ids[source], batches[source] = result.run_id, result.batch_id
    with engine.connect() as conn:
        rows = {s: batch_rows(conn, s, b) for s, b in batches.items()}
    observations = market_cap_collector(candidate_symbols(rows))
    publication_id = publish(engine, run_ids, observations)
    logger.info("publication succeeded publication_id=%s", publication_id)
    return publication_id
