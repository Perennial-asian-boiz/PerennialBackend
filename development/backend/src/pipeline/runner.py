"""Database-first collection and publication. No source JSON files required."""

import inspect
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from functools import partial
from typing import Any, Callable, Mapping, Sequence

import requests
from sqlalchemy.engine import Connection, Engine

from src.db import models as m
from src.db.queries import batch_rows, latest_successful_run
from src.ingestion.diagnostics import exception_diagnostics
from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection
from src.pipeline.lifecycle import (
    abandon_stale_runs,
    begin_run,
    heartbeat_worker,
    source_lock,
)
from src.pipeline.publication import (
    PriorPublication,
    PublicationError,
    Reuse,
    prior_publication,
    publish,
)
from src.pipeline.ranking import candidate_symbols
from src.settings import QualityThreshold, Settings, _source_policy_defaults

logger = logging.getLogger(__name__)
UPSTREAM = ("congress_trades", "ark_holdings")
DOWNSTREAM = ("insider_trades", "short_interest")


class RequiredSourceUnavailableError(RuntimeError):
    """A required source has neither a fresh batch nor a reusable pinned one."""

    code = "required_source_unavailable"

    def __init__(self, source: str, failure_code: str | None):
        self.source, self.failure_code = source, failure_code
        super().__init__(f"collection failed for {source}: {failure_code}")


@dataclass(frozen=True)
class Attempt:
    """Today's outcome for one source: a fresh run, or a failure code."""

    run_id: int | None
    batch_id: int | None
    error_code: str | None
    dependencies: Mapping[str, int] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.error_code is None and self.batch_id is not None


@dataclass
class Selection:
    runs: dict[str, int] = field(default_factory=dict)
    batches: dict[str, int] = field(default_factory=dict)
    reuse: dict[str, Reuse] = field(default_factory=dict)

    def fresh(self, source: str, attempt: Attempt) -> None:
        assert attempt.run_id is not None and attempt.batch_id is not None
        self.runs[source], self.batches[source] = attempt.run_id, attempt.batch_id
        self.reuse.pop(source, None)

    def pinned(self, prior: PriorPublication | None, source: str, why: Reuse) -> None:
        pin = prior.pins.get(source) if prior is not None else None
        if pin is None or not pin.reusable:
            raise RequiredSourceUnavailableError(source, why.failure_code)
        self.runs[source], self.batches[source] = pin.run_id, pin.batch_id
        self.reuse[source] = why


def select_sources(
    prior: PriorPublication | None,
    attempt: Callable[[str, Mapping[str, int] | None], Attempt],
) -> Selection:
    """Choose each source's run: today's if it succeeded, else the pinned one.

    attempt(source, upstream_batches) collects (or replays a recorded outcome);
    downstream sources are attempted with the upstream batches selected here,
    so a reused upstream batch plans today's downstream collection.
    """
    chosen = Selection()
    for source in UPSTREAM:
        result = attempt(source, None)
        if result.ok:
            chosen.fresh(source, result)
        else:
            chosen.pinned(prior, source, Reuse(result.error_code, result.run_id))
    for source in DOWNSTREAM:
        if source in chosen.reuse:
            continue  # already taken from the prior cohort below
        upstream = {s: chosen.batches[s] for s in UPSTREAM}
        result = attempt(source, upstream)
        if result.ok and dict(result.dependencies) == upstream:
            chosen.fresh(source, result)
            continue
        code = result.error_code or "lineage_mismatch"
        pin = prior.pins.get(source) if prior is not None else None
        if pin is not None and pin.dependencies == upstream:
            chosen.pinned(prior, source, Reuse(code, result.run_id))
        else:
            _reuse_prior_cohort(chosen, prior, source, Reuse(code, result.run_id))
    return chosen


def _reuse_prior_cohort(
    chosen: Selection,
    prior: PriorPublication | None,
    source: str,
    why: Reuse,
) -> None:
    """Pin the prior publication's whole cohort around a reused downstream run.

    Today's upstream batches differ from the ones the reused run was planned
    from, so lineage needs that publication's Congress, ARK and sibling batches
    too. They passed lineage together when published, so this costs no provider
    calls; a sibling collected fresh today stays recorded but unpublished.
    """
    chosen.pinned(prior, source, why)
    for other in m.SOURCES:
        if other == source or other in chosen.reuse:
            continue
        chosen.pinned(
            prior,
            other,
            Reuse(None, cohort=True, unpublished_run_id=chosen.runs.get(other)),
        )


def _upstream(
    conn: Connection, batches: Mapping[str, int]
) -> dict[str, list[dict[str, Any]]]:
    result = {}
    for source, key in (("congress_trades", "trades"), ("ark_holdings", "holdings")):
        rows = batch_rows(conn, source, batches[source])
        result[key] = [dict(r, ticker=r["symbol"]) for r in rows]
    return result


def _accepts_settings(collector: Callable[..., CollectionOutcome]) -> bool:
    try:
        params = inspect.signature(collector).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(p.name == "settings" or p.kind is p.VAR_KEYWORD for p in params)


def collect_source(
    engine: Engine,
    source: str,
    *,
    upstream_batches: Mapping[str, int] | None = None,
    collector: Callable[..., CollectionOutcome] | None = None,
    settings: Settings | None = None,
) -> ImportResult:
    """Collect and import one source; settings reach collectors that declare them.

    Collectors that need no credential (ARK, short interest) do not take
    settings, so they are not passed. Fetcher module keys are only a fallback
    for collectors called without settings.
    """
    from src.ingestion.collectors import COLLECTORS

    collector = COLLECTORS[source] if collector is None else collector
    kwargs: dict[str, Any] = {}
    if settings is not None and _accepts_settings(collector):
        kwargs["settings"] = settings
    with source_lock(engine, source):
        dependencies = {}
        missing = False
        downstream = source in ("insider_trades", "short_interest")
        with engine.connect() as conn:
            if downstream:
                if upstream_batches is None:
                    selected = {s: latest_successful_run(conn, s) for s in UPSTREAM}
                    missing = any(
                        r is None or r.collection_mode != "live"
                        for r in selected.values()
                    )
                    if not missing:
                        dependencies = {
                            s: r.batch_id for s, r in selected.items() if r is not None
                        }
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
                    if downstream:
                        kwargs["upstream"] = upstream
                    outcome = collector(**kwargs)
                    if (
                        not isinstance(outcome, CollectionOutcome)
                        or outcome.source != source
                        or outcome.mode != "live"
                    ):
                        raise ValueError("collector returned mismatched outcome")
                except Exception as exc:
                    # Known transport failures differ from bugs in the collector contract.
                    # The importer allowlists class names and never stores messages.
                    code = (
                        "collection_failed"
                        if isinstance(exc, requests.RequestException)
                        else "internal_error"
                    )
                    outcome = CollectionOutcome.failure(
                        source, "live", code, diagnostics=exception_diagnostics(exc)
                    )
            result = import_collection(engine, outcome, run_id=run_id)
        logger.info(
            "collection source=%s run_id=%s status=%s code=%s",
            source,
            run_id,
            result.status,
            result.error_code,
        )
        return result


def _classification(raw: Any, max_length: int) -> str | None:
    """Sector and industry are optional labels: bad values become null, never abort."""
    if not isinstance(raw, str):
        return None
    label = raw.strip()
    return label if 0 < len(label) <= max_length else None


def collect_market_caps(
    symbols: Sequence[str],
    *,
    session: requests.Session | None = None,
    api_key: str | None = None,
    max_seconds: float = 300,
) -> dict[str, dict[str, Any]]:
    """A missing profile is an explicit null; transport/parse failures abort publication."""
    from src.ingestion.collectors import _fetcher, _get_json

    key = api_key if api_key is not None else _fetcher("fmp").FMP_API_KEY
    if not key or key.lower().startswith("your_"):
        raise PublicationError("market-cap credential is missing")
    owned = session is None
    session = session or requests.Session()
    deadline = time.monotonic() + max_seconds
    observations = {}
    try:
        for symbol in symbols:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PublicationError("market-cap collection deadline exceeded")
            response = _get_json(
                session,
                "https://financialmodelingprep.com/stable/profile",
                params={"symbol": symbol, "apikey": key},
                timeout=min(10, remaining),
                deadline_seconds=remaining,
            )
            if time.monotonic() >= deadline:
                raise PublicationError("market-cap collection deadline exceeded")
            if not response.ok:
                raise PublicationError("market-cap collection failed")
            payload = response.data
            if not isinstance(payload, list) or len(payload) > 1:
                raise PublicationError("market-cap response is malformed")
            value = sector = industry = None
            if payload:
                item = payload[0]
                if not isinstance(item, dict) or item.get("symbol") != symbol:
                    raise PublicationError(
                        "market-cap response has mismatched security"
                    )
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
                        raise PublicationError(
                            "market-cap value is malformed"
                        ) from None
                sector = _classification(item.get("sector"), 64)
                industry = _classification(item.get("industry"), 128)
            observations[symbol] = {
                "value": value,
                "currency": "USD",
                "provider": "fmp",
                "retrieved_at": datetime.now(timezone.utc),
                "observed_at": None,
                "sector": sector,
                "industry": industry,
            }
        return observations
    finally:
        if owned:
            session.close()


def run_pipeline(
    engine: Engine,
    *,
    collectors: Mapping[str, Callable[..., CollectionOutcome]] | None = None,
    market_cap_collector: Callable[[Sequence[str]], Mapping[str, dict[str, Any]]]
    | None = None,
    quality_thresholds: Mapping[str, QualityThreshold] | None = None,
    settings: Settings | None = None,
) -> int:
    """Stop on a failed stage; all publication writes commit together at the end."""
    with source_lock(engine, "pipeline"):
        return _run_pipeline(
            engine,
            collectors=collectors,
            market_cap_collector=market_cap_collector,
            quality_thresholds=quality_thresholds,
            settings=settings,
        )


def _run_pipeline(
    engine: Engine,
    *,
    collectors: Mapping[str, Callable[..., CollectionOutcome]] | None = None,
    market_cap_collector: Callable[[Sequence[str]], Mapping[str, dict[str, Any]]]
    | None = None,
    quality_thresholds: Mapping[str, QualityThreshold] | None = None,
    settings: Settings | None = None,
) -> int:
    from src.ingestion.collectors import COLLECTORS

    collectors = COLLECTORS if collectors is None else collectors
    if market_cap_collector is None:
        fmp_key = settings.fmp_api_key if settings is not None else None
        api_key = fmp_key.get_secret_value() if fmp_key is not None else None
        market_cap_collector = partial(collect_market_caps, api_key=api_key)
    policies = (
        settings.source_policies if settings is not None else _source_policy_defaults()
    )
    max_age = {s: p.max_age for s, p in policies.items()}
    reuse_limits = {s: p.max_reuse_age for s, p in policies.items()}
    abandon_stale_runs(engine)
    prior = prior_publication(engine, datetime.now(timezone.utc), max_age, reuse_limits)

    def attempt(source: str, upstream: Mapping[str, int] | None) -> Attempt:
        result = collect_source(
            engine,
            source,
            upstream_batches=upstream,
            collector=collectors[source],
            settings=settings,
        )
        if not result.succeeded:
            return Attempt(
                result.run_id, None, result.error_code or "collection_failed"
            )
        return Attempt(result.run_id, result.batch_id, None, dict(upstream or {}))

    chosen = select_sources(prior, attempt)
    with engine.connect() as conn:
        rows = {s: batch_rows(conn, s, b) for s, b in chosen.batches.items()}
    observations = market_cap_collector(candidate_symbols(rows))
    publication_id = publish(
        engine,
        chosen.runs,
        observations,
        max_age=max_age,
        quality_thresholds=quality_thresholds,
        reuse=chosen.reuse,
        reuse_limits=reuse_limits,
    )
    logger.info(
        "publication succeeded publication_id=%s reused=%s",
        publication_id,
        ",".join(sorted(chosen.reuse)) or "none",
    )
    return publication_id
