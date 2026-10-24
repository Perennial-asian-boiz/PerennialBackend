"""Database-first collection and publication. No source JSON files required."""

import inspect
import logging
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from functools import partial
from typing import Any, Callable, Mapping, Sequence

import requests
from sqlalchemy import select
from sqlalchemy.engine import Connection, Engine

from src.db import models as m
from src.db.queries import batch_rows, latest_successful_run
from src.ingestion.diagnostics import exception_diagnostics
from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection
from src.ingestion.market_caps import ADAPTERS, MarketCapAdapter, collect
from src.pipeline import congress_history
from src.pipeline.demo_guard import DEMO_MODES, LIVE_MODES, check_demo_database
from src.pipeline.lifecycle import (
    abandon_stale_runs,
    begin_run,
    heartbeat_worker,
    source_lock,
)
from src.pipeline.publication import (
    PriorPublication,
    PublicationError,
    PublicationQualityError,
    PublicationRegressionError,
    Reuse,
    prior_publication,
    publish,
)
from src.pipeline.ranking import candidate_symbols
from src.settings import (
    MARKET_CAP_MAX_REUSE_AGE,
    QualityThreshold,
    Settings,
    _source_policy_defaults,
)

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
    for other in m.WATCHLIST_SOURCES:
        if other == source or other in chosen.reuse:
            continue
        chosen.pinned(
            prior,
            other,
            Reuse(None, cohort=True, unpublished_run_id=chosen.runs.get(other)),
        )


def _upstream(
    conn: Connection, batches: Mapping[str, int], history_through: int
) -> dict[str, list[dict[str, Any]]]:
    """Downstream plans cover what publish ranks: windowed Congress history (CI-1)."""
    congress = congress_history.ranking_rows(
        conn, history_through, datetime.now(timezone.utc)
    )
    holdings = batch_rows(conn, "ark_holdings", batches["ark_holdings"])
    return {
        "trades": [dict(r, ticker=r["symbol"]) for r in congress],
        "holdings": [dict(r, ticker=r["symbol"]) for r in holdings],
    }


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
    record_mode: str = "live",
) -> ImportResult:
    """Collect and import one source; settings reach collectors that declare them.

    record_mode "fixture" (demo only) records the run as fixture-mode: the
    collector ran, but against recorded provider responses, not a provider.

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
        # market_caps (M4 MC-B) plans from the same upstream rows and lineage.
        downstream = source in ("insider_trades", "short_interest", "market_caps")
        history_through = None
        with engine.begin() as conn:
            if downstream:
                if upstream_batches is None:
                    selected = {s: latest_successful_run(conn, s) for s in UPSTREAM}
                    missing = any(
                        r is None or r.collection_mode != record_mode
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
            upstream = None
            if dependencies:
                if record_mode != "live":
                    check_demo_database(conn)  # fixture history: demo DB only
                congress_history.fold_batch(
                    conn,
                    dependencies["congress_trades"],
                    LIVE_MODES if record_mode == "live" else DEMO_MODES,
                )
                history_through = congress_history.watermark(conn)
                upstream = _upstream(conn, dependencies, history_through)
        run_id = begin_run(
            engine,
            source,
            record_mode,
            dependencies=dependencies,
            history_through=history_through,
        )
        with heartbeat_worker(engine, run_id):
            if missing:
                outcome = CollectionOutcome.failure(
                    source, record_mode, "missing_upstream"
                )
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
                    if record_mode != "live":
                        outcome = replace(outcome, mode=record_mode)
                except Exception as exc:
                    # Known transport failures differ from bugs in the collector contract.
                    # The importer allowlists class names and never stores messages.
                    code = (
                        "collection_failed"
                        if isinstance(exc, requests.RequestException)
                        else "internal_error"
                    )
                    outcome = CollectionOutcome.failure(
                        source,
                        record_mode,
                        code,
                        diagnostics=exception_diagnostics(exc),
                    )
            result = import_collection(engine, outcome, run_id=run_id)
        if source == "congress_trades" and result.succeeded:
            # Fold every live page into history now: an unpublished (evening)
            # run's rows would otherwise scroll off the free page unseen.
            with engine.begin() as conn:
                if record_mode != "live":
                    check_demo_database(conn)  # fixture history: demo DB only
                derived = congress_history.derive(
                    conn,
                    result.run_id,
                    LIVE_MODES if record_mode == "live" else DEMO_MODES,
                )
            logger.info(
                "congress history run_id=%s added=%s possible_gaps=%s gap_days=%s",
                result.run_id,
                derived["history_added"],
                derived["possible_gaps"],
                derived["gap_days"],
            )
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


def publish_or_reuse_refused(
    prior: PriorPublication | None,
    chosen: Selection,
    attempts: Mapping[str, Attempt],
    publish_selection: Callable[[Selection], int],
    record_count: Callable[[int], int],
) -> tuple[int, Selection]:
    """Publish; a fresh batch refused by quality or the date guard falls back (M2 Q7).

    The refused batch is never published and no guard is relaxed: its source is
    re-selected as if today's collection had failed with the refusal code, so
    the same reuse limits and cohort rules apply, without new provider calls.

    Exception: a row_count refusal of an *empty* batch never falls back. A day
    with no qualifying rows (for example no insider buys) may be the true
    result, and serving old rows as merely degraded would hide that; the
    original refusal stands for an operator to override or not.
    """
    refused: dict[str, str] = {}
    while True:
        try:
            return publish_selection(chosen), chosen
        except PublicationQualityError as exc:
            sources = {check.split(":", 1)[0] for check in exc.checks}
            code = exc.code
            error: PublicationError = exc
            emptied = {
                c.split(":", 1)[0]
                for c in exc.checks
                if c.endswith(":row_count")
                and (batch := chosen.batches.get(c.split(":", 1)[0])) is not None
                and record_count(batch) == 0
            }
            if emptied:
                raise
        except PublicationRegressionError as exc:
            sources, code, error = {exc.source}, exc.code, exc
        new = sources - set(chosen.reuse) - set(refused)
        if not new:
            raise error
        refused.update(dict.fromkeys(new, code))
        logger.warning(
            "fresh batch refused; reusing pinned batch sources=%s code=%s",
            ",".join(sorted(new)),
            code,
        )

        def replay(source: str, upstream: Mapping[str, int] | None) -> Attempt:
            recorded = attempts[source]
            if source in refused:
                return Attempt(recorded.run_id, None, refused[source])
            return recorded

        try:
            chosen = select_sources(prior, replay)
        except RequiredSourceUnavailableError:
            raise error from None


def market_caps_or_fallback(
    engine: Engine,
    symbols: Sequence[str],
    collector: Callable[[Sequence[str]], Mapping[str, dict[str, Any]]],
    max_reuse_age: timedelta,
) -> tuple[Mapping[str, dict[str, Any]], bool]:
    """Collect market caps; if collection fails, reuse stored ones (M2 Q3).

    Each symbol falls back to its latest stored observation retrieved within
    max_reuse_age. A symbol without one gets value None (attempted now, nothing
    usable), which ranking already reports as market_cap_unavailable.
    """
    try:
        return collector(symbols), False
    except PublicationError:
        logger.warning("market-cap collection failed; using stored observations")
    now = datetime.now(timezone.utc)
    latest = (
        select(
            m.securities.c.symbol,
            m.market_cap_observations.c.value,
            m.market_cap_observations.c.observed_at,
            m.market_cap_observations.c.retrieved_at,
            m.market_cap_observations.c.sector,
            m.market_cap_observations.c.industry,
        )
        .join(
            m.securities,
            m.securities.c.id == m.market_cap_observations.c.security_id,
        )
        .where(
            m.securities.c.symbol.in_(symbols),
            m.market_cap_observations.c.retrieved_at >= now - max_reuse_age,
            m.market_cap_observations.c.value.is_not(None),
        )
        .order_by(
            m.securities.c.symbol,
            m.market_cap_observations.c.retrieved_at.desc(),
            m.market_cap_observations.c.id.desc(),
        )
        .distinct(m.securities.c.symbol)
    )
    with engine.connect() as conn:
        stored = {row.symbol: row for row in conn.execute(latest)}
    observations: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        row = stored.get(symbol)
        observations[symbol] = {
            "value": row.value if row else None,
            "currency": "USD",
            "provider": "fmp",
            "observed_at": row.observed_at if row else None,
            "retrieved_at": row.retrieved_at if row else now,
            "sector": row.sector if row else None,
            "industry": row.industry if row else None,
        }
    return observations, True


def run_pipeline(
    engine: Engine,
    *,
    collectors: Mapping[str, Callable[..., CollectionOutcome]] | None = None,
    market_cap_collector: Callable[[Sequence[str]], Mapping[str, dict[str, Any]]]
    | None = None,
    quality_thresholds: Mapping[str, QualityThreshold] | None = None,
    settings: Settings | None = None,
    demo: bool = False,
    market_cap_adapter: MarketCapAdapter | None = None,
) -> int:
    """Stop on a failed stage; all publication writes commit together at the end.

    demo=True records fixture-mode runs and publishes only into the marked local
    demo database (checked here and again inside publish()).

    market_cap_adapter, else settings.market_cap_adapter, selects the M4 path:
    one market_caps collection per call and pinned rows at publish. Neither set
    keeps the legacy market-cap collector. A fixture adapter runs only in demo,
    and a live one only outside it.
    """
    if demo:
        with engine.connect() as conn:
            check_demo_database(conn)
    with source_lock(engine, "pipeline"):
        return _run_pipeline(
            engine,
            collectors=collectors,
            market_cap_collector=market_cap_collector,
            quality_thresholds=quality_thresholds,
            settings=settings,
            demo=demo,
            market_cap_adapter=market_cap_adapter,
        )


def _run_pipeline(
    engine: Engine,
    *,
    collectors: Mapping[str, Callable[..., CollectionOutcome]] | None = None,
    market_cap_collector: Callable[[Sequence[str]], Mapping[str, dict[str, Any]]]
    | None = None,
    quality_thresholds: Mapping[str, QualityThreshold] | None = None,
    settings: Settings | None = None,
    demo: bool = False,
    market_cap_adapter: MarketCapAdapter | None = None,
) -> int:
    from src.ingestion.collectors import COLLECTORS

    record_mode, modes = ("fixture", DEMO_MODES) if demo else ("live", LIVE_MODES)

    adapter = market_cap_adapter
    if adapter is None and settings is not None and settings.market_cap_adapter:
        adapter = ADAPTERS[settings.market_cap_adapter](settings)
    if adapter is not None:
        # MCB-2e: refuse before any collection; collect() guards again (MCA-3g).
        if adapter.collection_mode != record_mode:
            raise ValueError("market-cap adapter mode does not match the pipeline")
        if market_cap_collector is not None:
            raise ValueError(
                "pass a market-cap adapter or a legacy collector, not both"
            )

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
    prior = prior_publication(
        engine, datetime.now(timezone.utc), max_age, reuse_limits, modes
    )

    attempts: dict[str, Attempt] = {}

    def attempt(source: str, upstream: Mapping[str, int] | None) -> Attempt:
        result = collect_source(
            engine,
            source,
            upstream_batches=upstream,
            collector=collectors[source],
            settings=settings,
            record_mode=record_mode,
        )
        if not result.succeeded:
            outcome = Attempt(
                result.run_id, None, result.error_code or "collection_failed"
            )
        else:
            outcome = Attempt(
                result.run_id, result.batch_id, None, dict(upstream or {})
            )
        attempts[source] = outcome
        return outcome

    chosen = select_sources(prior, attempt)
    cap_run_id = None
    if adapter is not None:
        # MCB-2a/2b: once per call, planned from the selected upstream; a failed
        # collection is recorded and publish() falls back to earlier rows.
        cap_run_id = collect_source(
            engine,
            "market_caps",
            upstream_batches={s: chosen.batches[s] for s in UPSTREAM},
            collector=partial(collect, adapter, record_mode=record_mode),
            record_mode=record_mode,
        ).run_id
    cap_limit = (
        settings.market_cap_max_reuse_age
        if settings is not None
        else MARKET_CAP_MAX_REUSE_AGE
    )

    cap_cache: dict[tuple[str, ...], tuple[Mapping[str, dict[str, Any]], bool]] = {}

    def publish_selection(selection: Selection) -> int:
        if adapter is not None:  # MCB-8: a Q7 retry reselects stored rows
            return publish(
                engine,
                selection.runs,
                None,
                max_age=max_age,
                quality_thresholds=quality_thresholds,
                reuse=selection.reuse,
                reuse_limits=reuse_limits,
                demo=demo,
                market_caps="pinned",
                market_cap_max_age=cap_limit,
                market_cap_run_id=cap_run_id,
                market_cap_adapter=adapter.name,
            )
        # Candidates must match what publish() ranks: the history the
        # downstream runs were planned from.
        with engine.begin() as conn:
            rows = {s: batch_rows(conn, s, b) for s, b in selection.batches.items()}
            congress_history.derive(conn, selection.runs["congress_trades"], modes)
            through = congress_history.planned_watermark(conn, selection.runs)
            if through is None:
                through = congress_history.watermark(conn)
            rows["congress_trades"] = congress_history.ranking_rows(
                conn, through, datetime.now(timezone.utc)
            )
        symbols = tuple(candidate_symbols(rows))
        if symbols not in cap_cache:  # a Q7 retry must not call FMP again
            cap_cache[symbols] = market_caps_or_fallback(
                engine, symbols, market_cap_collector, cap_limit
            )
        observations, cap_fallback = cap_cache[symbols]
        return publish(
            engine,
            selection.runs,
            observations,
            max_age=max_age,
            quality_thresholds=quality_thresholds,
            reuse=selection.reuse,
            reuse_limits=reuse_limits,
            market_cap_fallback=cap_limit if cap_fallback else None,
            demo=demo,
        )

    def record_count(batch_id: int) -> int:
        with engine.connect() as conn:
            return int(
                conn.execute(
                    select(m.source_batches.c.record_count).where(
                        m.source_batches.c.id == batch_id
                    )
                ).scalar_one()
            )

    publication_id, chosen = publish_or_reuse_refused(
        prior, chosen, attempts, publish_selection, record_count
    )
    logger.info(
        "publication succeeded publication_id=%s reused=%s",
        publication_id,
        ",".join(sorted(chosen.reuse)) or "none",
    )
    return publication_id
