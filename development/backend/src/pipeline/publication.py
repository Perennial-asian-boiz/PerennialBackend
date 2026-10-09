"""Publish an eligible, fully pinned watchlist in one transaction.

Only this module changes the production pointer. Importing/backfilling remains
independent. Defaults are deliberately visible policy, configurable by the caller.
"""

from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any, Mapping, Sequence, cast

from sqlalchemy import insert, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection, Engine, RowMapping

from src.db import models as m
from src.db.queries import batch_rows
from src.pipeline.ranking import DEFAULT_CONFIG, VERSION, calculate, candidate_symbols

MAX_AGE = {
    "congress_trades": timedelta(days=3),
    "ark_holdings": timedelta(days=3),
    "insider_trades": timedelta(days=3),
    "short_interest": timedelta(days=45),
}


class PublicationError(ValueError):
    """A fixed, safe publication eligibility error."""


def _time_ok(value: Any, now: datetime, max_age: timedelta) -> bool:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and now - max_age <= value <= now + timedelta(minutes=5)
    )


def _selected(
    conn: Connection,
    run_ids: Mapping[str, int],
    now: datetime,
    max_age: Mapping[str, timedelta],
) -> tuple[
    dict[str, RowMapping],
    dict[str, int],
    dict[str, list[dict[str, Any]]],
    dict[str, str | None],
]:
    if set(run_ids) != set(m.SOURCES):
        raise PublicationError("all source run IDs are required")
    runs: dict[str, RowMapping] = {}
    batches: dict[str, int] = {}
    rows: dict[str, list[dict[str, Any]]] = {}
    as_of: dict[str, str | None] = {}
    for source in m.SOURCES:
        run = (
            conn.execute(
                select(m.ingestion_runs).where(m.ingestion_runs.c.id == run_ids[source])
            )
            .mappings()
            .one_or_none()
        )
        if not run or run["source"] != source or run["status"] != "succeeded":
            raise PublicationError("source run is not successful")
        if run["collection_mode"] != "live":
            raise PublicationError("production requires live collection mode")
        if not _time_ok(run["collected_at"], now, max_age[source]):
            raise PublicationError("source collection is stale or future-dated")
        if not run["coverage"] or run["coverage"].get("complete") is not True:
            raise PublicationError("source coverage is incomplete or unknown")
        batch = (
            conn.execute(
                select(m.source_batches).where(m.source_batches.c.id == run["batch_id"])
            )
            .mappings()
            .one()
        )
        day = batch["source_as_of"]
        if day and day > now.date():
            raise PublicationError("source observation is future-dated")
        if source == "ark_holdings" and (
            day is None or day < (now - timedelta(days=7)).date()
        ):
            raise PublicationError("ARK observation date is stale or unknown")
        runs[source], batches[source] = run, run["batch_id"]
        rows[source] = batch_rows(conn, source, run["batch_id"])
        as_of[source] = day.isoformat() if day else None
        if source == "short_interest" and any(
            r["settlement_date"] > now.date()
            or r["settlement_date"] < (now - timedelta(days=45)).date()
            for r in rows[source]
        ):
            raise PublicationError(
                "short-interest observation is stale or future-dated"
            )
    expected = {s: batches[s] for s in ("congress_trades", "ark_holdings")}
    for source in ("insider_trades", "short_interest"):
        deps = (
            conn.execute(
                select(
                    m.run_dependencies.c.source, m.run_dependencies.c.batch_id
                ).where(m.run_dependencies.c.run_id == run_ids[source])
            )
            .tuples()
            .all()
        )
        if dict(deps) != expected:
            raise PublicationError(
                "downstream source lineage does not match selected upstream batches"
            )
    return runs, batches, rows, as_of


def _caps(
    observations: Mapping[str, dict[str, Any]], symbols: Sequence[str], now: datetime
) -> dict[str, dict[str, Any]]:
    if set(observations) != set(symbols):
        raise PublicationError("market-cap coverage does not match candidate universe")
    normalized = {}
    for symbol in symbols:
        obs = observations[symbol]
        if not isinstance(obs, dict) or "value" not in obs:
            raise PublicationError("market-cap observation is malformed")
        if obs.get("currency") != "USD" or obs.get("provider") != "fmp":
            raise PublicationError("unsupported market-cap currency or provider")
        if not _time_ok(obs.get("retrieved_at"), now, timedelta(days=1)):
            raise PublicationError("market-cap retrieval is stale or future-dated")
        observed = obs.get("observed_at")
        if observed is not None and not _time_ok(observed, now, timedelta(days=3)):
            raise PublicationError("market-cap observation is stale or future-dated")
        value = obs["value"]
        try:
            if isinstance(value, bool):
                raise InvalidOperation
            value = Decimal(str(value)) if value is not None else None
            if value is not None:
                if not value.is_finite() or value <= 0 or value >= Decimal("1e22"):
                    raise InvalidOperation
                value = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
                if value <= 0:
                    raise InvalidOperation
        except (InvalidOperation, ValueError, TypeError):
            raise PublicationError("market-cap value is invalid") from None
        normalized[symbol] = dict(
            value=value,
            currency="USD",
            provider="fmp",
            observed_at=observed,
            retrieved_at=obs["retrieved_at"],
        )
    if symbols and all(o["value"] is None for o in normalized.values()):
        raise PublicationError("all market-cap values are unavailable")
    return normalized


def publish(
    engine: Engine,
    run_ids: Mapping[str, int],
    observations: Mapping[str, dict[str, Any]],
    *,
    max_age: Mapping[str, timedelta] | None = None,
) -> int:
    max_age = dict(MAX_AGE if max_age is None else max_age)
    if set(max_age) != set(m.SOURCES) or any(
        v.total_seconds() <= 0 for v in max_age.values()
    ):
        raise ValueError("freshness policy must cover every source with positive ages")
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        # Serialize pointer changes and compare against the actual prior publication.
        conn.execute(text("SELECT pg_advisory_xact_lock(170099)"))
        runs, batches, rows, as_of = _selected(conn, run_ids, now, max_age)
        prior = conn.execute(
            select(m.current_publication.c.publication_id).where(
                m.current_publication.c.channel == "production"
            )
        ).scalar_one_or_none()
        if prior is not None:
            previous = (
                conn.execute(
                    select(
                        m.ingestion_runs.c.source,
                        m.ingestion_runs.c.collected_at,
                        m.source_batches.c.source_as_of,
                    )
                    .select_from(
                        m.publication_sources.join(
                            m.ingestion_runs,
                            m.ingestion_runs.c.id == m.publication_sources.c.run_id,
                        ).join(
                            m.source_batches,
                            m.source_batches.c.id == m.publication_sources.c.batch_id,
                        )
                    )
                    .where(m.publication_sources.c.publication_id == prior)
                )
                .tuples()
                .all()
            )
            for old in previous:
                if runs[old.source]["collected_at"] < old.collected_at:
                    raise PublicationError("publication would regress collection time")
                if old.source_as_of and (
                    as_of[old.source] is None
                    or as_of[old.source] < old.source_as_of.isoformat()
                ):
                    raise PublicationError("publication would regress observation date")
        symbols = candidate_symbols(rows)
        cap_data = _caps(observations, symbols, now)
        config = {
            **DEFAULT_CONFIG,
            "source_as_of": as_of,
            "freshness_seconds": {
                s: int(age.total_seconds()) for s, age in max_age.items()
            },
        }
        output = calculate(
            rows, {s: o["value"] for s, o in cap_data.items()}, now, config
        )
        pub_id = conn.execute(
            insert(m.publications)
            .values(
                created_at=now,
                ranking_version=VERSION,
                ranking_config=config,
                output=output,
            )
            .returning(m.publications.c.id)
        ).scalar_one()
        conn.execute(
            insert(m.publication_sources),
            [
                dict(
                    publication_id=pub_id,
                    source=s,
                    run_id=run_ids[s],
                    batch_id=batches[s],
                )
                for s in m.SOURCES
            ],
        )
        security_ids = dict(
            conn.execute(
                select(m.securities.c.symbol, m.securities.c.id).where(
                    m.securities.c.symbol.in_(symbols)
                )
            )
            .tuples()
            .all()
        )
        for symbol, obs in cap_data.items():
            obs_id = conn.execute(
                insert(m.market_cap_observations)
                .values(security_id=security_ids[symbol], **obs)
                .returning(m.market_cap_observations.c.id)
            ).scalar_one()
            conn.execute(
                insert(m.publication_market_caps).values(
                    publication_id=pub_id,
                    security_id=security_ids[symbol],
                    observation_id=obs_id,
                )
            )
        conn.execute(
            pg_insert(m.current_publication)
            .values(channel="production", publication_id=pub_id, promoted_at=now)
            .on_conflict_do_update(
                index_elements=["channel"],
                set_={"publication_id": pub_id, "promoted_at": now},
            )
        )
        return cast(int, pub_id)


def current(engine: Engine) -> dict[str, Any] | None:
    with engine.connect() as conn:
        row = (
            conn.execute(
                select(m.publications)
                .join(
                    m.current_publication,
                    m.current_publication.c.publication_id == m.publications.c.id,
                )
                .where(m.current_publication.c.channel == "production")
            )
            .mappings()
            .one_or_none()
        )
        return dict(row) if row else None


def replay(engine: Engine, publication_id: int) -> dict[str, Any]:
    """Recompute an old output without freshness checks, network or the clock."""
    with engine.connect() as conn:
        pub = (
            conn.execute(
                select(m.publications).where(m.publications.c.id == publication_id)
            )
            .mappings()
            .one()
        )
        sources = (
            conn.execute(
                select(m.publication_sources).where(
                    m.publication_sources.c.publication_id == publication_id
                )
            )
            .mappings()
            .all()
        )
        rows = {
            s["source"]: batch_rows(conn, s["source"], s["batch_id"]) for s in sources
        }
        caps = dict(
            conn.execute(
                select(m.securities.c.symbol, m.market_cap_observations.c.value)
                .select_from(
                    m.publication_market_caps.join(
                        m.market_cap_observations,
                        m.market_cap_observations.c.id
                        == m.publication_market_caps.c.observation_id,
                    ).join(
                        m.securities,
                        m.securities.c.id == m.publication_market_caps.c.security_id,
                    )
                )
                .where(m.publication_market_caps.c.publication_id == publication_id)
            )
            .tuples()
            .all()
        )
        return calculate(
            rows,
            caps,
            pub["created_at"],
            pub["ranking_config"],
            version=pub["ranking_version"],
        )
