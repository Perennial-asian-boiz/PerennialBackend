"""The Confidence Score jobs (S2-01 JOB-2/3, S2-03 JOB-5). Run once with:

    python -m src.pipeline.scheduler --once --job scores

Two derived ingestion runs, each writing an immutable batch:

* company_scores: score-v1 (scoring.blend) for every security in the latest
  successful fundamental_scores batch, with the latest successful
  ticker_sentiment batch when there is one; both are named in run_dependencies.
  recommended is judged against the current production watchlist publication,
  whose id every row stores. current_scores['production'] then points at the
  batch, and the API reads scores only through it.
* score_explanations: explain-v1 for every row of the pointer's scores batch,
  named in run_dependencies. A stored explanation of the same scores row with
  an equal input digest is kept as it is (S2-03 AC4).

Identical inputs give identical records, so the importer reuses the stored batch
and the pointer, with its promoted_at (the API's as-of), stays put (S2-01 AC2).
The job recomputes rather than comparing dependency ids, so a new publication or
a new score version is never missed.
"""

import logging
import sys
from collections.abc import Mapping
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection, Engine
from src.db import models as m
from src.db.queries import batch_rows, latest_successful_run
from src.ingestion.diagnostics import exception_diagnostics
from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection
from src.pipeline.lifecycle import begin_run, heartbeat_worker, source_lock
from src.services.scores import explain as explanations
from src.services.scores.scoring import ScoreWeights, blend, normalize_sentiment

logger = logging.getLogger(__name__)

FUNDAMENTALS, SENTIMENT = "fundamental_scores", "ticker_sentiment"
SCORES, EXPLANATIONS = "company_scores", "score_explanations"
CHANNEL = "production"
RECOMMEND_AT = Decimal("60")
# "On the current consensus list" (the S2-03 definition in the sprint matrix):
# every bucket of the publication, unresolved included.
LISTED_BUCKETS = ("popular_stable", "affordable_growing", "unresolved")


# ── JOB-3: the sentiment contract reader ─────────────────


def read_ticker_sentiment(conn: Connection, batch_id: int) -> dict[int, dict[str, Any]]:
    """{security_id: row} of one ticker_sentiment batch (the PR #85 contract).

    A row with a NULL value had nothing to score, so, like a security with no
    row, it has no sentiment for the score.
    """
    source = conn.execute(
        select(m.source_batches.c.source).where(m.source_batches.c.id == batch_id)
    ).scalar_one_or_none()
    if source != SENTIMENT:
        raise ValueError("not a ticker_sentiment batch")
    table = m.ticker_sentiment
    found = conn.execute(select(table).where(table.c.batch_id == batch_id)).mappings()
    return {row["security_id"]: dict(row) for row in found}


# ── JOB-2: derive_scores ─────────────────────────────────


def _publication(conn: Connection) -> tuple[int, set[str]] | None:
    """The current production watchlist publication: its id and listed tickers."""
    pub = conn.execute(
        select(m.publications.c.id, m.publications.c.output)
        .join(
            m.current_publication,
            m.current_publication.c.publication_id == m.publications.c.id,
        )
        .where(m.current_publication.c.channel == CHANNEL)
    ).one_or_none()
    if pub is None:
        return None
    output = pub.output if isinstance(pub.output, dict) else {}
    listed = {
        item["ticker"]
        for bucket in LISTED_BUCKETS
        for item in output.get(bucket) or []
        if isinstance(item, dict) and isinstance(item.get("ticker"), str)
    }
    return int(pub.id), listed


def _utc_date(moment: datetime) -> date:
    return moment.astimezone(timezone.utc).date()


def score_record(
    symbol: str,
    fundamentals: Mapping[str, Any],
    sentiment: Mapping[str, Any] | None,
    publication_id: int | None,
    listed: bool,
    weights: ScoreWeights = ScoreWeights(),
) -> dict[str, Any]:
    """One security's company_scores record from the rows the score reads.

    Each present component cites its row: fundamentals by the fiscal year end
    and fundamentals-v1, sentiment by last_at's UTC date and weights_version.
    """
    cited: dict[str, tuple[Decimal, date, str, int]] = {}
    if fundamentals["score"] is not None:
        cited["fundamentals"] = (
            fundamentals["score"],
            fundamentals["fiscal_year_end"],
            fundamentals["score_version"],
            fundamentals["id"],
        )
    if sentiment is not None and sentiment["value"] is not None:
        cited["sentiment"] = (
            normalize_sentiment(sentiment["value"]),
            _utc_date(sentiment["last_at"]),
            sentiment["weights_version"],
            sentiment["id"],
        )
    result = blend({name: values[0] for name, values in cited.items()}, weights)
    record: dict[str, Any] = {
        "ticker": symbol,
        "status": result.status,
        "unavailable_reason": result.unavailable_reason,
        "score": result.score,
        "score_version": result.version,
        "missing": list(result.missing),
        "recommended": result.score is not None
        and result.score >= RECOMMEND_AT
        and listed
        and publication_id is not None,
        "watchlist_publication_id": publication_id,
    }
    for c in result.components:
        _, as_of, version, record_id = cited.get(c.name, (None, None, None, None))
        record.update(
            {
                f"{c.name}_value": c.value,
                f"{c.name}_nominal_weight": c.nominal_weight,
                f"{c.name}_weight": c.weight,
                f"{c.name}_contribution": c.contribution,
                f"{c.name}_input_as_of": as_of,
                f"{c.name}_input_version": version,
                f"{c.name}_record_id": record_id,
            }
        )
    return record


def _scores_outcome(
    conn: Connection,
    fundamentals_batch: int,
    sentiment_batch: int | None,
    weights: ScoreWeights,
) -> CollectionOutcome:
    sentiment = read_ticker_sentiment(conn, sentiment_batch) if sentiment_batch else {}
    publication = _publication(conn)
    publication_id, listed = publication if publication else (None, set())
    records = [
        score_record(
            row["symbol"],
            row,
            sentiment.get(row["security_id"]),
            publication_id,
            row["symbol"] in listed,
            weights,
        )
        for row in sorted(
            batch_rows(conn, FUNDAMENTALS, fundamentals_batch),
            key=lambda r: r["symbol"],
        )
    ]
    envelope: dict[str, Any] = {
        **weights.envelope(),
        "fundamental_scores_batch_id": fundamentals_batch,
        "universe": len(records),
        "scored": sum(r["status"] == "ok" for r in records),
        "unavailable_count": sum(r["status"] != "ok" for r in records),
    }
    if sentiment_batch is not None:
        envelope["ticker_sentiment_batch_id"] = sentiment_batch
    if publication_id is not None:
        envelope["watchlist_publication_id"] = publication_id
    return CollectionOutcome.success(
        SCORES,
        "derived",
        records,
        coverage={"complete": True, "scope": "derivation"},
        envelope=envelope,
    )


def _promote(engine: Engine, batch_id: int) -> None:
    """Point current_scores at the batch; an unchanged pointer keeps its promoted_at."""
    table = m.current_scores
    stmt = pg_insert(table).values(
        channel=CHANNEL, batch_id=batch_id, promoted_at=func.now()
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[table.c.channel],
        set_={"batch_id": stmt.excluded.batch_id, "promoted_at": func.now()},
        where=table.c.batch_id != stmt.excluded.batch_id,
    )
    with engine.begin() as conn:
        conn.execute(stmt)


def derive_scores(
    engine: Engine, weights: ScoreWeights = ScoreWeights()
) -> ImportResult:
    """Score every security in the latest good fundamentals batch, then promote."""
    with source_lock(engine, SCORES):
        with engine.connect() as conn:
            fundamentals = latest_successful_run(conn, FUNDAMENTALS)
            sentiment = latest_successful_run(conn, SENTIMENT)
        dependencies = {
            source: run.batch_id
            for source, run in ((FUNDAMENTALS, fundamentals), (SENTIMENT, sentiment))
            if run is not None
        }
        run_id = begin_run(engine, SCORES, "derived", dependencies=dependencies)
        with heartbeat_worker(engine, run_id):
            if fundamentals is None:
                outcome = CollectionOutcome.failure(
                    SCORES, "derived", "missing_upstream"
                )
            else:
                try:
                    with engine.connect() as conn:
                        outcome = _scores_outcome(
                            conn,
                            fundamentals.batch_id,
                            sentiment.batch_id if sentiment else None,
                            weights,
                        )
                except Exception as exc:  # a bug in the job: record it, write nothing
                    outcome = CollectionOutcome.failure(
                        SCORES,
                        "derived",
                        "internal_error",
                        diagnostics=exception_diagnostics(exc),
                    )
            result = import_collection(engine, outcome, run_id=run_id)
        if result.succeeded and result.batch_id is not None:
            _promote(engine, result.batch_id)
    logger.info(
        "company scores status=%s code=%s run_id=%s batch_id=%s reused=%s",
        result.status,
        result.error_code,
        result.run_id,
        result.batch_id,
        result.reused_batch,
    )
    return result


# ── JOB-5: derive_explanations ───────────────────────────


def _rows_by_id(
    conn: Connection, table: Any, ids: set[int]
) -> dict[int, dict[str, Any]]:
    if not ids:
        return {}
    found = conn.execute(select(table).where(table.c.id.in_(ids))).mappings()
    return {row["id"]: dict(row) for row in found}


def _latest_explanations(
    conn: Connection, score_ids: set[int]
) -> dict[int, dict[str, Any]]:
    """The newest stored explanation of each scores row."""
    table = m.score_explanations
    found = conn.execute(
        select(table)
        .where(table.c.company_score_id.in_(score_ids))
        .order_by(table.c.id.desc())
    ).mappings()
    latest: dict[int, dict[str, Any]] = {}
    for row in found:
        latest.setdefault(row["company_score_id"], dict(row))
    return latest


def _window_days(conn: Connection, batch_ids: set[int]) -> dict[int, int] | None:
    """Each cited ticker_sentiment batch's window, from its envelope (decision 108),
    or None when any of them has no positive int window_days. The rule is on the
    cited batch, whether or not a claim ends up needing the window."""
    if not batch_ids:
        return {}
    found = conn.execute(
        select(m.source_batches.c.id, m.source_batches.c.payload).where(
            m.source_batches.c.id.in_(batch_ids)
        )
    ).tuples()
    windows = {
        batch_id: (payload.get("envelope") or {}).get("window_days")
        for batch_id, payload in found
    }
    # Exactly an int of at least 1, as explain() requires: not a bool, nothing coerced.
    usable = {b: w for b, w in windows.items() if type(w) is int and w >= 1}
    return usable if len(usable) == len(windows) else None


def _explanations_outcome(conn: Connection, scores_batch: int) -> CollectionOutcome:
    scores = sorted(batch_rows(conn, SCORES, scores_batch), key=lambda r: r["symbol"])
    fundamentals = _rows_by_id(
        conn,
        m.fundamental_scores,
        {r["fundamentals_record_id"] for r in scores if r["fundamentals_record_id"]},
    )
    sentiment = _rows_by_id(
        conn,
        m.ticker_sentiment,
        {r["sentiment_record_id"] for r in scores if r["sentiment_record_id"]},
    )
    windows = _window_days(conn, {r["batch_id"] for r in sentiment.values()})
    if windows is None:
        return CollectionOutcome.failure(EXPLANATIONS, "derived", "invalid_upstream")
    previous = _latest_explanations(conn, {r["id"] for r in scores})
    records = []
    carried = 0
    for row in scores:
        score = {k: v for k, v in row.items() if k != "symbol"}  # the table's columns
        before = previous.get(score["id"])
        cited = sentiment.get(score["sentiment_record_id"])
        explanation = explanations.explain(
            score,
            fundamentals.get(score["fundamentals_record_id"]),
            cited,
            before,
            window_days=windows.get(cited["batch_id"]) if cited else None,
        )
        carried += (
            before is not None and before["input_digest"] == explanation.input_digest
        )
        records.append(
            {
                "ticker": row["symbol"],
                "company_score_id": score["id"],
                "status": explanation.status,
                "reason": explanation.reason,
                "template_version": explanation.template_version,
                "claims": list(explanation.claims),
                "input_digest": explanation.input_digest,
            }
        )
    return CollectionOutcome.success(
        EXPLANATIONS,
        "derived",
        records,
        coverage={"complete": True, "scope": "derivation"},
        envelope={
            "template_version": explanations.TEMPLATE_VERSION,
            "company_scores_batch_id": scores_batch,
            "universe": len(records),
            "explained": sum(r["status"] == "ok" for r in records),
            "carried_forward_count": carried,
        },
    )


def derive_explanations(engine: Engine) -> ImportResult:
    """Explain every row of the scores batch current_scores points at."""
    with source_lock(engine, EXPLANATIONS):
        with engine.connect() as conn:
            scores_batch: int | None = conn.execute(
                select(m.current_scores.c.batch_id).where(
                    m.current_scores.c.channel == CHANNEL
                )
            ).scalar_one_or_none()
        dependencies = {SCORES: scores_batch} if scores_batch is not None else {}
        run_id = begin_run(engine, EXPLANATIONS, "derived", dependencies=dependencies)
        with heartbeat_worker(engine, run_id):
            if scores_batch is None:
                outcome = CollectionOutcome.failure(
                    EXPLANATIONS, "derived", "missing_upstream"
                )
            else:
                try:
                    with engine.connect() as conn:
                        outcome = _explanations_outcome(conn, scores_batch)
                except Exception as exc:  # a bug in the job: record it, write nothing
                    outcome = CollectionOutcome.failure(
                        EXPLANATIONS,
                        "derived",
                        "internal_error",
                        diagnostics=exception_diagnostics(exc),
                    )
            result = import_collection(engine, outcome, run_id=run_id)
    logger.info(
        "score explanations status=%s code=%s run_id=%s batch_id=%s reused=%s",
        result.status,
        result.error_code,
        result.run_id,
        result.batch_id,
        result.reused_batch,
    )
    return result


# ── JOB-6: --job scores ──────────────────────────────────


def run_once(settings: Any = None) -> int:
    """Scheduler entry point: scores, then their explanations; 0 when both succeed."""
    from src.db import session
    from src.ingestion.redaction import describe_exception
    from src.settings import load_settings

    engine = None
    try:
        settings = load_settings() if settings is None else settings
        engine = session.make_engine(settings.database_url.get_secret_value())
        if not derive_scores(engine).succeeded:
            return 1
        return 0 if derive_explanations(engine).succeeded else 1
    except Exception as exc:
        logger.error("scores run failed: %s", describe_exception(exc))
        return 1
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    from src.utils.logger import configure_logging

    configure_logging()
    sys.exit(run_once())
