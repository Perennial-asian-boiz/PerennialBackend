"""The Confidence Score jobs (Sprint 2 JOB-2, JOB-3, JOB-5, JOB-6). Run once with:

    python -m src.pipeline.scheduler --once --job scores

Two derived ingestion runs, each writing an immutable batch
(output/sprint-2/s2-stories-plan.md §3.1-§3.3):

* company_scores: score-v1 (scoring.blend) over the latest successful
  fundamental_scores batch and, when there is one, the latest successful
  ticker_sentiment batch, both named in run_dependencies. One row per security
  in the fundamentals batch. recommended is judged against the current
  production watchlist publication, whose id every row stores. Then
  current_scores['production'] points at the batch; the API reads scores only
  through it.
* score_explanations: explain-v1 over the pointer's scores batch (named in
  run_dependencies), one row per score row. A stored explanation for the same
  score row with an equal input digest is carried forward (S2-03 AC4); the
  digest covers the rows' ids, so a new scores batch regenerates (decision D1).

Unchanged inputs give identical content, so the importer reuses the stored
batch (as fundamental_scores does) and the pointer, with its promoted_at, stays
where it was (S2-01 AC2). Recomputing rather than skipping on equal dependency
ids means a new publication or a new score version is never missed.
"""

import logging
import sys
from collections.abc import Mapping, Sequence
from datetime import date
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
from src.services.scores import explain as explain_module
from src.services.scores import scoring

logger = logging.getLogger(__name__)

FUNDAMENTALS, SENTIMENT = "fundamental_scores", "ticker_sentiment"
SCORES, EXPLANATIONS = "company_scores", "score_explanations"
CHANNEL = "production"
RECOMMENDED_AT = Decimal("60")
# Whether "unresolved" candidates count is open (plan §8); they don't in v1.
MEMBER_BUCKETS = ("popular_stable", "affordable_growing")
# Per component: the cited record's as-of column and version column.
CITED = {
    "fundamentals": ("fiscal_year_end", "score_version"),
    "sentiment": ("window_end", "rollup_version"),
}


def _text(value: Decimal | date | None) -> str | None:
    if value is None:
        return None
    return value.isoformat() if isinstance(value, date) else str(value)


def _plain(row: Mapping[str, Any]) -> dict[str, Any]:
    """A stored row as its table's columns (batch_rows adds the symbol)."""
    return {k: v for k, v in row.items() if k != "symbol"}


# ── JOB-3: the sentiment contract reader ─────────────────


def read_ticker_sentiment(conn: Connection, batch_id: int) -> dict[int, dict[str, Any]]:
    """{security_id: row} of one ticker_sentiment batch. A component's as-of is
    window_end; a security with no row has no sentiment coverage."""
    source = conn.execute(
        select(m.source_batches.c.source).where(m.source_batches.c.id == batch_id)
    ).scalar_one_or_none()
    if source != SENTIMENT:
        raise ValueError("batch is not a ticker_sentiment batch")
    table = m.ticker_sentiment
    return {
        row["security_id"]: dict(row)
        for row in conn.execute(select(table).where(table.c.batch_id == batch_id))
        .mappings()
        .all()
    }


# ── JOB-2: derive_scores ─────────────────────────────────


def _production_publication(conn: Connection) -> tuple[int, set[str]] | None:
    """The current production publication's id and its listed tickers."""
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
        for bucket in MEMBER_BUCKETS
        for item in output.get(bucket) or []
        if isinstance(item, dict) and isinstance(item.get("ticker"), str)
    }
    return int(pub.id), listed


def score_record(
    symbol: str,
    fundamentals: Mapping[str, Any],
    sentiment: Mapping[str, Any] | None,
    publication_id: int | None,
    member: bool,
    weights: scoring.ScoreWeights = scoring.ScoreWeights(),
) -> dict[str, Any]:
    """One company_scores record from a stored fundamentals row and sentiment row."""
    values: dict[str, Decimal | None] = {}
    if fundamentals.get("score") is not None:
        values["fundamentals"] = fundamentals["score"]
    if sentiment is not None:
        values["sentiment"] = scoring.normalize_sentiment(sentiment["value"])
    result = scoring.blend(values, weights)
    recommended = (
        result.status == "ok"
        and result.score is not None
        and result.score >= RECOMMENDED_AT
        and member
        and publication_id is not None
    )
    record: dict[str, Any] = {
        "ticker": symbol,
        "status": result.status,
        "unavailable_reason": result.unavailable_reason,
        "score": _text(result.score),
        "score_version": result.version,
        "missing": list(result.missing),
        "recommended": recommended,
        "watchlist_publication_id": publication_id,
    }
    cited = {"fundamentals": fundamentals, "sentiment": sentiment}
    for c in result.components:
        record.update(
            {
                f"{c.name}_value": _text(c.value),
                f"{c.name}_nominal_weight": _text(c.nominal_weight),
                f"{c.name}_weight": _text(c.weight),
                f"{c.name}_contribution": _text(c.contribution),
                f"{c.name}_input_as_of": None,
                f"{c.name}_input_version": None,
                f"{c.name}_record_id": None,
            }
        )
        row = cited.get(c.name)
        if c.value is not None and row is not None and c.name in CITED:
            as_of, version = CITED[c.name]
            record[f"{c.name}_input_as_of"] = _text(row[as_of])
            record[f"{c.name}_input_version"] = row[version]
            record[f"{c.name}_record_id"] = row["id"]
    return record


def _promote(engine: Engine, batch_id: int) -> None:
    """Point current_scores at the batch; an unchanged pointer keeps promoted_at."""
    stmt = pg_insert(m.current_scores).values(
        channel=CHANNEL, batch_id=batch_id, promoted_at=func.now()
    )
    with engine.begin() as conn:
        conn.execute(
            stmt.on_conflict_do_update(
                index_elements=[m.current_scores.c.channel],
                set_={"batch_id": batch_id, "promoted_at": func.now()},
                where=m.current_scores.c.batch_id.is_distinct_from(batch_id),
            )
        )


def _scores_outcome(
    conn: Connection,
    fundamentals_batch: int,
    sentiment_batch: int | None,
    weights: scoring.ScoreWeights,
) -> CollectionOutcome:
    fundamentals = batch_rows(conn, FUNDAMENTALS, fundamentals_batch)
    sentiment = (
        read_ticker_sentiment(conn, sentiment_batch)
        if sentiment_batch is not None
        else {}
    )
    publication = _production_publication(conn)
    publication_id, listed = publication if publication else (None, set())
    if publication is None:
        logger.warning("company scores: no production publication; none recommended")
    records = [
        score_record(
            row["symbol"],
            _plain(row),
            sentiment.get(row["security_id"]),
            publication_id,
            row["symbol"] in listed,
            weights,
        )
        for row in sorted(fundamentals, key=lambda r: r["symbol"])
    ]
    unavailable = sum(1 for r in records if r["status"] != "ok")
    return CollectionOutcome.success(
        SCORES,
        "derived",
        records,
        coverage={"complete": True, "scope": "derivation"},
        envelope={
            **weights.envelope(),
            "fundamental_scores_batch_id": fundamentals_batch,
            "ticker_sentiment_batch_id": sentiment_batch,
            "watchlist_publication_id": publication_id,
            "universe": len(records),
            "scored": len(records) - unavailable,
            "unavailable_count": unavailable,
        },
    )


def derive_scores(
    engine: Engine, weights: scoring.ScoreWeights = scoring.ScoreWeights()
) -> ImportResult:
    """Blend the latest good fundamentals and sentiment batches, then promote."""
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
                except Exception as exc:  # a bug here: record it, write nothing
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


def _current_scores_batch(conn: Connection) -> int | None:
    batch: int | None = conn.execute(
        select(m.current_scores.c.batch_id).where(m.current_scores.c.channel == CHANNEL)
    ).scalar_one_or_none()
    return batch


def _by_id(
    conn: Connection, table: Any, ids: Sequence[int]
) -> dict[int, dict[str, Any]]:
    if not ids:
        return {}
    found = conn.execute(select(table).where(table.c.id.in_(ids))).mappings().all()
    return {row["id"]: dict(row) for row in found}


def _previous_explanations(
    conn: Connection, score_ids: Sequence[int]
) -> dict[int, dict[str, Any]]:
    """The newest stored explanation of each score row."""
    if not score_ids:
        return {}
    table = m.score_explanations
    found = (
        conn.execute(
            select(table)
            .where(table.c.company_score_id.in_(score_ids))
            .order_by(table.c.id.desc())
        )
        .mappings()
        .all()
    )
    out: dict[int, dict[str, Any]] = {}
    for row in found:
        out.setdefault(row["company_score_id"], dict(row))
    return out


def _explanations_outcome(conn: Connection, scores_batch: int) -> CollectionOutcome:
    scores = sorted(batch_rows(conn, SCORES, scores_batch), key=lambda r: r["symbol"])
    fundamentals = _by_id(
        conn,
        m.fundamental_scores,
        [r["fundamentals_record_id"] for r in scores if r["fundamentals_record_id"]],
    )
    sentiment = _by_id(
        conn,
        m.ticker_sentiment,
        [r["sentiment_record_id"] for r in scores if r["sentiment_record_id"]],
    )
    previous = _previous_explanations(conn, [r["id"] for r in scores])
    records = []
    carried = 0
    for row in scores:
        score = _plain(row)
        before = previous.get(score["id"])
        explanation = explain_module.explain(
            score,
            fundamentals.get(score["fundamentals_record_id"]),
            sentiment.get(score["sentiment_record_id"]),
            before,
        )
        carried += before is not None and (
            before.get("input_digest"),
            before.get("template_version"),
        ) == (explanation.input_digest, explanation.template_version)
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
            "template_version": explain_module.TEMPLATE_VERSION,
            "company_scores_batch_id": scores_batch,
            "universe": len(records),
            "explained": sum(1 for r in records if r["status"] == "ok"),
            "carried_forward_count": carried,
        },
    )


def derive_explanations(engine: Engine) -> ImportResult:
    """Explain the pointer's scores batch, as a derived run."""
    with source_lock(engine, EXPLANATIONS):
        with engine.connect() as conn:
            scores_batch = _current_scores_batch(conn)
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
                except Exception as exc:  # a bug here: record it, write nothing
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
    """Scheduler entry point: scores, then explanations; 0 only when both succeed."""
    from src.db.session import make_engine
    from src.ingestion.redaction import describe_exception
    from src.settings import load_settings

    engine = None
    try:
        settings = load_settings() if settings is None else settings
        engine = make_engine(settings.database_url.get_secret_value())
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
