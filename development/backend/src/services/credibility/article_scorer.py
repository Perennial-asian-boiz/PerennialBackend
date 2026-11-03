"""JOB-7: the S1-04 article scorer for #85's score_articles slot, FinBERT over the
headline and Finnhub's summary (a stand-in until the article-sentiment-scoring
work replaces it).

score_articles is a derived article_sentiment run. Its input is every stored
news article published in derive_sentiment's window that has no score from
MODEL_VERSION yet, so a failed run is caught up by the next one, a new
MODEL_VERSION re-scores the window once, and nothing outside the window is
read. The scorer reads the texts in news_article_id order, in chunks of at most
BATCH_SIZE; each article's score is P(positive) - P(negative). With nothing new
to score there is no run at all.

The run fails closed: the model failing to load (torch or transformers missing,
or the weights unavailable) or the scorer raising or answering badly fails it
"scoring_failed" with no batch, and no headline or summary reaches a log or the
run record.

torch and transformers are imported only on the scoring path, so importing this
module (and CI, which installs neither) never needs them. On first load,
transformers fetches the pinned weights from the Hugging Face hub.
"""

import logging
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import exists, select
from sqlalchemy.engine import Connection, Engine
from src.db import models as m
from src.db.queries import latest_successful_run
from src.ingestion.diagnostics import exception_diagnostics
from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection
from src.pipeline.lifecycle import begin_run, heartbeat_worker, source_lock
from src.services.credibility.job import WINDOW_DAYS

logger = logging.getLogger(__name__)

SOURCE, NEWS = "article_sentiment", "news_articles"
MODEL_NAME = "ProsusAI/finbert"
MODEL_REVISION = "0bc2ac10dda6929bcb1f77ccc43c7ec5c424f031"  # main as of 2026-11-01
# "+text2" is the text recipe, headline + summary (decision 114): scores of the
# headline alone carry no suffix, so the two never share a version.
MODEL_VERSION = f"{MODEL_NAME}@{MODEL_REVISION}+text2"
MAX_TEXT_CHARS = 512  # BERT reads at most 512 tokens; a headline is far shorter
BATCH_SIZE = 32
LABELS = ("positive", "negative", "neutral")

# texts -> one {"positive", "negative", "neutral"} probability mapping per text.
Scorer = Callable[[Sequence[str]], Sequence[Mapping[str, float]]]


def article_text(article: Mapping[str, Any]) -> str:
    """What the model reads: the headline, then the stored summary on its own
    line (the news job's content-hash join), capped. Without a summary, the
    headline alone."""
    headline = str(article["headline"]).strip()
    summary = (article.get("summary") or "").strip()
    return (f"{headline}\n{summary}" if summary else headline)[:MAX_TEXT_CHARS]


def polarity(probabilities: Mapping[str, float]) -> Decimal:
    """P(positive) - P(negative), to 4 places."""
    value = probabilities["positive"] - probabilities["negative"]
    return Decimal(f"{value:.4f}") + 0  # + 0 turns "-0.0000" into "0.0000"


def _probability(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("a probability is missing or not a number")
    if not 0 <= value <= 1:  # NaN fails this too
        raise ValueError("a probability is outside [0, 1]")
    return float(value)


def _checked(answers: Sequence[Mapping[str, float]], count: int) -> None:
    """One probability distribution over LABELS per text, else ValueError."""
    if len(answers) != count:
        raise ValueError("the scorer answered a different number of texts")
    for answer in answers:
        total = sum(_probability(answer.get(label)) for label in LABELS)
        if not math.isclose(total, 1, abs_tol=1e-3):
            raise ValueError("the probabilities don't sum to 1")


def finbert() -> Scorer:
    """FinBERT at the pinned revision. Raises if torch, transformers or the
    weights are unavailable."""
    import torch
    from transformers import AutoModelForSequenceClassification, AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, revision=MODEL_REVISION)
    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_NAME, revision=MODEL_REVISION
    )
    model.eval()
    labels = [model.config.id2label[i].lower() for i in range(model.config.num_labels)]

    def score(texts: Sequence[str]) -> list[dict[str, float]]:
        inputs = tokenizer(
            list(texts),
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="pt",
        )
        with torch.no_grad():
            rows = torch.softmax(model(**inputs).logits, dim=-1).tolist()
        return [dict(zip(labels, row)) for row in rows]

    return score


def _unscored(conn: Connection, start: datetime, end: datetime) -> list[Any]:
    """The window's articles with no score from MODEL_VERSION, by id."""
    na, a = m.news_articles, m.article_sentiment
    scored = exists().where(a.c.news_article_id == na.c.id, a.c.model == MODEL_VERSION)
    return list(
        conn.execute(
            select(na.c.id, m.securities.c.symbol, na.c.headline, na.c.summary)
            .join(m.securities, m.securities.c.id == na.c.security_id)
            .where(na.c.published_at >= start, na.c.published_at <= end, ~scored)
            .order_by(na.c.id)
        ).all()
    )


def _scores_outcome(
    articles: Sequence[Any], scorer: Scorer | None, news_batch: int
) -> CollectionOutcome:
    score = scorer or finbert()
    records = []
    for i in range(0, len(articles), BATCH_SIZE):
        chunk = articles[i : i + BATCH_SIZE]
        answers = score([article_text(a._mapping) for a in chunk])
        _checked(answers, len(chunk))
        records += [
            {
                "ticker": article.symbol,
                "news_article_id": article.id,
                "score": str(polarity(answer)),
                "model": MODEL_VERSION,
            }
            for article, answer in zip(chunk, answers)
        ]
    return CollectionOutcome.success(
        SOURCE,
        "derived",
        records,
        coverage={"complete": True, "scope": "derivation"},
        envelope={
            "model": MODEL_VERSION,
            "input_batch_id": news_batch,
            "articles_scored": len(records),
        },
    )


def score_articles(
    engine: Engine, *, scorer: Scorer | None = None, now: datetime | None = None
) -> ImportResult | None:
    """Score the window's new articles as a derived run; None when there were none.
    scorer=None means FinBERT."""
    now = now or datetime.now(timezone.utc)
    start = now - timedelta(days=WINDOW_DAYS)
    with source_lock(engine, SOURCE):
        with engine.connect() as conn:
            news = latest_successful_run(conn, NEWS)
            articles = _unscored(conn, start, now) if news is not None else []
        if news is not None and not articles:
            logger.info("article scores: nothing new to score")
            return None
        dependencies = {NEWS: news.batch_id} if news is not None else {}
        run_id = begin_run(engine, SOURCE, "derived", dependencies=dependencies)
        with heartbeat_worker(engine, run_id):
            if news is None:
                outcome = CollectionOutcome.failure(
                    SOURCE, "derived", "missing_upstream"
                )
            else:
                try:
                    outcome = _scores_outcome(articles, scorer, news.batch_id)
                except Exception as exc:  # the class name only: never a headline
                    outcome = CollectionOutcome.failure(
                        SOURCE,
                        "derived",
                        "scoring_failed",
                        diagnostics=exception_diagnostics(exc),
                    )
            result = import_collection(engine, outcome, run_id=run_id)
    logger.info(
        "article scores status=%s code=%s run_id=%s batch_id=%s scored=%s",
        result.status,
        result.error_code,
        result.run_id,
        result.batch_id,
        result.accepted_count,
    )
    return result
