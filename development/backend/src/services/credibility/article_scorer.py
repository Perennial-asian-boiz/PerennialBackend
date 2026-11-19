"""JOB-7: the S1-04 article scorer for #85's score_articles slot, FinBERT over the
headline and Finnhub's summary (a stand-in until the article-sentiment-scoring
work replaces it).

score_articles is a derived article_sentiment run. Its input, from
article_loader, is every stored news article published in derive_sentiment's
window that has no score from MODEL_VERSION yet, so an article that wasn't
scored is caught up by a later run, a new MODEL_VERSION re-scores the window
once (with fresh retries), and nothing outside the window is read. The
scorer reads the texts in news_article_id order, in chunks of at most
BATCH_SIZE.

Each news_articles row is one (story, company) pair, and Finnhub often ties one
story to several companies ("AMD gains share from Intel" is stored for both).
The text the model reads is therefore per company (recipe "+text3"): the
headline and the summary's sentences, less the ones that name another company
the same story is stored for but not this one. A story stored for one company
only reads as before (headline, then summary). If nothing is left, the headline
alone. Naming follows match-v1 (news/matching.py) sentence by sentence.

For each article it stores:

  * score: P(positive) - P(negative), in [-1, 1];
  * label: negative for score <= -0.2, positive for score >= 0.2, else neutral;
  * confidence: the model's probability for the label's own class, in [0, 1];
  * model: MODEL_VERSION.

A chunk that fails (the scorer raises or answers badly) is skipped and the next
chunk is scored. A chunk that takes longer than BATCH_TIMEOUT seconds stops the
run's scoring: the model is still busy with it, so the remaining chunks are not
attempted. Whatever was scored is stored, and the run records which chunks
failed ("BATCH-<n>", code "failed" or "timeout") and the counts. With nothing
scored at all (including the model failing to load: torch or transformers
missing, or the weights unavailable) the run fails "scoring_failed" with no
batch. With nothing new to score there is no run at all. No headline or
summary reaches a log or the run record.

Retries (AC 4, article_loader.record_attempts): after the import, each article
in a failed or timed-out chunk gains one attempt with that code, and if the
import itself fails every scored article gains one with "import_failed". An
article at MAX_ATTEMPTS is not read again for this MODEL_VERSION. Articles that
were never attempted (after a timeout) and a model that fails to load don't
count against any article. A scored article's attempts are forgotten.

torch and transformers are imported only on the scoring path, so importing this
module (and CI, which installs neither) never needs them. On first load,
transformers fetches the pinned weights from the Hugging Face hub.
"""

import logging
import math
import re
from collections.abc import Callable, Collection, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from src.db.queries import latest_successful_run
from src.ingestion.diagnostics import exception_diagnostics
from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection
from src.pipeline.lifecycle import begin_run, heartbeat_worker, source_lock
from src.services.credibility.article_loader import (
    co_tagged,
    record_attempts,
    unscored_articles,
)
from src.services.credibility.job import WINDOW_DAYS
from src.services.credibility.labels import (  # noqa: F401  (re-exported)
    NEGATIVE_AT_MOST,
    POSITIVE_AT_LEAST,
    label_for,
)
from src.services.news.job import company_names
from src.services.news.matching import match_rule, name_variants

logger = logging.getLogger(__name__)

SOURCE, NEWS = "article_sentiment", "news_articles"
MODEL_NAME = "ProsusAI/finbert"
MODEL_REVISION = "0bc2ac10dda6929bcb1f77ccc43c7ec5c424f031"  # main as of 2026-11-01
# The suffix names the text recipe, so scores of different recipes never share a
# version: none is the headline alone, "+text2" headline + summary (decision
# 114), "+text3" headline + summary less the sentences that name only another
# company the same story is stored for.
MODEL_VERSION = f"{MODEL_NAME}@{MODEL_REVISION}+text3"
MAX_TEXT_CHARS = 512  # BERT reads at most 512 tokens; a headline is far shorter
BATCH_SIZE = 32
BATCH_TIMEOUT = 120.0  # seconds one chunk may take before the run stops scoring
LABELS = ("positive", "negative", "neutral")
UNIT_KIND = "article batches"

# texts -> one {"positive", "negative", "neutral"} probability mapping per text.
Scorer = Callable[[Sequence[str]], Sequence[Mapping[str, float]]]


# A sentence ends at . ! or ? followed by space and a capital, quote, bracket or
# $, so "Apple Inc. reported" and "3.5%" stay whole.
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[A-Z\"'(\[$])")


@dataclass(frozen=True)
class Company:
    """A symbol and its match-v1 name variants."""

    symbol: str
    variants: frozenset[str] = frozenset()

    def named_in(self, text: str) -> bool:
        return match_rule(text, "", self.symbol, set(self.variants)) is not None


def sentences(text: str) -> list[str]:
    """The non-blank sentences of a text, whitespace normalized."""
    return [p for p in (" ".join(s.split()) for s in _SENTENCE_END.split(text)) if p]


def article_text(
    article: Mapping[str, Any],
    company: Company | None = None,
    others: Collection[Company] = (),
) -> str:
    """What the model reads for `company`, capped at MAX_TEXT_CHARS: the headline,
    then the stored summary on its own line, each without the sentences that name
    one of `others` (the other companies the story is stored for) but not
    `company`. With no others, the whole headline and summary; without a summary,
    the headline alone; with nothing left, the headline."""
    headline = str(article["headline"]).strip()
    summary = (article.get("summary") or "").strip()
    if company is not None and others:

        def kept(text: str) -> str:
            return " ".join(
                s
                for s in sentences(text)
                if company.named_in(s) or not any(o.named_in(s) for o in others)
            )

        focused = "\n".join(t for t in (kept(headline), kept(summary)) if t)
        return (focused or headline)[:MAX_TEXT_CHARS]
    return (f"{headline}\n{summary}" if summary else headline)[:MAX_TEXT_CHARS]


def companies_for(
    conn: Connection, articles: Sequence[Any]
) -> dict[int, list[Company]]:
    """{news_article_id: [its own company, then the story's other companies]}."""
    tags = co_tagged(conn, [a.finnhub_id for a in articles])
    symbols = sorted({a.symbol for a in articles}.union(*tags.values()))
    names = company_names(conn, symbols)
    company = {
        s: Company(s, frozenset(name_variants(names.get(s, [])))) for s in symbols
    }
    return {
        a.id: [company[a.symbol]]
        + [company[s] for s in sorted(tags.get(a.finnhub_id, set()) - {a.symbol})]
        for a in articles
    }


def polarity(probabilities: Mapping[str, float]) -> Decimal:
    """P(positive) - P(negative), to 4 places."""
    value = probabilities["positive"] - probabilities["negative"]
    return Decimal(f"{value:.4f}") + 0  # + 0 turns "-0.0000" into "0.0000"


def confidence(probabilities: Mapping[str, float], label: str) -> Decimal:
    """The model's probability for the label's own class, to 4 places."""
    return Decimal(f"{probabilities[label]:.4f}")


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


def _record(article: Any, probabilities: Mapping[str, float]) -> dict[str, str]:
    score = polarity(probabilities)
    label = label_for(score)
    return {
        "ticker": article.symbol,
        "news_article_id": article.id,
        "score": str(score),
        "label": label,
        "confidence": str(confidence(probabilities, label)),
        "model": MODEL_VERSION,
    }


@dataclass
class _Scored:
    """What scoring produced: one record per scored article, and the chunks that
    failed as {"unit": "BATCH-<n>", "code": "failed" | "timeout"}."""

    records: list[dict[str, str]] = field(default_factory=list)
    failures: list[dict[str, str]] = field(default_factory=list)
    failed: dict[int, str] = field(default_factory=dict)  # article id -> code
    chunks: int = 0
    not_attempted: int = 0


def _score_chunks(
    articles: Sequence[Any],
    score: Scorer,
    companies: Mapping[int, Sequence[Company]] | None = None,
) -> _Scored:
    """Score every chunk, skipping one that fails; stop at the first timeout.
    companies: per article id, its own company then the story's other ones."""
    by_id = companies or {}

    def text(a: Any) -> str:
        own = by_id.get(a.id) or []
        return article_text(a._mapping, own[0] if own else None, own[1:])

    chunks = [articles[i : i + BATCH_SIZE] for i in range(0, len(articles), BATCH_SIZE)]
    out = _Scored(chunks=len(chunks))
    pool = ThreadPoolExecutor(max_workers=1)  # one chunk at a time, with a deadline
    try:
        for n, chunk in enumerate(chunks, start=1):
            texts = [text(a) for a in chunk]
            try:
                answers = pool.submit(score, texts).result(timeout=BATCH_TIMEOUT)
                _checked(answers, len(chunk))
            except FutureTimeout:
                out.failures.append({"unit": f"BATCH-{n}", "code": "timeout"})
                out.failed.update((a.id, "timeout") for a in chunk)
                out.not_attempted = len(chunks) - n
                break
            except Exception:  # the class name isn't kept: never a headline
                out.failures.append({"unit": f"BATCH-{n}", "code": "failed"})
                out.failed.update((a.id, "failed") for a in chunk)
                continue
            out.records += [_record(a, p) for a, p in zip(chunk, answers)]
    finally:
        pool.shutdown(wait=False, cancel_futures=True)  # never wait on a hung chunk
    return out


def _scores_outcome(scored: _Scored, news_batch: int) -> CollectionOutcome:
    attempted = scored.chunks - scored.not_attempted
    diagnostics = {
        "unit_kind": UNIT_KIND,
        "planned": scored.chunks,
        "attempted": attempted,
        "completed": attempted - len(scored.failures),
        "failed": len(scored.failures),
        "not_attempted": scored.not_attempted,
        "failures": scored.failures,
    }
    if not scored.records:
        return CollectionOutcome.failure(
            SOURCE, "derived", "scoring_failed", diagnostics=diagnostics
        )
    return CollectionOutcome.success(
        SOURCE,
        "derived",
        scored.records,
        coverage={"complete": not scored.failures, "scope": "derivation"},
        envelope={
            "model": MODEL_VERSION,
            "input_batch_id": news_batch,
            "articles_scored": len(scored.records),
        },
        diagnostics=diagnostics if scored.failures else None,
    )


def _record_attempts(engine: Engine, scored: _Scored, result: ImportResult) -> None:
    ids = [int(r["news_article_id"]) for r in scored.records]
    failed = dict(scored.failed)
    if result.succeeded:
        done: list[int] = ids
    else:  # nothing was stored: the scored articles failed too
        done = []
        failed.update((i, "import_failed") for i in ids)
    try:
        record_attempts(engine, MODEL_VERSION, done, failed)
    except SQLAlchemyError as exc:  # the scores are stored; only retries are lost
        logger.warning("article scores: attempts not recorded (%s)", type(exc).__name__)


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
            articles = (
                unscored_articles(conn, start, now, MODEL_VERSION)
                if news is not None
                else []
            )
            companies = companies_for(conn, articles) if articles else {}
        if news is not None and not articles:
            logger.info("article scores: nothing new to score")
            return None
        dependencies = {NEWS: news.batch_id} if news is not None else {}
        scored: _Scored | None = None
        run_id = begin_run(engine, SOURCE, "derived", dependencies=dependencies)
        with heartbeat_worker(engine, run_id):
            if news is None:
                outcome = CollectionOutcome.failure(
                    SOURCE, "derived", "missing_upstream"
                )
            else:
                try:
                    scored = _score_chunks(articles, scorer or finbert(), companies)
                    outcome = _scores_outcome(scored, news.batch_id)
                except Exception as exc:  # the class name only: never a headline
                    outcome = CollectionOutcome.failure(
                        SOURCE,
                        "derived",
                        "scoring_failed",
                        diagnostics=exception_diagnostics(exc),
                    )
            result = import_collection(engine, outcome, run_id=run_id)
            if scored is not None:
                _record_attempts(engine, scored, result)
    logger.info(
        "article scores status=%s code=%s run_id=%s batch_id=%s scored=%s",
        result.status,
        result.error_code,
        result.run_id,
        result.batch_id,
        result.accepted_count,
    )
    return result
