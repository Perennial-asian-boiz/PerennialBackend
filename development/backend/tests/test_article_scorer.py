"""JOB-7: the S1-04 article scorer for #85's score_articles slot (a stand-in until
CohenK's article-sentiment-scoring replaces it). Spec-first; synthetic only, and a
fake scorer everywhere, so nothing is downloaded and torch is never imported.

Pinned interface, src/services/credibility/article_scorer.py (my proposal):

* MODEL_NAME = "ProsusAI/finbert"; MODEL_REVISION, a pinned 40-hex commit;
  MODEL_VERSION = f"{MODEL_NAME}@{MODEL_REVISION}+text2" since decision 114,
  stored as article_sentiment.model.
* article_text(article) -> str: the text the model reads, stripped and capped at
  MAX_TEXT_CHARS (at most 512). Decision 114 adds the stored summary after the
  headline (test_d114_article_summary); an article without one reads as before.
* polarity(probabilities) -> Decimal: P(positive) - P(negative), to 4 places.
* label_for(score) -> "negative" for [-1, -0.2], "neutral" for (-0.2, 0.2),
  "positive" for [0.2, 1]; confidence(probabilities, label) -> Decimal: the
  probability of the label's own class, to 4 places. Both stored per article.
* score_articles(engine, *, scorer=None, now=None) -> ImportResult, the slot's
  Callable[[Engine], ImportResult]. A derived article_sentiment run:
  - input: every stored news_articles row published in derive_sentiment's window
    ([now - WINDOW_DAYS, now]) that has no score for MODEL_VERSION yet. A failed
    run is caught up by the next one; nothing outside the window is read.
  - scorer(texts) -> one {"positive", "negative", "neutral"} probability mapping
    per text, called in chunks of at most BATCH_SIZE texts, ordered by
    news_article_id. None means FinBERT, whose torch and transformers imports
    happen inside the scoring path only.
  - one row per article (row_number in the importer's canonical order); run_dependencies =
    the latest successful news_articles batch; envelope {model, input_batch_id,
    articles_scored}.
  - nothing to score: no new batch and no new rows.
  - no news batch at all: failed, missing_upstream.
  - a chunk the scorer raises on or answers badly for is skipped and the next one
    is scored; a chunk slower than BATCH_TIMEOUT stops scoring (the rest are not
    attempted). What was scored is stored (coverage incomplete; the run's
    diagnostics name the failed chunks as BATCH-<n> with code failed/timeout);
    the skipped articles are scored by a later run.
  - retries (article_scoring_attempts): each article of a failed / timed-out
    chunk gains one attempt with code failed / timeout (every scored article
    gains import_failed if the import fails); at 3 attempts it is not read again
    for this MODEL_VERSION; a scored article's attempts row is deleted; articles
    never attempted and a model that fails to load count against nothing.
  - nothing scored at all (every chunk failed, or the model failing to load):
    failed, "scoring_failed" (in ERROR_SUMMARIES), no batch, no headline in any log.
* credibility.job.run_once passes score_articles to run_sentiment.
"""

import ast
import logging
import subprocess
import sys
import time
import types
from contextlib import contextmanager
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func, select, text
from src.db import models
from src.services.credibility import job as credibility_job
from src.services.news.finnhub import RawArticle
from src.services.news.job import run_news
from test_credibility import (  # noqa: F401  (seeded is a fixture)
    NOW,
    NewsClient,
    seeded,
)

BACKEND = Path(__file__).resolve().parents[1]
MODULE = BACKEND / "src" / "services" / "credibility" / "article_scorer.py"
WINDOW = timedelta(days=credibility_job.WINDOW_DAYS)


def scorer_module():
    from src.services.credibility import article_scorer

    return article_scorer


def news(fid, headline, at):
    return RawArticle(
        fid, headline, "", "Yahoo", f"https://finnhub.io/api/news?id={fid}", at
    )


IN_WINDOW = {
    "RKLB": [
        news(11, "Rocket Lab wins a launch contract", NOW - timedelta(days=1)),
        news(12, "Rocket Lab delays a launch", NOW - timedelta(days=2)),
    ],
    "NVDA": [news(13, "NVDA rises", NOW - timedelta(days=3))],
}
OLD = {"NVDA": [news(14, "NVDA fell weeks ago", NOW - WINDOW - timedelta(days=1))]}


class Fake:
    """A scorer: probabilities from a table keyed by text, and a log of its calls."""

    def __init__(self, table=None, fail_on_call=None):
        self.table, self.calls, self.fail_on_call = table or {}, [], fail_on_call

    def __call__(self, texts):
        self.calls.append(list(texts))
        if self.fail_on_call is not None and len(self.calls) == self.fail_on_call:
            raise RuntimeError("synthetic model failure")
        return [
            self.table.get(t, {"positive": 0.6, "negative": 0.1, "neutral": 0.3})
            for t in texts
        ]

    @property
    def texts(self):
        return [t for call in self.calls for t in call]


def stored_articles(engine):
    with engine.connect() as conn:
        return {
            r.finnhub_id: r
            for r in conn.execute(
                select(models.news_articles, models.securities.c.symbol).join(
                    models.securities
                )
            )
        }


def scores(engine):
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                models.news_articles.c.finnhub_id,
                models.article_sentiment.c.score,
                models.article_sentiment.c.label,
                models.article_sentiment.c.confidence,
                models.article_sentiment.c.model,
                models.article_sentiment.c.batch_id,
                models.article_sentiment.c.row_number,
            ).join(
                models.news_articles,
                models.news_articles.c.id == models.article_sentiment.c.news_article_id,
            )
        ).all()
    return {r.finnhub_id: r for r in rows}


def attempts(engine):
    """{finnhub_id: (attempts, last_error)} for the current MODEL_VERSION."""
    t = models.article_scoring_attempts
    with engine.connect() as conn:
        rows = conn.execute(
            select(models.news_articles.c.finnhub_id, t.c.attempts, t.c.last_error)
            .join(
                models.news_articles, models.news_articles.c.id == t.c.news_article_id
            )
            .where(t.c.model == scorer_module().MODEL_VERSION)
        ).all()
    return {r.finnhub_id: (r.attempts, r.last_error) for r in rows}


def batches(engine):
    with engine.connect() as conn:
        return conn.execute(
            select(func.count()).where(
                models.source_batches.c.source == "article_sentiment"
            )
        ).scalar_one()


def latest_news_batch(engine):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT batch_id FROM perennial.ingestion_runs WHERE source = 'news_articles'"
                " AND status = 'succeeded' ORDER BY finished_at DESC, id DESC LIMIT 1"
            )
        ).scalar_one()


def run_of(engine, result):
    with engine.connect() as conn:
        return (
            conn.execute(
                select(models.ingestion_runs).where(
                    models.ingestion_runs.c.id == result.run_id
                )
            )
            .mappings()
            .one()
        )


def envelope(engine, batch_id):
    with engine.connect() as conn:
        return conn.execute(
            select(models.source_batches.c.payload).where(
                models.source_batches.c.id == batch_id
            )
        ).scalar_one()["envelope"]


def dependencies(engine, run_id):
    with engine.connect() as conn:
        return dict(
            conn.execute(
                text(
                    "SELECT source, batch_id FROM perennial.run_dependencies WHERE run_id = :r"
                ),
                {"r": run_id},
            ).all()
        )


def score_now(engine, scorer):
    return scorer_module().score_articles(engine, scorer=scorer, now=NOW)


# ── the model and the text (pure) ────────────────────────


def test_the_model_is_finbert_at_a_pinned_revision():
    mod = scorer_module()
    assert mod.MODEL_NAME == "ProsusAI/finbert"
    assert len(mod.MODEL_REVISION) == 40 and set(mod.MODEL_REVISION) <= set(
        "0123456789abcdef"
    )
    # Decision 114: "+text2" names the headline + summary text (test_d114_article_summary).
    assert mod.MODEL_VERSION == f"ProsusAI/finbert@{mod.MODEL_REVISION}+text2"
    assert (
        len(mod.MODEL_VERSION) <= 64
    )  # article_sentiment.model, ticker_sentiment.model_version


@pytest.mark.parametrize(
    "probabilities,expected",
    [
        ({"positive": 0.8, "negative": 0.1, "neutral": 0.1}, Decimal("0.7000")),
        ({"positive": 0.05, "negative": 0.9, "neutral": 0.05}, Decimal("-0.8500")),
        ({"positive": 0.2, "negative": 0.2, "neutral": 0.6}, Decimal("0.0000")),
        ({"positive": 1.0, "negative": 0.0, "neutral": 0.0}, Decimal("1.0000")),
        ({"positive": 0.0, "negative": 1.0, "neutral": 0.0}, Decimal("-1.0000")),
    ],
)
def test_polarity_is_p_positive_minus_p_negative(probabilities, expected):
    assert scorer_module().polarity(probabilities) == expected


@pytest.mark.parametrize(
    "score,label",
    [
        (Decimal("-1.0000"), "negative"),
        (Decimal("-0.2000"), "negative"),
        (Decimal("-0.1999"), "neutral"),
        (Decimal("0.0000"), "neutral"),
        (Decimal("0.1999"), "neutral"),
        (Decimal("0.2000"), "positive"),
        (Decimal("1.0000"), "positive"),
    ],
)
def test_the_label_comes_from_the_score_thresholds(score, label):
    assert scorer_module().label_for(score) == label


def test_the_confidence_is_the_probability_of_the_labels_own_class():
    mod = scorer_module()
    # +0.15 is neutral although positive is the single largest class.
    probabilities = {"positive": 0.40, "negative": 0.25, "neutral": 0.35}
    label = mod.label_for(mod.polarity(probabilities))
    assert label == "neutral"
    assert mod.confidence(probabilities, label) == Decimal("0.3500")
    assert mod.confidence(
        {"positive": 0.8, "negative": 0.1, "neutral": 0.1}, "positive"
    ) == (Decimal("0.8000"))


def test_the_text_is_the_headline_only_stripped_and_capped():
    mod = scorer_module()
    assert 0 < mod.MAX_TEXT_CHARS <= 512
    article = {
        "headline": "  Rocket Lab wins  ",
        "url": "https://x.example/a",
        "outlet": "Yahoo",
    }
    assert mod.article_text(article) == "Rocket Lab wins"
    long = {"headline": "Rocket Lab " + "x" * 2000}
    assert mod.article_text(long) == long["headline"][: mod.MAX_TEXT_CHARS]


# ── the module: no network, no torch at import, no default database ──


def test_importing_the_module_imports_neither_torch_nor_transformers():
    code = (
        "import sys; import src.services.credibility.article_scorer;"
        " print(sorted(m for m in ('torch', 'transformers') if m in sys.modules))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        cwd=BACKEND,
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == "[]"


BANNED_IMPORTS = {
    "requests",
    "urllib",
    "urllib3",
    "http",
    "httpx",
    "aiohttp",
    "socket",
    "bs4",
    "selenium",
    "playwright",
    "newspaper",
    "trafilatura",
}


def test_the_module_has_no_network_client_scraper_or_default_database():
    source = MODULE.read_text()
    tree = ast.parse(source)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            imported.add(node.module.split(".")[0])
    assert not imported & BANNED_IMPORTS, imported & BANNED_IMPORTS
    for banned in (
        "User-Agent",
        "Mozilla",
        "Chrome",
        "SUPABASE",
        "postgres",
        "DATABASE_URL",
        "create_engine",
        "make_engine",
    ):
        assert banned.lower() not in source.lower(), banned
    # torch and transformers only inside a function (lazy).
    top_level = {
        a.name.split(".")[0]
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for a in (
            node.names
            if isinstance(node, ast.Import)
            else [ast.alias(node.module or "")]
        )
    }
    assert not top_level & {"torch", "transformers"}


# ── the run ──────────────────────────────────────────────


@pytest.mark.db
def test_every_article_in_the_window_gets_one_row_and_the_run_names_its_input(
    engine,
    seeded,  # noqa: F811
):
    run_news(
        engine,
        NewsClient({**IN_WINDOW, "NVDA": IN_WINDOW["NVDA"] + OLD["NVDA"]}),
        NOW.date(),
    )
    articles = stored_articles(engine)
    assert set(articles) == {11, 12, 13, 14}, (
        "precondition: the old article is stored too"
    )
    table = {
        "Rocket Lab wins a launch contract": {
            "positive": 0.8,
            "negative": 0.1,
            "neutral": 0.1,
        },
        "Rocket Lab delays a launch": {
            "positive": 0.05,
            "negative": 0.9,
            "neutral": 0.05,
        },
    }
    fake = Fake(table)
    result = score_now(engine, fake)
    assert result.succeeded, result.error_code
    mod = scorer_module()
    stored = scores(engine)
    assert set(stored) == {11, 12, 13}  # 14 is outside the window: never read
    assert {f: r.score for f, r in stored.items()} == {
        11: Decimal("0.7000"),
        12: Decimal("-0.8500"),
        13: Decimal("0.5000"),
    }
    assert {f: (r.label, r.confidence) for f, r in stored.items()} == {
        11: ("positive", Decimal("0.8000")),
        12: ("negative", Decimal("0.9000")),
        13: ("positive", Decimal("0.6000")),
    }
    assert {r.model for r in stored.values()} == {mod.MODEL_VERSION}
    assert {r.batch_id for r in stored.values()} == {result.batch_id}
    # Deterministic: the scorer reads the texts, and rows are numbered, by article id.
    by_id = sorted(stored, key=lambda f: articles[f].id)
    assert fake.texts == [mod.article_text(articles[f]._mapping) for f in by_id]
    # The importer numbers rows in its canonical (sorted-JSON) order, not by id.
    assert sorted(stored[f].row_number for f in by_id) == [1, 2, 3]
    news_batch = latest_news_batch(engine)
    assert dependencies(engine, result.run_id) == {"news_articles": news_batch}
    assert envelope(engine, result.batch_id) == {
        "model": mod.MODEL_VERSION,
        "input_batch_id": news_batch,
        "articles_scored": 3,
    }
    assert run_of(engine, result)["collection_mode"] == "derived"


@pytest.mark.db
def test_the_scorer_is_called_in_bounded_chunks(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
):
    run_news(engine, NewsClient(IN_WINDOW), NOW.date())
    monkeypatch.setattr(scorer_module(), "BATCH_SIZE", 2)
    fake = Fake()
    assert score_now(engine, fake).succeeded
    assert [len(c) for c in fake.calls] == [2, 1]


@pytest.mark.db
def test_a_failed_chunk_is_skipped_the_rest_is_stored_and_a_rerun_catches_up(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
):
    run_news(engine, NewsClient(IN_WINDOW), NOW.date())
    monkeypatch.setattr(scorer_module(), "BATCH_SIZE", 1)
    articles = stored_articles(engine)
    by_id = sorted(articles, key=lambda f: articles[f].id)
    with captured(monkeypatch) as handler:
        result = score_now(engine, Fake(fail_on_call=2))
    assert result.succeeded, result.error_code
    assert set(scores(engine)) == {by_id[0], by_id[2]}  # chunk 2 skipped, 3 scored
    run = run_of(engine, result)
    assert run["coverage"] == {"complete": False, "scope": "derivation"}
    assert run["diagnostics"]["failures"] == [{"unit": "BATCH-2", "code": "failed"}]
    assert (run["diagnostics"]["attempted"], run["diagnostics"]["failed"]) == (3, 1)
    for headline in HEADLINES:
        assert headline not in " ".join([*handler.messages, str(run["diagnostics"])])
    again = Fake()
    assert score_now(engine, again).succeeded
    assert again.texts == [articles[by_id[1]].headline]
    assert set(scores(engine)) == {11, 12, 13}


@pytest.mark.db
def test_a_failing_article_is_retried_up_to_three_attempts_then_left_unscored(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
):
    run_news(engine, NewsClient(IN_WINDOW), NOW.date())
    monkeypatch.setattr(scorer_module(), "BATCH_SIZE", 1)
    articles = stored_articles(engine)
    by_id = sorted(articles, key=lambda f: articles[f].id)
    bad = articles[by_id[1]].headline

    def failing_on_bad(texts):
        if texts == [bad]:
            raise RuntimeError("synthetic model failure")
        return Fake()(texts)

    assert score_now(engine, failing_on_bad).succeeded
    assert attempts(engine) == {by_id[1]: (1, "failed")}
    for n in (2, 3):
        result = score_now(engine, failing_on_bad)
        assert (result.status, result.error_code) == ("failed", "scoring_failed")
        assert attempts(engine) == {by_id[1]: (n, "failed")}
    fake = Fake()
    assert score_now(engine, fake) is None  # exhausted: nothing left to read
    assert fake.texts == [] and by_id[1] not in scores(engine)  # stays unscored


@pytest.mark.db
def test_scoring_an_article_forgets_its_failed_attempts(
    engine,
    seeded,  # noqa: F811
):
    run_news(engine, NewsClient({"RKLB": IN_WINDOW["RKLB"]}), NOW.date())
    assert not score_now(engine, Fake(fail_on_call=1)).succeeded
    assert attempts(engine) == {11: (1, "failed"), 12: (1, "failed")}
    assert score_now(engine, Fake()).succeeded
    assert attempts(engine) == {} and set(scores(engine)) == {11, 12}


@pytest.mark.db
def test_a_model_that_fails_to_load_counts_against_no_article(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
):
    run_news(engine, NewsClient(IN_WINDOW), NOW.date())
    monkeypatch.setitem(sys.modules, "torch", None)
    monkeypatch.setitem(sys.modules, "transformers", None)
    result = scorer_module().score_articles(engine, now=NOW)
    assert (result.status, result.error_code) == ("failed", "scoring_failed")
    assert attempts(engine) == {}


class Slow:
    """A scorer whose given call outlasts the batch timeout."""

    def __init__(self, slow_call):
        self.fake, self.slow_call = Fake(), slow_call

    def __call__(self, texts):
        if len(self.fake.calls) + 1 == self.slow_call:
            self.fake.calls.append(list(texts))
            time.sleep(1.0)
            return []
        return self.fake(texts)


@pytest.mark.db
def test_a_chunk_that_times_out_stops_scoring_and_keeps_what_was_scored(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
):
    run_news(engine, NewsClient(IN_WINDOW), NOW.date())
    monkeypatch.setattr(scorer_module(), "BATCH_SIZE", 1)
    monkeypatch.setattr(scorer_module(), "BATCH_TIMEOUT", 0.2)
    articles = stored_articles(engine)
    by_id = sorted(articles, key=lambda f: articles[f].id)
    slow = Slow(slow_call=2)
    result = score_now(engine, slow)
    assert result.succeeded, result.error_code
    assert len(slow.fake.calls) == 2  # chunk 3 never attempted
    assert set(scores(engine)) == {by_id[0]}
    diagnostics = run_of(engine, result)["diagnostics"]
    assert diagnostics["failures"] == [{"unit": "BATCH-2", "code": "timeout"}]
    assert diagnostics["not_attempted"] == 1
    assert attempts(engine) == {by_id[1]: (1, "timeout")}  # chunk 3 isn't counted
    monkeypatch.setattr(scorer_module(), "BATCH_TIMEOUT", 120.0)
    assert score_now(engine, Fake()).succeeded
    assert set(scores(engine)) == {11, 12, 13}


@pytest.mark.db
def test_a_rerun_scores_only_what_is_new_and_writes_nothing_when_nothing_is(
    engine,
    seeded,  # noqa: F811
):
    run_news(engine, NewsClient({"RKLB": IN_WINDOW["RKLB"]}), NOW.date())
    first = score_now(engine, Fake())
    assert first.succeeded
    before = (batches(engine), scores(engine))
    again = Fake()
    result = score_now(engine, again)
    assert again.texts == []  # nothing re-read or re-scored
    assert (batches(engine), scores(engine)) == before  # no new batch, no new rows
    assert not (getattr(result, "status", None) == "failed")
    run_news(engine, NewsClient({"NVDA": IN_WINDOW["NVDA"]}), NOW.date())
    third = Fake()
    result = score_now(engine, third)
    assert result.succeeded
    assert third.texts == ["NVDA rises"]
    assert set(scores(engine)) == {11, 12, 13}
    assert envelope(engine, result.batch_id)["articles_scored"] == 1


@pytest.mark.db
def test_a_new_model_version_scores_the_window_again(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
):
    run_news(engine, NewsClient(IN_WINDOW), NOW.date())
    assert score_now(engine, Fake()).succeeded
    monkeypatch.setattr(
        scorer_module(), "MODEL_VERSION", "ProsusAI/finbert@" + "0" * 40
    )
    fake = Fake()
    result = score_now(engine, fake)
    assert result.succeeded and len(fake.texts) == 3
    with engine.connect() as conn:
        models_per_article = conn.execute(
            select(models.article_sentiment.c.news_article_id, func.count()).group_by(
                models.article_sentiment.c.news_article_id
            )
        ).all()
    assert {n for _, n in models_per_article} == {2}


@pytest.mark.db
def test_a_failed_run_is_caught_up_by_the_next(
    engine,
    seeded,  # noqa: F811
):
    run_news(engine, NewsClient({"RKLB": IN_WINDOW["RKLB"]}), NOW.date())
    assert not score_now(engine, Fake(fail_on_call=1)).succeeded
    run_news(engine, NewsClient({"NVDA": IN_WINDOW["NVDA"]}), NOW.date())
    fake = Fake()
    result = score_now(engine, fake)
    assert result.succeeded
    assert sorted(fake.texts) == [
        "NVDA rises",
        "Rocket Lab delays a launch",
        "Rocket Lab wins a launch contract",
    ]
    assert set(scores(engine)) == {11, 12, 13}
    assert dependencies(engine, result.run_id) == {
        "news_articles": latest_news_batch(engine)
    }


@pytest.mark.db
def test_without_a_news_batch_the_run_fails_missing_upstream(
    engine,
    seeded,  # noqa: F811
):
    fake = Fake()
    result = score_now(engine, fake)
    assert (result.status, result.error_code) == ("failed", "missing_upstream")
    assert fake.calls == [] and batches(engine) == 0


# ── failing closed ───────────────────────────────────────


HEADLINES = [a.headline for articles in IN_WINDOW.values() for a in articles]


class Handler(logging.Handler):
    """Every record logged under "src" while a run fails (fileConfig can disable it)."""

    def __init__(self):
        super().__init__(logging.DEBUG)
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


@contextmanager
def captured(monkeypatch):
    handler = Handler()
    log = logging.getLogger("src")
    monkeypatch.setattr(log, "disabled", False)
    previous = log.level
    # setLevel, not the attribute: it clears every logger's isEnabledFor cache, which
    # otherwise keeps an INFO line from an earlier run in this process switched off.
    log.setLevel(logging.DEBUG)
    log.addHandler(handler)
    try:
        yield handler
    finally:
        log.removeHandler(handler)
        log.setLevel(previous)


def assert_failed_closed(engine, result, handler):
    from src.ingestion.diagnostics import ERROR_SUMMARIES

    assert (result.status, result.error_code) == ("failed", "scoring_failed")
    assert "scoring_failed" in ERROR_SUMMARIES
    assert batches(engine) == 0 and scores(engine) == {}  # no partial batch
    run = run_of(engine, result)
    leaked = " ".join(
        [*handler.messages, str(run["error_summary"]), str(run["diagnostics"])]
    )
    for headline in HEADLINES:
        assert headline not in leaked, headline


class Wrong:
    def __init__(self, answer):
        self.answer = answer

    def __call__(self, texts):
        return self.answer(texts)


# Every chunk fails, so nothing is scored and the run fails closed.
FAILURES = {
    "raises on every chunk": lambda: Wrong(lambda texts: 1 / 0),
    "too few answers": lambda: Wrong(
        lambda texts: [
            {"positive": 1.0, "negative": 0.0, "neutral": 0.0} for _ in texts[1:]
        ]
    ),
    "a missing label": lambda: Wrong(
        lambda texts: [{"positive": 0.5, "neutral": 0.5} for _ in texts]
    ),
    "a probability above 1": lambda: Wrong(
        lambda texts: [
            {"positive": 1.5, "negative": 0.0, "neutral": 0.0} for _ in texts
        ]
    ),
    "not a number": lambda: Wrong(
        lambda texts: [
            {"positive": float("nan"), "negative": 0.0, "neutral": 0.0} for _ in texts
        ]
    ),
}


@pytest.mark.db
@pytest.mark.parametrize("failure", sorted(FAILURES))
def test_a_scorer_failure_fails_the_run_closed(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
    failure,
):
    run_news(engine, NewsClient(IN_WINDOW), NOW.date())
    monkeypatch.setattr(scorer_module(), "BATCH_SIZE", 2)
    with captured(monkeypatch) as handler:
        result = score_now(engine, FAILURES[failure]())
    assert_failed_closed(engine, result, handler)


class LoadFailure(Exception):
    pass


def fake_transformers(calls):
    """A transformers stand-in: every loader records its arguments, then fails."""

    def loader(kind):
        def load(*args, **kwargs):
            calls.append((kind, args, kwargs))
            raise LoadFailure("synthetic: no model here")

        return load

    module = types.ModuleType("transformers")
    for name in (
        "AutoTokenizer",
        "AutoModelForSequenceClassification",
        "BertTokenizer",
        "BertTokenizerFast",
        "BertForSequenceClassification",
    ):
        setattr(module, name, types.SimpleNamespace(from_pretrained=loader(name)))
    module.pipeline = loader("pipeline")
    return module


@pytest.mark.db
def test_the_real_model_loads_at_the_pinned_revision_and_a_load_failure_fails_closed(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
):
    run_news(engine, NewsClient(IN_WINDOW), NOW.date())
    calls = []
    monkeypatch.setitem(sys.modules, "transformers", fake_transformers(calls))
    monkeypatch.setitem(sys.modules, "torch", types.ModuleType("torch"))
    with captured(monkeypatch) as handler:
        result = scorer_module().score_articles(engine, now=NOW)  # scorer=None: FinBERT
    mod = scorer_module()
    assert calls, "the default scorer loads the model through transformers"
    for kind, args, kwargs in calls:
        named = kwargs.get("model", args[0] if args else None)
        assert named == mod.MODEL_NAME, (kind, args, kwargs)
        assert kwargs.get("revision") == mod.MODEL_REVISION, (kind, kwargs)
    assert_failed_closed(engine, result, handler)


@pytest.mark.db
def test_without_torch_installed_the_run_fails_closed(
    engine,
    seeded,  # noqa: F811
    monkeypatch,
):
    run_news(engine, NewsClient(IN_WINDOW), NOW.date())
    monkeypatch.setitem(sys.modules, "torch", None)  # import torch raises ImportError
    monkeypatch.setitem(sys.modules, "transformers", None)
    with captured(monkeypatch) as handler:
        result = scorer_module().score_articles(engine, now=NOW)
    assert_failed_closed(engine, result, handler)


# ── end to end and the wiring ────────────────────────────


@pytest.mark.db
def test_derive_sentiment_names_the_scorers_model_version(
    engine,
    seeded,  # noqa: F811
):
    run_news(engine, NewsClient(IN_WINDOW), NOW.date())
    assert score_now(engine, Fake()).succeeded
    assert credibility_job.derive_sentiment(engine, now=NOW).succeeded
    with engine.connect() as conn:
        versions = conn.execute(
            select(
                models.securities.c.symbol, models.ticker_sentiment.c.model_version
            ).join(models.securities)
        ).all()
    assert dict(versions)["RKLB"] == scorer_module().MODEL_VERSION


def test_run_once_plugs_the_scorer_into_the_slot(monkeypatch):
    from pydantic import SecretStr

    seen = {}

    def run_sentiment(engine, news_client, resolver, **kwargs):
        seen.update(kwargs)
        return types.SimpleNamespace(succeeded=True)

    settings = types.SimpleNamespace(
        require_provider_keys=lambda *names: None,
        database_url=SecretStr("postgresql+psycopg://synthetic@127.0.0.1:1/none"),
        finnhub_api_key=SecretStr("synthetic"),
    )
    monkeypatch.setattr(credibility_job, "run_sentiment", run_sentiment)
    assert credibility_job.run_once(settings) == 0
    plugged = seen.get("score_articles")
    assert plugged is not None
    assert getattr(plugged, "func", plugged) is scorer_module().score_articles


# ── dependencies: CI installs no model stack ─────────────


def test_ci_installs_neither_torch_nor_transformers():
    lock = (BACKEND.parents[1] / "requirements-dev.lock").read_text().lower()
    for package in ("torch==", "transformers==", "tokenizers==", "safetensors=="):
        assert package not in lock, package
