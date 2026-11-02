"""Sprint 2 TEST-2: the score jobs (JOB-2, JOB-3, JOB-5, JOB-6). Synthetic rows only.

Spec: output/sprint-2/s2-stories-plan.md §3.1-§3.3, §5 (b) and §7; S2-01 AC1-AC4,
S2-03 AC4/AC5. Tables as migration 0018 (test_scores_schema.py, P1-P8);
scoring and explain as JOB-1 and JOB-4 (test_scores_scoring.py, test_scores_explain.py).

Pinned interface, src/services/scores/job.py:

* derive_scores(engine) -> a result with `.succeeded` and `.error_code`. A
  derived company_scores run: the latest successful fundamental_scores and
  ticker_sentiment runs (named in run_dependencies) and the current production
  watchlist publication. One row per security in the fundamental_scores batch,
  then current_scores['production'] points at the batch. Unchanged inputs leave
  the pointer (and its promoted_at, the API's as_of) where it was.
* read_ticker_sentiment(conn, batch_id) -> {security_id: row}, the contract
  reader, empty rows included. Decision 108: ticker_sentiment is #85's table
  plus model_version. Sentiment is missing for a security whose row has a NULL
  value (nothing counted) or that has no row; no ticker_sentiment batch at all
  makes it missing for everyone. The component's as-of is last_at's UTC date
  and its version is weights_version.
* derive_explanations(engine): a derived score_explanations run on the
  current scores batch (named in run_dependencies), one row per score row, with
  company_score_id. Re-explaining the same scores row returns its stored
  explanation unchanged (03-AC4); a new scores batch regenerates (decision D1).
  The explanation's window is the cited ticker_sentiment batch envelope's
  window_days, passed to explain() as window_days.
* run_once(settings) for `--job scores`: derive_scores, then derive_explanations.

recommended = status ok, score >= 60, and the security is on the current
production publication's consensus list in any bucket: popular_stable,
affordable_growing or unresolved (plan §8, decided 2026-10-31). Every row names
the publication it was judged against.

Explanations follow explain()'s claim rule, decision 103 (c) (#93): only
supporting signals are claims, a fundamentals measure worth at least 50 points
and a sentiment that leans positive, which keeps the last place.
"""

from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from test_read_api import seed_security

pytestmark = pytest.mark.db
D = Decimal


def job():
    from src.services.scores import job as module

    return module


def scoring():
    from src.services.scores import scoring as module

    return module


def explain_module():
    from src.services.scores import explain

    return explain


# ── seeding through the ingestion contract ───────────────


def _import(engine, source, records, envelope=None):
    from src.ingestion.importer import CollectionOutcome, import_collection

    if envelope is None and source == "ticker_sentiment":
        envelope = sentiment_envelope()
    result = import_collection(
        engine,
        CollectionOutcome.success(
            source,
            "derived",
            records,
            coverage={"complete": True, "scope": "derivation"},
            envelope=envelope,
        ),
    )
    assert result.succeeded, (source, result.error_code, result.error_summary)
    return result.batch_id


def fundamentals_record(ticker, score, *, growth="0.18", margin="0.42", trend=None):
    if score is None:
        return {
            "ticker": ticker,
            "score": None,
            "missing": ["not_enough_data"],
            "piotroski_signals": 0,
            "score_version": "fundamentals-v1",
        }
    return {
        "ticker": ticker,
        "fiscal_year_end": "2025-06-30",
        "model": "general",
        "score": score,
        "missing": [],
        "revenue_growth_3y": growth,
        "operating_margin": margin,
        "operating_margin_trend": trend,
        "profit_source": "operating_income",
        "piotroski_signals": 9,
        "piotroski_score": 7,
        "score_version": "fundamentals-v1",
    }


def sentiment_record(
    ticker, value, count=12, *, social=0, last_at="2026-11-04T16:30:00+00:00"
):
    """A scored #85 row: `count` news articles and `social` posts."""
    return {
        "ticker": ticker,
        "value": value,
        "plain_average": value,
        "article_count": count + social,
        "news_count": count,
        "social_count": social,
        "first_at": "2026-10-29T14:05:00+00:00",
        "last_at": last_at,
        "weights_version": "weights-v1",
        "model_version": "ProsusAI/finbert@synthetic0001" if count else None,
    }


def empty_sentiment_record(ticker):
    """Nothing counted for the security in the window: a NULL value."""
    return {
        "ticker": ticker,
        "value": None,
        "plain_average": None,
        "article_count": 0,
        "news_count": 0,
        "social_count": 0,
        "first_at": None,
        "last_at": None,
        "weights_version": "weights-v1",
        "model_version": None,
    }


def sentiment_envelope(window_days=7):
    """#85's ticker_sentiment envelope keys."""
    return {
        "weights_version": "weights-v1",
        "window_days": window_days,
        "universe": 5,
        "scored": 4,
        "no_data_count": 1,
        "news_used": 27,
        "social_used": 6,
        "social_batch_fresh": True,
        "publishers_unknown": 0,
    }


def publish(engine, popular=(), affordable=(), unresolved=()):
    """A production watchlist publication listing these tickers; returns its id."""
    import json

    output = {
        "popular_stable": [{"ticker": t} for t in popular],
        "affordable_growing": [{"ticker": t} for t in affordable],
        "unresolved": [{"ticker": t} for t in unresolved],
    }
    with engine.begin() as conn:
        pub = conn.execute(
            text(
                "INSERT INTO perennial.publications (created_at, ranking_version, ranking_config, output)"
                " VALUES (now(), 'watchlist-v5', '{}', CAST(:o AS jsonb)) RETURNING id"
            ),
            {"o": json.dumps(output)},
        ).scalar_one()
        conn.execute(
            text(
                "INSERT INTO perennial.current_publication (channel, publication_id, promoted_at)"
                " VALUES ('production', :p, now()) ON CONFLICT (channel)"
                " DO UPDATE SET publication_id = :p, promoted_at = now()"
            ),
            {"p": pub},
        )
    return pub


TICKERS = ("ZZJA", "ZZJB", "ZZJC", "ZZJD", "ZZJE")


@pytest.fixture
def world(engine):
    """A: 78 and +0.31, listed. B: 50 and -0.5, listed (42.6). C: 78, an empty
    sentiment row (nothing counted). D: no fundamentals score, has sentiment.
    E: 90 and +0.9 from social posts only, not listed."""
    for t in TICKERS:
        seed_security(engine, t)
    fundamentals = _import(
        engine,
        "fundamental_scores",
        [
            fundamentals_record("ZZJA", "78.0"),
            fundamentals_record("ZZJB", "50.0", growth="0.05", margin="-0.10"),
            fundamentals_record("ZZJC", "78.0"),
            fundamentals_record("ZZJD", None),
            fundamentals_record("ZZJE", "90.0", growth="0.30", trend="0.05"),
        ],
    )
    sentiment = _import(
        engine,
        "ticker_sentiment",
        [
            sentiment_record("ZZJA", "0.3100"),
            sentiment_record("ZZJB", "-0.5000", 3),
            empty_sentiment_record("ZZJC"),
            sentiment_record("ZZJD", "0.2000"),
            sentiment_record("ZZJE", "0.9000", 0, social=6),
        ],
    )
    publication = publish(engine, popular=("ZZJA", "ZZJB"), affordable=("ZZJC", "ZZJD"))
    return SimpleNamespace(
        fundamentals=fundamentals, sentiment=sentiment, publication=publication
    )


# ── reading back ─────────────────────────────────────────


def pointer(engine):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT batch_id, promoted_at FROM perennial.current_scores WHERE channel = 'production'"
            )
        ).one_or_none()


def rows(engine, table, batch_id):
    with engine.connect() as conn:
        found = conn.execute(
            text(
                f"SELECT s.symbol, t.* FROM perennial.{table} t JOIN perennial.securities s"
                " ON s.id = t.security_id WHERE t.batch_id = :b ORDER BY s.symbol"
            ),
            {"b": batch_id},
        ).mappings()
        return {r["symbol"]: dict(r) for r in found}


def batches(engine, source):
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT count(*) FROM perennial.source_batches WHERE source = :s"),
            {"s": source},
        ).scalar_one()


def dependencies(engine, batch_id):
    """run_dependencies of the latest successful run that wrote this batch."""
    with engine.connect() as conn:
        run_id = conn.execute(
            text(
                "SELECT id FROM perennial.ingestion_runs WHERE batch_id = :b AND status = 'succeeded'"
                " ORDER BY id DESC LIMIT 1"
            ),
            {"b": batch_id},
        ).scalar_one()
        return dict(
            conn.execute(
                text(
                    "SELECT source, batch_id FROM perennial.run_dependencies WHERE run_id = :r"
                ),
                {"r": run_id},
            )
            .tuples()
            .all()
        )


def source_rows(engine, table, batch_id):
    return {symbol: row for symbol, row in rows(engine, table, batch_id).items()}


def expected_score_row(fund, sent, publication, member):
    """What derive_scores must store for one security (fund/sent: stored rows or None)."""
    s = scoring()
    values = {}
    if fund is not None and fund["score"] is not None:
        values["fundamentals"] = fund["score"]
    if sent is not None and sent["value"] is not None:
        values["sentiment"] = s.normalize_sentiment(sent["value"])
    result = s.blend(values)
    row = {
        "status": result.status,
        "unavailable_reason": result.unavailable_reason,
        "score": result.score,
        "score_version": "score-v1",
        "missing": list(result.missing),
        "recommended": bool(result.status == "ok" and result.score >= 60 and member),
        "watchlist_publication_id": publication,
    }
    cited = {
        "fundamentals": (
            fund,
            lambda r: r["fiscal_year_end"],
            lambda r: r["score_version"],
        ),
        "sentiment": (  # decision 108
            sent,
            lambda r: r["last_at"].astimezone(timezone.utc).date(),
            lambda r: r["weights_version"],
        ),
    }
    for c in result.components:
        row[f"{c.name}_value"] = c.value
        row[f"{c.name}_nominal_weight"] = c.nominal_weight
        row[f"{c.name}_weight"] = c.weight
        row[f"{c.name}_contribution"] = c.contribution
        record, as_of, version = cited.get(c.name, (None, None, None))
        present = c.value is not None and record is not None
        row[f"{c.name}_input_as_of"] = as_of(record) if present else None
        row[f"{c.name}_input_version"] = version(record) if present else None
        row[f"{c.name}_record_id"] = record["id"] if present else None
    return row


def same(stored, expected):
    return {k: stored[k] for k in expected} == expected


def scores_ok(engine):
    result = job().derive_scores(engine)
    assert result.succeeded, getattr(result, "error_code", None)
    return pointer(engine)


# ── JOB-2: derive_scores ─────────────────────────────────


def test_derive_scores_stores_one_row_per_security_and_promotes_the_batch(
    engine, world
):
    current = scores_ok(engine)
    assert current is not None
    stored = rows(engine, "company_scores", current.batch_id)
    assert set(stored) == set(TICKERS)
    fund = source_rows(engine, "fundamental_scores", world.fundamentals)
    sent = source_rows(engine, "ticker_sentiment", world.sentiment)
    members = {"ZZJA", "ZZJB", "ZZJC", "ZZJD"}
    for t in TICKERS:
        expected = expected_score_row(
            fund[t], sent.get(t), world.publication, t in members
        )
        assert same(stored[t], expected), (
            t,
            {k: (stored[t][k], v) for k, v in expected.items() if stored[t][k] != v},
        )


def test_the_stored_outcomes_per_security(engine, world):
    stored = rows(engine, "company_scores", scores_ok(engine).batch_id)
    summary = {
        t: (r["status"], r["unavailable_reason"], r["score"], r["recommended"])
        for t, r in stored.items()
    }
    assert summary == {
        "ZZJA": ("ok", None, D("74.3"), True),  # §3.4's example
        "ZZJB": ("ok", None, D("42.6"), False),  # below 60
        "ZZJC": ("unavailable", "only_fundamentals", None, False),  # 01-AC4
        "ZZJD": ("unavailable", "no_fundamentals", None, False),
        "ZZJE": (
            "ok",
            None,
            D("91.5"),
            False,
        ),  # 63.53 + 27.94: high, but not on the consensus list
    }
    assert list(stored["ZZJA"]["missing"]) == ["event_exposure"]  # 01-AC3
    assert (
        stored["ZZJA"]["fundamentals_weight"],
        stored["ZZJA"]["sentiment_weight"],
    ) == (
        D("0.7059"),
        D("0.2941"),
    )


def test_recommended_starts_at_exactly_60(engine):
    """F: 60.0 and +0.2 -> 42.35 + 17.65 = 60.0. G: 60.0 and 0 -> 42.35 + 14.70 = 57.0 (half-even)."""
    for t in ("ZZJF", "ZZJG"):
        seed_security(engine, t)
    _import(
        engine,
        "fundamental_scores",
        [fundamentals_record("ZZJF", "60.0"), fundamentals_record("ZZJG", "60.0")],
    )
    _import(
        engine,
        "ticker_sentiment",
        [sentiment_record("ZZJF", "0.2000"), sentiment_record("ZZJG", "0.0000")],
    )
    publish(engine, popular=("ZZJF", "ZZJG"))
    stored = rows(engine, "company_scores", scores_ok(engine).batch_id)
    assert {t: (r["score"], r["recommended"]) for t, r in stored.items()} == {
        "ZZJF": (D("60.0"), True),
        "ZZJG": (D("57.0"), False),
    }


def test_the_sentiment_as_of_is_last_at_and_its_version_the_weights(engine, world):
    stored = rows(engine, "company_scores", scores_ok(engine).batch_id)
    assert stored["ZZJA"]["sentiment_input_as_of"] == date(2026, 11, 4)
    assert stored["ZZJA"]["fundamentals_input_as_of"] == date(2025, 6, 30)
    assert stored["ZZJA"]["sentiment_input_version"] == "weights-v1"
    assert stored["ZZJE"]["sentiment_input_version"] == "weights-v1"  # social only


def test_the_sentiment_as_of_is_last_ats_utc_date_whatever_the_session_zone(engine):
    """03:30 UTC on Nov 5 is still Nov 4 in New York: the as-of is the UTC date."""
    from sqlalchemy import create_engine

    seed_security(engine, "ZZJM")
    _import(engine, "fundamental_scores", [fundamentals_record("ZZJM", "78.0")])
    late = "2026-11-05T03:30:00+00:00"
    _import(
        engine, "ticker_sentiment", [sentiment_record("ZZJM", "0.3100", last_at=late)]
    )
    publish(engine, popular=("ZZJM",))
    new_york = create_engine(
        engine.url, connect_args={"options": "-c timezone=America/New_York"}
    )
    try:
        result = job().derive_scores(new_york)
    finally:
        new_york.dispose()
    assert result.succeeded, getattr(result, "error_code", None)
    stored = rows(engine, "company_scores", pointer(engine).batch_id)
    assert stored["ZZJM"]["sentiment_input_as_of"] == date(2026, 11, 5)


def test_an_empty_row_and_no_row_are_both_missing_sentiment(engine):
    """Decision 108: a NULL value (nothing counted) and no row at all both mean missing."""
    for t in ("ZZJA", "ZZJC", "ZZJH"):
        seed_security(engine, t)
    _import(
        engine,
        "fundamental_scores",
        [fundamentals_record(t, "78.0") for t in ("ZZJA", "ZZJC", "ZZJH")],
    )
    _import(
        engine,
        "ticker_sentiment",
        [sentiment_record("ZZJA", "0.3100"), empty_sentiment_record("ZZJC")],
    )
    publish(engine, popular=("ZZJA", "ZZJC", "ZZJH"))
    stored = rows(engine, "company_scores", scores_ok(engine).batch_id)
    assert stored["ZZJA"]["status"] == "ok"
    for t in ("ZZJC", "ZZJH"):
        row = stored[t]
        assert (row["status"], row["unavailable_reason"]) == (
            "unavailable",
            "only_fundamentals",
        ), t
        assert list(row["missing"]) == ["sentiment", "event_exposure"], t
        assert (
            row["sentiment_value"],
            row["sentiment_input_as_of"],
            row["sentiment_input_version"],
            row["sentiment_record_id"],
        ) == (None, None, None, None), t


def test_every_ok_row_reproduces_through_breakdown(engine, world):
    stored = rows(engine, "company_scores", scores_ok(engine).batch_id)
    for t, row in stored.items():
        components = scoring().breakdown(
            row
        )  # raises if the row disagrees with score-v1
        assert (len(components) == 3) == (row["status"] == "ok"), t


def test_lineage_names_both_input_batches_and_the_envelope_the_weights(engine, world):
    current = scores_ok(engine)
    assert dependencies(engine, current.batch_id) == {
        "fundamental_scores": world.fundamentals,
        "ticker_sentiment": world.sentiment,
    }
    with engine.connect() as conn:
        envelope = conn.execute(
            text(
                "SELECT payload -> 'envelope' FROM perennial.source_batches WHERE id = :b"
            ),
            {"b": current.batch_id},
        ).scalar_one()
    weights = scoring().ScoreWeights().envelope()
    assert envelope["score_version"] == weights["score_version"]
    assert envelope["weights"] == weights["weights"]


def test_unchanged_inputs_leave_the_pointer_and_the_rows_alone(engine, world):
    """01-AC2: the same stored inputs give the identical stored score, and no new batch."""
    first = scores_ok(engine)
    before_rows = rows(engine, "company_scores", first.batch_id)
    before_batches = batches(engine, "company_scores")
    again = scores_ok(engine)
    assert (again.batch_id, again.promoted_at) == (first.batch_id, first.promoted_at)
    assert batches(engine, "company_scores") == before_batches
    assert rows(engine, "company_scores", again.batch_id) == before_rows


def test_a_new_fundamentals_batch_moves_the_pointer(engine, world):
    first = scores_ok(engine)
    newer = _import(
        engine,
        "fundamental_scores",
        [
            fundamentals_record("ZZJA", "80.0"),
            fundamentals_record("ZZJB", "50.0", growth="0.05", margin="-0.10"),
            fundamentals_record("ZZJC", "78.0"),
            fundamentals_record("ZZJD", None),
            fundamentals_record("ZZJE", "90.0", growth="0.30", trend="0.05"),
        ],
    )
    second = scores_ok(engine)
    assert second.batch_id != first.batch_id
    assert dependencies(engine, second.batch_id)["fundamental_scores"] == newer
    stored = rows(engine, "company_scores", second.batch_id)
    assert stored["ZZJA"]["fundamentals_value"] == D("80.0")
    assert stored["ZZJA"]["score"] == D("75.7")  # 56.47 + 19.26 = 75.73
    assert rows(engine, "company_scores", first.batch_id)["ZZJA"]["score"] == D(
        "74.3"
    )  # immutable


def test_a_new_publication_rejudges_recommended(engine, world):
    first = scores_ok(engine)
    newer = publish(engine, popular=("ZZJA", "ZZJE"))
    second = scores_ok(engine)
    assert second.batch_id != first.batch_id
    stored = rows(engine, "company_scores", second.batch_id)
    assert {t: r["recommended"] for t, r in stored.items()} == {
        "ZZJA": True,
        "ZZJB": False,
        "ZZJC": False,
        "ZZJD": False,
        "ZZJE": True,
    }
    assert {r["watchlist_publication_id"] for r in stored.values()} == {newer}


@pytest.mark.parametrize("bucket", ["popular", "affordable", "unresolved"])
def test_every_bucket_of_the_consensus_list_counts_as_listed(engine, world, bucket):
    """Plan §8: "on the current consensus list" is every bucket, unresolved
    included. Listing alone isn't enough: ok and >= 60 still apply."""
    newer = publish(engine, **{bucket: ("ZZJB", "ZZJC", "ZZJE")})
    current = scores_ok(engine)
    stored = rows(engine, "company_scores", current.batch_id)
    assert {t: (r["score"], r["recommended"]) for t, r in stored.items()} == {
        "ZZJA": (D("74.3"), False),  # no longer listed
        "ZZJB": (D("42.6"), False),  # listed, below 60
        "ZZJC": (None, False),  # listed, unavailable
        "ZZJD": (None, False),
        "ZZJE": (D("91.5"), True),  # listed, ok and >= 60
    }
    assert {r["watchlist_publication_id"] for r in stored.values()} == {newer}
    explanations_ok(engine)
    _, explained = latest_explanations(engine)
    assert explained["ZZJE"]["status"] == "ok"  # recommended, so explained
    assert explained["ZZJA"]["status"] == "not_recommended"


def test_without_any_sentiment_batch_sentiment_is_missing_for_everyone(engine):
    for t in ("ZZJA", "ZZJD"):
        seed_security(engine, t)
    fundamentals = _import(
        engine,
        "fundamental_scores",
        [fundamentals_record("ZZJA", "78.0"), fundamentals_record("ZZJD", None)],
    )
    publish(engine, popular=("ZZJA",))
    current = scores_ok(engine)
    stored = rows(engine, "company_scores", current.batch_id)
    assert (stored["ZZJA"]["status"], stored["ZZJA"]["unavailable_reason"]) == (
        "unavailable",
        "only_fundamentals",
    )
    assert list(stored["ZZJA"]["missing"]) == ["sentiment", "event_exposure"]
    assert (stored["ZZJD"]["status"], stored["ZZJD"]["unavailable_reason"]) == (
        "unavailable",
        "no_fundamentals",
    )
    assert dependencies(engine, current.batch_id) == {
        "fundamental_scores": fundamentals
    }


def test_a_failed_sentiment_run_keeps_the_last_good_batch(engine, world):
    from src.ingestion.importer import CollectionOutcome, import_collection

    failed = import_collection(
        engine,
        CollectionOutcome.failure("ticker_sentiment", "derived", "missing_upstream"),
    )
    assert not failed.succeeded
    current = scores_ok(engine)
    assert dependencies(engine, current.batch_id)["ticker_sentiment"] == world.sentiment


def test_without_a_fundamentals_score_run_the_job_fails_missing_upstream(engine):
    seed_security(engine, "ZZJA")
    _import(engine, "ticker_sentiment", [sentiment_record("ZZJA", "0.3100")])
    publish(engine, popular=("ZZJA",))
    result = job().derive_scores(engine)
    assert (result.succeeded, result.error_code) == (False, "missing_upstream")
    assert pointer(engine) is None
    assert batches(engine, "company_scores") == 0


def test_a_held_source_lock_writes_nothing(engine, world):
    from src.pipeline.lifecycle import SOURCE_LOCK_KEYS

    derive_scores = job().derive_scores
    with engine.connect() as holder:
        holder.execute(
            text("SELECT pg_advisory_lock(:k)"),
            {"k": SOURCE_LOCK_KEYS["company_scores"]},
        )
        holder.commit()
        try:
            try:
                derive_scores(engine)
            except Exception:
                pass
            assert batches(engine, "company_scores") == 0 and pointer(engine) is None
        finally:
            holder.execute(
                text("SELECT pg_advisory_unlock(:k)"),
                {"k": SOURCE_LOCK_KEYS["company_scores"]},
            )
            holder.commit()


# ── JOB-3: the sentiment contract reader ─────────────────


def test_read_ticker_sentiment_keys_rows_by_security(engine, world):
    with engine.connect() as conn:
        found = job().read_ticker_sentiment(conn, world.sentiment)
        ids = dict(
            conn.execute(text("SELECT symbol, id FROM perennial.securities"))
            .tuples()
            .all()
        )
    assert set(found) == {ids[t] for t in TICKERS}  # the empty row too
    row = found[ids["ZZJA"]]
    assert (
        row["value"],
        row["news_count"],
        row["social_count"],
        row["last_at"],
        row["batch_id"],
    ) == (
        D("0.3100"),
        12,
        0,
        datetime(2026, 11, 4, 16, 30, tzinfo=timezone.utc),
        world.sentiment,
    )
    assert row["weights_version"] == "weights-v1" and isinstance(row["id"], int)
    assert row["model_version"] == "ProsusAI/finbert@synthetic0001"
    assert found[ids["ZZJC"]]["value"] is None


def test_read_ticker_sentiment_refuses_another_sources_batch(engine, world):
    with engine.connect() as conn, pytest.raises(ValueError):
        job().read_ticker_sentiment(conn, world.fundamentals)


# ── JOB-5: derive_explanations ───────────────────────────


def explanations_ok(engine):
    result = job().derive_explanations(engine)
    assert result.succeeded, getattr(result, "error_code", None)
    return result


def latest_explanations(engine):
    with engine.connect() as conn:
        batch = conn.execute(
            text(
                "SELECT batch_id FROM perennial.ingestion_runs WHERE source = 'score_explanations'"
                " AND status = 'succeeded' ORDER BY finished_at DESC, id DESC LIMIT 1"
            )
        ).scalar_one()
    return batch, rows(engine, "score_explanations", batch)


def test_one_explanation_per_score_row_matching_explain(engine, world):
    current = scores_ok(engine)
    explanations_ok(engine)
    batch, stored = latest_explanations(engine)
    assert dependencies(engine, batch) == {"company_scores": current.batch_id}
    scores = rows(engine, "company_scores", current.batch_id)
    fund = source_rows(engine, "fundamental_scores", world.fundamentals)
    sent = source_rows(engine, "ticker_sentiment", world.sentiment)
    assert set(stored) == set(scores)
    for t, row in stored.items():
        assert row["company_score_id"] == scores[t]["id"], t
        assert row["template_version"] == "explain-v2"
        assert len(row["input_digest"]) == 64
        if scores[t]["recommended"]:
            claims = explain_module().claims_for(
                scores[t], fund[t], sent.get(t), window_days=7
            )
            assert (row["status"], row["reason"], row["claims"]) == (
                "ok",
                None,
                claims,
            ), t
    summary = {
        t: (r["status"], r["reason"], len(r["claims"])) for t, r in stored.items()
    }
    assert summary == {
        "ZZJA": ("ok", None, 2),
        "ZZJB": ("not_recommended", None, 0),
        "ZZJC": ("unavailable", "score_unavailable", 0),
        "ZZJD": ("unavailable", "score_unavailable", 0),
        "ZZJE": ("not_recommended", None, 0),
    }
    # Decision 103 (c), #93: 18% growth (45 points) is under 50, so it isn't a
    # claim; the 42-cent margin is, and the positive sentiment keeps the last place.
    assert [(c["component"], c["measure"]) for c in stored["ZZJA"]["claims"]] == [
        ("fundamentals", "operating_margin"),
        ("sentiment", "value"),
    ]
    assert stored["ZZJA"]["claims"][-1]["text"] == (
        "News coverage over the last 7 days leaned positive (12 articles)."
    )


def test_the_explanation_window_is_the_sentiment_envelopes_window_days(engine):
    for t in ("ZZJA", "ZZJS"):
        seed_security(engine, t)
    _import(
        engine,
        "fundamental_scores",
        [fundamentals_record("ZZJA", "78.0"), fundamentals_record("ZZJS", "78.0")],
    )
    _import(
        engine,
        "ticker_sentiment",
        [
            sentiment_record("ZZJA", "0.3100", 9, social=3),
            # Positive: under decision 103 (c) only a positive lean is a claim.
            sentiment_record("ZZJS", "0.2000", 0, social=4),
        ],
        envelope=sentiment_envelope(window_days=3),
    )
    publish(engine, popular=("ZZJA", "ZZJS"))
    scores_ok(engine)
    explanations_ok(engine)
    _, stored = latest_explanations(engine)
    assert stored["ZZJA"]["claims"][-1]["text"] == (
        "News and social posts over the last 3 days leaned positive"
        " (9 articles, 3 posts)."
    )
    assert stored["ZZJS"]["claims"][-1]["text"] == (
        "Social posts over the last 3 days leaned positive (4 posts)."
    )
    assert stored["ZZJS"]["claims"][-1]["as_of"] == "2026-11-04"


def test_every_claim_cites_a_record_the_score_used(engine, world):
    """03-AC2/AC3: each claim's record is the score row's own fundamentals or sentiment record."""
    current = scores_ok(engine)
    explanations_ok(engine)
    _, stored = latest_explanations(engine)
    scores = rows(engine, "company_scores", current.batch_id)
    for t, row in stored.items():
        for claim in row["claims"]:
            component = claim["component"]
            assert component not in scores[t]["missing"]
            assert claim["record"]["id"] == scores[t][f"{component}_record_id"]


def served_explanation_id(engine, symbol):
    """The score_explanations row the API serves for a symbol: the newest for the
    pointer's score row (test_read_api_score.py)."""
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT e.id FROM perennial.current_scores p"
                " JOIN perennial.company_scores c ON c.batch_id = p.batch_id"
                " JOIN perennial.securities s ON s.id = c.security_id"
                " JOIN perennial.score_explanations e ON e.company_score_id = c.id"
                " WHERE p.channel = 'production' AND s.symbol = :s ORDER BY e.id DESC LIMIT 1"
            ),
            {"s": symbol},
        ).scalar_one()


def test_a_rerun_with_unchanged_inputs_serves_the_same_explanation_row(engine, world):
    """03-AC4 (decision D1): the scores run is skipped, the pointer is unchanged, and the
    API serves the same score_explanations row, with no new explanations batch."""
    first_pointer = scores_ok(engine)
    explanations_ok(engine)
    first_batch, first = latest_explanations(engine)
    served = {t: served_explanation_id(engine, t) for t in TICKERS}
    count = batches(engine, "score_explanations")
    assert scores_ok(engine) == first_pointer
    explanations_ok(engine)
    second_batch, second = latest_explanations(engine)
    assert (second_batch, second) == (first_batch, first)
    assert batches(engine, "score_explanations") == count
    assert {t: served_explanation_id(engine, t) for t in TICKERS} == served


def _reword(engine, batch, symbol):
    """Edit one stored explanation's wording, as if an earlier template had written it."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE perennial.score_explanations e SET claims = ("
                " SELECT jsonb_agg(jsonb_set(c, '{text}', to_jsonb('stored wording'::text)) ORDER BY ord)"
                " FROM jsonb_array_elements(e.claims) WITH ORDINALITY AS x(c, ord))"
                " FROM perennial.securities s WHERE s.id = e.security_id AND s.symbol = :s AND e.batch_id = :b"
            ),
            {"s": symbol, "b": batch},
        )


def test_re_explaining_the_same_score_row_returns_the_stored_explanation(
    engine, world, monkeypatch
):
    """03-AC4: for the same scores row an equal digest returns the stored explanation, not
    a regenerated one. Here the wording code changes between the two runs (same
    template_version): the stored wording must still be served."""
    scores_ok(engine)
    explanations_ok(engine)
    _, before = latest_explanations(engine)
    served = served_explanation_id(engine, "ZZJA")
    real = explain_module().claims_for

    def reworded(*args, **kwargs):
        return [dict(c, text="new wording") for c in real(*args, **kwargs)]

    monkeypatch.setattr(explain_module(), "claims_for", reworded)
    explanations_ok(engine)
    _, after = latest_explanations(engine)
    assert after["ZZJA"]["claims"] == before["ZZJA"]["claims"]
    assert all(c["text"] != "new wording" for c in after["ZZJA"]["claims"])
    assert served_explanation_id(engine, "ZZJA") == served


def test_an_explanation_stored_under_explain_v1_is_regenerated(
    engine, world, monkeypatch
):
    """F1 on #93: decision 103 (c) changed which claims are chosen, so the template is
    explain-v2. A row stored under explain-v1 with an equal digest is regenerated, not
    carried forward, and the new batch carries nothing forward."""
    scores_ok(engine)
    real = explain_module().claims_for

    def old_wording(*args, **kwargs):
        return [dict(c, text="stored wording") for c in real(*args, **kwargs)]

    with monkeypatch.context() as m:  # what explain-v1 stored
        m.setattr(explain_module(), "TEMPLATE_VERSION", "explain-v1")
        m.setattr(explain_module(), "claims_for", old_wording)
        explanations_ok(engine)
    batch, old = latest_explanations(engine)
    assert {r["template_version"] for r in old.values()} == {"explain-v1"}
    served = served_explanation_id(engine, "ZZJA")
    explanations_ok(engine)
    new_batch, stored = latest_explanations(engine)
    assert new_batch != batch
    assert {r["template_version"] for r in stored.values()} == {"explain-v2"}
    assert {t: r["input_digest"] for t, r in stored.items()} == {
        t: r["input_digest"] for t, r in old.items()
    }  # equal digests: only the template changed
    assert stored["ZZJA"]["status"] == "ok"
    assert all(c["text"] != "stored wording" for c in stored["ZZJA"]["claims"])
    assert served_explanation_id(engine, "ZZJA") != served
    with engine.connect() as conn:
        envelope = conn.execute(
            text(
                "SELECT payload -> 'envelope' FROM perennial.source_batches WHERE id = :b"
            ),
            {"b": new_batch},
        ).scalar_one()
    assert (envelope["template_version"], envelope["carried_forward_count"]) == (
        "explain-v2",
        0,
    )


def test_a_new_scores_batch_regenerates_even_with_identical_values(engine, world):
    """Decision D1: the digest keeps the rows' ids, so a new scores batch (here a new
    publication listing the same companies) regenerates, citing the current rows."""
    scores_ok(engine)
    explanations_ok(engine)
    batch, _ = latest_explanations(engine)
    _reword(engine, batch, "ZZJA")
    publish(engine, popular=("ZZJA", "ZZJB"), affordable=("ZZJC", "ZZJD"))
    current = scores_ok(engine)
    explanations_ok(engine)
    new_batch, stored = latest_explanations(engine)
    assert new_batch != batch
    assert stored["ZZJA"]["status"] == "ok"
    assert all(c["text"] != "stored wording" for c in stored["ZZJA"]["claims"])
    score = rows(engine, "company_scores", current.batch_id)["ZZJA"]
    assert stored["ZZJA"]["company_score_id"] == score["id"]
    for c in stored["ZZJA"]["claims"]:
        assert c["record"]["id"] == score[f"{c['component']}_record_id"]


def test_a_changed_input_regenerates_the_explanation(engine, world):
    scores_ok(engine)
    explanations_ok(engine)
    batch, _ = latest_explanations(engine)
    _reword(engine, batch, "ZZJA")
    _import(
        engine,
        "fundamental_scores",
        [
            fundamentals_record("ZZJA", "80.0"),
            fundamentals_record("ZZJB", "50.0", growth="0.05", margin="-0.10"),
            fundamentals_record("ZZJC", "78.0"),
            fundamentals_record("ZZJD", None),
            fundamentals_record("ZZJE", "90.0", growth="0.30", trend="0.05"),
        ],
    )
    scores_ok(engine)
    explanations_ok(engine)
    _, stored = latest_explanations(engine)
    assert stored["ZZJA"]["status"] == "ok"
    assert all(c["text"] != "stored wording" for c in stored["ZZJA"]["claims"])


def _bad_window_world(engine, envelope, listed=True):
    """ZZJA scores ok from a ticker_sentiment batch whose envelope has this window."""
    seed_security(engine, "ZZJA")
    _import(engine, "fundamental_scores", [fundamentals_record("ZZJA", "78.0")])
    _import(
        engine,
        "ticker_sentiment",
        [sentiment_record("ZZJA", "0.3100")],
        envelope=envelope,
    )
    publish(engine, popular=("ZZJA",) if listed else ())
    return scores_ok(engine)


def _explanations_run(engine, run_id):
    with engine.connect() as conn:
        return (
            conn.execute(
                text(
                    "SELECT status, error_code, error_summary, CAST(diagnostics AS text) AS diagnostics"
                    " FROM perennial.ingestion_runs WHERE id = :r"
                ),
                {"r": run_id},
            )
            .mappings()
            .one()
        )


def _without(key):
    return {k: v for k, v in sentiment_envelope().items() if k != key}


BAD_WINDOWS = {
    "absent": _without("window_days"),
    "null": {**sentiment_envelope(), "window_days": None},
    "zero": {**sentiment_envelope(), "window_days": 0},
    "negative": {**sentiment_envelope(), "window_days": -3},
    "text": {**sentiment_envelope(), "window_days": "7"},
    "fraction": {**sentiment_envelope(), "window_days": 7.5},
    "boolean": {**sentiment_envelope(), "window_days": True},
}


@pytest.mark.parametrize("case", sorted(BAD_WINDOWS))
def test_a_sentiment_envelope_without_a_usable_window_fails_closed(engine, case):
    """No positive int window_days on the cited ticker_sentiment batch: the run fails
    invalid_upstream, writes no explanations batch, and leaves the pointer alone."""
    import logging

    from src.ingestion.diagnostics import ERROR_SUMMARIES

    current = _bad_window_world(engine, BAD_WINDOWS[case])
    records = []

    class Keep(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    loggers = [logging.getLogger(n) for n in ("src", "src.services.scores.job")]
    saved = [(lg.disabled, lg.level) for lg in loggers]
    handler = Keep(level=logging.DEBUG)
    for lg in loggers:
        lg.disabled = False
        lg.setLevel(logging.DEBUG)
    loggers[0].addHandler(handler)
    try:
        result = job().derive_explanations(engine)
    finally:
        loggers[0].removeHandler(handler)
        for lg, (disabled, level) in zip(loggers, saved):
            lg.disabled, lg.level = disabled, level
    assert (result.succeeded, result.error_code) == (False, "invalid_upstream")
    assert "invalid_upstream" in ERROR_SUMMARIES
    assert batches(engine, "score_explanations") == 0
    assert pointer(engine) == current
    run = _explanations_run(engine, result.run_id)
    assert (run["status"], run["error_code"]) == ("failed", "invalid_upstream")
    assert records, "the job logs its outcome"
    seen = " ".join([*records, run["error_summary"] or "", run["diagnostics"] or ""])
    for row_data in ("ZZJA", "0.3100", "0.31", "ProsusAI", "weights-v1"):
        assert row_data not in seen, row_data


def test_an_unlisted_score_citing_a_windowless_batch_also_fails_closed(engine):
    """The rule is on the cited batch, not on whether a claim needs the window."""
    current = _bad_window_world(engine, _without("window_days"), listed=False)
    result = job().derive_explanations(engine)
    assert (result.succeeded, result.error_code) == (False, "invalid_upstream")
    assert batches(engine, "score_explanations") == 0
    assert pointer(engine) == current


def test_without_a_current_scores_batch_explanations_fail_missing_upstream(engine):
    result = job().derive_explanations(engine)
    assert (result.succeeded, result.error_code) == (False, "missing_upstream")
    assert batches(engine, "score_explanations") == 0


# ── JOB-6: --job scores ──────────────────────────────────


def test_the_scheduler_offers_and_dispatches_the_scores_job(monkeypatch):
    from src.pipeline import scheduler

    calls = []
    monkeypatch.setattr(
        job(), "run_once", lambda settings=None: calls.append(settings) or 0
    )
    assert scheduler.run_once("scores", settings="synthetic-settings") == 0
    assert calls == ["synthetic-settings"]
    import inspect
    import re

    assert re.search(
        r'choices=\([^)]*"scores"', inspect.getsource(scheduler)
    )  # --job scores


@pytest.mark.parametrize(
    "scores,explanations,code,order",
    [
        (True, True, 0, ["scores", "explanations"]),
        (True, False, 1, ["scores", "explanations"]),
        (False, True, 1, ["scores"]),
    ],
)
def test_run_once_derives_scores_then_explanations(
    monkeypatch, scores, explanations, code, order
):
    from src.db import session

    calls = []

    class FakeEngine:
        def dispose(self):
            calls.append("disposed")

    def result(name, ok):
        def run(engine):
            calls.append(name)
            return SimpleNamespace(
                succeeded=ok, error_code=None if ok else "missing_upstream"
            )

        return run

    monkeypatch.setattr(session, "make_engine", lambda *a, **k: FakeEngine())
    monkeypatch.setattr(job(), "derive_scores", result("scores", scores))
    monkeypatch.setattr(
        job(), "derive_explanations", result("explanations", explanations)
    )
    settings = SimpleNamespace(
        database_url=SimpleNamespace(get_secret_value=lambda: "postgresql://synthetic")
    )
    assert job().run_once(settings) == code
    assert [c for c in calls if c != "disposed"] == order
    assert calls[-1] == "disposed"
