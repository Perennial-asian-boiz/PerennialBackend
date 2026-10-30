"""Sprint 2 TEST-3: explain-v1, the "why Perennial recommends this company" claims.

Spec: output/sprint-2/s2-stories-plan.md §3.3 and §5 (JOB-4, TEST-3); S2-03
AC1-AC5. Pure: rows go in as plain mappings, shaped as migration 0018 stores
them (test_scores_schema.py) and as fundamental_scores stores a fundamentals score.

Pinned interface, src/services/scores/explain.py:

* TEMPLATE_VERSION = "explain-v1".
* explain(score_row, fundamentals_row, sentiment_row, previous=None) -> Explanation
  with status, reason, template_version, claims (a list of dicts) and input_digest.
  `previous` is the last stored score_explanations row for the security: an equal
  digest under the same template returns it unchanged (AC4).
* claims_for(score_row, fundamentals_row, sentiment_row) -> the claims alone.
* input_digest(score_row, fundamentals_row, sentiment_row) -> 64 lower-case hex.

A claim is {order, text, component, measure, record: {table, id, batch_id},
as_of, value}. `measure` is the cited record's column and `value` its stored
value as a string. Decision 103 (c): only supporting signals are claims. A
fundamentals measure that fundamentals.scoring.breakdown() scores at 50 points
or more is one, by points descending (ties in MEASURES order); sentiment is
one when it leans positive, and then always keeps one of the three places. A
fundamentals claim's text is key_reasons()' sentence for that measure. The
sentiment row is the PR #85 contract (decision 108): news and social counts,
first_at and last_at.
"""

import dataclasses
import json
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

D = Decimal


def explain_module():
    from src.services.scores import explain

    return explain


def fundamentals_row(**over):
    """A general-model fundamentals score: growth 45 points, margin 100, trend 55."""
    return {
        "id": 812,
        "batch_id": 5,
        "fiscal_year_end": date(2025, 6, 30),
        "model": "general",
        "score": D("78.0"),
        "missing": [],
        "revenue_growth_3y": D("0.180000"),
        "operating_margin": D("0.420000"),
        "operating_margin_trend": D("0.010000"),
        "return_on_equity": None,
        "efficiency_ratio": None,
        "profit_source": "operating_income",
        "gross_margin": D("0.550000"),
        "piotroski_score": 7,
        "piotroski_signals": 9,
        "score_version": "fundamentals-v1",
        **over,
    }


def bank_row(**over):
    """A bank: growth 40 points, return on equity 75, efficiency 66.7."""
    return fundamentals_row(
        model="bank",
        revenue_growth_3y=D("0.100000"),
        operating_margin=None,
        operating_margin_trend=None,
        return_on_equity=D("0.150000"),
        efficiency_ratio=D("0.600000"),
        profit_source=None,
        gross_margin=None,
        **over,
    )


def sentiment_row(**over):
    """A ticker_sentiment row of the PR #85 contract (decision 108)."""
    return {
        "id": 77,
        "batch_id": 9,
        "value": D("0.3100"),
        "plain_average": D("0.2500"),
        "article_count": 12,
        "news_count": 10,
        "social_count": 2,
        "first_at": datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc),
        "last_at": datetime(2026, 11, 4, 17, 2, tzinfo=timezone.utc),
        "weights_version": "weights-v1",
        "model_version": "ProsusAI/finbert@synthetic0001",
        **over,
    }


def score_row(**over):
    """A recommended company_scores row (§3.4's full example)."""
    row = {
        "id": 501,
        "batch_id": 11,
        "status": "ok",
        "unavailable_reason": None,
        "score": D("74.3"),
        "score_version": "score-v1",
        "missing": ["event_exposure"],
        "recommended": True,
        "watchlist_publication_id": 3,
        "fundamentals_value": D("78.0"),
        "fundamentals_nominal_weight": D("0.6000"),
        "fundamentals_weight": D("0.7059"),
        "fundamentals_contribution": D("55.06"),
        "fundamentals_input_as_of": date(2025, 6, 30),
        "fundamentals_input_version": "fundamentals-v1",
        "fundamentals_record_id": 812,
        "sentiment_value": D("65.5"),
        "sentiment_nominal_weight": D("0.2500"),
        "sentiment_weight": D("0.2941"),
        "sentiment_contribution": D("19.26"),
        "sentiment_input_as_of": date(2026, 11, 4),
        "sentiment_input_version": "weights-v1",
        "sentiment_record_id": 77,
        "event_exposure_value": None,
        "event_exposure_nominal_weight": D("0.1500"),
        "event_exposure_weight": D("0.0000"),
        "event_exposure_contribution": D("0.00"),
        "event_exposure_input_as_of": None,
        "event_exposure_input_version": None,
        "event_exposure_record_id": None,
    }
    return {**row, **over}


def explain(score=None, fundamentals="default", sentiment="default", **kw):
    score = score_row() if score is None else score
    fundamentals = fundamentals_row() if fundamentals == "default" else fundamentals
    sentiment = sentiment_row() if sentiment == "default" else sentiment
    return explain_module().explain(score, fundamentals, sentiment, **kw)


def sentence(measure, row):
    """key_reasons()' sentence for one measure of a fundamentals row."""
    from src.services.fundamentals.scoring import key_reasons

    only = {
        "model": row["model"],
        "profit_source": row["profit_source"],
        measure: row[measure],
    }
    (text,) = key_reasons(only)
    return text


def fundamentals_claim(order, measure, row=None):
    row = row or fundamentals_row()
    return {
        "order": order,
        "text": sentence(measure, row),
        "component": "fundamentals",
        "measure": measure,
        "record": {
            "table": "fundamental_scores",
            "id": row["id"],
            "batch_id": row["batch_id"],
        },
        "as_of": row["fiscal_year_end"].isoformat(),
        "value": str(row[measure]),
    }


LEAN = (
    "News and social posts between Oct 29 and Nov 4 leaned positive"
    " (10 news articles and 2 social posts)."
)


def sentiment_claim(order, text=LEAN, **over):
    row = sentiment_row(**over)
    return {
        "order": order,
        "text": text,
        "component": "sentiment",
        "measure": "value",
        "record": {
            "table": "ticker_sentiment",
            "id": row["id"],
            "batch_id": row["batch_id"],
        },
        "as_of": row["last_at"].date().isoformat(),
        "value": str(row["value"]),
    }


# ── AC1: up to three claims, in a fixed order ────────────


def test_template_version():
    assert explain_module().TEMPLATE_VERSION == "explain-v1"


NEGATIVE = sentiment_row(value=D("-0.2000"))


def test_without_positive_sentiment_three_supporting_measures_fill_the_claims():
    """Margin 100 points, growth 62.5, trend 55, and sentiment that leans negative."""
    row = fundamentals_row(revenue_growth_3y=D("0.250000"))
    result = explain(fundamentals=row, sentiment=NEGATIVE)
    assert (result.status, result.reason, result.template_version) == (
        "ok",
        None,
        "explain-v1",
    )
    assert list(result.claims) == [
        fundamentals_claim(1, "operating_margin", row),
        fundamentals_claim(2, "revenue_growth_3y", row),
        fundamentals_claim(3, "operating_margin_trend", row),
    ]


def test_positive_sentiment_always_keeps_the_last_place():
    """Three supporting measures: the two with the most points, then sentiment."""
    row = fundamentals_row(revenue_growth_3y=D("0.250000"))
    result = explain(fundamentals=row)
    assert list(result.claims) == [
        fundamentals_claim(1, "operating_margin", row),
        fundamentals_claim(2, "revenue_growth_3y", row),
        sentiment_claim(3),
    ]
    assert [c["text"] for c in result.claims] == [
        "Keeps 42 cents of every sales dollar as operating profit.",
        "Revenue grew 25% a year over the last 3 years.",
        LEAN,
    ]


def test_a_measure_below_50_points_is_not_a_reason():
    """Growth of 18% a year is 45 points: not a reason to recommend."""
    result = explain()
    assert [c["measure"] for c in result.claims] == [
        "operating_margin",
        "operating_margin_trend",
        "value",
    ]


@pytest.mark.parametrize(
    "over,absent",
    [
        # GTLB on the live run: recommended at 71.9 with a -7% operating margin.
        ({"operating_margin": D("-0.070000")}, "Spends more than it earns"),
        # DKNG: a -0.3% margin read "operating loss of 0 cents per sales dollar".
        ({"operating_margin": D("-0.003000")}, "0 cents"),
        ({"operating_margin_trend": D("-0.050000")}, "worsened"),
        ({"revenue_growth_3y": D("-0.020000")}, "shrank"),
    ],
)
def test_losses_declines_and_shrinking_revenue_are_never_reasons(over, absent):
    result = explain(fundamentals=fundamentals_row(**over))
    assert result.claims and all(absent not in c["text"] for c in result.claims)
    assert {c["measure"] for c in result.claims}.isdisjoint(over)


def test_a_trend_that_rounds_to_0_points_is_not_a_reason():
    """+0.4 points is 52 points, but would read "Profitability improved 0 points"."""
    row = fundamentals_row(operating_margin_trend=D("0.004000"))
    result = explain(fundamentals=row, sentiment=NEGATIVE)
    assert [c["measure"] for c in result.claims] == ["operating_margin"]
    assert all("improved 0 points" not in c["text"] for c in result.claims)


def test_bank_measures_order_by_their_points():
    """Return on equity 75 points, efficiency 66.7, growth 40 (not a reason)."""
    row = bank_row()
    result = explain(fundamentals=row)
    assert [c["measure"] for c in result.claims] == [
        "return_on_equity",
        "efficiency_ratio",
        "value",
    ]
    assert [c["text"] for c in result.claims[:2]] == [
        "Earns 15% a year on shareholders' money.",
        "Spends 60 cents to earn each dollar of revenue.",
    ]


def test_equal_points_keep_the_measures_order():
    """Growth 0.30 and trend 0.05 are both 75 points; growth comes first in MEASURES."""
    row = fundamentals_row(
        revenue_growth_3y=D("0.300000"), operating_margin_trend=D("0.050000")
    )
    assert [
        c["measure"] for c in explain(fundamentals=row, sentiment=NEGATIVE).claims
    ] == [
        "operating_margin",
        "revenue_growth_3y",
        "operating_margin_trend",
    ]


def test_gross_margin_is_not_a_scored_measure_and_never_a_claim():
    row = fundamentals_row(operating_margin_trend=None, revenue_growth_3y=None)
    measures = [c["measure"] for c in explain(fundamentals=row).claims]
    assert measures == ["operating_margin", "value"]
    assert "gross_margin" not in json.dumps(explain(fundamentals=row).claims)


@pytest.mark.parametrize(
    "news,social,text",
    [
        (10, 2, LEAN),
        (
            12,
            0,
            "News coverage between Oct 29 and Nov 4 leaned positive (12 news articles).",
        ),
        (
            0,
            3,
            "Social posts between Oct 29 and Nov 4 leaned positive (3 social posts).",
        ),
        (
            1,
            1,
            "News and social posts between Oct 29 and Nov 4 leaned positive"
            " (1 news article and 1 social post).",
        ),
    ],
)
def test_sentiment_claim_wording_names_its_sources(news, social, text):
    """#85 rows can be news only, social only (until S1-04 scores articles) or both."""
    counts = {
        "article_count": news + social,
        "news_count": news,
        "social_count": social,
    }
    row = fundamentals_row(operating_margin_trend=None, revenue_growth_3y=None)
    result = explain(fundamentals=row, sentiment=sentiment_row(**counts))
    assert result.claims[-1] == sentiment_claim(2, text, **counts)


@pytest.mark.parametrize(
    "first_at,last_at,span",
    [
        (
            datetime(2026, 11, 4, 9, 0, tzinfo=timezone.utc),
            datetime(2026, 11, 4, 17, 0, tzinfo=timezone.utc),
            "on Nov 4",
        ),
        (
            datetime(2026, 12, 29, 9, 0, tzinfo=timezone.utc),
            datetime(2027, 1, 4, 17, 0, tzinfo=timezone.utc),
            "between Dec 29, 2026 and Jan 4, 2027",
        ),
    ],
)
def test_the_sentiment_span_reads_naturally(first_at, last_at, span):
    sentiment = sentiment_row(first_at=first_at, last_at=last_at)
    text = explain(sentiment=sentiment).claims[-1]["text"]
    assert text.startswith(f"News and social posts {span} leaned positive")


def test_the_sentiment_as_of_is_the_utc_date_of_last_at():
    pacific = timezone(timedelta(hours=-8))
    late = sentiment_row(last_at=datetime(2026, 11, 4, 23, 30, tzinfo=pacific))
    claim = explain(sentiment=late).claims[-1]
    assert claim["as_of"] == "2026-11-05"
    assert "and Nov 5 leaned" in claim["text"]


@pytest.mark.parametrize("value", ["0.0000", "-0.2500"])
def test_neutral_or_negative_sentiment_is_not_a_reason(value):
    result = explain(sentiment=sentiment_row(value=D(value)))
    assert [c["component"] for c in result.claims] == ["fundamentals", "fundamentals"]


# ── AC2/AC3: traceable, and nothing about an unavailable component ─


def test_every_claim_cites_a_stored_record_and_an_as_of_date():
    for fundamentals in (
        fundamentals_row(),
        fundamentals_row(operating_margin_trend=None),
        bank_row(),
    ):
        for claim in explain(fundamentals=fundamentals).claims:
            assert set(claim) == {
                "order",
                "text",
                "component",
                "measure",
                "record",
                "as_of",
                "value",
            }
            assert claim["record"]["table"] in (
                "fundamental_scores",
                "ticker_sentiment",
            )
            assert isinstance(claim["record"]["id"], int) and isinstance(
                claim["record"]["batch_id"], int
            )
            assert date.fromisoformat(claim["as_of"])
            assert claim["text"] and claim["text"] == claim["text"].strip()


def test_claims_are_json_ready():
    claims = explain(fundamentals=fundamentals_row(operating_margin_trend=None)).claims
    assert json.loads(json.dumps(list(claims))) == list(claims)


def test_no_claim_about_sentiment_when_the_score_lists_it_missing():
    """The sentiment row is ignored when company_scores says sentiment was missing."""
    score = score_row(
        missing=["sentiment"],
        sentiment_value=None,
        sentiment_weight=D("0"),
        sentiment_contribution=D("0"),
        sentiment_record_id=None,
        event_exposure_value=D("40.0"),
    )
    row = fundamentals_row(revenue_growth_3y=D("0.250000"), operating_margin_trend=None)
    result = explain(score, fundamentals=row, sentiment=sentiment_row())
    assert all(c["component"] != "sentiment" for c in result.claims)
    assert len(result.claims) == 2


def test_no_claim_about_a_missing_component_even_without_its_row():
    row = fundamentals_row(revenue_growth_3y=D("0.250000"), operating_margin_trend=None)
    score = score_row(
        missing=["sentiment"], sentiment_record_id=None, event_exposure_value=D("40.0")
    )
    assert [
        c["component"] for c in explain(score, fundamentals=row, sentiment=None).claims
    ] == [
        "fundamentals",
        "fundamentals",
    ]


def test_event_exposure_never_produces_a_claim_in_v1():
    result = explain()
    assert all(c["component"] in ("fundamentals", "sentiment") for c in result.claims)


@pytest.mark.parametrize(
    "which,over",
    [
        ("fundamentals", {"id": 813}),
        ("sentiment", {"id": 78}),
    ],
)
def test_a_row_that_is_not_the_scores_cited_record_is_refused(which, over):
    """The claims must cite the records the score was computed from."""
    kwargs = (
        {"fundamentals": fundamentals_row(**over)} if which == "fundamentals" else {}
    )
    if which == "sentiment":
        kwargs = {
            "fundamentals": fundamentals_row(operating_margin_trend=None),
            "sentiment": sentiment_row(**over),
        }
    with pytest.raises(ValueError):
        explain(**kwargs)


# ── statuses (AC5, and only recommended companies) ───────


def test_a_score_that_is_not_recommended_gets_no_claims():
    result = explain(
        score_row(recommended=False, score=D("55.0"), watchlist_publication_id=None)
    )
    assert (result.status, result.reason, list(result.claims)) == (
        "not_recommended",
        None,
        [],
    )
    assert result.template_version == "explain-v1"


def test_an_unavailable_score_gets_no_claims():
    score = score_row(
        status="unavailable",
        unavailable_reason="only_fundamentals",
        score=None,
        recommended=False,
        missing=["sentiment", "event_exposure"],
    )
    result = explain(score, sentiment=None)
    assert (result.status, result.reason, list(result.claims)) == (
        "unavailable",
        "score_unavailable",
        [],
    )


def test_zero_candidate_claims_is_unavailable_for_insufficient_data():
    """AC5's fixture: recommended, but no scored measure and no sentiment to cite."""
    bare = fundamentals_row(
        revenue_growth_3y=None,
        operating_margin=None,
        operating_margin_trend=None,
    )
    score = score_row(
        missing=["sentiment"], sentiment_record_id=None, event_exposure_value=D("40.0")
    )
    result = explain(score, fundamentals=bare, sentiment=None)
    assert (result.status, result.reason, list(result.claims)) == (
        "unavailable",
        "insufficient_supporting_data",
        [],
    )


def test_a_recommended_score_with_no_supporting_signal_is_unavailable():
    """AC5 under decision 103 (c): every measure below 50 points, sentiment negative."""
    weak = fundamentals_row(
        revenue_growth_3y=D("0.050000"),
        operating_margin=D("-0.070000"),
        operating_margin_trend=D("-0.020000"),
    )
    result = explain(fundamentals=weak, sentiment=NEGATIVE)
    assert (result.status, result.reason, list(result.claims)) == (
        "unavailable",
        "insufficient_supporting_data",
        [],
    )


def test_claims_for_matches_explain():
    m = explain_module()
    assert list(m.claims_for(score_row(), fundamentals_row(), sentiment_row())) == list(
        explain().claims
    )


# ── AC4: the digest, and returning the stored explanation ─


def digest(score=None, fundamentals=None, sentiment="default"):
    return explain_module().input_digest(
        score or score_row(),
        fundamentals or fundamentals_row(),
        sentiment_row() if sentiment == "default" else sentiment,
    )


def test_the_digest_is_sha256_hex_and_deterministic():
    first = digest()
    assert re.fullmatch(r"[0-9a-f]{64}", first)
    assert first == digest()
    assert explain().input_digest == first


def test_the_digest_ignores_key_order():
    reordered = dict(reversed(list(score_row().items())))
    assert digest(score=reordered) == digest()


@pytest.mark.parametrize(
    "change",
    [
        {"score": {"score": D("74.4")}},
        {"score": {"missing": ["event_exposure", "sentiment"]}},
        {"score": {"recommended": False}},
        {"fundamentals": {"operating_margin": D("0.410000")}},
        {"fundamentals": {"revenue_growth_3y": D("0.190000")}},
        {"fundamentals": {"fiscal_year_end": date(2026, 6, 30)}},
        {"sentiment": {"article_count": 13}},
        {"sentiment": {"news_count": 11}},
        {"sentiment": {"value": D("0.3200")}},
        {"sentiment": {"last_at": datetime(2026, 11, 5, 9, 0, tzinfo=timezone.utc)}},
    ],
)
def test_the_digest_changes_with_any_input_a_claim_or_the_score_uses(change):
    ((which, over),) = change.items()
    kwargs = {
        "score": score_row(**over) if which == "score" else None,
        "fundamentals": fundamentals_row(**over) if which == "fundamentals" else None,
    }
    if which == "sentiment":
        kwargs["sentiment"] = sentiment_row(**over)
    assert digest(**kwargs) != digest()


def stored(result, **over):
    """A score_explanations row as stored."""
    return {
        "status": result.status,
        "reason": result.reason,
        "template_version": result.template_version,
        "claims": [dict(c, text=f"stored wording {c['order']}") for c in result.claims],
        "input_digest": result.input_digest,
        **over,
    }


def test_an_equal_digest_returns_the_stored_explanation_unchanged():
    previous = stored(explain())
    again = explain(previous=previous)
    assert list(again.claims) == previous["claims"]  # returned, not regenerated
    assert (again.status, again.reason, again.input_digest) == (
        "ok",
        None,
        previous["input_digest"],
    )


def test_a_changed_input_regenerates():
    previous = stored(explain())
    again = explain(score_row(score=D("74.4")), previous=previous)
    assert all(not c["text"].startswith("stored wording") for c in again.claims)
    assert again.input_digest != previous["input_digest"]


def test_a_new_template_version_regenerates_even_with_an_equal_digest():
    previous = stored(explain(), template_version="explain-v0")
    again = explain(previous=previous)
    assert again.template_version == "explain-v1"
    assert all(not c["text"].startswith("stored wording") for c in again.claims)


def test_the_result_is_frozen():
    result = explain()
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.status = "not_recommended"


def test_explain_has_no_io():
    import ast
    import inspect

    source = inspect.getsource(explain_module())
    imported = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    forbidden = (
        "sqlalchemy",
        "requests",
        "httpx",
        "psycopg",
        "anthropic",
        "openai",
        "src.db.session",
        "src.settings",
    )
    assert not [m for m in imported if m.startswith(forbidden)], imported
