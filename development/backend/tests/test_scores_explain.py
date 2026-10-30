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
value as a string. Candidates are the fundamentals measures that
fundamentals.scoring.breakdown() scores, by points descending (ties in
MEASURES order), then sentiment; the first three are kept. A fundamentals
claim's text is key_reasons()' sentence for that measure.
"""

import dataclasses
import json
import re
from datetime import date
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
    return {
        "id": 77,
        "batch_id": 9,
        "value": D("0.3100"),
        "article_count": 12,
        "window_start": date(2026, 10, 29),
        "window_end": date(2026, 11, 4),
        "model_version": "ProsusAI/finbert@synthetic0001",
        "source_weights_version": "weights-v1",
        "method": "credibility_weighted_mean",
        "rollup_version": "sentiment-rollup-v1",
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
        "sentiment_input_version": "sentiment-rollup-v1",
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


def sentiment_claim(
    order,
    text="News coverage over the last 7 days leaned positive (12 articles).",
    **over,
):
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
        "as_of": row["window_end"].isoformat(),
        "value": str(row["value"]),
    }


# ── AC1: up to three claims, in a fixed order ────────────


def test_template_version():
    assert explain_module().TEMPLATE_VERSION == "explain-v1"


def test_three_fundamentals_measures_fill_the_claims_by_points():
    """Margin 100 points, trend 55, growth 45: sentiment is a fourth candidate and is cut."""
    result = explain()
    assert (result.status, result.reason, result.template_version) == (
        "ok",
        None,
        "explain-v1",
    )
    assert list(result.claims) == [
        fundamentals_claim(1, "operating_margin"),
        fundamentals_claim(2, "operating_margin_trend"),
        fundamentals_claim(3, "revenue_growth_3y"),
    ]


def test_the_plans_example_two_measures_then_sentiment():
    """§3.4: growth 18% and a 42-cent margin, then the 7-day news lean."""
    row = fundamentals_row(operating_margin_trend=None)
    result = explain(fundamentals=row)
    assert [c["text"] for c in result.claims] == [
        "Keeps 42 cents of every sales dollar as operating profit.",
        "Revenue grew 18% a year over the last 3 years.",
        "News coverage over the last 7 days leaned positive (12 articles).",
    ]
    assert list(result.claims) == [
        fundamentals_claim(1, "operating_margin", row),
        fundamentals_claim(2, "revenue_growth_3y", row),
        sentiment_claim(3),
    ]


def test_bank_measures_order_by_their_points():
    row = bank_row()
    result = explain(fundamentals=row)
    assert [c["measure"] for c in result.claims] == [
        "return_on_equity",
        "efficiency_ratio",
        "revenue_growth_3y",
    ]
    assert result.claims[1]["text"] == "Spends 60 cents to earn each dollar of revenue."


def test_equal_points_keep_the_measures_order():
    """Growth 0.20 and trend 0.00 are both 50 points; growth comes first in MEASURES."""
    row = fundamentals_row(
        revenue_growth_3y=D("0.200000"), operating_margin_trend=D("0.000000")
    )
    assert [c["measure"] for c in explain(fundamentals=row).claims] == [
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
    "value,count,text",
    [
        (
            "0.3100",
            12,
            "News coverage over the last 7 days leaned positive (12 articles).",
        ),
        (
            "-0.2500",
            3,
            "News coverage over the last 7 days leaned negative (3 articles).",
        ),
        (
            "0.0100",
            1,
            "News coverage over the last 7 days leaned positive (1 article).",
        ),
    ],
)
def test_sentiment_claim_wording(value, count, text):
    row = fundamentals_row(operating_margin_trend=None, revenue_growth_3y=None)
    result = explain(
        fundamentals=row, sentiment=sentiment_row(value=D(value), article_count=count)
    )
    assert result.claims[-1] == sentiment_claim(
        2, text, value=D(value), article_count=count
    )


def test_a_zero_sentiment_claims_no_lean():
    row = fundamentals_row(operating_margin_trend=None, revenue_growth_3y=None)
    text = explain(fundamentals=row, sentiment=sentiment_row(value=D("0.0000"))).claims[
        -1
    ]["text"]
    assert "positive" not in text and "negative" not in text
    assert "(12 articles)" in text


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
    row = fundamentals_row(operating_margin_trend=None)
    result = explain(score, fundamentals=row, sentiment=sentiment_row())
    assert all(c["component"] != "sentiment" for c in result.claims)
    assert len(result.claims) == 2


def test_no_claim_about_a_missing_component_even_without_its_row():
    row = fundamentals_row(operating_margin_trend=None)
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
        {"sentiment": {"value": D("0.3200")}},
        {"sentiment": {"window_end": date(2026, 11, 5)}},
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
