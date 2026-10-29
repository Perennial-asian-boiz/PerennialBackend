"""Sprint 2 TEST-1: score-v1, the Confidence Score blend (pure Decimal, no I/O).

Spec: output/sprint-2/s2-stories-plan.md §3.2 and §5 (JOB-1, TEST-1);
docs/technical-specs/DECISIONS.md "Confidence Score composition" rules 1, 2, 4, 6.
S2-01 AC2/3/4, S2-02 AC2/4.

Pinned interface, src/services/scores/scoring.py:

* COMPONENTS = ("fundamentals", "sentiment", "event_exposure"), the fixed order.
* ScoreWeights: a frozen dataclass of Decimals (0.60, 0.25, 0.15) and
  version "score-v1"; envelope() is JSON-safe for the run envelope.
* normalize_sentiment(value) -> Decimal: (value + 1) x 50, quantized to 0.1.
* blend(values, weights=ScoreWeights()) -> ScoreResult. `values` maps a component
  to its 0-100 Decimal; an absent key or None is a missing component.
* breakdown(row) -> the components recomputed from a stored company_scores row
  (`<component>_value`, `<component>_nominal_weight`, ...), as the API serves them.

Rounding (the plan says "quantized" without a mode): half-even, Decimal's default.
Effective weight w_i / sum(w_present) to 0.0001, contribution value x weight to
0.01, score = sum of the rounded contributions, to 0.1.
"""

import dataclasses
import json
from decimal import Decimal

import pytest

D = Decimal


def scoring():
    from src.services.scores import scoring as module

    return module


def blend(fundamentals=None, sentiment=None, event_exposure=None, **kw):
    values = {
        "fundamentals": fundamentals,
        "sentiment": sentiment,
        "event_exposure": event_exposure,
    }
    return scoring().blend({k: v for k, v in values.items() if v is not None}, **kw)


def parts(result):
    return {
        c.name: (c.status, c.value, c.nominal_weight, c.weight, c.contribution)
        for c in result.components
    }


# ── weights and envelope ─────────────────────────────────


def test_components_and_weights_are_fixed_decimals():
    s = scoring()
    assert s.COMPONENTS == ("fundamentals", "sentiment", "event_exposure")
    w = s.ScoreWeights()
    assert (w.fundamentals, w.sentiment, w.event_exposure, w.version) == (
        D("0.60"),
        D("0.25"),
        D("0.15"),
        "score-v1",
    )
    assert all(
        isinstance(x, Decimal) for x in (w.fundamentals, w.sentiment, w.event_exposure)
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        w.fundamentals = D("0.5")


def test_envelope_is_stable_and_json_safe():
    """Stored in the run envelope like ScoreRule.envelope(); the sanitizer keeps only
    str, int, float, bool and lists of them, so no Decimal may appear."""
    s = scoring()
    first, second = s.ScoreWeights().envelope(), s.ScoreWeights().envelope()
    assert first == second
    assert first["score_version"] == "score-v1"
    assert json.loads(json.dumps(first)) == first
    weights = json.dumps(first)
    for text in ("fundamentals", "0.60", "sentiment", "0.25", "event_exposure", "0.15"):
        assert text in weights, text

    def scalar_or_list(v):
        if isinstance(v, list):
            return all(isinstance(x, (str, int, float, bool)) for x in v)
        return isinstance(v, (str, int, float, bool)) or v is None

    assert all(scalar_or_list(v) for v in first.values()), first


def test_a_retuned_weight_changes_the_envelope():
    s = scoring()
    retuned = s.ScoreWeights(
        fundamentals=D("0.70"), sentiment=D("0.15"), version="score-v2"
    )
    assert retuned.envelope() != s.ScoreWeights().envelope()


# ── sentiment normalization (DECISIONS rule 1) ───────────


@pytest.mark.parametrize(
    "value,expected",
    [
        ("-1", "0.0"),
        ("0", "50.0"),
        ("1", "100.0"),
        ("0.31", "65.5"),
        ("-0.62", "19.0"),
        ("0.0010", "50.0"),  # 50.05: half-even keeps 50.0
        ("0.0030", "50.2"),  # 50.15 -> 50.2
        ("-0.9999", "0.0"),  # 0.005 -> 0.0
    ],
)
def test_normalize_sentiment_maps_minus_one_to_one_onto_zero_to_hundred(
    value, expected
):
    result = scoring().normalize_sentiment(D(value))
    assert result == D(expected) and str(result) == expected


@pytest.mark.parametrize("value", ["-1.0001", "1.0001", "2"])
def test_normalize_sentiment_refuses_values_outside_minus_one_to_one(value):
    with pytest.raises(ValueError):
        scoring().normalize_sentiment(D(value))


def test_normalize_sentiment_refuses_floats():
    with pytest.raises(TypeError):
        scoring().normalize_sentiment(0.31)


# ── the blend (AC1, AC3) ─────────────────────────────────


def test_the_plans_hand_calculation_78_and_65_5_gives_74_3():
    result = blend(D("78.0"), D("65.5"))
    assert (result.status, result.unavailable_reason, result.score, result.version) == (
        "ok",
        None,
        D("74.3"),
        "score-v1",
    )
    assert list(result.missing) == ["event_exposure"]
    assert [c.name for c in result.components] == list(scoring().COMPONENTS)
    assert parts(result) == {
        "fundamentals": ("ok", D("78.0"), D("0.60"), D("0.7059"), D("55.06")),
        "sentiment": ("ok", D("65.5"), D("0.25"), D("0.2941"), D("19.26")),
        "event_exposure": ("unavailable", None, D("0.15"), D("0"), D("0")),
    }


def test_exact_decimal_places():
    result = blend(D("78.0"), D("65.5"))
    assert str(result.score) == "74.3"
    by = {c.name: c for c in result.components}
    assert (str(by["fundamentals"].weight), str(by["fundamentals"].contribution)) == (
        "0.7059",
        "55.06",
    )
    assert (str(by["sentiment"].weight), str(by["sentiment"].contribution)) == (
        "0.2941",
        "19.26",
    )


@pytest.mark.parametrize(
    "fundamentals,sentiment,contributions,score",
    [
        ("40.0", "19.0", ("28.24", "5.59"), "33.8"),
        ("50.0", "50.0", ("35.30", "14.70"), "50.0"),  # 14.7050: half-even 14.70
        ("50.0", "0.5", ("35.30", "0.15"), "35.4"),  # 35.45: half-even 35.4
        ("0.0", "6.3", ("0.00", "1.85"), "1.8"),  # unrounded 1.85283 would give 1.9
        ("0.3", "39.9", ("0.21", "11.73"), "11.9"),  # unrounded weights would give 12.0
        ("100.0", "100.0", ("70.59", "29.41"), "100.0"),
        ("0.0", "0.0", ("0.00", "0.00"), "0.0"),
    ],
)
def test_more_hand_calculations(fundamentals, sentiment, contributions, score):
    result = blend(D(fundamentals), D(sentiment))
    by = {c.name: c for c in result.components}
    assert (by["fundamentals"].contribution, by["sentiment"].contribution) == tuple(
        D(c) for c in contributions
    )
    assert result.score == D(score)


def test_every_present_component_redistributes_over_the_present_weights():
    """Event exposure is always missing in score-v1's job, but the rule is general."""
    result = blend(D("80.0"), D("60.0"), D("40.0"))
    assert list(result.missing) == []
    assert parts(result) == {
        "fundamentals": ("ok", D("80.0"), D("0.60"), D("0.6000"), D("48.00")),
        "sentiment": ("ok", D("60.0"), D("0.25"), D("0.2500"), D("15.00")),
        "event_exposure": ("ok", D("40.0"), D("0.15"), D("0.1500"), D("6.00")),
    }
    assert result.score == D("69.0")


def test_fundamentals_and_event_exposure_without_sentiment_is_sufficient():
    result = blend(D("70.0"), event_exposure=D("30.0"))
    assert (result.status, list(result.missing)) == ("ok", ["sentiment"])
    by = {c.name: c for c in result.components}
    assert (by["fundamentals"].weight, by["event_exposure"].weight) == (
        D("0.8000"),
        D("0.2000"),
    )
    assert (
        by["sentiment"].status,
        by["sentiment"].weight,
        by["sentiment"].contribution,
    ) == (
        "unavailable",
        D("0"),
        D("0"),
    )
    assert result.score == D("62.0")


# ── sufficiency (AC4) ────────────────────────────────────


def test_fundamentals_only_is_unavailable_and_invents_nothing():
    result = blend(D("78.0"))
    assert (result.status, result.unavailable_reason, result.score) == (
        "unavailable",
        "only_fundamentals",
        None,
    )
    assert list(result.missing) == ["sentiment", "event_exposure"]
    assert parts(result) == {
        "fundamentals": ("ok", D("78.0"), D("0.60"), None, None),
        "sentiment": ("unavailable", None, D("0.25"), None, None),
        "event_exposure": ("unavailable", None, D("0.15"), None, None),
    }


@pytest.mark.parametrize("sentiment", [None, D("65.5")])
def test_no_fundamentals_is_unavailable_even_with_sentiment(sentiment):
    result = blend(sentiment=sentiment)
    assert (result.status, result.unavailable_reason, result.score) == (
        "unavailable",
        "no_fundamentals",
        None,
    )
    expected = (
        ["fundamentals", "event_exposure"]
        if sentiment is not None
        else list(scoring().COMPONENTS)
    )
    assert list(result.missing) == expected
    assert all(c.weight is None and c.contribution is None for c in result.components)


def test_two_other_components_without_fundamentals_are_still_unavailable():
    """Fundamentals are mandatory (§3.2): two components are not enough without them."""
    result = blend(sentiment=D("65.5"), event_exposure=D("40.0"))
    assert (result.status, result.unavailable_reason, result.score) == (
        "unavailable",
        "no_fundamentals",
        None,
    )
    assert list(result.missing) == ["fundamentals"]


def test_none_and_an_absent_key_both_mean_missing():
    s = scoring()
    assert s.blend(
        {"fundamentals": D("78.0"), "sentiment": D("65.5"), "event_exposure": None}
    ) == s.blend({"fundamentals": D("78.0"), "sentiment": D("65.5")})


def test_a_neutral_sentiment_is_not_missing():
    """DECISIONS rule 4: absence is never treated as neutral, and neutral is not absence."""
    result = blend(D("78.0"), D("50.0"))
    assert (result.status, list(result.missing)) == ("ok", ["event_exposure"])


# ── input guards ─────────────────────────────────────────


@pytest.mark.parametrize("bad", [78.0, 78])
def test_blend_refuses_non_decimal_values(bad):
    with pytest.raises(TypeError):
        blend(bad, D("65.5"))


@pytest.mark.parametrize("bad", ["-0.1", "100.1"])
def test_blend_refuses_values_outside_zero_to_hundred(bad):
    with pytest.raises(ValueError):
        blend(D("78.0"), D(bad))


def test_blend_refuses_an_unknown_component():
    with pytest.raises(ValueError):
        scoring().blend({"fundamentals": D("78.0"), "insider": D("50.0")})


# ── determinism (AC2) and consistency (S2-02 AC2) ────────


def test_the_same_inputs_give_an_identical_result():
    first, second = blend(D("78.0"), D("65.5")), blend(D("78.0"), D("65.5"))
    assert first == second
    with pytest.raises(
        dataclasses.FrozenInstanceError
    ):  # a result is never edited after the fact
        first.score = D("99.9")


def _sweep():
    s = scoring()
    for f in range(0, 1001, 37):
        for raw in range(-10000, 10001, 613):
            yield D(f) / 10, s.normalize_sentiment(D(raw) / 10000)


def test_contributions_add_up_to_the_score_over_a_sweep():
    """S2-02 AC2 asks for +/-0.5; by construction it holds to the rounding of the sum."""
    for fundamentals, sentiment in _sweep():
        result = blend(fundamentals, sentiment)
        total = sum(c.contribution for c in result.components)
        assert result.score == total.quantize(D("0.1")), (fundamentals, sentiment)
        assert abs(total - result.score) <= D("0.05")
        assert sum(c.weight for c in result.components) == D("1.0000")
        assert D("0") <= result.score <= D("100")


def test_sentiment_moves_the_score_less_than_15_points_from_neutral():
    """DECISIONS rule 2 holds by construction in v1: 0.2941 x 50 = 14.7."""
    s = scoring()
    for f in range(0, 1001, 13):
        fundamentals = D(f) / 10
        neutral = blend(fundamentals, D("50.0")).score
        for raw in ("-1", "1"):
            moved = blend(fundamentals, s.normalize_sentiment(D(raw))).score
            assert abs(moved - neutral) < 15, (fundamentals, raw)


# ── breakdown from a stored row (S2-02 AC1/AC4, API-2) ───


def stored(result):
    """A company_scores row as migration 0018 stores it (test_scores_schema.py)."""
    row = {
        "status": result.status,
        "unavailable_reason": result.unavailable_reason,
        "score": result.score,
        "score_version": result.version,
        "missing": list(result.missing),
    }
    for c in result.components:
        row.update(
            {
                f"{c.name}_value": c.value,
                f"{c.name}_nominal_weight": c.nominal_weight,
                f"{c.name}_weight": c.weight,
                f"{c.name}_contribution": c.contribution,
            }
        )
    return row


def test_breakdown_reproduces_the_stored_contributions():
    result = blend(D("78.0"), D("65.5"))
    shown = scoring().breakdown(stored(result))
    assert [
        (c.name, c.status, c.value, c.nominal_weight, c.weight, c.contribution)
        for c in shown
    ] == [
        (c.name, c.status, c.value, c.nominal_weight, c.weight, c.contribution)
        for c in result.components
    ]


def test_breakdown_recomputes_rather_than_echoing_the_row():
    """A row whose stored contribution disagrees with its value and weights is refused."""
    row = stored(blend(D("78.0"), D("65.5")))
    row["fundamentals_contribution"] = D("60.00")
    with pytest.raises(ValueError):
        scoring().breakdown(row)


def test_breakdown_of_an_unavailable_score_is_empty():
    assert list(scoring().breakdown(stored(blend(D("78.0"))))) == []


def test_breakdown_refuses_an_unknown_score_version():
    row = stored(blend(D("78.0"), D("65.5")))
    row["score_version"] = "score-v0"
    with pytest.raises(ValueError):
        scoring().breakdown(row)


def test_scoring_has_no_io():
    """Pure: no database, HTTP or settings imports (the score job owns those)."""
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(scoring()))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    forbidden = {
        "sqlalchemy",
        "requests",
        "httpx",
        "psycopg",
        "src.db.session",
        "src.settings",
    }
    assert not {
        m for m in imported if m.split(".")[0] in forbidden or m in forbidden
    }, imported
    assert "float(" not in inspect.getsource(scoring())
