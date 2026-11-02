"""The Confidence Score (score-v1), output/sprint-2/s2-stories-plan.md §3.2.

Pure Decimal functions: no network, no database, no binary floating point. The
weights and version are part of ScoreWeights and stored in the scores batch's
envelope, so a stored score can always be explained and recomputed.

Rounding is half-even at each step: effective weight w_i / sum(w_present) to
0.0001, contribution value x weight to 0.01, and the score, the sum of the
rounded contributions, to 0.1. So the contributions shown always add up to the
score (S2-02 AC2).

DECISIONS rule 2 (sentiment moves the score by less than 15 points) holds by
construction in score-v1: fundamentals are mandatory, so sentiment's largest
effective weight is 0.2941, a swing of at most 14.7 from neutral. There is no
cap code; revisit when event exposure arrives.
"""

from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Any, Mapping

COMPONENTS = ("fundamentals", "sentiment", "event_exposure")

WEIGHT_PLACES = Decimal("0.0001")
CONTRIBUTION_PLACES = Decimal("0.01")
SCORE_PLACES = Decimal("0.1")
ZERO = Decimal("0")
HUNDRED = Decimal("100")


@dataclass(frozen=True)
class ScoreWeights:
    fundamentals: Decimal = Decimal("0.60")
    sentiment: Decimal = Decimal("0.25")
    event_exposure: Decimal = Decimal("0.15")
    version: str = "score-v1"

    def nominal(self, component: str) -> Decimal:
        weight: Decimal = getattr(self, component)
        return weight

    def envelope(self) -> dict[str, Any]:
        """JSON-safe: the weights as "component:weight" strings, in component order."""
        return {
            "score_version": self.version,
            "weights": [f"{c}:{self.nominal(c)}" for c in COMPONENTS],
        }


# The versions breakdown() can recompute: a retuned weight is a new version.
VERSIONS = {"score-v1": ScoreWeights()}


@dataclass(frozen=True)
class Component:
    name: str
    status: str  # "ok" or "unavailable"
    value: Decimal | None
    nominal_weight: Decimal
    weight: Decimal | None  # None when the score itself is unavailable
    contribution: Decimal | None


@dataclass(frozen=True)
class ScoreResult:
    status: str  # "ok" or "unavailable"
    unavailable_reason: str | None  # "no_fundamentals" or "only_fundamentals"
    score: Decimal | None
    version: str
    missing: tuple[str, ...]
    components: tuple[Component, ...]


def _quantize(value: Decimal, places: Decimal) -> Decimal:
    return value.quantize(places, rounding=ROUND_HALF_EVEN)


def _decimal(value: object, what: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{what} must be a Decimal, not {type(value).__name__}")
    return value


def normalize_sentiment(value: Decimal) -> Decimal:
    """A sentiment in [-1, 1] on 0-100: (value + 1) x 50, to 0.1 (DECISIONS rule 1)."""
    value = _decimal(value, "sentiment")
    if not -1 <= value <= 1:
        raise ValueError("sentiment must be between -1 and 1")
    return _quantize((value + 1) * 50, SCORE_PLACES)


def blend(
    values: Mapping[str, Decimal | None], weights: ScoreWeights = ScoreWeights()
) -> ScoreResult:
    """The score from each component's 0-100 value; absent or None is missing.

    Sufficient only with fundamentals and at least one other component (S2-01
    AC1/AC4). A missing component's weight is spread over the present ones
    (AC3), and absence is never treated as neutral (DECISIONS rule 4).
    """
    unknown = sorted(set(values) - set(COMPONENTS))
    if unknown:
        raise ValueError(f"unknown score components: {', '.join(unknown)}")
    present: dict[str, Decimal] = {}
    for name in COMPONENTS:
        raw = values.get(name)
        if raw is None:
            continue
        value = _decimal(raw, name)
        if not ZERO <= value <= HUNDRED:
            raise ValueError(f"{name} must be between 0 and 100")
        present[name] = value
    missing = tuple(c for c in COMPONENTS if c not in present)

    reason = None
    if "fundamentals" not in present:
        reason = "no_fundamentals"
    elif len(present) == 1:
        reason = "only_fundamentals"
    if reason is not None:
        return ScoreResult(
            status="unavailable",
            unavailable_reason=reason,
            score=None,
            version=weights.version,
            missing=missing,
            components=tuple(
                Component(
                    name=c,
                    status="ok" if c in present else "unavailable",
                    value=present.get(c),
                    nominal_weight=weights.nominal(c),
                    weight=None,
                    contribution=None,
                )
                for c in COMPONENTS
            ),
        )

    total = sum((weights.nominal(c) for c in present), ZERO)
    components = []
    for c in COMPONENTS:
        if c in present:
            weight = _quantize(weights.nominal(c) / total, WEIGHT_PLACES)
            contribution = _quantize(present[c] * weight, CONTRIBUTION_PLACES)
            components.append(
                Component(c, "ok", present[c], weights.nominal(c), weight, contribution)
            )
        else:
            components.append(
                Component(c, "unavailable", None, weights.nominal(c), ZERO, ZERO)
            )
    score = _quantize(
        sum((p.contribution or ZERO for p in components), ZERO), SCORE_PLACES
    )
    return ScoreResult(
        status="ok",
        unavailable_reason=None,
        score=score,
        version=weights.version,
        missing=missing,
        components=tuple(components),
    )


def breakdown(row: Mapping[str, Any]) -> list[Component]:
    """The components of a stored company_scores row, recomputed from its values.

    The row's own weights and contributions must agree with the recomputation,
    so what is shown is always what score-v1 gives, never an echo of the row.
    An unavailable score has no breakdown.
    """
    weights = VERSIONS.get(str(row.get("score_version")))
    if weights is None:
        raise ValueError("unknown score version")
    if row.get("status") != "ok":
        return []
    result = blend({c: row.get(f"{c}_value") for c in COMPONENTS}, weights)
    if result.status != "ok" or result.score != row.get("score"):
        raise ValueError("stored score does not match its components")
    for c in result.components:
        stored = {
            "nominal_weight": row.get(f"{c.name}_nominal_weight"),
            "weight": row.get(f"{c.name}_weight"),
            "contribution": row.get(f"{c.name}_contribution"),
        }
        computed = {
            "nominal_weight": c.nominal_weight,
            "weight": c.weight,
            "contribution": c.contribution,
        }
        for key, expected in computed.items():
            found = stored[key]
            if c.status == "unavailable" and key != "nominal_weight" and found is None:
                continue  # a missing component may store no weight at all
            if found is None or Decimal(found) != expected:
                raise ValueError(
                    f"stored {c.name} {key.replace('_', ' ')} does not match"
                )
    if list(row.get("missing") or []) != list(result.missing):
        raise ValueError("stored missing components do not match")
    return list(result.components)
