"""explain-v1: why Perennial recommends a company, output/sprint-2/s2-stories-plan.md §3.3.

Pure functions over stored rows (company_scores, fundamental_scores,
ticker_sentiment as plain mappings): no network, no database, no model.

A claim is {order, text, component, measure, record: {table, id, batch_id},
as_of, value}: every sentence cites the stored record and value it came from
(S2-03 AC2). Candidates are the fundamentals measures the fundamentals score
uses, by points descending (ties in MEASURES order), then sentiment; the first
three are kept (AC1). A component the score lists as missing produces no claim
(AC3). Claims are generated only for a recommended score.

The input digest is SHA-256 over the canonical JSON of the score row and the
records it cites. An equal digest under the same template returns the stored
explanation unchanged (AC4), so the wording a user saw doesn't drift.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.services.fundamentals.scoring import MEASURES, breakdown, key_reasons

TEMPLATE_VERSION = "explain-v1"
MAX_CLAIMS = 3


@dataclass(frozen=True)
class Explanation:
    status: str  # "ok", "not_recommended" or "unavailable"
    reason: str | None  # "score_unavailable" or "insufficient_supporting_data"
    template_version: str
    claims: tuple[dict[str, Any], ...]
    input_digest: str


def _missing(score_row: Mapping[str, Any]) -> set[str]:
    return set(score_row.get("missing") or ())


def _cited(
    score_row: Mapping[str, Any],
    component: str,
    row: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    """The component's record, if the score used it; refuses one it didn't cite."""
    if component in _missing(score_row):
        return None
    if row is None or row.get("id") != score_row.get(f"{component}_record_id"):
        raise ValueError(f"the {component} row is not the record the score cites")
    return row


def _fundamentals_claims(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    parts = breakdown(row)
    ordered = sorted(parts, key=lambda k: (-parts[k]["points"], MEASURES.index(k)))
    claims = []
    for measure in ordered:
        only = {
            "model": row.get("model"),
            "profit_source": row.get("profit_source"),
            measure: row[measure],
        }
        (text,) = key_reasons(only)
        claims.append(
            {
                "text": text,
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
        )
    return claims


def _sentiment_claim(row: Mapping[str, Any]) -> dict[str, Any]:
    value: Decimal = row["value"]
    count = int(row["article_count"])
    start: date = row["window_start"]
    end: date = row["window_end"]
    days = (end - start).days + 1
    articles = f"{count} article{'' if count == 1 else 's'}"
    if value > 0:
        lean = "leaned positive"
    elif value < 0:
        lean = "leaned negative"
    else:
        lean = "was neutral"
    return {
        "text": f"News coverage over the last {days} days {lean} ({articles}).",
        "component": "sentiment",
        "measure": "value",
        "record": {
            "table": "ticker_sentiment",
            "id": row["id"],
            "batch_id": row["batch_id"],
        },
        "as_of": end.isoformat(),
        "value": str(value),
    }


def claims_for(
    score_row: Mapping[str, Any],
    fundamentals_row: Mapping[str, Any] | None,
    sentiment_row: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """Up to three claims, in order, from the records the score cites."""
    candidates: list[dict[str, Any]] = []
    fundamentals = _cited(score_row, "fundamentals", fundamentals_row)
    if fundamentals is not None:
        candidates.extend(_fundamentals_claims(fundamentals))
    sentiment = _cited(score_row, "sentiment", sentiment_row)
    if sentiment is not None:
        candidates.append(_sentiment_claim(sentiment))
    return [
        {"order": order, **claim}
        for order, claim in enumerate(candidates[:MAX_CLAIMS], start=1)
    ]


def _json_default(value: object) -> str:
    if isinstance(value, (Decimal, date)):
        return value.isoformat() if isinstance(value, date) else str(value)
    raise TypeError(f"cannot digest {type(value).__name__}")


def input_digest(
    score_row: Mapping[str, Any],
    fundamentals_row: Mapping[str, Any] | None,
    sentiment_row: Mapping[str, Any] | None,
) -> str:
    """SHA-256 (hex) of the score row and the records it cites, as canonical JSON."""
    missing = _missing(score_row)
    payload = {
        "score": dict(score_row),
        "fundamentals": None
        if "fundamentals" in missing or fundamentals_row is None
        else dict(fundamentals_row),
        "sentiment": None
        if "sentiment" in missing or sentiment_row is None
        else dict(sentiment_row),
    }
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def explain(
    score_row: Mapping[str, Any],
    fundamentals_row: Mapping[str, Any] | None,
    sentiment_row: Mapping[str, Any] | None,
    previous: Mapping[str, Any] | None = None,
) -> Explanation:
    """The explanation for one score; `previous` is its last stored explanation."""
    recommended = score_row.get("status") == "ok" and score_row.get("recommended")
    claims: Sequence[dict[str, Any]] = ()
    if recommended:
        claims = claims_for(score_row, fundamentals_row, sentiment_row)
    digest = input_digest(score_row, fundamentals_row, sentiment_row)
    if (
        previous is not None
        and previous.get("input_digest") == digest
        and previous.get("template_version") == TEMPLATE_VERSION
    ):
        return Explanation(
            status=previous["status"],
            reason=previous.get("reason"),
            template_version=TEMPLATE_VERSION,
            claims=tuple(previous.get("claims") or ()),
            input_digest=digest,
        )
    if score_row.get("status") != "ok":
        status, reason = "unavailable", "score_unavailable"
    elif not recommended:
        status, reason = "not_recommended", None
    elif not claims:
        status, reason = "unavailable", "insufficient_supporting_data"
    else:
        status, reason = "ok", None
    return Explanation(
        status=status,
        reason=reason,
        template_version=TEMPLATE_VERSION,
        claims=tuple(claims),
        input_digest=digest,
    )
