"""explain-v1: why Perennial recommends a company, output/sprint-2/s2-stories-plan.md §3.3.

Pure functions over stored rows (company_scores, fundamental_scores,
ticker_sentiment as plain mappings): no network, no database, no model.

A claim is {order, text, component, measure, record: {table, id, batch_id},
as_of, value}: every sentence cites the stored record and value it came from
(S2-03 AC2). Candidates are the fundamentals measures the fundamentals score
uses, by points descending (ties in MEASURES order), then sentiment; the first
three are kept (AC1). A component the score lists as missing produces no claim
(AC3). Claims are generated only for a recommended score.

Decision 108: ticker_sentiment is #85's table plus model_version. A sentiment
claim names what was counted (news articles, social posts or both), its window
is the ticker_sentiment batch envelope's window_days, which the caller passes,
and its as_of is last_at's UTC date. A row with a NULL value had nothing to
score: it is missing sentiment, so a score that cites one is refused.

The input digest is SHA-256 over the canonical JSON of the score row, the
records it cites and, when sentiment is cited, the window. An equal digest under the same template returns the stored
explanation unchanged (AC4), so the wording a user saw doesn't drift.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
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


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}{'' if count == 1 else 's'}"


def _sentiment_claim(row: Mapping[str, Any], window_days: int | None) -> dict[str, Any]:
    value: Decimal | None = row.get("value")
    last_at: datetime | None = row.get("last_at")
    if value is None or last_at is None:
        raise ValueError("the cited sentiment row has no value: nothing was scored")
    if last_at.tzinfo is None or last_at.utcoffset() is None:
        raise ValueError("the cited sentiment row's last_at has no time zone")
    # Exactly an int of at least 1: not a bool, and nothing is coerced.
    if type(window_days) is not int or window_days < 1:
        raise ValueError(
            "a sentiment claim needs the batch's window_days, a positive int"
        )
    news, social = int(row["news_count"]), int(row["social_count"])
    if news and social:
        subject = "News and social posts"
        counted = f"{_plural(news, 'article')}, {_plural(social, 'post')}"
    elif social:
        subject, counted = "Social posts", _plural(social, "post")
    elif news:
        subject, counted = "News coverage", _plural(news, "article")
    else:
        raise ValueError("the cited sentiment row counts no articles or posts")
    days = _plural(window_days, "day")
    if value > 0:
        lean = "leaned positive"
    elif value < 0:
        lean = "leaned negative"
    else:
        lean = "was neutral"
    return {
        "text": f"{subject} over the last {days} {lean} ({counted}).",
        "component": "sentiment",
        "measure": "value",
        "record": {
            "table": "ticker_sentiment",
            "id": row["id"],
            "batch_id": row["batch_id"],
        },
        "as_of": last_at.astimezone(timezone.utc).date().isoformat(),
        "value": str(value),
    }


def claims_for(
    score_row: Mapping[str, Any],
    fundamentals_row: Mapping[str, Any] | None,
    sentiment_row: Mapping[str, Any] | None,
    *,
    window_days: int | None = None,
) -> list[dict[str, Any]]:
    """Up to three claims, in order, from the records the score cites."""
    candidates: list[dict[str, Any]] = []
    fundamentals = _cited(score_row, "fundamentals", fundamentals_row)
    if fundamentals is not None:
        candidates.extend(_fundamentals_claims(fundamentals))
    sentiment = _cited(score_row, "sentiment", sentiment_row)
    if sentiment is not None:
        candidates.append(_sentiment_claim(sentiment, window_days))
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
    *,
    window_days: int | None = None,
) -> str:
    """SHA-256 (hex) of the score row, the whole records it cites and, when it cites
    sentiment, the window, as canonical JSON. A record the score lists as missing is
    left out, so an empty sentiment row digests the same as none."""
    missing = _missing(score_row)
    sentiment = None if "sentiment" in missing else sentiment_row
    payload: dict[str, Any] = {
        "score": dict(score_row),
        "fundamentals": None
        if "fundamentals" in missing or fundamentals_row is None
        else dict(fundamentals_row),
        "sentiment": None if sentiment is None else dict(sentiment),
    }
    if sentiment is not None:
        payload["window_days"] = window_days
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
    *,
    window_days: int | None = None,
) -> Explanation:
    """The explanation for one score; `previous` is its last stored explanation."""
    recommended = score_row.get("status") == "ok" and score_row.get("recommended")
    claims: Sequence[dict[str, Any]] = ()
    if recommended:
        claims = claims_for(
            score_row, fundamentals_row, sentiment_row, window_days=window_days
        )
    digest = input_digest(
        score_row, fundamentals_row, sentiment_row, window_days=window_days
    )
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
