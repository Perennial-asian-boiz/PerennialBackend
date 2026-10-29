"""Sprint 2 TEST-4: GET /v1/securities/{symbol}/score (API-1..API-5). Synthetic rows only.

Spec: output/sprint-2/s2-stories-plan.md §3.4 and §5 (c); S2-01 AC1/AC4, S2-02
AC1-AC4, S2-03 AC1-AC5. Tables as migration 0018 (test_scores_schema.py, P1-P8).

Pinned:
* The read bucket, Cache-Control: no-cache, an ETag and 304 on If-None-Match.
* 404 unknown_security and 422 invalid_request, as /v1/securities/{symbol}.
* 200 with score.status "unavailable" when the security has no scored row.
* Served as the perennial_api login, through current_scores['production'] ->
  company_scores -> score_explanations (explicit columns only: the API can't
  read watchlist_publication_id or input_digest).
* as_of is current_scores.promoted_at (P8), for the score and the explanation.
* §3.4's shapes. Numbers are JSON numbers. Claims are served through the typed
  Claim model: the stored keys of TEST-3's claim shape, and nothing else.
* The OpenAPI snapshot and API.md cover the route (API-4); /v1/securities/{symbol}
  and its schemas stay exactly as they were.

The ETag must change when the pointer moves and when the explanation for the
served score is written (derive_explanations runs after derive_scores promotes),
so a client holding an old ETag is never told 304 over a newer body.
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import text
from test_read_api import (  # noqa: F401
    SNAPSHOT,
    _router_app,
    _v1_openapi,
    api,
    groups,
    seed_security,
)
from test_scores_schema import (
    _batch,
    _insert,
    _publication,
    explanation_row,
    score_row,
    unavailable_row,
)

FROZEN = Path(__file__).parent / "fixtures" / "api" / "securities_symbol_v1.json"
PROMOTED = datetime(2026, 11, 5, 17, 2, 11, tzinfo=timezone.utc)
NOTE = "weight redistributed to the remaining components"
D = Decimal


def scored(
    engine,
    symbol,
    row,
    explanation="ok",
    *,
    batch=None,
    promote=True,
    promoted_at=PROMOTED,
    alias=(),
):
    """A security with one company_scores row (and, unless None, its explanation),
    in `batch` (a new company_scores batch by default), with the pointer on it."""
    with engine.connect() as conn:
        security_id = conn.execute(
            text("SELECT id FROM perennial.securities WHERE symbol = :s"), {"s": symbol}
        ).scalar_one_or_none()
    if security_id is None:
        security_id = seed_security(engine, symbol, *alias)
    with engine.begin() as conn:
        if batch is None:
            batch = _batch(conn, "company_scores")
        if row.get("watchlist_publication_id") == "publication":
            row = {**row, "watchlist_publication_id": _publication(conn)}
        score_id = _insert(
            conn, "company_scores", row, batch_id=batch, security_id=security_id
        )
        if explanation is not None:
            stored = explanation if isinstance(explanation, dict) else explanation_row()
            _insert(
                conn,
                "score_explanations",
                {"company_score_id": score_id, **stored},
                batch_id=_batch(conn, "score_explanations"),
                security_id=security_id,
            )
        if promote:
            point(conn, batch, promoted_at)
    return batch, score_id


def point(conn, batch, promoted_at=PROMOTED):
    conn.execute(
        text(
            "INSERT INTO perennial.current_scores (channel, batch_id, promoted_at) VALUES ('production', :b, :t)"
            " ON CONFLICT (channel) DO UPDATE SET batch_id = :b, promoted_at = :t"
        ),
        {"b": batch, "t": promoted_at},
    )


def cited(**over):
    """An ok score row citing its fundamentals and sentiment records, as the job stores it."""
    return score_row(
        **{"fundamentals_record_id": 812, "sentiment_record_id": 77, **over}
    )


def recommended(**over):
    return cited(recommended=True, watchlist_publication_id="publication", **over)


def as_of(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def get(client, symbol, **headers):
    return client.get(f"/v1/securities/{symbol}/score", headers=headers)


FULL_COMPONENTS = [
    {
        "name": "fundamentals",
        "status": "ok",
        "value": 78.0,
        "nominal_weight": 0.6,
        "weight": 0.7059,
        "contribution": 55.06,
        "input_as_of": "2025-06-30",
        "input_version": "fundamentals-v1",
        "record": {"table": "fundamental_scores", "id": 812},
    },
    {
        "name": "sentiment",
        "status": "ok",
        "value": 65.5,
        "nominal_weight": 0.25,
        "weight": 0.2941,
        "contribution": 19.26,
        "input_as_of": "2026-11-04",
        "input_version": "sentiment-rollup-v1",
        "record": {"table": "ticker_sentiment", "id": 77},
    },
    {
        "name": "event_exposure",
        "status": "unavailable",
        "value": None,
        "nominal_weight": 0.15,
        "weight": 0.0,
        "contribution": 0.0,
        "input_as_of": None,
        "input_version": None,
        "record": None,
        "note": NOTE,
    },
]


# ── 200, the full score (01-AC1, 02-AC1..4, 03-AC1/2) ─────


@pytest.mark.db
def test_a_full_score_with_its_breakdown_and_explanation(engine, api):  # noqa: F811
    scored(engine, "ZZSA", recommended())
    response = get(api, "ZZSA")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert set(body) == {"api_version", "symbol", "score", "explanation"}
    assert (body["api_version"], body["symbol"]) == ("v1", "ZZSA")
    score = body["score"]
    assert set(score) == {
        "status",
        "value",
        "score_version",
        "as_of",
        "recommended",
        "components",
        "missing",
    }
    assert (
        score["status"],
        score["value"],
        score["score_version"],
        score["recommended"],
    ) == (
        "ok",
        74.3,
        "score-v1",
        True,
    )
    assert as_of(score["as_of"]) == PROMOTED
    assert score["missing"] == ["event_exposure"]
    assert score["components"] == FULL_COMPONENTS
    explanation = body["explanation"]
    assert set(explanation) == {"status", "template_version", "as_of", "claims"}
    assert (explanation["status"], explanation["template_version"]) == (
        "ok",
        "explain-v1",
    )
    assert as_of(explanation["as_of"]) == PROMOTED
    assert explanation["claims"] == explanation_row()["claims"]  # served as stored


@pytest.mark.db
def test_unknown_claim_keys_are_never_served(engine, api):  # noqa: F811
    """Auditor (API-2): claims are untyped JSONB, so they are served through the typed
    Claim model and only its keys reach a client, at the top level and in record."""
    clean = explanation_row()["claims"]
    dirty = [
        {
            **c,
            "internal_note": "never served",
            "record": {**c["record"], "debug": "never served"},
        }
        for c in clean
    ]
    scored(engine, "ZZSK", recommended(), explanation_row(claims=dirty))
    response = get(api, "ZZSK")
    assert response.json()["explanation"]["claims"] == clean
    assert "never served" not in response.text


@pytest.mark.db
def test_the_contributions_add_up_to_the_score(engine, api):  # noqa: F811
    """02-AC2: within 0.5; score-v1 makes it exact to the rounding of the sum."""
    scored(engine, "ZZSA", recommended())
    score = get(api, "ZZSA").json()["score"]
    total = sum(D(str(c["contribution"])) for c in score["components"])
    assert abs(total - D(str(score["value"]))) <= D("0.05")


@pytest.mark.db
def test_numbers_are_json_numbers_and_the_body_is_canonical(engine, api):  # noqa: F811
    scored(engine, "ZZSA", recommended())
    response = get(api, "ZZSA")
    raw = response.content.decode()
    assert '"value":74.3' in raw and '"weight":0.7059' in raw
    assert raw == json.dumps(
        response.json(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


@pytest.mark.db
def test_an_alias_finds_the_canonical_security(engine, api):  # noqa: F811
    scored(engine, "BRK.B", recommended(), alias=("BRK-B",))
    response = get(api, "BRK-B")
    assert response.status_code == 200
    assert response.json()["symbol"] == "BRK.B"
    assert response.json()["score"]["value"] == 74.3


@pytest.mark.db
def test_nothing_the_api_may_not_read_is_served(engine, api):  # noqa: F811
    scored(engine, "ZZSA", recommended())
    raw = get(api, "ZZSA").text
    for hidden in ("input_digest", "watchlist_publication_id", "abab"):
        assert hidden not in raw, hidden


# ── 200, unavailable (01-AC4, 02-AC4, 03-AC5) ────────────


@pytest.mark.db
def test_a_stored_unavailable_score(engine, api):  # noqa: F811
    """§3.4's unavailable example: fundamentals only."""
    stored = explanation_row(
        status="unavailable", reason="score_unavailable", claims=[]
    )
    scored(engine, "ZZSU", unavailable_row(fundamentals_record_id=812), stored)
    response = get(api, "ZZSU")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-cache"
    body = response.json()
    score = dict(body["score"])
    assert as_of(score.pop("as_of")) == PROMOTED
    assert score == {
        "status": "unavailable",
        "reason": "only_fundamentals",
        "value": None,
        "score_version": "score-v1",
        "recommended": False,
        "components": [],
        "missing": ["sentiment", "event_exposure"],
    }
    assert body["explanation"]["status"] == "unavailable"
    assert body["explanation"]["reason"] == "score_unavailable"
    assert body["explanation"]["claims"] == []


@pytest.mark.parametrize("pointer", ["none", "elsewhere"])
@pytest.mark.db
def test_no_scored_row_is_200_unavailable_not_404(engine, api, pointer):  # noqa: F811
    seed_security(engine, "ZZSN")
    if pointer == "elsewhere":
        scored(engine, "ZZSO", recommended())  # the pointer's batch has no ZZSN row
    response = get(api, "ZZSN")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-cache"
    body = response.json()
    assert body["symbol"] == "ZZSN"
    score, explanation = body["score"], body["explanation"]
    assert (
        score["status"],
        score["value"],
        score["recommended"],
        score["components"],
    ) == (
        "unavailable",
        None,
        False,
        [],
    )
    assert (explanation["status"], explanation["claims"]) == ("unavailable", [])


@pytest.mark.db
def test_a_score_that_is_not_recommended_has_no_claims(engine, api):  # noqa: F811
    stored = explanation_row(status="not_recommended", claims=[])
    scored(engine, "ZZSR", cited(), stored)
    explanation = get(api, "ZZSR").json()["explanation"]
    assert (explanation["status"], explanation["claims"]) == ("not_recommended", [])


@pytest.mark.db
def test_insufficient_supporting_data_is_said_so(engine, api):  # noqa: F811
    stored = explanation_row(
        status="unavailable", reason="insufficient_supporting_data", claims=[]
    )
    scored(engine, "ZZSI", recommended(), stored)
    explanation = get(api, "ZZSI").json()["explanation"]
    assert (explanation["status"], explanation["reason"], explanation["claims"]) == (
        "unavailable",
        "insufficient_supporting_data",
        [],
    )


@pytest.mark.db
def test_a_score_whose_explanation_is_not_written_yet(engine, api):  # noqa: F811
    """derive_explanations runs after derive_scores promotes: the score is served,
    and the explanation is unavailable rather than invented."""
    scored(engine, "ZZSW", recommended(), explanation=None)
    body = get(api, "ZZSW").json()
    assert body["score"]["value"] == 74.3
    assert (body["explanation"]["status"], body["explanation"]["claims"]) == (
        "unavailable",
        [],
    )


# ── 404 and 422 ──────────────────────────────────────────


@pytest.mark.db
def test_an_unknown_symbol_is_404_unknown_security(engine, api):  # noqa: F811
    response = get(api, "ZZNOPE")
    assert response.status_code == 404
    assert response.json()["code"] == "unknown_security"


@pytest.mark.db
@pytest.mark.parametrize("bad", ["%00", "A" * 1000, "ZZ%0AAA"])
def test_an_invalid_symbol_is_422_and_never_echoed(engine, api, bad):  # noqa: F811
    response = api.get(f"/v1/securities/{bad}/score")
    assert response.status_code == 422
    assert response.json()["code"] == "invalid_request"
    assert "AAAAAAAAAA" not in response.text


# ── the pointer decides what is served ───────────────────


@pytest.mark.db
def test_the_pointer_not_the_newest_batch_is_served(engine, api):  # noqa: F811
    """Rollback is re-pointing current_scores (DOC-3)."""
    old, _ = scored(engine, "ZZSP", recommended())
    newer, _ = scored(
        engine,
        "ZZSP",
        cited(
            score=D("74.4"),
            fundamentals_value=D("78.1"),
            fundamentals_contribution=D("55.13"),
        ),
        promote=False,
    )
    assert get(api, "ZZSP").json()["score"]["value"] == 74.3
    with engine.begin() as conn:
        point(conn, newer, datetime(2026, 11, 6, tzinfo=timezone.utc))
    body = get(api, "ZZSP").json()
    assert body["score"]["value"] == 74.4
    assert as_of(body["score"]["as_of"]) == datetime(2026, 11, 6, tzinfo=timezone.utc)


# ── ETag, 304 and Cache-Control ──────────────────────────


@pytest.mark.db
def test_etag_and_304(engine, api):  # noqa: F811
    scored(engine, "ZZSE", recommended())
    first = get(api, "ZZSE")
    etag = first.headers["etag"]
    assert etag.startswith('"') and etag.endswith('"')
    assert first.headers["cache-control"] == "no-cache"
    assert get(api, "ZZSE").headers["etag"] == etag
    for header in (etag, f"W/{etag}", f'"other", {etag}'):
        cached = get(api, "ZZSE", **{"If-None-Match": header})
        assert cached.status_code == 304, header
        assert (
            cached.headers["etag"] == etag
            and cached.headers["cache-control"] == "no-cache"
        )
        assert cached.content == b""
    assert get(api, "ZZSE", **{"If-None-Match": '"other"'}).status_code == 200


@pytest.mark.db
def test_the_etag_changes_when_the_pointer_moves(engine, api):  # noqa: F811
    old, _ = scored(engine, "ZZSE", recommended())
    etag = get(api, "ZZSE").headers["etag"]
    newer, _ = scored(engine, "ZZSE", recommended(), promote=False)
    with engine.begin() as conn:
        point(conn, newer)
    fresh = get(api, "ZZSE", **{"If-None-Match": etag})
    assert fresh.status_code == 200
    assert fresh.headers["etag"] != etag


@pytest.mark.db
def test_the_etag_changes_when_the_explanation_is_written(engine, api):  # noqa: F811
    batch, score_id = scored(engine, "ZZSX", recommended(), explanation=None)
    before = get(api, "ZZSX")
    assert before.json()["explanation"]["status"] == "unavailable"
    with engine.begin() as conn:
        security_id = conn.execute(
            text("SELECT id FROM perennial.securities WHERE symbol = 'ZZSX'")
        ).scalar_one()
        _insert(
            conn,
            "score_explanations",
            {"company_score_id": score_id, **explanation_row()},
            batch_id=_batch(conn, "score_explanations"),
            security_id=security_id,
        )
    after = get(api, "ZZSX", **{"If-None-Match": before.headers["etag"]})
    assert after.status_code == 200
    assert after.json()["explanation"]["status"] == "ok"
    assert after.headers["etag"] != before.headers["etag"]


# ── API-3/API-4: the route, OpenAPI and docs ─────────────


def test_the_route_is_a_documented_read_route():
    paths = _v1_openapi(_router_app())["paths"]
    operation = paths["/v1/securities/{symbol}/score"]["get"]
    assert {"200", "304", "404", "422", "429"} <= set(operation["responses"])


def test_the_response_schemas_are_named():
    schemas = _v1_openapi(_router_app())["schemas"]
    for name in ("ScoreResponse", "Score", "ScoreComponent", "Claim", "Explanation"):
        assert name in schemas, name


def test_the_existing_securities_route_is_unchanged():
    """The plan's decision 4: a separate route, so /v1/securities/{symbol} stays as it is."""
    frozen = json.loads(FROZEN.read_text())
    current = _v1_openapi(_router_app())
    assert current["paths"]["/v1/securities/{symbol}"] == frozen["path"]
    assert {n: current["schemas"][n] for n in frozen["schemas"]} == frozen["schemas"]


def test_the_snapshot_includes_the_score_route():
    """API-4: regenerated in the same PR (UPDATE_API_SNAPSHOT=1)."""
    assert "/v1/securities/{symbol}/score" in json.loads(SNAPSHOT.read_text())["paths"]


def test_api_md_documents_the_score_route():
    from test_read_api import REPO

    content = (REPO / "docs" / "technical-specs" / "API.md").read_text()
    for term in (
        "/v1/securities/{symbol}/score",
        "score_version",
        "nominal_weight",
        "contribution",
        "input_as_of",
        "missing",
        "recommended",
        "template_version",
        "not_recommended",
        "insufficient_supporting_data",
        "score_unavailable",
        "only_fundamentals",
        "no_fundamentals",
        NOTE,
    ):
        assert term in content, term
