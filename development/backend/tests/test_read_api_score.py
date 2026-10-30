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
import re
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
        **{
            "fundamentals_record_id": 812,
            "sentiment_record_id": 77,
            "sentiment_input_version": "weights-v1",
            **over,
        }
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
        "input_version": "weights-v1",  # decision 108: #85's weights_version
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


# ── accepted contract choices (a)-(d), pinned for the frontend (#77) ─


@pytest.mark.parametrize("pointer", ["none", "elsewhere"])
@pytest.mark.db
def test_a_no_row_score_is_exactly_not_scored(engine, api, pointer):  # noqa: F811
    """(a) No row in the current scores batch: reason not_scored, nothing invented."""
    seed_security(engine, "ZZCN")
    if pointer == "elsewhere":
        scored(engine, "ZZCO", recommended())
    body = get(api, "ZZCN").json()
    assert body["score"] == {
        "status": "unavailable",
        "reason": "not_scored",
        "value": None,
        "score_version": None,
        "as_of": None,
        "recommended": False,
        "components": [],
        "missing": [],
    }
    assert body["explanation"] == {
        "status": "unavailable",
        "reason": "not_scored",
        "claims": [],
    }


@pytest.mark.db
def test_an_explanation_not_written_yet_is_not_explained_yet(engine, api):  # noqa: F811
    """(b) The score is promoted before derive_explanations writes its row."""
    scored(engine, "ZZCW", recommended(), explanation=None)
    assert get(api, "ZZCW").json()["explanation"] == {
        "status": "unavailable",
        "reason": "not_explained_yet",
        "claims": [],
    }


REASONS = {
    # case: (score row, stored explanation or None, score reason, explanation reason)
    "ok_recommended": (lambda: recommended(), lambda: explanation_row(), None, None),
    "ok_not_recommended": (
        lambda: cited(),
        lambda: explanation_row(status="not_recommended", claims=[]),
        None,
        None,
    ),
    "ok_insufficient": (
        lambda: recommended(),
        lambda: explanation_row(
            status="unavailable", reason="insufficient_supporting_data", claims=[]
        ),
        None,
        "insufficient_supporting_data",
    ),
    "ok_not_explained_yet": (
        lambda: recommended(),
        lambda: None,
        None,
        "not_explained_yet",
    ),
    "unavailable_only_fundamentals": (
        lambda: unavailable_row(fundamentals_record_id=812),
        lambda: explanation_row(
            status="unavailable", reason="score_unavailable", claims=[]
        ),
        "only_fundamentals",
        "score_unavailable",
    ),
    "unavailable_no_fundamentals": (
        lambda: unavailable_row(
            unavailable_reason="no_fundamentals",
            missing=["fundamentals", "sentiment", "event_exposure"],
            fundamentals_value=None,
            fundamentals_input_as_of=None,
            fundamentals_input_version=None,
        ),
        lambda: explanation_row(
            status="unavailable", reason="score_unavailable", claims=[]
        ),
        "no_fundamentals",
        "score_unavailable",
    ),
}


@pytest.mark.parametrize("case", sorted(REASONS))
@pytest.mark.db
def test_reason_is_present_exactly_when_it_has_a_value(engine, api, case):  # noqa: F811
    """(c) No `"reason": null` anywhere: the key is there with a value, or absent."""
    row, stored, score_reason, explanation_reason = REASONS[case]
    scored(engine, "ZZCR", row(), stored() if stored() is not None else None)
    body = get(api, "ZZCR").json()
    for part, expected in (
        ("score", score_reason),
        ("explanation", explanation_reason),
    ):
        if expected is None:
            assert "reason" not in body[part], (case, part)
        else:
            assert body[part]["reason"] == expected, (case, part)


@pytest.mark.db
def test_a_row_that_breakdown_refuses_fails_closed(engine, groups):  # noqa: F811
    """(d) A stored score whose contribution disagrees with score-v1 is never served:
    not 200, the {detail, code} error shape, and nothing from the refused row or its
    explanation in the body. The status code is not pinned."""
    from fastapi.testclient import TestClient
    from src.api.read import create_app
    from test_read_api import login

    marker = "refused row claim"
    stored = explanation_row(
        claims=[dict(c, text=marker) for c in explanation_row()["claims"]]
    )
    scored(engine, "ZZCF", recommended(fundamentals_contribution=D("60.12")), stored)
    with login(engine, groups) as (api_engine, _, _password):
        with TestClient(
            create_app(api_engine, api_role=groups["api"]),
            raise_server_exceptions=False,
        ) as client:
            response = get(client, "ZZCF")
    assert response.status_code != 200
    body = response.json()
    assert set(body) == {"detail", "code"} and isinstance(body["code"], str)
    for leaked in ("60.12", "74.3", "78.0", "65.5", marker, "ZZCF"):
        assert leaked not in response.text, leaked


# ── checker F1 on c3c1fee, and the explicit API column lists ─


def _no_raise_client(engine, groups):  # noqa: F811
    """A client served by the API login that returns server errors instead of raising."""
    from contextlib import contextmanager

    from fastapi.testclient import TestClient
    from src.api.read import create_app
    from test_read_api import login

    @contextmanager
    def opened():
        with login(engine, groups) as (api_engine, _, _password):
            with TestClient(
                create_app(api_engine, api_role=groups["api"]),
                raise_server_exceptions=False,
            ) as client:
                yield client

    return opened()


MALFORMED = {
    "no_record": lambda c: {k: v for k, v in c.items() if k != "record"},
    "record_not_an_object": lambda c: {**c, "record": "fundamental_scores:812"},
    "record_without_id": lambda c: {
        **c,
        "record": {"table": "fundamental_scores", "batch_id": 5},
    },
    "no_text": lambda c: {k: v for k, v in c.items() if k != "text"},
    "order_not_a_number": lambda c: {**c, "order": "first"},
    "not_an_object": lambda c: "a bare string claim",
}


@pytest.mark.parametrize("case", sorted(MALFORMED))
@pytest.mark.db
def test_a_malformed_stored_claim_fails_closed(engine, groups, case):  # noqa: F811
    """Checker F1: a stored claim that isn't Claim-shaped is never served and never a
    plain-text 500: the same fail-closed JSON as (d), with no claim text or stored value."""
    marker = "malformed claim text"
    claims = [dict(c, text=marker) for c in explanation_row()["claims"]]
    claims[0] = MALFORMED[case](claims[0])
    scored(engine, "ZZCM", recommended(), explanation_row(claims=claims))
    with _no_raise_client(engine, groups) as client:
        response = get(client, "ZZCM")
    assert response.status_code != 200
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert set(body) == {"detail", "code"} and isinstance(body["code"], str)
    for leaked in (
        marker,
        "a bare string claim",
        "fundamental_scores:812",
        "74.3",
        "55.06",
        "ZZCM",
    ):
        assert leaked not in response.text, leaked


def _selected(statement, table):
    """The columns of `table` in a statement's select list (before its FROM)."""
    head = re.split(r"\bFROM\b", statement, maxsplit=1)[0]
    return set(re.findall(rf"\bperennial\.{table}\.(\w+)", head))


@pytest.mark.db
def test_the_route_selects_exactly_the_granted_api_columns(engine, groups):  # noqa: F811
    """Checker remark: the route reads exactly provision_roles.API_COLUMNS for both score
    tables, not "every model column but one", so a column added to the model later
    without an API grant can't break it or be read."""
    from fastapi.testclient import TestClient
    from sqlalchemy import event
    from src.api.read import create_app
    from test_read_api import login
    from test_scores_schema import _provision_roles

    granted = _provision_roles().API_COLUMNS
    scored(engine, "ZZCC", recommended())
    statements = []
    with login(engine, groups) as (api_engine, _, _password):

        @event.listens_for(api_engine, "before_cursor_execute")
        def capture(conn, cursor, statement, *rest):
            statements.append(statement)

        with TestClient(create_app(api_engine, api_role=groups["api"])) as client:
            statements.clear()  # only the request's own reads
            assert get(client, "ZZCC").status_code == 200
        event.remove(api_engine, "before_cursor_execute", capture)
    for table in ("company_scores", "score_explanations"):
        reads = [
            st for st in statements if re.search(rf"\bFROM perennial\.{table}\b", st)
        ]
        assert reads, table
        for statement in reads:
            # company_scores serves every granted column; explanations need fewer.
            assert _selected(statement, table) <= set(granted[table]), (
                table,
                statement,
            )


@pytest.mark.parametrize("table", ["company_scores", "score_explanations"])
@pytest.mark.db
def test_an_ungranted_model_column_is_never_read(engine, api, monkeypatch, table):  # noqa: F811
    """Checker remark, the stronger form: give the model one more column, as a later
    migration would, with no API grant. A route that reads "every column but one"
    selects it and fails; one that reads the API_COLUMNS list still answers 200."""
    from sqlalchemy import Column, Integer, MetaData
    from src.db import models

    scored(engine, "ZZCC", recommended())
    before = get(api, "ZZCC")
    assert before.status_code == 200
    widened = getattr(models, table).to_metadata(MetaData())
    widened.append_column(Column("zz_ungranted_probe", Integer))
    monkeypatch.setattr(models, table, widened)
    after = get(api, "ZZCC")
    assert after.status_code == 200, after.text
    assert after.json() == before.json()
