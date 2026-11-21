"""S3-02 AC2: GET /v1/securities/{symbol}/sentiment/trend. Synthetic rows only.

Spec: docs/technical-specs/SENTIMENT_PULSE.md §13.

Pinned:
* 30 points, one per UTC day, oldest first, ending today (sentiment.today()).
* Each point is the value of that day's stored ticker_sentiment row (tallied_on),
  to 4 places; a day tallied more than once uses its newest batch.
* A day with no row, or a row that counted nothing (value NULL), is null: a gap,
  never zero. Rows outside the window, and other securities' rows, are ignored.
* 404 unknown_security only for an unknown ticker, 422 for an invalid one; a known
  security with nothing tallied is 200 with 30 nulls. no-cache, ETag, 304.
* Served as perennial_api with its column grants (ticker_sentiment.value is granted).
"""

import json
from datetime import date, timedelta

import pytest
from test_read_api import (  # noqa: F401
    REPO,
    SNAPSHOT,
    _router_app,
    _v1_openapi,
    api,
    assert_error,
    groups,
    seed_security,
)
from test_read_api_sentiment import _security_id
from test_scores_schema import _batch, _insert

from src.api import sentiment

END = date(2026, 11, 19)
START = END - timedelta(days=29)


@pytest.fixture(autouse=True)
def fixed_today(monkeypatch):
    monkeypatch.setattr(sentiment, "today", lambda: END)


def tally(engine, symbol, day, value, *, batch=None):
    """A ticker_sentiment row for `symbol` tallied on `day` with `value` (None: nothing
    counted), in a new (or the given) batch. Returns the batch."""
    security_id = _security_id(engine, symbol)
    counted = value is not None
    with engine.begin() as conn:
        batch = batch or _batch(conn, "ticker_sentiment")
        _insert(
            conn,
            "ticker_sentiment",
            {
                "row_number": 1,
                "value": value,
                "plain_average": value,
                "article_count": 3 if counted else 0,
                "news_count": 3 if counted else 0,
                "social_count": 0,
                "first_at": f"{day.isoformat()}T00:00:00+00:00" if counted else None,
                "last_at": f"{day.isoformat()}T12:00:00+00:00" if counted else None,
                "weights_version": "weights-v4",
                "model_version": "finbert" if counted else None,
                "social_sentiment": None,
                "tallied_on": day,
            },
            batch_id=batch,
            security_id=security_id,
        )
    return batch


def get(client, symbol, **headers):
    return client.get(f"/v1/securities/{symbol}/sentiment/trend", headers=headers)


def values(body):
    return {p["date"]: p["value"] for p in body["points"]}


# ── the window ───────────────────────────────────────────


@pytest.mark.db
def test_thirty_days_ending_today_oldest_first(engine, api):  # noqa: F811
    tally(engine, "ZZTA", END, "0.3200")
    tally(engine, "ZZTA", START, "-0.1500")
    response = get(api, "ZZTA")
    assert response.status_code == 200
    body = response.json()
    assert body["api_version"] == "v1" and body["symbol"] == "ZZTA"
    assert (body["days"], body["start"], body["end"]) == (30, "2026-10-21", "2026-11-19")
    dates = [p["date"] for p in body["points"]]
    assert dates == [(START + timedelta(days=i)).isoformat() for i in range(30)]
    assert body["points"][0] == {"date": "2026-10-21", "value": -0.15}
    assert body["points"][-1] == {"date": "2026-11-19", "value": 0.32}


@pytest.mark.db
def test_days_without_a_reading_are_null_not_zero(engine, api):  # noqa: F811
    tally(engine, "ZZTG", END - timedelta(days=2), "0.2500")
    tally(engine, "ZZTG", END - timedelta(days=1), None)  # tallied, nothing scored
    tally(engine, "ZZTG", END, "0.0000")  # a real neutral reading
    shown = values(get(api, "ZZTG").json())
    assert shown["2026-11-17"] == 0.25
    assert shown["2026-11-18"] is None
    assert shown["2026-11-19"] == 0.0
    assert sum(v is None for v in shown.values()) == 28


@pytest.mark.db
def test_a_day_tallied_twice_uses_its_newest_batch(engine, api):  # noqa: F811
    tally(engine, "ZZTN", END, "0.4000")
    tally(engine, "ZZTN", END, "-0.2000")
    assert values(get(api, "ZZTN").json())["2026-11-19"] == -0.2


@pytest.mark.db
def test_rows_outside_the_window_and_other_securities_are_ignored(engine, api):  # noqa: F811
    tally(engine, "ZZTW", START - timedelta(days=1), "0.9000")
    tally(engine, "ZZTW", END + timedelta(days=1), "0.9000")
    tally(engine, "ZZTO", END - timedelta(days=3), "0.7000")
    body = get(api, "ZZTW").json()
    assert all(p["value"] is None for p in body["points"])


@pytest.mark.db
def test_values_keep_four_places_and_the_full_range(engine, api):  # noqa: F811
    tally(engine, "ZZTR", END - timedelta(days=1), "-1.0000")
    tally(engine, "ZZTR", END, "0.1234")
    shown = values(get(api, "ZZTR").json())
    assert shown["2026-11-18"] == -1.0 and shown["2026-11-19"] == 0.1234


@pytest.mark.db
def test_nothing_tallied_is_200_with_thirty_gaps(engine, api):  # noqa: F811
    seed_security(engine, "ZZTU")
    response = get(api, "ZZTU")
    assert response.status_code == 200
    points = response.json()["points"]
    assert len(points) == 30 and all(p["value"] is None for p in points)


@pytest.mark.db
def test_nothing_the_api_may_not_read_is_served(engine, api):  # noqa: F811
    tally(engine, "ZZTX", END, "0.3000")
    body = get(api, "ZZTX").text
    for term in ("weights-v4", "finbert", "plain_average", "article_count"):
        assert term not in body, term


# ── errors, ETag and 304 ─────────────────────────────────


@pytest.mark.db
def test_an_unknown_symbol_is_404_unknown_security(engine, api):  # noqa: F811
    assert_error(get(api, "ZZNONE"), 404, "unknown_security")


@pytest.mark.db
@pytest.mark.parametrize("bad", ["%00", "A" * 1000, "ZZ%0AAA"])
def test_an_invalid_symbol_is_422_and_never_echoed(engine, api, bad):  # noqa: F811
    response = get(api, bad)
    assert_error(response, 422, "invalid_request")
    assert "A" * 1000 not in response.text


@pytest.mark.db
def test_etag_and_304(engine, api, monkeypatch):  # noqa: F811
    tally(engine, "ZZTE", END, "0.3000")
    first = get(api, "ZZTE")
    etag = first.headers["etag"]
    assert first.headers["cache-control"] == "no-cache"
    cached = get(api, "ZZTE", **{"If-None-Match": etag})
    assert cached.status_code == 304 and cached.content == b""
    tally(engine, "ZZTE", END, "0.1000")  # a newer tally
    assert get(api, "ZZTE", **{"If-None-Match": etag}).status_code == 200
    etag = get(api, "ZZTE").headers["etag"]
    monkeypatch.setattr(sentiment, "today", lambda: END + timedelta(days=1))  # a new day
    assert get(api, "ZZTE", **{"If-None-Match": etag}).status_code == 200


@pytest.mark.db
def test_the_body_is_canonical_json(engine, api):  # noqa: F811
    tally(engine, "ZZTJ", END, "0.3000")
    response = get(api, "ZZTJ")
    assert response.text == json.dumps(
        response.json(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


# ── the route, OpenAPI and docs ──────────────────────────


def test_today_is_the_current_utc_date(monkeypatch):
    monkeypatch.undo()  # the real sentiment.today, not the fixture's
    from datetime import datetime, timezone

    assert sentiment.today() == datetime.now(timezone.utc).date()


def test_the_route_is_a_documented_read_route():
    paths = _v1_openapi(_router_app())["paths"]
    operation = paths["/v1/securities/{symbol}/sentiment/trend"]["get"]
    assert {"200", "304", "404", "422", "429"} <= set(operation["responses"])


def test_the_response_schemas_are_named():
    schemas = _v1_openapi(_router_app())["schemas"]
    for name in ("SentimentTrendResponse", "TrendPoint"):
        assert name in schemas, name


def test_the_snapshot_includes_the_trend_route():
    """Regenerated in the same PR (UPDATE_API_SNAPSHOT=1)."""
    assert "/v1/securities/{symbol}/sentiment/trend" in json.loads(SNAPSHOT.read_text())["paths"]


def test_api_md_documents_the_trend_route():
    content = (REPO / "docs" / "technical-specs" / "API.md").read_text()
    for term in ("/v1/securities/{symbol}/sentiment/trend", "a gap, never zero", "newest batch"):
        assert term in content, term
