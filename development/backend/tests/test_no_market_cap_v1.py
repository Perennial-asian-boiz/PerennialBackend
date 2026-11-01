"""No market cap on /v1 (decision 112 A1), spec-first, after #84's share price
(test_no_price_v1.py).

Every published record carries a top-level market_cap (ranking.py), and
GET /v1/watchlist and GET /v1/securities/{symbol} serve the records as stored.
Staging's current publication carries FMP market caps, which have no display
licence (decision 95). So at serve time both /v1 paths drop market_cap from
every record, in every bucket, whatever its value (null included). Everything
else is served as stored: buckets and their order, warnings (with
market_cap_unavailable), the envelope's market_caps_unavailable count and
every other field. The stored publication isn't changed, and bodies stay
canonical JSON. Plus: no stored market cap value appears in any API body.

Unchanged and not tested here: the ranking and what it publishes (v6 is Hong's),
and the ETag, which still names the publication (as #84 left it).
"""

import json

import pytest
from pr22_support import pr22_roles  # noqa: F401  (pytest fixture)
from sqlalchemy import select
from src.db import models
from test_dashboard_sector import dashboard, record
from test_degraded_publishing import Feed, run
from test_no_price_v1 import (
    canonical,
    keys,
    publish_all,
    stored_output,
    without_market_cap,
)
from test_read_api import seed_security

BUCKETS = ("popular_stable", "affordable_growing", "unresolved")
# Distinctive: no other field holds them, as numbers or as text.
CAPS = (987654321012, "45678901234.5", 3210987654)
TICKERS = ("ZZMA", "ZZMB", "ZZMC", "ZZMD", "ZZME")


def capped_publication(engine):
    """A stored publication whose records carry market caps, in all three buckets."""
    popular = [
        record("ZZMA", bucket="popular_stable", rank=1, market_cap=CAPS[0]),
        record(
            "ZZMB",
            bucket="popular_stable",
            rank=2,
            market_cap=CAPS[1],
            signals={
                "ark": {"funds": ["ARKK"], "fund_count": 1, "total_weight": 2.5},
                "congress": None,
            },
        ),
    ]
    affordable = [
        record("ZZMC", bucket="affordable_growing", rank=1, market_cap=CAPS[2])
    ]
    unresolved = [
        record(
            "ZZMD",
            bucket="unresolved",
            rank=None,
            market_cap=None,
            warnings=["market_cap_unavailable"],
        ),
        record(
            "ZZME", bucket="unresolved", rank=None, warnings=["market_cap_unavailable"]
        ),
    ]
    for r in (*popular, *affordable, *unresolved):
        seed_security(
            engine,
            r["ticker"],
            name=f"Synthetic {r['ticker']}",
            profile={"sector": "Energy"},
        )
    output = publish_all(engine, affordable, popular, unresolved)
    assert "market_cap" in set(keys(stored_output(engine, current_id(engine))))
    return output


def current_id(engine):
    with engine.connect() as conn:
        return conn.execute(
            select(models.current_publication.c.publication_id)
        ).scalar_one()


def assert_no_cap(body_text, where):
    for cap in CAPS:
        assert str(cap) not in body_text, (where, cap)


@pytest.mark.db
def test_v1_watchlist_drops_market_cap_from_every_record(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    output = capped_publication(engine)
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        response = client.get("/v1/watchlist")
    assert response.status_code == 200, response.text
    assert response.content == canonical(response.content)
    body = response.json()
    watchlist = body["watchlist"]
    assert "market_cap" not in set(keys(watchlist))
    assert_no_cap(response.text, "/v1/watchlist")
    # Everything else is served as stored: buckets, order, warnings, other fields.
    assert watchlist == without_market_cap(output)
    assert [r["ticker"] for r in watchlist["popular_stable"]] == ["ZZMA", "ZZMB"]
    assert [r["warnings"] for r in watchlist["unresolved"]] == [
        ["market_cap_unavailable"],
        ["market_cap_unavailable"],
    ]
    assert body["publication"]["market_caps_unavailable"] == 2


@pytest.mark.db
@pytest.mark.parametrize("ticker", TICKERS)
def test_v1_securities_drops_market_cap_from_the_entry(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
    ticker,
):
    output = capped_publication(engine)
    stored = next(r for b in BUCKETS for r in output[b] if r["ticker"] == ticker)
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        response = client.get(f"/v1/securities/{ticker}")
    assert response.status_code == 200, response.text
    assert response.content == canonical(response.content)
    body = response.json()
    assert body["current"]["entry"] == without_market_cap(stored)
    assert "market_cap" not in set(keys(body))
    assert body["current"]["publication"]["market_caps_unavailable"] == 2
    assert_no_cap(response.text, ticker)


@pytest.mark.db
def test_serving_leaves_the_stored_publication_unchanged(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    output = capped_publication(engine)
    pid = current_id(engine)
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        client.get("/v1/watchlist")
        client.get("/v1/securities/ZZMA")
    assert stored_output(engine, pid) == output  # still has market_cap


@pytest.mark.db
def test_no_api_response_body_contains_a_stored_market_cap(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    capped_publication(engine)
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        # Every GET route the app documents (included routers aren't in app.routes).
        documented = client.get("/openapi.json").json()["paths"]
        paths = sorted(
            path for path, ops in documented.items() if "get" in ops and "{" not in path
        )
        paths += ["/openapi.json", "/docs", "/redoc"]
        paths += [f"/v1/securities/{t}" for t in TICKERS]
        paths += [f"/v1/securities/{t}/score" for t in TICKERS]
        assert "/api/watchlist" in paths and "/v1/watchlist" in paths
        responses = {path: client.get(path) for path in paths}
    for path, response in responses.items():
        assert_no_cap(response.text, path)
        if response.headers.get("content-type", "").startswith("application/json"):
            assert "market_cap" not in set(keys(response.json())), path


@pytest.mark.db
def test_a_pipeline_publication_is_served_without_its_market_caps(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
    fixture_path,
):
    """A publication as publish() makes it today: its records carry market_cap."""
    pid = run(engine, Feed(fixture_path))
    stored = stored_output(engine, pid)
    assert "market_cap" in set(keys(stored)), "precondition: the ranking publishes it"
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        response = client.get("/v1/watchlist")
        tickers = [r["ticker"] for b in BUCKETS for r in stored[b]]
        entries = {t: client.get(f"/v1/securities/{t}").json() for t in tickers}
    body = response.json()
    assert body["publication"]["id"] == pid
    assert body["watchlist"] == without_market_cap(stored)
    assert "market_cap" not in set(keys(body))
    assert tickers and all("market_cap" not in set(keys(e)) for e in entries.values())


@pytest.mark.db
def test_the_watchlist_etag_still_names_the_publication(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    capped_publication(engine)
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        response = client.get("/v1/watchlist")
        again = client.get(
            "/v1/watchlist", headers={"If-None-Match": response.headers["etag"]}
        )
    assert response.headers["etag"] == f'"v1-{current_id(engine)}"'
    assert again.status_code == 304


def test_the_openapi_description_says_market_cap_is_not_served():
    from fastapi import FastAPI
    from src.api.read import router

    app = FastAPI()
    app.include_router(router)
    watchlist = app.openapi()["components"]["schemas"]["WatchlistResponse"]
    description = watchlist["properties"]["watchlist"]["description"]
    # Today: "publications.output, verbatim except ARK's share price".
    assert "market cap" in description.lower().replace("_", " "), description
