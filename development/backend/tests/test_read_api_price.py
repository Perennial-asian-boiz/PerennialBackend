"""S3-01: GET /v1/securities/{symbol}/price. Synthetic rows only.

Pinned:
* AC 1, 2: the newest licensed daily_prices row is served: its close, the change
  in dollars and percent against its previous close, the 52-week high and low
  with their dates, the provider's name as source and the trading day as as_of.
  Served as the perennial_api login, reading only the granted columns.
* AC 3: a provider missing from licensed_price_providers.json is never served:
  its rows are left out before anything is computed, so a newer unlicensed row
  doesn't replace an older licensed one, and with only unlicensed rows the body
  is exactly the no-price body. The committed list names each provider's terms
  and approver; a malformed list is refused at startup.
* AC 4: no licensed price is 200 with status "unavailable" and every value null.
  404 unknown_security only for an unknown ticker; 422 for an invalid one.
* stale is true once the trading day is more than 7 days old.
* Cache-Control: no-cache, an ETag that changes with the row, the licences and
  stale, and 304 on If-None-Match.
"""

import json

import pytest
from sqlalchemy import text
from src.api import market_status, price
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
from test_scores_schema import _batch, _insert

FRIDAY = "2026-11-13"
EXPECTED = {
    "status": "ok",
    "source": "Finnhub",
    "as_of": FRIDAY,
    "currency": "USD",
    "close": 182.41,
    "previous_close": 184.58,
    "change": -2.17,
    "change_percent": -1.175642,
    "high_52w": 207.52,
    "high_52w_date": "2026-02-18",
    "low_52w": 63.4,
    "low_52w_date": "2025-11-21",
    "stale": False,
}
UNAVAILABLE = {
    "status": "unavailable",
    "source": None,
    "as_of": None,
    "currency": None,
    "close": None,
    "previous_close": None,
    "change": None,
    "change_percent": None,
    "high_52w": None,
    "high_52w_date": None,
    "low_52w": None,
    "low_52w_date": None,
    "stale": False,
}


@pytest.fixture(autouse=True)
def saturday(monkeypatch):
    clock(monkeypatch, "2026-11-14T15:00:00+00:00")


def clock(monkeypatch, at):
    from datetime import datetime

    monkeypatch.setattr(market_status, "now", lambda: datetime.fromisoformat(at))


def stored(engine, symbol, **changes):
    """A security (created once) and a new daily_prices batch holding one row."""
    with engine.connect() as conn:
        security_id = conn.execute(
            text("SELECT id FROM perennial.securities WHERE symbol = :s"), {"s": symbol}
        ).scalar_one_or_none()
    if security_id is None:
        security_id = seed_security(engine, symbol)
    row = {
        "trading_date": FRIDAY,
        "quoted_at": "2026-11-13T21:00:00+00:00",
        "close": "182.41",
        "previous_close": "184.58",
        "high_52w": "207.52",
        "high_52w_date": "2026-02-18",
        "low_52w": "63.40",
        "low_52w_date": "2025-11-21",
        "currency": "USD",
        "provider": "finnhub",
        **changes,
    }
    with engine.begin() as conn:
        batch = _batch(conn, "daily_prices")
        _insert(conn, "daily_prices", row, batch_id=batch, security_id=security_id)
    return batch


def get(client, symbol, **headers):
    return client.get(f"/v1/securities/{symbol}/price", headers=headers)


def body(symbol, served):
    return {"api_version": "v1", "symbol": symbol, **served}


# ── AC 1, 2: the latest licensed close ───────────────────


@pytest.mark.db
def test_the_latest_close_change_range_source_and_date_are_served(engine, api):  # noqa: F811
    stored(engine, "ZZPA")
    response = get(api, "ZZPA")
    assert response.status_code == 200
    assert response.json() == body("ZZPA", EXPECTED)
    assert response.text == json.dumps(
        response.json(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


@pytest.mark.db
def test_the_newest_batch_is_served(engine, api):  # noqa: F811
    stored(engine, "ZZPA")
    stored(engine, "ZZPA", trading_date="2026-11-16", close="190", previous_close="182.41")
    served = get(api, "ZZPA").json()
    assert (served["as_of"], served["close"], served["change"]) == ("2026-11-16", 190.0, 7.59)


@pytest.mark.db
def test_without_a_previous_close_there_is_no_change(engine, api):  # noqa: F811
    stored(engine, "ZZPA", previous_close=None)
    served = get(api, "ZZPA").json()
    assert served["close"] == 182.41 and served["change"] is None and served["change_percent"] is None


@pytest.mark.db
def test_without_a_range_the_close_is_still_served(engine, api):  # noqa: F811
    stored(engine, "ZZPA", high_52w=None, high_52w_date=None, low_52w=None, low_52w_date=None)
    served = get(api, "ZZPA").json()
    assert served["status"] == "ok" and served["high_52w"] is None and served["low_52w"] is None


@pytest.mark.db
def test_another_securitys_price_is_never_served(engine, api):  # noqa: F811
    stored(engine, "ZZPA")
    stored(engine, "ZZPB", close="999")
    assert get(api, "ZZPA").json()["close"] == 182.41


# ── AC 3: unlicensed prices are never served ─────────────


@pytest.mark.db
def test_an_unlicensed_price_is_served_as_no_price_at_all(engine, api):  # noqa: F811
    stored(engine, "ZZPF", provider="fmp", close="987654.321")
    seed_security(engine, "ZZPN")
    unlicensed, none = get(api, "ZZPF"), get(api, "ZZPN")
    assert unlicensed.json() == body("ZZPF", UNAVAILABLE)
    assert unlicensed.headers["etag"] == none.headers["etag"]
    assert "987654" not in unlicensed.text and "fmp" not in unlicensed.text


@pytest.mark.db
def test_a_newer_unlicensed_row_never_replaces_a_licensed_one(engine, api):  # noqa: F811
    stored(engine, "ZZPA")
    stored(engine, "ZZPA", provider="fmp", close="987654.321", previous_close="1")
    response = get(api, "ZZPA")
    assert response.json() == body("ZZPA", EXPECTED)
    assert "987654" not in response.text


@pytest.mark.db
def test_removing_a_licence_stops_serving_its_prices(engine, api, monkeypatch):  # noqa: F811
    stored(engine, "ZZPA")
    etag = get(api, "ZZPA").headers["etag"]
    monkeypatch.setattr(price, "LICENSED", {})
    response = get(api, "ZZPA", **{"If-None-Match": etag})
    assert response.status_code == 200 and response.json() == body("ZZPA", UNAVAILABLE)


def test_the_committed_licences_name_finnhub_its_terms_and_approver():
    data = json.loads(price.LICENCE_FILE.read_text())
    (entry,) = data["providers"]
    assert entry["provider"] == "finnhub" and entry["terms"].startswith("https://finnhub.io/")
    assert entry["approved_by"] and entry["approved_on"]
    assert price.LICENSED == {"finnhub": "Finnhub"}


GOOD = {"provider": "finnhub", "name": "Finnhub", "terms": "https://finnhub.io/terms-of-service",
        "scope": "quote", "approved_by": "Bryan Tieu", "approved_on": "2026-11-16"}


@pytest.mark.parametrize(
    "data",
    [
        [],
        {"providers": "finnhub"},
        {"providers": [{k: v for k, v in GOOD.items() if k != "approved_by"}]},
        {"providers": [{**GOOD, "approved_by": " "}]},
        {"providers": [{**GOOD, "terms": "http://finnhub.io/terms"}]},
        {"providers": [{**GOOD, "provider": "Finnhub"}]},
        {"providers": [{**GOOD, "approved_on": "16 Nov"}]},
        {"providers": [GOOD, GOOD]},
        {"providers": [{**GOOD, "extra": "x"}]},
    ],
)
def test_a_malformed_licence_list_is_refused(data):
    with pytest.raises(ValueError):
        price.parse_licences(data)


def test_an_empty_licence_list_is_valid_and_serves_nothing():
    assert price.parse_licences({"providers": []}) == {}


# ── AC 4 and errors ──────────────────────────────────────


@pytest.mark.db
def test_no_price_is_200_unavailable_not_404(engine, api):  # noqa: F811
    seed_security(engine, "ZZPN")
    response = get(api, "ZZPN")
    assert response.status_code == 200 and response.json() == body("ZZPN", UNAVAILABLE)


@pytest.mark.db
def test_an_unknown_symbol_is_404_unknown_security(engine, api):  # noqa: F811
    assert_error(get(api, "ZZNONE"), 404, "unknown_security")


@pytest.mark.db
@pytest.mark.parametrize("bad", ["%00", "A" * 1000, "ZZ%0AAA"])
def test_an_invalid_symbol_is_422_and_never_echoed(engine, api, bad):  # noqa: F811
    response = get(api, bad)
    assert_error(response, 422, "invalid_request")
    assert "A" * 1000 not in response.text


# ── stale ────────────────────────────────────────────────


@pytest.mark.db
def test_a_close_more_than_seven_days_old_is_stale(engine, api, monkeypatch):  # noqa: F811
    stored(engine, "ZZPA")
    clock(monkeypatch, "2026-11-20T23:00:00+00:00")  # Friday + 7, 18:00 New York
    fresh = get(api, "ZZPA")
    assert fresh.json()["stale"] is False
    clock(monkeypatch, "2026-11-21T15:00:00+00:00")  # Friday + 8
    old = get(api, "ZZPA")
    assert old.json()["stale"] is True and old.json()["close"] == 182.41
    assert old.headers["etag"] != fresh.headers["etag"]


@pytest.mark.db
def test_the_day_is_new_yorks_not_utcs(engine, api, monkeypatch):  # noqa: F811
    stored(engine, "ZZPA")
    clock(monkeypatch, "2026-11-21T03:00:00+00:00")  # still Friday + 7 in New York
    assert get(api, "ZZPA").json()["stale"] is False


# ── ETag, 304 and Cache-Control ──────────────────────────


@pytest.mark.db
def test_etag_and_304(engine, api):  # noqa: F811
    stored(engine, "ZZPA")
    first = get(api, "ZZPA")
    etag = first.headers["etag"]
    assert etag.startswith('"') and etag.endswith('"')
    assert first.headers["cache-control"] == "no-cache"
    for header in (etag, f"W/{etag}", f'"other", {etag}'):
        cached = get(api, "ZZPA", **{"If-None-Match": header})
        assert cached.status_code == 304, header
        assert cached.headers["etag"] == etag and cached.content == b""
    assert get(api, "ZZPA", **{"If-None-Match": '"other"'}).status_code == 200


@pytest.mark.db
def test_the_etag_changes_with_a_new_price(engine, api):  # noqa: F811
    stored(engine, "ZZPA")
    etag = get(api, "ZZPA").headers["etag"]
    stored(engine, "ZZPA", close="183")
    fresh = get(api, "ZZPA", **{"If-None-Match": etag})
    assert fresh.status_code == 200 and fresh.headers["etag"] != etag


# ── the route and OpenAPI ────────────────────────────────


def test_the_route_is_a_documented_read_route():
    operation = _v1_openapi(_router_app())["paths"]["/v1/securities/{symbol}/price"]["get"]
    assert {"200", "304", "404", "422", "429"} <= set(operation["responses"])
    assert "PriceResponse" in _v1_openapi(_router_app())["schemas"]


def test_the_snapshot_includes_the_price_route():
    """Regenerated in the same PR (UPDATE_API_SNAPSHOT=1)."""
    assert "/v1/securities/{symbol}/price" in json.loads(SNAPSHOT.read_text())["paths"]


def test_api_md_documents_the_price_route():
    content = (REPO / "docs" / "technical-specs" / "API.md").read_text()
    for term in (
        "/v1/securities/{symbol}/price",
        "licensed_price_providers.json",
        "change_percent",
        "high_52w",
        "stale",
        '"status": "unavailable"',
    ):
        assert term in content, term
