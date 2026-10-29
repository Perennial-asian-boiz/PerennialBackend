"""The dashboard shows no price (decision 95, L5 (a), typed by Bryan 2026-10-28),
spec-first.

The licensing map (docs/work-items/provider-licensing-map.md on
docs/licensing-map @ 84e795e) found no licence to display the price
GET /api/watchlist served: a record's "price", or else ARK's share_price
(src/api/dashboard.py company_from_record). L5 (a): don't show a price.

Pinned:
- DashboardCompanyResponse keeps its price field (nullable) so the frontend
  doesn't break, but every company's price is null: whatever the record or its
  ARK signal carries.
- Through the route, in publication mode as the provisioned API login, every
  served company has "price": null, and no stored price appears anywhere in the
  body. Every other field is unchanged (test_dashboard_sector covers them).
- Fixture mode (the labelled demo data, dashboard_fixture.py) serves null too,
  so the frontend sees the same contract in both modes. The coordinator asked
  whether the fixture should follow; this is the test-maker's recommendation,
  in its own test so it can be dropped on its own.
"""

from decimal import Decimal

import pytest
from pr22_support import pr22_roles  # noqa: F401  (pytest fixture)
from test_dashboard_sector import companies, dashboard, publish, record, seed

PRICED = [
    pytest.param({"price": 225.5}, id="record_price"),
    pytest.param({"price": Decimal("225.5")}, id="decimal_price"),
    pytest.param({"price": 0}, id="zero_price"),
    pytest.param(
        {"signals": {"ark": {"share_price": 12.5}, "congress": None}},
        id="ark_share_price",
    ),
    pytest.param(
        {"price": 225.5, "signals": {"ark": {"share_price": 12.5}, "congress": None}},
        id="both",
    ),
    pytest.param(
        {"price": None, "signals": {"ark": {"share_price": 12.5}, "congress": None}},
        id="null_price_with_ark",
    ),
]


@pytest.mark.parametrize("extra", PRICED)
@pytest.mark.parametrize("bucket", ["affordable_growing", "popular_stable"])
def test_no_record_or_signal_price_is_served(extra, bucket):
    from src.api.dashboard import company_from_record

    company = company_from_record(record("ZZPR", **extra), bucket)
    assert company.price is None
    assert "price" in company.model_dump()


def test_the_price_field_stays_in_the_contract_and_is_nullable():
    from src.api.dashboard import DashboardCompanyResponse

    schema = DashboardCompanyResponse.model_json_schema()
    assert "price" in schema["properties"]
    assert "price" in schema.get("required", [])  # the key is always sent
    price = schema["properties"]["price"]
    allowed = {option.get("type") for option in price.get("anyOf", [price])}
    assert {"number", "null"} <= allowed


@pytest.mark.db
def test_the_route_serves_a_null_price_for_every_company(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    affordable = [
        record("ZZPA", price=225.5),
        record("ZZPB", signals={"ark": {"share_price": 12.75}, "congress": None}),
    ]
    popular = [
        record("ZZPC", price=987.25, change_percent=-1.5, confidence_score=61),
        record("ZZPD"),
    ]
    for r in (*affordable, *popular):
        seed(engine, r["ticker"], name=f"Synthetic {r['ticker']}", sector="Energy")
    publish(engine, affordable, popular)
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        response = client.get("/api/watchlist")
    assert response.status_code == 200, response.text
    served = companies(response.json())
    assert set(served) == {"ZZPA", "ZZPB", "ZZPC", "ZZPD"}
    for ticker, company in served.items():
        assert "price" in company and company["price"] is None, ticker
    for number in ("225.5", "12.75", "987.25"):
        assert number not in response.text
    # Other fields still come through.
    assert served["ZZPC"]["change_percent"] == -1.5
    assert served["ZZPC"]["confidence_score"] == 61


def test_the_fixture_serves_no_price_either():
    from src.api.dashboard_fixture import temporary_dashboard_response

    response = temporary_dashboard_response()
    every = [*response.affordable_growing, *response.popular_stable]
    assert len(every) == 66
    assert all(company.price is None for company in every)
    assert all("price" in company.model_dump() for company in every)
