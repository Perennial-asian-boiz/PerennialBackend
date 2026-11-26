"""The dashboard shows the licensed close (S3-01, decision 118 extended to
GET /api/watchlist; proposed in DECISIONS.md, pending Bryan). Synthetic rows only.

Pinned:
- A row's price and change_percent are its security's newest daily_prices row
  from a licensed provider: the same close and change the Price Breakdown
  serves (price.latest_closes reuses price.served's body), with its source,
  trading day and stale flag beside them.
- A provider missing from licensed_price_providers.json is never shown: a newer
  unlicensed row doesn't replace an older licensed one, and with only unlicensed
  rows the price is null and its number appears nowhere in the body. With no
  licences at all, every price is null.
- Without a licensed row the price and change are null: the record's own price,
  change_percent and ARK share price are still never served (decision 95,
  test_dashboard_no_price.py).
- An old close is still shown, with price_stale true.
- The prices take one statement whatever the number of companies.
"""

import pytest
from pr22_support import pr22_roles  # noqa: F401  (pytest fixture)
from test_dashboard_sector import (
    companies,
    dashboard,
    publish,
    record,
    seed,
    statements,
)
from test_read_api_price import FRIDAY, clock, stored

QUOTE = {
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
NONE = {
    "price": None,
    "change_percent": None,
    "price_source": None,
    "price_as_of": None,
    "price_stale": False,
}


@pytest.fixture(autouse=True)
def saturday(monkeypatch):
    clock(monkeypatch, "2026-11-14T15:00:00+00:00")


def price_fields(company):
    return {key: company[key] for key in NONE}


# ── company_from_record ──────────────────────────────────


def test_the_lookup_close_is_the_price_and_the_record_price_never_is():
    from src.api.dashboard import Display, company_from_record

    priced = record("ZZQA", price=999.0, change_percent=9.9)
    shown = company_from_record(
        priced, "popular_stable", {"ZZQA": Display("Q", "Energy", QUOTE)}
    )
    assert price_fields(shown.model_dump()) == {
        "price": 182.41,
        "change_percent": -1.175642,
        "price_source": "Finnhub",
        "price_as_of": FRIDAY,
        "price_stale": False,
    }
    for display in (None, {"ZZQA": Display("Q", "Energy")}, {}):
        company = company_from_record(priced, "popular_stable", display)
        assert price_fields(company.model_dump()) == NONE, display


# ── Through the route, as the provisioned API login ──────


def serve(engine, roles, monkeypatch, tickers, **records):
    for ticker in tickers:
        seed(engine, ticker, name=f"Synthetic {ticker}", sector="Energy")
    rows = [record(t, **records.get(t, {})) for t in tickers]
    publish(engine, rows[:1], rows[1:])
    with dashboard(engine, roles, monkeypatch) as (_, client):
        response = client.get("/api/watchlist")
    assert response.status_code == 200, response.text
    return response, companies(response.json())


@pytest.mark.db
def test_the_latest_licensed_close_is_shown(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    for ticker in ("ZZLA", "ZZLB"):
        seed(engine, ticker, name=f"Synthetic {ticker}", sector="Energy")
    stored(engine, "ZZLA", close="100.00", trading_date="2026-11-12")
    stored(engine, "ZZLA")  # the newer batch wins
    stored(engine, "ZZLB", close="50.00", previous_close=None)
    publish(engine, [record("ZZLA")], [record("ZZLB")])
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        served = companies(client.get("/api/watchlist").json())
    assert price_fields(served["ZZLA"]) == {
        "price": 182.41,
        "change_percent": -1.175642,
        "price_source": "Finnhub",
        "price_as_of": FRIDAY,
        "price_stale": False,
    }
    assert served["ZZLB"]["price"] == 50.0
    assert served["ZZLB"]["change_percent"] is None  # no previous close
    assert served["ZZLA"]["sector"] == "Energy"  # the rest is unchanged


@pytest.mark.db
def test_an_unlicensed_price_is_never_shown(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    seed(engine, "ZZUA", name="Synthetic ZZUA", sector="Energy")
    seed(engine, "ZZUB", name="Synthetic ZZUB", sector="Energy")
    stored(engine, "ZZUA")
    stored(engine, "ZZUA", provider="fmp", close="4321.987654")  # newer, unlicensed
    stored(engine, "ZZUB", provider="fmp", close="98.765432")
    publish(engine, [record("ZZUA")], [record("ZZUB")])
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        response = client.get("/api/watchlist")
    served = companies(response.json())
    assert served["ZZUA"]["price"] == 182.41
    assert price_fields(served["ZZUB"]) == NONE
    for number in ("4321.98", "98.765"):
        assert number not in response.text


@pytest.mark.db
def test_with_no_licences_no_price_is_shown(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    from src.api import price

    seed(engine, "ZZNL", name="Synthetic ZZNL", sector="Energy")
    stored(engine, "ZZNL")
    monkeypatch.setattr(price, "LICENSED", {})
    publish(engine, [record("ZZNL")], [])
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        served = companies(client.get("/api/watchlist").json())
    assert price_fields(served["ZZNL"]) == NONE


@pytest.mark.db
def test_no_licensed_row_leaves_the_price_empty_and_the_record_price_unserved(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    response, served = serve(
        engine,
        pr22_roles,
        monkeypatch,
        ["ZZMA", "ZZMB"],
        ZZMA={"price": 987.25, "change_percent": -7.25},
        ZZMB={"signals": {"ark": {"share_price": 12.75}, "congress": None}},
    )
    assert price_fields(served["ZZMA"]) == NONE
    assert price_fields(served["ZZMB"]) == NONE
    for number in ("987.25", "-7.25", "12.75"):
        assert number not in response.text


@pytest.mark.db
def test_an_old_close_is_shown_and_marked_stale(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    seed(engine, "ZZST", name="Synthetic ZZST", sector="Energy")
    stored(engine, "ZZST", trading_date="2026-11-06")  # 8 days before Saturday
    publish(engine, [record("ZZST")], [])
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        served = companies(client.get("/api/watchlist").json())
    assert served["ZZST"]["price"] == 182.41
    assert served["ZZST"]["price_as_of"] == "2026-11-06"
    assert served["ZZST"]["price_stale"] is True


@pytest.mark.db
def test_the_prices_take_one_statement_whatever_the_size(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    def count(n):
        from sqlalchemy import text

        with engine.begin() as conn:
            conn.execute(text("TRUNCATE perennial.current_publication"))
        tickers = [f"ZZC{i}X{n}" for i in range(n)]
        for t in tickers:
            seed(engine, t, name=None, sector="Energy")
            stored(engine, t)
        records = [record(t) for t in tickers]
        publish(engine, records[: n // 2], records[n // 2 :])
        with statements(app) as seen:
            response = client.get("/api/watchlist")
        assert response.status_code == 200, response.text
        prices = [c["price"] for c in companies(response.json()).values()]
        assert prices == [182.41] * n
        return len(seen)

    with dashboard(engine, pr22_roles, monkeypatch) as (app, client):
        assert count(2) == count(12), "the prices must not take one query per ticker"
