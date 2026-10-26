"""The dashboard's sector and name (staging demo, 2026-10-26), spec-first.

Staging published, but GET /api/watchlist in publication mode took sector and
name from the publication record, which carries neither. Every company was
"Unclassified", and the frontend filters its dashboard to its sector list
(AppShell.tsx: sectors.includes(company.sector)), so the dashboard was empty.

The rule pinned here (Bryan; vocabulary agreed with the coordinator 15:41Z):
1. sector comes from perennial.company_profiles.sector via
   securities.company_profile_id, mapped onto the frontend's eleven sectors
   (frontend data.ts `topics`). FMP's Financial Services, Consumer Cyclical,
   Consumer Defensive and Basic Materials are renamed; Technology, Healthcare
   and the other five pass through unchanged. A stored GICS "Information
   Technology" or "Health Care" maps to Technology or Healthcare so it stays
   visible. NULL, no securities row, no profile or any other name gives
   "Unclassified".
2. name = securities.name, falling back to the ticker. Never legal_name: the API
   login has no grant on it.
3. Only granted columns are selected: these tests run as a provisioned API
   login, and no provision_roles change is needed.
4. Unchanged: fixture mode, the no-publication response, bucket order and every
   other field. One lookup query inside the existing read transaction, not one
   per ticker.

Proposed name (adopt it, or send the tests back): src.api.dashboard.dashboard_sector,
the mapping function. company_from_record(record, bucket) must stay callable
with those two arguments (an optional third is fine): test 4 compares against it.
"""

from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
from pr22_support import integrated_app, pr22_roles  # noqa: F401  (pytest fixture)
from sqlalchemy import event, insert
from src.db import models

# The frontend's sector list (frontend data.ts `topics`, staging checkout db81648).
FRONTEND_SECTORS = (
    "Technology",
    "Financials",
    "Healthcare",
    "Industrials",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Utilities",
    "Materials",
    "Communication Services",
    "Real Estate",
)

MAPPING = [
    # FMP's names
    ("Technology", "Technology"),
    ("Healthcare", "Healthcare"),
    ("Financial Services", "Financials"),
    ("Consumer Cyclical", "Consumer Discretionary"),
    ("Consumer Defensive", "Consumer Staples"),
    ("Basic Materials", "Materials"),
    ("Communication Services", "Communication Services"),
    ("Energy", "Energy"),
    ("Industrials", "Industrials"),
    ("Real Estate", "Real Estate"),
    ("Utilities", "Utilities"),
    # GICS names already stored
    ("Information Technology", "Technology"),
    ("Health Care", "Healthcare"),
    ("Financials", "Financials"),
    ("Consumer Discretionary", "Consumer Discretionary"),
    ("Consumer Staples", "Consumer Staples"),
    ("Materials", "Materials"),
    # nothing usable
    (None, "Unclassified"),
    ("", "Unclassified"),
    ("N/A", "Unclassified"),
    ("Conglomerates", "Unclassified"),
    ("Shell Companies", "Unclassified"),
]


# ── The mapping alone ────────────────────────────────────


@pytest.mark.parametrize("stored,shown", MAPPING, ids=[repr(s) for s, _ in MAPPING])
def test_sector_mapping(stored, shown):
    from src.api.dashboard import dashboard_sector

    assert dashboard_sector(stored) == shown


def test_every_mapped_sector_is_one_the_frontend_shows_or_unclassified():
    from src.api.dashboard import dashboard_sector

    shown = {dashboard_sector(stored) for stored, _ in MAPPING}
    assert shown <= {*FRONTEND_SECTORS, "Unclassified"}
    assert set(FRONTEND_SECTORS) <= shown


# ── Through the route, as the provisioned API login ──────


def record(ticker, **extra):
    return {
        "ticker": ticker,
        "signals": {"congress": {"buy_count": 1}, "ark": None},
        "warnings": [],
        **extra,
    }


def seed(engine, symbol, *, name=None, sector="no profile", legal_name=None):
    """A security, with a profile unless sector is "no profile"."""
    with engine.begin() as conn:
        profile_id = None
        if sector != "no profile":
            profile_id = conn.execute(
                insert(models.company_profiles)
                .values(sector=sector, legal_name=legal_name)
                .returning(models.company_profiles.c.id)
            ).scalar_one()
        conn.execute(
            insert(models.securities).values(
                symbol=symbol, name=name, company_profile_id=profile_id
            )
        )


def share_profile(engine, symbol, with_symbol):
    """Point `symbol`'s security at `with_symbol`'s profile (share classes)."""
    from sqlalchemy import select, update

    with engine.begin() as conn:
        profile_id = conn.execute(
            select(models.securities.c.company_profile_id).where(
                models.securities.c.symbol == with_symbol
            )
        ).scalar_one()
        conn.execute(
            update(models.securities)
            .where(models.securities.c.symbol == symbol)
            .values(company_profile_id=profile_id)
        )


def publish(engine, affordable, popular):
    created = datetime(2026, 10, 26, 12, tzinfo=timezone.utc)
    output = {
        "schema_version": "1.0",
        "generated_at": created.isoformat(),
        "affordable_growing": affordable,
        "popular_stable": popular,
        "unresolved": [],
    }
    with engine.begin() as conn:
        pid = conn.execute(
            insert(models.publications)
            .values(
                created_at=created,
                ranking_version="synthetic",
                ranking_config={},
                output=output,
            )
            .returning(models.publications.c.id)
        ).scalar_one()
        conn.execute(
            insert(models.current_publication).values(
                channel="production", publication_id=pid, promoted_at=created
            )
        )
    return output


@contextmanager
def dashboard(engine, roles, monkeypatch, mode="publication"):
    from fastapi.testclient import TestClient

    env = {"DASHBOARD_DATA_MODE": mode}
    with integrated_app(engine, roles, monkeypatch, env=env) as (app, _, _):
        with TestClient(app) as client:
            yield app, client


@contextmanager
def statements(app):
    seen = []
    listener = lambda *a: seen.append(a[2])  # noqa: E731
    event.listen(app.state.engine, "before_cursor_execute", listener)
    try:
        yield seen
    finally:
        event.remove(app.state.engine, "before_cursor_execute", listener)


def companies(body):
    return {
        c["ticker"]: c
        for b in ("affordable_growing", "popular_stable")
        for c in body[b]
    }


@pytest.mark.db
def test_sector_and_name_come_from_the_security_and_its_profile(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    seed(engine, "ZZTEC", name="Synthetic Tech Inc", sector="Technology")
    seed(engine, "ZZFIN", name="Synthetic Bank Corp", sector="Financial Services")
    seed(engine, "ZZCYC", name="Synthetic Retail Co", sector="Consumer Cyclical")
    seed(engine, "ZZDEF", name="Synthetic Foods", sector="Consumer Defensive")
    seed(engine, "ZZMAT", name="Synthetic Mining", sector="Basic Materials")
    seed(engine, "ZZHLT", name="Synthetic Pharma", sector="Health Care")
    publish(
        engine,
        [record("ZZTEC"), record("ZZFIN"), record("ZZCYC")],
        [record("ZZDEF"), record("ZZMAT"), record("ZZHLT")],
    )
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        response = client.get("/api/watchlist")
    assert response.status_code == 200, response.text
    got = {t: (c["name"], c["sector"]) for t, c in companies(response.json()).items()}
    assert got == {
        "ZZTEC": ("Synthetic Tech Inc", "Technology"),
        "ZZFIN": ("Synthetic Bank Corp", "Financials"),
        "ZZCYC": ("Synthetic Retail Co", "Consumer Discretionary"),
        "ZZDEF": ("Synthetic Foods", "Consumer Staples"),
        "ZZMAT": ("Synthetic Mining", "Materials"),
        "ZZHLT": ("Synthetic Pharma", "Healthcare"),
    }


@pytest.mark.db
def test_missing_data_falls_back_to_the_ticker_and_unclassified(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    seed(engine, "ZZNONAME", name=None, sector="Energy")  # no name: the ticker
    seed(engine, "ZZNOPROF", name="Synthetic No Profile")  # no profile
    seed(engine, "ZZNULL", name="Synthetic Null Sector", sector=None)
    seed(engine, "ZZODD", name="Synthetic Odd Sector", sector="Conglomerates")
    # ZZGONE has no securities row at all.
    publish(
        engine,
        [record("ZZNONAME"), record("ZZNOPROF"), record("ZZGONE")],
        [record("ZZNULL"), record("ZZODD")],
    )
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        response = client.get("/api/watchlist")
    assert response.status_code == 200, response.text
    got = {t: (c["name"], c["sector"]) for t, c in companies(response.json()).items()}
    assert got == {
        "ZZNONAME": ("ZZNONAME", "Energy"),
        "ZZNOPROF": ("Synthetic No Profile", "Unclassified"),
        "ZZGONE": ("ZZGONE", "Unclassified"),
        "ZZNULL": ("Synthetic Null Sector", "Unclassified"),
        "ZZODD": ("Synthetic Odd Sector", "Unclassified"),
    }


@pytest.mark.db
def test_share_classes_on_one_profile_both_get_its_sector(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    seed(engine, "ZZA.A", name="Synthetic Holdings A", sector="Financial Services")
    seed(engine, "ZZA.B", name="Synthetic Holdings B")
    share_profile(engine, "ZZA.B", "ZZA.A")
    publish(engine, [], [record("ZZA.A"), record("ZZA.B")])
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        got = companies(client.get("/api/watchlist").json())
    assert {t: c["sector"] for t, c in got.items()} == {
        "ZZA.A": "Financials",
        "ZZA.B": "Financials",
    }


@pytest.mark.db
def test_legal_name_is_never_read_or_served(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    """The API login has no grant on legal_name: selecting it would be a 500."""
    seed(
        engine, "ZZLEGAL", name=None, sector="Utilities", legal_name="ZZ Legal Name LLC"
    )
    publish(engine, [record("ZZLEGAL")], [])
    with dashboard(engine, pr22_roles, monkeypatch) as (app, client):
        with statements(app) as seen:
            response = client.get("/api/watchlist")
    assert response.status_code == 200, response.text
    company = companies(response.json())["ZZLEGAL"]
    assert company["name"] == "ZZLEGAL"
    assert company["sector"] == "Utilities"
    assert "ZZ Legal Name LLC" not in response.text
    assert not [s for s in seen if "legal_name" in s]


@pytest.mark.db
def test_every_other_field_and_the_bucket_order_are_unchanged(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    from src.api.dashboard import company_from_record

    affordable = [
        record("ZZB2", signals={"ark": {"share_price": 12.5}, "congress": None}),
        record("ZZB1", warnings=["market_cap_unavailable"]),
        record("ZZB3", signals={"congress": None, "ark": None}, confidence_score=61),
    ]
    popular = [record("ZZP2", price=99.0, change_percent=-1.5), record("ZZP1")]
    for r in (*affordable, *popular):
        seed(engine, r["ticker"], name=f"Synthetic {r['ticker']}", sector="Energy")
    output = publish(engine, affordable, popular)
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        body = client.get("/api/watchlist").json()
    assert body["schema_version"] == "1.0"
    assert body["generated_at"] == output["generated_at"]
    for bucket, records in (
        ("affordable_growing", affordable),
        ("popular_stable", popular),
    ):
        assert [c["ticker"] for c in body[bucket]] == [r["ticker"] for r in records]
        for served, r in zip(body[bucket], records):
            expected = company_from_record(r, bucket).model_dump()
            expected.update(name=f"Synthetic {r['ticker']}", sector="Energy")
            assert served == expected, r["ticker"]


@pytest.mark.db
def test_one_lookup_inside_the_one_read_transaction_whatever_the_size(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    from src.api import read

    real, entered = read.read_transaction, []

    @contextmanager
    def counting(*args, **kwargs):
        entered.append(1)
        with real(*args, **kwargs) as conn:
            yield conn

    def count(n):
        from sqlalchemy import text

        with engine.begin() as conn:
            conn.execute(text("TRUNCATE perennial.current_publication"))
        tickers = [f"ZZN{i}" for i in range(n)]
        for t in tickers:
            seed(engine, f"{t}X{n}", name=None, sector="Energy")
        records = [record(f"{t}X{n}") for t in tickers]
        publish(engine, records[: n // 2], records[n // 2 :])
        entered.clear()
        with statements(app) as seen:
            response = client.get("/api/watchlist")
        assert response.status_code == 200, response.text
        assert all(c["sector"] == "Energy" for c in companies(response.json()).values())
        assert entered == [1], "one read transaction per request"
        return len(seen)

    with dashboard(engine, pr22_roles, monkeypatch) as (app, client):
        monkeypatch.setattr(read, "read_transaction", counting)
        assert count(2) == count(12), "the lookup must not issue one query per ticker"


@pytest.mark.db
def test_no_publication_is_still_the_empty_response(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        response = client.get("/api/watchlist")
    assert response.status_code == 200
    assert response.json() == {
        "schema_version": "1.0",
        "generated_at": "",
        "affordable_growing": [],
        "popular_stable": [],
    }


@pytest.mark.db
def test_fixture_mode_is_unchanged(engine, pr22_roles, monkeypatch):  # noqa: F811
    from src.api.dashboard_fixture import temporary_dashboard_response

    seed(engine, "PLTR", name="Synthetic Not Palantir", sector="Energy")
    with dashboard(engine, pr22_roles, monkeypatch, mode="fixture") as (_, client):
        response = client.get("/api/watchlist")
    assert response.status_code == 200
    served = response.json()
    expected = temporary_dashboard_response().model_dump(mode="json")
    served.pop("generated_at"), expected.pop("generated_at")  # the fixture's "now"
    assert served == expected
