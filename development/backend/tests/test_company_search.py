"""Global search: real SELECTs on synthetic data, plus API-role integration.

The in-memory cases exercise the HTTP contract without a running PostgreSQL.
The db case uses the existing disposable PostgreSQL/least-privilege fixtures.
Neither uses a production database or changes shared grants.
"""

import pytest
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from src.api import read
from src.api.app import invalid_input


@pytest.fixture
def search_api():
    engine = create_engine(
        "sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    with engine.begin() as conn:
        conn.exec_driver_sql("ATTACH DATABASE ':memory:' AS perennial")
        conn.exec_driver_sql(
            "CREATE TABLE perennial.securities "
            "(id INTEGER PRIMARY KEY, symbol TEXT, name TEXT, company_profile_id INTEGER)"
        )
        conn.exec_driver_sql(
            "CREATE TABLE perennial.company_profiles (id INTEGER PRIMARY KEY, sector TEXT)"
        )
        conn.exec_driver_sql(
            "CREATE TABLE perennial.security_identifiers "
            "(security_id INTEGER, scheme TEXT, value TEXT)"
        )
        conn.execute(
            text(
                "INSERT INTO perennial.company_profiles VALUES (1, 'Healthcare'), (2, 'Technology')"
            )
        )
        rows = [
            (1, "NVDA", "NVIDIA Corporation", 2),
            (2, "N", "Neutral Holdings", None),
            (3, "JNJ", "Johnson & Johnson", 1),
            (4, "BRK.B", "Berkshire Hathaway", None),
            (5, "PCT", "% Literal Percent", None),
            (6, "NONAME", None, None),
            (7, "USCORE", "_ Literal Underscore", None),
            *[(i + 10, f"NN{i:03}", f"New Company {i}", 1) for i in range(60)],
        ]
        conn.execute(
            text(
                "INSERT INTO perennial.securities VALUES (:id, :symbol, :name, :profile)"
            ),
            [dict(zip(("id", "symbol", "name", "profile"), row)) for row in rows],
        )
        conn.execute(
            text(
                "INSERT INTO perennial.security_identifiers VALUES (4, 'ticker', 'BRK-B'), (4, 'ticker', 'BRK.B')"
            )
        )
    app = FastAPI()
    app.state.engine = engine
    app.include_router(read.router)
    read.install(app, read.RateLimitConfig())

    @app.exception_handler(read.APIError)
    async def api_error(request, exc):
        return read._error(exc.status, exc.code, exc.detail)

    app.add_exception_handler(RequestValidationError, invalid_input)

    # read_transaction's execution option is PostgreSQL-specific, but SQLite
    # ignores it and executes the same bounded, parameterized SELECT statement.
    try:
        with TestClient(app) as client:
            yield client
    finally:
        engine.dispose()


def test_single_letter_returns_all_sectors_without_publication_or_preferences(
    search_api,
):
    response = search_api.get("/v1/securities", params={"q": "N", "limit": 100})
    assert response.status_code == 200
    body = response.json()
    assert body["results"][0]["symbol"] == "N"  # exact before prefix matches
    symbols = {row["symbol"] for row in body["results"]}
    assert {"NVDA", "NONAME", "NN059"} <= symbols
    assert "JNJ" not in symbols  # contains N, but neither name nor ticker starts N
    assert body["has_more"] is False
    assert set(body["results"][0]) == {"symbol", "name", "sector"}
    assert response.headers["cache-control"] == "no-cache"


def test_names_case_and_aliases_with_nullable_metadata(search_api):
    assert search_api.get("/v1/securities", params={"q": "  nvidia  "}).json()[
        "results"
    ] == [{"symbol": "NVDA", "name": "NVIDIA Corporation", "sector": "Technology"}]
    for query in ("brk-b", "BRK."):
        results = search_api.get("/v1/securities", params={"q": query}).json()[
            "results"
        ]
        assert results == [
            {"symbol": "BRK.B", "name": "Berkshire Hathaway", "sector": None}
        ]


def test_more_letters_narrow_prefix_and_other_sectors_are_still_searchable(search_api):
    assert [
        row["symbol"]
        for row in search_api.get("/v1/securities", params={"q": "NV"}).json()[
            "results"
        ]
    ] == ["NVDA"]
    health = search_api.get("/v1/securities", params={"q": "johnson"}).json()["results"]
    assert health == [
        {"symbol": "JNJ", "name": "Johnson & Johnson", "sector": "Healthcare"}
    ]
    assert (
        search_api.get("/v1/securities", params={"q": "vidia"}).json()["results"] == []
    )


def test_pagination_covers_every_match_once(search_api):
    first = search_api.get("/v1/securities", params={"q": "NN"}).json()
    second = search_api.get("/v1/securities", params={"q": "NN", "offset": 50}).json()
    assert len(first["results"]) == 50 and first["has_more"] is True
    assert len(second["results"]) == 10 and second["has_more"] is False
    assert [row["symbol"] for row in first["results"] + second["results"]] == [
        f"NN{i:03}" for i in range(60)
    ]


@pytest.mark.parametrize("query, expected", [("%", "PCT"), ("_", "USCORE")])
def test_like_wildcards_are_literal(search_api, query, expected):
    rows = search_api.get("/v1/securities", params={"q": query}).json()["results"]
    assert [row["symbol"] for row in rows] == [expected]


def test_no_match_and_hostile_text_are_empty_not_sql(search_api):
    for query in ("zzzznomatch", "' OR 1=1 --"):
        body = search_api.get("/v1/securities", params={"q": query}).json()
        assert body["results"] == [] and body["has_more"] is False


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"q": ""},
        {"q": "   "},
        {"q": "a" * 101},
        {"q": "N", "limit": 0},
        {"q": "N", "limit": 101},
        {"q": "N", "offset": -1},
        {"q": "N", "offset": 100001},
    ],
)
def test_invalid_queries_and_page_bounds(search_api, params):
    response = search_api.get("/v1/securities", params=params)
    assert response.status_code == 422
    assert response.json()["code"] == "invalid_request"


def test_search_uses_existing_read_rate_limit(search_api):
    statuses = [
        search_api.get("/v1/securities", params={"q": "N"}).status_code
        for _ in range(31)
    ]
    assert statuses[:30] == [200] * 30
    assert statuses[-1] == 429


@pytest.mark.db
def test_api_role_can_search_unpublished_companies(engine, api):
    from test_read_api import seed_security

    seed_security(
        engine, "ZZHEALTH", name="New Health", profile={"sector": "Healthcare"}
    )
    seed_security(
        engine, "ZZTECH", "ZZ-ALIAS", name="New Tech", profile={"sector": "Technology"}
    )
    response = api.get("/v1/securities", params={"q": "New"})
    assert response.status_code == 200
    assert {row["symbol"] for row in response.json()["results"]} == {
        "ZZHEALTH",
        "ZZTECH",
    }
    assert (
        api.get("/v1/securities", params={"q": "zz-alias"}).json()["results"][0][
            "symbol"
        ]
        == "ZZTECH"
    )


# Reuse the repository's API-login fixtures only for the marked db test.
from test_read_api import api, groups  # noqa: E402, F401
