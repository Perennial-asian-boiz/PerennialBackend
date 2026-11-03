"""S2-04: GET /v1/securities/{symbol}/financials. Synthetic rows only.

Spec: Bryan's S2-04 brief (decision 113) and its follow-up (net_margin, the
fixture, "source").

Pinned:
* The years: every fiscal year in the security's newest fundamentals batch,
  newest first, with that batch's id. Served as the perennial_api login, reading
  only fundamentals' nine granted columns (never concept or row_number).
* Per year: revenue, net_income, long_term_debt, equity and operating_cash_flow,
  each {value, unit} or null; form and filed from the year's latest filed row.
* Computed: revenue_growth against the next-older year, if it ended about a year
  earlier (365 +/- 20 days, as the fundamentals score); net_margin; debt_to_equity.
  Each is null when an input is missing, the units differ, or the divisor is <= 0
  (equity, revenue, or the prior year's revenue).
* Banks (the fundamentals score's is_bank) use revenue net of interest, and
  revenue_basis says which line item "revenue" is.
* 200 with status "unavailable" and no years when the security has no
  fundamentals; 404 unknown_security only for an unknown ticker; 422 for an
  invalid one. Cache-Control: no-cache, an ETag that changes with the batch, and
  304 on If-None-Match.
"""

import json
from pathlib import Path

import pytest
from sqlalchemy import text
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

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "financials_v1.json").read_text())
CASES = sorted(k for k in FIXTURE if not k.startswith("_"))


def stored(engine, symbol, rows, *alias):
    """A security (created once) and a new fundamentals batch holding `rows`."""
    with engine.connect() as conn:
        security_id = conn.execute(
            text("SELECT id FROM perennial.securities WHERE symbol = :s"), {"s": symbol}
        ).scalar_one_or_none()
    if security_id is None:
        security_id = seed_security(engine, symbol, *alias)
    with engine.begin() as conn:
        batch = _batch(conn, "fundamentals")
        for n, (end, item, value, unit, form, filed) in enumerate(rows, start=1):
            _insert(
                conn,
                "fundamentals",
                {
                    "row_number": n,
                    "fiscal_year_end": end,
                    "line_item": item,
                    "value": value,
                    "unit": unit,
                    "concept": f"us-gaap:Synthetic{item.title().replace('_', '')}",
                    "form": form,
                    "filed": filed,
                },
                batch_id=batch,
                security_id=security_id,
            )
    return batch


def get(client, symbol, **headers):
    return client.get(f"/v1/securities/{symbol}/financials", headers=headers)


# ── AC 1-3: the served years against the committed fixture ──


@pytest.mark.parametrize("case", CASES)
@pytest.mark.db
def test_each_fixture_company_is_served_as_expected(engine, api, case):  # noqa: F811
    batch = stored(engine, case, FIXTURE[case]["rows"])
    response = get(api, case)
    assert response.status_code == 200
    assert response.json() == {
        "api_version": "v1",
        "symbol": case,
        "batch_id": batch,
        **FIXTURE[case]["expected"],
    }


@pytest.mark.db
def test_numbers_are_json_numbers_and_the_body_is_canonical(engine, api):  # noqa: F811
    stored(engine, "ZZFA", FIXTURE["ZZFA"]["rows"])
    response = get(api, "ZZFA")
    assert response.text == json.dumps(
        response.json(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    year = response.json()["years"][0]
    assert isinstance(year["revenue"]["value"], float) and isinstance(year["net_margin"], float)


@pytest.mark.db
def test_nothing_the_api_may_not_read_is_served(engine, api):  # noqa: F811
    stored(engine, "ZZFA", FIXTURE["ZZFA"]["rows"])
    body = get(api, "ZZFA").text
    assert "Synthetic" not in body and "row_number" not in body
    # line items the table doesn't show stay out
    assert "assets" not in body and "noninterest_expense" not in body


# ── which batch ──────────────────────────────────────────


@pytest.mark.db
def test_the_newest_batch_is_served_and_older_rows_are_not_mixed_in(engine, api):  # noqa: F811
    stored(engine, "ZZFA", FIXTURE["ZZFA"]["rows"])
    newer = stored(engine, "ZZFA", [["2026-12-31", "revenue", "1500", "USD", "10-K", "2027-02-01"]])
    body = get(api, "ZZFA").json()
    assert body["batch_id"] == newer
    assert [y["fiscal_year_end"] for y in body["years"]] == ["2026-12-31"]


@pytest.mark.db
def test_another_securitys_batch_is_never_served(engine, api):  # noqa: F811
    own = stored(engine, "ZZFA", FIXTURE["ZZFA"]["rows"])
    stored(engine, "ZZFM", FIXTURE["ZZFM"]["rows"])  # newer, but not ZZFA's
    assert get(api, "ZZFA").json()["batch_id"] == own


@pytest.mark.db
def test_an_alias_finds_the_canonical_security(engine, api):  # noqa: F811
    stored(engine, "ZZF.A", FIXTURE["ZZFA"]["rows"], "ZZF-A")
    body = get(api, "ZZF-A").json()
    assert body["symbol"] == "ZZF.A" and len(body["years"]) == 4


# ── gaps and errors ──────────────────────────────────────


@pytest.mark.db
def test_no_fundamentals_is_200_unavailable_not_404(engine, api):  # noqa: F811
    seed_security(engine, "ZZFU")
    response = get(api, "ZZFU")
    assert response.status_code == 200
    assert response.json() == {
        "api_version": "v1",
        "symbol": "ZZFU",
        "source": "SEC EDGAR",
        "status": "unavailable",
        "batch_id": None,
        "revenue_basis": None,
        "years": [],
    }


@pytest.mark.db
def test_an_unknown_symbol_is_404_unknown_security(engine, api):  # noqa: F811
    assert_error(get(api, "ZZNONE"), 404, "unknown_security")


@pytest.mark.db
@pytest.mark.parametrize("bad", ["%00", "A" * 1000, "ZZ%0AAA"])
def test_an_invalid_symbol_is_422_and_never_echoed(engine, api, bad):  # noqa: F811
    response = get(api, bad)
    assert_error(response, 422, "invalid_request")
    assert "A" * 1000 not in response.text


# ── ETag, 304 and Cache-Control ──────────────────────────


@pytest.mark.db
def test_etag_and_304(engine, api):  # noqa: F811
    stored(engine, "ZZFA", FIXTURE["ZZFA"]["rows"])
    first = get(api, "ZZFA")
    etag = first.headers["etag"]
    assert etag.startswith('"') and etag.endswith('"')
    assert first.headers["cache-control"] == "no-cache"
    assert get(api, "ZZFA").headers["etag"] == etag
    for header in (etag, f"W/{etag}", f'"other", {etag}'):
        cached = get(api, "ZZFA", **{"If-None-Match": header})
        assert cached.status_code == 304, header
        assert cached.headers["etag"] == etag and cached.headers["cache-control"] == "no-cache"
        assert cached.content == b""
    assert get(api, "ZZFA", **{"If-None-Match": '"other"'}).status_code == 200


@pytest.mark.db
def test_the_etag_changes_with_a_new_batch(engine, api):  # noqa: F811
    stored(engine, "ZZFA", FIXTURE["ZZFA"]["rows"])
    etag = get(api, "ZZFA").headers["etag"]
    stored(engine, "ZZFA", FIXTURE["ZZFA"]["rows"][:5])
    fresh = get(api, "ZZFA", **{"If-None-Match": etag})
    assert fresh.status_code == 200 and fresh.headers["etag"] != etag


@pytest.mark.db
def test_an_unavailable_body_has_an_etag_too(engine, api):  # noqa: F811
    seed_security(engine, "ZZFU")
    etag = get(api, "ZZFU").headers["etag"]
    assert get(api, "ZZFU", **{"If-None-Match": etag}).status_code == 304


# ── the route, OpenAPI and docs ──────────────────────────


def test_the_route_is_a_documented_read_route():
    operation = _v1_openapi(_router_app())["paths"]["/v1/securities/{symbol}/financials"]["get"]
    assert {"200", "304", "404", "422", "429"} <= set(operation["responses"])


def test_the_response_schemas_are_named():
    schemas = _v1_openapi(_router_app())["schemas"]
    for name in ("FinancialsResponse", "FinancialYear", "Amount"):
        assert name in schemas, name


def test_the_snapshot_includes_the_financials_route():
    """Regenerated in the same PR (UPDATE_API_SNAPSHOT=1)."""
    assert "/v1/securities/{symbol}/financials" in json.loads(SNAPSHOT.read_text())["paths"]


def test_api_md_documents_the_financials_route():
    content = (REPO / "docs" / "technical-specs" / "API.md").read_text()
    for term in (
        "/v1/securities/{symbol}/financials",
        "SEC EDGAR",
        "revenue_basis",
        "revenue_net_of_interest",
        "revenue_growth",
        "net_margin",
        "debt_to_equity",
        "operating_cash_flow",
        '"status": "unavailable"',
    ):
        assert term in content, term
