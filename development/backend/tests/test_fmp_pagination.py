"""Synthetic page boundaries for both FMP collection paths."""

import json
from math import ceil

import pytest

from src.ingestion import collectors
from src.ingestion.diagnostics import safe_diagnostics
from test_collectors import FakeResponse, FakeSession, NO_SLEEP, fmp_item


def _trades(count):
    return [dict(fmp_item(symbol=f"ZZ{i:04d}"), id=f"synthetic-{i}") for i in range(count)]


def _session(items, *, status_page=None):
    def handler(url, params):
        if not url.endswith("-latest"):
            return FakeResponse(body=[])
        if "house-latest" in url:
            return FakeResponse(body=[])
        page = params["page"]
        if page == status_page:
            return FakeResponse(status=402, body={})
        return FakeResponse(body=items[page * 20 : (page + 1) * 20])

    return FakeSession(handler)


def test_page_counts_survive_diagnostic_sanitization():
    assert safe_diagnostics({"senate_pages_fetched": 4, "house_pages_fetched": 1}) == {
        "senate_pages_fetched": 4,
        "house_pages_fetched": 1,
    }


@pytest.mark.parametrize("count", [0, 1, 19, 20, 21, 59, 60, 61])
def test_database_congress_fetches_until_short_or_empty_page(count):
    session = _session(_trades(count))
    outcome = collectors.collect_congress(
        session, api_key="synthetic-fmp", sleep=NO_SLEEP
    )
    assert outcome.succeeded
    assert len(outcome.records) == count
    expected_pages = count // 20 + 1 if count % 20 == 0 else ceil(count / 20)
    assert outcome.diagnostics["senate_pages_fetched"] == expected_pages
    assert outcome.diagnostics["house_pages_fetched"] == 1
    senate_pages = [
        params["page"] for url, params in session.calls if url.endswith("senate-latest")
    ]
    assert senate_pages == list(range(expected_pages))


@pytest.mark.parametrize(
    "full_pages,succeeds", [(1, True), (2, True), (3, False)]
)
def test_database_congress_safety_cap_boundary(monkeypatch, full_pages, succeeds):
    fmp = collectors._fetcher("fmp")
    monkeypatch.setattr(fmp, "MAX_PAGES", 3)
    session = _session(_trades(full_pages * 20 + 1))
    outcome = collectors.collect_congress(
        session, api_key="synthetic-fmp", sleep=NO_SLEEP
    )
    assert outcome.succeeded is succeeds
    if not succeeds:
        assert outcome.diagnostics["failures"] == [
            {"unit": "senate page 2", "code": "pagination_exhausted"}
        ]
        assert outcome.diagnostics["senate_pages_fetched"] == 3


def test_database_congress_402_on_later_page_is_explicit_failure():
    outcome = collectors.collect_congress(
        _session(_trades(61), status_page=2),
        api_key="synthetic-fmp",
        sleep=NO_SLEEP,
    )
    assert not outcome.succeeded
    assert outcome.diagnostics["failures"] == [
        {"unit": "senate page 2", "code": "http_402"}
    ]


@pytest.mark.parametrize("count", [0, 1, 19, 20, 21, 59, 60, 61])
def test_legacy_fmp_fetches_all_pages_without_changing_small_outputs(
    count, monkeypatch
):
    fmp = collectors._fetcher("fmp")
    records = _trades(count)
    pages = []

    class Response:
        status_code = 200

        def __init__(self, body):
            self.body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self.body

    def get(url, *, params, timeout):
        assert timeout == 15
        pages.append(params["page"])
        page = params["page"]
        return Response(records[page * 20 : (page + 1) * 20])

    monkeypatch.setattr(fmp.requests, "get", get)
    actual = fmp.fetch_chamber_trades("senate", api_key="synthetic")
    expected = [dict(record, _chamber="Senate") for record in records]
    assert json.dumps(actual, sort_keys=True) == json.dumps(expected, sort_keys=True)
    expected_pages = count // 20 + 1 if count % 20 == 0 else ceil(count / 20)
    assert pages == list(range(expected_pages))


@pytest.mark.parametrize("max_pages", [1, 2, 3])
def test_legacy_fmp_configured_page_cap_fails_instead_of_truncating(
    max_pages, monkeypatch
):
    """A configured cap below MAX_PAGES must still fail, never return a partial set."""
    fmp = collectors._fetcher("fmp")
    records = _trades(20 * (max_pages + 1))
    pages = []

    class Response:
        status_code = 200

        def __init__(self, body):
            self.body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self.body

    def get(url, *, params, timeout):
        pages.append(params["page"])
        page = params["page"]
        return Response(records[page * 20 : (page + 1) * 20])

    monkeypatch.setattr(fmp.requests, "get", get)
    with pytest.raises(RuntimeError, match="pagination exhausted"):
        fmp.fetch_chamber_trades("senate", api_key="synthetic", max_pages=max_pages)
    assert pages == list(range(max_pages))
