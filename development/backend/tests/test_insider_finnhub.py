"""Legacy insider.py against the Finnhub response shape: purchases only, failures never read as empty."""

from datetime import datetime, timedelta, timezone

import pytest

from src.ingestion import collectors


@pytest.fixture
def insider(monkeypatch):
    module = collectors._fetcher("insider")
    monkeypatch.setattr(module, "FINNHUB_API_KEY", "synthetic-key")
    monkeypatch.setattr(module, "REQUEST_DELAY", 0)
    monkeypatch.setattr(module.time, "sleep", lambda seconds: None)
    return module


def recent(days_ago=5):
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).date().isoformat()


def txn(**overrides):
    row = {
        "name": " Synthetic Insider ",
        "transactionCode": "P",
        "transactionDate": recent(),
        "change": 1000,
        "transactionPrice": 25.5,
        "isDerivative": False,
    }
    row.update(overrides)
    return row


class FakeResponse:
    def __init__(self, status=200, body=None):
        self.status_code = status
        self._body = body

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


def test_purchase_is_kept_with_computed_value(insider):
    buys, code, detail = insider.parse_finnhub_buys(
        "ZZAA", {"data": [txn()]}, "popular_stable"
    )
    assert (code, detail) == (None, None)
    assert len(buys) == 1
    buy = buys[0]
    assert buy["value"] == 25500.0 and buy["shares"] == 1000
    assert buy["insider_name"] == "Synthetic Insider"
    assert buy["transaction_type"] == "Purchase" and buy["data_source"] == "finnhub"
    assert buy["ticker"] == "ZZAA" and buy["bucket"] == "popular_stable"


@pytest.mark.parametrize(
    "row",
    [
        txn(transactionCode="S", change=-1000),
        txn(transactionCode="A"),
        txn(transactionCode="M"),
        txn(isDerivative=True),
        txn(change=10),  # $255, under MIN_BUY_VALUE
        txn(transactionPrice=None),
        txn(transactionDate=recent(days_ago=400)),
    ],
)
def test_non_qualifying_rows_are_skipped_not_failed(insider, row):
    assert insider.parse_finnhub_buys("ZZAA", {"data": [row]}, "b") == ([], None, None)


def test_empty_data_is_a_confirmed_no_buys_answer(insider):
    assert insider.parse_finnhub_buys("ZZAA", {"data": []}, "b") == ([], None, None)


@pytest.mark.parametrize(
    "raw",
    [
        None,
        [],
        {},
        {"data": None},
        {"transactions": []},
        {"data": [txn(transactionDate="09/01/2026")]},
        {"data": [txn(transactionDate=None)]},
        {"data": [txn(change="1000")]},
        {"data": ["not a row"]},
    ],
)
def test_unparseable_response_is_bad_response_not_empty(insider, raw):
    buys, code, _detail = insider.parse_finnhub_buys("ZZAA", raw, "b")
    assert buys == [] and code == insider.ERR_BAD_RESPONSE


def test_fetch_sends_key_in_header_and_returns_payload(insider, monkeypatch):
    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append((url, params, headers))
        return FakeResponse(body={"data": [], "symbol": "ZZAA"})

    monkeypatch.setattr(insider.requests, "get", fake_get)
    result = insider.fetch_insider_activity("ZZAA")
    assert result["status"] == insider.STATUS_SUCCESS
    url, params, headers = calls[0]
    assert url == insider.FINNHUB_URL and params["symbol"] == "ZZAA"
    assert headers == {"X-Finnhub-Token": "synthetic-key"}
    assert "synthetic-key" not in str(params)


@pytest.mark.parametrize(
    "status,code",
    [(401, "credential_error"), (403, "http_error"), (500, "http_error")],
)
def test_fetch_http_failures_are_classified(insider, monkeypatch, status, code):
    monkeypatch.setattr(insider.requests, "get", lambda *a, **kw: FakeResponse(status))
    result = insider.fetch_insider_activity("ZZAA")
    assert result["status"] == insider.STATUS_FAILED and result["error_code"] == code


def test_fetch_retries_once_after_rate_limit(insider, monkeypatch):
    responses = [FakeResponse(429), FakeResponse(body={"data": []})]
    monkeypatch.setattr(insider.requests, "get", lambda *a, **kw: responses.pop(0))
    assert insider.fetch_insider_activity("ZZAA")["status"] == insider.STATUS_SUCCESS

    monkeypatch.setattr(insider.requests, "get", lambda *a, **kw: FakeResponse(429))
    assert insider.fetch_insider_activity("ZZAA")["error_code"] == "rate_limited"


def test_fetch_non_json_body_is_bad_response(insider, monkeypatch):
    monkeypatch.setattr(
        insider.requests, "get", lambda *a, **kw: FakeResponse(body=ValueError("x"))
    )
    assert (
        insider.fetch_insider_activity("ZZAA")["error_code"] == insider.ERR_BAD_RESPONSE
    )


@pytest.mark.parametrize("key", ["", "your_finnhub_key_here"])
def test_missing_key_fails_every_ticker_without_calling_provider(
    insider, monkeypatch, key
):
    def forbidden(*args, **kwargs):
        pytest.fail("provider was called without a usable key")

    monkeypatch.setattr(insider, "FINNHUB_API_KEY", key)
    monkeypatch.setattr(insider.requests, "get", forbidden)
    buys, outcomes = insider.scan_tickers(["ZZAA", "ZZBB"], "b", "label")
    assert buys == []
    assert [o["error_code"] for o in outcomes] == ["credential_error"] * 2
    coverage = insider.build_coverage(outcomes, {})
    assert coverage["run_status"] == insider.STATUS_FAILED
    assert coverage["errors_by_code"]["credential_error"]["category"] == "config"


def test_rejected_key_stops_calling_provider_after_first_ticker(insider, monkeypatch):
    calls = []

    def fake_get(*args, **kwargs):
        calls.append(1)
        return FakeResponse(401)

    monkeypatch.setattr(insider.requests, "get", fake_get)
    _buys, outcomes = insider.scan_tickers(["ZZAA", "ZZBB", "ZZCC"], "b", "label")
    assert len(calls) == 1
    assert [o["status"] for o in outcomes] == [insider.STATUS_FAILED] * 3


def test_scan_separates_buys_no_buys_and_unknown(insider, monkeypatch):
    bodies = {
        "ZZAA": FakeResponse(body={"data": [txn()]}),
        "ZZBB": FakeResponse(body={"data": []}),
        "ZZCC": FakeResponse(500),
    }
    monkeypatch.setattr(
        insider.requests, "get", lambda url, params=None, **kw: bodies[params["symbol"]]
    )
    buys, outcomes = insider.scan_tickers(list(bodies), "b", "label")
    assert [b["ticker"] for b in buys] == ["ZZAA"]
    coverage = insider.build_coverage(outcomes, {})
    assert coverage["run_status"] == insider.STATUS_PARTIAL
    assert coverage["with_buys"] == 1 and coverage["confirmed_no_buys"] == 1
    assert coverage["failed_tickers"] == ["ZZCC"]
    assert "not_found" not in coverage
