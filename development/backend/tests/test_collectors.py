"""Live-collector wrappers against a fake HTTP session: explicit outcomes, no partial success, no leaks."""

import json

import pytest
import requests

from src.ingestion import collectors
from src.ingestion.diagnostics import render_summary, safe_diagnostics

SECRET = "SYNTHETIC_SECRET_7f3a"
NO_SLEEP = lambda seconds: None  # noqa: E731


class FakeResponse:
    def __init__(self, status=200, body=None, raw=None, headers=None):
        self.status_code = status
        self._raw = raw if raw is not None else json.dumps(body).encode()
        self.headers = headers or {}

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self._raw), chunk_size):
            yield self._raw[i : i + chunk_size]

    def close(self):
        pass


class FakeSession:
    """Routes get() to handler(url, params) which returns a FakeResponse or raises."""

    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None, stream=False):
        assert stream and timeout
        self.calls.append((url, dict(params or {})))
        return self.handler(url, params or {})


def holding(ticker, weight=1.0, price=10.0, day="2026-09-30"):
    return {
        "ticker": ticker,
        "company": f"Synthetic {ticker}",
        "weight": weight,
        "share_price": price,
        "date": day,
    }


def ark_session(overrides=None):
    overrides = overrides or {}

    def handler(url, params):
        symbol = params["symbol"]
        if symbol in overrides:
            return overrides[symbol]
        return FakeResponse(
            body={
                "symbol": symbol,
                "holdings": [
                    holding("ZZAA", 2.5),
                    holding(f"ZZ{symbol[-1]}X", 1.25),
                    {"ticker": None, "weight": 0.1},
                ],
            }
        )

    return FakeSession(handler)


def assert_clean(outcome, capsys):
    blob = json.dumps(outcome.diagnostics or {}) + render_summary(
        outcome.error_code or "collection_failed", safe_diagnostics(outcome.diagnostics)
    )
    out = capsys.readouterr()
    assert SECRET not in blob + out.out + out.err


# ── ARK ──────────────────────────────────────────────────


def test_ark_success_merges_funds_and_reads_provider_date():
    outcome = collectors.collect_ark(ark_session(), sleep=NO_SLEEP)
    assert outcome.succeeded and outcome.mode == "live"
    zzaa = next(h for h in outcome.records if h["ticker"] == "ZZAA")
    assert zzaa["fund_count"] == 4 and zzaa["total_weight"] == 10.0
    assert str(outcome.source_as_of) == "2026-09-30"
    assert outcome.diagnostics["attempted"] == outcome.diagnostics["completed"] == 4


@pytest.mark.parametrize(
    "bad,code",
    [
        (FakeResponse(status=500, body={}), "http_500"),
        (FakeResponse(status=402, body={}), "http_402"),
        (FakeResponse(body={"holdings": []}), "empty_or_malformed_holdings"),
        (FakeResponse(body={"holdings": [{}]}), "empty_or_malformed_holdings"),
        (FakeResponse(body={}), "empty_or_malformed_holdings"),
        (
            FakeResponse(body={"holdings": [holding("ZZQ", weight="heavy")]}),
            "malformed_holding",
        ),
        (FakeResponse(raw=b"<html>"), "invalid_json"),
        (
            FakeResponse(
                body={},
                headers={"Content-Length": str(collectors.MAX_RESPONSE_BYTES + 1)},
            ),
            "response_too_large",
        ),
    ],
)
def test_ark_one_bad_fund_fails_whole_collection(bad, code):
    outcome = collectors.collect_ark(ark_session({"ARKG": bad}), sleep=NO_SLEEP)
    assert not outcome.succeeded and outcome.error_code == "partial_collection"
    assert outcome.diagnostics["failures"] == [{"unit": "ARKG", "code": code}]


def test_ark_mixed_dates_leave_source_as_of_unknown():
    session = ark_session(
        {"ARKQ": FakeResponse(body={"holdings": [holding("ZZAA", day="2026-09-29")]})}
    )
    assert collectors.collect_ark(session, sleep=NO_SLEEP).source_as_of is None


def test_streamed_body_over_cap_is_rejected(monkeypatch):
    monkeypatch.setattr(collectors, "MAX_RESPONSE_BYTES", 100)
    resp = collectors._get_json(
        FakeSession(lambda u, p: FakeResponse(raw=b"[" + b"1," * 200 + b"1]")), "u"
    )
    assert resp.code == "response_too_large"


# ── Congress / FMP ───────────────────────────────────────


def fmp_item(symbol="ZZAA"):
    return {
        "symbol": symbol,
        "firstName": "Synthetic",
        "lastName": "Member",
        "type": "Purchase",
        "amount": "$1,001 - $15,000",
        "transactionDate": "2026-09-01",
        "disclosureDate": "2026-09-10",
        "link": "https://disclosures.example.invalid/1",
    }


def congress_session(fail_with=None):
    def handler(url, params):
        if fail_with and "apikey" in params:
            return fail_with(url, params)
        assert url.endswith("-latest"), "only the two FMP chamber endpoints"
        return FakeResponse(body=[fmp_item()] if params["page"] == 0 else [])

    return FakeSession(handler)


def test_congress_success_and_page_end():
    outcome = collectors.collect_congress(
        congress_session(), api_key="synthetic-key", sleep=NO_SLEEP
    )
    assert (
        outcome.succeeded and len(outcome.records) == 2
    )  # chamber observations remain distinct


def test_congress_exception_text_with_key_never_leaks(capsys):
    def explode(url, params):
        raise requests.ConnectionError(
            f"Max retries exceeded with url: {url}?apikey={params['apikey']}"
        )

    outcome = collectors.collect_congress(
        congress_session(explode), api_key=SECRET, sleep=NO_SLEEP
    )
    assert not outcome.succeeded
    assert outcome.diagnostics["failures"][0] == {
        "unit": "senate page 0",
        "code": "request_error:ConnectionError",
    }
    assert_clean(outcome, capsys)


def test_congress_402_truncation_is_failure():
    def limited(url, params):
        if params["page"] >= 1:
            return FakeResponse(status=402, body={})
        return FakeResponse(body=[fmp_item() for _ in range(20)])

    outcome = collectors.collect_congress(
        congress_session(limited), api_key="k", sleep=NO_SLEEP
    )
    assert outcome.error_code == "partial_collection"


def test_congress_malformed_date_fails_before_filter():
    def handler(url, params):
        if url.endswith("senate-latest") and params["page"] == 0:
            bad = dict(fmp_item(), transactionDate="2026/09/01")
            return FakeResponse(body=[bad, fmp_item(symbol="ZZBB")])
        return FakeResponse(body=[])

    outcome = collectors.collect_congress(
        FakeSession(handler), api_key="k", sleep=NO_SLEEP
    )
    assert not outcome.succeeded and outcome.error_code == "parse_error"


def test_congress_requests_only_fmp_chambers():
    """Senate Stock Watcher was removed as a feature; nothing else is downloaded."""
    session = congress_session()
    outcome = collectors.collect_congress(
        session, api_key="synthetic-key", sleep=NO_SLEEP
    )
    assert outcome.succeeded
    assert outcome.diagnostics["planned"] == 2
    assert all(call[0].endswith("-latest") for call in session.calls)
    assert all(r["data_source"] == "fmp" for r in outcome.records)


def test_congress_missing_key():
    assert collectors.collect_congress(api_key="").error_code == "missing_credential"


# ── Insider ──────────────────────────────────────────────


@pytest.fixture
def upstream(tmp_path, monkeypatch):
    insider = collectors._fetcher("insider")
    si = collectors._fetcher("short_interest")
    (tmp_path / "trades_congress.json").write_text(
        json.dumps({"trades": [{"ticker": "ZZAA", "trade_type": "Purchase"}]})
    )
    (tmp_path / "ark_holdings.json").write_text(
        json.dumps(
            {
                "holdings": [
                    {"ticker": "ZZDD", "fund_count": 1},
                    {"ticker": "ZZEE", "fund_count": 2},
                ]
            }
        )
    )
    monkeypatch.setattr(insider, "LOCAL_DATA_DIR", tmp_path)
    monkeypatch.setattr(si, "LOCAL_DATA_DIR", tmp_path)
    return tmp_path


def purchase(**over):
    """One Finnhub insider-transactions row: a $50,000 open-market share purchase."""
    from datetime import date

    txn = {
        "name": "Synthetic Insider",
        "transactionCode": "P",
        "transactionDate": date.today().isoformat(),
        "change": 100,
        "transactionPrice": 500.0,
        "isDerivative": False,
    }
    txn.update(over)
    return txn


def insider_session(per_symbol):
    """Routes on the Finnhub `symbol` param; any other symbol answers with no rows."""

    def handler(url, params):
        assert url == collectors._fetcher("insider").FINNHUB_URL
        return per_symbol.get(params["symbol"], FakeResponse(body={"data": []}))

    return FakeSession(handler)


def test_insider_zero_buys_is_success(upstream):
    outcome = collectors.collect_insider(insider_session({}), sleep=NO_SLEEP)
    assert outcome.succeeded and outcome.records == []
    assert outcome.diagnostics["completed"] == 3


def test_insider_buys_collected(upstream):
    ok = FakeResponse(body={"data": [purchase()]})
    outcome = collectors.collect_insider(insider_session({"ZZEE": ok}), sleep=NO_SLEEP)
    assert outcome.succeeded and [b["ticker"] for b in outcome.records] == ["ZZEE"]
    assert outcome.records[0]["bucket"] == "affordable_growing"
    assert outcome.records[0]["value"] == 50000.0
    assert outcome.records[0]["data_source"] == "finnhub"


def test_insider_key_travels_in_header_not_params(upstream):
    seen = []

    class HeaderSession(FakeSession):
        def get(self, url, params=None, headers=None, timeout=None, stream=False):
            seen.append((dict(params or {}), dict(headers or {})))
            return super().get(url, params, headers, timeout, stream)

    outcome = collectors.collect_insider(
        HeaderSession(lambda u, p: FakeResponse(body={"data": []})), sleep=NO_SLEEP
    )
    assert outcome.succeeded
    for params, headers in seen:
        assert headers == {"X-Finnhub-Token": "synthetic-finnhub-key"}
        assert "synthetic-finnhub-key" not in json.dumps(params)
        assert set(params) == {"symbol", "from", "to"}


@pytest.mark.parametrize("key", ["", "your_finnhub_key_here"])
def test_insider_missing_key_fails_without_requests(upstream, monkeypatch, key):
    monkeypatch.setattr(collectors._fetcher("insider"), "FINNHUB_API_KEY", key)
    session = insider_session({})
    outcome = collectors.collect_insider(session, sleep=NO_SLEEP)
    assert outcome.error_code == "missing_credential" and session.calls == []


def test_insider_key_comes_from_settings_before_module_default(upstream):
    from pydantic import SecretStr

    from src.settings import ProviderSettings

    seen = []

    class HeaderSession(FakeSession):
        def get(self, url, params=None, headers=None, timeout=None, stream=False):
            seen.append(dict(headers or {}))
            return super().get(url, params, headers, timeout, stream)

    settings = ProviderSettings(finnhub_api_key=SecretStr("settings-finnhub-key"))
    outcome = collectors.collect_insider(
        HeaderSession(lambda u, p: FakeResponse(body={"data": []})),
        sleep=NO_SLEEP,
        settings=settings,  # type: ignore[arg-type]
    )
    assert outcome.succeeded
    assert {h["X-Finnhub-Token"] for h in seen} == {"settings-finnhub-key"}


def test_insider_asks_provider_symbol_but_keeps_planned_ticker():
    upstream = {
        "trades": [],
        "holdings": [
            {"ticker": "RKLB UQ", "fund_count": 1},
            {"ticker": "BRK.B", "fund_count": 1},
            {"ticker": "GOOG", "fund_count": 1},
        ],
    }
    ok = FakeResponse(body={"data": [purchase()]})
    session = insider_session({"RKLB": ok})
    outcome = collectors.collect_insider(session, sleep=NO_SLEEP, upstream=upstream)
    assert [p["symbol"] for _url, p in session.calls] == ["RKLB", "BRK-B", "GOOGL"]
    assert outcome.succeeded and [b["ticker"] for b in outcome.records] == ["RKLB UQ"]


def test_insider_non_us_listing_is_skipped_without_request():
    upstream = {
        "trades": [],
        "holdings": [
            {"ticker": "ZZQQ/U", "fund_count": 1},
            {"ticker": "ZZAA", "fund_count": 1},
        ],
    }
    session = insider_session({})
    outcome = collectors.collect_insider(session, sleep=NO_SLEEP, upstream=upstream)
    assert [p["symbol"] for _url, p in session.calls] == ["ZZAA"]
    assert outcome.succeeded and outcome.diagnostics["completed"] == 2
    assert outcome.envelope["unsupported_tickers"] == ["ZZQQ/U"]


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"data": {}},
        {"data": None},
        {"data": ["not a row"]},
        {"data": [purchase(change="100")]},
        {"data": [purchase(transactionPrice="500")]},
        {"data": [purchase(transactionDate=None)]},
        {"data": [purchase(transactionDate="09/01/2026")]},
        {"data": [purchase(name=None)]},
        {"data": [purchase(isDerivative="no")]},
    ],
)
def test_insider_malformed_payload_fails_instead_of_dropping(upstream, body):
    outcome = collectors.collect_insider(
        insider_session({"ZZAA": FakeResponse(body=body)}), sleep=NO_SLEEP
    )
    assert (
        outcome.error_code == "collection_failed"
    )  # first unit failed; later successes never run
    assert outcome.diagnostics["failures"] == [
        {"unit": "ZZAA", "code": "malformed_response"}
    ]


def test_insider_rate_limit_is_failure_not_empty(upstream):
    outcome = collectors.collect_insider(
        insider_session({"ZZAA": FakeResponse(status=429, body={})}), sleep=NO_SLEEP
    )
    assert outcome.diagnostics["failures"] == [{"unit": "ZZAA", "code": "http_429"}]


def test_plans_match_legacy_loaders(upstream):
    insider = collectors._fetcher("insider")
    si = collectors._fetcher("short_interest")
    data, failure = collectors._read_upstream("insider_trades", upstream)
    assert failure is None
    legacy, congress_error = insider.legacy_load_congress_tickers()
    ark_tickers, ark_error = insider.legacy_load_ark_tickers()
    assert congress_error is None and ark_error is None
    legacy_ark = [t for t in ark_tickers if t not in set(legacy)]
    assert collectors._insider_plan(insider, data) == (
        [(t, "popular_stable") for t in legacy]
        + [(t, "affordable_growing") for t in legacy_ark]
    )
    assert collectors._short_interest_plan(si, data) == ["ZZAA", "ZZDD", "ZZEE"]


@pytest.mark.parametrize(
    "name,content,code",
    [
        ("ark_holdings.json", {"holdings": [{}]}, "malformed_file"),
        (
            "ark_holdings.json",
            {"holdings": [{"ticker": "ZZDD", "fund_count": "1"}]},
            "malformed_file",
        ),
        (
            "trades_congress.json",
            {"trades": [{"ticker": None, "trade_type": "Purchase"}]},
            "malformed_file",
        ),
        ("trades_congress.json", {"rows": []}, "malformed_file"),
        ("trades_congress.json", {"trades": [{"ticker": "ZZAA"}]}, "malformed_file"),
        (
            "trades_congress.json",
            {"trades": [{"ticker": "ZZAA", "trade_type": " "}]},
            "malformed_file",
        ),
        ("trades_congress.json", "{not json", "unreadable_file"),
    ],
)
def test_malformed_upstream_fails_instead_of_partial_plan(
    upstream, name, content, code
):
    path = upstream / name
    path.write_text(content if isinstance(content, str) else json.dumps(content))
    for collect in (collectors.collect_insider, collectors.collect_short_interest):
        outcome = collect(
            FakeSession(lambda u, p: FakeResponse(status=404)), sleep=NO_SLEEP
        )
        assert outcome.error_code == "missing_upstream"
        assert outcome.diagnostics["failures"] == [{"unit": name, "code": code}]


def test_oversized_upstream_fails(upstream, monkeypatch):
    from src.ingestion import files

    monkeypatch.setattr(files, "MAX_FILE_BYTES", 10)
    monkeypatch.setattr(
        collectors,
        "read_json_bounded",
        lambda path: files.read_json_bounded(path, max_bytes=10),
    )
    outcome = collectors.collect_insider(
        FakeSession(lambda u, p: FakeResponse(status=404)), sleep=NO_SLEEP
    )
    assert {
        "unit": "trades_congress.json",
        "code": "file_too_large",
    } in outcome.diagnostics["failures"]


def test_oversized_plan_is_refused_without_requests(upstream, monkeypatch):
    holdings = [{"ticker": f"ZZ{i}", "fund_count": 1} for i in range(30)]
    (upstream / "ark_holdings.json").write_text(json.dumps({"holdings": holdings}))
    monkeypatch.setattr(collectors, "MAX_PLANNED_REQUESTS", 25)
    for collect in (collectors.collect_insider, collectors.collect_short_interest):
        session = FakeSession(lambda u, p: FakeResponse(status=404))
        outcome = collect(session, sleep=NO_SLEEP)
        assert outcome.error_code == "plan_too_large" and session.calls == []


def test_insider_transaction_without_code_is_malformed(upstream):
    body = {"data": [{}]}
    outcome = collectors.collect_insider(
        insider_session({"ZZAA": FakeResponse(body=body)}), sleep=NO_SLEEP
    )
    assert outcome.diagnostics["failures"] == [
        {"unit": "ZZAA", "code": "malformed_response"}
    ]


def test_missing_upstream_file_fails(upstream):
    (upstream / "trades_congress.json").unlink()
    outcome = collectors.collect_insider(insider_session({}), sleep=NO_SLEEP)
    assert outcome.error_code == "missing_upstream"
    assert outcome.diagnostics["failures"] == [
        {"unit": "trades_congress.json", "code": "missing_file"}
    ]


# ── Short interest ───────────────────────────────────────


def si_rows(date_value="09/15/2026"):
    return {
        "data": {
            "shortInterestTable": {
                "rows": [
                    {
                        "settlementDate": date_value,
                        "interest": "1,000",
                        "avgDailyShareVolume": "500",
                        "daysToCover": "2.0",
                    }
                ]
            }
        }
    }


def si_session(per_ticker):
    def handler(url, params):
        ticker = url.split("/quote/")[1].split("/")[0]
        return per_ticker.get(ticker, FakeResponse(body={"data": None}))

    return FakeSession(handler)


def test_short_interest_explicit_no_record_and_rows(upstream):
    outcome = collectors.collect_short_interest(
        si_session({"ZZAA": FakeResponse(body=si_rows())}), sleep=NO_SLEEP
    )
    assert outcome.succeeded and len(outcome.records) == 1
    assert (
        outcome.records[0]["short_interest"] == 1000
        and outcome.records[0]["settlement_date"] == "2026-09-15"
    )


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"data": {}},
        {"data": {"foo": 1}},
        si_rows(date_value=123),
        {"data": {"shortInterestTable": {"rows": "x"}}},
    ],
)
def test_short_interest_malformed_fails(upstream, body):
    outcome = collectors.collect_short_interest(
        si_session({"ZZDD": FakeResponse(body=body)}), sleep=NO_SLEEP
    )
    assert outcome.error_code == "partial_collection"
    assert outcome.diagnostics["failures"][0]["unit"] == "ZZDD"


def test_short_interest_timeout_fails(upstream):
    def handler(url, params):
        raise requests.Timeout(f"timed out {SECRET}")

    outcome = collectors.collect_short_interest(FakeSession(handler), sleep=NO_SLEEP)
    assert outcome.error_code == "collection_failed"
    assert outcome.diagnostics["failures"][0]["code"] == "timeout"
