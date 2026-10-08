"""Synthetic regressions for source completeness, identities and transport policy."""

import copy
import json
from datetime import datetime, timezone

import pytest
import requests

from src.ingestion import collectors
from src.ingestion.schemas import ShortInterestRecord
from tests.test_collectors import (
    FakeResponse, FakeSession, NO_SLEEP, fmp_item, si_rows, ark_session,
)


UPSTREAM = {"trades": [{"ticker": "ZZAA", "trade_type": "Purchase"}], "holdings": []}


@pytest.mark.parametrize("field", ["interest", "avgDailyShareVolume", "daysToCover"])
@pytest.mark.parametrize("value", ["garbage", "1,2,3", "NaN", "inf", True, -1])
def test_malformed_nasdaq_number_fails_collection(field, value):
    body = si_rows()
    body["data"]["shortInterestTable"]["rows"][0][field] = value
    outcome = collectors.collect_short_interest(
        FakeSession(lambda u, p: FakeResponse(body=body)), sleep=NO_SLEEP, upstream=UPSTREAM)
    assert not outcome.succeeded
    assert outcome.coverage is None


@pytest.mark.parametrize("marker", [None, "", "N/A", "--", "-"])
def test_null_markers_remain_null_in_current_and_history(marker):
    body = si_rows()
    body["data"]["shortInterestTable"]["rows"][0].update(
        interest=marker, avgDailyShareVolume=marker, daysToCover=marker)
    outcome = collectors.collect_short_interest(
        FakeSession(lambda u, p: FakeResponse(body=body)), sleep=NO_SLEEP, upstream=UPSTREAM)
    assert outcome.succeeded
    record = ShortInterestRecord.model_validate(outcome.records[0]).canonical()
    for row in [record, *record["history"]]:
        assert row["short_interest_shares"] is None
        assert row["average_daily_volume"] is None
        assert row["days_to_cover"] is None


def test_bad_history_number_fails_entire_collection():
    body = si_rows()
    rows = body["data"]["shortInterestTable"]["rows"]
    rows.append(dict(rows[0], interest="bad"))
    outcome = collectors.collect_short_interest(
        FakeSession(lambda u, p: FakeResponse(body=body)), sleep=NO_SLEEP, upstream=UPSTREAM)
    assert not outcome.succeeded


def test_canonical_and_legacy_short_interest_inputs_match():
    canonical = {"ticker": "ZZAA", "settlement_date": "2026-09-15",
                 "short_interest_shares": 1000, "average_daily_volume": 500,
                 "history": [{"short_interest_shares": 1000, "average_daily_volume": 500}]}
    legacy = {"ticker": "ZZAA", "settlement_date": "2026-09-15", "short_interest": 1000,
              "avg_daily_volume": 500, "history": [{"short_interest": 1000, "avg_daily_volume": 500}]}
    a, b = [ShortInterestRecord.model_validate(r).canonical() for r in (canonical, legacy)]
    assert a == b
    assert a["history"][0]["short_interest_shares"] == 1000


@pytest.mark.parametrize("collect", [collectors.collect_insider, collectors.collect_short_interest])
def test_explicit_upstream_never_reads_files(collect, monkeypatch):
    def forbidden(*args):
        pytest.fail("explicit upstream must not read local files")
    monkeypatch.setattr(collectors, "_read_upstream", forbidden)
    session = FakeSession(lambda u, p: FakeResponse(body={"data": None}))
    assert collect(session, sleep=NO_SLEEP, upstream={"trades": [], "holdings": []}).error_code == "no_input_tickers"
    assert collect(session, sleep=NO_SLEEP, upstream={}).error_code == "missing_upstream"
    assert session.calls == []


def test_congress_preserves_distinct_and_ambiguous_observations():
    fmp = collectors._fetcher("fmp")
    base = fmp.parse_trades([dict(fmp_item(), _chamber="Senate")])[0]
    other_amount = dict(base, amount_range="$50,001 - $100,000")
    other_filing = dict(base, source_link="https://example.invalid/filing/2")
    other_provider = dict(base, data_source="senate_watcher")
    rows = [base, other_amount, other_filing, other_provider, dict(base)]
    assert fmp.deduplicate_proven_repeats(rows) == rows  # no transaction ID: cannot prove duplicates
    identified = dict(base, provider_record_id="txn-1")
    assert fmp.deduplicate_proven_repeats([identified, dict(identified), other_amount]) == [identified, other_amount]


def test_congress_parser_preserves_provider_identity():
    fmp = collectors._fetcher("fmp")
    result = fmp.parse_trades([dict(fmp_item(), id="txn-123")])[0]
    assert result["provider_record_id"] == "txn-123"
    assert result["data_source"] == "fmp"


@pytest.mark.parametrize("ending", ["full", "short", "empty", "402"])
def test_congress_pagination_completeness(ending, monkeypatch):
    fmp = collectors._fetcher("fmp")
    monkeypatch.setattr(fmp, "MAX_PAGES", 2)
    def handler(url, params):
        if not url.endswith("-latest"):
            return FakeResponse(body=[])
        if params["page"] == 0 or ending == "full":
            return FakeResponse(body=[dict(fmp_item(), id=str(i)) for i in range(20)])
        if ending == "402":
            return FakeResponse(status=402)
        return FakeResponse(body=[fmp_item()] if ending == "short" else [])
    result = collectors.collect_congress(FakeSession(handler), api_key="test", sleep=NO_SLEEP)
    assert result.succeeded == (ending in {"short", "empty"})
    if result.succeeded:
        assert result.coverage == {"complete": True, "scope": "recent_window"}
    else:
        assert result.coverage is None
        assert result.diagnostics["failures"][0]["code"] == ("pagination_exhausted" if ending == "full" else "http_402")


def test_ark_complete_coverage():
    assert collectors.collect_ark(ark_session(), sleep=NO_SLEEP).coverage == {"complete": True, "scope": "snapshot"}


@pytest.mark.parametrize("failure", [429, 500, 502, 503, 504, "timeout"])
def test_transient_retries_are_bounded_and_recover(failure):
    calls = []
    sleeps = []
    def handler(u, p):
        calls.append(1)
        if len(calls) == 1:
            if failure == "timeout":
                raise requests.Timeout("private-value")
            return FakeResponse(status=failure, headers={"Retry-After": "3"})
        return FakeResponse(body={"ok": True})
    result = collectors._get_json(FakeSession(handler), "synthetic", sleep=sleeps.append, jitter=lambda: 0)
    assert result.ok and result.data == {"ok": True}
    assert len(calls) == 2 and len(sleeps) == 1
    assert sleeps[0] == (1 if failure == "timeout" else 3)


@pytest.mark.parametrize("response", [FakeResponse(status=401), FakeResponse(status=403), FakeResponse(status=402), FakeResponse(raw=b"bad-json")])
def test_auth_and_parse_failures_are_not_retried(response):
    session = FakeSession(lambda u, p: response)
    result = collectors._get_json(session, "synthetic", sleep=NO_SLEEP, jitter=lambda: 0)
    assert not result.ok and len(session.calls) == 1


def test_retry_after_exceeding_budget_fails_without_early_retry():
    sleeps = []
    session = FakeSession(lambda u, p: FakeResponse(status=429, headers={"Retry-After": "999999"}))
    result = collectors._get_json(session, "synthetic", sleep=sleeps.append, jitter=lambda: 0)
    assert result.code == "deadline_exceeded"
    assert len(session.calls) == 1 and sleeps == []


def test_deadline_stops_retries_and_caps_request_timeout():
    now = [0.0]
    class Session(FakeSession):
        def get(self, *args, **kwargs):
            assert kwargs["timeout"] <= 0.5
            return super().get(*args, **kwargs)
    session = Session(lambda u, p: FakeResponse(status=503))
    result = collectors._get_json(session, "synthetic", sleep=lambda n: now.__setitem__(0, now[0] + n),
                                  jitter=lambda: 0, monotonic=lambda: now[0], deadline_seconds=0.5)
    assert result.code == "deadline_exceeded" and len(session.calls) == 1


@pytest.mark.parametrize("header,expected", [
    ("Wed, 07 Oct 2026 00:00:05 GMT", 5), ("Wed, 07 Oct 2026 00:00:20 GMT", 20),
    ("20", 20),
    ("bad header", 1), ("NaN", 1), ("-10", 1),
])
def test_retry_after_dates_and_malformed_headers(header, expected):
    from datetime import datetime, timezone
    epoch = datetime(2026, 10, 7, tzinfo=timezone.utc).timestamp()
    sleeps = []
    responses = iter([FakeResponse(status=503, headers={"Retry-After": header}), FakeResponse(body=[])])
    result = collectors._get_json(FakeSession(lambda u, p: next(responses)), "synthetic",
                                  jitter=lambda: 0, sleep=sleeps.append, wall_time=lambda: epoch)
    assert result.ok and sleeps == [expected]


@pytest.mark.parametrize("error", [requests.ConnectionError, requests.exceptions.ProxyError])
def test_connection_errors_retry_and_recover(error):
    attempts = []
    sleeps = []
    def handler(u, p):
        attempts.append(1)
        if len(attempts) == 1:
            raise error("SYNTHETIC_SECRET_7f3a")
        return FakeResponse(body={"ok": True})
    result = collectors._get_json(FakeSession(handler), "synthetic", sleep=sleeps.append, jitter=lambda: 0)
    assert result.ok and result.data == {"ok": True}
    assert len(attempts) == 2 and sleeps == [1]


def test_ssl_errors_fail_without_retry():
    def handler(u, p):
        raise requests.exceptions.SSLError("SYNTHETIC_SECRET_7f3a")
    session = FakeSession(handler)
    result = collectors._get_json(session, "synthetic", sleep=NO_SLEEP)
    assert result.code == "request_error:SSLError" and len(session.calls) == 1


def test_persistent_connection_failure_is_bounded_and_safe():
    def handler(u, p):
        raise requests.ConnectionError("SYNTHETIC_SECRET_7f3a")
    session = FakeSession(handler)
    result = collectors._get_json(session, "synthetic", sleep=NO_SLEEP)
    assert result.code == "request_error:ConnectionError" and len(session.calls) == 3


@pytest.mark.parametrize("header", ["120", "Wed, 07 Oct 2026 00:02:00 GMT"])
def test_retry_after_date_or_seconds_beyond_deadline_does_not_sleep(header):
    from datetime import datetime, timezone
    epoch = datetime(2026, 10, 7, tzinfo=timezone.utc).timestamp()
    sleeps = []
    session = FakeSession(lambda u, p: FakeResponse(status=503, headers={"Retry-After": header}))
    result = collectors._get_json(session, "synthetic", sleep=sleeps.append, wall_time=lambda: epoch)
    assert result.code == "deadline_exceeded" and sleeps == [] and len(session.calls) == 1


def test_response_is_closed_before_backoff_and_stream_timeout_recovers():
    closed = []
    class Response(FakeResponse):
        def iter_content(self, chunk_size=1):
            if self.status_code == 200:
                yield b'{"ok":true}'
            else:
                raise requests.Timeout("SYNTHETIC_SECRET_7f3a")
        def close(self):
            closed.append(self)
    first, second = Response(status=200), Response(status=200)
    def timed_out(chunk_size=1):
        yield b"{"
        raise requests.Timeout("SYNTHETIC_SECRET_7f3a")
    first.iter_content = timed_out
    responses = iter([first, second])
    def sleep(seconds):
        assert closed == [first]
    result = collectors._get_json(FakeSession(lambda u, p: next(responses)), "synthetic",
                                  sleep=sleep, jitter=lambda: 1)
    assert result.ok and result.data == {"ok": True} and closed == [first, second]


def test_stream_exceeding_deadline_is_rejected_and_closed():
    now = [0.0]
    closed = []
    class Response(FakeResponse):
        def iter_content(self, chunk_size=1):
            now[0] = 61.0
            yield b"{}"
        def close(self):
            closed.append(True)
    session = FakeSession(lambda u, p: Response())
    result = collectors._get_json(session, "synthetic", monotonic=lambda: now[0])
    assert result.code == "deadline_exceeded" and closed == [True] and len(session.calls) == 1


def test_max_retries_override_cannot_make_unbounded_requests():
    session = FakeSession(lambda u, p: FakeResponse(status=500))
    result = collectors._get_json(session, "synthetic", sleep=NO_SLEEP, jitter=lambda: 0, max_retries=1000)
    assert result.code == "http_500" and len(session.calls) == 3


def test_failed_coverage_and_new_diagnostics_are_safe(capsys):
    from src.ingestion.diagnostics import safe_diagnostics
    body = si_rows()
    body["data"]["shortInterestTable"]["rows"][0]["interest"] = "SYNTHETIC_SECRET_7f3a"
    outcome = collectors.collect_short_interest(
        FakeSession(lambda u, p: FakeResponse(body=body)), sleep=NO_SLEEP, upstream=UPSTREAM)
    assert not outcome.succeeded and outcome.coverage is None
    captured = capsys.readouterr()
    assert "SYNTHETIC_SECRET_7f3a" not in captured.out + captured.err
    for code in ("deadline_exceeded", "pagination_exhausted"):
        clean = safe_diagnostics({"failures": [{"unit": "ZZAA", "code": code}]})
        assert clean["failures"][0]["code"] == code


@pytest.mark.parametrize("status,complete", [(404, True), (503, False)])
def test_insider_empty_or_failed_outcome_coverage(status, complete):
    result = collectors.collect_insider(
        FakeSession(lambda u, p: FakeResponse(status=status)), sleep=NO_SLEEP, upstream=UPSTREAM)
    assert result.succeeded is complete
    assert result.coverage == ({"complete": True, "scope": "selected_universe"} if complete else None)


def test_short_interest_explicit_no_data_has_complete_coverage():
    result = collectors.collect_short_interest(
        FakeSession(lambda u, p: FakeResponse(body={"data": None})), sleep=NO_SLEEP, upstream=UPSTREAM)
    assert result.succeeded and result.records == []
    assert result.coverage == {"complete": True, "scope": "selected_universe"}


def test_congress_identity_survives_normalization_without_database_column():
    from src.ingestion.schemas import CongressTrade
    fmp = collectors._fetcher("fmp")
    record = fmp.parse_trades([dict(fmp_item(), transactionId=123)])[0]
    parsed = CongressTrade.model_validate(record)
    assert parsed.canonical()["provider_record_id"] == "123"
    assert parsed.canonical()["data_source"] == "fmp"
    assert "provider_record_id" not in parsed.db_values()


@pytest.mark.parametrize("collect", [collectors.collect_insider, collectors.collect_short_interest])
@pytest.mark.parametrize("failed_index", [0, 2])
def test_fail_fast_stops_after_first_failed_ticker(collect, failed_index, monkeypatch):
    """F2: ten planned tickers; the failed unit alone gets its bounded HTTP retries."""
    from src.ingestion.diagnostics import safe_diagnostics
    tickers = [f"ZZ{i:02d}" for i in range(10)]
    upstream = {"trades": [], "holdings": [{"ticker": t, "fund_count": 1} for t in tickers]}
    requested, sleeps = [], []
    def handler(url, params):
        ticker = url.split("/quote/")[1].split("/")[0] if "/quote/" in url else url.split("/")[-2]
        requested.append(ticker)
        if ticker == tickers[failed_index]:
            return FakeResponse(status=500)
        # Both kinds are explicit no-record successes, including insider 404.
        return FakeResponse(body={"data": None}) if collect is collectors.collect_short_interest else FakeResponse(status=404)
    for name in ("insider", "short_interest"):
        monkeypatch.setattr(collectors._fetcher(name), "REQUEST_DELAY", 0.125)
    get_json = collectors._get_json
    monkeypatch.setattr(collectors, "_get_json", lambda *a, **kw: get_json(*a, **kw, jitter=lambda: 0))
    result = collect(FakeSession(handler), upstream=upstream, sleep=sleeps.append)
    assert requested == tickers[:failed_index] + [tickers[failed_index]] * 3
    assert result.error_code == ("partial_collection" if failed_index else "collection_failed")
    assert result.coverage is None
    expected = {"planned": 10, "attempted": failed_index + 1, "completed": failed_index,
                "failed": 1, "not_attempted": 9 - failed_index}
    assert {k: result.diagnostics[k] for k in expected} == expected
    clean = safe_diagnostics(result.diagnostics)
    assert {k: clean[k] for k in expected} == expected
    assert sleeps == [0.125] * failed_index + [1, 2]  # no REQUEST_DELAY after failure


@pytest.mark.parametrize("collect", [collectors.collect_insider, collectors.collect_short_interest])
def test_fail_fast_also_stops_on_malformed_response(collect):
    upstream = {"trades": [], "holdings": [{"ticker": f"ZZ{i:02d}", "fund_count": 1} for i in range(10)]}
    session = FakeSession(lambda u, p: FakeResponse(body={}))
    sleeps = []
    result = collect(session, upstream=upstream, sleep=sleeps.append)
    assert len(session.calls) == 1 and sleeps == []
    assert result.error_code == "collection_failed"
    assert result.diagnostics["not_attempted"] == 9
    assert result.diagnostics["failures"] == [{"unit": "ZZ00", "code": "malformed_response"}]


def test_fail_fast_ark_stops_after_failed_fund_without_delay():
    session = FakeSession(lambda u, p: FakeResponse(status=401))
    sleeps = []
    result = collectors.collect_ark(session, sleep=sleeps.append)
    assert len(session.calls) == 1 and sleeps == []
    assert result.error_code == "collection_failed"
    assert result.diagnostics["planned"] == 4 and result.diagnostics["not_attempted"] == 3


def test_fail_fast_congress_stops_before_other_chamber_and_watcher():
    session = FakeSession(lambda u, p: FakeResponse(status=401))
    sleeps = []
    result = collectors.collect_congress(session, api_key="synthetic", sleep=sleeps.append)
    assert len(session.calls) == 1 and sleeps == []
    assert result.error_code == "collection_failed"
    assert result.diagnostics["failures"] == [{"unit": "senate page 0", "code": "http_401"}]


def test_fail_fast_short_interest_parse_error_stops_remaining_plan():
    body = si_rows()
    body["data"]["shortInterestTable"]["rows"][0]["interest"] = "malformed"
    upstream = {"trades": [], "holdings": [{"ticker": f"ZZ{i:02d}", "fund_count": 1} for i in range(10)]}
    session = FakeSession(lambda u, p: FakeResponse(body=body))
    sleeps = []
    result = collectors.collect_short_interest(session, upstream=upstream, sleep=sleeps.append)
    assert len(session.calls) == 1 and sleeps == []
    assert result.diagnostics["failures"] == [{"unit": "ZZ00", "code": "parse_error"}]


def test_planned_counts_are_bounded_in_safe_diagnostics():
    from src.ingestion.diagnostics import safe_diagnostics
    assert safe_diagnostics({"planned": 10, "not_attempted": 7}) == {"planned": 10, "not_attempted": 7}
    assert safe_diagnostics({"planned": True, "not_attempted": 10**30}) is None


def test_worker_abandoned_has_fixed_safe_template():
    from src.ingestion.diagnostics import safe_code, render_summary
    assert safe_code("worker_abandoned") == "worker_abandoned"
    assert render_summary("worker_abandoned", None) == "Worker heartbeat expired."


@pytest.fixture
def legacy_golden(fixture_path):
    """Golden generated by actual fab7188 run/save functions imported in scratch.

    /private/tmp/perennial-pipeline-rig/generate-legacy-golden-20261007.py
    extracted git show fab7188 modules; synthetic fetches and a frozen UTC clock
    produced both legacy JSON files and Congress signals. Its database hash was
    pinned from the pre-split strict collector, preserving all three observations.
    """
    return json.loads(fixture_path("legacy_json_golden_fab7188.json").read_text())


def _freeze_golden_time(module, monkeypatch, golden):
    frozen = datetime.fromisoformat(golden["frozen_now"])
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return frozen if tz is not None else frozen.replace(tzinfo=None)
    monkeypatch.setattr(module, "datetime", FrozenDateTime)


def test_legacy_fmp_run_matches_fab7188_golden_and_buy_count(legacy_golden, monkeypatch, tmp_path):
    """F1: compare the actual legacy JSON writer and consensus loader to fab7188."""
    from src.services.consensus_watchlist import consensus
    fmp = collectors._fetcher("fmp")
    _freeze_golden_time(fmp, monkeypatch, legacy_golden)
    monkeypatch.setattr(fmp, "FMP_API_KEY", "synthetic-golden-key")
    monkeypatch.setattr(fmp, "LOCAL_DATA_DIR", tmp_path)
    inputs = legacy_golden["inputs"]
    monkeypatch.setattr(fmp, "fetch_chamber_trades", lambda c: copy.deepcopy(inputs["fmp_" + c]))
    monkeypatch.setattr(fmp, "fetch_senate_watcher", lambda: copy.deepcopy(inputs["senate_watcher"]))
    fmp.run()
    path = tmp_path / "trades_congress.json"
    actual = json.loads(path.read_text())
    assert actual == legacy_golden["expected"]["trades_congress.json"]
    signals = consensus.load_congress_signals(path)
    assert signals == legacy_golden["expected"]["congress_signals"]
    assert signals["ZZAA"]["buy_count"] == 1


def test_legacy_short_interest_run_keeps_malformed_ticker_like_fab7188(legacy_golden, monkeypatch, tmp_path):
    """F1: real run/fetch/save path retains ticker and replaces only bad number with null."""
    si = collectors._fetcher("short_interest")
    _freeze_golden_time(si, monkeypatch, legacy_golden)
    inputs = legacy_golden["inputs"]["short_interest"]
    monkeypatch.setattr(si, "resolve_data_dir", lambda: tmp_path)
    monkeypatch.setattr(si, "get_candidate_tickers", lambda: [inputs["ticker"]])
    monkeypatch.setattr(si.time, "sleep", NO_SLEEP)
    class Session:
        def get(self, *a, **kw):
            class Response:
                status_code = 200
                def json(self):
                    return copy.deepcopy(inputs["payload"])
            return Response()
    monkeypatch.setattr(si.requests, "Session", Session)
    si.run()
    actual = json.loads((tmp_path / "short_interest.json").read_text())
    assert actual == legacy_golden["expected"]["short_interest.json"]
    assert actual["records"][0]["ticker"] == "ZZAA"
    assert actual["records"][0]["short_interest"] is None
    assert actual["records"][0]["history"][1]["short_interest"] == 1000


def _golden_congress_session(golden):
    inputs = golden["inputs"]
    def handler(url, params):
        if url.endswith("senate-latest"):
            return FakeResponse(body=inputs["fmp_senate"])
        if url.endswith("house-latest"):
            return FakeResponse(body=inputs["fmp_house"])
        return FakeResponse(body=inputs["senate_watcher"])
    return FakeSession(handler)


def test_database_congress_golden_hash_and_observations_stay_unchanged(legacy_golden, monkeypatch):
    from src.ingestion.canonical import canonicalize
    from src.ingestion.importer import validate_records
    fmp = collectors._fetcher("fmp")
    _freeze_golden_time(fmp, monkeypatch, legacy_golden)
    result = collectors.collect_congress(_golden_congress_session(legacy_golden), api_key="synthetic", sleep=NO_SLEEP)
    assert result.succeeded
    duplicate = [r for r in result.records if r["ticker"] == "ZZAA"]
    assert len(duplicate) == 2 and {r["data_source"] for r in duplicate} == {"fmp", "senate_watcher"}
    batch = canonicalize("congress_trades", validate_records("congress_trades", result.records))
    assert batch.content_hash == legacy_golden["database"]["content_hash"]
    assert batch.payload["records"] == legacy_golden["database"]["records"]


def test_database_collectors_cannot_reach_legacy_dedup_or_lenient_parser(legacy_golden, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("database collector reached a legacy compatibility function")
    fmp = collectors._fetcher("fmp")
    si = collectors._fetcher("short_interest")
    _freeze_golden_time(fmp, monkeypatch, legacy_golden)
    monkeypatch.setattr(fmp, "deduplicate", forbidden)
    monkeypatch.setattr(si, "parse_short_interest", forbidden)
    congress = collectors.collect_congress(_golden_congress_session(legacy_golden), api_key="synthetic", sleep=NO_SLEEP)
    assert congress.succeeded and len(congress.records) == 3
    malformed = legacy_golden["inputs"]["short_interest"]["payload"]
    result = collectors.collect_short_interest(FakeSession(lambda u, p: FakeResponse(body=malformed)), upstream=UPSTREAM, sleep=NO_SLEEP)
    assert not result.succeeded and result.diagnostics["failures"][0]["code"] == "parse_error"
    result = collectors.collect_short_interest(FakeSession(lambda u, p: FakeResponse(body=si_rows())), upstream=UPSTREAM, sleep=NO_SLEEP)
    assert result.succeeded and result.records[0]["short_interest_shares"] == 1000
