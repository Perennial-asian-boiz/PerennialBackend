"""Short interest: slash symbols and Nasdaq 404s, spec-first.

Staging run 4 (main 27bb4f0): 897 planned, 281 attempted, 280 completed, 1
failed with http_404, 616 not attempted. The collector stopped at that first
failure, so the source failed (partial_collection) and the run ended
required_source_unavailable, deterministically on every rerun.

The rule pinned here (sources may tune the share; the tests read it):
- a 404 for a ticker counts like Nasdaq's explicit empty answer: completed, not
  a failure, no rows, and the collector carries on;
- but 404s that look like an outage or an endpoint change still fail the
  source (scope.md: no false success): every attempted ticker 404s, or more than
  collectors.SHORT_INTEREST_MAX_404_SHARE of the planned tickers do. Assumed 0.05:
  a few delisted or renamed tickers in a plan of hundreds, never a broken
  endpoint. Small plans are strict by design (1 of 2 is 50%).
- 429, 5xx, timeouts and invalid JSON fail exactly as before.

The 404 ticker was ETHQ/U, a unit: the Nasdaq URL is built from the symbol as a
path segment (NASDAQ_URL = .../api/quote/{symbol}/short-interest), so a "/" made
.../quote/ETHQ/U/short-interest, our malformed URL rather than Nasdaq having no
record. So a symbol Nasdaq's path can't address (units, warrants, rights or any
other form written with "/") is handled before any request: left out of the
plan, or recorded as not applicable, never sent. Only a genuine 404 for a
well-formed symbol is "no record", with the outage guard.

Fake HTTP only (test_collectors' FakeSession); no Nasdaq call is ever made.
"""

import math

import pytest
import requests
from src.ingestion import collectors
from test_collectors import NO_SLEEP, FakeResponse, FakeSession, si_rows

NONE = {"data": None}  # Nasdaq's explicit "no short-interest record"


def tickers(n):
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return [f"ZQ{letters[i // 26]}{letters[i % 26]}" for i in range(n)]


def upstream(symbols):
    return {"trades": [], "holdings": [{"ticker": t, "fund_count": 1} for t in symbols]}


def session(answers, default=None):
    """answers: ticker -> FakeResponse, or an exception to raise."""

    def handler(url, params):
        ticker = url.split("/quote/")[1].split("/")[0]
        answer = answers.get(ticker, default or FakeResponse(body=si_rows()))
        if isinstance(answer, Exception):
            raise answer
        return answer

    return FakeSession(handler)


def collect(symbols, answers, default=None):
    fake = session(answers, default)
    outcome = collectors.collect_short_interest(
        fake, sleep=NO_SLEEP, upstream=upstream(symbols)
    )
    return outcome, [url.split("/quote/")[1].split("/")[0] for url, _ in fake.calls]


def stored(outcome):
    """The records, less fetched_at (each run's own timestamp)."""
    return [{k: v for k, v in r.items() if k != "fetched_at"} for r in outcome.records]


def not_found():
    return FakeResponse(404, {"message": "synthetic not found"})


# ── one 404 is "no record" ───────────────────────────────────


def test_a_404_is_recorded_like_an_explicit_empty_answer_and_the_run_continues():
    symbols = tickers(100)
    missing = symbols[40]
    outcome, requested = collect(symbols, {missing: not_found()})
    assert outcome.succeeded, outcome.error_code
    assert requested == symbols  # it carried on past the 404
    assert missing not in {r["ticker"] for r in outcome.records}
    assert len(outcome.records) == 99
    d = outcome.diagnostics
    assert (d["planned"], d["attempted"], d["completed"], d["failed"]) == (
        100,
        100,
        100,
        0,
    )
    assert d["failures"] == [] and d["not_attempted"] == 0


def test_a_404_and_an_explicit_empty_answer_give_the_same_stored_data():
    symbols = tickers(100)
    missing = symbols[7]
    with_404, _ = collect(symbols, {missing: not_found()})
    with_none, _ = collect(symbols, {missing: FakeResponse(body=NONE)})
    assert with_404.succeeded and with_none.succeeded
    assert stored(with_404) == stored(with_none)  # nothing else changes
    assert with_404.diagnostics["failures"] == with_none.diagnostics["failures"] == []


def test_the_first_ticker_404_does_not_stop_the_rest():
    symbols = tickers(50)
    outcome, requested = collect(symbols, {symbols[0]: not_found()})
    assert outcome.succeeded and requested == symbols and len(outcome.records) == 49


# ── the guard: 404s that look like an outage still fail ──────


def share():
    value = getattr(collectors, "SHORT_INTEREST_MAX_404_SHARE")
    assert 0.01 <= value < 0.5, (
        "a share that tolerates an outage, or not even one ticker in 100"
    )
    return value


def test_404s_up_to_the_share_of_the_plan_are_tolerated():
    symbols = tickers(100)
    allowed = math.floor(len(symbols) * share())
    assert allowed >= 1
    outcome, requested = collect(symbols, {t: not_found() for t in symbols[:allowed]})
    assert outcome.succeeded and requested == symbols
    assert len(outcome.records) == len(symbols) - allowed


def test_404s_over_the_share_of_the_plan_fail_the_source():
    symbols = tickers(100)
    over = math.floor(len(symbols) * share()) + 1
    outcome, _ = collect(symbols, {t: not_found() for t in symbols[:over]})
    assert not outcome.succeeded and outcome.error_code
    assert any(f["code"] == "http_404" for f in outcome.diagnostics["failures"])


@pytest.mark.parametrize(
    "n", [1, 3, 100], ids=["one_of_one", "three_of_three", "all_100"]
)
def test_every_attempted_ticker_404ing_fails_the_source(n):
    outcome, _ = collect(tickers(n), {}, default=not_found())
    assert not outcome.succeeded and outcome.error_code
    assert any(f["code"] == "http_404" for f in outcome.diagnostics["failures"])


def test_half_the_plan_404ing_is_an_outage_not_missing_records():
    symbols = tickers(100)
    outcome, _ = collect(symbols, {t: not_found() for t in symbols[::2]})
    assert not outcome.succeeded


# ── other errors fail as before ──────────────────────────────


@pytest.mark.parametrize(
    "answer, code",
    [
        (FakeResponse(429, {"message": "synthetic"}), "http_429"),
        (FakeResponse(500, {"message": "synthetic"}), "http_500"),
        (FakeResponse(503, {"message": "synthetic"}), "http_503"),
        (requests.Timeout("synthetic"), "timeout"),
        (FakeResponse(200, raw=b"<html>synthetic</html>"), "invalid_json"),
        (FakeResponse(200, {"data": {"foo": 1}}), "malformed_response"),
    ],
    ids=["http_429", "http_500", "http_503", "timeout", "invalid_json", "malformed"],
)
def test_other_errors_still_fail_the_source(answer, code):
    symbols = tickers(20)
    outcome, _ = collect(symbols, {symbols[5]: answer})
    assert not outcome.succeeded and outcome.error_code == "partial_collection"
    assert outcome.diagnostics["failures"][0] == {"unit": symbols[5], "code": code}


def test_a_404_does_not_excuse_another_error_later():
    symbols = tickers(20)
    outcome, _ = collect(
        symbols,
        {
            symbols[2]: not_found(),
            symbols[9]: FakeResponse(500, {"message": "synthetic"}),
        },
    )
    assert not outcome.succeeded
    assert {"unit": symbols[9], "code": "http_500"} in outcome.diagnostics["failures"]


def test_all_explicit_empty_answers_are_still_empty_short_interest():
    outcome, _ = collect(tickers(5), {}, default=FakeResponse(body=NONE))
    assert not outcome.succeeded and outcome.error_code == "empty_short_interest"


# ── symbols Nasdaq's path can't address ─────────────────────

SLASH_SYMBOLS = ["ETHQ/U", "ZQWW/WS", "ZQRR/R", "ZQUU/U", "BRK/B"]


def path_segments(fake_calls):
    """The {symbol} part of each requested short-interest URL, as sent."""
    return [
        url.split("/api/quote/", 1)[1].split("/short-interest", 1)[0]
        for url, _ in fake_calls
    ]


@pytest.mark.parametrize("symbol", SLASH_SYMBOLS)
def test_a_slash_symbol_is_never_sent_as_a_malformed_url(symbol):
    symbols = tickers(20)
    fake = session({})
    outcome = collectors.collect_short_interest(
        fake, sleep=NO_SLEEP, upstream=upstream(symbols + [symbol])
    )
    sent = path_segments(fake.calls)
    assert all("/" not in segment for segment in sent), sent
    assert not any(symbol.split("/")[0] in url for url, _ in fake.calls), (
        "it was requested"
    )
    assert sorted(sent) == symbols
    assert outcome.succeeded, outcome.error_code
    assert symbol not in {r["ticker"] for r in outcome.records}
    d = outcome.diagnostics
    assert d["failures"] == [] and d["failed"] == 0
    # Accounted for, not silently dropped: out of the plan, counted as not
    # applicable, or listed as unsupported_tickers (insider's pattern, in the envelope).
    not_applicable = sum(
        v for k, v in d.items() if "not_applicable" in k and isinstance(v, int)
    )
    unsupported = [
        *((outcome.coverage or {}).get("unsupported_tickers") or []),
        *((outcome.envelope or {}).get("unsupported_tickers") or []),
    ]
    assert d["planned"] == 20 or not_applicable == 1 or symbol in unsupported, d


def test_ethq_u_beside_a_genuine_404_still_tolerates_only_the_genuine_one():
    """ETHQ/U never reaches Nasdaq, and a real 404 for a well-formed ticker is no record."""
    symbols = tickers(50)
    fake = session({symbols[3]: not_found()})
    outcome = collectors.collect_short_interest(
        fake, sleep=NO_SLEEP, upstream=upstream(symbols + ["ETHQ/U"])
    )
    assert outcome.succeeded
    assert sorted(path_segments(fake.calls)) == symbols
    assert len(outcome.records) == 49


def test_a_plan_of_only_slash_symbols_makes_no_request_and_does_not_succeed():
    fake = session({})
    outcome = collectors.collect_short_interest(
        fake, sleep=NO_SLEEP, upstream=upstream(["ETHQ/U", "ZQWW/WS"])
    )
    assert fake.calls == []
    assert not outcome.succeeded  # nothing collected is never a quiet success
