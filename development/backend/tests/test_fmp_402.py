"""FMP premium-only symbols (decision 84), spec-first.

On staging (main 50fa623), FMP's free plan answers /stable/profile?symbol=BRK.B
with HTTP 402, and one 402 sinks everything: collect_market_caps raises
PublicationError (publish then refuses with every cap unavailable), and the
summary job fails the whole run.

The rule pinned here:
- A 402 for one symbol means "not on this plan" for that symbol only.
  - Market caps: the symbol's cap is unavailable, exactly like FMP's 200 [], and
    collection continues. Ranking then reports it in unresolved with
    market_cap_unavailable (decision 12).
  - Summaries: that member gets no new summary and the run continues. A
    member with a stored summary keeps its row untouched (stale-good).
- The guard (as #65): if every requested symbol 402s, or more than
  collectors.FMP_MAX_402_SHARE of them do, it's a plan or key problem and the
  source or run fails as today. Suggested 0.25; the tests read the constant.
- 401, 403, 429, 5xx, timeouts, malformed, mismatched or non-USD payloads and a
  missing key stay fatal, and a 402 never excuses a later fatal error.
- The 402 body is never read or shown, and the key never appears in an error,
  log or count. 402s are counted where staging can see them.

Fake HTTP only (test_collectors' FakeSession); no FMP call is ever made.
"""

import logging
import math

import pytest
import requests
from sqlalchemy import text
from test_collectors import FakeResponse, FakeSession
from test_read_api import seed_security

KEY = "synthetic-fmp-key-0123456789"
PREMIUM_BODY = (
    "Premium Query Parameter synthetic: not available under your current subscription"
)
DESCRIPTION = (
    "The firm makes tools. It sells them in shops. Its staff work in one town. "
    "The shops are near homes."
)


def symbols(n, *, with_brk=True):
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    plain = [
        f"ZQ{letters[i // 26]}{letters[i % 26]}"
        for i in range(n - (1 if with_brk else 0))
    ]
    return sorted(plain + (["BRK.B"] if with_brk else []))


def premium():
    return FakeResponse(402, {"Error Message": PREMIUM_BODY})


def profile(symbol, **fields):
    item = {
        "symbol": symbol,
        "currency": "USD",
        "marketCap": 1_000_000_000 + len(symbol),
        "sector": "Industrials",
        "industry": "Tools",
        "description": DESCRIPTION,
    }
    item.update(fields)
    return [item]


def fmp(answers):
    """symbol -> FakeResponse or an exception; anything else gets a normal profile."""

    def handler(url, params):
        symbol = params["symbol"]
        answer = answers.get(symbol)
        if isinstance(answer, Exception):
            raise answer
        return answer if answer is not None else FakeResponse(200, profile(symbol))

    return FakeSession(handler)


def share():
    from src.ingestion import collectors

    value = getattr(collectors, "FMP_MAX_402_SHARE")
    assert 0.05 <= value < 0.5, (
        "a share that hides a plan change, or not even one symbol in 20"
    )
    return value


@pytest.fixture
def no_retries(monkeypatch):
    from src.ingestion import collectors

    monkeypatch.setattr(collectors, "MAX_HTTP_RETRIES", 0)


def no_secret_or_body(*texts):
    for value in texts:
        assert KEY not in value and "Premium Query Parameter" not in value, value


# ── market caps: collect_market_caps ─────────────────────────


def caps(plan, answers):
    from src.pipeline.runner import collect_market_caps

    session = fmp(answers)
    return collect_market_caps(plan, session=session, api_key=KEY), session


def comparable(observation):
    return {k: v for k, v in observation.items() if k != "retrieved_at"}


def test_brk_b_402_is_an_unavailable_cap_and_collection_continues():
    plan = symbols(20)
    observations, session = caps(plan, {"BRK.B": premium()})
    assert [p["symbol"] for _, p in session.calls] == plan  # it carried on
    brk = observations["BRK.B"]
    assert (brk["value"], brk["sector"], brk["industry"]) == (None, None, None)
    assert all(observations[s]["value"] is not None for s in plan if s != "BRK.B")


def test_a_402_cap_is_exactly_fmps_empty_answer():
    plan = symbols(20)
    with_402, _ = caps(plan, {"BRK.B": premium()})
    with_empty, _ = caps(plan, {"BRK.B": FakeResponse(200, [])})
    assert {s: comparable(o) for s, o in with_402.items()} == {
        s: comparable(o) for s, o in with_empty.items()
    }


def test_402s_up_to_the_share_publish_as_unavailable():
    plan = symbols(40)
    allowed = math.floor(len(plan) * share())
    assert allowed >= 1
    observations, _ = caps(plan, {s: premium() for s in plan[:allowed]})
    assert sum(o["value"] is None for o in observations.values()) == allowed


def test_402s_over_the_share_fail_market_caps():
    from src.pipeline.publication import PublicationError

    plan = symbols(40)
    over = math.floor(len(plan) * share()) + 1
    with pytest.raises(PublicationError) as caught:
        caps(plan, {s: premium() for s in plan[:over]})
    no_secret_or_body(str(caught.value), repr(caught.value))


@pytest.mark.parametrize(
    "n", [1, 3, 20], ids=["one_of_one", "three_of_three", "all_20"]
)
def test_every_symbol_402ing_fails_market_caps(n):
    from src.pipeline.publication import PublicationError

    plan = symbols(n, with_brk=False)
    with pytest.raises(PublicationError):
        caps(plan, {s: premium() for s in plan})


FATAL = {
    "http_401": FakeResponse(401, {"Error Message": "synthetic"}),
    "http_403": FakeResponse(403, {"Error Message": "synthetic"}),
    "http_429": FakeResponse(429, {"Error Message": "synthetic"}),
    "http_500": FakeResponse(500, {"Error Message": "synthetic"}),
    "timeout": requests.Timeout("synthetic"),
    "malformed": FakeResponse(200, {"symbol": "x"}),
    "mismatched": "MISMATCHED",
    "non_usd": "NON_USD",
}


def fatal_answer(case, symbol):
    if FATAL[case] == "MISMATCHED":
        return FakeResponse(200, profile("ZZZZ"))
    if FATAL[case] == "NON_USD":
        return FakeResponse(200, profile(symbol, currency="EUR"))
    return FATAL[case]


@pytest.mark.parametrize("case", sorted(FATAL))
def test_other_market_cap_errors_stay_fatal_even_after_a_402(no_retries, case):
    from src.pipeline.publication import PublicationError

    plan = symbols(20)
    later = plan[-1]
    with pytest.raises(PublicationError):
        caps(plan, {"BRK.B": premium(), later: fatal_answer(case, later)})


def test_a_missing_key_stays_fatal_for_market_caps():
    from src.pipeline.publication import PublicationError
    from src.pipeline.runner import collect_market_caps

    session = fmp({})
    with pytest.raises(PublicationError):
        collect_market_caps(symbols(5), session=session, api_key="your_fmp_key_here")
    assert session.calls == []


def test_market_cap_402s_are_counted_in_the_log_without_key_or_body(caplog):
    caplog.set_level(logging.DEBUG)
    caplog.clear()
    caps(symbols(20), {"BRK.B": premium()})
    messages = [r.getMessage() for r in caplog.records]
    assert any("402" in m and "1" in m for m in messages), messages
    no_secret_or_body(*messages)


def test_the_fallback_path_is_not_taken_for_one_402():
    from src.pipeline.runner import collect_market_caps, market_caps_or_fallback

    plan = symbols(20)
    session = fmp({"BRK.B": premium()})
    observations, fell_back = market_caps_or_fallback(
        None,  # the engine is only read on a fallback
        plan,
        lambda s: collect_market_caps(s, session=session, api_key=KEY),
        max_reuse_age=__import__("datetime").timedelta(days=7),
    )
    assert fell_back is False and observations["BRK.B"]["value"] is None


# ── summaries: summarize through the real FMP adapter ────────


def summary_run(engine, plan, answers):
    from src.ingestion.company_summaries import summarize
    from src.ingestion.fmp_descriptions import FmpProfileDescriptions

    adapter = FmpProfileDescriptions(KEY, session=fmp(answers))
    return summarize(engine, adapter, symbols=plan)


def seeded(engine, plan):
    for symbol in plan:
        seed_security(
            engine,
            symbol,
            name=f"Synthetic {symbol}",
            profile={"sector": "Industrials"},
        )
    return plan


def rows(engine, plan):
    with engine.connect() as conn:
        return {
            r.symbol: (r.summary_status, r.summary, r.xmin)
            for r in conn.execute(
                text(
                    "SELECT s.symbol, p.summary_status, p.summary, p.xmin::text AS xmin"
                    " FROM perennial.company_profiles p JOIN perennial.securities s"
                    " ON s.company_profile_id = p.id WHERE s.symbol = ANY(:s)"
                ),
                {"s": list(plan)},
            )
        }


@pytest.mark.db
def test_a_402_member_gets_no_summary_and_the_others_are_written(engine, caplog):
    plan = seeded(engine, symbols(20))
    caplog.set_level(logging.DEBUG)
    result = summary_run(engine, plan, {"BRK.B": premium()})
    assert result.succeeded, result.error_code
    stored = rows(engine, plan)
    assert stored["BRK.B"][1] is None  # "Summary not available"
    assert all(stored[s][0] == "ok" for s in plan if s != "BRK.B")
    assert result.with_summary == 19 and getattr(result, "skipped_402") == 1
    no_secret_or_body(*(r.getMessage() for r in caplog.records), repr(result))


@pytest.mark.db
def test_a_402_leaves_a_stored_summary_untouched(engine):
    plan = seeded(engine, symbols(20))
    assert summary_run(engine, plan, {}).succeeded
    before = rows(engine, plan)["BRK.B"]
    assert before[0] == "ok"
    result = summary_run(engine, plan, {"BRK.B": premium()})
    assert result.succeeded
    assert rows(engine, plan)["BRK.B"] == before  # stale-good: same row, same xmin


@pytest.mark.db
def test_402s_over_the_share_fail_the_summary_run_and_write_nothing(engine):
    plan = seeded(engine, symbols(40))
    before = rows(engine, plan)
    over = math.floor(len(plan) * share()) + 1
    result = summary_run(engine, plan, {s: premium() for s in plan[:over]})
    assert not result.succeeded and result.error_code
    assert rows(engine, plan) == before


@pytest.mark.db
def test_402s_up_to_the_share_let_the_summary_run_succeed(engine):
    plan = seeded(engine, symbols(40))
    allowed = math.floor(len(plan) * share())
    result = summary_run(engine, plan, {s: premium() for s in plan[:allowed]})
    assert result.succeeded and result.skipped_402 == allowed
    assert result.with_summary == len(plan) - allowed


@pytest.mark.db
@pytest.mark.parametrize("n", [1, 3], ids=["one_of_one", "three_of_three"])
def test_every_member_402ing_fails_the_summary_run(engine, n):
    plan = seeded(engine, symbols(n, with_brk=False))
    before = rows(engine, plan)
    result = summary_run(engine, plan, {s: premium() for s in plan})
    assert not result.succeeded
    assert rows(engine, plan) == before


@pytest.mark.db
@pytest.mark.parametrize("case", ["http_401", "http_500", "timeout", "mismatched"])
def test_a_402_never_excuses_a_later_fatal_summary_error(engine, no_retries, case):
    plan = seeded(engine, symbols(20))
    before = rows(engine, plan)
    later = plan[-1]
    result = summary_run(
        engine, plan, {"BRK.B": premium(), later: fatal_answer(case, later)}
    )
    assert not result.succeeded
    assert rows(engine, plan) == before
