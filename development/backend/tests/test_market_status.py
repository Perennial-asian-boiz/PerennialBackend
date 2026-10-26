"""S1-11: GET /v1/market-status (MS-1 to MS-7), spec-first.

Spec: docs/work-items/s1-stories-plan.md (S1-11 and the F4 contract), on
docs/s1-stories-plan @ 8cc9a21. The tests pin behaviour, not a calendar
library (Q8 is open), and import none.

The seam: a module src.api.market_status whose now() returns the current time
as an aware UTC datetime; the route is on src.api.read.router, so it carries
#52's read bucket. Each test patches now(), so nothing depends on today's date.

Fixture dates are confirmed against NYSE's published 2026 calendar
(nyse.com/markets/hours-calendars, read 2026-10-25). Holidays: Jan 1, Jan 19,
Feb 16, Apr 3 (Good Friday), May 25, Jun 19, Jul 3 (Independence Day observed),
Sep 7, Nov 26 and Dec 25. Early closes at 1:00 p.m. ET: Fri Nov 27 and Thu
Dec 24 (none on Jul 2). Daylight saving time: Sun Mar 8 and Sun Nov 1, 2026.

The app is built on an engine that can't connect, and TestClient isn't entered,
so startup's database check never runs: the endpoint touches no database.
"""

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

ET = ZoneInfo("America/New_York")
REPO = Path(__file__).resolve().parents[3]
REGULAR_NOTE = (
    "Regular hours, 9:30 a.m.–4:00 p.m. ET. Some brokers allow extended-hours trading."
)
EARLY_NOTE = (
    "Regular hours, 9:30 a.m.–1:00 p.m. ET (early close). "
    "Some brokers allow extended-hours trading."
)
KEYS = {
    "exchange",
    "is_open",
    "session",
    "label",
    "note",
    "as_of",
    "next_open",
    "next_close",
    "early_close",
}
OPEN_EXAMPLE = {  # the plan's open example, verbatim
    "exchange": "XNYS",
    "is_open": True,
    "session": "regular",
    "label": "Market Open",
    "note": REGULAR_NOTE,
    "as_of": "2026-10-26T14:00:00Z",
    "next_close": "2026-10-26T16:00:00-04:00",
    "next_open": "2026-10-27T09:30:00-04:00",
    "early_close": False,
}
CLOSED_EXAMPLE = {  # the plan's closed example (a Saturday), verbatim
    "exchange": "XNYS",
    "is_open": False,
    "session": "closed",
    "label": "Market Closed",
    "note": REGULAR_NOTE,
    "as_of": "2026-10-31T16:00:00Z",
    "next_open": "2026-11-02T09:30:00-05:00",
    "next_close": "2026-11-02T16:00:00-05:00",
    "early_close": False,
}
RATE_LIMITED = {"detail": "too many requests; retry later", "code": "rate_limited"}


def et(*parts):
    """A wall-clock time in New York, as the aware UTC instant now() returns."""
    return datetime(*parts, tzinfo=ET).astimezone(timezone.utc)


class Clock:
    def __init__(self, at):
        self.at, self.calls = at, 0

    def __call__(self):
        self.calls += 1
        return self.at


def _app(burst=10000):
    from sqlalchemy import create_engine, event
    from src.api.read import RateLimitConfig, create_app

    engine = create_engine("postgresql+psycopg://synthetic@127.0.0.1:9/unreachable")
    used = []
    event.listen(engine, "connect", lambda *a: used.append("connect"))
    event.listen(engine, "checkout", lambda *a: used.append("checkout"))
    app = create_app(engine, rate_limit=RateLimitConfig(burst=burst))
    return app, used


@pytest.fixture
def market(monkeypatch):
    """status(at) -> the response at that instant, from one app instance."""
    from fastapi.testclient import TestClient
    from src.api import market_status

    app, used = _app()
    client = TestClient(app)  # not entered: no startup check, no database

    def status(at, **kwargs):
        clock = Clock(at)
        monkeypatch.setattr(market_status, "now", clock)
        response = client.get("/v1/market-status", **kwargs)
        assert response.status_code == 200, response.text
        return response

    yield status
    assert used == [], "the endpoint used the database engine"


def body(market, at):
    return market(at).json()


def max_age(response):
    match = re.fullmatch(r"public, max-age=(\d+)", response.headers["cache-control"])
    assert match, response.headers["cache-control"]
    return int(match.group(1))


# ── MS-1: open ───────────────────────────────────────────────


def test_ms1_the_plans_open_example_monday_10_00_et(market):
    assert body(market, et(2026, 10, 26, 10, 0)) == OPEN_EXAMPLE


@pytest.mark.parametrize(
    "at",
    [et(2026, 10, 26, 9, 30), et(2026, 10, 26, 15, 59, 59)],
    ids=["09_30_exactly", "15_59_59"],
)
def test_ms1_open_through_the_whole_regular_session(market, at):
    status = body(market, at)
    assert status["is_open"] is True and status["session"] == "regular"
    assert status["label"] == "Market Open"
    assert status["next_close"] == "2026-10-26T16:00:00-04:00"
    assert status["next_open"] == "2026-10-27T09:30:00-04:00"


# ── MS-2: closed ─────────────────────────────────────────────

CLOSED = {
    "saturday_noon": (
        et(2026, 10, 31, 12, 0),
        "2026-11-02T09:30:00-05:00",
        "2026-11-02T16:00:00-05:00",
    ),
    "friday_16_00_exactly": (
        et(2026, 10, 23, 16, 0),
        "2026-10-26T09:30:00-04:00",
        "2026-10-26T16:00:00-04:00",
    ),
    "weekday_08_00": (
        et(2026, 10, 27, 8, 0),
        "2026-10-27T09:30:00-04:00",
        "2026-10-27T16:00:00-04:00",
    ),
    "weekday_after_close": (
        et(2026, 10, 26, 16, 0),
        "2026-10-27T09:30:00-04:00",
        "2026-10-27T16:00:00-04:00",
    ),
    "holiday_new_years_day": (
        et(2026, 1, 1, 12, 0),
        "2026-01-02T09:30:00-05:00",
        "2026-01-02T16:00:00-05:00",
    ),
    "holiday_mlk_from_the_weekend": (
        et(2026, 1, 17, 12, 0),
        "2026-01-20T09:30:00-05:00",
        "2026-01-20T16:00:00-05:00",
    ),
    "holiday_good_friday": (
        et(2026, 4, 3, 12, 0),
        "2026-04-06T09:30:00-04:00",
        "2026-04-06T16:00:00-04:00",
    ),
    "holiday_juneteenth": (
        et(2026, 6, 19, 12, 0),
        "2026-06-22T09:30:00-04:00",
        "2026-06-22T16:00:00-04:00",
    ),
    "holiday_independence_observed": (
        et(2026, 7, 3, 12, 0),
        "2026-07-06T09:30:00-04:00",
        "2026-07-06T16:00:00-04:00",
    ),
    "holiday_christmas": (
        et(2026, 12, 25, 12, 0),
        "2026-12-28T09:30:00-05:00",
        "2026-12-28T16:00:00-05:00",
    ),
}


def test_ms2_the_plans_closed_example_saturday_noon(market):
    assert body(market, et(2026, 10, 31, 12, 0)) == CLOSED_EXAMPLE


@pytest.mark.parametrize("case", sorted(CLOSED))
def test_ms2_closed_gives_the_next_sessions_open_and_close(market, case):
    at, next_open, next_close = CLOSED[case]
    status = body(market, at)
    assert set(status) == KEYS
    assert status["is_open"] is False and status["session"] == "closed"
    assert status["label"] == "Market Closed"
    assert (status["next_open"], status["next_close"]) == (next_open, next_close)
    assert status["early_close"] is False and status["note"] == REGULAR_NOTE


def test_ms2_a_regular_session_day_before_a_holiday_points_past_it(market):
    status = body(market, et(2026, 4, 2, 10, 0))  # Thursday before Good Friday
    assert status["is_open"] is True
    assert status["next_close"] == "2026-04-02T16:00:00-04:00"
    assert status["next_open"] == "2026-04-06T09:30:00-04:00"


# ── MS-3: early closes, holidays and daylight saving ─────────


def test_ms3_an_early_close_day_is_open_until_13_00(market):
    status = body(market, et(2026, 11, 27, 10, 0))  # the day after Thanksgiving
    assert status["is_open"] is True and status["label"] == "Market Open"
    assert status["next_close"] == "2026-11-27T13:00:00-05:00"
    assert status["early_close"] is True and status["note"] == EARLY_NOTE
    assert status["next_open"] == "2026-11-30T09:30:00-05:00"


@pytest.mark.parametrize(
    "at",
    [et(2026, 11, 27, 13, 0), et(2026, 11, 27, 13, 30)],
    ids=["13_00_exactly", "13_30"],
)
def test_ms3_an_early_close_day_is_closed_from_13_00(market, at):
    status = body(market, at)
    assert status["is_open"] is False and status["session"] == "closed"
    assert status["next_open"] == "2026-11-30T09:30:00-05:00"
    assert status["next_close"] == "2026-11-30T16:00:00-05:00"
    assert status["early_close"] is False and status["note"] == REGULAR_NOTE


def test_ms3_christmas_eve_closes_early_then_skips_christmas(market):
    before = body(market, et(2026, 12, 24, 12, 59, 59))
    assert (
        before["is_open"] is True
        and before["next_close"] == "2026-12-24T13:00:00-05:00"
    )
    assert before["early_close"] is True and before["note"] == EARLY_NOTE
    after = body(market, et(2026, 12, 24, 14, 0))
    assert after["is_open"] is False
    assert after["next_open"] == "2026-12-28T09:30:00-05:00"
    assert after["early_close"] is False


@pytest.mark.parametrize(
    "at, next_open",
    [
        (et(2026, 11, 26, 12, 0), "2026-11-27T09:30:00-05:00"),  # Thanksgiving
        (et(2026, 12, 23, 17, 0), "2026-12-24T09:30:00-05:00"),  # the evening before
        (et(2026, 12, 24, 8, 0), "2026-12-24T09:30:00-05:00"),  # that morning
    ],
    ids=["thanksgiving", "evening_before_christmas_eve", "christmas_eve_morning"],
)
def test_ms3_while_closed_early_close_describes_the_next_session(market, at, next_open):
    status = body(market, at)
    assert status["is_open"] is False
    assert status["next_open"] == next_open
    assert status["next_close"] == next_open[:11] + "13:00:00-05:00"
    assert status["early_close"] is True and status["note"] == EARLY_NOTE


def test_ms3_july_2_2026_is_a_full_session(market):
    status = body(market, et(2026, 7, 2, 15, 0))  # no early close before Jul 3 in 2026
    assert status["is_open"] is True and status["early_close"] is False
    assert status["next_close"] == "2026-07-02T16:00:00-04:00"
    assert status["next_open"] == "2026-07-06T09:30:00-04:00"


@pytest.mark.parametrize(
    "at, is_open, next_open, next_close",
    [
        # Spring forward (Sun Mar 8): a Friday evening in EST points to Monday in EDT.
        (
            et(2026, 3, 6, 17, 0),
            False,
            "2026-03-09T09:30:00-04:00",
            "2026-03-09T16:00:00-04:00",
        ),
        (
            et(2026, 3, 8, 12, 0),
            False,
            "2026-03-09T09:30:00-04:00",
            "2026-03-09T16:00:00-04:00",
        ),
        (
            et(2026, 3, 9, 10, 0),
            True,
            "2026-03-10T09:30:00-04:00",
            "2026-03-09T16:00:00-04:00",
        ),
        # Fall back (Sun Nov 1): a Friday evening in EDT points to Monday in EST.
        (
            et(2026, 10, 30, 16, 30),
            False,
            "2026-11-02T09:30:00-05:00",
            "2026-11-02T16:00:00-05:00",
        ),
        (
            et(2026, 11, 2, 10, 0),
            True,
            "2026-11-03T09:30:00-05:00",
            "2026-11-02T16:00:00-05:00",
        ),
    ],
    ids=[
        "spring_friday_evening",
        "spring_sunday",
        "spring_monday",
        "fall_friday_evening",
        "fall_monday",
    ],
)
def test_ms3_both_daylight_saving_changes_give_the_right_offsets(
    market, at, is_open, next_open, next_close
):
    status = body(market, at)
    assert status["is_open"] is is_open
    assert (status["next_open"], status["next_close"]) == (next_open, next_close)


@pytest.mark.parametrize(
    "utc, is_open",
    [
        (datetime(2026, 11, 2, 14, 29, 59, tzinfo=timezone.utc), False),  # 09:29:59 EST
        (datetime(2026, 11, 2, 14, 30, tzinfo=timezone.utc), True),  # 09:30 EST
        (datetime(2026, 10, 30, 13, 30, tzinfo=timezone.utc), True),  # 09:30 EDT
        (datetime(2026, 10, 30, 13, 29, 59, tzinfo=timezone.utc), False),
    ],
    ids=["est_open_minus_1s", "est_open", "edt_open", "edt_open_minus_1s"],
)
def test_ms3_the_open_is_09_30_new_york_time_in_utc_on_either_side(
    market, utc, is_open
):
    assert body(market, utc)["is_open"] is is_open


def test_ms3_the_endpoint_does_not_use_the_hand_maintained_holiday_list(monkeypatch):
    """Never a hand list: settings.TRADING_HOLIDAYS (which health.py uses) has no
    say. Pinned by source and by behaviour: emptying it keeps Thanksgiving closed,
    and listing a regular day there keeps that day open."""
    import importlib
    import inspect

    from fastapi.testclient import TestClient
    from src import settings
    from src.api import market_status

    source = inspect.getsource(market_status)
    assert "TRADING_HOLIDAYS" not in source and "trading_holidays" not in source
    for holidays, at, is_open in (
        ((), et(2026, 11, 26, 12, 0), False),
        ((datetime(2026, 10, 27).date(),), et(2026, 10, 27, 10, 0), True),
    ):
        monkeypatch.setattr(settings, "TRADING_HOLIDAYS", holidays)
        importlib.reload(market_status)
        monkeypatch.setattr(market_status, "now", Clock(at))
        app, _ = _app()
        assert TestClient(app).get("/v1/market-status").json()["is_open"] is is_open
    monkeypatch.undo()
    importlib.reload(market_status)


# ── MS-4: time-zone independence ─────────────────────────────

INSTANTS = [
    et(2026, 10, 26, 10, 0),
    et(2026, 10, 31, 12, 0),
    et(2026, 11, 27, 10, 0),
    et(2026, 11, 2, 0, 30),  # 05:30 UTC: already Monday in Tokyo, still Sunday in LA
    et(2026, 3, 8, 23, 30),
]


@pytest.fixture
def process_tz():
    original = os.environ.get("TZ")

    def set_tz(name):
        os.environ["TZ"] = name
        time.tzset()

    yield set_tz
    if original is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = original
    time.tzset()


def test_ms4_the_process_time_zone_changes_nothing(market, process_tz):
    results = {}
    for name in ("UTC", "Asia/Tokyo", "America/Los_Angeles"):
        process_tz(name)
        results[name] = [market(at).content for at in INSTANTS]
    assert results["UTC"] == results["Asia/Tokyo"] == results["America/Los_Angeles"]


def test_ms4_no_request_input_changes_the_answer(market):
    at = et(2026, 10, 26, 10, 0)
    plain = market(at).content
    for kwargs in (
        {"params": {"tz": "Asia/Tokyo", "exchange": "XLON", "date": "2026-12-25"}},
        {
            "headers": {
                "Accept-Language": "ja-JP",
                "X-Timezone": "Asia/Tokyo",
                "Date": "Fri, 25 Dec 2026 12:00:00 GMT",
                "TZ": "Asia/Tokyo",
            }
        },
    ):
        assert market(at, **kwargs).content == plain


# ── MS-5: wording ────────────────────────────────────────────


@pytest.mark.parametrize("case", sorted(CLOSED))
def test_ms5_label_and_note_wording(market, case):
    for at in (CLOSED[case][0], et(2026, 10, 26, 10, 0), et(2026, 11, 27, 10, 0)):
        status = body(market, at)
        assert status["label"] in ("Market Open", "Market Closed")
        assert status["label"] == (
            "Market Open" if status["is_open"] else "Market Closed"
        )
        assert "Regular hours" in status["note"]
        assert "Some brokers allow extended-hours trading" in status["note"]


# ── MS-6: live update support (the API half) ─────────────────


@pytest.mark.parametrize(
    "at, expected",
    [
        (et(2026, 10, 26, 10, 0), 15),  # hours from a transition
        (et(2026, 10, 31, 12, 0), 15),  # a Saturday
        (et(2026, 10, 26, 15, 59, 50), 10),  # T-10 s before the close
        (et(2026, 10, 26, 9, 29, 50), 10),  # T-10 s before the open
        (et(2026, 11, 27, 12, 59, 55), 5),  # before an early close
        (
            et(2026, 10, 26, 15, 59, 45, 500000),
            14,
        ),  # whole seconds: 14.5 s left gives 14
        (et(2026, 10, 26, 15, 59, 59), 1),
        (et(2026, 10, 26, 15, 59, 59, 500000), 1),  # never 0
    ],
    ids=[
        "mid_session",
        "weekend",
        "close_minus_10s",
        "open_minus_10s",
        "early_close_minus_5s",
        "fractional",
        "close_minus_1s",
        "close_minus_half_a_second",
    ],
)
def test_ms6_max_age_is_min_15_whole_seconds_to_the_next_transition_at_least_1(
    market, at, expected
):
    assert max_age(market(at)) == expected


def test_ms6_a_cached_answer_never_spans_a_transition(market):
    close = et(2026, 10, 26, 16, 0)
    opening = et(2026, 10, 27, 9, 30)
    for transition in (close, opening):
        for seconds in range(1, 31):
            at = datetime.fromtimestamp(transition.timestamp() - seconds, timezone.utc)
            assert max_age(market(at)) == min(15, seconds), seconds


# ── MS-7: plumbing ───────────────────────────────────────────


def test_ms7_the_read_bucket_comes_first_and_a_429_matches_rl2(monkeypatch):
    from fastapi.testclient import TestClient
    from src.api import market_status

    clock = Clock(et(2026, 10, 26, 10, 0))
    monkeypatch.setattr(market_status, "now", clock)
    app, used = _app(burst=1)
    client = TestClient(app, client=("198.51.100.7", 40000))
    assert client.get("/v1/market-status").status_code == 200
    calls = clock.calls
    limited = client.get("/v1/market-status")
    assert limited.status_code == 429
    assert limited.json() == RATE_LIMITED
    assert (
        limited.content
        == json.dumps(RATE_LIMITED, sort_keys=True, separators=(",", ":")).encode()
    )
    assert re.fullmatch(r"[1-9][0-9]*", limited.headers["retry-after"])
    assert limited.headers["cache-control"] == "no-store"
    assert clock.calls == calls, "a limited request computed the status"
    assert used == []


def test_ms7_the_route_is_on_the_read_router_with_its_429():
    from fastapi import FastAPI
    from src.api.read import router

    app = FastAPI()
    app.include_router(router)
    operation = app.openapi()["paths"]["/v1/market-status"]["get"]
    assert "429" in operation["responses"]


def test_ms7_openapi_documents_the_contract_with_both_examples():
    from fastapi import FastAPI
    from src.api.read import router

    app = FastAPI()
    app.include_router(router)
    document = app.openapi()
    content = document["paths"]["/v1/market-status"]["get"]["responses"]["200"][
        "content"
    ]
    examples = [e["value"] for e in content["application/json"]["examples"].values()]
    assert sorted(examples, key=json.dumps) == sorted(
        [OPEN_EXAMPLE, CLOSED_EXAMPLE], key=json.dumps
    )
    schema = content["application/json"]["schema"]
    if "$ref" in schema:
        schema = document["components"]["schemas"][schema["$ref"].rsplit("/", 1)[1]]
    assert set(schema["properties"]) == KEYS
    assert set(schema.get("required", ())) == KEYS


def test_ms7_the_snapshot_includes_the_route():
    snapshot = json.loads(
        (Path(__file__).parent / "fixtures" / "api" / "openapi_v1.json").read_text()
    )
    assert "/v1/market-status" in snapshot["paths"]


def test_ms7_api_md_documents_the_endpoint_and_both_examples():
    content = (REPO / "docs" / "technical-specs" / "API.md").read_text()
    for term in (
        "/v1/market-status",
        '"session": "regular"',
        '"session": "closed"',
        "Market Open",
        "Market Closed",
        "max-age",
        "next_open",
        "next_close",
        "early_close",
        "(early close)",
        "America/New_York",
    ):
        assert term in content, term
