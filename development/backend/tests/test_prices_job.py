"""S3-01: the daily price snapshot job (src/services/prices). Finnhub is faked.

Pinned:
* Parsing: Finnhub's quote gives the price, previous close and time; its zeros
  mean no price. The 52-week range counts only when both ends are present,
  positive and in order; an unreadable date is dropped, not the range.
* The client sends the key in a header, never the URL, and spaces its requests.
* Collection: one row per priced ticker, dated by the quote's New York trading
  day. A ticker without a price is listed under no_quote, one without a range is
  stored without it (no_range), and a failing or malformed ticker is listed under
  errors while the run continues. Every ticker failing fails the run.
* The run: while the NYSE session is open no run starts. Otherwise it stores the
  batch for the current watchlist, and without a watchlist it fails as
  missing_upstream.
"""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import insert, select
from src.db import models
from src.ingestion import collectors
from src.pipeline import scheduler
from src.services.fundamentals.job import Universe
from src.services.news.finnhub import FinnhubError
from src.services.prices import finnhub, job
from src.services.prices.finnhub import (
    FinnhubPriceClient,
    Quote,
    Range,
    parse_quote,
    parse_range,
)
from src.services.prices.job import collect_prices, run_prices

FRIDAY_CLOSE = 1794603600  # 2026-11-13 16:00 New York (21:00 UTC)
SATURDAY = datetime(2026, 11, 14, 15, 0, tzinfo=timezone.utc)
FRIDAY_MIDDAY = datetime(2026, 11, 13, 17, 0, tzinfo=timezone.utc)  # 12:00 New York


def quote(price, previous="184.58", stamp=FRIDAY_CLOSE):
    return Quote(
        Decimal(price),
        None if previous is None else Decimal(previous),
        datetime.fromtimestamp(stamp, tz=timezone.utc),
    )


SPAN = Range(Decimal("207.52"), date(2026, 2, 18), Decimal("63.4"), date(2025, 11, 21))


class Client:
    def __init__(self, quotes, ranges=None, failing=()):
        self.quotes, self.ranges, self.failing = quotes, ranges or {}, set(failing)
        self.session = None

    def quote(self, symbol):
        if symbol in self.failing:
            raise FinnhubError("http_429")
        return self.quotes.get(symbol)

    def range_52w(self, symbol):
        return self.ranges.get(symbol)


# ── parsing ──────────────────────────────────────────────


def test_a_quote_gives_the_price_previous_close_and_time():
    parsed = parse_quote({"c": 182.41, "pc": 184.58, "d": -2.17, "t": FRIDAY_CLOSE})
    assert parsed == quote("182.41")
    assert parsed.quoted_at.tzinfo == timezone.utc


@pytest.mark.parametrize(
    "payload",
    [
        {"c": 0, "d": None, "dp": None, "pc": 0, "t": 0},  # Finnhub's answer for an unknown ticker
        {"c": 182.41},
        {"c": -1, "t": FRIDAY_CLOSE},
        {"c": True, "t": FRIDAY_CLOSE},
    ],
)
def test_no_usable_price_is_no_quote(payload):
    assert parse_quote(payload) is None


def test_a_zero_previous_close_is_unknown():
    assert parse_quote({"c": 5, "pc": 0, "t": FRIDAY_CLOSE}).previous_close is None


def test_a_non_object_is_malformed():
    for parse in (parse_quote, parse_range):
        with pytest.raises(FinnhubError, match="^malformed_response$"):
            parse([])


def test_a_range_needs_both_ends_in_order():
    metric = {"52WeekHigh": 207.52, "52WeekHighDate": "2026-02-18",
              "52WeekLow": 63.4, "52WeekLowDate": "2025-11-21"}
    assert parse_range({"metric": metric}) == SPAN
    assert parse_range({"metric": {**metric, "52WeekLow": None}}) is None
    assert parse_range({"metric": {**metric, "52WeekLow": 300}}) is None
    assert parse_range({"metric": {}}) is None
    assert parse_range({"metric": {**metric, "52WeekHighDate": "Feb 18"}}).high_date is None


def test_client_sends_the_key_in_a_header_and_paces_requests(monkeypatch):
    seen, clock, slept = [], [0.0], []

    def get(session, url, *, params, headers):
        seen.append((url, params, headers))
        return collectors._Response(True, status=200, data={"c": 1, "t": FRIDAY_CLOSE, "metric": {}})

    monkeypatch.setattr(finnhub, "_get_json", get)
    client = FinnhubPriceClient(object(), "secret", interval=1.1,
                                monotonic=lambda: clock[0], sleep=slept.append)
    client.quote("PLTR")
    client.range_52w("PLTR")
    assert slept == [1.1]
    assert [p for _, p, _ in seen] == [{"symbol": "PLTR"}, {"symbol": "PLTR", "metric": "all"}]
    assert all("token" not in p and h == {"X-Finnhub-Token": "secret"} for _, p, h in seen)


def test_client_errors_carry_only_a_code(monkeypatch):
    monkeypatch.setattr(finnhub, "_get_json", lambda *a, **k: collectors._Response(False, code="http_429"))
    with pytest.raises(FinnhubError, match="^http_429$"):
        FinnhubPriceClient(object(), "k", interval=0).quote("PLTR")


# ── collection ───────────────────────────────────────────


def test_each_priced_ticker_gets_one_row_dated_by_its_new_york_trading_day():
    outcome = collect_prices(Client({"ZZPA": quote("182.41")}, {"ZZPA": SPAN}), ["ZZPA"])
    assert outcome.succeeded
    (row,) = outcome.records
    assert row["trading_date"] == date(2026, 11, 13)  # 21:00 UTC is still Friday in New York
    assert row["close"] == Decimal("182.41") and row["previous_close"] == Decimal("184.58")
    assert (row["high_52w"], row["low_52w"]) == (SPAN.high, SPAN.low)
    assert row["provider"] == "finnhub" and row["price_currency"] == "USD"


def test_late_evening_quotes_keep_their_trading_day():
    late = quote("5", stamp=FRIDAY_CLOSE + 4 * 3600)  # 20:00 New York, 01:00 UTC Saturday
    (row,) = collect_prices(Client({"ZZPA": late}), ["ZZPA"]).records
    assert row["trading_date"] == date(2026, 11, 13)


def test_no_price_no_range_and_errors_are_listed_and_the_run_continues():
    client = Client(
        {"ZZPA": quote("182.41"), "ZZPB": quote("10"), "ZZPD": quote("10", previous="9")},
        {"ZZPA": SPAN, "ZZPD": Range(Decimal("12"), None, Decimal("13"), None)},
        failing={"ZZPC"},
    )
    outcome = collect_prices(client, ["ZZPA", "ZZPB", "ZZPC", "ZZPD", "ZZPE"])
    assert outcome.succeeded and outcome.coverage == {"complete": False, "scope": "selected_universe"}
    assert [r["ticker"] for r in outcome.records] == ["ZZPA", "ZZPB"]
    env = outcome.envelope
    assert env["no_quote"] == ["ZZPE"] and env["no_range"] == ["ZZPB"]
    assert env["errors"] == ["ZZPC", "ZZPD"]  # ZZPD's range is inverted: malformed
    assert (env["universe"], env["priced"], env["errors_count"]) == (5, 2, 2)


def test_every_ticker_failing_fails_the_run():
    outcome = collect_prices(Client({}, failing={"ZZPA", "ZZPB"}), ["ZZPA", "ZZPB"])
    assert not outcome.succeeded and outcome.error_code == "collection_failed"
    assert outcome.diagnostics["unit_kind"] == "price tickers"


# ── the run ──────────────────────────────────────────────


def seed(engine, *symbols):
    with engine.begin() as conn:
        for symbol in symbols:
            sid = conn.execute(insert(models.securities).values(symbol=symbol)
                               .returning(models.securities.c.id)).scalar_one()
            conn.execute(insert(models.security_identifiers).values(
                scheme="ticker", value=symbol, security_id=sid))


def stored(engine):
    with engine.connect() as conn:
        return conn.execute(select(models.daily_prices).order_by(models.daily_prices.c.id)).mappings().all()


@pytest.mark.db
def test_a_run_stores_a_snapshot_and_a_new_price_makes_a_new_batch(engine, monkeypatch):
    seed(engine, "ZZPA", "ZZPB")
    monkeypatch.setattr(job, "current_universe", lambda conn: Universe(["ZZPA", "ZZPB"], {}, {}))
    first = run_prices(engine, Client({"ZZPA": quote("182.41")}, {"ZZPA": SPAN}), SATURDAY)
    assert first.succeeded and first.accepted_count == 1
    second = run_prices(engine, Client({"ZZPA": quote("190")}, {"ZZPA": SPAN}), SATURDAY)
    assert second.succeeded and second.batch_id > first.batch_id
    rows = stored(engine)
    assert [r["close"] for r in rows] == [Decimal("182.410000"), Decimal("190.000000")]
    assert rows[0]["currency"] == "USD" and rows[0]["high_52w_date"] == date(2026, 2, 18)


@pytest.mark.db
def test_no_run_starts_while_the_market_is_open(engine, monkeypatch):
    seed(engine, "ZZPA")
    monkeypatch.setattr(job, "current_universe", lambda conn: Universe(["ZZPA"], {}, {}))
    assert run_prices(engine, Client({"ZZPA": quote("1")}), FRIDAY_MIDDAY) is None
    with engine.connect() as conn:
        runs = conn.execute(select(models.ingestion_runs).where(
            models.ingestion_runs.c.source == "daily_prices")).all()
    assert runs == [] and stored(engine) == []


@pytest.mark.db
def test_without_a_watchlist_the_run_fails_as_missing_upstream(engine, monkeypatch):
    monkeypatch.setattr(job, "current_universe", lambda conn: None)
    result = run_prices(engine, Client({}), SATURDAY)
    assert not result.succeeded and result.error_code == "missing_upstream"


def test_the_scheduler_runs_the_prices_job(monkeypatch):
    calls = []
    monkeypatch.setattr(job, "run_once", lambda settings: calls.append(settings) or 0)
    assert scheduler.run_once("prices", settings="s") == 0 and calls == ["s"]
