"""Ticker loader: reads perennial.securities, falls back to {ticker: ticker}."""

import asyncio

import pytest
from sqlalchemy import insert
from sqlalchemy.exc import OperationalError
from src.db.config import DatabaseConfigError
from src.db.models import securities
from src.utils import fetch_tickers as ft

from sentiment.reddit.reddit_processing import TickerMatcher


def test_map_uses_name_and_falls_back_to_ticker():
    rows = [
        ("AAPL", "Apple Inc."),
        ("MSFT", None),
        ("NVDA", ""),
        ("TSLA", "   "),
        ("AMZN", " Amazon.com Inc "),
    ]
    assert ft.build_ticker_map(rows) == {
        "Apple Inc.": "AAPL",
        "MSFT": "MSFT",
        "NVDA": "NVDA",
        "TSLA": "TSLA",
        "Amazon.com Inc": "AMZN",
    }


def test_shared_name_keeps_every_ticker():
    rows = [("GOOG", "Alphabet Inc."), ("GOOGL", "Alphabet Inc.")]
    assert ft.build_ticker_map(rows) == {"Alphabet Inc.": "GOOG", "GOOGL": "GOOGL"}


def test_unusable_symbols_are_skipped():
    assert ft.build_ticker_map([(None, "X"), ("  ", "Y"), ("$ko", None)]) == {
        "KO": "KO"
    }


@pytest.mark.parametrize(
    "error", [DatabaseConfigError("DATABASE_URL is not set."), OperationalError("s", {}, Exception())]
)
def test_unavailable_database_returns_none(monkeypatch, error):
    def boom(engine=None):
        raise error

    monkeypatch.setattr(ft, "_query_rows", boom)
    assert asyncio.run(ft.fetch_ticker_map()) is None
    assert asyncio.run(ft.fetch_tickers()) is None
    assert asyncio.run(ft.fetch_ticker_list()) is None


def test_no_active_securities_returns_none(monkeypatch):
    monkeypatch.setattr(ft, "_query_rows", lambda engine=None: [])
    assert asyncio.run(ft.fetch_ticker_map()) is None
    assert asyncio.run(ft.fetch_tickers()) is None


def test_public_functions_share_one_read(monkeypatch):
    rows = [("AAPL", "Apple Inc."), ("MSFT", None)]
    monkeypatch.setattr(ft, "_query_rows", lambda engine=None: rows)
    assert asyncio.run(ft.fetch_ticker_map()) == {"Apple Inc.": "AAPL", "MSFT": "MSFT"}
    assert asyncio.run(ft.fetch_tickers()) == {"AAPL", "MSFT"}
    assert asyncio.run(ft.fetch_ticker_list()) == ["AAPL", "MSFT"]


def test_reddit_matcher_ignores_ticker_used_as_name():
    # "ON" has no company name: it must not match the plain word "on".
    matcher = TickerMatcher({"ON": "ON", "Apple Inc.": "AAPL"})
    assert "ON" not in matcher.find("I put it on my list")


@pytest.mark.db
def test_reads_active_securities_from_database(engine):
    with engine.begin() as conn:
        conn.execute(
            insert(securities),
            [
                {"symbol": "AAPL", "name": "Apple Inc."},
                {"symbol": "MSFT", "name": None},
                {"symbol": "NVDA", "name": ""},
            ],
        )
        conn.execute(
            insert(securities).values(symbol="DEAD", name="Gone Corp", is_active=False)
        )
    assert ft._read_ticker_map(engine) == {
        "Apple Inc.": "AAPL",
        "MSFT": "MSFT",
        "NVDA": "NVDA",
    }
    assert ft._read_ticker_list(engine) == ["AAPL", "MSFT", "NVDA"]
