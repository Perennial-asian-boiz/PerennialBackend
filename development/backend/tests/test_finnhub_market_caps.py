"""Market caps from Finnhub's free company profile. Synthetic responses only."""

import json
from decimal import Decimal

import pytest

KEY = "synthetic-finnhub-key"


class FakeHttp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code = status
        self.headers = headers or {}
        self._body = {} if body is None else body
        self._raw = json.dumps(self._body).encode()

    def iter_content(self, chunk_size=1):
        yield self._raw

    def json(self):
        return self._body

    def close(self):
        pass


class FakeSession:
    """Answers by Finnhub symbol from `answers` ({} when unlisted); records calls."""

    def __init__(self, answers=None):
        self.answers = answers or {}
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None, stream=False):
        self.calls.append((url, dict(params or {}), dict(headers or {})))
        answer = self.answers.get(params["symbol"], FakeHttp(body={}))
        return answer.pop(0) if isinstance(answer, list) else answer


def profile(ticker, millions, currency="USD", industry="Synthetic", country="US"):
    return FakeHttp(
        body={
            "ticker": ticker,
            "country": country,
            "currency": currency,
            "marketCapitalization": millions,
            "finnhubIndustry": industry,
        }
    )


# ── parse_profile ────────────────────────────────────────


def test_millions_become_whole_dollars():
    from src.ingestion.finnhub_profile import parse_profile

    got = parse_profile({"ticker": "ZZAA", "currency": "usd", "marketCapitalization": 10000.5})
    assert got.market_cap == Decimal("10000500000.0")  # above the $10B threshold
    assert got.currency == "USD" and got.ticker == "ZZAA"


@pytest.mark.parametrize("raw", [None, 0])
def test_null_or_zero_market_cap_is_no_value(raw):
    from src.ingestion.finnhub_profile import parse_profile

    assert parse_profile({"ticker": "ZZAA", "marketCapitalization": raw}).market_cap is None


def test_unknown_symbol_is_none():
    from src.ingestion.finnhub_profile import parse_profile

    assert parse_profile({}) is None


# Shapes seen live on 2026-10-30 (synthetic tickers): Finnhub answers with the
# company's primary listing, e.g. BRK-B as BRK.A (US, USD) and TSM as 2330.TW.
@pytest.mark.parametrize(
    ("asked", "body", "expected"),
    [
        ("ZZB-B", {"ticker": "ZZB.B", "country": "US", "currency": "USD"}, Decimal(5_000_000)),
        ("ZZB-B", {"ticker": "ZZB.A", "country": "US", "currency": "USD"}, Decimal(5_000_000)),
        ("ZZAA", {"country": "US", "currency": "USD"}, Decimal(5_000_000)),
        ("ZZNL", {"ticker": "ZZNL", "country": "NL", "currency": "USD"}, Decimal(5_000_000)),
        ("ZZTW", {"ticker": "9999.TW", "country": "TW", "currency": "TWD"}, None),
        ("ZZGB", {"ticker": "ZZGB.L", "country": "GB", "currency": "USD"}, None),
        ("ZZCA", {"ticker": "ZZCA.TO", "country": "CA", "currency": "USD"}, None),
        ("ZZAA", {"ticker": "ZZAA", "country": "US", "currency": "EUR"}, None),
    ],
)
def test_usd_value_keeps_only_plainly_usd_values_for_this_company(asked, body, expected):
    from src.ingestion.finnhub_profile import parse_profile, usd_value

    got = usd_value(parse_profile(body | {"marketCapitalization": 5}), asked)
    assert got == expected


def test_usd_value_of_nothing_is_none():
    from src.ingestion.finnhub_profile import parse_profile, usd_value

    assert usd_value(None, "ZZAA") is None
    assert usd_value(parse_profile({"ticker": "ZZAA", "currency": "USD"}), "ZZAA") is None


@pytest.mark.parametrize(
    "payload",
    [[], "x", {"ticker": 5}, {"marketCapitalization": "12"},
     {"marketCapitalization": True}, {"marketCapitalization": -1},
     {"marketCapitalization": float("inf")}, {"marketCapitalization": 1e17}],
)
def test_malformed_profiles_raise(payload):
    from src.ingestion.finnhub_profile import MalformedProfile, parse_profile

    with pytest.raises(MalformedProfile):
        parse_profile(payload)


# ── FinnhubMarketCapAdapter (database pipeline) ──────────


def adapter(key=KEY):
    from src.ingestion.market_caps import FinnhubMarketCapAdapter

    return FinnhubMarketCapAdapter(key)


def test_adapter_returns_company_wide_usd_quotes_with_the_key_in_a_header():
    session = FakeSession({"ZZAA": profile("ZZAA", 2500), "ZZB-B": profile("ZZB.B", 50000)})
    slept = []
    got = adapter().fetch(["ZZAA", "ZZB.B", "ZZZZ"], session=session, sleep=slept.append)
    assert got["ZZAA"].value == Decimal("2500000000")
    assert got["ZZB.B"].value == Decimal("50000000000")
    assert got["ZZAA"].issuer_level is True and got["ZZAA"].currency == "USD"
    assert got["ZZAA"].sector is None and got["ZZAA"].industry == "Synthetic"
    assert got["ZZZZ"] is None
    assert [c[1]["symbol"] for c in session.calls] == ["ZZAA", "ZZB-B", "ZZZZ"]
    for url, params, headers in session.calls:
        assert url == "https://finnhub.io/api/v1/stock/profile2"
        assert set(params) == {"symbol"} and headers == {"X-Finnhub-Token": KEY}
    assert slept == [1.1, 1.1]  # paced between requests, not before the first


@pytest.mark.parametrize("key", [None, "", "your_finnhub_key_here"])
def test_adapter_without_a_key_is_missing_credential(key):
    from src.ingestion.market_caps import MarketCapAdapterError

    with pytest.raises(MarketCapAdapterError) as caught:
        adapter(key).fetch(["ZZAA"], session=FakeSession())
    assert caught.value.code == "missing_credential"


def test_non_usd_profile_and_non_us_listing_have_no_value():
    session = FakeSession({"ZZAA": profile("ZZAA", 2500, currency="TWD")})
    got = adapter().fetch(["ZZAA", "ZZCC/U"], session=session, sleep=lambda s: None)
    assert got == {"ZZAA": None, "ZZCC/U": None}
    assert [c[1]["symbol"] for c in session.calls] == ["ZZAA"]  # no request for ZZCC/U


def test_primary_listing_answers_are_kept_for_us_companies_only():
    session = FakeSession({
        "ZZB-B": profile("ZZB.A", 1000000),  # a US share class: the company total
        "ZZTW": profile("9999.TW", 5000, currency="USD", country="TW"),  # foreign primary
    })
    got = adapter().fetch(["ZZB.B", "ZZTW"], session=session, sleep=lambda s: None)
    assert got["ZZB.B"].value == Decimal("1000000000000") and got["ZZB.B"].issuer_level
    assert got["ZZTW"] is None


def test_a_few_403s_are_unavailable_but_too_many_fail():
    from src.ingestion.market_caps import MarketCapAdapterError

    symbols = ["ZZAA", "ZZBB", "ZZCC", "ZZDD", "ZZEE"]
    one = FakeSession({"ZZAA": FakeHttp(403)} | {s: profile(s, 1) for s in symbols[1:]})
    got = adapter().fetch(symbols, session=one, sleep=lambda s: None)
    assert got["ZZAA"] is None and got["ZZBB"].value == Decimal(1_000_000)

    two = FakeSession({"ZZAA": FakeHttp(403), "ZZBB": FakeHttp(403)})
    with pytest.raises(MarketCapAdapterError) as caught:
        adapter().fetch(symbols, session=two, sleep=lambda s: None)
    assert caught.value.code == "http_403"


@pytest.mark.parametrize(
    ("answer", "code"),
    [(FakeHttp(500), "http_500"), (FakeHttp(401), "http_401"),
     (FakeHttp(body={"marketCapitalization": "12"}), "malformed_response")],
)
def test_other_failures_fail_the_fetch(answer, code):
    from src.ingestion.market_caps import MarketCapAdapterError

    with pytest.raises(MarketCapAdapterError) as caught:
        adapter().fetch(["ZZAA"], session=FakeSession({"ZZAA": answer}), sleep=lambda s: None)
    assert caught.value.code == code


def test_registry_builds_the_adapter_from_the_finnhub_key(monkeypatch):
    from src.ingestion.market_caps import ADAPTERS, FinnhubMarketCapAdapter
    from src.settings import load_settings

    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://user:synthetic@localhost/test")
    monkeypatch.setenv("FINNHUB_API_KEY", KEY)
    monkeypatch.setenv("MARKET_CAP_ADAPTER", "finnhub")
    settings = load_settings(local_dev=False)
    built = ADAPTERS[settings.market_cap_adapter](settings)
    assert isinstance(built, FinnhubMarketCapAdapter) and built.api_key == KEY


# ── consensus.fetch_market_cap (legacy JSON pipeline) ────


def test_legacy_lookup_uses_finnhub_and_converts_millions():
    from src.services.consensus_watchlist import consensus

    session = FakeSession({"ZZB-B": profile("ZZB.B", 12000)})
    slept, cache = [], {}
    value = consensus.fetch_market_cap("ZZB.B", cache, session=session, api_key=KEY, sleep=slept.append)
    assert value == 12_000_000_000.0 and cache == {"ZZB.B": value}
    url, params, headers = session.calls[0]
    assert url == "https://finnhub.io/api/v1/stock/profile2"
    assert params == {"symbol": "ZZB-B"} and headers == {"X-Finnhub-Token": KEY}
    assert slept == [1.1]
    # Cached: no second request and no wait.
    assert consensus.fetch_market_cap("ZZB.B", cache, session=session, api_key=KEY, sleep=slept.append) == value
    assert len(session.calls) == 1 and slept == [1.1]


def test_legacy_lookup_waits_out_a_429_then_retries():
    from src.services.consensus_watchlist import consensus

    session = FakeSession({"ZZAA": [FakeHttp(429), profile("ZZAA", 3000)]})
    slept = []
    value = consensus.fetch_market_cap("ZZAA", {}, session=session, api_key=KEY, sleep=slept.append)
    assert value == 3_000_000_000.0
    assert len(session.calls) == 2 and len(slept) == 2  # the 429 wait, then pacing


@pytest.mark.parametrize(
    "answer", [FakeHttp(403), FakeHttp(body={}), profile("ZZAA", 3000, currency="EUR"),
               profile("ZZAA.L", 3000, country="GB"), FakeHttp(body={"marketCapitalization": "x"})],
)
def test_legacy_lookup_without_a_usable_value_is_none(answer):
    from src.services.consensus_watchlist import consensus

    cache = {}
    got = consensus.fetch_market_cap("ZZAA", cache, session=FakeSession({"ZZAA": answer}),
                                     api_key=KEY, sleep=lambda s: None)
    assert got is None and cache == {"ZZAA": None}
