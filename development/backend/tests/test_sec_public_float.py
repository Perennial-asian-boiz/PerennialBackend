"""Company size from SEC EDGAR's public float (#90). Synthetic responses only."""

import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

AGENT = "Perennial tests synthetic@perennial.test"
TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"


def concept_url(cik):
    return (
        "https://data.sec.gov/api/xbrl/companyconcept/"
        f"CIK{cik:010d}/dei/EntityPublicFloat.json"
    )


def fact(val, end, filed, form="10-K"):
    return {"val": val, "end": end, "filed": filed, "form": form, "fy": 2025, "fp": "FY"}


class FakeHttp:
    def __init__(self, status=200, body=None):
        self.status_code, self.headers = status, {}
        self._raw = json.dumps({} if body is None else body).encode()

    def iter_content(self, chunk_size=1):
        yield self._raw

    def close(self):
        pass


class FakeSec:
    """SEC's ticker map and companyconcept answers; a "limited" agent always gets 429."""

    def __init__(self, tickers, floats):
        self.tickers, self.floats, self.calls = tickers, floats, []

    def get(self, url, params=None, headers=None, timeout=None, stream=False):
        assert stream and timeout and headers["User-Agent"]
        self.calls.append(url)
        if "limited" in headers["User-Agent"]:
            return FakeHttp(429)
        if url == TICKER_MAP_URL:
            rows = {
                str(i): {"cik_str": cik, "ticker": t, "title": f"Synthetic {t}"}
                for i, (t, cik) in enumerate(self.tickers.items())
            }
            return FakeHttp(body=rows)
        for cik, answer in self.floats.items():
            if url == concept_url(cik):
                return answer if isinstance(answer, FakeHttp) else FakeHttp(body=answer)
        return FakeHttp(404)


def concept(*facts):
    return {"cik": 1, "taxonomy": "dei", "tag": "EntityPublicFloat", "units": {"USD": list(facts)}}


# ── latest_public_float ──────────────────────────────────


def test_latest_measurement_wins_and_an_amendment_beats_its_original():
    from src.services.fundamentals.edgar import latest_public_float

    got = latest_public_float(
        concept(
            fact(5e9, "2024-06-30", "2025-02-20"),
            fact(7e9, "2025-06-30", "2026-02-19"),
            fact(7.5e9, "2025-06-30", "2026-03-10", form="10-K/A"),
        )
    )
    assert got.value == Decimal("7500000000.0") and got.as_of == date(2025, 6, 30)
    assert got.filed == date(2026, 3, 10) and got.form == "10-K/A"


@pytest.mark.parametrize(
    "data",
    [
        concept(),  # no USD values
        {"units": {"EUR": [fact(5e9, "2025-06-30", "2026-02-19")]}},
        concept(fact(5e9, "2024-06-30", "2025-02-20"), fact(0, "2025-06-30", "2026-02-19")),
    ],
)
def test_no_value_when_none_is_usable(data):
    """A latest value of 0 (listed after its measurement date) is not replaced by an older one."""
    from src.services.fundamentals.edgar import latest_public_float

    assert latest_public_float(data) is None


@pytest.mark.parametrize(
    "data",
    [
        [],
        {"units": []},
        {"units": {"USD": {}}},
        concept({"val": 5e9, "end": "2025-06-30"}),  # no filed date
        concept(fact("5e9", "2025-06-30", "2026-02-19")),
        concept(fact(True, "2025-06-30", "2026-02-19")),
        concept(fact(5e9, "June 2025", "2026-02-19")),
        concept(fact(float("nan"), "2025-06-30", "2026-02-19")),
    ],
)
def test_malformed_responses_raise(data):
    from src.services.fundamentals.edgar import EdgarError, latest_public_float

    with pytest.raises(EdgarError) as caught:
        latest_public_float(data)
    assert caught.value.args == ("public_float_malformed",)


def test_client_public_float_404_is_none():
    import requests
    from src.services.fundamentals.edgar import EdgarClient

    client = EdgarClient(FakeSec({}, {}), AGENT, sleep=lambda s: None)
    assert client.public_float(1) is None
    assert isinstance(client.session, FakeSec) and not isinstance(client.session, requests.Session)


@pytest.mark.parametrize(
    ("symbol", "spelled"),
    [("ZZAA", "ZZAA"), ("zzb.b", "ZZB-B"), ("ZZCC UW", "ZZCC"), ("ZZDD/U", None), ("", None)],
)
def test_sec_symbol(symbol, spelled):
    from src.ingestion.market_caps import sec_symbol

    assert sec_symbol(symbol) == spelled


# ── SecPublicFloatAdapter (database pipeline) ────────────


def sec():
    return FakeSec(
        {"ZZAA": 1, "ZZB-B": 2, "ZZOLD": 3, "ZZNEW": 4},
        {
            1: concept(fact(12e9, "2025-06-30", "2026-02-19")),
            2: concept(fact(800e6, "2025-03-31", "2025-11-20")),
            4: concept(fact(0, "2025-06-30", "2026-02-19")),
            9: concept(fact(450e9, "2025-06-30", "2026-02-19")),
        },
    )


def adapter(agent=AGENT, seeded=None):
    from src.ingestion.market_caps import SecPublicFloatAdapter

    return SecPublicFloatAdapter(agent, seeded_ciks=None if seeded is None else lambda: seeded)


def test_adapter_returns_dated_company_wide_usd_values():
    session = sec()
    got = adapter().fetch(
        ["ZZAA", "ZZB.B", "ZZOLD", "ZZNEW", "ZZZZ", "ZZDD/U"], session=session, sleep=lambda s: None
    )
    assert got["ZZAA"].value == Decimal("12000000000.0") and got["ZZAA"].currency == "USD"
    assert got["ZZAA"].issuer_level is True
    assert got["ZZAA"].observed_at == datetime(2025, 6, 30, tzinfo=timezone.utc)
    assert got["ZZAA"].sector is None and got["ZZAA"].industry is None
    assert got["ZZB.B"].value == Decimal("800000000.0")
    assert got["ZZOLD"] is None  # 404: no public float filed
    assert got["ZZNEW"] is None  # 0: listed after the measurement date
    assert got["ZZZZ"] is None and got["ZZDD/U"] is None  # no CIK
    assert session.calls[0] == TICKER_MAP_URL and len(session.calls) == 5  # map + 4 CIKs


def test_a_seeded_cik_beats_the_ticker_map():
    """SEC moved XOM's ticker to a new entity; company_profiles.cik keeps the right one."""
    got = adapter(seeded={"ZZAA": 9}).fetch(["ZZAA"], session=sec(), sleep=lambda s: None)
    assert got["ZZAA"].value == Decimal("450000000000.0")


@pytest.mark.parametrize("agent", [None, "", "Perennial research you@example.com"])
def test_adapter_without_a_contact_is_missing_credential(agent):
    from src.ingestion.market_caps import MarketCapAdapterError

    with pytest.raises(MarketCapAdapterError) as caught:
        adapter(agent).fetch(["ZZAA"], session=sec())
    assert caught.value.code == "missing_credential"


def test_a_failed_request_fails_the_fetch_with_its_code():
    from src.ingestion.market_caps import MarketCapAdapterError

    session = sec()
    session.floats[1] = FakeHttp(500)
    with pytest.raises(MarketCapAdapterError) as caught:
        adapter().fetch(["ZZAA"], session=session, sleep=lambda s: None)
    assert caught.value.code == "http_500"


def test_registry_builds_the_adapter_without_touching_the_database(monkeypatch):
    from src.ingestion.market_caps import ADAPTERS, SecPublicFloatAdapter
    from src.settings import load_settings

    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://user:synthetic@localhost/test")
    monkeypatch.setenv("SEC_USER_AGENT", AGENT)
    monkeypatch.setenv("MARKET_CAP_ADAPTER", "sec_public_float")
    settings = load_settings(local_dev=False)
    built = ADAPTERS[settings.market_cap_adapter](settings)
    assert isinstance(built, SecPublicFloatAdapter) and built.user_agent == AGENT
    assert callable(built.seeded_ciks)  # the database is read only when fetch() runs


# ── consensus.py (legacy JSON pipeline) ──────────────────


class FakeClient:
    def __init__(self, floats, ciks=None):
        from src.services.fundamentals.edgar import PublicFloat

        self.ciks = ciks or {"ZZAA": 1, "ZZB-B": 2}
        self.floats = {
            cik: PublicFloat(Decimal(v), date.fromisoformat(d), date(2026, 2, 19), "10-K")
            for cik, (v, d) in floats.items()
        }
        self.asked = []

    def ticker_map(self):
        return dict(self.ciks)

    def public_float(self, cik):
        self.asked.append(cik)
        return self.floats.get(cik)


def test_legacy_lookup_returns_the_value_and_its_measurement_date():
    from src.services.consensus_watchlist import consensus

    client = FakeClient({2: ("12000000000", "2025-06-30")})
    lookup = consensus.PublicFloatLookup(AGENT, client=client)
    cache = {}
    assert consensus.fetch_market_cap("ZZB.B", cache, lookup) == (12e9, "2025-06-30")
    assert consensus.fetch_market_cap("ZZAA", cache, lookup) == (None, None)
    assert consensus.fetch_market_cap("ZZZZ", cache, lookup) == (None, None)
    assert client.asked == [2, 1]  # ZZZZ has no CIK, so no request
    assert cache == {"ZZB.B": 12e9, "ZZAA": None, "ZZZZ": None}
    assert consensus.fetch_market_cap("ZZB.B", cache, lookup) == (12e9, None)  # cached
    assert client.asked == [2, 1]


def test_legacy_lookup_without_a_contact_has_no_values(caplog):
    from src.services.consensus_watchlist import consensus

    lookup = consensus.PublicFloatLookup("Perennial research you@example.com")
    with caplog.at_level(logging.WARNING, logger="consensus"):
        assert consensus.fetch_market_cap("ZZAA", {}, lookup) == (None, None)
    assert "SEC_USER_AGENT is not set" in caplog.text


def test_legacy_lookup_logs_a_failed_request_and_moves_on(caplog):
    from src.services.consensus_watchlist import consensus
    from src.services.fundamentals.edgar import EdgarError

    client = FakeClient({})
    client.public_float = lambda cik: (_ for _ in ()).throw(EdgarError("http_503"))
    lookup = consensus.PublicFloatLookup(AGENT, client=client)
    with caplog.at_level(logging.WARNING, logger="consensus"):
        assert consensus.fetch_market_cap("ZZAA", {}, lookup) == (None, None)
    assert "Public float lookup failed for ZZAA: http_503" in caplog.text
