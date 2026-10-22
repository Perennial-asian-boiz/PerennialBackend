"""GDELT macro events: DOC API parsing, pacing, spike rule, theme map, and the daily run."""
import json
from datetime import date, datetime, timedelta, timezone

import pytest
from src.ingestion import collectors
from src.services.gdelt import client as gdelt_client
from src.services.gdelt.client import (
    Article,
    DayVolume,
    DocApiClient,
    GdeltError,
    Pacer,
    parse_articles,
    parse_tone,
    parse_volume,
)
from src.services.gdelt.job import run_gdelt
from src.services.gdelt.themes import ThemeMap, load_theme_map

DAY = date(2026, 10, 13)


def test_volume_sums_points_per_utc_day():
    payload = {"timeline": [{"series": "Article Count", "data": [
        {"date": "20261012T000000Z", "value": 40, "norm": 1000},
        {"date": "20261013T000000Z", "value": 10, "norm": 500},
        {"date": "20261013T120000Z", "value": 5, "norm": 400},
    ]}]}
    assert parse_volume(payload) == {
        date(2026, 10, 12): DayVolume(40, 1000),
        DAY: DayVolume(15, 900),
    }
    assert parse_volume({}) == {}


@pytest.mark.parametrize("payload", [
    [], {"timeline": "x"}, {"timeline": [{"data": [{"date": "bad", "value": 1}]}]},
    {"timeline": [{"data": [{"date": "20261013T000000Z", "value": -1}]}]},
    {"timeline": [{"data": [{"date": "20261013T000000Z", "value": True}]}]},
])
def test_malformed_volume_is_rejected(payload):
    with pytest.raises(GdeltError):
        parse_volume(payload)


def test_tone_averages_points_per_day():
    payload = {"timeline": [{"data": [
        {"date": "20261013T000000Z", "value": -2.0},
        {"date": "20261013T120000Z", "value": -4.0},
    ]}]}
    assert parse_tone(payload) == {DAY: -3.0}


def test_articles_skip_missing_urls_and_normalize_fields():
    payload = {"articles": [
        {"url": "https://www.reuters.com/a", "title": " Oil jumps ", "domain": "Reuters.com",
         "seendate": "20261013T101500Z"},
        {"title": "no url"},
        {"url": "https://x.example/b", "title": "", "seendate": "garbage"},
    ]}
    assert parse_articles(payload) == [
        Article("https://www.reuters.com/a", "Oil jumps", "reuters.com",
                datetime(2026, 10, 13, 10, 15, tzinfo=timezone.utc)),
        Article("https://x.example/b", None, None, None),
    ]


def test_pacer_spaces_requests():
    clock, slept = [100.0], []
    def sleep(s):
        slept.append(s)
        clock[0] += s
    pacer = Pacer(5.5, monotonic=lambda: clock[0], sleep=sleep)
    pacer.wait()
    clock[0] += 2
    pacer.wait()
    clock[0] += 10
    pacer.wait()
    assert slept == [3.5]


def test_client_sends_theme_query_and_surfaces_error_codes(monkeypatch):
    seen = []
    def get(session, url, *, params):
        seen.append(params)
        return collectors._Response(False, status=429, code="http_429")
    monkeypatch.setattr(gdelt_client, "_get_json", get)
    client = DocApiClient(session=object(), pacer=Pacer(0))
    with pytest.raises(GdeltError, match="http_429"):
        client.volume("ECON_OILPRICE")
    assert seen == [{"query": "theme:ECON_OILPRICE sourcelang:english",
                     "mode": "timelinevolraw", "timespan": "3m", "format": "json"}]


def _baseline(value, days=28, end=DAY):
    return {end - timedelta(days=n): value for n in range(1, days + 1)}


def test_theme_map_matches_subdomains_and_rejects_bad_codes(tmp_path):
    shipped = load_theme_map()
    assert "ECON_OILPRICE" in shipped.themes
    assert shipped.is_evidence_domain("uk.reuters.com")
    assert not shipped.is_evidence_domain("notreuters.com")
    path = tmp_path / "map.json"
    path.write_text(json.dumps({"themes": {"oil prices": {}}}))
    with pytest.raises(ValueError, match="GDELT theme codes"):
        load_theme_map(path)


class FakeClient:
    def __init__(self, volumes, failing=()):
        self.volumes, self.failing = volumes, set(failing)

    def volume(self, theme):
        if theme in self.failing:
            raise GdeltError("http_429")
        return self.volumes[theme]

    def tone(self, theme):
        return {d: -1.5 for d in self.volumes[theme]}

    def articles(self, theme, day):
        return [Article(f"https://www.reuters.com/{theme}/{i}", f"t{i}", "reuters.com", None)
                for i in range(7)] + [Article("https://spam.example/x", "x", "spam.example", None)]


def _volumes(target_count):
    counts = {**_baseline(30), DAY: target_count, DAY + timedelta(days=1): 3}
    return {d: DayVolume(c, 10_000) for d, c in counts.items()}


@pytest.mark.db
def test_daily_run_goes_through_the_contract_and_leaves_the_0005_tables(engine):
    theme_map = ThemeMap({"ECON_OILPRICE": {}, "ECON_INFLATION": {}, "ECON_TRADE_DISPUTE": {}},
                         ("reuters.com",))
    now = datetime(2026, 10, 14, 16, 30, tzinfo=timezone.utc)
    volumes = {"ECON_OILPRICE": _volumes(90), "ECON_INFLATION": _volumes(31),
               "ECON_TRADE_DISPUTE": _volumes(5)}
    fake = FakeClient(volumes, failing={"ECON_TRADE_DISPUTE"})

    summary = run_gdelt(engine, client=fake, theme_map=theme_map, now=now)
    assert summary.day == DAY
    # One failed theme fails the volume batch (G1); articles and the derivation
    # (nothing to derive from yet) are reported per source with fixed codes.
    assert summary.failed == {"gdelt_theme_volume": "partial_collection",
                              "gdelt_spike_results": "missing_upstream"}
    assert summary.succeeded == ["gdelt_theme_articles"]

    fake.failing = set()
    summary = run_gdelt(engine, client=fake, theme_map=theme_map, now=now)
    assert summary.failed == {} and summary.events == ["ECON_OILPRICE"]
