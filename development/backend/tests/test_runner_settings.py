"""Startup Settings reach every collector the runner calls (roadmap Gate M0).

Provider keys exist only in the environment Settings read; every fetcher
module constant is cleared, and HTTP is stubbed at requests.Session. Fails on
12e4b1b, where run-pipeline called collectors without Settings and Congress
ended missing_credential.
"""

import json

import pytest
import requests
from sqlalchemy import select

from src.db import models
from src.ingestion.importer import CollectionOutcome
from test_pipeline_contract import Provider, StubResponse

FMP_KEY = "synthetic-settings-fmp"
FINNHUB_KEY = "synthetic-settings-finnhub"


class KeyedProvider(Provider):
    """The contract-test provider plus FMP profiles, recording the key on each call."""

    def __init__(self):
        super().__init__()
        self.keys = {"fmp": [], "finnhub": []}

    def get(self, session, url, params=None, headers=None, **kwargs):
        params, headers = params or {}, headers or {}
        if "financialmodelingprep.com" in url:
            self.keys["fmp"].append(params.get("apikey"))
        if "finnhub.io" in url:
            self.keys["finnhub"].append(
                headers.get("X-Finnhub-Token") or params.get("token")
            )
        if url.endswith("/stable/profile"):
            return StubResponse(
                [{"symbol": params["symbol"], "marketCap": 200000000000}]
            )
        return super().get(session, url, params=params, **kwargs)


@pytest.fixture
def keyed_provider(monkeypatch):
    from src.ingestion import collectors

    stub = KeyedProvider()

    def get(self, url, params=None, headers=None, **kwargs):
        return stub.get(self, url, params=params, headers=headers, **kwargs)

    def forbidden(self, method, url, *args, **kwargs):
        raise AssertionError(f"unexpected network request: {method} {url.split('?')[0]}")

    monkeypatch.setattr(requests.Session, "get", get)
    monkeypatch.setattr(requests.Session, "request", forbidden)
    # These tests pin FMP key routing, so Congress takes both chambers from FMP;
    # the House Clerk default is covered by the test at the end of this file.
    monkeypatch.setenv("CONGRESS_HOUSE_SOURCE", "fmp")
    monkeypatch.setenv("CONGRESS_INCLUDE_FMP_SENATE", "true")
    for name in ("ark", "insider", "short_interest", "fmp"):
        module = collectors._fetcher(name)
        if hasattr(module, "REQUEST_DELAY"):
            monkeypatch.setattr(module, "REQUEST_DELAY", 0)
        for attr in ("FMP_API_KEY", "FINNHUB_API_KEY", "API_KEY"):
            if hasattr(module, attr):
                monkeypatch.setattr(module, attr, None)
    return stub


@pytest.mark.db
def test_cli_run_pipeline_publishes_with_keys_only_in_settings(
    engine, db_url, keyed_provider, monkeypatch, capsys
):
    from src.ingestion import cli

    monkeypatch.setenv("DATABASE_URL", db_url.render_as_string(hide_password=False))
    monkeypatch.setenv("FMP_API_KEY", FMP_KEY)
    monkeypatch.setenv("FINNHUB_API_KEY", FINNHUB_KEY)

    assert cli.main(["run-pipeline"]) == 0
    out = capsys.readouterr().out
    # The fmp fetcher still print()s progress lines first (P2-A logging scope).
    publication_id = json.loads(out[out.rindex("{\n"):])["publication_id"]

    assert keyed_provider.unrouted == []
    # Congress and market caps both called FMP, and only with the Settings key.
    assert keyed_provider.keys["fmp"] and set(keyed_provider.keys["fmp"]) == {FMP_KEY}
    assert set(keyed_provider.keys["finnhub"]) <= {FINNHUB_KEY}
    with engine.connect() as conn:
        pinned = conn.execute(
            select(models.publication_sources.c.source).where(
                models.publication_sources.c.publication_id == publication_id
            )
        ).scalars()
        assert sorted(pinned) == sorted(models.SOURCES)
        statuses = dict(
            conn.execute(
                select(models.ingestion_runs.c.source, models.ingestion_runs.c.status)
            ).all()
        )
    assert statuses == {source: "succeeded" for source in models.SOURCES}


@pytest.mark.db
def test_collect_source_passes_settings_only_to_collectors_that_declare_them(engine):
    from src.pipeline.runner import collect_source
    from src.settings import Settings

    settings = Settings(database_url="postgresql+psycopg://u@127.0.0.1/x_test")
    seen = {}

    def declares(*, settings=None):
        seen["declares"] = settings
        return CollectionOutcome.failure("ark_holdings", "live", "collection_failed")

    def plain():
        seen["plain"] = True
        return CollectionOutcome.failure("ark_holdings", "live", "collection_failed")

    for collector in (declares, plain):
        result = collect_source(
            engine, "ark_holdings", collector=collector, settings=settings
        )
        # A TypeError from an unexpected keyword would surface as internal_error.
        assert result.error_code == "collection_failed"
    assert seen == {"declares": settings, "plain": True}


def test_scheduler_passes_startup_settings_to_the_pipeline(monkeypatch):
    from src.pipeline import scheduler
    from src.settings import Settings

    settings = Settings(
        database_url="postgresql+psycopg://u@127.0.0.1/x_test",
        fmp_api_key=FMP_KEY,
        finnhub_api_key=FINNHUB_KEY,
    )
    received = {}

    class Engine:
        def dispose(self):
            pass

    monkeypatch.setattr(scheduler, "make_engine", lambda _url, **kwargs: Engine())

    def fake_run_pipeline(engine, **kwargs):
        received.update(kwargs)
        return 7

    monkeypatch.setattr(scheduler, "run_pipeline", fake_run_pipeline)
    assert scheduler.run_full_pipeline(settings) == 7
    assert received["settings"] is settings


# Sources whose collector needs a provider key, and which key. ARK and short
# interest are keyless public endpoints.
KEYED_SOURCES = {"congress_trades": "fmp", "insider_trades": "finnhub"}


def test_every_keyed_collector_declares_settings():
    import inspect

    from src.ingestion.collectors import COLLECTORS
    from src.pipeline.runner import _accepts_settings

    for source in KEYED_SOURCES:
        assert "settings" in inspect.signature(COLLECTORS[source]).parameters, source
        assert _accepts_settings(COLLECTORS[source]), source


@pytest.mark.db
def test_cli_fetch_uses_keys_only_in_settings(
    engine, db_url, keyed_provider, monkeypatch, capsys
):
    """L5: `fetch <source>` passes startup Settings, like run-pipeline."""
    from src.ingestion import cli

    monkeypatch.setenv("DATABASE_URL", db_url.render_as_string(hide_password=False))
    monkeypatch.setenv("FMP_API_KEY", FMP_KEY)
    monkeypatch.setenv("FINNHUB_API_KEY", FINNHUB_KEY)

    # insider_trades plans from the latest live Congress and ARK batches.
    for source in ("congress_trades", "ark_holdings", "insider_trades"):
        assert cli.main(["fetch", source]) == 0, source
    capsys.readouterr()

    assert keyed_provider.unrouted == []
    assert keyed_provider.keys["fmp"] and set(keyed_provider.keys["fmp"]) == {FMP_KEY}
    assert keyed_provider.keys["finnhub"]
    assert set(keyed_provider.keys["finnhub"]) == {FINNHUB_KEY}
    with engine.connect() as conn:
        statuses = dict(
            conn.execute(
                select(models.ingestion_runs.c.source, models.ingestion_runs.c.status)
            ).all()
        )
    assert statuses == {
        "congress_trades": "succeeded",
        "ark_holdings": "succeeded",
        "insider_trades": "succeeded",
    }


class RawStubResponse(StubResponse):
    """A binary body (House Clerk ZIP index or PTR PDF)."""

    def __init__(self, body: bytes, status=200):
        super().__init__(None, status)
        self._raw = self.content = body
        self.headers = {"Content-Length": str(len(body))}


@pytest.mark.db
def test_cli_run_pipeline_takes_house_trades_from_the_house_clerk_by_default(
    engine, db_url, keyed_provider, monkeypatch, capsys, tmp_path
):
    """Production defaults: House from the Clerk of the House, no FMP Senate,
    FMP used only for market caps; the publication ranks with watchlist-v3."""
    from datetime import date, timedelta

    from src.ingestion import cli, collectors
    from src.pipeline import ranking
    from test_house_clerk import index_row, index_zip, ptr_text

    hc = collectors._fetcher("house_clerk")
    monkeypatch.setattr(hc, "LOCAL_DATA_DIR", tmp_path)  # cache stays in the test
    monkeypatch.setenv("CONGRESS_HOUSE_SOURCE", "house_clerk")
    monkeypatch.setenv("CONGRESS_INCLUDE_FMP_SENATE", "false")
    monkeypatch.setenv("DATABASE_URL", db_url.render_as_string(hide_password=False))
    monkeypatch.setenv("FMP_API_KEY", FMP_KEY)
    monkeypatch.setenv("FINNHUB_API_KEY", FINNHUB_KEY)

    recent = (date.today() - timedelta(days=10)).strftime("%m/%d/%Y")
    index = index_zip([index_row("20000001", recent), index_row("8000002", recent)])
    text = ptr_text(
        "Synthetic Gamma Corp Common Stock (ZZCC) [ST]\n"
        f"P {recent} {recent} $1,001 - $15,000"
    )
    monkeypatch.setattr(hc, "extract_text", lambda pdf: text if pdf == b"efiled" else "")
    original = keyed_provider.get

    def get(session, url, params=None, headers=None, **kwargs):
        if "disclosures-clerk.house.gov" in url:
            if url.endswith("FD.zip"):
                return RawStubResponse(index)
            return RawStubResponse(b"efiled" if url.endswith("20000001.pdf") else b"scan")
        return original(session, url, params=params, headers=headers, **kwargs)

    monkeypatch.setattr(keyed_provider, "get", get)

    assert cli.main(["run-pipeline"]) == 0
    out = capsys.readouterr().out
    publication_id = json.loads(out[out.rindex("{\n"):])["publication_id"]

    assert keyed_provider.unrouted == []
    with engine.connect() as conn:
        congress_batch = conn.execute(
            select(models.ingestion_runs.c.batch_id).where(
                models.ingestion_runs.c.source == "congress_trades"
            )
        ).scalar_one()
        from src.db.queries import batch_rows

        rows = batch_rows(conn, "congress_trades", congress_batch)
        output, version = conn.execute(
            select(models.publications.c.output, models.publications.c.ranking_version)
            .where(models.publications.c.id == publication_id)
        ).one()
    assert [(r["symbol"], r["chamber"], r["data_source"]) for r in rows] == [
        ("ZZCC", "House", "house_clerk")
    ]
    assert version == output["ranking_version"] == ranking.VERSION
    tickers = {r["ticker"] for b in ("popular_stable", "affordable_growing") for r in output[b]}
    assert "ZZCC" in tickers
