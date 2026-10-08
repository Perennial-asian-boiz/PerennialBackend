"""
Contract and end-to-end tests between the pipeline runner and the REAL collectors.

Written by the perennial-database rig (roadmap REVISION 1, P1-3) against the
interface the perennial-pipeline `sources` seat announced:
  * collect_insider(..., *, upstream=None) / collect_short_interest(..., *, upstream=None)
    with upstream = {"trades": [...], "holdings": [...]}; supplied data never
    falls back to files;
  * every successful outcome carries coverage {"complete": True, "scope": ...}
    (Congress "recent_window", ARK "snapshot"), only when every unit succeeded.

Unlike tests/test_pipeline.py, nothing here injects fake collectors into the
runner: COLLECTORS are the production functions, and only HTTP is stubbed at
requests.Session. A test that passes only with fake collectors cannot catch a
signature mismatch such as D1.

Uses the shared `engine` / `fixture_path` fixtures from conftest.py read-only.
"""

import builtins
import inspect
import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import requests
from sqlalchemy import select

from src.db import models
from src.ingestion.importer import CollectionOutcome, import_collection
from src.pipeline.lifecycle import begin_run

pytestmark = pytest.mark.db

DOWNSTREAM = ("insider_trades", "short_interest")
UPSTREAM = ("congress_trades", "ark_holdings")
FIXTURE_FILES = {
    "congress_trades": ("trades_congress.json", "trades"),
    "ark_holdings": ("ark_holdings.json", "holdings"),
    "insider_trades": ("trades_insider.json", "transactions"),
    "short_interest": ("short_interest.json", "records"),
}


# ── helpers ──────────────────────────────────────────────

def seed_live_upstream(engine, fixture_path):
    """Import the Congress and ARK fixtures as successful *live* runs; return their batch IDs."""
    batches = {}
    for source in UPSTREAM:
        filename, key = FIXTURE_FILES[source]
        run = begin_run(engine, source, "live")
        records = json.loads(fixture_path(filename).read_text())[key]
        outcome = CollectionOutcome.success(
            source, "live", records, coverage={"complete": True, "scope": "snapshot"},
            source_as_of=datetime.now(timezone.utc).date() if source == "ark_holdings" else None)
        result = import_collection(engine, outcome, run_id=run)
        assert result.succeeded
        batches[source] = result.batch_id
    return batches


class StubResponse:
    """Enough of requests.Response for both streamed and non-streamed readers."""

    def __init__(self, body, status=200):
        self.status_code = status
        self._raw = json.dumps(body).encode()
        self.headers = {"Content-Type": "application/json", "Content-Length": str(len(self._raw))}
        self.content = self._raw
        self.text = self._raw.decode()
        self.ok = status < 400

    def iter_content(self, chunk_size=1, decode_unicode=False):
        for i in range(0, len(self._raw), chunk_size or 1):
            yield self._raw[i:i + (chunk_size or 1)]

    def json(self, **kwargs):
        return json.loads(self._raw)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"http {self.status_code}")

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Provider:
    """Synthetic provider universe; records every requested ticker per source."""

    def __init__(self):
        today = datetime.now(timezone.utc).date()
        self.today = today
        self.trade_day = (today - timedelta(days=10)).isoformat()
        self.requested = {"insider_trades": [], "short_interest": []}
        self.unrouted = []

    def fmp_trades(self, chamber):
        return [{
            "symbol": "ZZAA" if chamber == "senate" else "ZZCC",
            "firstName": "Synthetic", "lastName": f"Member {chamber}", "type": "Purchase",
            "amount": "$1,001 - $15,000", "transactionDate": self.trade_day,
            "disclosureDate": (self.today - timedelta(days=3)).isoformat(),
            "assetDescription": "Synthetic Corp", "assetType": "Stock", "district": "",
            "link": f"https://disclosures.example.invalid/{chamber}/1",
        }]

    def ark_holdings(self, fund):
        day = self.today.isoformat()
        rows = [{"ticker": "ZZAA", "company": "Synthetic Alpha", "weight": 2.5, "share_price": 10.0, "date": day}]
        rows.append({"ticker": {"ARKK": "ZZDD", "ARKQ": "ZZEE", "ARKG": "ZZDD", "ARKW": "ZZEE"}[fund],
                     "company": "Synthetic Other", "weight": 1.25, "share_price": 5.0, "date": day})
        return {"symbol": fund, "holdings": rows}

    def insider(self, ticker):
        return {"data": {"insider_transactions": {"recent": [{
            "type": "Purchase", "insider": f"Synthetic Insider {ticker}",
            "date": (self.today - timedelta(days=5)).isoformat(), "value": 50000, "shares": 1000,
        }]}}}

    def short_interest(self, ticker):
        settle = (self.today - timedelta(days=10)).strftime("%m/%d/%Y")
        return {"data": {"shortInterestTable": {"rows": [{
            "settlementDate": settle, "interest": "1,000,000",
            "avgDailyShareVolume": "500,000", "daysToCover": "2.0",
        }]}}}

    def get(self, session, url, params=None, **kwargs):
        params = params or {}
        if "financialmodelingprep.com" in url and url.endswith("-latest"):
            chamber = url.rsplit("/", 1)[1].split("-")[0]
            return StubResponse(self.fmp_trades(chamber) if int(params.get("page", 0)) == 0 else [])
        if "senate-stock-watcher-data" in url:
            return StubResponse([])
        if "arkfunds.io" in url:
            return StubResponse(self.ark_holdings(params["symbol"]))
        if "securitiesdb.com" in url and url.endswith("/insider-activity"):
            ticker = url.rstrip("/").split("/")[-2]
            self.requested["insider_trades"].append(ticker)
            return StubResponse(self.insider(ticker))
        if "api.nasdaq.com" in url and "/short-interest" in url:
            ticker = url.split("/quote/")[1].split("/")[0]
            self.requested["short_interest"].append(ticker)
            return StubResponse(self.short_interest(ticker))
        self.unrouted.append(url.split("?")[0])
        return StubResponse({}, status=404)


@pytest.fixture
def provider(monkeypatch):
    """Stub HTTP for every production collector; forbid any other network path."""
    from src.ingestion import collectors

    stub = Provider()

    def get(self, url, params=None, **kwargs):
        return stub.get(self, url, params=params, **kwargs)

    def forbidden(self, method, url, *args, **kwargs):
        raise AssertionError(f"unexpected network request: {method} {url.split('?')[0]}")

    monkeypatch.setattr(requests.Session, "get", get)
    monkeypatch.setattr(requests.Session, "request", forbidden)
    for name in ("ark", "insider", "short_interest"):
        monkeypatch.setattr(collectors._fetcher(name), "REQUEST_DELAY", 0)
    monkeypatch.setattr(collectors._fetcher("fmp"), "FMP_API_KEY", "synthetic-test-key")
    return stub


@pytest.fixture
def poisoned_files(tmp_path, monkeypatch):
    """
    Point every fetcher data directory at files naming a ticker that exists
    nowhere else, and record any attempt to open a file there or in the real
    LOCAL_DATA_DIR. The database path must read neither.
    """
    from src.ingestion import collectors
    from src.services.consensus_watchlist import paths as package_paths

    (tmp_path / "trades_congress.json").write_text(json.dumps(
        {"trades": [{"ticker": "ZZPOISON", "trade_type": "Purchase"}]}))
    (tmp_path / "ark_holdings.json").write_text(json.dumps(
        {"holdings": [{"ticker": "ZZPOISON", "fund_count": 1, "funds": ["ARKK"]}]}))
    watched = (str(tmp_path), str(package_paths.LOCAL_DATA_DIR))
    for module in [collectors._fetcher(n) for n in ("insider", "short_interest")]:
        monkeypatch.setattr(module, "LOCAL_DATA_DIR", tmp_path)
    import paths as fetcher_paths  # the module object the fetchers import by bare name

    monkeypatch.setattr(fetcher_paths, "LOCAL_DATA_DIR", tmp_path)

    opened = []
    real_os_open, real_open = os.open, builtins.open

    def spy_os_open(path, *args, **kwargs):
        if str(path).startswith(watched):
            opened.append(str(path))
        return real_os_open(path, *args, **kwargs)

    def spy_open(file, *args, **kwargs):
        if isinstance(file, (str, Path)) and str(file).startswith(watched):
            opened.append(str(file))
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(os, "open", spy_os_open)
    monkeypatch.setattr(builtins, "open", spy_open)
    return opened


def stub_market_caps(symbols):
    caps = {"ZZAA": Decimal("200000000000"), "ZZCC": Decimal("50000000000")}
    return {s: {"value": caps.get(s, Decimal("3000000000")), "currency": "USD", "provider": "fmp",
                "retrieved_at": datetime.now(timezone.utc), "observed_at": None} for s in symbols}


# ── P1-3: signature contract (D1) ────────────────────────

def test_real_collectors_accept_exactly_what_runner_passes(engine, fixture_path):
    """
    Record the call collect_source() actually makes for each source, then bind
    that same call to the production COLLECTORS entry. Fails on the pre-fix
    tree: the runner passes upstream= to collect_insider / collect_short_interest,
    which do not declare it (D1).
    """
    from src.ingestion.collectors import COLLECTORS
    from src.pipeline.runner import collect_source

    batches = seed_live_upstream(engine, fixture_path)
    mismatches = {}
    for source in models.SOURCES:
        calls = []

        def spy(*args, _source=source, **kwargs):
            calls.append((args, kwargs))
            return CollectionOutcome.failure(_source, "live", "collection_failed")

        upstream = {s: batches[s] for s in UPSTREAM} if source in DOWNSTREAM else None
        collect_source(engine, source, upstream_batches=upstream, collector=spy)
        assert len(calls) == 1, f"runner did not call the {source} collector exactly once"
        args, kwargs = calls[0]
        if source in DOWNSTREAM:
            assert set(kwargs) == {"upstream"} and set(kwargs["upstream"]) == {"trades", "holdings"}
        try:
            inspect.signature(COLLECTORS[source]).bind(*args, **kwargs)
        except TypeError as exc:
            mismatches[source] = f"{sorted(kwargs)} rejected: {exc}"
    assert mismatches == {}


def test_runner_upstream_rows_satisfy_collector_plan_contract(engine, fixture_path):
    """
    The DB-shaped rows the runner hands downstream must carry every field the
    collectors' plan builders read (ticker, trade_type, fund_count, funds).
    """
    from src.pipeline.runner import _upstream

    batches = seed_live_upstream(engine, fixture_path)
    with engine.connect() as conn:
        upstream = _upstream(conn, batches)
    assert upstream["trades"] and upstream["holdings"]
    for row in upstream["trades"]:
        assert isinstance(row["ticker"], str) and row["ticker"].strip()
        assert isinstance(row["trade_type"], str) and row["trade_type"].strip()
    for row in upstream["holdings"]:
        assert isinstance(row["ticker"], str) and row["ticker"].strip()
        assert isinstance(row["fund_count"], int) and row["fund_count"] >= 1
        assert len(row["funds"]) == row["fund_count"]


# ── P1-3: end to end with real collectors (D1, D2, D4) ───

def test_run_pipeline_end_to_end_with_real_collectors(engine, provider, poisoned_files):
    """
    run_pipeline() with production COLLECTORS and HTTP stubbed must publish;
    downstream plans must come only from the pinned upstream batches; replay
    must reproduce the current output.
    """
    from src.pipeline.publication import current, replay
    from src.pipeline.runner import run_pipeline

    pub = run_pipeline(engine, market_cap_collector=stub_market_caps)

    assert provider.unrouted == []
    assert current(engine)["id"] == pub
    assert replay(engine, pub) == current(engine)["output"]

    with engine.connect() as conn:
        selected = dict(conn.execute(
            select(models.publication_sources.c.source, models.publication_sources.c.run_id)
            .where(models.publication_sources.c.publication_id == pub)).all())
        batch_of = dict(conn.execute(
            select(models.publication_sources.c.source, models.publication_sources.c.batch_id)
            .where(models.publication_sources.c.publication_id == pub)).all())
        runs = {r["source"]: r for r in conn.execute(
            select(models.ingestion_runs).where(models.ingestion_runs.c.id.in_(selected.values()))
        ).mappings()}
        deps = {s: dict(conn.execute(
            select(models.run_dependencies.c.source, models.run_dependencies.c.batch_id)
            .where(models.run_dependencies.c.run_id == selected[s])).all()) for s in DOWNSTREAM}
        upstream_symbols = {s: set(conn.execute(
            select(models.securities.c.symbol)
            .select_from(models.SOURCE_TABLES[s].join(
                models.securities, models.securities.c.id == models.SOURCE_TABLES[s].c.security_id))
            .where(models.SOURCE_TABLES[s].c.batch_id == batch_of[s])).scalars()) for s in UPSTREAM}

    # D2: real collectors report complete coverage with the announced scopes.
    for source, run in runs.items():
        assert run["coverage"] and run["coverage"]["complete"] is True, source
    assert runs["congress_trades"]["coverage"]["scope"] == "recent_window"
    assert runs["ark_holdings"]["coverage"]["scope"] == "snapshot"

    # D4: lineage is the upstream batches the publication uses, and the plans
    # were built from exactly those batches, never from files.
    expected_deps = {s: batch_of[s] for s in UPSTREAM}
    assert deps == {s: expected_deps for s in DOWNSTREAM}
    universe = upstream_symbols["congress_trades"] | upstream_symbols["ark_holdings"]
    assert universe == {"ZZAA", "ZZCC", "ZZDD", "ZZEE"}
    assert set(provider.requested["insider_trades"]) == universe
    assert set(provider.requested["short_interest"]) == universe
    assert "ZZPOISON" not in provider.requested["insider_trades"] + provider.requested["short_interest"]
    assert poisoned_files == []


# ── P1-5: programming errors stay visible (closed vocabulary) ──

def test_collector_programming_error_is_classified_without_message(engine):
    """
    A TypeError inside a collector (like D1) must surface as internal_error with
    the exception class name in diagnostics, never as a bare collection_failed,
    and never with the exception message.
    """
    from src.pipeline.runner import collect_source

    def broken():
        raise TypeError("SYNTHETIC_SECRET_7f3a must not be stored")

    result = collect_source(engine, "ark_holdings", collector=broken)
    assert result.status == "failed"
    with engine.connect() as conn:
        run = conn.execute(select(models.ingestion_runs)
                           .where(models.ingestion_runs.c.id == result.run_id)).mappings().one()
    assert "SYNTHETIC_SECRET" not in json.dumps(dict(run), default=str)
    assert run["error_code"] == "internal_error"
    assert (run["diagnostics"] or {}).get("exception", {}).get("name") == "TypeError"
