"""Synthetic logging boundaries for provider errors and CLI diagnostics."""

import ast
import logging
from pathlib import Path

import pytest
import requests
from src.ingestion import collectors

SECRET = "SYNTHETIC_FETCHER_LOG_SECRET"
URL = "https://provider.invalid/private?apikey=" + SECRET


class FailingSession:
    calls = 0

    def get(self, *args, **kwargs):
        self.calls += 1
        raise requests.ConnectionError(URL)


@pytest.mark.parametrize("name", ["ark", "congress", "insider", "short_interest"])
def test_each_collector_error_is_safe(name, caplog, capsys):
    session = FailingSession()
    kwargs = {"session": session, "sleep": lambda _: None}
    if name in {"congress", "insider"}:
        kwargs["api_key"] = SECRET
    if name in {"insider", "short_interest"}:
        kwargs["upstream"] = {
            "trades": [{"ticker": "ZZAA", "trade_type": "Purchase"}],
            "holdings": [],
        }
    with caplog.at_level(logging.INFO):
        result = getattr(collectors, "collect_" + name)(**kwargs)
    assert session.calls > 0 and not result.succeeded
    out = capsys.readouterr()
    combined = caplog.text + out.out + out.err
    assert SECRET not in combined and URL not in combined
    assert out.out == ""


@pytest.mark.parametrize("name", ["fmp", "ark", "insider", "short_interest"])
def test_legacy_fetcher_error_logs_are_safe(name, monkeypatch, caplog, capsys):
    module = collectors._fetcher(name)
    session = FailingSession()
    monkeypatch.setattr(module.requests, "get", session.get)
    if hasattr(module, "time"):
        monkeypatch.setattr(module.time, "sleep", lambda _: None)
    with caplog.at_level(logging.INFO):
        if name == "fmp":
            assert module.legacy_fetch_chamber_trades("house", api_key=SECRET) == []
        elif name == "ark":
            assert module.legacy_fetch_fund_holdings("ARKK") == []
        elif name == "insider":
            monkeypatch.setattr(module, "FINNHUB_API_KEY", SECRET)
            _, outcomes = module.legacy_scan_tickers(["ZZAA"], "popular_stable", "synthetic")
            assert outcomes[0]["status"] == module.STATUS_FAILED
        else:
            assert module.legacy_fetch_short_interest("ZZAA", session) is None
    out = capsys.readouterr()
    combined = caplog.text + out.out + out.err
    assert SECRET not in combined and URL not in combined
    assert out.out == ""
    assert caplog.records, "a failed request must produce a diagnostic"


def test_src_has_no_print_calls_except_legacy_cron():
    root = Path(__file__).resolve().parents[1] / "src"
    violations = []
    for path in root.rglob("*.py"):
        if path == root / "services/consensus_watchlist/scheduler/cron.py":
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "print"
            ):
                violations.append(f"{path.relative_to(root)}:{node.lineno}")
    assert violations == []


@pytest.fixture
def search_module(monkeypatch):
    # Optional parsers are not installed in the core CI environment. Stub only
    # those dependencies; exercise the actual search logging/HTTP code below.
    import importlib.util
    import sys
    from types import ModuleType

    bs4 = ModuleType("bs4")
    bs4.BeautifulSoup = lambda *_: None
    monkeypatch.setitem(sys.modules, "bs4", bs4)
    monkeypatch.setitem(sys.modules, "trafilatura", ModuleType("trafilatura"))
    path = (
        Path(__file__).resolve().parents[1]
        / "src/services/search_engine/search_engine.py"
    )
    spec = importlib.util.spec_from_file_location("synthetic_search_logging", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_search_does_not_log_query_or_result_url(search_module, monkeypatch, caplog):
    from types import SimpleNamespace

    response = SimpleNamespace(
        status_code=200, text="synthetic", raise_for_status=lambda: None
    )
    link = SimpleNamespace(get=lambda key: URL)
    item = SimpleNamespace(select_one=lambda selector: link)
    soup = SimpleNamespace(select=lambda selector: [item])
    monkeypatch.setattr(search_module.requests, "get", lambda *a, **k: response)
    monkeypatch.setattr(search_module, "BeautifulSoup", lambda *a: soup)
    with caplog.at_level(logging.INFO):
        assert search_module.get_top_urls(SECRET, logging.getLogger("test.search")) == [
            URL
        ]
    assert caplog.records
    assert SECRET not in caplog.text and URL not in caplog.text


def test_search_extraction_error_is_safe(search_module, monkeypatch, caplog, tmp_path):
    def fail(url):
        raise requests.HTTPError(URL)

    monkeypatch.setattr(search_module, "extract_content", fail)
    with caplog.at_level(logging.INFO):
        search_module.handle_urls(
            [URL], tmp_path / "results.json", logging.getLogger("test.search")
        )
    assert caplog.records
    assert SECRET not in caplog.text and URL not in caplog.text
    assert "HTTPError" in caplog.text


def test_gdelt_theme_failure_is_safe(caplog):
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from src.services.gdelt import contract

    def fail(theme):
        raise contract.GdeltError(URL)

    client = SimpleNamespace(volume=fail, tone=fail)
    themes = SimpleNamespace(themes=["SYNTHETIC_THEME"])
    with caplog.at_level(logging.INFO):
        outcome = contract.collect_volume(client, themes, datetime.now(timezone.utc))
    assert not outcome.succeeded
    assert "GdeltError" in caplog.text
    assert SECRET not in caplog.text and URL not in caplog.text


@pytest.mark.parametrize("name", ["fmp", "ark", "insider", "short_interest"])
def test_fetcher_import_from_unrelated_directory(name, tmp_path):
    import subprocess
    import sys

    path = (
        Path(__file__).resolve().parents[1]
        / "src/services/consensus_watchlist/fetchers"
        / (name + ".py")
    )
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "import runpy, sys; runpy.run_path(sys.argv[1], run_name='synthetic_import')",
            str(path),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "" and result.stderr == ""
