"""S1-12, decision 76 (option 1): a manual `summaries` subcommand, spec-first.

Bryan runs `python -m src.ingestion.cli summaries` by hand on staging. It loads
settings, builds FmpProfileDescriptions from settings.fmp_api_key, runs
company_summaries.summarize(engine, adapter) as the importer login (the
application's DATABASE_URL, OPERATIONS.md), prints the CS-7 coverage counts
only, and exits non-zero on a failed run. It is never scheduled.

HTTP is faked at requests.Session.get with test_collectors' FakeResponse, so
the real adapter runs and no FMP call is ever made.
"""

import json
import logging
import re
from pathlib import Path

import pytest
import requests
from sqlalchemy import text
from test_collectors import FakeResponse
from test_read_api import groups, login, seed_security  # noqa: F401  (groups: fixture)

KEY = "synthetic-fmp-key-0123456789"
DESCRIPTION = (
    "The firm makes tools. It sells them in shops. Its staff work in one town. "
    "The shops are near homes. It has a small team."
)
REPO = Path(__file__).resolve().parents[3]


@pytest.fixture
def fmp(monkeypatch):
    """requests.Session.get answers FMP profile calls; returns the recorded calls."""
    calls, answer = [], {"status": 200}

    def get(self, url, params=None, headers=None, timeout=None, stream=False):
        calls.append((url, dict(params or {})))
        symbol = (params or {}).get("symbol")
        if answer["status"] != 200:
            return FakeResponse(
                answer["status"], {"Error Message": "synthetic provider body"}
            )
        return FakeResponse(
            200,
            [
                {
                    "symbol": symbol,
                    "description": DESCRIPTION,
                    "currency": "USD",
                    "marketCap": 1000,
                }
            ],
        )

    monkeypatch.setattr(requests.Session, "get", get)
    from src.ingestion import collectors

    monkeypatch.setattr(collectors, "MAX_HTTP_RETRIES", 0)
    return calls, answer


@pytest.fixture
def importer_url(engine, groups):  # noqa: F811
    """DATABASE_URL for a login with exactly importer membership (OPERATIONS.md)."""
    with login(engine, groups, members=("importer",), connect=True) as (
        _,
        name,
        password,
    ):
        yield engine.url.set(username=name, password=password).render_as_string(
            hide_password=False
        )


def members(monkeypatch, *symbols):
    from src.ingestion import company_summaries

    monkeypatch.setattr(
        company_summaries, "consensus_members", lambda engine: list(symbols)
    )


def row(engine, symbol):
    with engine.connect() as conn:
        return (
            conn.execute(
                text(
                    "SELECT p.summary_status, p.summary, p.xmin::text AS xmin"
                    " FROM perennial.company_profiles p JOIN perennial.securities s"
                    " ON s.company_profile_id = p.id WHERE s.symbol = :s"
                ),
                {"s": symbol},
            )
            .mappings()
            .one()
        )


def cli_main(argv):
    from src.ingestion import cli

    return cli.main(argv)


def no_secret_or_company_text(*outputs):
    for output in outputs:
        assert KEY not in output
        for sentence in DESCRIPTION.split(". "):
            assert sentence.strip(". ") not in output, sentence
        assert "synthetic provider body" not in output


# ── the subcommand ───────────────────────────────────────────


def test_the_summaries_subcommand_is_wired_like_the_others():
    from src.ingestion import cli

    parser = cli._parser()
    assert parser.parse_args(["summaries"]).command == "summaries"
    assert "summaries" in parser.format_help()


@pytest.mark.db
def test_a_successful_run_exits_0_and_prints_counts_only(
    engine, importer_url, fmp, monkeypatch, capsys, caplog
):
    seed_security(
        engine, "ZZAA", name="Synthetic ZZAA", profile={"sector": "Industrials"}
    )
    members(monkeypatch, "ZZAA")
    monkeypatch.setenv("DATABASE_URL", importer_url)
    monkeypatch.setenv("FMP_API_KEY", KEY)
    caplog.set_level(logging.DEBUG)
    assert cli_main(["summaries"]) == 0
    out = capsys.readouterr()
    report = json.loads(out.out)
    assert {k: report[k] for k in ("members", "with_summary", "unavailable")} == {
        "members": 1,
        "with_summary": 1,
        "unavailable": 0,
    }
    assert all(isinstance(v, (int, bool)) or v is None for v in report.values()), report
    no_secret_or_company_text(
        out.out, out.err, *(r.getMessage() for r in caplog.records)
    )
    assert (
        row(engine, "ZZAA")["summary_status"] == "ok"
    )  # written by the importer login
    calls, _ = fmp
    assert [p["symbol"] for _, p in calls] == ["ZZAA"]


@pytest.mark.db
def test_a_failed_run_exits_nonzero_with_only_its_code_and_writes_nothing(
    engine, importer_url, fmp, monkeypatch, capsys, caplog
):
    seed_security(
        engine, "ZZAB", name="Synthetic ZZAB", profile={"sector": "Industrials"}
    )
    members(monkeypatch, "ZZAB")
    before = row(engine, "ZZAB")
    _, answer = fmp
    answer["status"] = 500
    monkeypatch.setenv("DATABASE_URL", importer_url)
    monkeypatch.setenv("FMP_API_KEY", KEY)
    caplog.set_level(logging.DEBUG)
    assert cli_main(["summaries"]) != 0
    out = capsys.readouterr()
    report = json.loads(out.out)
    assert set(report) == {"error_code"} and re.fullmatch(
        r"[a-z0-9_]{1,64}", report["error_code"]
    )
    no_secret_or_company_text(
        out.out, out.err, *(r.getMessage() for r in caplog.records)
    )
    assert row(engine, "ZZAB") == before


@pytest.mark.db
@pytest.mark.parametrize(
    "key", [None, "your_fmp_key_here"], ids=["unset", "placeholder"]
)
def test_a_missing_key_fails_as_missing_credential_before_any_request(
    engine, importer_url, fmp, monkeypatch, capsys, caplog, key
):
    from src.ingestion.collectors import _fetcher

    seed_security(
        engine, "ZZAC", name="Synthetic ZZAC", profile={"sector": "Industrials"}
    )
    members(monkeypatch, "ZZAC")
    before = row(engine, "ZZAC")
    monkeypatch.setenv("DATABASE_URL", importer_url)
    if key is None:
        monkeypatch.delenv("FMP_API_KEY", raising=False)
    else:
        monkeypatch.setenv("FMP_API_KEY", key)
    monkeypatch.setattr(_fetcher("fmp"), "FMP_API_KEY", None)
    caplog.set_level(logging.DEBUG)
    assert cli_main(["summaries"]) != 0
    out = capsys.readouterr()
    shown = out.out + out.err + "".join(r.getMessage() for r in caplog.records)
    assert "missing_credential" in shown
    calls, _ = fmp
    assert calls == []
    assert row(engine, "ZZAC") == before


def test_it_uses_the_importer_database_url_not_an_owner_or_api_engine(fmp, monkeypatch):
    """DATABASE_URL is the importer identity (OPERATIONS.md); the subcommand must
    use it, with a CLI or pipeline workload, never migrate or api."""
    from src.ingestion import cli

    url = "postgresql+psycopg://importer_login:synthetic@127.0.0.1:1/perennial"
    monkeypatch.setenv("DATABASE_URL", url)
    monkeypatch.setenv("FMP_API_KEY", KEY)
    seen = []

    class Stop(Exception):
        pass

    def spy(given, *args, **kwargs):
        seen.append((given, kwargs.get("workload", "cli")))
        raise Stop

    monkeypatch.setattr(cli, "make_engine", spy)
    cli_main(["summaries"])
    assert len(seen) == 1
    given, workload = seen[0]
    assert str(given) == url and workload in ("cli", "pipeline")


# ── decision 76: manual only ─────────────────────────────────


def test_summaries_are_never_scheduled_or_part_of_the_daily_pipeline():
    backend = REPO / "development" / "backend" / "src"
    automated = [
        backend / "pipeline" / "runner.py",
        *sorted(
            (backend / "services" / "consensus_watchlist" / "scheduler").rglob("*.py")
        ),
        *sorted((REPO / ".github" / "workflows").glob("*.y*ml")),
    ]
    assert automated
    for path in automated:
        content = path.read_text()
        for marker in (
            "company_summaries",
            "summarize(",
            "fmp_descriptions",
            " summaries",
        ):
            assert marker not in content, (path.name, marker)
    for path in sorted((REPO / "development").rglob("*")):
        if path.is_file() and (
            path.name.startswith(("Dockerfile", "Procfile"))
            or path.suffix in (".service", ".timer", ".cron")
        ):
            assert "summaries" not in path.read_text(errors="ignore"), path
