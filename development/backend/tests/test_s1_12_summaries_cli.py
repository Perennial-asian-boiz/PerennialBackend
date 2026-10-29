"""S1-12, decision 76 (option 1): a manual `summaries` subcommand, spec-first.

Bryan runs `python -m src.ingestion.cli summaries` by hand on staging. It runs
company_summaries.summarize(engine, adapter) as the importer login (the
application's DATABASE_URL, OPERATIONS.md), prints the CS-7 coverage counts
only, and exits non-zero on a failed run. It is never scheduled.

Amended 2026-10-28 (ED-8, decisions 89-90; packet 2d52f06): the adapter is
SecTenKDescriptions(EdgarClient(requests.Session(), settings.sec_user_agent),
seeded_ciks(engine)). FMP_API_KEY is neither needed nor read, and the FMP adapter
is unused. A missing or blank SEC_USER_AGENT, or one containing example.com (the
.env.example placeholder), fails as missing_credential before any request and
any write.

HTTP is faked at requests.Session.get with test_s1_12_edgar_adapter's FakeSec,
so the real adapter and EdgarClient run and no SEC call is ever made.
"""

import json
import logging
import re
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import requests
from sqlalchemy import text
from test_collectors import FakeResponse
from test_read_api import groups, login, seed_security  # noqa: F401  (groups: fixture)
from test_s1_12_edgar_adapter import MAP_URL, PROSE, FakeSec, company, doc_url, sub_url

UA = "Synthetic Research ops@perennial.test"
DESCRIPTION = " ".join(PROSE)
REPO = Path(__file__).resolve().parents[3]


@pytest.fixture
def sec(monkeypatch):
    """requests.Session.get answers SEC's endpoints from a FakeSec; returns it."""
    fake = FakeSec()
    monkeypatch.setattr(
        requests.Session, "get", lambda self, url, **kwargs: fake.get(url, **kwargs)
    )
    from src.ingestion import collectors

    monkeypatch.setattr(collectors, "MAX_HTTP_RETRIES", 0)
    monkeypatch.delenv("PERENNIAL_ENV", raising=False)  # never read a local .env
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    return fake


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


def source(engine, symbol):
    with engine.connect() as conn:
        return tuple(
            conn.execute(
                text(
                    "SELECT p.summary_source_name, p.summary_source_url"
                    " FROM perennial.company_profiles p JOIN perennial.securities s"
                    " ON s.company_profile_id = p.id WHERE s.symbol = :s"
                ),
                {"s": symbol},
            ).one()
        )


def cli_main(argv):
    from src.ingestion import cli

    return cli.main(argv)


def no_secret_or_company_text(*outputs):
    for output in outputs:
        assert UA not in output
        for sentence in PROSE:
            assert sentence.strip(". ") not in output, sentence
        assert "synthetic provider body" not in output
        assert "sec.gov/Archives" not in output


def sec_hosts_only(fake):
    assert fake.requests
    assert {urlsplit(url).hostname for url in fake.urls()} <= {"www.sec.gov", "data.sec.gov"}


# ── the subcommand ───────────────────────────────────────────


def test_the_summaries_subcommand_is_wired_like_the_others():
    from src.ingestion import cli

    parser = cli._parser()
    assert parser.parse_args(["summaries"]).command == "summaries"
    assert "summaries" in parser.format_help()


@pytest.mark.db
def test_a_successful_run_exits_0_and_prints_counts_only(
    engine, importer_url, sec, monkeypatch, capsys, caplog
):
    seed_security(
        engine, "ZZAA", name="Synthetic ZZAA", profile={"sector": "Industrials"}
    )
    members(monkeypatch, "ZZAA")
    company(sec, "ZZAA", 111)
    monkeypatch.setenv("DATABASE_URL", importer_url)
    monkeypatch.setenv("SEC_USER_AGENT", UA)
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
    assert row(engine, "ZZAA")["summary_status"] == "ok"  # by the importer login
    assert source(engine, "ZZAA") == ("SEC EDGAR", doc_url(111))
    sec_hosts_only(sec)  # no FMP request, and FMP_API_KEY is unset
    assert all(headers == {"User-Agent": UA} for _, _, headers, _ in sec.requests)


@pytest.mark.db
def test_a_seeded_cik_is_used_and_the_map_is_not_fetched(
    engine, importer_url, sec, monkeypatch, capsys
):
    seed_security(
        engine,
        "ZZAS",
        name="Synthetic ZZAS",
        profile={"sector": "Industrials", "cik": "0000000444"},
    )
    members(monkeypatch, "ZZAS")
    company(sec, "ZZAS", 444, mapped=False)
    monkeypatch.setenv("DATABASE_URL", importer_url)
    monkeypatch.setenv("SEC_USER_AGENT", UA)
    assert cli_main(["summaries"]) == 0
    assert MAP_URL not in sec.urls()
    assert sub_url(444) in sec.urls()
    assert row(engine, "ZZAS")["summary_status"] == "ok"


@pytest.mark.db
def test_a_failed_run_exits_nonzero_with_only_its_code_and_writes_nothing(
    engine, importer_url, sec, monkeypatch, capsys, caplog
):
    seed_security(
        engine, "ZZAB", name="Synthetic ZZAB", profile={"sector": "Industrials"}
    )
    members(monkeypatch, "ZZAB")
    company(sec, "ZZAB", 112)
    sec.subs[112] = FakeResponse(503, {"message": "synthetic provider body"})
    before = row(engine, "ZZAB")
    monkeypatch.setenv("DATABASE_URL", importer_url)
    monkeypatch.setenv("SEC_USER_AGENT", UA)
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
    "agent",
    [
        None,
        "",
        "   ",
        "Perennial research you@example.com",
        "Synthetic Team admin@example.com",
    ],
    ids=["unset", "empty", "blank", "env_example_placeholder", "example_domain"],
)
def test_a_missing_or_placeholder_user_agent_fails_before_any_request_or_write(
    engine, importer_url, sec, monkeypatch, capsys, caplog, agent
):
    seed_security(
        engine, "ZZAC", name="Synthetic ZZAC", profile={"sector": "Industrials"}
    )
    members(monkeypatch, "ZZAC")
    company(sec, "ZZAC", 113)
    before = row(engine, "ZZAC")
    monkeypatch.setenv("DATABASE_URL", importer_url)
    monkeypatch.setenv("FMP_API_KEY", "synthetic-fmp-key-0123456789")  # irrelevant now
    if agent is None:
        monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    else:
        monkeypatch.setenv("SEC_USER_AGENT", agent)
    caplog.set_level(logging.DEBUG)
    assert cli_main(["summaries"]) != 0
    out = capsys.readouterr()
    assert json.loads(out.out) == {"error_code": "missing_credential"}
    assert sec.requests == []
    assert row(engine, "ZZAC") == before


@pytest.mark.db
def test_no_fmp_key_is_needed(engine, importer_url, sec, monkeypatch, capsys):
    from src.ingestion.collectors import _fetcher

    seed_security(
        engine, "ZZAD", name="Synthetic ZZAD", profile={"sector": "Industrials"}
    )
    members(monkeypatch, "ZZAD")
    company(sec, "ZZAD", 114)
    monkeypatch.setenv("DATABASE_URL", importer_url)
    monkeypatch.setenv("SEC_USER_AGENT", UA)
    monkeypatch.setattr(_fetcher("fmp"), "FMP_API_KEY", None)
    assert cli_main(["summaries"]) == 0
    sec_hosts_only(sec)


def test_it_uses_the_importer_database_url_not_an_owner_or_api_engine(sec, monkeypatch):
    """DATABASE_URL is the importer identity (OPERATIONS.md); the subcommand must
    use it, with a CLI or pipeline workload, never migrate or api."""
    from src.ingestion import cli

    url = "postgresql+psycopg://importer_login:synthetic@127.0.0.1:1/perennial"
    monkeypatch.setenv("DATABASE_URL", url)
    monkeypatch.setenv("SEC_USER_AGENT", UA)
    monkeypatch.delenv("FMP_API_KEY", raising=False)
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
            "sec_descriptions",
            " summaries",
        ):
            assert marker not in content, (path.name, marker)
    for path in sorted((REPO / "development").rglob("*")):
        if path.is_file() and (
            path.name.startswith(("Dockerfile", "Procfile"))
            or path.suffix in (".service", ".timer", ".cron")
        ):
            assert "summaries" not in path.read_text(errors="ignore"), path
