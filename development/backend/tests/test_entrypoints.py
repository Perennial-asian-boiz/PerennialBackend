"""CLI and Alembic entry points, and regression for the existing JSON pipeline."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from src.db import config
from src.ingestion import cli
from src.ingestion.importer import CollectionOutcome

SECRET = "SYNTHETIC_SECRET_7f3a"
BACKEND = Path(__file__).resolve().parents[1]
DATABASE_DIR = BACKEND.parent / "database"
UNREACHABLE = f"postgresql+psycopg://perennial:{SECRET}@127.0.0.1:1/perennial_test"


@pytest.fixture
def no_env_file(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "ENV_PATH", tmp_path / "absent.env")
    monkeypatch.delenv("DATABASE_URL", raising=False)


def test_cli_missing_database_url(no_env_file, capsys):
    assert cli.main(["latest", "ark_holdings"]) == 2
    assert "DATABASE_URL is not set" in capsys.readouterr().err


def test_cli_connection_failure_is_credential_free(no_env_file, monkeypatch, capsys, fixture_path):
    monkeypatch.setenv("DATABASE_URL", UNREACHABLE)
    code = cli.main(["import-file", "ark_holdings", str(fixture_path("ark_holdings.json")), "--fixture"])
    out = capsys.readouterr()
    assert code == 2 and "OperationalError" in out.err
    assert SECRET not in out.out + out.err


def test_alembic_connection_failure_is_credential_free(no_env_file):
    env = dict(os.environ, DATABASE_URL=UNREACHABLE)
    proc = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=DATABASE_DIR, env=env, capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode != 0
    assert "could not connect to the database" in proc.stderr
    assert SECRET not in proc.stdout + proc.stderr


@pytest.mark.db
def test_cli_import_fetch_and_latest(engine, db_url, no_env_file, monkeypatch, capsys, fixture_path):
    monkeypatch.setenv("DATABASE_URL", db_url.render_as_string(hide_password=False))
    path = str(fixture_path("short_interest.json"))

    assert cli.main(["import-file", "short_interest", path]) == 1  # legacy file needs attestation
    assert json.loads(capsys.readouterr().out)["error_code"] == "collection_unattested"

    assert cli.main(["import-file", "short_interest", path, "--fixture"]) == 0
    first = json.loads(capsys.readouterr().out)
    assert cli.main(["import-file", "short_interest", path, "--attest-complete"]) == 0
    assert json.loads(capsys.readouterr().out)["reused_batch"] is True

    from src.ingestion import collectors

    monkeypatch.setitem(collectors.COLLECTORS, "short_interest", lambda: CollectionOutcome.failure(
        "short_interest", "live", "collection_failed"))
    assert cli.main(["fetch", "short_interest"]) == 1
    capsys.readouterr()

    assert cli.main(["latest", "short_interest", "--limit", "1"]) == 0
    latest = json.loads(capsys.readouterr().out)
    assert latest["latest"]["batch_id"] == first["batch_id"] and latest["row_count"] == 2
    assert len(latest["rows"]) == 1


# ── existing pipeline regression ─────────────────────────

def test_scheduler_and_consensus_still_work_from_json(tmp_path, monkeypatch, fixture_path):
    from src.ingestion.collectors import _fetcher

    _fetcher("fmp")  # puts fetchers/ on sys.path the way cron.py does
    sys.path.insert(0, str(BACKEND / "src" / "services" / "consensus_watchlist"))
    sys.path.insert(0, str(BACKEND / "src" / "services" / "consensus_watchlist" / "scheduler"))
    import consensus
    import cron

    assert callable(cron.run_full_pipeline) and callable(cron.main)
    monkeypatch.setattr(consensus, "FMP_API_KEY", "")  # no network lookups
    monkeypatch.setattr(consensus, "resolve_data_dirs", lambda: [tmp_path])  # never touch local_data/
    caps = {"ZZAA": 2e11, "ZZB.B": 5e11, "ZZCC": 1e9, "ZZDD": 1e9, "ZZEE": 3e9}
    result = consensus.run(
        congress_file=fixture_path("trades_congress.json"),
        ark_file=fixture_path("ark_holdings.json"),
        insider_file=fixture_path("trades_insider.json"),
        short_interest_file=fixture_path("short_interest.json"),
        profile_cache=dict(caps),
    )
    written = json.loads((tmp_path / "consensus_watchlist.json").read_text())
    assert written["popular_stable"] == result["popular_stable"]
    tickers = {r["ticker"] for bucket in ("popular_stable", "affordable_growing") for r in written[bucket]}
    assert {"ZZAA", "ZZDD", "ZZEE"} <= tickers
