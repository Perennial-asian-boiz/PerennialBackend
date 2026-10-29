"""Startup configuration is validated without exposing provider credentials."""

import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import SecretStr


def test_settings_hide_secrets_and_supply_source_quality_defaults(monkeypatch):
    from src.settings import Settings

    monkeypatch.setenv(
        "DATABASE_URL", "postgresql+psycopg://user:synthetic-db-secret@localhost/test"
    )
    monkeypatch.setenv("FMP_API_KEY", "synthetic-provider-secret")
    settings = Settings(_env_file=None)

    assert isinstance(settings.database_url, SecretStr)
    assert isinstance(settings.fmp_api_key, SecretStr)
    assert "synthetic-db-secret" not in repr(settings)
    assert "synthetic-provider-secret" not in repr(settings)
    assert settings.fmp_max_pages == 1  # the free FMP plan serves only page 0
    assert settings.quality_thresholds["short_interest"].allow_equal_as_of
    assert settings.quality_thresholds["congress_trades"].allow_equal_as_of
    assert settings.quality_thresholds["insider_trades"].allow_equal_as_of


def test_missing_database_setting_names_field_without_value(monkeypatch):
    from src.settings import SettingsError, load_settings

    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(SettingsError, match="DATABASE_URL") as caught:
        load_settings(local_dev=False)
    assert "synthetic" not in str(caught.value)


def test_live_settings_require_provider_keys(monkeypatch):
    from src.settings import SettingsError, load_settings

    monkeypatch.setenv(
        "DATABASE_URL", "postgresql+psycopg://user:synthetic@localhost/test"
    )
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    settings = load_settings(local_dev=False)
    with pytest.raises(SettingsError, match="FMP_API_KEY"):
        settings.require_provider_keys("fmp")


def test_cli_validates_live_provider_key_before_database_access(monkeypatch, capsys):
    from src.ingestion import cli

    monkeypatch.setenv(
        "DATABASE_URL", "postgresql+psycopg://user:synthetic@localhost/test"
    )
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    monkeypatch.setattr(
        cli, "make_engine", lambda *args, **kwargs: pytest.fail("engine opened")
    )

    assert cli.main(["run-pipeline"]) == 2
    assert "FMP_API_KEY" in capsys.readouterr().err


def test_scheduler_validates_settings_before_database_access(monkeypatch):
    from src.pipeline import scheduler

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        scheduler, "make_engine", lambda *args, **kwargs: pytest.fail("engine opened")
    )
    with pytest.raises(scheduler.PipelineRunFailed):
        scheduler.run_full_pipeline()


def test_database_env_file_is_local_opt_in(monkeypatch, tmp_path):
    from src.db import config

    env_file = tmp_path / ".env"
    env_file.write_text(
        "DATABASE_URL=postgresql+psycopg://user:synthetic@localhost/test\n"
    )
    monkeypatch.setattr(config, "ENV_PATH", env_file)
    with pytest.raises(config.DatabaseConfigError):
        config.get_database_url(environ={})
    assert (
        config.get_database_url(environ={"PERENNIAL_ENV": "local"}).database == "test"
    )


def test_partial_quality_override_keeps_other_source_defaults(monkeypatch):
    from src.settings import load_settings

    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://user:synthetic@localhost/test")
    monkeypatch.setenv(
        "QUALITY_THRESHOLDS", '{"short_interest":{"max_count_ratio":5}}'
    )
    settings = load_settings(local_dev=False)
    assert settings.quality_thresholds["short_interest"].max_count_ratio == 5
    assert settings.quality_thresholds["short_interest"].allow_equal_as_of
    assert settings.quality_thresholds["ark_holdings"].max_count_ratio == 4


def test_consensus_import_does_not_load_env_file():
    backend = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import dotenv; dotenv.load_dotenv = lambda *a, **k: 1 / 0; "
            "import src.services.consensus_watchlist.consensus",
        ],
        cwd=backend,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_consensus_market_cap_receives_provider_key_explicitly():
    from src.services.consensus_watchlist import consensus

    class Session:
        def get(self, url, *, params, headers, timeout):
            assert params == {"symbol": "ZZAA"}
            assert headers == {"X-Finnhub-Token": "synthetic"}
            assert timeout == 10
            body = {"ticker": "ZZAA", "currency": "USD", "marketCapitalization": 42}
            return type("Response", (), {"status_code": 200, "json": lambda self: body})()

    got = consensus.fetch_market_cap(
        "ZZAA", {}, session=Session(), api_key="synthetic", sleep=lambda s: None
    )
    assert got == 42_000_000  # Finnhub reports millions


def test_source_policy_defaults_follow_d7():
    from datetime import timedelta

    from src.settings import Settings

    settings = Settings(database_url="postgresql+psycopg://u@127.0.0.1/x_test")
    days = {
        source: (p.max_reuse_age / timedelta(days=1), p.max_age / timedelta(days=1))
        for source, p in settings.source_policies.items()
    }
    assert days == {
        "congress_trades": (3, 3),
        "ark_holdings": (3, 3),
        "insider_trades": (3, 3),
        "short_interest": (15, 45),
    }
    assert all(p.required for p in settings.source_policies.values())
    assert settings.market_cap_max_reuse_age == timedelta(days=3)


def test_source_policy_overrides_merge_and_are_validated(monkeypatch):
    import pytest

    from src.settings import SettingsError, load_settings

    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u@127.0.0.1/x_test")
    monkeypatch.setenv(
        "SOURCE_POLICIES",
        '{"short_interest": {"max_reuse_age": "P10D", "max_age": "P45D"}}',
    )
    settings = load_settings(local_dev=False)
    assert settings.source_policies["short_interest"].max_reuse_age.days == 10
    assert settings.source_policies["congress_trades"].max_age.days == 3
    for bad in (
        '{"short_interest": {"max_reuse_age": "P50D", "max_age": "P45D"}}',
        '{"short_interest": {"max_reuse_age": "PT0S", "max_age": "P45D"}}',
        '{"insider_trades": {"required": false, "max_reuse_age": "P3D", "max_age": "P3D"}}',
        '{"gdelt": {"max_reuse_age": "P1D", "max_age": "P1D"}}',
    ):
        monkeypatch.setenv("SOURCE_POLICIES", bad)
        with pytest.raises(SettingsError) as caught:
            load_settings(local_dev=False)
        assert "SOURCE_POLICIES" in str(caught.value)


def test_trading_holidays_cover_this_year_and_next():
    """Fails once the NYSE list stops covering next year: extend it (M2 N3)."""
    from datetime import date

    from src.settings import TRADING_HOLIDAYS

    covered = {d.year for d in TRADING_HOLIDAYS}
    this_year = date.today().year
    assert {this_year, this_year + 1} <= covered, (
        "extend settings.TRADING_HOLIDAYS with the NYSE closures for "
        f"{sorted({this_year, this_year + 1} - covered)}"
    )
    # A covered year is complete enough to include its January closures (MLK Day at least).
    for year in covered:
        assert any(d.year == year and d.month == 1 for d in TRADING_HOLIDAYS)
