"""N1: cli run-pipeline reports a required-source abort as a fixed code."""

import json

from src.pipeline import runner
from src.pipeline.runner import RequiredSourceUnavailableError


def test_cli_run_pipeline_reports_required_source_unavailable(monkeypatch, capsys):
    from src.ingestion import cli

    def unavailable(engine, **kwargs):
        raise RequiredSourceUnavailableError(
            "ark_holdings", "https://provider.invalid/SYNTHETIC_SECRET"
        )

    # The engine is never connected; run_pipeline is replaced before any query.
    monkeypatch.setenv(
        "DATABASE_URL", "postgresql+psycopg://synthetic:synthetic@127.0.0.1:1/none"
    )
    monkeypatch.setenv("FMP_API_KEY", "synthetic-key")
    monkeypatch.setenv("FINNHUB_API_KEY", "synthetic-key")
    monkeypatch.setattr(runner, "run_pipeline", unavailable)
    assert cli.main(["run-pipeline"]) == 1
    out = capsys.readouterr()
    assert json.loads(out.out) == {
        "error_code": "required_source_unavailable",
        "source": "ark_holdings",
    }
    assert "SYNTHETIC_SECRET" not in out.out + out.err
