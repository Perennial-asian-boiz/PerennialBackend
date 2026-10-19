"""Publication checks use synthetic batches and explicit policy boundaries."""

import json
from datetime import date, datetime, timezone

import pytest

from src.ingestion.importer import CollectionOutcome, import_collection
from src.pipeline.lifecycle import begin_run
from src.pipeline.publication import PublicationError, current, publish, replay
from src.settings import QualityThreshold
from test_pipeline import caps, inputs


def test_publication_failure_code_is_in_safe_diagnostic_vocabulary():
    from src.ingestion.diagnostics import render_summary, safe_code

    assert safe_code("publication_quality_failed") == "publication_quality_failed"
    assert render_summary("publication_quality_failed", None) == "publication quality checks failed"


def test_cli_exposes_manual_publish_override():
    from src.ingestion.cli import _parser

    args = _parser().parse_args(
        [
            "publish",
            "--override",
            "synthetic correction",
            "--actor",
            "synthetic-operator",
        ]
    )
    assert args.command == "publish"
    assert args.override == "synthetic correction"


@pytest.mark.parametrize(
    "new_count,blocked",
    [(2, False), (1, True), (16, False), (17, True)],
)
def test_row_count_boundary(new_count, blocked):
    from src.pipeline.publication import _quality_failures

    threshold = QualityThreshold(allow_equal_as_of=True, null_fields=("shares",))
    previous = [{"shares": 1}] * 4
    candidate = [{"shares": 1}] * new_count
    issues = _quality_failures(
        previous, candidate, date(2026, 10, 1), date(2026, 10, 1), threshold
    )
    assert ("row_count" in issues) is blocked


def test_null_share_margin_boundary():
    from src.pipeline.publication import _quality_failures

    previous = [{"shares": None}] + [{"shares": 1}] * 3
    candidate = [{"shares": None}] * 2 + [{"shares": 1}] * 2
    at_boundary = QualityThreshold(
        null_share_margin=0.25, allow_equal_as_of=True, null_fields=("shares",)
    )
    below_boundary = QualityThreshold(
        null_share_margin=0.24, allow_equal_as_of=True, null_fields=("shares",)
    )
    assert "null_share" not in _quality_failures(
        previous, candidate, date(2026, 10, 1), date(2026, 10, 1), at_boundary
    )
    assert "null_share" in _quality_failures(
        previous, candidate, date(2026, 10, 1), date(2026, 10, 1), below_boundary
    )


@pytest.mark.parametrize(
    "current_day,allow_equal,blocked",
    [
        (date(2026, 10, 2), False, False),
        (date(2026, 10, 1), False, True),
        (date(2026, 10, 1), True, False),
        (date(2026, 9, 30), True, True),
        (None, True, True),
    ],
)
def test_source_observation_date_boundary(current_day, allow_equal, blocked):
    from src.pipeline.publication import _quality_failures

    threshold = QualityThreshold(allow_equal_as_of=allow_equal, null_fields=("shares",))
    rows = [{"shares": 1}]
    issues = _quality_failures(rows, rows, date(2026, 10, 1), current_day, threshold)
    assert ("source_as_of" in issues) is blocked


@pytest.mark.db
def test_first_publication_records_no_baseline_and_override_is_audited(
    engine, fixture_path
):
    runs, batches = inputs(engine, fixture_path)
    first = publish(engine, runs, caps(engine, batches))
    first_publication = current(engine)
    assert first_publication["id"] == first
    assert all(
        record["status"] == "no_baseline"
        for record in first_publication["ranking_config"]["quality_audit"][
            "sources"
        ].values()
    )

    insider = json.loads(fixture_path("trades_insider.json").read_text())[
        "transactions"
    ][:1]
    run_id = begin_run(
        engine,
        "insider_trades",
        dependencies={s: batches[s] for s in ("congress_trades", "ark_holdings")},
    )
    outcome = CollectionOutcome.success(
        "insider_trades",
        "live",
        insider,
        source_as_of=datetime.now(timezone.utc).date(),
        coverage={"complete": True, "scope": "snapshot"},
    )
    result = import_collection(engine, outcome, run_id=run_id)
    assert result.succeeded
    replacement = {**runs, "insider_trades": result.run_id}
    observations = caps(engine, batches)
    with pytest.raises(PublicationError) as caught:
        publish(engine, replacement, observations)
    assert caught.value.code == "publication_quality_failed"
    assert current(engine)["id"] == first

    second = publish(
        engine,
        replacement,
        observations,
        override_reason="synthetic source correction",
        override_actor="synthetic-operator",
    )
    saved = current(engine)
    assert saved["id"] == second
    audit = saved["ranking_config"]["quality_audit"]["override"]
    assert audit == {
        "actor": "synthetic-operator",
        "reason": "synthetic source correction",
        "checks": ["insider_trades:row_count"],
    }
    assert replay(engine, first) == first_publication["output"]
    assert replay(engine, second) == saved["output"]


@pytest.mark.db
def test_fixture_imports_are_not_a_live_publication_baseline(engine, fixture_path):
    inputs(engine, fixture_path, mode="fixture")
    runs, batches = inputs(engine, fixture_path)
    publication_id = publish(engine, runs, caps(engine, batches))
    audit = current(engine)["ranking_config"]["quality_audit"]["sources"]
    assert current(engine)["id"] == publication_id
    assert all(item["status"] == "no_baseline" for item in audit.values())


@pytest.mark.db
def test_cli_publish_reports_quality_code_and_records_operator(
    engine, db_url, fixture_path, monkeypatch, capsys
):
    from src.ingestion import cli
    from src.pipeline import runner

    runs, batches = inputs(engine, fixture_path)
    first = publish(engine, runs, caps(engine, batches))
    insider = json.loads(fixture_path("trades_insider.json").read_text())[
        "transactions"
    ][:1]
    run_id = begin_run(
        engine,
        "insider_trades",
        dependencies={s: batches[s] for s in ("congress_trades", "ark_holdings")},
    )
    outcome = CollectionOutcome.success(
        "insider_trades",
        "live",
        insider,
        source_as_of=datetime.now(timezone.utc).date(),
        coverage={"complete": True, "scope": "snapshot"},
    )
    assert import_collection(engine, outcome, run_id=run_id).succeeded
    monkeypatch.setenv("DATABASE_URL", db_url.render_as_string(hide_password=False))
    monkeypatch.setenv("FMP_API_KEY", "synthetic-key")
    monkeypatch.setattr(
        runner, "collect_market_caps", lambda symbols, **kwargs: caps(engine, batches)
    )

    assert cli.main(["publish"]) == 1
    assert (
        json.loads(capsys.readouterr().out)["error_code"]
        == "publication_quality_failed"
    )
    assert current(engine)["id"] == first
    assert (
        cli.main(
            [
                "publish",
                "--override",
                "synthetic correction",
                "--actor",
                "synthetic-operator",
            ]
        )
        == 0
    )
    published = json.loads(capsys.readouterr().out)["publication_id"]
    assert current(engine)["id"] == published
    assert (
        current(engine)["ranking_config"]["quality_audit"]["override"]["actor"]
        == "synthetic-operator"
    )
