"""Structured logging contract; all error payloads are synthetic."""

import asyncio
import io
import json
import logging

import pytest
from src.utils import logger as log


@pytest.fixture
def capture():
    assert hasattr(log, "JsonFormatter"), "JSON logging formatter is missing"
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(log.JsonFormatter())
    target = logging.getLogger("test.structured")
    target.addHandler(handler)
    target.setLevel(logging.INFO)
    target.propagate = False
    yield target, stream
    target.removeHandler(handler)
    target.propagate = True


def test_json_shape_and_exception_redaction(capture):
    target, stream = capture
    secret = "SYNTHETIC_LOG_SECRET"
    url = "https://provider.invalid/path?apikey=" + secret
    try:
        raise RuntimeError(url)
    except RuntimeError:
        target.exception("collection_failed")
    row = json.loads(stream.getvalue())
    assert row["level"] == "ERROR"
    assert row["logger"] == "test.structured"
    assert row["message"] == "collection_failed"
    assert row["exception"] == "RuntimeError"
    assert row["timestamp"].endswith("+00:00")
    assert all(row[key] is None for key in ("run_id", "source", "publication_id"))
    assert secret not in stream.getvalue() and url not in stream.getvalue()


def test_context_is_nested_restored_and_task_local(capture):
    target, stream = capture

    async def worker(run_id):
        with log.log_context(run_id=run_id, source="ark_holdings"):
            await asyncio.sleep(0)
            with log.log_context(publication_id=17):
                target.info("published")
            target.info("collected")

    async def run():
        await asyncio.gather(worker(1), worker(2))

    asyncio.run(run())
    target.info("outside")
    rows = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [(r["run_id"], r["publication_id"]) for r in rows] == [
        (1, 17),
        (1, None),
        (2, 17),
        (2, None),
        (None, None),
    ]
    assert rows[-1]["source"] is None


def test_exception_helper_does_not_put_secrets_in_records(caplog):
    assert hasattr(log, "log_exception"), "safe exception helper is missing"
    target = logging.getLogger("test.safe_error")
    log.log_exception(
        target, "collection_failed", ValueError("https://invalid/?apikey=SENTINEL_KEY")
    )
    assert "ValueError" in caplog.text
    assert "SENTINEL_KEY" not in caplog.text
    assert "https://invalid" not in caplog.text


def test_configuration_is_idempotent_and_writes_json_to_stderr(capsys):
    assert hasattr(log, "configure_logging"), "process logging configuration is missing"
    log.configure_logging()
    log.configure_logging()
    logging.getLogger("test.configured").warning("one_event")
    out = capsys.readouterr()
    rows = [json.loads(line) for line in out.err.splitlines()]
    assert len(rows) == 1 and rows[0]["message"] == "one_event"
    assert out.out == ""


def test_cli_error_is_json_and_stdout_stays_empty(monkeypatch, capsys):
    from src.ingestion import cli

    def broken():
        raise RuntimeError("https://invalid/?apikey=CLI_SENTINEL")

    monkeypatch.setattr(cli, "load_settings", broken)
    assert cli.main(["current"]) == 2
    out = capsys.readouterr()
    assert out.out == ""
    row = json.loads(out.err)
    assert row["level"] == "ERROR"
    assert "RuntimeError" in row["message"]
    assert "CLI_SENTINEL" not in out.err and "https://invalid" not in out.err


def test_emit_keeps_cli_json_on_stdout(capsys):
    from src.ingestion import cli

    assert hasattr(cli, "emit"), "single CLI output helper is missing"
    cli.emit({"publication_id": 42})
    out = capsys.readouterr()
    assert json.loads(out.out) == {"publication_id": 42}
    assert out.err == ""


def test_scheduler_configures_json_at_startup(monkeypatch):
    from src.pipeline import scheduler

    assert hasattr(scheduler, "configure_logging"), "scheduler JSON wiring is missing"
    called = []
    monkeypatch.setattr(
        scheduler, "configure_logging", lambda: called.append("configured")
    )

    def stop():
        assert called == ["configured"]
        raise RuntimeError("synthetic stop before scheduling")

    monkeypatch.setattr(scheduler, "load_settings", stop)
    with pytest.raises(RuntimeError, match="synthetic stop"):
        scheduler.main()


def test_context_restores_after_exception(capture):
    target, stream = capture
    with log.log_context(source="ark_holdings"):
        with pytest.raises(ValueError):
            with log.log_context(source="insider_trades", run_id=5):
                raise ValueError("synthetic")
        target.info("outer")
    row = json.loads(stream.getvalue())
    assert row["source"] == "ark_holdings" and row["run_id"] is None


def test_formatter_redacts_text_and_ignores_request_extras(capture):
    target, stream = capture
    target.warning("token=FAKE_TOKEN", extra={"params": {"apikey": "FAKE_PARAM"}})
    row = json.loads(stream.getvalue())
    assert row["message"] == "token=***"
    assert "FAKE_TOKEN" not in stream.getvalue()
    assert "FAKE_PARAM" not in stream.getvalue()


def test_cli_diagnostic_carries_source_and_clears_context(monkeypatch, capsys):
    from src.ingestion import cli

    def broken():
        raise RuntimeError("synthetic")

    monkeypatch.setattr(cli, "load_settings", broken)
    assert cli.main(["latest", "ark_holdings"]) == 2
    row = json.loads(capsys.readouterr().err)
    assert row["source"] == "ark_holdings"
    assert cli.main(["current"]) == 2
    assert json.loads(capsys.readouterr().err)["source"] is None


def test_formatter_strips_whole_urls_from_message_args_and_context(capture):
    target, stream = capture
    url = "https://provider.invalid/v1/SYNTHETIC_PATH_SECRET/rows?apikey=SYNTHETIC_QUERY_SECRET"
    with log.log_context(source=f"fetch {url}", run_id=7):
        target.warning(f"request {url}")
        target.warning("request %s failed", url)
    rows = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [row["message"] for row in rows] == [
        "request <url>",
        "request <url> failed",
    ]
    assert all(row["source"] == "fetch <url>" and row["run_id"] == 7 for row in rows)
    text = stream.getvalue()
    for leaked in (
        "SYNTHETIC_PATH_SECRET",
        "SYNTHETIC_QUERY_SECRET",
        "provider.invalid",
    ):
        assert leaked not in text


@pytest.mark.parametrize(
    "job, patch, message",
    [
        (
            "pipeline",
            ("src.settings", "load_settings"),
            "pipeline failed: RuntimeError",
        ),
        ("gdelt", ("src.db.session", "make_engine"), "gdelt run failed: RuntimeError"),
    ],
)
def test_scheduler_once_logs_json_through_the_safe_formatter(
    monkeypatch, capsys, job, patch, message
):
    # MAIN-1: `python -m src.pipeline.scheduler --once` is the scheduled
    # production entry point; it must not fall back to plain-text logging.
    import importlib
    import runpy
    import sys
    import warnings

    def broken(*args, **kwargs):
        raise RuntimeError(
            "https://provider.invalid/SYNTHETIC_PATH_SECRET?apikey=SYNTHETIC_QUERY_SECRET"
        )

    module, name = patch
    monkeypatch.setattr(importlib.import_module(module), name, broken)
    monkeypatch.setattr(sys, "argv", ["scheduler", "--once", "--job", job])
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    root.handlers = []  # as in a fresh process: no host-installed handlers
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)  # module already imported
            with pytest.raises(SystemExit) as exit_:
                runpy.run_module("src.pipeline.scheduler", run_name="__main__")
    finally:
        root.handlers = handlers
        root.setLevel(level)
    assert exit_.value.code == 1
    err = capsys.readouterr().err
    rows = [json.loads(line) for line in err.splitlines()]
    assert rows[-1]["level"] == "ERROR" and rows[-1]["message"] == message
    assert "SYNTHETIC" not in err and "provider.invalid" not in err
