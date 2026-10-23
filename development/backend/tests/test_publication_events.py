"""Publication audit events, narrowed overrides, empty baselines and the fail-fast lock.

Synthetic fixtures only. Each test here fails on 12e4b1b, where the audit
lived in ranking_config and publishers queued on a blocking advisory lock.
"""

import json
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text, update
from sqlalchemy.exc import IntegrityError

from src.db import models
from src.ingestion.importer import CollectionOutcome, import_collection
from src.pipeline import publication
from src.pipeline.lifecycle import begin_run
from src.pipeline.publication import PublicationError, current, publish, replay
from test_pipeline import caps, inputs
from test_publication_quality import events

UPSTREAM = ("congress_trades", "ark_holdings")


def _records(fixture_path, name, key):
    return json.loads(fixture_path(name).read_text())[key]


def _import(engine, source, records, deps=None, source_as_of=None):
    run = begin_run(engine, source, "live", dependencies=deps or {})
    result = import_collection(
        engine,
        CollectionOutcome.success(
            source,
            "live",
            records,
            source_as_of=source_as_of,
            coverage={"complete": True, "scope": "snapshot"},
        ),
        run_id=run,
    )
    assert result.succeeded
    return result.run_id, result.batch_id


def _one_insider_row(engine, fixture_path, batches):
    """A live insider batch that trips row_count against the full fixture."""
    rows = _records(fixture_path, "trades_insider.json", "transactions")[:1]
    deps = {s: batches[s] for s in UPSTREAM}
    run_id, _ = _import(
        engine,
        "insider_trades",
        rows,
        deps,
        source_as_of=datetime.now(timezone.utc).date(),
    )
    return run_id


@pytest.mark.db
def test_ranking_config_holds_ranking_inputs_only(engine, fixture_path):
    runs, batches = inputs(engine, fixture_path)
    pub = publish(engine, runs, caps(engine, batches))
    saved = current(engine)
    assert set(saved["ranking_config"]) == {
        "market_cap_threshold",
        "source_as_of",
        "freshness_seconds",
    }
    [event] = events(engine, pub)
    assert event["kind"] == "quality_passed"
    assert event["checks"]["failed"] == [] and event["checks"]["overridden"] == []
    assert set(event["checks"]["sources"]) == set(models.SOURCES)
    assert replay(engine, pub) == saved["output"]


@pytest.mark.db
def test_legacy_ranking_config_with_quality_audit_replays_byte_identically(
    engine, fixture_path
):
    """Publications written before the migration keep their audit in ranking_config."""
    runs, batches = inputs(engine, fixture_path)
    pub = publish(engine, runs, caps(engine, batches))
    saved = current(engine)
    legacy = {
        **saved["ranking_config"],
        "quality_audit": {
            "sources": {s: {"status": "no_baseline"} for s in models.SOURCES},
            "override": {"actor": "a", "reason": "r", "checks": []},
        },
    }
    with engine.begin() as conn:
        conn.execute(
            update(models.publications)
            .where(models.publications.c.id == pub)
            .values(ranking_config=legacy)
        )
    assert json.dumps(replay(engine, pub), sort_keys=True) == json.dumps(
        saved["output"], sort_keys=True
    )


@pytest.mark.db
def test_publication_events_constraints(engine, fixture_path):
    runs, batches = inputs(engine, fixture_path)
    pub = publish(engine, runs, caps(engine, batches))
    now = datetime.now(timezone.utc)
    base = dict(publication_id=pub, checks={}, created_at=now)
    bad = [
        dict(base, kind="unknown"),
        dict(base, kind="quality_override", reason="r"),  # no actor
        dict(base, kind="rollback", actor="a"),  # no reason
        dict(base, kind="degraded", actor="  "),
        dict(base, kind="degraded", checks=[]),  # not an object
        dict(base, kind="quality_passed"),  # second quality verdict
        dict(base, kind="quality_override", actor="a", reason="r"),
    ]
    for values in bad:
        with engine.begin() as conn, pytest.raises(IntegrityError):
            conn.execute(models.publication_events.insert().values(**values))
    # One degraded marker per publication (0007); rollbacks may repeat.
    rollback = dict(base, kind="rollback", actor="synthetic-op", reason="synthetic rollback")
    with engine.begin() as conn:
        conn.execute(models.publication_events.insert().values(**base, kind="degraded"))
        for _ in range(2):
            conn.execute(models.publication_events.insert().values(**rollback))
    with engine.begin() as conn, pytest.raises(IntegrityError):
        conn.execute(models.publication_events.insert().values(**base, kind="degraded"))
    assert [e["kind"] for e in events(engine, pub)] == [
        "quality_passed",
        "degraded",
        "rollback",
        "rollback",
    ]


@pytest.mark.db
def test_override_skips_only_the_named_checks(engine, fixture_path):
    runs, batches = inputs(engine, fixture_path)
    first = publish(engine, runs, caps(engine, batches))
    replacement = {**runs, "insider_trades": _one_insider_row(engine, fixture_path, batches)}
    observations = caps(engine, batches)
    reason = dict(override_reason="synthetic correction", override_actor="synthetic-op")

    # A failure the override does not name still blocks.
    with pytest.raises(PublicationError) as caught:
        publish(engine, replacement, observations, **reason,
                override_checks=["short_interest:row_count"])
    assert caught.value.code == "publication_quality_failed"
    assert caught.value.checks == ("insider_trades:row_count",)
    # Naming an extra check that did not fail is refused, not silently ignored.
    with pytest.raises(PublicationError, match="did not fail"):
        publish(engine, replacement, observations, **reason,
                override_checks=["insider_trades:row_count", "insider_trades:null_share"])
    assert current(engine)["id"] == first

    second = publish(engine, replacement, observations, **reason,
                     override_checks=["insider_trades:row_count"])
    [event] = events(engine, second)
    assert event["kind"] == "quality_override"
    assert event["checks"]["overridden"] == ["insider_trades:row_count"]


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(override_reason="r", override_actor="a"),  # no checks named
        dict(override_reason="r", override_actor="a", override_checks=[]),
        dict(override_reason="r", override_actor="a", override_checks=["insider_trades:typo"]),
        dict(override_reason="r", override_actor="a", override_checks=["nope:row_count"]),
        dict(override_checks=["insider_trades:row_count"]),  # no reason
    ],
)
def test_override_arguments_are_validated_before_touching_the_database(kwargs):
    with pytest.raises(ValueError):
        publish(None, {}, {}, **kwargs)  # type: ignore[arg-type]


@pytest.mark.db
def test_override_never_skips_the_observation_date_regression_guard(engine, fixture_path):
    runs, batches = inputs(engine, fixture_path)
    first = publish(engine, runs, caps(engine, batches))
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    ark_run, ark_batch = _import(
        engine,
        "ark_holdings",
        _records(fixture_path, "ark_holdings.json", "holdings"),
        source_as_of=yesterday,
    )
    assert ark_batch != batches["ark_holdings"]
    deps = {"congress_trades": batches["congress_trades"], "ark_holdings": ark_batch}
    insider_run, _ = _import(
        engine, "insider_trades", _records(fixture_path, "trades_insider.json", "transactions"), deps
    )
    short_run, _ = _import(
        engine, "short_interest", _records(fixture_path, "short_interest.json", "records"), deps
    )
    candidate = {
        "congress_trades": runs["congress_trades"],
        "ark_holdings": ark_run,
        "insider_trades": insider_run,
        "short_interest": short_run,
    }
    observations = caps(engine, {**batches, "ark_holdings": ark_batch})
    with pytest.raises(PublicationError) as caught:
        publish(engine, candidate, observations)
    assert "ark_holdings:source_as_of" in caught.value.checks
    # The quality check is overridable; going backwards in time is not.
    with pytest.raises(PublicationError, match="regress observation date"):
        publish(
            engine,
            candidate,
            observations,
            override_reason="synthetic correction",
            override_actor="synthetic-op",
            override_checks=list(caught.value.checks),
        )
    assert current(engine)["id"] == first


@pytest.mark.db
def test_empty_baseline_records_no_baseline_instead_of_failing(engine, fixture_path):
    runs, batches = inputs(engine, fixture_path)
    deps = {s: batches[s] for s in UPSTREAM}
    today = datetime.now(timezone.utc).date()
    # The live baseline has zero rows.
    _import(engine, "insider_trades", [], deps, source_as_of=today - timedelta(days=1))
    rows = _records(fixture_path, "trades_insider.json", "transactions")
    insider_run, insider_batch = _import(engine, "insider_trades", rows, deps, today)
    candidate = {**runs, "insider_trades": insider_run}
    pub = publish(engine, candidate, caps(engine, batches))
    audit = events(engine, pub)[0]["checks"]["sources"]["insider_trades"]
    assert audit["status"] == "passed"
    assert audit["no_baseline_checks"] == ["row_count"]
    assert audit["failed_checks"] == []
    # Re-collecting the same content reuses the batch, so the empty batch stays
    # the baseline; it must keep passing rather than fail on every run.
    again_run, again_batch = _import(engine, "insider_trades", rows, deps, today)
    assert again_batch == insider_batch
    assert publish(engine, {**candidate, "insider_trades": again_run}, caps(engine, batches))


def test_failure_code_for_concurrent_publish_is_in_safe_vocabulary():
    from src.ingestion.diagnostics import render_summary, safe_code

    assert safe_code("publication_in_progress") == "publication_in_progress"
    assert render_summary("publication_in_progress", None).startswith(
        "another publication is in progress"
    )


@pytest.mark.db
def test_concurrent_publisher_fails_fast_while_another_holds_the_lock(
    engine, fixture_path, monkeypatch
):
    runs, batches = inputs(engine, fixture_path)
    observations = caps(engine, batches)
    entered, release = threading.Event(), threading.Event()
    original = publication._selected

    def held_selected(*args, **kwargs):
        entered.set()
        release.wait(10)
        return original(*args, **kwargs)

    monkeypatch.setattr(publication, "_selected", held_selected)
    outcome: dict[str, object] = {}

    def first_publisher():
        try:
            outcome["id"] = publish(engine, runs, observations)
        except Exception as exc:  # surfaced through the assertion below
            outcome["error"] = exc

    worker = threading.Thread(target=first_publisher)
    worker.start()
    try:
        assert entered.wait(10)
        monkeypatch.setattr(publication, "_selected", original)
        started = time.monotonic()
        with pytest.raises(PublicationError) as caught:
            publish(engine, runs, observations)
        assert caught.value.code == "publication_in_progress"
        assert time.monotonic() - started < 2
    finally:
        release.set()
        worker.join(15)
    assert "error" not in outcome
    assert current(engine)["id"] == outcome["id"]
    # Once the first publisher commits, the lock is free again.
    assert publish(engine, runs, observations) != outcome["id"]


@pytest.mark.db
def test_cli_publish_reports_publication_in_progress(
    engine, db_url, fixture_path, monkeypatch, capsys
):
    from src.ingestion import cli
    from src.pipeline import runner

    runs, batches = inputs(engine, fixture_path)
    monkeypatch.setenv("DATABASE_URL", db_url.render_as_string(hide_password=False))
    monkeypatch.setenv("FMP_API_KEY", "synthetic-key")
    monkeypatch.setattr(
        runner, "collect_market_caps", lambda symbols, **kwargs: caps(engine, batches)
    )
    with engine.begin() as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(:key)"),
                     {"key": publication.PUBLICATION_LOCK})
        assert cli.main(["publish"]) == 1
    assert json.loads(capsys.readouterr().out) == {"error_code": "publication_in_progress"}
    assert current(engine) is None


@pytest.mark.parametrize(
    "argv",
    [
        ["publish", "--override", "synthetic correction"],
        ["publish", "--override-check", "insider_trades:row_count"],
    ],
)
def test_cli_override_and_override_check_go_together(argv, capsys):
    from src.ingestion import cli

    with pytest.raises(SystemExit) as caught:
        cli.main(argv)
    assert caught.value.code == 2
    assert "--override-check" in capsys.readouterr().err


# ── SEC-M1-01: override audit text is refused unless safe to retain ──

SENTINELS = (
    "SYNTHETIC_ACTOR_SECRET",
    "SYNTHETIC_REASON_SECRET",
    "SYNTHETIC_BEARER",
    "SYNTHETIC_SECRET",
)


def _database_text(engine):
    """Every publication and event row rendered as text, for sentinel searches."""
    with engine.connect() as conn:
        return " ".join(
            conn.execute(text(f"SELECT coalesce(string_agg(t::text, ' '), '') FROM {table} t"))
            .scalar_one()
            for table in ("perennial.publications", "perennial.publication_events")
        )


VALID_ACTOR = "synthetic-op"
VALID_REASON = "synthetic correction after provider switch"


def _refused(actor, reason):
    from src.pipeline.publication import OverrideInputError

    with pytest.raises(OverrideInputError) as caught:
        publish(
            None,  # type: ignore[arg-type]  # refused before any connection
            {},
            {},
            override_reason=reason,
            override_actor=actor,
            override_checks=["insider_trades:row_count"],
        )
    assert caught.value.code == "override_input_rejected"
    message = str(caught.value)
    assert all(s not in message for s in SENTINELS)
    assert all(not text or text not in message for text in (reason, actor))


@pytest.mark.parametrize(
    "reason",
    [
        # URLs and header-style credentials
        "see https://example.invalid/data?apikey=SYNTHETIC_REASON_SECRET",
        "www.example.invalid/path",
        "synthetic correction\nAuthorization: Bearer SYNTHETIC_BEARER",
        "retry with Bearer SYNTHETIC_BEARER",
        # quoted JSON / Python dict keys, short values (the 794e878 bypass)
        'provider error: {"api_key": "SYNTHETIC_SECRET"}',
        "provider error: {'password': 'pw1'}",
        '"token":"t1"',
        "'secret' : s1",
        '"x_auth": "a1"',
        "`key`: k1",
        'key="k1"',
        "sig: s1",
        # credential vocabulary without any delimiter
        "api_key SYNTHETIC_SECRET",
        "apikey is SYNTHETIC_SECRET",
        "the access token was t1",
        "x_api_key t1",
        "password pw1",
        "pwd pw1",
        "client secret c1",
        "credentials c1",
        "session cookie c1",
        # assignment or dump syntax, short values
        "provider said apikey=SYNTHETIC_REASON_SECRET",
        "value = v1",
        "dump <v1>",
        "escaped \\u0041 text",
        # key-shaped runs
        "key 0123456789abcdef0123456789abcdef pasted",
        "ref abcdefghij0123456789xyz",
        # control and format characters, length
        "tab\tseparated",
        "escape \x1b[31m sequence",
        "zero\u200bwidth",
        "x" * 501,
        "",
    ],
)
def test_unsafe_override_reason_is_refused_with_a_valid_actor(reason):
    from src.pipeline.publication import _check_override_text

    _check_override_text(VALID_ACTOR, VALID_REASON)  # the actor alone is fine
    _refused(VALID_ACTOR, reason)


@pytest.mark.parametrize(
    "actor",
    [
        "operator password=SYNTHETIC_ACTOR_SECRET",
        "synthetic op",
        "-leading-dash",
        "op\nx",  # surrounding whitespace is stripped; an inner newline is not
        "op\"x\"",
        "x" * 201,
    ],
)
def test_unsafe_override_actor_is_refused_with_a_valid_reason(actor):
    from src.pipeline.publication import _check_override_text

    _check_override_text(VALID_ACTOR, VALID_REASON)  # the reason alone is fine
    _refused(actor, VALID_REASON)


@pytest.mark.parametrize(
    "actor,reason",
    [
        ("brybry.o_o", "Finnhub replaced SecuritiesDB on 2026-10-20; first batch differs"),
        ("ops@perennial", "insider_trades:row_count expected after provider switch (ticket 42)"),
        ("synthetic-op", "Short interest settlement moved; NASDAQ confirmed. Résumé ok."),
        ("synthetic-op", "The provider's first batch didn't match; row count is the key signal"),
    ],
)
def test_plain_override_text_is_accepted(actor, reason):
    from src.pipeline.publication import _check_override_text

    _check_override_text(actor, reason)


@pytest.mark.db
def test_publish_never_persists_quoted_json_credential_in_reason(engine, fixture_path):
    """The SEC-M1-01 recheck reproduction: valid actor, quoted JSON key in the reason."""
    from src.pipeline.publication import OverrideInputError

    runs, batches = inputs(engine, fixture_path)
    first = publish(engine, runs, caps(engine, batches))
    replacement = {**runs, "insider_trades": _one_insider_row(engine, fixture_path, batches)}
    with pytest.raises(OverrideInputError):
        publish(
            engine,
            replacement,
            caps(engine, batches),
            override_checks=["insider_trades:row_count"],
            override_actor=VALID_ACTOR,
            override_reason='provider error: {"api_key": "SYNTHETIC_SECRET"}',
        )
    assert current(engine)["id"] == first
    assert "SYNTHETIC_SECRET" not in _database_text(engine)


@pytest.mark.db
def test_publish_never_persists_credential_bearing_override_text(engine, fixture_path):
    """The auditor's SEC-M1-01 reproduction through the real publish path."""
    from src.pipeline.publication import OverrideInputError

    runs, batches = inputs(engine, fixture_path)
    first = publish(engine, runs, caps(engine, batches))
    replacement = {**runs, "insider_trades": _one_insider_row(engine, fixture_path, batches)}
    with pytest.raises(OverrideInputError):
        publish(
            engine,
            replacement,
            caps(engine, batches),
            override_checks=["insider_trades:row_count"],
            override_actor="operator password=SYNTHETIC_ACTOR_SECRET",
            override_reason="provider exception: https://example.invalid/data?apikey="
            "SYNTHETIC_REASON_SECRET\nAuthorization: Bearer SYNTHETIC_BEARER",
        )
    assert current(engine)["id"] == first
    assert len(events(engine, first)) == 1
    stored = _database_text(engine)
    assert all(s not in stored for s in SENTINELS)


@pytest.mark.db
def test_cli_rejects_credential_bearing_override_without_echo(
    engine, db_url, fixture_path, monkeypatch, capsys
):
    from src.ingestion import cli
    from src.pipeline import runner

    runs, batches = inputs(engine, fixture_path)
    first = publish(engine, runs, caps(engine, batches))
    _one_insider_row(engine, fixture_path, batches)
    monkeypatch.setenv("DATABASE_URL", db_url.render_as_string(hide_password=False))
    monkeypatch.setenv("FMP_API_KEY", "synthetic-key")
    # Fake provider: market caps come from the synthetic helper, never the network.
    monkeypatch.setattr(
        runner, "collect_market_caps", lambda symbols, **kwargs: caps(engine, batches)
    )
    code = cli.main(
        [
            "publish",
            "--override",
            "see https://example.invalid/x?apikey=SYNTHETIC_REASON_SECRET",
            "--actor",
            "operator password=SYNTHETIC_ACTOR_SECRET",
            "--override-check",
            "insider_trades:row_count",
        ]
    )
    out = capsys.readouterr()
    assert code == 2
    assert "override_input_rejected" in out.err
    assert all(s not in out.out + out.err for s in SENTINELS)
    assert current(engine)["id"] == first
    assert all(s not in _database_text(engine) for s in SENTINELS)
    code = cli.main(
        [
            "publish",
            "--override",
            'provider error: {"api_key": "SYNTHETIC_SECRET"}',
            "--actor",
            VALID_ACTOR,
            "--override-check",
            "insider_trades:row_count",
        ]
    )
    out = capsys.readouterr()
    assert code == 2 and "override_input_rejected" in out.err
    assert "SYNTHETIC_SECRET" not in out.out + out.err
    assert current(engine)["id"] == first
    assert "SYNTHETIC_SECRET" not in _database_text(engine)
