"""Demo mode: the full walkthrough in a marked demo DB, and the isolation guard.

Each test that needs a demo database creates its own perennial_demo_<hex> on the
loopback test server and drops it afterwards. Nothing contacts a provider.
"""

import secrets
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import select, text

from src.db import models
from src.pipeline.demo import DemoReplay, create_demo_database, run_demo, walkthrough
from src.pipeline.demo_guard import DEMO_MARKER, DemoGuardError, check_demo_database
from src.pipeline.publication import PublicationError, publish


@pytest.fixture
def demo_engine(db_url):
    from conftest import pinned_engine

    admin = pinned_engine(db_url, isolation_level="AUTOCOMMIT")
    name = f"perennial_demo_{secrets.token_hex(4)}"
    create_demo_database(admin, name)
    engine = pinned_engine(db_url.set(database=name))
    try:
        yield engine
    finally:
        engine.dispose()
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


@pytest.mark.db
def test_walkthrough_runs_every_step_in_the_demo_database(demo_engine):
    steps = {s["step"]: s for s in walkthrough(demo_engine)}
    assert steps["publish"]["events"] == ["quality_passed"]
    assert steps["current"]["demo_marker"] is True and steps["current"]["tickers"] > 0
    assert steps["replay"]["byte_identical"] is True
    assert "degraded" in steps["source outage (ARK)"]["events"]
    assert steps["quality refusal"] == {
        "step": "quality refusal",
        "refused": True,
        "error_code": "publication_quality_failed",
        "checks": ["insider_trades:row_count"],
    }
    assert steps["audited override"]["events"] == ["quality_override"]
    assert "rollback" in steps  # performed once PR #32's rollback API is merged
    assert "health" in steps


@pytest.mark.db
def test_demo_runs_are_recorded_as_fixture_mode_and_marked(demo_engine):
    pub = run_demo(demo_engine, DemoReplay())
    with demo_engine.connect() as conn:
        modes = set(conn.execute(select(models.ingestion_runs.c.collection_mode)).scalars())
        config = conn.execute(
            select(models.publications.c.ranking_config).where(models.publications.c.id == pub)
        ).scalar_one()
    assert modes == {"fixture"}
    assert config["demo"] is True


@pytest.mark.db
def test_fixture_runs_still_cannot_publish_without_the_demo_flag(demo_engine):
    """The safety rule holds even inside the demo database."""
    from src.db.queries import latest_successful_run

    run_demo(demo_engine, DemoReplay())
    with demo_engine.connect() as conn:
        runs = {s: latest_successful_run(conn, s).run_id for s in models.SOURCES}
    with pytest.raises(PublicationError, match="live collection mode"):
        publish(demo_engine, runs, {})


@pytest.mark.db
def test_demo_cannot_run_or_publish_in_a_non_demo_database(engine):
    """The regular test database fails the guard before anything is written."""
    from src.pipeline.runner import run_pipeline

    with engine.connect() as conn, pytest.raises(DemoGuardError):
        check_demo_database(conn)
    with pytest.raises(DemoGuardError):
        run_pipeline(engine, demo=True)
    with pytest.raises(DemoGuardError):
        publish(engine, {}, {}, demo=True)
    with engine.connect() as conn:
        assert conn.execute(select(models.ingestion_runs)).all() == []
        assert conn.execute(select(models.publications)).all() == []


@pytest.mark.db
@pytest.mark.parametrize("named_demo,marked", [(True, False), (False, True)])
def test_guard_needs_both_the_demo_name_and_the_marker(db_url, named_demo, marked):
    from conftest import pinned_engine

    admin = pinned_engine(db_url, isolation_level="AUTOCOMMIT")
    name = ("perennial_demo_" if named_demo else "perennial_test_notdemo_") + secrets.token_hex(4)
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
        if marked:
            conn.execute(text(f"COMMENT ON DATABASE \"{name}\" IS '{DEMO_MARKER}'"))
    engine = pinned_engine(db_url.set(database=name))
    try:
        with engine.connect() as conn, pytest.raises(DemoGuardError):
            check_demo_database(conn)
    finally:
        engine.dispose()
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


def test_create_refuses_a_non_demo_name():
    with pytest.raises(ValueError, match="perennial_demo"):
        create_demo_database(None, "perennial")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+psycopg://u@db.example.com:5432/perennial_demo",
        "postgresql+psycopg://u@127.0.0.1:5433/perennial",
        "not a url",
    ],
)
def test_demo_script_refuses_remote_or_non_demo_urls(url):
    script = Path(__file__).resolve().parents[2] / "database" / "ops" / "demo.py"
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=60,
        env={"PATH": "", "DEMO_DATABASE_URL": url},
    )
    assert result.returncode == 2
    assert "error:" in result.stderr and "Traceback" not in result.stderr


def test_guard_refuses_a_non_loopback_url_before_querying():
    class Conn:
        class engine:
            class url:
                host = "db.example.com"

        def execute(self, *a, **k):
            raise AssertionError("must not query a non-loopback server")

    with pytest.raises(DemoGuardError):
        check_demo_database(Conn())  # type: ignore[arg-type]
