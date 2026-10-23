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
    engine = pinned_engine(db_url.set(database=name))
    try:  # cleanup covers a failed bootstrap too
        create_demo_database(admin, name)
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
    from src.pipeline.demo import demo_marked

    pub = run_demo(demo_engine, DemoReplay())
    with demo_engine.connect() as conn:
        modes = set(conn.execute(select(models.ingestion_runs.c.collection_mode)).scalars())
        config = conn.execute(
            select(models.publications.c.ranking_config).where(models.publications.c.id == pub)
        ).scalar_one()
    assert modes == {"fixture"}
    assert demo_marked(demo_engine, pub) is True  # DM-4: marked in the audit event
    assert "demo" not in config  # ranking_config holds ranking inputs only


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
    engine = pinned_engine(db_url.set(database=name))
    try:
        with admin.connect() as conn:
            conn.execute(text(f'CREATE DATABASE "{name}"'))
            if marked:
                conn.execute(text(f"COMMENT ON DATABASE \"{name}\" IS '{DEMO_MARKER}'"))
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


# ── SEC-DEMO-02: effective connection target, pinned factory ──


class _Info:
    def __init__(self, host, hostaddr=""):
        self.host, self.hostaddr = host, hostaddr


@pytest.mark.parametrize(
    "info,local",
    [
        (_Info("127.0.0.1"), True),
        (_Info("localhost", "127.0.0.1"), True),
        (_Info("/var/run/postgresql"), True),
        (_Info("localhost", "192.0.2.1"), False),  # URL says localhost, routed away
        (_Info("db.example.com"), False),
        (_Info("db.example.com", "::1"), True),  # effective address is loopback
    ],
)
def test_guard_judges_the_effective_target_not_the_url(info, local):
    from src.pipeline.demo_guard import _target_is_local

    assert _target_is_local(info) is local


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+psycopg://u@localhost:5433/perennial_demo?hostaddr=192.0.2.1",
        "postgresql+psycopg://u@localhost:5433/perennial_demo?host=db.example.com",
        "postgresql+psycopg://u@localhost:5433/perennial_demo?service=prod",
        "postgresql+psycopg://u@db.example.com:5433/perennial_demo",
        "postgresql+psycopg://u@127.0.0.1:5433/perennial",
    ],
)
def test_pinned_factory_refuses_routing_options_and_non_demo_urls(url):
    from src.pipeline.demo_guard import local_engine

    with pytest.raises(DemoGuardError):
        local_engine(url)


@pytest.mark.db
def test_pinned_engine_ignores_ambient_libpq_routing(demo_engine, monkeypatch):
    from src.pipeline.demo_guard import local_engine

    monkeypatch.setenv("PGHOSTADDR", "192.0.2.1")  # would route away if honoured
    engine = local_engine(demo_engine.url.render_as_string(hide_password=False))
    try:
        with engine.connect() as conn:
            check_demo_database(conn)
            assert conn.connection.dbapi_connection.info.hostaddr == "127.0.0.1"
    finally:
        engine.dispose()


def test_bootstrap_checks_locality_before_any_statement():
    """A non-local admin connection is refused before its first statement."""
    executed = []

    class Conn:
        class connection:
            class dbapi_connection:
                info = _Info("localhost", "192.0.2.1")

        def execute(self, *a, **k):
            executed.append(a)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class Admin:
        def connect(self):
            return Conn()

    with pytest.raises(DemoGuardError):
        create_demo_database(Admin(), "perennial_demo_x")  # type: ignore[arg-type]
    assert executed == []


# ── SEC-DEMO-01: never adopt an existing database ──


@pytest.mark.db
@pytest.mark.parametrize("comment", [None, "other-team-database"])
def test_bootstrap_refuses_an_existing_unmarked_database_untouched(db_url, comment):
    from conftest import pinned_engine
    from src.pipeline.demo_guard import database_marker

    admin = pinned_engine(db_url, isolation_level="AUTOCOMMIT")
    name = f"perennial_demo_{secrets.token_hex(4)}"
    try:
        with admin.connect() as conn:
            conn.execute(text(f'CREATE DATABASE "{name}"'))
            if comment:
                conn.execute(text(f"COMMENT ON DATABASE \"{name}\" IS '{comment}'"))
        with pytest.raises(DemoGuardError):
            create_demo_database(admin, name)
        with admin.connect() as conn:
            assert database_marker(conn, name) == comment
        target = pinned_engine(db_url.set(database=name))
        try:
            with target.connect() as conn:  # not migrated either
                assert conn.execute(text("SELECT to_regclass('public.alembic_version')")).scalar() is None
        finally:
            target.dispose()
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


@pytest.mark.db
def test_bootstrap_losing_a_creation_race_does_not_adopt_the_winner(db_url):
    """Another session creates the database between our existence check and our
    CREATE; our CREATE fails and the unmarked winner is refused, untouched."""
    from sqlalchemy import event

    from conftest import pinned_engine
    from src.pipeline.demo_guard import database_marker

    admin = pinned_engine(db_url, isolation_level="AUTOCOMMIT")
    rival = pinned_engine(db_url, isolation_level="AUTOCOMMIT")
    name = f"perennial_demo_{secrets.token_hex(4)}"

    @event.listens_for(admin, "before_cursor_execute")
    def race(conn, cursor, statement, params, context, executemany):
        if statement.startswith("CREATE DATABASE"):
            with rival.connect() as other:
                other.execute(text(f'CREATE DATABASE "{name}"'))

    try:
        with pytest.raises(DemoGuardError):
            create_demo_database(admin, name)
        event.remove(admin, "before_cursor_execute", race)
        with admin.connect() as conn:
            assert database_marker(conn, name) is None
    finally:
        with rival.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()
        rival.dispose()


@pytest.mark.db
def test_bootstrap_reuses_an_already_marked_demo_database(demo_engine, db_url):
    from conftest import pinned_engine

    admin = pinned_engine(db_url, isolation_level="AUTOCOMMIT")
    try:
        create_demo_database(admin, demo_engine.url.database)  # idempotent
    finally:
        admin.dispose()
    with demo_engine.connect() as conn:
        check_demo_database(conn)


# ── SEC-DEMO-03: no dotenv or provider-key read on the demo path ──


def test_demo_replay_reads_no_dotenv_or_provider_key_in_a_fresh_process():
    backend = Path(__file__).resolve().parents[1]
    probe = """
import json, os, dotenv
calls = []
dotenv.load_dotenv = lambda *a, **k: calls.append("load_dotenv") or False
os.environ["FINNHUB_API_KEY"] = "SYNTHETIC_ENV_KEY"
os.environ["FMP_API_KEY"] = "SYNTHETIC_ENV_KEY"
from src.pipeline.demo import DemoReplay, replaying
with replaying(DemoReplay()):
    from src.ingestion import collectors
    key = collectors._fetcher("insider").FINNHUB_API_KEY
print(json.dumps({"calls": calls, "key_seen": key == "SYNTHETIC_ENV_KEY",
                  "env_restored": os.environ["FINNHUB_API_KEY"] == "SYNTHETIC_ENV_KEY"}))
"""
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=backend,
        capture_output=True,
        text=True,
        timeout=60,
        env={"PATH": "", "PYTHONPATH": str(backend)},
    )
    assert result.returncode == 0, result.stderr[-300:]
    import json

    report = json.loads(result.stdout.strip().splitlines()[-1])
    assert report == {"calls": [], "key_seen": False, "env_restored": True}
