"""Safety and real PostgreSQL checks for operational tooling."""
import os
from pathlib import Path
import subprocess
import sys

import pytest

OPS = Path(__file__).resolve().parents[2] / "database" / "ops"
sys.path.insert(0, str(OPS))


def test_ci_requires_database_url():
    env = dict(os.environ, REQUIRE_DATABASE_TESTS="1")
    env.pop("TEST_DATABASE_URL", None)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "tests/test_contracts.py"],
        cwd=Path(__file__).resolve().parents[1], env=env, capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert "REQUIRE_DATABASE_TESTS" in result.stderr + result.stdout


@pytest.mark.parametrize("url", [
    "postgresql://user@remote.example/perennial_test",
    "postgresql://user@127.0.0.1/production",
    "postgresql://user@127.0.0.1/perennial_test?hostaddr=8.8.8.8",
    "postgresql://user@127.0.0.1/perennial_test?service=prod",
    "postgresql://user@127.0.0.1/perennial_test?dbname=production",
])
def test_restore_rejects_unsafe_sources(url):
    from restore_drill import test_connection
    with pytest.raises(ValueError):
        test_connection(url)


def test_restore_pins_loopback_and_removes_libpq_defaults(monkeypatch):
    from restore_drill import test_connection, clean_environment
    monkeypatch.setenv("PGSERVICE", "prod")
    monkeypatch.setenv("PGHOSTADDR", "8.8.8.8")
    assert not any(k.startswith("PG") for k in clean_environment())
    params = test_connection("postgresql+psycopg://user@localhost/perennial_test")
    assert params["hostaddr"] == "127.0.0.1"


@pytest.mark.db
def test_restore_round_trip(engine, fixture_path, tmp_path):
    from restore_drill import run_drill
    from src.ingestion.importer import CollectionOutcome, import_collection
    import json
    payload = json.loads(fixture_path("ark_holdings.json").read_text())
    assert import_collection(engine, CollectionOutcome.success(
        "ark_holdings", "fixture", payload["holdings"])).succeeded
    report = run_drill(engine.url.render_as_string(hide_password=False), tmp_path / "drill")
    assert report["verified"] is True
    assert report["tables"]["ark_holdings"]["rows"] > 0
    assert report["target_database"].startswith("perennial_test_restore_")
    assert report["target_dropped"] is True
    assert (tmp_path / "drill" / "database.dump").is_file()


@pytest.mark.db
def test_roles_enforce_immutable_data_and_support_imports(engine, fixture_path):
    """Exercise SET ROLE, actual imports, reruns, and deny historical mutation."""
    from provision_roles import provision
    from sqlalchemy import event, text
    from sqlalchemy.exc import DBAPIError
    from src.db.session import make_engine
    from src.ingestion.importer import CollectionOutcome, import_collection
    import json
    import secrets

    suffix = secrets.token_hex(6)
    owner, importer, reader = [f"ops_test_{r}_{suffix}" for r in ("owner", "importer", "reader")]
    roles = (owner, importer, reader)
    with engine.begin() as conn:
        # Legacy broad grants, including column grants, must be removed on reapply.
        provision(conn.connection.driver_connection, roles, phase="bootstrap")
        provision(conn.connection.driver_connection, roles, phase="grants")
        conn.execute(text(f'GRANT UPDATE ON perennial.source_batches TO "{importer}"'))
        conn.execute(text(f'GRANT UPDATE (symbol) ON perennial.securities TO "{importer}"'))
        provision(conn.connection.driver_connection, roles, phase="grants")
    try:
        restricted = make_engine(engine.url, connect_args={"hostaddr": "127.0.0.1"})
        @event.listens_for(restricted, "connect")
        def set_role(dbapi, _):
            with dbapi.cursor() as cur:
                cur.execute(f'SET ROLE "{importer}"')
            dbapi.commit()
        payload = json.loads(fixture_path("ark_holdings.json").read_text())
        result = import_collection(restricted, CollectionOutcome.success(
            "ark_holdings", "fixture", payload["holdings"]))
        assert result.succeeded
        with restricted.begin() as conn:
            assert conn.execute(text("SELECT count(*) FROM perennial.ark_holdings")).scalar() > 0
            conn.execute(text("UPDATE perennial.securities SET exchange='NASDAQ' WHERE exchange IS NULL"))
        from test_pipeline import inputs, caps
        from src.pipeline.publication import publish, replay, current
        runs, batches = inputs(restricted, fixture_path)
        publication = publish(restricted, runs, caps(restricted, batches))
        assert replay(restricted, publication) == current(restricted)["output"]
        # A second publication exercises UPDATE of the current pointer.
        runs, batches = inputs(restricted, fixture_path)
        assert publish(restricted, runs, caps(restricted, batches)) != publication
        for statement in (
            "UPDATE perennial.source_batches SET record_count=0",
            "UPDATE perennial.securities SET symbol='BAD'",
            "DELETE FROM perennial.ark_holdings",
            "TRUNCATE perennial.source_batches CASCADE",
            "UPDATE perennial.publications SET output='{}'::jsonb",
        ):
            with restricted.begin() as conn, pytest.raises(DBAPIError):
                conn.execute(text(statement))
        restricted.dispose()
        with engine.begin() as conn:
            conn.execute(text(f'SET LOCAL ROLE "{reader}"'))
            assert conn.execute(text("SELECT count(*) FROM perennial.ark_holdings")).scalar() > 0
            with pytest.raises(DBAPIError):
                conn.execute(text("INSERT INTO perennial.securities(symbol) VALUES ('READER')"))
        with engine.connect() as conn:
            for table in ("run_dependencies", "publications", "publication_sources", "market_cap_observations", "publication_market_caps"):
                assert conn.execute(text("SELECT has_table_privilege(:role,:table,'INSERT')"),
                                    {"role": importer, "table": "perennial." + table}).scalar()
                assert not conn.execute(text("SELECT has_table_privilege(:role,:table,'UPDATE')"),
                                        {"role": importer, "table": "perennial." + table}).scalar()
            assert conn.execute(text("SELECT has_table_privilege(:role,'perennial.current_publication','UPDATE')"),
                                {"role": importer}).scalar()
        with engine.begin() as conn:
            conn.execute(text(f'SET LOCAL ROLE "{owner}"'))
            conn.execute(text("CREATE TABLE perennial.future_table (id bigint GENERATED ALWAYS AS IDENTITY)"))
        with engine.begin() as conn:
            for role in (importer, reader):
                assert not conn.execute(text("SELECT has_table_privilege(:role,'perennial.future_table','UPDATE')"),
                                        {"role": role}).scalar()
                assert not conn.execute(text("SELECT has_table_privilege(:role,'perennial.future_table','SELECT')"),
                                        {"role": role}).scalar()
            conn.execute(text("DROP TABLE perennial.future_table"))
    finally:
        # These unique roles and only their objects belong to this disposable fixture DB.
        with engine.begin() as conn:
            admin = conn.execute(text("SELECT current_user")).scalar()
            for role in roles:
                conn.execute(text(f'REASSIGN OWNED BY "{role}" TO "{admin}"'))
                conn.execute(text(f'DROP OWNED BY "{role}" CASCADE'))
                conn.execute(text(f'DROP ROLE "{role}"'))
