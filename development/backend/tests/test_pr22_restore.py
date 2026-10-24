"""#22 fix branch RB-13: backup and restore once schema app has FORCE RLS.

Spec: docs/work-items/m5-pr22-fix-packet.md, change 8 and RB-13 (PK-2, PK-7 to
PK-10; decisions 32 and 34). Every test names the login used in each step and
uses unique role names (MR-1). Synthetic rows only.
"""

import os
import secrets
import subprocess
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text

from pr22_support import (
    APP_TABLES,
    drop_roles,
    pr22_roles,  # noqa: F401  (pytest fixture)
    provision_api,
    provision_roles,
    role_names,
)
from test_backup import keypair, loaded  # noqa: F401  (pytest fixtures)
from test_read_api import login

pytestmark = pytest.mark.db

NOW = datetime.now(timezone.utc)


def _seed_profile(engine):
    with engine.begin() as conn:  # superuser: seed past RLS
        conn.execute(text("INSERT INTO app.profiles (id, email, full_name)"
                          " VALUES (:i, 'one@example.com', 'One')"), {"i": str(uuid4())})


def _url(engine, user, password):
    return engine.url.set(username=user, password=password).render_as_string(
        hide_password=False)


@contextmanager
def _login_url(engine, names, **kwargs):
    with login(engine, names, **kwargs) as (_, user, password):
        yield _url(engine, user, password), user


# ── backup.py's early check (PK-9) ───────────────────────


def test_an_owner_class_login_is_refused_before_pg_dump(engine, pr22_roles, keypair, tmp_path):  # noqa: F811
    """The migration (owner) login is NOBYPASSRLS: refused with a fixed error, no dump."""
    from backup import LocalStore, run_backup

    _seed_profile(engine)
    with _login_url(engine, pr22_roles, members=("owner",)) as (url, user):
        store = LocalStore(tmp_path / "store")
        report = run_backup(url, keypair / "cert.pem", tmp_path / "spool", store)
    assert report["status"] == "failed" and report["error_code"] == "rls_bypass_required"
    assert store.names() == [] and list((tmp_path / "spool").iterdir()) == []
    assert user not in str(report)


def test_the_early_check_also_covers_rls_enabled_but_not_forced(engine, pr22_roles, keypair, tmp_path):  # noqa: F811
    """PK-9: relrowsecurity alone is enough to filter a non-owner without BYPASSRLS."""
    from backup import LocalStore, run_backup

    with engine.begin() as conn:
        for table in APP_TABLES:
            conn.execute(text(f"ALTER TABLE app.{table} NO FORCE ROW LEVEL SECURITY"))
    try:
        with engine.begin() as conn:
            reader_grant = f"rb_readall_{secrets.token_hex(4)}"
            conn.execute(text(f'CREATE ROLE "{reader_grant}" NOLOGIN'))
            conn.execute(text(f'GRANT pg_read_all_data TO "{reader_grant}"'))
        names = {**pr22_roles, "readall": reader_grant}
        with _login_url(engine, names, members=("readall",)) as (url, _):
            report = run_backup(url, keypair / "cert.pem", tmp_path / "spool",
                                LocalStore(tmp_path / "store"))
        assert report["error_code"] == "rls_bypass_required"
    finally:
        with engine.begin() as conn:
            for table in APP_TABLES:
                conn.execute(text(f"ALTER TABLE app.{table} FORCE ROW LEVEL SECURITY"))
        drop_roles(engine, [reader_grant])


def test_the_backup_login_dumps_every_app_row_and_the_drill_verifies_them(
    engine, pr22_roles, loaded, keypair, tmp_path  # noqa: F811
):
    """Decisions 32 and 34: BYPASSRLS + pg_read_all_data backs up; the admin login drills."""
    from backup import LocalStore, run_backup
    from restore_drill import run_backup_drill

    _seed_profile(engine)
    reader_grant = f"rb_readall_{secrets.token_hex(4)}"
    with engine.begin() as conn:
        conn.execute(text(f'CREATE ROLE "{reader_grant}" NOLOGIN'))
        conn.execute(text(f'GRANT pg_read_all_data TO "{reader_grant}"'))
    try:
        names = {**pr22_roles, "readall": reader_grant}
        store = LocalStore(tmp_path / "store")
        with _login_url(engine, names, options="BYPASSRLS", members=("readall",)) as (url, _):
            report = run_backup(url, keypair / "cert.pem", tmp_path / "spool", store)
        assert report["status"] == "ok", report
        # The drill restores and row-verifies as the administrative (superuser) login.
        drill = run_backup_drill(loaded, store, keypair / "key.pem", tmp_path / "drill")
        assert drill["verified"] is True
        app_rows = {k: v for k, v in drill["tables"].items() if k.startswith("app.")}
        assert set(app_rows) == {f"app.{t}" for t in APP_TABLES}
        assert app_rows["app.profiles"]["rows"] == 1
    finally:
        drop_roles(engine, [reader_grant])


def test_the_backup_login_cannot_restore(engine, pr22_roles, loaded, keypair, tmp_path):  # noqa: F811
    """PK-8: no DDL rights, so no disposable database and no objects."""
    from backup import LocalStore, run_backup
    from restore_drill import run_backup_drill

    reader_grant = f"rb_readall_{secrets.token_hex(4)}"
    with engine.begin() as conn:
        conn.execute(text(f'CREATE ROLE "{reader_grant}" NOLOGIN'))
        conn.execute(text(f'GRANT pg_read_all_data TO "{reader_grant}"'))
    try:
        names = {**pr22_roles, "readall": reader_grant}
        store = LocalStore(tmp_path / "store")
        with _login_url(engine, names, options="BYPASSRLS", members=("readall",)) as (url, _):
            assert run_backup(url, keypair / "cert.pem", tmp_path / "spool", store)["status"] == "ok"
            try:
                drill = run_backup_drill(url, store, keypair / "key.pem", tmp_path / "drill")
            except Exception:
                restored = False
            else:
                restored = drill.get("verified") is True
        assert not restored, "a restore as the backup login must not succeed"
    finally:
        drop_roles(engine, [reader_grant])


# ── The restore order: roles, restore, grants (PK-2, PK-10) ─


def _pg_env(engine):
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    env["PGPASSWORD"] = engine.url.password or ""
    return env


def _pg_args(engine, database):
    url = engine.url
    return ["-h", "127.0.0.1", "-p", str(url.port or 5432), "-U", url.username, "-d", database]


@contextmanager
def _database(engine, name=None):
    """A scratch database on the test server; yields (name, engine)."""
    from tests.conftest import pinned_engine

    name = name or f"perennial_test_rb_{secrets.token_hex(4)}"
    admin = pinned_engine(engine.url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    scratch = pinned_engine(engine.url.set(database=name))
    try:
        yield name, scratch
    finally:
        scratch.dispose()
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


def _migrate(scratch):
    from alembic import command

    from tests.conftest import alembic_config

    with scratch.connect() as conn:
        command.upgrade(alembic_config(conn), "head")
        conn.commit()


@pytest.fixture
def dumped(engine, tmp_path):
    """A provisioned, populated source database dumped as pg_dump -Fc --no-acl --no-owner.

    Its API role is then dropped from the cluster, so a restore target lacks it.
    Yields (dump path, role names).
    """
    names = role_names("rr")
    dump = tmp_path / "source.dump"
    try:  # roles are cluster-wide: dropped even when setup fails (MR-1)
        with _database(engine) as (source, scratch):
            try:
                _migrate(scratch)
                provision_roles(scratch, names)
                provision_api(scratch, names)
                _seed_profile(scratch)
                subprocess.run(["pg_dump", "-Fc", "--no-acl", "--no-owner", "-f", str(dump),
                                *_pg_args(engine, source)], env=_pg_env(engine), check=True,
                               capture_output=True)
            finally:
                with scratch.begin() as conn:  # objects in the source go before the roles
                    admin = conn.execute(text("SELECT current_user")).scalar()
                    for role in names.values():
                        if conn.execute(text("SELECT 1 FROM pg_roles WHERE rolname = :r"),
                                        {"r": role}).scalar():
                            conn.execute(text(f'REASSIGN OWNED BY "{role}" TO "{admin}"'))
                            conn.execute(text(f'DROP OWNED BY "{role}" CASCADE'))
        drop_roles(engine, names.values())
        yield dump, names
    finally:
        drop_roles(engine, names.values())


def _restore(engine, database, dump):
    return subprocess.run(["pg_restore", "--exit-on-error", "--no-owner", "-d", database,
                           *_pg_args(engine, database)[:-2], str(dump)],
                          env=_pg_env(engine), capture_output=True, text=True)


def test_a_restore_needs_the_roles_first(engine, dumped):
    dump, names = dumped
    with _database(engine) as (target, _):
        failed = _restore(engine, target, dump)
    assert failed.returncode != 0 and names["api"] in failed.stderr  # role does not exist
    with _database(engine) as (target, scratch):
        provision_roles(scratch, names, "roles")
        restored = _restore(engine, target, dump)
        assert restored.returncode == 0, restored.stderr[-2000:]


def test_the_roles_phase_creates_roles_and_nothing_else(engine, dumped):
    """PK-10: four group roles; no perennial or app schema; database grants unchanged."""
    _, names = dumped
    with _database(engine) as (target, scratch):
        with scratch.connect() as conn:
            before = conn.execute(text(
                "SELECT datacl::text FROM pg_database WHERE datname = current_database()")).scalar()
        provision_roles(scratch, names, "roles")
        with scratch.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM pg_roles WHERE rolname = ANY(:n)"),
                                {"n": list(names.values())}).scalar() == 4
            assert conn.execute(text(
                "SELECT count(*) FROM pg_namespace WHERE nspname IN ('perennial', 'app')")).scalar() == 0
            assert conn.execute(text(
                "SELECT datacl::text FROM pg_database WHERE datname = current_database()")).scalar() == before


def test_bootstrap_before_a_restore_fails_on_the_existing_schema(engine, dumped):
    """Why step 1 uses the roles phase: the dump creates schema perennial itself."""
    dump, names = dumped
    with _database(engine) as (target, scratch):
        provision_roles(scratch, names, "bootstrap")
        result = _restore(engine, target, dump)
    assert result.returncode != 0
    assert 'schema "perennial" already exists' in result.stderr


@pytest.mark.parametrize("unsafe", ["api_bypassrls", "group_member_of_another"])
def test_the_roles_phase_keeps_bootstraps_refusals(engine, unsafe):
    names = role_names("rp")
    extra = f"rp_extra_{secrets.token_hex(4)}"
    try:
        with engine.begin() as conn:
            if unsafe == "api_bypassrls":
                conn.execute(text(f'CREATE ROLE "{names["api"]}" NOLOGIN BYPASSRLS'))
            else:
                conn.execute(text(f'CREATE ROLE "{extra}" NOLOGIN'))
                conn.execute(text(f'CREATE ROLE "{names["reader"]}" NOLOGIN IN ROLE "{extra}"'))
        errors = []
        for phase in ("roles", "bootstrap"):
            with pytest.raises(ValueError) as caught:
                provision_roles(engine, names, phase)
            errors.append(str(caught.value))
        assert errors[0] == errors[1]
        with engine.connect() as conn:  # nothing created by the refused runs
            created = conn.execute(text("SELECT count(*) FROM pg_roles WHERE rolname = ANY(:n)"),
                                   {"n": [names["owner"], names["importer"]]}).scalar()
        assert created == 0
    finally:
        drop_roles(engine, [*names.values(), extra])


def test_after_the_restore_grants_and_provision_api_role_re_own_and_re_grant(engine, dumped):
    dump, names = dumped
    with _database(engine) as (target, scratch):
        provision_roles(scratch, names, "roles")
        assert _restore(engine, target, dump).returncode == 0
        with scratch.connect() as conn:  # before: owned by the restorer
            admin = conn.execute(text("SELECT current_user")).scalar()
            assert conn.execute(text(
                "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'app'")).scalar() == admin
        provision_roles(scratch, names, "grants")
        provision_api(scratch, names)
        with scratch.connect() as conn:
            owners = set(conn.execute(text(
                "SELECT pg_get_userbyid(c.relowner) FROM pg_class c"
                " WHERE c.relnamespace IN ('perennial'::regnamespace, 'app'::regnamespace)"
                " AND c.relkind IN ('r', 'S')")).scalars())
            assert owners == {names["owner"]}
            for schema in ("perennial", "app"):
                assert conn.execute(text(
                    "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = :s"),
                    {"s": schema}).scalar() == names["owner"]
            assert conn.execute(text(
                "SELECT pg_get_userbyid(proowner) FROM pg_proc"
                " WHERE oid = 'app.email_is_registered(text)'::regprocedure")).scalar() == admin
            roles = set(conn.execute(text(
                "SELECT DISTINCT unnest(roles) FROM pg_policies WHERE schemaname = 'app'")).scalars())
            assert roles == {names["api"]}
            for table in APP_TABLES:
                assert conn.execute(text("SELECT has_table_privilege(:r, :t, 'SELECT')"),
                                    {"r": names["api"], "t": f"app.{table}"}).scalar()
        with scratch.begin() as conn:  # leave nothing that pins the roles in this database
            for role in names.values():
                conn.execute(text(f'REASSIGN OWNED BY "{role}" TO "{admin}"'))
                conn.execute(text(f'DROP OWNED BY "{role}" CASCADE'))


def test_the_runbook_documents_the_three_logins_and_the_restore_order():
    """Change 8: OPERATIONS.md splits the migration, backup and restore logins."""
    doc = (Path(__file__).resolve().parents[3] / "docs" / "technical-specs" / "OPERATIONS.md").read_text()
    for term in ("BYPASSRLS", "pg_read_all_data", "--phase roles", "provision_api_role.py",
                 "Backup login", "Restore"):
        assert term in doc, term
    assert doc.index("--phase roles") < doc.rindex("pg_restore")  # roles before the restore
