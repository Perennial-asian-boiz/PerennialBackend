"""Owner migrations (decision 37, option 2): MO-1 to MO-9, the parts that apply on main.

Spec: docs/work-items/m5-pr22-auth-hardening-packet.md @ 3df8817, Part 2, "Draft
acceptance criteria (MO)"; Bryan picked option 2. The documented release path is
`provision_roles.py --phase bootstrap` as the admin, then `alembic upgrade head` as
the owner (its session role, with no CREATE on the database), then `--phase grants`.
Bootstrap pre-creates the schemas, and a migration creates a schema only when
to_regnamespace() finds none.

Each test runs in its own scratch database with uniquely named group roles, and
drops both. The roles are cluster-wide, so they are dropped in `finally`, even when
setup fails. The same file works with main's three group roles and #22's four.
"""

import re
import secrets
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.db

ROOT = Path(__file__).resolve().parents[3]
OPS = ROOT / "development" / "database" / "ops"
MIGRATIONS = ROOT / "development" / "database" / "migrations" / "versions"


def provision_roles():
    if str(OPS) not in sys.path:
        sys.path.insert(0, str(OPS))
    import provision_roles as module

    return module


def group_roles(names):
    """(owner, importer, reader), plus the API role once provision_roles defines one."""
    roles = (names["owner"], names["importer"], names["reader"])
    return roles + (names["api"],) if hasattr(provision_roles(), "API_TABLES") else roles


@contextmanager
def scratch(engine):
    """A fresh database and four unique role names; both are dropped afterwards."""
    from tests.conftest import pinned_engine

    suffix = secrets.token_hex(5)
    names = {r: f"rd_{r}_{suffix}" for r in ("owner", "importer", "reader", "api")}
    database = f"perennial_test_rd_{suffix}"
    admin = pinned_engine(engine.url, isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            conn.execute(text(f'CREATE DATABASE "{database}"'))
        target = pinned_engine(engine.url.set(database=database))
        try:
            yield target, names
        finally:
            target.dispose()
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'))
            for role in names.values():  # nothing of theirs survives the database
                conn.execute(text(f'DROP ROLE IF EXISTS "{role}"'))
        admin.dispose()


def provision(target, names, phase):
    with target.begin() as conn:
        provision_roles().provision(conn.connection.driver_connection, group_roles(names),
                                    phase=phase)


def migrate(target, revision="head", *, owner=None):
    """`alembic upgrade` (or downgrade, for a revision below the current one), as the
    superuser test login or, with `owner`, as the owner's session role."""
    from alembic import command

    from tests.conftest import alembic_config

    with target.connect() as conn:
        if owner:
            conn.execute(text(f'SET ROLE "{owner}"'))
            conn.commit()  # a session role; Alembic opens its own transaction
        try:
            step = command.downgrade if revision == "base" else command.upgrade
            step(alembic_config(conn), revision)
        finally:
            conn.rollback()
            if owner:
                conn.execute(text("RESET ROLE"))
                conn.commit()


def version(target):
    with target.connect() as conn:
        if not conn.execute(text("SELECT to_regclass('public.alembic_version')")).scalar():
            return None
        return conn.execute(text("SELECT version_num FROM public.alembic_version")).scalar()


def head():
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    from tests.conftest import ALEMBIC_INI

    return ScriptDirectory.from_config(Config(str(ALEMBIC_INI))).get_current_head()


def schemas(target):
    with target.connect() as conn:
        return set(conn.execute(text(
            "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg\\_%'"
            " AND nspname NOT IN ('information_schema', 'public')")).scalars())


def owners(target, schema="perennial"):
    """{owner} of the schema, its tables and its sequences, and of alembic_version."""
    with target.connect() as conn:
        found = set(conn.execute(text(
            "SELECT pg_get_userbyid(c.relowner) FROM pg_class c"
            " WHERE c.relnamespace = to_regnamespace(:s) AND c.relkind IN ('r', 'p', 'S')"),
            {"s": schema}).scalars())
        found.add(conn.execute(text(
            "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = :s"),
            {"s": schema}).scalar())
        found.add(conn.execute(text(
            "SELECT pg_get_userbyid(relowner) FROM pg_class"
            " WHERE oid = 'public.alembic_version'::regclass")).scalar())
    return found


# ── MO-1: a fresh database, in the documented order ──────


def test_mo1_a_fresh_database_migrates_as_the_owner_in_the_documented_order(engine):
    """Bootstrap as the admin, upgrade head as the owner, then grants."""
    with scratch(engine) as (target, names):
        provision(target, names, "bootstrap")
        migrate(target, owner=names["owner"])
        provision(target, names, "grants")
        assert version(target) == head()
        assert owners(target) == {names["owner"]}


# ── MO-3: the owner's rights don't change ────────────────


def test_mo3_the_owner_holds_no_create_on_the_database_and_nothing_else_new(engine):
    with scratch(engine) as (target, names):
        provision(target, names, "bootstrap")
        migrate(target)  # as the superuser: MO-1 is pinned on its own
        provision(target, names, "grants")
        with target.connect() as conn:
            assert conn.execute(text(
                "SELECT has_database_privilege(:o, current_database(), 'CREATE')"),
                {"o": names["owner"]}).scalar() is False
            login, bypass = conn.execute(text(
                "SELECT rolcanlogin, rolbypassrls FROM pg_roles WHERE rolname = :o"),
                {"o": names["owner"]}).one()
            assert (login, bypass) == (False, False)
            assert conn.execute(text(
                "SELECT count(*) FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member"
                " WHERE r.rolname = :o"), {"o": names["owner"]}).scalar() == 0


# ── MO-4 (main's part): re-running bootstrap changes nothing ─


ACL_SNAPSHOT = """
SELECT 'database', datname, datacl::text FROM pg_database WHERE datname = current_database()
UNION ALL SELECT 'schema', nspname, nspowner::regrole::text || ' ' || coalesce(nspacl::text, '')
  FROM pg_namespace WHERE nspname NOT LIKE 'pg\\_%' AND nspname <> 'information_schema'
UNION ALL SELECT 'relation', c.oid::regclass::text, c.relowner::regrole::text || ' '
  || coalesce(c.relacl::text, '') FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname NOT LIKE 'pg\\_%' AND n.nspname <> 'information_schema'
UNION ALL SELECT 'column', a.attrelid::regclass::text || '.' || a.attname, a.attacl::text
  FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE a.attacl IS NOT NULL AND n.nspname NOT LIKE 'pg\\_%'
UNION ALL SELECT 'default', defaclrole::regrole::text || ' ' || defaclnamespace::text || ' '
  || defaclobjtype::text, defaclacl::text FROM pg_default_acl
ORDER BY 1, 2
"""


def test_mo4_rerunning_bootstrap_on_a_provisioned_database_changes_nothing(engine):
    """Bootstrap now runs before every upgrade, so on a provisioned database it must
    leave the grants, default privileges and owners exactly as they were."""
    with scratch(engine) as (target, names):
        provision(target, names, "bootstrap")
        migrate(target)
        provision(target, names, "grants")
        with target.connect() as conn:
            before = conn.execute(text(ACL_SNAPSHOT)).all()
        provision(target, names, "bootstrap")
        with target.connect() as conn:
            assert conn.execute(text(ACL_SNAPSHOT)).all() == before


# ── MO-5: the guard ──────────────────────────────────────


def _migration_sources():
    return {path.name: path.read_text() for path in sorted(MIGRATIONS.glob("*.py"))}


def test_mo5_no_migration_uses_create_schema_if_not_exists():
    """PostgreSQL checks CREATE on the database before it checks IF NOT EXISTS."""
    found = [name for name, source in _migration_sources().items()
             if re.search(r"CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS", source, re.IGNORECASE)]
    assert found == [], found


def test_mo5_every_create_schema_is_behind_a_catalog_check():
    """Each CREATE SCHEMA sits inside a to_regnamespace(...) check, in the same
    statement or within the three lines before it (a DO block or a Python branch)."""
    unguarded = []
    for name, source in _migration_sources().items():
        lines = source.splitlines()
        for number, line in enumerate(lines):
            if re.search(r"CREATE\s+SCHEMA", line, re.IGNORECASE):
                window = "\n".join(lines[max(0, number - 3):number + 1])
                if "to_regnamespace" not in window:
                    unguarded.append(f"{name}:{number + 1}")
    assert unguarded == [], unguarded


def test_mo5_every_schema_a_migration_creates_is_in_bootstraps_list(engine):
    """Measured, not scanned: the schemas `upgrade head` creates in an empty database
    (no bootstrap) are all among those bootstrap creates in another empty one."""
    with scratch(engine) as (migrated, _):
        migrate(migrated)
        created_by_migrations = schemas(migrated)
    with scratch(engine) as (bootstrapped, names):
        provision(bootstrapped, names, "bootstrap")
        created_by_bootstrap = schemas(bootstrapped)
    assert created_by_migrations and created_by_migrations <= created_by_bootstrap, (
        created_by_migrations, created_by_bootstrap)


# ── MO-6: without bootstrap (tests, dev Compose, the quality gate) ─


def test_mo6_without_bootstrap_upgrade_head_still_creates_the_schemas(engine):
    """As the superuser test login, as the tests and the quality gate migrate. The
    gate's round trip and drift check are test_quality_gate.py's."""
    with scratch(engine) as (target, _):
        migrate(target)
        assert version(target) == head()
        assert "perennial" in schemas(target)


# ── MO-8 (main's form): downgrade, then upgrade, as the owner ─


def test_mo8_after_a_downgrade_bootstrap_lets_the_owner_upgrade_again(engine):
    """As the owner: downgrade to base (0001 drops perennial), bootstrap, then
    upgrade head. Main's form of MO-8, whose #22 form downgrades to 0010."""
    with scratch(engine) as (target, names):
        provision(target, names, "bootstrap")
        migrate(target)
        provision(target, names, "grants")
        migrate(target, "base", owner=names["owner"])
        assert "perennial" not in schemas(target)
        provision(target, names, "bootstrap")
        migrate(target, owner=names["owner"])
        assert version(target) == head()


def test_mo8_without_bootstrap_the_owners_upgrade_fails_and_changes_nothing(engine):
    from sqlalchemy.exc import DBAPIError

    with scratch(engine) as (target, names):
        provision(target, names, "bootstrap")
        migrate(target)
        provision(target, names, "grants")
        migrate(target, "base", owner=names["owner"])
        with pytest.raises(DBAPIError) as caught:
            migrate(target, owner=names["owner"])
        assert caught.value.orig.sqlstate == "42501"  # permission denied for the database
        assert version(target) is None and "perennial" not in schemas(target)


# ── MO-9: docs ───────────────────────────────────────────


def test_mo9_operations_md_runs_bootstrap_before_every_upgrade():
    doc = (ROOT / "docs" / "technical-specs" / "OPERATIONS.md").read_text()
    assert re.search(r"bootstrap[^\n]*before every upgrade|before every upgrade[^\n]*bootstrap",
                     doc, re.IGNORECASE), "OPERATIONS.md: bootstrap before every upgrade"
    assert re.search(r"no `?CREATE`? on the database", doc, re.IGNORECASE), \
        "OPERATIONS.md: the owner deliberately holds no CREATE on the database"
    assert re.search(r"add (it|the schema|that schema) to bootstrap", doc, re.IGNORECASE), \
        "OPERATIONS.md: a migration that adds a schema adds it to bootstrap"


def test_mo9_provision_roles_docstring_says_bootstrap_before_every_upgrade():
    assert re.search(r"before every upgrade", provision_roles().__doc__ or "", re.IGNORECASE)
