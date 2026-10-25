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

import logging
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
    from conftest import pinned_engine

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


def provisioned(target, names):
    """The documented order as the superuser: bootstrap, upgrade head, grants, and,
    where ops has it (#22), provision_api_role.py, which makes the owner own app."""
    provision(target, names, "bootstrap")
    migrate(target)
    provision(target, names, "grants")
    if (OPS / "provision_api_role.py").exists():
        import provision_api_role

        with target.begin() as conn:
            provision_api_role.provision(conn.connection.driver_connection, names["api"],
                                         owner=names["owner"], importer=names["importer"],
                                         reader=names["reader"])


def migrate(target, revision="head", *, owner=None, down=False):
    """`alembic upgrade` (or, with `down`, downgrade) to `revision`, as the superuser
    test login or, with `owner`, as the owner's session role."""
    from alembic import command

    from conftest import alembic_config

    with target.connect() as conn:
        if owner:
            conn.execute(text(f'SET ROLE "{owner}"'))
            conn.commit()  # a session role; Alembic opens its own transaction
        try:
            step = command.downgrade if down else command.upgrade
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

    from conftest import ALEMBIC_INI

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


@contextmanager
def supabase_style_admin(engine, target, names):
    """An administrative login shaped like Supabase's postgres: LOGIN, NOSUPERUSER,
    CREATEROLE and BYPASSRLS, owning the database. Added to `names`, so scratch()
    drops it last, after the database and the group roles."""
    from conftest import pinned_engine

    names["admin"] = f"rd_admin_{secrets.token_hex(5)}"
    password = f"synthetic-{secrets.token_hex(8)}"  # hex only, safe to inline in DDL
    with engine.begin() as conn:  # the superuser test login
        conn.execute(text(f'CREATE ROLE "{names["admin"]}" LOGIN NOSUPERUSER CREATEROLE'
                          f" BYPASSRLS PASSWORD '{password}'"))
        conn.execute(text(f'ALTER DATABASE "{target.url.database}" OWNER TO "{names["admin"]}"'))
    admin = pinned_engine(target.url.set(username=names["admin"], password=password))
    try:
        yield admin
    finally:
        admin.dispose()


def test_mo1_a_non_superuser_createrole_admin_runs_the_documented_order(engine):
    """Check S3 (Supabase): on PostgreSQL 16+ a CREATEROLE admin gets only ADMIN
    OPTION on the roles it creates, not SET, so CREATE SCHEMA ... AUTHORIZATION and
    ALTER ... OWNER TO the owner need more. The same order, run by such an admin,
    reaches MO-1's end state, and the admin can SET ROLE to the owner without
    inheriting its privileges."""
    with scratch(engine) as (target, names), supabase_style_admin(engine, target, names) as admin:
        provision(admin, names, "bootstrap")
        migrate(target, owner=names["owner"])  # the migration login's session role
        provision(admin, names, "grants")
        assert version(target) == head()
        assert owners(target) == {names["owner"]}
        with target.connect() as conn:
            can_set, inherits = conn.execute(text(
                "SELECT pg_has_role(:a, :o, 'SET'), pg_has_role(:a, :o, 'USAGE')"),
                {"a": names["admin"], "o": names["owner"]}).one()
            assert (can_set, inherits) == (True, False)
            assert not conn.execute(text(
                "SELECT has_table_privilege(:a, 'perennial.securities', 'INSERT')"),
                {"a": names["admin"]}).scalar()


def admin_creates_roles(admin, names):
    """The group roles, created by the admin as a roles-only step would: it gets ADMIN
    OPTION on them (PostgreSQL 16+), and nothing more."""
    with admin.begin() as conn:
        for role in group_roles(names):
            conn.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))


def test_ro1_grants_as_the_admin_re_own_what_a_no_owner_restore_left_it(engine):
    """Checker RO-1, the restore path: after pg_restore --no-owner, run by the admin,
    the admin owns perennial, its tables and alembic_version (here the admin runs the
    migrations itself, which leaves the same state). Grants as that admin must end
    where MO-1 ends, with the admin still SET-only on the owner."""
    with scratch(engine) as (target, names), supabase_style_admin(engine, target, names) as admin:
        admin_creates_roles(admin, names)
        migrate(admin)
        assert owners(target) == {names["admin"]}  # the restored state
        provision(admin, names, "grants")
        assert owners(target) == {names["owner"]}
        with target.connect() as conn:
            assert conn.execute(text(
                "SELECT pg_has_role(:a, :o, 'SET'), pg_has_role(:a, :o, 'USAGE')"),
                {"a": names["admin"], "o": names["owner"]}).one() == (True, False)


# ── A shared platform database (auditor): PUBLIC's CONNECT ─


def _outsider_connects(engine, target, names):
    """A login outside our roles that relies on PUBLIC's CONNECT, as Supabase's
    service logins (authenticator, supabase_auth_admin, ...) do."""
    from sqlalchemy.exc import OperationalError

    from conftest import pinned_engine

    names["outsider"] = f"rd_outsider_{secrets.token_hex(5)}"
    password = f"synthetic-{secrets.token_hex(8)}"
    with engine.begin() as conn:
        conn.execute(text(f'CREATE ROLE "{names["outsider"]}" LOGIN PASSWORD \'{password}\''))
    outsider = pinned_engine(target.url.set(username=names["outsider"], password=password))
    try:
        with outsider.connect() as conn:
            return conn.execute(text("SELECT 1")).scalar() == 1
    except OperationalError:
        return False
    finally:
        outsider.dispose()


def _public_privileges(target):
    with target.connect() as conn:
        return conn.execute(text(
            "SELECT has_database_privilege('public', current_database(), 'CONNECT'),"
            " has_database_privilege('public', current_database(), 'TEMPORARY')")).one()


def _notices(caplog):
    return [r.getMessage() for r in caplog.records
            if r.levelno >= logging.INFO and "public" in r.getMessage().lower()]


def _group_roles_connect(target, names):
    """Our group roles' CONNECT comes from explicit grants, never only from PUBLIC."""
    with target.connect() as conn:
        explicit = set(conn.execute(text(
            "SELECT pg_get_userbyid(a.grantee) FROM pg_database d, aclexplode(d.datacl) a"
            " WHERE d.datname = current_database() AND a.privilege_type = 'CONNECT'"
            " AND a.grantee <> 0")).scalars())
    return set(group_roles(names)) <= explicit


def test_bootstrap_keeps_publics_connect_on_a_shared_platform_database(engine, caplog):
    """With a schema named auth (Supabase's postgres database), bootstrap skips the
    PUBLIC revoke on CONNECT and TEMPORARY, and says so in a log record."""
    with scratch(engine) as (target, names):
        with target.begin() as conn:
            conn.execute(text("CREATE SCHEMA auth"))
        caplog.set_level(logging.DEBUG)
        caplog.clear()
        provision(target, names, "bootstrap")
        assert tuple(_public_privileges(target)) == (True, True)
        assert _outsider_connects(engine, target, names)
        assert _notices(caplog), "no log record says PUBLIC's revoke was skipped"
        assert _group_roles_connect(target, names)


def test_bootstrap_still_revokes_publics_connect_on_a_dedicated_database(engine, caplog):
    with scratch(engine) as (target, names):
        caplog.set_level(logging.DEBUG)
        caplog.clear()
        provision(target, names, "bootstrap")
        assert tuple(_public_privileges(target)) == (False, False)
        assert not _outsider_connects(engine, target, names)
        assert _notices(caplog) == []
        assert _group_roles_connect(target, names)


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
        provisioned(target, names)
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
        provisioned(target, names)
        migrate(target, "base", owner=names["owner"], down=True)
        assert "perennial" not in schemas(target)
        provision(target, names, "bootstrap")
        migrate(target, owner=names["owner"])
        assert version(target) == head()


def test_mo8_without_bootstrap_the_owners_upgrade_fails_and_changes_nothing(engine):
    from sqlalchemy.exc import DBAPIError

    with scratch(engine) as (target, names):
        provisioned(target, names)
        migrate(target, "base", owner=names["owner"], down=True)
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
