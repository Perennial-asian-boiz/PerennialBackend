"""provision_roles' table check (decision 85 (b): our team fixes #66's F1), spec-first.

56a85b4 relaxed the grants phase's check from "the perennial tables equal
TABLES" to "no unknown table". Grants run before `alembic upgrade head` then pass
silently, and after the upgrade the new tables (perennial.fundamentals, ...) have
no ACL, so the importer's job fails later with permission denied; it would also
hide a dropped table. Nothing is widened (fail closed).

Pinned here (ops chooses between the reviewers' two options):
- Behind head (alembic_version older than the code's head): grants either refuse
  with the fixed text, or succeed with a WARNING that says to rerun the grants
  after the upgrade. Never silently. If ops chooses to refuse, the two MO-2 tests
  in test_pr22_fix_edges.py (which grant with today's code at an older revision)
  come back to the test maker.
- At head, a listed table that is missing refuses with the fixed text.
- An unknown table still refuses (unchanged).
- The normal path (bootstrap, upgrade head, grants) leaves the schema equal to
  TABLES and gives every IMMUTABLE table exactly its ACL; rerunning grants after a
  behind-head grant and the upgrade gives the new tables their ACL.
Every database here is a scratch database; roles are uniquely named and dropped.

T1 (checker on ops's 7773f31, 2026-10-28): option 2 was built, so the chosen
behaviour is pinned below. Behind head, grants succeed, log BEHIND_HEAD_NOTICE and
grant the tables that exist. "Behind head" means exactly one alembic_version row
naming an ancestor of the code's head. An unknown revision, no version table, or
an empty or multi-row one, with a listed table missing, refuses with the fixed
text.
"""

import logging
import re
from contextlib import contextmanager

import pytest
from pr22_support import drop_roles, ops_module, provision_roles, role_names
from sqlalchemy import text
from test_pr22_fix import _script, _version
from test_pr22_restore import _database

REFUSAL = (
    "Application table policy differs from migrated schema; update explicit grants"
)


def _upgrade(scratch, revision):
    from alembic import command
    from conftest import alembic_config

    with scratch.connect() as conn:
        command.upgrade(alembic_config(conn), revision)
        conn.commit()


def _head():
    return _script().get_current_head()


def _behind_head():
    """The head's down_revision: the newest migration's tables are still missing."""
    return _script().get_revision(_head()).down_revision


def _tables(scratch):
    with scratch.connect() as conn:
        return {
            r[0]
            for r in conn.execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname = 'perennial'")
            )
        }


def _privileges(scratch, role, table):
    with scratch.connect() as conn:
        return {
            p
            for p in (
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "TRUNCATE",
                "REFERENCES",
                "TRIGGER",
            )
            if conn.execute(
                text("SELECT has_table_privilege(:r, :t, :p)"),
                {"r": role, "t": f"perennial.{table}", "p": p},
            ).scalar()
        }


class _Warnings(logging.Handler):
    """provision_roles' own warnings. Not caplog: alembic's env.py runs fileConfig
    during an upgrade, which removes the root logger's handlers (caplog's too)."""

    def __init__(self):
        super().__init__(logging.WARNING)
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


@contextmanager
def _provisioning_warnings():
    logger = logging.getLogger("provision_roles")
    handler, disabled = _Warnings(), logger.disabled
    logger.disabled = False  # fileConfig may disable existing loggers
    logger.addHandler(handler)
    try:
        yield handler.messages
    finally:
        logger.removeHandler(handler)
        logger.disabled = disabled


@pytest.fixture
def roles(engine):
    names = role_names("ps")
    try:
        yield names
    finally:
        drop_roles(engine, names.values())


def test_the_newest_migration_adds_listed_tables():
    """The behind-head cases need the head migration to create perennial tables."""
    from pathlib import Path

    source = Path(_script().get_revision(_head()).path).read_text()
    created = set(re.findall(r"create_table\(\s*[\"'](\w+)[\"']", source))
    assert created & set(ops_module("provision_roles").TABLES), created


@pytest.mark.db
def test_grants_behind_head_refuse_or_warn_to_rerun_after_the_upgrade(engine, roles):
    with _database(engine) as (_, scratch):
        provision_roles(scratch, roles, "bootstrap")
        _upgrade(scratch, _behind_head())
        missing = set(ops_module("provision_roles").TABLES) - _tables(scratch)
        assert missing  # the precondition: listed tables not created yet
        with _provisioning_warnings() as messages:
            try:
                provision_roles(scratch, roles, "grants")
            except ValueError as refused:
                assert str(refused) == REFUSAL
                return
        warnings = [m.lower() for m in messages]
        rerun = re.compile(r"\bre-?run\b[^.]*\bgrants?\b[^.]*\bafter\b[^.]*\bupgrad")
        assert any(rerun.search(m) for m in warnings), (
            "grants behind head must say to rerun the grants after the upgrade",
            warnings,
        )


@pytest.mark.db
def test_a_listed_table_missing_at_head_refuses(engine, roles):
    with _database(engine) as (_, scratch):
        provision_roles(scratch, roles, "bootstrap")
        _upgrade(scratch, "head")
        with scratch.begin() as conn:
            conn.execute(text("DROP TABLE perennial.news_coverage CASCADE"))
        with pytest.raises(ValueError) as refused:
            provision_roles(scratch, roles, "grants")
        assert str(refused.value) == REFUSAL


@pytest.mark.db
def test_an_unknown_table_still_refuses(engine, roles):
    with _database(engine) as (_, scratch):
        provision_roles(scratch, roles, "bootstrap")
        _upgrade(scratch, "head")
        with scratch.begin() as conn:
            conn.execute(text("CREATE TABLE perennial.zz_unlisted (id int)"))
        with pytest.raises(ValueError) as refused:
            provision_roles(scratch, roles, "grants")
        assert str(refused.value) == REFUSAL


def _assert_immutable_acls(scratch, roles, tables):
    for table in tables:
        assert _privileges(scratch, roles["importer"], table) == {"SELECT", "INSERT"}, (
            table
        )
        assert _privileges(scratch, roles["reader"], table) == {"SELECT"}, table


@pytest.mark.db
def test_the_normal_path_gives_every_immutable_table_its_acl(engine, roles):
    module = ops_module("provision_roles")
    with _database(engine) as (_, scratch):
        provision_roles(scratch, roles, "bootstrap")
        _upgrade(scratch, "head")
        with _provisioning_warnings() as messages:
            provision_roles(scratch, roles, "grants")
        with scratch.connect() as conn:
            assert _version(conn) == _head()
        assert _tables(scratch) == set(module.TABLES)
        _assert_immutable_acls(scratch, roles, module.IMMUTABLE)
        assert not [m for m in messages if "upgrad" in m.lower()], "no notice at head"


@pytest.mark.db
def test_rerunning_grants_after_the_upgrade_gives_the_new_tables_their_acl(
    engine, roles
):
    module = ops_module("provision_roles")
    with _database(engine) as (_, scratch):
        provision_roles(scratch, roles, "bootstrap")
        _upgrade(scratch, _behind_head())
        early = _tables(scratch)
        try:
            provision_roles(scratch, roles, "grants")
        except ValueError:
            pass  # the refusing option: nothing granted yet, which is fine here
        provision_roles(scratch, roles, "bootstrap")  # before every upgrade
        _upgrade(scratch, "head")
        provision_roles(scratch, roles, "grants")
        new = _tables(scratch) - early
        assert new
        _assert_immutable_acls(
            scratch, roles, [t for t in module.IMMUTABLE if t in new]
        )


# ── T1: the chosen behaviour (option 2) ──────────────────


@pytest.mark.db
def test_grants_behind_head_succeed_warn_and_grant_the_present_tables(engine, roles):
    module = ops_module("provision_roles")
    with _database(engine) as (_, scratch):
        provision_roles(scratch, roles, "bootstrap")
        _upgrade(scratch, _behind_head())
        present = _tables(scratch)
        assert set(module.TABLES) - present  # listed tables still missing
        with _provisioning_warnings() as messages:
            provision_roles(scratch, roles, "grants")  # must not raise
        assert module.BEHIND_HEAD_NOTICE in messages
        _assert_immutable_acls(
            scratch, roles, [t for t in module.IMMUTABLE if t in present]
        )
        assert _tables(scratch) == present  # nothing created


def _set_versions(scratch, *versions):
    with scratch.begin() as conn:
        conn.execute(text("DELETE FROM public.alembic_version"))
        for version in versions:
            conn.execute(
                text("INSERT INTO public.alembic_version (version_num) VALUES (:v)"),
                {"v": version},
            )


@contextmanager
def _behind_head_database(engine, roles):
    """A scratch database at the head's down_revision, bootstrapped."""
    with _database(engine) as (_, scratch):
        provision_roles(scratch, roles, "bootstrap")
        _upgrade(scratch, _behind_head())
        yield scratch


@pytest.mark.db
@pytest.mark.parametrize(
    "versions",
    [("zz99",), (), "two_rows"],
    ids=["unknown_revision", "empty_version_table", "two_version_rows"],
)
def test_a_version_that_is_not_a_single_ancestor_refuses(engine, roles, versions):
    with _behind_head_database(engine, roles) as scratch:
        if versions == "two_rows":
            with scratch.connect() as conn:
                current = _version(conn)
            versions = (current, _script().get_revision(current).down_revision)
        _set_versions(scratch, *versions)
        with pytest.raises(ValueError) as refused:
            provision_roles(scratch, roles, "grants")
        assert str(refused.value) == REFUSAL


@pytest.mark.db
def test_no_version_table_refuses(engine, roles):
    with _behind_head_database(engine, roles) as scratch:
        with scratch.begin() as conn:
            conn.execute(text("DROP TABLE public.alembic_version"))
        with pytest.raises(ValueError) as refused:
            provision_roles(scratch, roles, "grants")
        assert str(refused.value) == REFUSAL
