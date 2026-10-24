"""Owner migrations (decision 37, option 2), #22's half (B2): schema app.

Spec: docs/work-items/m5-pr22-auth-hardening-packet.md @ 3df8817, Part 2, MO-1 to
MO-9. test_owner_migrations.py (B1, identical on main) pins the perennial half and
the MO-3, MO-5 and MO-9 guards for both schemas. This file adds what only #22 has:
schema app, provision_api_role.py and migration 0011. MO-2 is in
test_pr22_fix_edges.py (the parked 0010 case, reworked); MO-7 is the 0009/0010
older-backup restore tests there.
"""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from pr22_support import provision_api
from test_owner_migrations import (
    ACL_SNAPSHOT,
    head,
    migrate,
    owners,
    provision,
    provisioned,
    schemas,
    scratch,
    supabase_style_admin,
    version,
)
from test_pr22_fix import EXPECTED_APP, _app_privileges

pytestmark = pytest.mark.db

POLICIES = ("SELECT tablename, policyname, roles::text, qual, with_check FROM pg_policies"
            " WHERE schemaname = 'app' ORDER BY 1, 2")


# ── MO-1 (#22's form) ────────────────────────────────────


def test_mo1_a_fresh_database_reaches_the_provisioned_app_as_the_owner(engine):
    """Bootstrap, upgrade head as the owner, grants, then provision_api_role.py:
    perennial and app owned by the owner, the API's app privileges as provisioned."""
    with scratch(engine) as (target, names):
        provision(target, names, "bootstrap")
        migrate(target, owner=names["owner"])
        provision(target, names, "grants")
        provision_api(target, names)
        assert version(target) == head()
        assert owners(target, "perennial") == {names["owner"]}
        assert owners(target, "app") == {names["owner"]}
        assert _app_privileges(target, names["api"]) == EXPECTED_APP


def test_mo1_s3_a_non_superuser_createrole_admin_provisions_app(engine):
    """Check S3, #22's half: the Supabase-style admin also runs provision_api_role.py.
    It re-owns app to the owner and, as option (a)'s definer, takes
    app.email_is_registered itself, ending with SET but not INHERIT on the owner."""
    with scratch(engine) as (target, names), supabase_style_admin(engine, target, names) as admin:
        provision(admin, names, "bootstrap")
        migrate(target, owner=names["owner"])  # the migration login's session role
        provision(admin, names, "grants")
        provision_api(admin, names)
        assert version(target) == head()
        assert owners(target, "app") == {names["owner"]}
        assert _app_privileges(target, names["api"]) == EXPECTED_APP
        with target.connect() as conn:
            assert conn.execute(text(
                "SELECT pg_get_userbyid(proowner) FROM pg_proc"
                " WHERE oid = 'app.email_is_registered(text)'::regprocedure")).scalar() == names["admin"]
            can_set, inherits = conn.execute(text(
                "SELECT pg_has_role(:a, :o, 'SET'), pg_has_role(:a, :o, 'USAGE')"),
                {"a": names["admin"], "o": names["owner"]}).one()
            assert (can_set, inherits) == (True, False)


# ── MO-4: bootstrap and app ──────────────────────────────


def test_mo4_bootstrap_creates_app_for_the_owner_and_nobody_else(engine):
    with scratch(engine) as (target, names):
        provision(target, names, "bootstrap")
        with target.connect() as conn:
            owner = conn.execute(text(
                "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'app'")).scalar()
            assert owner == names["owner"], "bootstrap doesn't create schema app"
            for grantee in ("public", names["importer"], names["reader"], names["api"]):
                for privilege in ("USAGE", "CREATE"):
                    assert not conn.execute(text("SELECT has_schema_privilege(:g, 'app', :p)"),
                                            {"g": grantee, "p": privilege}).scalar(), (grantee, privilege)


def test_mo4_rerunning_bootstrap_keeps_app_grants_policies_and_owners(engine):
    """Bootstrap runs before every upgrade: on a fully provisioned database it never
    revokes the API role's app grants and changes no policy, grant or owner."""
    with scratch(engine) as (target, names):
        provisioned(target, names)
        with target.connect() as conn:
            before = (conn.execute(text(ACL_SNAPSHOT)).all(), conn.execute(text(POLICIES)).all())
        provision(target, names, "bootstrap")
        with target.connect() as conn:
            after = (conn.execute(text(ACL_SNAPSHOT)).all(), conn.execute(text(POLICIES)).all())
        assert after == before
        assert _app_privileges(target, names["api"]) == EXPECTED_APP


# ── MO-6 (#22's half): no bootstrap ──────────────────────


def test_mo6_without_bootstrap_upgrade_head_creates_both_schemas(engine):
    with scratch(engine) as (target, _):
        migrate(target)
        assert version(target) == head()
        assert {"perennial", "app"} <= schemas(target)


# ── MO-8 (#22's form): downgrade to 0010, then upgrade ───


def test_mo8_after_dropping_app_bootstrap_lets_the_owner_upgrade_again(engine):
    """As the owner: downgrade to 0010 (it drops app), bootstrap, upgrade head."""
    with scratch(engine) as (target, names):
        provisioned(target, names)
        migrate(target, "0010", owner=names["owner"], down=True)
        assert version(target) == "0010" and "app" not in schemas(target)
        provision(target, names, "bootstrap")
        migrate(target, owner=names["owner"])
        assert version(target) == head()


def test_mo8_without_bootstrap_the_owners_upgrade_fails_at_0011_and_stays_at_0010(engine):
    with scratch(engine) as (target, names):
        provisioned(target, names)
        migrate(target, "0010", owner=names["owner"], down=True)
        with pytest.raises(DBAPIError) as caught:
            migrate(target, owner=names["owner"])
        assert caught.value.orig.sqlstate == "42501"  # permission denied for the database
        assert version(target) == "0010" and "app" not in schemas(target)
