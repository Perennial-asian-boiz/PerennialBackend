"""Schema app grants, ownership and RLS policies for the FastAPI group role.

An administrator runs this AFTER migrations and after provision_roles.py, which
alone creates the API group role (and its perennial grants). This script manages
schema app only and never touches perennial or Supabase's auth schema. Login
credentials and membership are deployment-owned; the API must not run as
postgres or as the schema owner.

Re-run it after every migration, with provision_roles.py --phase grants, in
either order (#22 fix packet, changes 4 and 6).
"""

import os
import re
import sys

import psycopg
from psycopg import sql

APP_TABLES = (
    "profiles",
    "preferences",
    "notifications",
    "watchlist",
    "auth_challenges",
    "verified_sessions",
)
# Each user table's owning column; a policy compares it with the request subject.
USER_TABLES = {
    "profiles": "id",
    "preferences": "user_id",
    "notifications": "user_id",
    "watchlist": "user_id",
}
SUPABASE_API_ROLES = ("anon", "authenticated", "service_role")
LOOKUP = "app.email_is_registered(text)"
_NAME = re.compile(r"[a-z][a-z0-9_]{0,62}")


def _refuse_supabase_exposure(conn, supabase_roles):
    """Fail before any change if a Supabase API role can reach schema app.

    As provision_roles.py does for perennial; names absent from this server are
    skipped, and OIDs keep the privilege functions from failing on names.
    """
    present = [
        r
        for r in supabase_roles
        if conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (r,)).fetchone()
    ]
    if not present:
        return
    exposed = conn.execute(
        "SELECT EXISTS (SELECT 1 FROM unnest(%s::text[]) AS r(name) WHERE"
        " has_schema_privilege(r.name, 'app', 'USAGE,CREATE') OR EXISTS ("
        "  SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
        "  WHERE n.nspname = 'app' AND c.relkind IN ('r', 'p') AND ("
        "   has_table_privilege(r.name, c.oid,"
        "    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')"
        "   OR has_any_column_privilege(r.name, c.oid,"
        "    'SELECT,INSERT,UPDATE,REFERENCES'))))",
        (present,),
    ).fetchone()[0]
    if exposed:
        raise ValueError(
            "A Supabase API role can reach schema app; revoke it before provisioning"
        )


def provision(
    conn,
    role="perennial_api",
    *,
    owner="perennial_owner",
    supabase_roles=SUPABASE_API_ROLES,
):
    """Apply inside the caller's transaction; every refusal comes before any change."""
    if (
        not all(_NAME.fullmatch(name) for name in (role, owner))
        or role == owner
        or any(name.startswith("pg_") for name in (role, owner))
    ):
        raise ValueError("Choose dedicated, lowercase API and owner group names")
    for name in (role, owner):
        if not conn.execute(
            "SELECT 1 FROM pg_roles WHERE rolname=%s", (name,)
        ).fetchone():
            raise ValueError(
                "Group roles are missing; run provision_roles.py --phase bootstrap first"
            )
    existing = conn.execute(
        "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls,"
        " rolcanlogin FROM pg_roles WHERE rolname=%s",
        (role,),
    ).fetchone()
    if any(existing):
        raise ValueError("Existing API group has unsafe privileges")
    if conn.execute(
        "SELECT 1 FROM pg_roles r WHERE r.rolname=%s AND (EXISTS (SELECT 1 FROM"
        " pg_class c WHERE c.relowner=r.oid) OR EXISTS (SELECT 1 FROM pg_namespace n"
        " WHERE n.nspowner=r.oid))",
        (role,),
    ).fetchone():
        raise ValueError("API group must not own database objects")
    if conn.execute(
        "SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member"
        " WHERE r.rolname=%s",
        (role,),
    ).fetchone():
        raise ValueError("API group must not inherit other roles")
    _refuse_supabase_exposure(conn, supabase_roles)

    api, own = sql.Identifier(role), sql.Identifier(owner)
    # Owned by the NOBYPASSRLS owner, so FORCE RLS binds even where the
    # administrator (Supabase's postgres) may bypass RLS.
    conn.execute(sql.SQL("ALTER SCHEMA app OWNER TO {}").format(own))
    for table in APP_TABLES:
        conn.execute(
            sql.SQL("ALTER TABLE app.{} OWNER TO {}").format(sql.Identifier(table), own)
        )  # owned sequences follow their table
    # Default privileges apply per creating role, so name the owner explicitly.
    for kind in ("TABLES", "SEQUENCES", "FUNCTIONS"):
        conn.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA app"
                " REVOKE ALL ON {} FROM PUBLIC, {}"
            ).format(own, sql.SQL(kind), api)
        )

    conn.execute(sql.SQL("REVOKE ALL ON SCHEMA app FROM PUBLIC, {}").format(api))
    conn.execute(sql.SQL("GRANT USAGE ON SCHEMA app TO {}").format(api))
    conn.execute(sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA app FROM {}").format(api))
    for table in APP_TABLES:
        conn.execute(
            sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON app.{} TO {}").format(
                sql.Identifier(table), api
            )
        )

    # RLS: one policy per table, naming only the API role (change 6). The
    # migration creates none, so an unprovisioned database denies everything.
    subject = "nullif(current_setting('request.jwt.claim.sub', true), '')::uuid"
    for table in APP_TABLES:
        column = USER_TABLES.get(table)
        rule = (
            sql.SQL("true")
            if column is None
            else sql.SQL(f"{{}} = {subject}").format(sql.Identifier(column))
        )
        ident = sql.Identifier(table)
        conn.execute(sql.SQL("DROP POLICY IF EXISTS own_rows ON app.{}").format(ident))
        conn.execute(
            sql.SQL(
                "CREATE POLICY own_rows ON app.{} TO {} USING ({}) WITH CHECK ({})"
            ).format(ident, api, rule, rule)
        )

    # The lookup is SECURITY DEFINER and reads auth.users, so it stays owned by
    # the administrative role; only the API role may call it.
    conn.execute(
        sql.SQL(f"REVOKE ALL ON FUNCTION {LOOKUP} FROM PUBLIC, {{}}").format(own)
    )
    conn.execute(sql.SQL(f"GRANT EXECUTE ON FUNCTION {LOOKUP} TO {{}}").format(api))


def main():
    raw = os.environ.get("OPS_DATABASE_URL")
    if not raw:
        raise SystemExit(
            "Set OPS_DATABASE_URL to administrative credentials (never frontend)."
        )
    role = os.environ.get("API_GROUP_ROLE", "perennial_api")
    owner = os.environ.get("OWNER_GROUP_ROLE", "perennial_owner")
    try:
        with psycopg.connect(raw) as connection:
            provision(connection, role, owner=owner)
    except Exception as error:
        # Database exceptions can echo connection details; expose only the class.
        raise SystemExit(f"API grants failed ({type(error).__name__})") from None
    sys.stdout.write("API grants and policies applied for schema app.\n")


if __name__ == "__main__":
    main()
