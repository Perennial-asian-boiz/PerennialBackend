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
from contextlib import contextmanager

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
# Found through the catalog: a regprocedure cast would need USAGE on schema app.
LOOKUP_OID = (
    "(SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace"
    " WHERE n.nspname = 'app' AND p.proname = 'email_is_registered'"
    " AND oidvectortypes(p.proargtypes) = 'text')"
)
_NAME = re.compile(r"[a-z][a-z0-9_]{0,62}")
LOOKUP_REFUSED = (
    "app.email_is_registered must be a STABLE SECURITY DEFINER function with an"
    " empty search_path, owned by the administrator and callable only by the API"
)


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
        "    'SELECT,INSERT,UPDATE,REFERENCES')))"
        # PostgREST exposes any callable function as RPC.
        " OR EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace"
        "  WHERE n.nspname = 'app' AND has_function_privilege(r.name, p.oid, 'EXECUTE')))",
        (present,),
    ).fetchone()[0]
    if exposed:
        raise ValueError(
            "A Supabase API role can reach schema app; revoke it before provisioning"
        )


def _lookup_shape(conn):
    """(owner is current_user, SECURITY DEFINER, search_path pinned empty, STABLE)."""
    shape = conn.execute(
        "SELECT proowner = (SELECT oid FROM pg_roles WHERE rolname = current_user),"
        " prosecdef,"
        " coalesce(proconfig && ARRAY['search_path=\"\"', 'search_path='], false),"
        " provolatile = 's'"
        f" FROM pg_proc WHERE oid = {LOOKUP_OID}"
    ).fetchone()
    if shape is None:
        raise ValueError(LOOKUP_REFUSED)
    return shape


def _owner_of(conn, catalog_query, *params):
    return conn.execute(catalog_query, params).fetchone()[0]


@contextmanager
def _as_owner(conn, owner):
    """Run statements as the owner, then return to the caller's role.

    SET LOCAL lasts until the transaction ends. On an autocommit connection it would
    do nothing, and a GRANT by a non-owner only warns, so check that it took effect.
    """
    previous = conn.execute("SELECT current_user").fetchone()[0]
    conn.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(owner)))
    if conn.execute("SELECT current_user").fetchone()[0] != owner:
        raise ValueError("Run provisioning inside a transaction")
    yield
    conn.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(previous)))


@contextmanager
def _owner_privileges(conn, owner):
    """Inherit the owner's privileges for a few statements, unless this login already
    does (a superuser). Turned off again before returning; an error rolls the caller's
    transaction back, this grant included, so it never outlives the transaction."""
    if conn.execute(
        "SELECT pg_has_role(current_user, %s, 'USAGE')", (owner,)
    ).fetchone()[0]:
        yield
        return
    conn.execute(
        sql.SQL("GRANT {} TO CURRENT_USER WITH INHERIT TRUE").format(
            sql.Identifier(owner)
        )
    )
    yield
    conn.execute(
        sql.SQL("GRANT {} TO CURRENT_USER WITH INHERIT FALSE").format(
            sql.Identifier(owner)
        )
    )


def provision(
    conn,
    role="perennial_api",
    *,
    owner="perennial_owner",
    importer="perennial_importer",
    reader="perennial_reader",
    supabase_roles=SUPABASE_API_ROLES,
):
    """Apply inside the caller's transaction; every refusal comes before any change.

    The administrator running this becomes the definer of
    app.email_is_registered (security decision, option (a)): re-running as the
    same administrator changes nothing, and running as another administrator
    moves the function to that login, which is accepted.
    """
    groups = (role, owner, importer, reader)
    if (
        not all(_NAME.fullmatch(name) for name in groups)
        or len(set(groups)) != len(groups)
        or any(name.startswith("pg_") for name in groups)
    ):
        raise ValueError("Choose dedicated, lowercase group role names")
    # The definer must never be a group role: none of them may read auth.
    if conn.execute("SELECT current_user").fetchone()[0] in groups:
        raise ValueError(
            "Run provisioning as an administrative login, not a group role"
        )
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
    if conn.execute(
        "SELECT coalesce((SELECT has_schema_privilege(%s, oid, 'USAGE,CREATE')"
        " FROM pg_namespace WHERE nspname = 'auth'), false)",
        (owner,),
    ).fetchone()[0]:
        raise ValueError("The owner group must hold nothing on schema auth")
    _, definer, pinned, stable = _lookup_shape(conn)
    if not (definer and pinned and stable):
        raise ValueError(LOOKUP_REFUSED)

    api, own = sql.Identifier(role), sql.Identifier(owner)
    # A non-superuser CREATEROLE administrator (Supabase's postgres) needs SET on the
    # owner to give objects to it, as provision_roles.py grants; never INHERIT for good.
    if not conn.execute(
        "SELECT pg_has_role(current_user, %s, 'SET')", (owner,)
    ).fetchone()[0]:
        conn.execute(
            sql.SQL("GRANT {} TO CURRENT_USER WITH INHERIT FALSE, SET TRUE").format(own)
        )
    # Owned by the NOBYPASSRLS owner, so FORCE RLS binds even where the
    # administrator (Supabase's postgres) may bypass RLS. Only what someone else
    # owns is re-owned. After a restore the administrator owns app and its tables:
    # the owner needs CREATE in app to take the tables, and the administrator can
    # name them only while it still owns app, so the schema moves last.
    schema_moves = (
        _owner_of(
            conn,
            "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'app'",
        )
        != owner
    )
    if schema_moves:
        conn.execute(sql.SQL("GRANT CREATE ON SCHEMA app TO {}").format(own))
    for table in APP_TABLES:
        if (
            _owner_of(
                conn,
                "SELECT pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n"
                " ON n.oid = c.relnamespace WHERE n.nspname = 'app' AND c.relname = %s",
                table,
            )
            != owner
        ):
            conn.execute(
                sql.SQL("ALTER TABLE app.{} OWNER TO {}").format(
                    sql.Identifier(table), own
                )
            )  # owned sequences follow their table
    if schema_moves:
        conn.execute(sql.SQL("ALTER SCHEMA app OWNER TO {}").format(own))
    # Grants, default privileges and policies on the owner's objects are made as
    # the owner, which an administrator without its privileges can't do itself.
    with _as_owner(conn, owner):
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
        conn.execute(
            sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA app FROM {}").format(api)
        )
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
            conn.execute(
                sql.SQL("DROP POLICY IF EXISTS own_rows ON app.{}").format(ident)
            )
            conn.execute(
                sql.SQL(
                    "CREATE POLICY own_rows ON app.{} TO {} USING ({}) WITH CHECK ({})"
                ).format(ident, api, rule, rule)
            )

    # The lookup is SECURITY DEFINER and reads auth.users: whoever migrated it,
    # its definer is this administrator, and only the API role may call it.
    # Taking it from the owner needs the owner's privileges, and naming it needs
    # USAGE on app, so these statements alone run with the owner's privileges.
    with _owner_privileges(conn, owner):
        conn.execute(f"ALTER FUNCTION {LOOKUP} OWNER TO CURRENT_USER")
        grantees = conn.execute(
            "SELECT DISTINCT a.grantee FROM pg_proc p, aclexplode(p.proacl) a"
            f" WHERE p.oid = {LOOKUP_OID} AND a.grantee <> 0"
            " AND a.grantee <> p.proowner"
        ).fetchall()
        conn.execute(f"REVOKE ALL ON FUNCTION {LOOKUP} FROM PUBLIC")
        for (grantee,) in grantees:
            conn.execute(
                sql.SQL(f"REVOKE ALL ON FUNCTION {LOOKUP} FROM {{}}").format(
                    sql.Identifier(
                        conn.execute(
                            "SELECT rolname FROM pg_roles WHERE oid = %s", (grantee,)
                        ).fetchone()[0]
                    )
                )
            )
        conn.execute(sql.SQL(f"GRANT EXECUTE ON FUNCTION {LOOKUP} TO {{}}").format(api))
    owned, definer, pinned, stable = _lookup_shape(conn)
    others = conn.execute(
        "SELECT count(*) FROM pg_proc p, aclexplode(p.proacl) a"
        f" WHERE p.oid = {LOOKUP_OID} AND a.privilege_type = 'EXECUTE'"
        " AND a.grantee NOT IN (p.proowner, (SELECT oid FROM pg_roles"
        " WHERE rolname = %s))",
        (role,),
    ).fetchone()[0]
    if not (owned and definer and pinned and stable) or others:
        raise ValueError(LOOKUP_REFUSED)


def main():
    raw = os.environ.get("OPS_DATABASE_URL")
    if not raw:
        raise SystemExit(
            "Set OPS_DATABASE_URL to administrative credentials (never frontend)."
        )
    role = os.environ.get("API_GROUP_ROLE", "perennial_api")
    owner = os.environ.get("OWNER_GROUP_ROLE", "perennial_owner")
    importer = os.environ.get("IMPORTER_GROUP_ROLE", "perennial_importer")
    reader = os.environ.get("READER_GROUP_ROLE", "perennial_reader")
    try:
        with psycopg.connect(raw) as connection:
            provision(connection, role, owner=owner, importer=importer, reader=reader)
    except Exception as error:
        # Database exceptions can echo connection details; expose only the class.
        raise SystemExit(f"API grants failed ({type(error).__name__})") from None
    sys.stdout.write("API grants and policies applied for schema app.\n")


if __name__ == "__main__":
    main()
