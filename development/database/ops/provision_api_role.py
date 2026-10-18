"""Dedicated non-owner FastAPI group. Never grants access to Supabase auth.*.

An administrator runs this AFTER migrations. Login credentials and membership
are deployment-owned; the API must not run as postgres/schema owner.
"""

import os
import re

import psycopg
from psycopg import sql


def provision(conn, role="perennial_api"):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", role) or role.startswith("pg_"):
        raise ValueError("Choose a dedicated, lowercase API group name")
    existing = conn.execute(
        "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolcanlogin FROM pg_roles WHERE rolname=%s",
        (role,),
    ).fetchone()
    if existing and any(existing):
        raise ValueError("Existing API group has unsafe privileges")
    if conn.execute(
        "SELECT 1 FROM pg_roles r WHERE r.rolname=%s AND (EXISTS (SELECT 1 FROM pg_class c WHERE c.relowner=r.oid) OR EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspowner=r.oid))",
        (role,),
    ).fetchone():
        raise ValueError("API group must not own database objects")
    if conn.execute(
        "SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member WHERE r.rolname=%s",
        (role,),
    ).fetchone():
        raise ValueError("API group must not inherit other roles")
    if not existing:
        conn.execute(
            sql.SQL(
                "CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
            ).format(sql.Identifier(role))
        )
    database = conn.execute("SELECT current_database()").fetchone()[0]
    conn.execute(
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(database), sql.Identifier(role)
        )
    )
    conn.execute(
        sql.SQL("REVOKE ALL ON SCHEMA app, perennial FROM {}").format(
            sql.Identifier(role)
        )
    )
    conn.execute(
        sql.SQL("GRANT USAGE ON SCHEMA app, perennial TO {}").format(
            sql.Identifier(role)
        )
    )
    conn.execute(
        sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA app, perennial FROM {}").format(
            sql.Identifier(role)
        )
    )
    for table in (
        "profiles",
        "preferences",
        "notifications",
        "watchlist",
        "auth_challenges",
        "verified_sessions",
    ):
        conn.execute(
            sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON app.{} TO {}").format(
                sql.Identifier(table), sql.Identifier(role)
            )
        )
    for table in ("publications", "current_publication"):
        conn.execute(
            sql.SQL("GRANT SELECT ON perennial.{} TO {}").format(
                sql.Identifier(table), sql.Identifier(role)
            )
        )
    # Boolean only; the runtime still cannot SELECT identities/password hashes.
    conn.execute(
        sql.SQL("GRANT EXECUTE ON FUNCTION app.email_is_registered(text) TO {}").format(
            sql.Identifier(role)
        )
    )


if __name__ == "__main__":
    raw = os.environ.get("OPS_DATABASE_URL")
    if not raw:
        raise SystemExit(
            "Set OPS_DATABASE_URL to administrative credentials (never frontend)."
        )
    try:
        with psycopg.connect(raw) as connection:
            provision(connection)
    except Exception as error:
        # Database exceptions can echo connection details; expose only the class.
        raise SystemExit(f"API grants failed ({type(error).__name__})") from None
    print(
        "API group ready. Assign a dedicated NOBYPASSRLS login before starting FastAPI."
    )
