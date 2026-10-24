"""Provision dedicated NOLOGIN owner/importer/reader/api groups using an explicit table policy.

Run bootstrap before migrations and grants after every migration. Before a
pg_restore of a whole-database dump, run roles: it only creates the group roles
that the dump's policies name, because the dump creates its own schemas. Administrative
credentials come only from OPS_DATABASE_URL; login membership/secrets are deployment-owned.
"""
import argparse
import os
import sys

import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url

IMMUTABLE = (
    "source_batches", "congress_trades", "ark_holdings", "insider_trades", "short_interest",
    "run_dependencies", "publications", "publication_events", "publication_sources", "market_cap_observations",
    "gdelt_theme_volume", "gdelt_theme_articles", "gdelt_spike_results",
    "publication_market_caps",
)
MUTABLE = ("ingestion_runs", "current_publication")
IMMUTABLE += ("gdelt_articles",)
# Congress history (free-plan accumulation): append-only like source batches.
IMMUTABLE += ("congress_trade_history", "congress_history_gaps", "publication_history_watermarks")
MUTABLE += ("gdelt_theme_days", "gdelt_spike_checks")
# M4 identity. Aliases are append-only: root imports register canonical tickers.
# Profiles are reviewed seed data written by the owner; the pipeline only reads them.
IMMUTABLE += ("security_identifiers",)
REFERENCE = ("company_profiles",)
TABLES = (*IMMUTABLE, *MUTABLE, *REFERENCE, "securities")
# M5 read API (perennial_api): published data only, never raw source tables,
# runs or operator text. Two grants are column-level, so code selects columns.
API_TABLES = ("publications", "current_publication", "securities", "security_identifiers")
API_COLUMNS = {
    "company_profiles": ("id", "sector", "industry"),
    "publication_events": ("id", "publication_id", "kind", "checks", "created_at"),
}
SUPABASE_API_ROLES = ("anon", "authenticated", "service_role")


def _refuse_supabase_exposure(conn, supabase_roles):
    """Fail before any change if a Supabase API role can reach schema perennial.

    PostgREST serves whatever these roles can read, and REVOKE ... FROM PUBLIC
    does not cover named roles, so this is asserted rather than assumed. Names
    that do not exist on this server (local and CI) are skipped.
    """
    present = [r for r in supabase_roles
               if conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (r,)).fetchone()]
    if not present or not conn.execute("SELECT to_regnamespace('perennial')").fetchone()[0]:
        return
    exposed = conn.execute(
        # OIDs, not names: the privilege functions may be evaluated before the
        # schema filter, and an OID never fails as a missing name would.
        "SELECT EXISTS (SELECT 1 FROM unnest(%s::text[]) AS r(name) WHERE "
        " has_schema_privilege(r.name, 'perennial', 'USAGE') OR EXISTS ("
        "  SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
        "  WHERE n.nspname = 'perennial' AND c.relkind IN ('r', 'p') AND ("
        "   has_table_privilege(r.name, c.oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')"
        "   OR has_any_column_privilege(r.name, c.oid, 'SELECT,INSERT,UPDATE,REFERENCES'))))",
        (present,)).fetchone()[0]
    if exposed:
        raise ValueError("A Supabase API role can reach schema perennial; revoke it before provisioning")


def provision(conn, roles, *, phase, supabase_roles=SUPABASE_API_ROLES):
    """Apply inside the caller's transaction. Existing dedicated roles must be safe.

    roles = (owner, importer, reader, api). Every refusal happens before the
    first change, so a refused run leaves the database as it was. Phase roles
    stops after the role checks and CREATE ROLE, before any database, schema or
    default-privilege statement (PK-10).
    """
    if phase not in {"roles", "bootstrap", "grants"}:
        raise ValueError("Unknown provisioning phase")
    if (len(roles) != 4 or len(set(roles)) != 4
            or any(not r or len(r.encode()) > 63 or r.startswith("pg_") or "\0" in r for r in roles)):
        raise ValueError("Choose four distinct dedicated role names of at most 63 bytes")
    owner, importer, reader, api = roles
    current_user, database = conn.execute("SELECT current_user, current_database()").fetchone()
    if current_user in roles:
        raise ValueError("Administrative login must differ from application group roles")
    _refuse_supabase_exposure(conn, supabase_roles)
    owns = conn.execute(
        "SELECT (SELECT count(*) FROM pg_class WHERE relowner = r.oid)"
        " + (SELECT count(*) FROM pg_namespace WHERE nspowner = r.oid)"
        " FROM pg_roles r WHERE r.rolname=%s", (api,)).fetchone()
    if owns and owns[0]:
        raise ValueError("The API group role may not own relations or schemas")
    for role in roles:
        existing = conn.execute("SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolcanlogin "
                                "FROM pg_roles WHERE rolname=%s", (role,)).fetchone()
        if existing and any(existing):
            raise ValueError("Existing application group role has unsafe attributes")
        parents = conn.execute("SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member "
                               "WHERE r.rolname=%s LIMIT 1", (role,)).fetchone()
        if parents:
            raise ValueError("Application group roles may not inherit other roles")
        if not existing:
            conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS")
                         .format(sql.Identifier(role)))
    if phase == "roles":
        return
    # Dedicated database: deployment uses authenticated member logins, not PUBLIC access.
    conn.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(sql.Identifier(database)))
    for role in roles:
        conn.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}")
                     .format(sql.Identifier(database), sql.Identifier(role)))
    conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS perennial AUTHORIZATION {}").format(sql.Identifier(owner)))
    conn.execute(sql.SQL("ALTER SCHEMA perennial OWNER TO {}").format(sql.Identifier(owner)))
    conn.execute(sql.SQL("REVOKE ALL ON SCHEMA perennial FROM PUBLIC, {}, {}, {}")
                 .format(sql.Identifier(importer), sql.Identifier(reader), sql.Identifier(api)))
    conn.execute(sql.SQL("GRANT USAGE ON SCHEMA perennial TO {}, {}, {}")
                 .format(sql.Identifier(importer), sql.Identifier(reader), sql.Identifier(api)))
    # Alembic's version table is in public; only the migration owner needs CREATE there.
    conn.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
    conn.execute(sql.SQL("GRANT USAGE, CREATE ON SCHEMA public TO {}").format(sql.Identifier(owner)))
    for schema in (None, "perennial"):
        scope = sql.SQL("") if schema is None else sql.SQL(" IN SCHEMA perennial")
        for kind in ("TABLES", "SEQUENCES", "FUNCTIONS"):
            conn.execute(sql.SQL("ALTER DEFAULT PRIVILEGES FOR ROLE {}{} REVOKE ALL ON {} FROM PUBLIC, {}, {}, {}")
                         .format(sql.Identifier(owner), scope, sql.SQL(kind),
                                 sql.Identifier(importer), sql.Identifier(reader), sql.Identifier(api)))
    if phase == "bootstrap":
        return
    actual = {r[0] for r in conn.execute("SELECT tablename FROM pg_tables WHERE schemaname='perennial'")}
    if actual != set(TABLES):
        raise ValueError("Application table policy differs from migrated schema; update explicit grants")
    for table in TABLES:
        ident = sql.Identifier("perennial", table)
        conn.execute(sql.SQL("ALTER TABLE {} OWNER TO {}").format(ident, sql.Identifier(owner)))
        groups = sql.SQL(", ").join(sql.Identifier(r) for r in (importer, reader, api))
        conn.execute(sql.SQL("REVOKE ALL ON TABLE {} FROM PUBLIC, {}").format(ident, groups))
        columns = [sql.Identifier(r[0]) for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='perennial' AND table_name=%s",
            (table,))]
        # Table REVOKE leaves any old column grants intact, so remove those explicitly too.
        conn.execute(sql.SQL("REVOKE ALL ({}) ON TABLE {} FROM PUBLIC, {}")
                     .format(sql.SQL(", ").join(columns), ident, groups))
        if table in API_TABLES:
            conn.execute(sql.SQL("GRANT SELECT ON TABLE {} TO {}").format(ident, sql.Identifier(api)))
        elif table in API_COLUMNS:
            conn.execute(sql.SQL("GRANT SELECT ({}) ON TABLE {} TO {}").format(
                sql.SQL(", ").join(sql.Identifier(c) for c in API_COLUMNS[table]), ident, sql.Identifier(api)))
        importer_privileges = "SELECT" if table in REFERENCE else "SELECT, INSERT"
        conn.execute(sql.SQL("GRANT {} ON TABLE {} TO {}")
                     .format(sql.SQL(importer_privileges), ident, sql.Identifier(importer)))
        conn.execute(sql.SQL("GRANT SELECT ON TABLE {} TO {}").format(ident, sql.Identifier(reader)))
        if table in MUTABLE:
            conn.execute(sql.SQL("GRANT UPDATE ON TABLE {} TO {}").format(ident, sql.Identifier(importer)))
        elif table == "securities":
            conn.execute(sql.SQL("GRANT UPDATE (exchange, currency) ON TABLE {} TO {}")
                         .format(ident, sql.Identifier(importer)))
        sequences = conn.execute(
            "SELECT pg_get_serial_sequence(%s, attname) FROM pg_attribute "
            "WHERE attrelid=%s::regclass AND attidentity <> '' AND NOT attisdropped",
            (f"perennial.{table}", f"perennial.{table}"),).fetchall()
        for (sequence,) in sequences:
            seq_schema, seq_name = conn.execute(
                "SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE c.oid=%s::regclass", (sequence,)).fetchone()
            seq_ident = sql.Identifier(seq_schema, seq_name)
            conn.execute(sql.SQL("REVOKE ALL ON SEQUENCE {} FROM PUBLIC, {}")
                         .format(seq_ident, groups))
            conn.execute(sql.SQL("GRANT USAGE, SELECT ON SEQUENCE {} TO {}")
                         .format(seq_ident, sql.Identifier(importer)))
    if conn.execute("SELECT to_regclass('public.alembic_version')").fetchone()[0]:
        conn.execute(sql.SQL("ALTER TABLE public.alembic_version OWNER TO {}").format(sql.Identifier(owner)))
        conn.execute(sql.SQL("REVOKE ALL ON TABLE public.alembic_version FROM PUBLIC, {}, {}, {}")
                     .format(sql.Identifier(importer), sql.Identifier(reader), sql.Identifier(api)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("roles", "bootstrap", "grants"), required=True)
    parser.add_argument("--owner", default="perennial_owner")
    parser.add_argument("--importer", default="perennial_importer")
    parser.add_argument("--reader", default="perennial_reader")
    parser.add_argument("--api", default="perennial_api")
    args = parser.parse_args()
    try:
        url = make_url(os.environ.get("OPS_DATABASE_URL", ""))
        if url.drivername not in {"postgresql", "postgresql+psycopg"} or not url.host or not url.database:
            raise ValueError("Expected explicit PostgreSQL OPS_DATABASE_URL")
        if set(url.query) - {"sslmode", "sslrootcert"}:
            raise ValueError("Unexpected connection options")
        for key in list(os.environ):
            if key.startswith("PG"):
                del os.environ[key]
        with psycopg.connect(host=url.host, port=url.port or 5432, dbname=url.database,
                            user=url.username, password=url.password or "", connect_timeout=10, **url.query) as conn:
            provision(conn, (args.owner, args.importer, args.reader, args.api), phase=args.phase)
    except Exception as exc:
        print(f"Role provisioning failed ({type(exc).__name__}); transaction rolled back", file=sys.stderr)
        return 1
    print(f"Role provisioning {args.phase} applied")
    return 0


if __name__ == "__main__":
    sys.exit(main())
