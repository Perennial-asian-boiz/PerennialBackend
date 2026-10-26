"""Provision dedicated NOLOGIN owner/importer/reader/api groups using an explicit table policy.

Run bootstrap before every upgrade, and grants after every migration. Bootstrap is
idempotent and creates the schemas the owner migrates into, perennial and app: the
owner holds no CREATE on the database. Before a pg_restore of a whole-database dump,
run roles: it only creates the group roles that the dump's policies name, because the
dump creates its own schemas. Administrative credentials come only from
OPS_DATABASE_URL; login membership/secrets are deployment-owned. The administrative
login may be a superuser or, as on Supabase, a non-superuser CREATEROLE role that owns
the database: it grants itself SET (not INHERIT) on the owner and acts as the owner for
the owner's objects. Needs PostgreSQL 16 or later.
"""
import argparse
import logging
import os
import sys
from contextlib import contextmanager

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
# M4 market caps through the ingestion contract: batch rows and publication pins.
IMMUTABLE += ("market_caps", "publication_market_cap_rows")
IMMUTABLE += ("fundamentals", "fundamental_scores")  # pattern evaluation
TABLES = (*IMMUTABLE, *MUTABLE, *REFERENCE, "securities")
# M5 read API (perennial_api): published data only, never raw source tables,
# runs or operator text. Two grants are column-level, so code selects columns.
API_TABLES = ("publications", "current_publication", "securities", "security_identifiers")
API_COLUMNS = {
    # S1-12: the stored summary it serves; never summary_grade or description_digest.
    "company_profiles": ("id", "sector", "industry", "summary", "summary_status",
                         "summary_source_name", "summary_source_url", "summary_as_of"),
    "publication_events": ("id", "publication_id", "kind", "checks", "created_at"),
}
# What the S1-12 summary job (importer) may write on company_profiles.
SUMMARY_COLUMNS = ("summary", "summary_grade", "summary_status", "summary_source_name",
                   "summary_source_url", "summary_as_of", "description_digest")
SUPABASE_API_ROLES = ("anon", "authenticated", "service_role")
SHARED_DATABASE_NOTICE = ("Schema auth exists, so this is a shared (Supabase) database: PUBLIC keeps CONNECT and "
                          "TEMPORARY, which the platform's own logins connect through")
log = logging.getLogger(__name__)


def _present_roles(conn, names):
    """The given role names that exist on this server, in order."""
    return [r for r in names
            if conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (r,)).fetchone()]


def _refuse_supabase_exposure(conn, supabase_roles):
    """Fail before any change if a Supabase API role can reach schema perennial.

    PostgREST serves whatever these roles can read, and REVOKE ... FROM PUBLIC
    does not cover named roles, so this is asserted rather than assumed. Names
    that do not exist on this server (local and CI) are skipped.
    """
    present = _present_roles(conn, supabase_roles)
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
    # A non-superuser CREATEROLE admin holds only ADMIN OPTION on roles it created.
    # CREATE SCHEMA ... AUTHORIZATION and ALTER ... OWNER TO need SET on the owner;
    # INHERIT is never granted, so the admin doesn't silently hold the owner's rights.
    # pg_auth_members then has two rows for the pair: the ADMIN grant CREATE ROLE
    # recorded, and this one; the admin's rights are their union.
    if not conn.execute("SELECT pg_has_role(current_user, %s, 'SET')", (owner,)).fetchone()[0]:
        conn.execute(sql.SQL("GRANT {} TO CURRENT_USER WITH INHERIT FALSE, SET TRUE")
                     .format(sql.Identifier(owner)))
    # Dedicated database: deployment uses authenticated member logins, not PUBLIC access.
    # A database the platform shares (Supabase has schema auth) keeps PUBLIC's CONNECT
    # and TEMPORARY: Auth, the Data API, Storage and the pooler connect through them.
    if conn.execute("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname='auth')").fetchone()[0]:
        log.warning(SHARED_DATABASE_NOTICE)
    else:
        conn.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(sql.Identifier(database)))
    for role in roles:
        conn.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}")
                     .format(sql.Identifier(database), sql.Identifier(role)))
    conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS perennial AUTHORIZATION {}").format(sql.Identifier(owner)))
    _give_schema_to_owner(conn, owner, "perennial")
    # Schema app (the accounts) for the owner, with no grants; an existing app is left
    # alone: provision_api_role.py owns its ownership, grants and policies.
    conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS app AUTHORIZATION {}").format(sql.Identifier(owner)))
    # Alembic's version table is in public; only the migration owner needs CREATE there.
    conn.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
    conn.execute(sql.SQL("GRANT USAGE, CREATE ON SCHEMA public TO {}").format(sql.Identifier(owner)))
    # Grants on the owner's objects are made as the owner: an admin that is not a
    # superuser and doesn't inherit the owner's rights couldn't make them itself.
    with _as_owner(conn, owner):
        conn.execute(sql.SQL("REVOKE ALL ON SCHEMA perennial FROM PUBLIC, {}, {}, {}")
                     .format(sql.Identifier(importer), sql.Identifier(reader), sql.Identifier(api)))
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA perennial TO {}, {}, {}")
                     .format(sql.Identifier(importer), sql.Identifier(reader), sql.Identifier(api)))
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
    # Objects the owner doesn't own yet (after a restore, the admin's) move to the owner first.
    # Read from the catalog: a regclass lookup would need USAGE on the owner's schema.
    relation_owner = ("SELECT pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n"
                      " ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s")
    for table in TABLES:
        _give_to_owner(conn, owner, "TABLE", sql.Identifier("perennial", table),
                       relation_owner, ("perennial", table))
    if conn.execute("SELECT to_regclass('public.alembic_version')").fetchone()[0]:
        _give_to_owner(conn, owner, "TABLE", sql.Identifier("public", "alembic_version"),
                       relation_owner, ("public", "alembic_version"))
    groups = sql.SQL(", ").join(sql.Identifier(r) for r in (importer, reader, api))
    with _as_owner(conn, owner):
        for table in TABLES:
            ident = sql.Identifier("perennial", table)
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
            elif table == "company_profiles":
                # S1-12: the summary job stores its outcome; the rest of a profile is owner-reviewed.
                conn.execute(sql.SQL("GRANT UPDATE ({}) ON TABLE {} TO {}").format(
                    sql.SQL(", ").join(sql.Identifier(c) for c in SUMMARY_COLUMNS), ident, sql.Identifier(importer)))
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
            conn.execute(sql.SQL("REVOKE ALL ON TABLE public.alembic_version FROM PUBLIC, {}, {}, {}")
                         .format(sql.Identifier(importer), sql.Identifier(reader), sql.Identifier(api)))
            # A restore as the admin recreates it under the admin's default ACL in public;
            # on Supabase that grants the Data API roles ALL, a write path to the migration pointer.
            present = _present_roles(conn, supabase_roles)
            if present:
                conn.execute(sql.SQL("REVOKE ALL ON TABLE public.alembic_version FROM {}")
                             .format(sql.SQL(", ").join(sql.Identifier(r) for r in present)))


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


def _give_schema_to_owner(conn, owner, schema):
    """Re-own a schema someone else owns (after a restore, the admin) with its tables.

    The order matters for an admin with SET but not INHERIT on the owner: the owner
    needs CREATE in the schema to take its tables, and the admin can name them only
    while it still owns the schema, so the schema itself moves last.
    """
    if conn.execute("SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=%s",
                    (schema,)).fetchone()[0] == owner:
        return
    conn.execute(sql.SQL("GRANT CREATE ON SCHEMA {} TO {}").format(sql.Identifier(schema), sql.Identifier(owner)))
    for (table,) in conn.execute(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace"
            " WHERE n.nspname=%s AND c.relkind IN ('r', 'p') AND pg_get_userbyid(c.relowner) <> %s",
            (schema, owner)).fetchall():
        # Indexes and identity sequences follow their table.
        conn.execute(sql.SQL("ALTER TABLE {} OWNER TO {}").format(sql.Identifier(schema, table), sql.Identifier(owner)))
    conn.execute(sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(sql.Identifier(schema), sql.Identifier(owner)))


def _give_to_owner(conn, owner, kind, name, owner_query, params):
    """ALTER ... OWNER TO the owner, as the admin, only if someone else owns it."""
    if conn.execute(owner_query, params).fetchone()[0] != owner:
        conn.execute(sql.SQL("ALTER {} {} OWNER TO {}").format(sql.SQL(kind), name, sql.Identifier(owner)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("roles", "bootstrap", "grants"), required=True)
    parser.add_argument("--owner", default="perennial_owner")
    parser.add_argument("--importer", default="perennial_importer")
    parser.add_argument("--reader", default="perennial_reader")
    parser.add_argument("--api", default="perennial_api")
    args = parser.parse_args()
    logging.basicConfig(format="%(message)s")  # provision()'s notices, on stderr
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
