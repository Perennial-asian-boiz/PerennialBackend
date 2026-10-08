"""Provision dedicated NOLOGIN owner/importer/reader groups using an explicit table policy.

Run bootstrap before migrations and grants after every migration. Administrative
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
    "run_dependencies", "publications", "publication_sources", "market_cap_observations",
    "publication_market_caps",
)
MUTABLE = ("ingestion_runs", "current_publication")
TABLES = (*IMMUTABLE, *MUTABLE, "securities")


def provision(conn, roles, *, phase):
    """Apply inside the caller's transaction. Existing dedicated roles must be safe."""
    if phase not in {"bootstrap", "grants"}:
        raise ValueError("Unknown provisioning phase")
    if (len(roles) != 3 or len(set(roles)) != 3
            or any(not r or len(r.encode()) > 63 or r.startswith("pg_") or "\0" in r for r in roles)):
        raise ValueError("Choose three distinct dedicated role names of at most 63 bytes")
    owner, importer, reader = roles
    current_user, database = conn.execute("SELECT current_user, current_database()").fetchone()
    if current_user in roles:
        raise ValueError("Administrative login must differ from application group roles")
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
    # Dedicated database: deployment uses authenticated member logins, not PUBLIC access.
    conn.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(sql.Identifier(database)))
    for role in roles:
        conn.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}")
                     .format(sql.Identifier(database), sql.Identifier(role)))
    conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS perennial AUTHORIZATION {}").format(sql.Identifier(owner)))
    conn.execute(sql.SQL("ALTER SCHEMA perennial OWNER TO {}").format(sql.Identifier(owner)))
    conn.execute(sql.SQL("REVOKE ALL ON SCHEMA perennial FROM PUBLIC, {}, {}")
                 .format(sql.Identifier(importer), sql.Identifier(reader)))
    conn.execute(sql.SQL("GRANT USAGE ON SCHEMA perennial TO {}, {}")
                 .format(sql.Identifier(importer), sql.Identifier(reader)))
    # Alembic's version table is in public; only the migration owner needs CREATE there.
    conn.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
    conn.execute(sql.SQL("GRANT USAGE, CREATE ON SCHEMA public TO {}").format(sql.Identifier(owner)))
    for schema in (None, "perennial"):
        scope = sql.SQL("") if schema is None else sql.SQL(" IN SCHEMA perennial")
        for kind in ("TABLES", "SEQUENCES", "FUNCTIONS"):
            conn.execute(sql.SQL("ALTER DEFAULT PRIVILEGES FOR ROLE {}{} REVOKE ALL ON {} FROM PUBLIC, {}, {}")
                         .format(sql.Identifier(owner), scope, sql.SQL(kind),
                                 sql.Identifier(importer), sql.Identifier(reader)))
    if phase == "bootstrap":
        return
    actual = {r[0] for r in conn.execute("SELECT tablename FROM pg_tables WHERE schemaname='perennial'")}
    if actual != set(TABLES):
        raise ValueError("Application table policy differs from migrated schema; update explicit grants")
    for table in TABLES:
        ident = sql.Identifier("perennial", table)
        conn.execute(sql.SQL("ALTER TABLE {} OWNER TO {}").format(ident, sql.Identifier(owner)))
        conn.execute(sql.SQL("REVOKE ALL ON TABLE {} FROM PUBLIC, {}, {}")
                     .format(ident, sql.Identifier(importer), sql.Identifier(reader)))
        columns = [sql.Identifier(r[0]) for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='perennial' AND table_name=%s",
            (table,))]
        # Table REVOKE leaves any old column grants intact, so remove those explicitly too.
        conn.execute(sql.SQL("REVOKE ALL ({}) ON TABLE {} FROM PUBLIC, {}, {}")
                     .format(sql.SQL(", ").join(columns), ident, sql.Identifier(importer), sql.Identifier(reader)))
        conn.execute(sql.SQL("GRANT SELECT, INSERT ON TABLE {} TO {}").format(ident, sql.Identifier(importer)))
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
            conn.execute(sql.SQL("REVOKE ALL ON SEQUENCE {} FROM PUBLIC, {}, {}")
                         .format(seq_ident, sql.Identifier(importer), sql.Identifier(reader)))
            conn.execute(sql.SQL("GRANT USAGE, SELECT ON SEQUENCE {} TO {}")
                         .format(seq_ident, sql.Identifier(importer)))
    if conn.execute("SELECT to_regclass('public.alembic_version')").fetchone()[0]:
        conn.execute(sql.SQL("ALTER TABLE public.alembic_version OWNER TO {}").format(sql.Identifier(owner)))
        conn.execute(sql.SQL("REVOKE ALL ON TABLE public.alembic_version FROM PUBLIC, {}, {}")
                     .format(sql.Identifier(importer), sql.Identifier(reader)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("bootstrap", "grants"), required=True)
    parser.add_argument("--owner", default="perennial_owner")
    parser.add_argument("--importer", default="perennial_importer")
    parser.add_argument("--reader", default="perennial_reader")
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
            provision(conn, (args.owner, args.importer, args.reader), phase=args.phase)
    except Exception as exc:
        print(f"Role provisioning failed ({type(exc).__name__}); transaction rolled back", file=sys.stderr)
        return 1
    print(f"Role provisioning {args.phase} applied")
    return 0


if __name__ == "__main__":
    sys.exit(main())
