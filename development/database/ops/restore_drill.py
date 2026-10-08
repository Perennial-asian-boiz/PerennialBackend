"""Dump and verify a synthetic local test database; never restore over an existing DB."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from sqlalchemy.engine import make_url

LOOPBACK = {"localhost": "127.0.0.1", "127.0.0.1": "127.0.0.1", "::1": "::1"}


def clean_environment():
    return {k: v for k, v in os.environ.items() if not k.startswith("PG")}


def test_connection(raw):
    try:
        url = make_url(raw)
        if (url.drivername not in {"postgresql", "postgresql+psycopg"}
                or url.host not in LOOPBACK or "test" not in (url.database or "")
                or set(url.query) - {"sslmode"}):
            raise ValueError
        return dict(host=url.host, hostaddr=LOOPBACK[url.host], port=url.port or 5432,
                    dbname=url.database, user=url.username or "perennial_test",
                    password=url.password or "", sslmode=url.query.get("sslmode", "prefer"),
                    connect_timeout=10)
    except Exception:
        raise ValueError("Expected a loopback PostgreSQL test URL without routing parameters") from None


def table_manifest(conn):
    """Hash all application rows in stable order using bounded client memory."""
    tables = conn.execute("SELECT tablename FROM pg_tables WHERE schemaname='perennial' ORDER BY tablename").fetchall()
    if not tables:
        raise ValueError("No perennial application tables to verify")
    result = {}
    for (table,) in tables:
        digest, count = hashlib.sha256(), 0
        with conn.cursor(name="drill_manifest") as rows:
            rows.execute(sql.SQL("SELECT row_to_json(t)::text FROM {} t ORDER BY row_to_json(t)::text COLLATE \"C\"")
                         .format(sql.Identifier("perennial", table)))
            for (row,) in rows:
                data = row.encode()
                digest.update(len(data).to_bytes(8, "big"))
                digest.update(data)
                count += 1
        result[table] = {"rows": count, "sha256": digest.hexdigest()}
    invalid = conn.execute("SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace "
                           "WHERE n.nspname='perennial' AND NOT c.convalidated").fetchone()[0]
    if invalid:
        raise ValueError("Unvalidated application constraints")
    return result


def pg_tool(tool, params, args, env):
    # Password stays out of argv, stdout and reports. Children get no ambient PG* defaults.
    child_env = dict(env, PGPASSWORD=params["password"])
    public_params = {k: v for k, v in params.items() if k != "password"}
    result = subprocess.run([tool, "--no-password", "--dbname", make_conninfo(**public_params), *args],
                            env=child_env, capture_output=True)
    if result.returncode:
        raise RuntimeError(f"{tool} failed (exit {result.returncode}); raw server output suppressed")


def run_drill(raw, output):
    source = test_connection(raw)
    # psycopg/libpq also consult the process environment, even with an explicit URL.
    for key in list(os.environ):
        if key.startswith("PG"):
            del os.environ[key]
    env = clean_environment()
    output = Path(output)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    archive = output / "database.dump"
    target_name = "perennial_test_restore_" + secrets.token_hex(12)
    target = dict(source, dbname=target_name)
    created = False
    report = {"target_database": target_name, "verified": False, "target_dropped": False}
    with psycopg.connect(**source, autocommit=True) as admin:
        try:
            with psycopg.connect(**source) as snapshot:
                snapshot.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                snapshot.execute("SET TIME ZONE 'UTC'")
                snapshot_id = snapshot.execute("SELECT pg_export_snapshot()").fetchone()[0]
                pg_tool("pg_dump", source, ["--format=custom", "--no-owner", "--no-acl",
                        "--snapshot", snapshot_id, "--file", str(archive)], env)
                expected = table_manifest(snapshot)
            admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(target_name)))
            created = True
            pg_tool("pg_restore", target, ["--exit-on-error", "--single-transaction", "--no-owner",
                    "--no-acl", str(archive)], env)
            with psycopg.connect(**target) as restored:
                restored.execute("SET TIME ZONE 'UTC'")
                actual = table_manifest(restored)
            if actual != expected:
                raise RuntimeError("Restored row counts/content hashes differ from exported snapshot")
            report.update(verified=True, tables=actual)
        finally:
            if created:
                admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(target_name)))
                report["target_dropped"] = True
            (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="New directory for dump and verification report")
    parser.add_argument("--synthetic-only", action="store_true", required=True,
                        help="Attest the source contains only synthetic disposable data")
    args = parser.parse_args()
    try:
        report = run_drill(os.environ.get("TEST_DATABASE_URL", ""), args.output)
    except Exception as exc:
        print(f"Restore drill failed ({type(exc).__name__}); no existing database was replaced", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
