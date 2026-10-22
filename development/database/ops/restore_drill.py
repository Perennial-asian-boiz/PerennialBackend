"""Dump (or fetch a backup.py archive), restore and verify a synthetic local test database.

Never restores over an existing database: every restore targets a new random one.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from sqlalchemy.engine import make_url

LOOPBACK = {"localhost": "127.0.0.1", "127.0.0.1": "127.0.0.1", "::1": "::1"}
RTO_SECONDS, RPO_SECONDS = 3600, 900  # D6 targets, not measured claims


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


def _clear_libpq_environment():
    # psycopg/libpq also consult the process environment, even with an explicit URL.
    for key in list(os.environ):
        if key.startswith("PG"):
            del os.environ[key]
    return clean_environment()


def restore_into_disposable(admin, source, archive, expected, env, report):
    """Restore into a new random database, compare, and always drop only that database."""
    target_name = "perennial_test_restore_" + secrets.token_hex(12)
    target = dict(source, dbname=target_name)
    report.update(target_database=target_name, verified=False, target_dropped=False)
    created = False
    try:
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


def run_drill(raw, output):
    source = test_connection(raw)
    env = _clear_libpq_environment()
    output = Path(output)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    archive = output / "database.dump"
    report = {"verified": False, "target_dropped": False}
    with psycopg.connect(**source, autocommit=True) as admin:
        try:
            with psycopg.connect(**source) as snapshot:
                snapshot.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                snapshot.execute("SET TIME ZONE 'UTC'")
                snapshot_id = snapshot.execute("SELECT pg_export_snapshot()").fetchone()[0]
                pg_tool("pg_dump", source, ["--format=custom", "--no-owner", "--no-acl",
                        "--snapshot", snapshot_id, "--file", str(archive)], env)
                expected = table_manifest(snapshot)
            restore_into_disposable(admin, source, archive, expected, env, report)
        finally:
            (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def run_backup_drill(raw, store, key, output, manifest_name=None, incident_start=None):
    """Timed restore of a stored backup (backup.py) against the D6 RTO/RPO targets.

    The clock starts at incident_start (default: now) and covers retrieval,
    checksum, decryption, restore and verification. Decrypted bytes stay in the
    new private output directory; a failed decryption is never restored.
    """
    from backup import MANIFEST_SUFFIX, manifests, sha256_file

    incident_start = incident_start or datetime.now(timezone.utc)
    clock = time.monotonic()
    marks = {}

    def mark(step):
        nonlocal clock
        now = time.monotonic()
        marks[step + "_seconds"] = round(now - clock, 3)
        clock = now

    source = test_connection(raw)
    env = _clear_libpq_environment()
    output = Path(output)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    report = {"verified": False, "target_dropped": False, "within_rto": False}
    try:
        found = manifests(store)
        if manifest_name is not None:
            found = [m for m in found if m["name"] + MANIFEST_SUFFIX == manifest_name
                     or m["name"] == manifest_name]
        if not found:
            raise RuntimeError("No verified backup manifest in the store")
        manifest = max(found, key=lambda m: m["snapshot_time"])
        report.update(backup=manifest["name"], snapshot_time=manifest["snapshot_time"],
                      schema_revision=manifest["schema_revision"])
        encrypted, archive = output / manifest["archive"], output / "database.dump"
        store.get(manifest["archive"], encrypted)
        if sha256_file(encrypted) != manifest["archive_sha256"]:
            raise RuntimeError("Stored archive checksum differs from its manifest")
        mark("retrieve")
        args = ["openssl", "cms", "-decrypt", "-binary", "-inform", "DER", "-in", str(encrypted),
                "-inkey", str(key), "-out", str(archive)]
        if os.environ.get("BACKUP_KEY_PASSPHRASE") is not None:
            args[-2:-2] = ["-passin", "env:BACKUP_KEY_PASSPHRASE"]
        if subprocess.run(args, env=env, capture_output=True).returncode:
            archive.unlink(missing_ok=True)
            raise RuntimeError("Archive did not decrypt and authenticate; nothing was restored")
        mark("decrypt")
        with psycopg.connect(**source, autocommit=True) as admin:
            restore_into_disposable(admin, source, archive, manifest["tables"], env, report)
        mark("restore_and_verify")
        rto = (datetime.now(timezone.utc) - incident_start).total_seconds()
        rpo = (incident_start - datetime.fromisoformat(manifest["snapshot_time"])).total_seconds()
        report.update(marks, rto_seconds=round(rto, 3), rto_target_seconds=RTO_SECONDS,
                      within_rto=report["verified"] and rto <= RTO_SECONDS,
                      rpo_seconds=round(rpo, 3), rpo_target_seconds=RPO_SECONDS,
                      within_rpo=rpo <= RPO_SECONDS)
    finally:
        report.update(marks)
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="New directory for dump and verification report")
    parser.add_argument("--synthetic-only", action="store_true", required=True,
                        help="Attest the source contains only synthetic disposable data")
    parser.add_argument("--from-store", help="Restore the newest verified backup.py archive in this store")
    parser.add_argument("--key", help="PEM private key for --from-store archives")
    parser.add_argument("--manifest", help="A specific backup name instead of the newest")
    args = parser.parse_args()
    if bool(args.from_store) != bool(args.key):
        parser.error("--from-store and --key must be given together")
    raw = os.environ.get("TEST_DATABASE_URL", "")
    try:
        if args.from_store:
            from backup import LocalStore
            report = run_backup_drill(raw, LocalStore(args.from_store), args.key, args.output,
                                      manifest_name=args.manifest)
        else:
            report = run_drill(raw, args.output)
    except Exception as exc:
        print(f"Restore drill failed ({type(exc).__name__}); no existing database was replaced", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2))
    return 0 if report["verified"] else 1


if __name__ == "__main__":
    sys.exit(main())
