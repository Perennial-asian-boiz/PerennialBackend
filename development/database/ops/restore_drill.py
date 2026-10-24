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


def bypasses_row_security(conn):
    """False if any table has row security and this login is neither superuser nor BYPASSRLS.

    Such a login is filtered by every RLS-enabled table it does not own, so a dump
    fails partway (pg_dump runs with row_security off) and a verification would
    count only the rows its policies allow. Callers refuse before starting (PK-9).
    """
    return conn.execute(
        "SELECT NOT EXISTS (SELECT 1 FROM pg_class WHERE relrowsecurity)"
        " OR (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user)").fetchone()[0]


def table_manifest(conn):
    """Hash all application rows in stable order using bounded client memory.

    Tables in schema perennial are keyed by name, those in app (accounts, #22) as
    "app.<table>". With row_security off, a login that RLS would filter fails here
    instead of verifying fewer rows.
    """
    conn.execute("SET row_security = off")
    tables = conn.execute("SELECT schemaname, tablename FROM pg_tables WHERE schemaname IN ('perennial', 'app') "
                          "ORDER BY schemaname, tablename").fetchall()
    if not any(schema == "perennial" for schema, _ in tables):
        raise ValueError("No perennial application tables to verify")
    result = {}
    for schema, table in tables:
        digest, count = hashlib.sha256(), 0
        with conn.cursor(name="drill_manifest") as rows:
            rows.execute(sql.SQL("SELECT row_to_json(t)::text FROM {} t ORDER BY row_to_json(t)::text COLLATE \"C\"")
                         .format(sql.Identifier(schema, table)))
            for (row,) in rows:
                data = row.encode()
                digest.update(len(data).to_bytes(8, "big"))
                digest.update(data)
                count += 1
        result[table if schema == "perennial" else f"{schema}.{table}"] = {"rows": count, "sha256": digest.hexdigest()}
    invalid = conn.execute("SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace "
                           "WHERE n.nspname IN ('perennial', 'app') AND NOT c.convalidated").fetchone()[0]
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


def provision_target(target, roles, phase):
    """The restore runbook's provisioning, run as the drill (administrative) login.

    Phase roles before pg_restore creates the group roles the dump's policies
    name. Phase grants after it re-owns and re-grants perennial, then
    provision_api_role.py does the same for app, policies included.
    """
    import provision_api_role
    import provision_roles

    with psycopg.connect(**target) as conn:
        provision_roles.provision(conn, roles, phase=phase)
        if phase == "grants" and conn.execute("SELECT to_regnamespace('app')").fetchone()[0]:
            provision_api_role.provision(conn, roles[3], owner=roles[0])


def restore_into_disposable(admin, source, expected, report, restore, roles=None):
    """Restore into a new random database, compare, and always drop only that database.

    restore(target) loads the new database given its connection parameters. With
    roles (owner, importer, reader, api) it follows the restore order: the roles
    phase, the restore, then the grants; without them the roles must already exist.
    """
    target_name = "perennial_test_restore_" + secrets.token_hex(12)
    target = dict(source, dbname=target_name)
    report.update(target_database=target_name, verified=False, target_dropped=False)
    created = False
    try:
        admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(target_name)))
        created = True
        if roles:
            provision_target(target, roles, "roles")
        restore(target)
        if roles:
            provision_target(target, roles, "grants")
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


ROW_SECURITY_REFUSAL = "Tables have row security; dump and restore need a superuser or BYPASSRLS login"


def run_drill(raw, output, roles=None):
    source = test_connection(raw)
    env = _clear_libpq_environment()
    output = Path(output)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    archive = output / "database.dump"
    report = {"verified": False, "target_dropped": False}
    with psycopg.connect(**source, autocommit=True) as admin:
        try:
            if not bypasses_row_security(admin):
                raise RuntimeError(ROW_SECURITY_REFUSAL)
            with psycopg.connect(**source) as snapshot:
                snapshot.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                snapshot.execute("SET TIME ZONE 'UTC'")
                snapshot_id = snapshot.execute("SELECT pg_export_snapshot()").fetchone()[0]
                pg_tool("pg_dump", source, ["--format=custom", "--no-owner", "--no-acl",
                        "--snapshot", snapshot_id, "--file", str(archive)], env)
                expected = table_manifest(snapshot)
            restore_into_disposable(admin, source, expected, report, lambda target: pg_tool(
                "pg_restore", target, ["--exit-on-error", "--single-transaction", "--no-owner",
                                       "--no-acl", str(archive)], env), roles)
        finally:
            (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


RESTORE_ARGS = ["--exit-on-error", "--single-transaction", "--no-owner", "--no-acl"]


def decrypt_into(target, encrypted, key, env):
    """openssl cms -decrypt stdout -> pg_restore stdin; plaintext never touches disk.

    openssl buffers the whole archive and checks the GCM tag before writing
    any output, so an archive that fails authentication passes no bytes on.
    Only the decryption child sees BACKUP_KEY_PASSPHRASE.
    """
    args = ["openssl", "cms", "-decrypt", "-binary", "-inform", "DER", "-in", str(encrypted),
            "-inkey", str(key)]
    if env.get("BACKUP_KEY_PASSPHRASE") is not None:
        args += ["-passin", "env:BACKUP_KEY_PASSPHRASE"]
    restore_env = {k: v for k, v in env.items() if k != "BACKUP_KEY_PASSPHRASE"}
    restore_env["PGPASSWORD"] = target["password"]
    public = {k: v for k, v in target.items() if k != "password"}
    children = []
    try:
        decrypt = subprocess.Popen(args, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.DEVNULL)
        children.append(decrypt)
        restore = subprocess.Popen(
            ["pg_restore", "--no-password", "--dbname", make_conninfo(**public), *RESTORE_ARGS],
            env=restore_env, stdin=decrypt.stdout, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL)
        children.append(restore)
        decrypt.stdout.close()  # pg_restore holds the read end now
        restored, decrypted = restore.wait(), decrypt.wait()
    finally:
        for proc in children:  # interruption: never leave a child holding plaintext
            if proc.poll() is None:
                proc.kill()
                proc.wait()
    if decrypted:
        raise RuntimeError("Archive did not decrypt and authenticate; nothing was restored")
    if restored:
        raise RuntimeError(f"pg_restore failed (exit {restored}); raw server output suppressed")


def run_backup_drill(raw, store, key, output, manifest_name=None, incident_start=None, roles=None):
    """Timed restore of a stored backup (backup.py) against the D6 RTO/RPO targets.

    The clock starts at incident_start (default: now) and covers retrieval,
    checksum, decryption, restore and verification. Only report.json is left
    in the new private output directory: the decrypted dump is streamed into
    pg_restore, and the copied encrypted archive is removed on every path. The
    restore and the row verification (app included, decision 32) run as this
    login, which must be the administrative one; roles as in restore_into_disposable.
    """
    from backup import ARCHIVE_SUFFIX, BACKUP_NAME, MANIFEST_SUFFIX, manifests, require_openssl3
    from backup import sha256_file

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
    encrypted = output / ("archive" + ARCHIVE_SUFFIX)  # fixed name, never manifest-chosen
    report = {"verified": False, "target_dropped": False, "within_rto": False}
    try:
        require_openssl3(env)
        found = manifests(store)
        if manifest_name is not None:
            wanted = manifest_name.removesuffix(MANIFEST_SUFFIX)
            if not BACKUP_NAME.fullmatch(wanted):
                raise ValueError("Backup name is not a canonical backup.py name")
            found = [m for m in found if m["name"] == wanted]
        if not found:
            raise RuntimeError("No verified backup manifest in the store")
        manifest = max(found, key=lambda m: m["snapshot_time"])
        report.update(backup=manifest["name"], snapshot_time=manifest["snapshot_time"],
                      schema_revision=manifest.get("schema_revision"))
        store.get(manifest["archive"], encrypted)
        if sha256_file(encrypted) != manifest["archive_sha256"]:
            raise RuntimeError("Stored archive checksum differs from its manifest")
        mark("retrieve")
        with psycopg.connect(**source, autocommit=True) as admin:
            if not bypasses_row_security(admin):
                raise RuntimeError(ROW_SECURITY_REFUSAL)
            restore_into_disposable(admin, source, manifest["tables"], report,
                                    lambda target: decrypt_into(target, encrypted, key, env), roles)
        mark("decrypt_restore_and_verify")
        rto = (datetime.now(timezone.utc) - incident_start).total_seconds()
        rpo = (incident_start - datetime.fromisoformat(manifest["snapshot_time"])).total_seconds()
        report.update(marks, rto_seconds=round(rto, 3), rto_target_seconds=RTO_SECONDS,
                      within_rto=report["verified"] and rto <= RTO_SECONDS,
                      rpo_seconds=round(rpo, 3), rpo_target_seconds=RPO_SECONDS,
                      within_rpo=rpo <= RPO_SECONDS)
    finally:
        encrypted.unlink(missing_ok=True)
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
    parser.add_argument("--roles", nargs=4, metavar=("OWNER", "IMPORTER", "READER", "API"),
                        help="Provision these group roles before and after the restore, as the runbook does")
    args = parser.parse_args()
    if bool(args.from_store) != bool(args.key):
        parser.error("--from-store and --key must be given together")
    raw = os.environ.get("TEST_DATABASE_URL", "")
    try:
        if args.from_store:
            from backup import LocalStore
            report = run_backup_drill(raw, LocalStore(args.from_store), args.key, args.output,
                                      manifest_name=args.manifest, roles=args.roles)
        else:
            report = run_drill(raw, args.output, roles=args.roles)
    except Exception as exc:
        print(f"Restore drill failed ({type(exc).__name__}); no existing database was replaced", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2))
    return 0 if report["verified"] else 1


if __name__ == "__main__":
    sys.exit(main())
