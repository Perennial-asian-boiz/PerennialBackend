"""Scheduled logical backup of a synthetic local test database (M3, D6/D14).

One run: take a repeatable-read snapshot, stream a custom-format pg_dump of it
straight into OpenSSL CMS public-key encryption (AES-256-GCM, so no plaintext
dump touches disk and the backup host never holds the decryption key), upload
the archive, verify the stored checksum, then write its manifest and apply
retention. A manifest exists only for a verified archive.

Only one backup runs at a time (advisory lock); an overlapping run reports
skipped_overlap and exits non-zero, so the scheduler alerts instead of
calling it success. Exit codes: 0 ok, 1 failed, 3 skipped_overlap.

The upload target is a stub until the Supabase project exists: --store names a
local directory standing in for off-site storage. Connection guards are the
restore drill's: loopback test URLs only, with --synthetic-only attested.
"""

import argparse
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from restore_drill import clean_environment, table_manifest, test_connection

BACKUP_LOCK = 170097  # beside source (170010+), pipeline (170098), publication (170099)
SNAPSHOT_RETENTION = timedelta(hours=24)  # every snapshot (OPERATIONS.md, proposed)
DAILY_RETENTION_DAYS = 7  # then the newest snapshot of each UTC day
ARCHIVE_SUFFIX, MANIFEST_SUFFIX = ".dump.cms", ".manifest.json"
ENCRYPTION = "openssl-cms-aes-256-gcm"


class BackupError(Exception):
    """A failed step, reported by fixed code; tool output is never surfaced."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


class LocalStore:
    """Stand-in for off-site storage: a directory outside the spool.

    A real uploader keeps the same four calls and returns a receipt with the
    provider's own checksum of the stored object.
    """

    kind = "local-stub"

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)

    def put(self, name, path):
        partial = self.root / f".{name}.partial"
        shutil.copyfile(path, partial)
        os.replace(partial, self.root / name)
        stored = self.root / name
        return {"store": self.kind, "name": name, "bytes": stored.stat().st_size,
                "sha256": sha256_file(stored)}

    def get(self, name, dest):
        shutil.copyfile(self.root / name, dest)

    def names(self):
        return sorted(p.name for p in self.root.iterdir() if not p.name.startswith("."))

    def delete(self, name):
        (self.root / name).unlink(missing_ok=True)


def manifests(store):
    found = []
    for name in store.names():
        if name.endswith(MANIFEST_SUFFIX):
            with open(store.root / name) as f:
                found.append(json.load(f))
    return found


def retention_plan(entries, now):
    """Names of verified backups to keep and to delete.

    Keeps every snapshot from the last SNAPSHOT_RETENTION, the newest snapshot
    of each of the last DAILY_RETENTION_DAYS UTC days, and always the newest
    verified backup, however old.
    """
    by_time = sorted(entries, key=lambda m: m["snapshot_time"], reverse=True)
    keep = {m["name"] for m in by_time[:1]}
    oldest_day = (now - timedelta(days=DAILY_RETENTION_DAYS)).date()
    days = set()
    for m in by_time:
        taken = datetime.fromisoformat(m["snapshot_time"])
        if now - taken <= SNAPSHOT_RETENTION:
            keep.add(m["name"])
        if taken.date() > oldest_day and taken.date() not in days:
            days.add(taken.date())
            keep.add(m["name"])
    return keep, {m["name"] for m in entries} - keep


def schema_revision(conn):
    row = conn.execute(
        "SELECT table_schema FROM information_schema.tables WHERE table_name='alembic_version' "
        "ORDER BY table_schema LIMIT 1").fetchone()
    if row is None:
        return None
    return conn.execute(sql.SQL("SELECT version_num FROM {}").format(
        sql.Identifier(row[0], "alembic_version"))).fetchone()[0]


def dump_encrypted(params, snapshot_id, recipient, archive, env):
    """pg_dump stdout -> openssl cms stdin -> archive; neither tool's output is kept."""
    child_env = dict(env, PGPASSWORD=params["password"])
    public = {k: v for k, v in params.items() if k != "password"}
    dump = subprocess.Popen(
        ["pg_dump", "--no-password", "--dbname", make_conninfo(**public), "--format=custom",
         "--no-owner", "--no-acl", "--snapshot", snapshot_id],
        env=child_env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        encrypt = subprocess.Popen(
            ["openssl", "cms", "-encrypt", "-binary", "-stream", "-aes-256-gcm", "-outform", "DER",
             "-out", str(archive), str(recipient)],
            env=env, stdin=dump.stdout, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except BaseException:
        dump.kill()
        dump.wait()
        raise
    finally:
        dump.stdout.close()  # openssl holds the read end; pg_dump sees EPIPE if it exits
    encrypted, dumped = encrypt.wait(), dump.wait()
    if dumped:
        raise BackupError("dump_failed")  # a truncated stream can still encrypt cleanly
    if encrypted:
        raise BackupError("encrypt_failed")
    check = subprocess.run(["openssl", "cms", "-cmsout", "-inform", "DER", "-noout", "-in",
                            str(archive)], env=env, capture_output=True)
    if check.returncode:
        raise BackupError("encrypt_failed")


def recipient_fingerprint(recipient, env):
    out = subprocess.run(["openssl", "x509", "-noout", "-fingerprint", "-sha256", "-in",
                          str(recipient)], env=env, capture_output=True, text=True)
    if out.returncode:
        raise BackupError("recipient_unreadable")
    return out.stdout.strip().partition("=")[2]


def run_backup(raw, recipient, spool, store, now=datetime.now):
    started = time.monotonic()
    report = {"status": "failed", "error_code": None}
    try:
        try:
            source = test_connection(raw)
        except ValueError:
            raise BackupError("unsafe_source") from None
        for key in list(os.environ):  # psycopg/libpq also read PG* defaults
            if key.startswith("PG"):
                del os.environ[key]
        env = clean_environment()
        spool = Path(spool)
        spool.mkdir(mode=0o700, parents=True, exist_ok=True)
        fingerprint = recipient_fingerprint(recipient, env)
        with psycopg.connect(**source, autocommit=True) as admin:
            if not admin.execute("SELECT pg_try_advisory_lock(%s)", (BACKUP_LOCK,)).fetchone()[0]:
                report.update(status="skipped_overlap", error_code="backup_in_progress")
                return report
            name = archive = None
            try:
                with psycopg.connect(**source) as snapshot:
                    snapshot.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                    snapshot.execute("SET TIME ZONE 'UTC'")
                    # The first statement fixes the snapshot; clock_timestamp() dates it.
                    snapshot_id, taken = snapshot.execute(
                        "SELECT pg_export_snapshot(), clock_timestamp()").fetchone()
                    name = f"perennial-{taken:%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"
                    archive = spool / (name + ARCHIVE_SUFFIX)
                    dump_encrypted(source, snapshot_id, recipient, archive, env)
                    tables = table_manifest(snapshot)
                    revision = schema_revision(snapshot)
                local_sha = sha256_file(archive)
                try:
                    receipt = store.put(name + ARCHIVE_SUFFIX, archive)
                except Exception:
                    raise BackupError("upload_failed") from None
                if receipt["sha256"] != local_sha:
                    raise BackupError("upload_verify_failed")
                completed = now(timezone.utc)
                manifest = {
                    "format": 1, "name": name, "archive": name + ARCHIVE_SUFFIX,
                    "snapshot_time": taken.isoformat(), "completed_time": completed.isoformat(),
                    "schema_revision": revision, "archive_sha256": local_sha,
                    "archive_bytes": archive.stat().st_size, "encryption": ENCRYPTION,
                    "recipient_sha256_fingerprint": fingerprint, "storage_receipt": receipt,
                    "tables": tables,
                }
                staged = spool / (name + MANIFEST_SUFFIX)
                staged.write_text(json.dumps(manifest, indent=2) + "\n")
                try:
                    store.put(name + MANIFEST_SUFFIX, staged)
                except Exception:
                    raise BackupError("upload_failed") from None
                report.update(
                    name=name, snapshot_time=manifest["snapshot_time"],
                    completed_time=manifest["completed_time"],
                    snapshot_age_seconds=round((completed - taken).total_seconds(), 3),
                    archive_bytes=manifest["archive_bytes"], schema_revision=revision)
                try:
                    keep, delete = retention_plan(manifests(store), completed)
                    for old in sorted(delete):
                        # Manifest first: an interruption leaves an orphan archive,
                        # never a manifest whose archive is gone.
                        store.delete(old + MANIFEST_SUFFIX)
                        store.delete(old + ARCHIVE_SUFFIX)
                except Exception:
                    # The new backup is verified and stored; storage growth must alert.
                    raise BackupError("retention_failed") from None
                report.update(status="ok", retained=len(keep), deleted=sorted(delete))
            finally:
                for leftover in spool.glob(f"{name}*") if name else ():
                    leftover.unlink(missing_ok=True)
                admin.execute("SELECT pg_advisory_unlock(%s)", (BACKUP_LOCK,))
    except BackupError as exc:
        report["error_code"] = exc.code
    except Exception as exc:
        report["error_code"] = "backup_failed"
        report["exception"] = type(exc).__name__
    finally:
        report["duration_seconds"] = round(time.monotonic() - started, 3)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--recipient", required=True, help="PEM certificate to encrypt for")
    parser.add_argument("--spool", required=True, help="Private working directory")
    parser.add_argument("--store", required=True, help="Directory standing in for off-site storage")
    parser.add_argument("--synthetic-only", action="store_true", required=True,
                        help="Attest the source contains only synthetic disposable data")
    args = parser.parse_args()
    report = run_backup(os.environ.get("TEST_DATABASE_URL", ""), args.recipient, args.spool,
                        LocalStore(args.store))
    print(json.dumps(report, indent=2))
    return {"ok": 0, "skipped_overlap": 3}.get(report["status"], 1)


if __name__ == "__main__":
    sys.exit(main())
