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
import re
import stat
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
ORPHAN_GRACE = timedelta(hours=1)  # an archive with no manifest after this is swept
ARCHIVE_SUFFIX, MANIFEST_SUFFIX = ".dump.cms", ".manifest.json"
ENCRYPTION = "openssl-cms-aes-256-gcm"
# Every stored name is built from this; anything else in a store is never acted on.
BACKUP_NAME = re.compile(r"perennial-(\d{8}T\d{6}Z)-[0-9a-f]{6}")
STORE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
MAX_MANIFEST_BYTES = 1 << 20
_HEX64 = re.compile(r"[0-9a-f]{64}")


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


def private_dir(path):
    """Create or tighten a directory to 0700; refuse a symlink in its place."""
    path = Path(path)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.is_symlink() or not path.is_dir():
        raise BackupError("unsafe_directory")
    os.chmod(path, 0o700)
    return path.resolve()


class LocalStore:
    """Stand-in for off-site storage: a directory outside the spool.

    A real uploader keeps the same four calls and returns a receipt with the
    provider's own checksum of the stored object. Every name is a plain
    basename confined to the root; symlinks and other non-regular entries are
    neither listed nor followed.
    """

    kind = "local-stub"

    def __init__(self, root):
        self.root = private_dir(root)

    def _path(self, name):
        if not isinstance(name, str) or not STORE_NAME.fullmatch(name):
            raise ValueError("unsafe store object name")
        path = self.root / name
        if path.parent != self.root:
            raise ValueError("unsafe store object name")
        return path

    def _regular(self, path):
        try:
            mode = os.lstat(path).st_mode
        except FileNotFoundError:
            return False
        if not stat.S_ISREG(mode):
            raise ValueError("store object is not a regular file")
        return True

    def put(self, name, path):
        stored = self._path(name)
        if stored.is_symlink():
            raise ValueError("store object is not a regular file")
        partial = self.root / f".{name}.partial"
        partial.unlink(missing_ok=True)
        with open(path, "rb") as src, os.fdopen(os.open(
                partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), "wb") as dst:
            shutil.copyfileobj(src, dst)
        os.replace(partial, stored)
        return {"store": self.kind, "name": name, "bytes": stored.stat().st_size,
                "sha256": sha256_file(stored)}

    def open(self, name):
        path = self._path(name)
        if not self._regular(path):
            raise FileNotFoundError(name)
        return open(path, "rb", opener=lambda p, flags: os.open(p, flags | os.O_NOFOLLOW))

    def get(self, name, dest):
        with self.open(name) as src, os.fdopen(os.open(
                dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), "wb") as dst:
            shutil.copyfileobj(src, dst)

    def names(self):
        found = []
        for entry in os.scandir(self.root):
            if STORE_NAME.fullmatch(entry.name) and entry.is_file(follow_symlinks=False):
                found.append(entry.name)
        return sorted(found)

    def delete(self, name):
        path = self._path(name)
        if self._regular(path):
            path.unlink()


def _backup_time(name):
    match = BACKUP_NAME.fullmatch(name)
    if match is None:
        return None
    return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def validate_manifest(filename, data):
    """The manifest if it is well formed and names only itself; otherwise None."""
    name = filename[: -len(MANIFEST_SUFFIX)] if filename.endswith(MANIFEST_SUFFIX) else None
    if name is None or _backup_time(name) is None or not isinstance(data, dict):
        return None
    try:
        taken = datetime.fromisoformat(data["snapshot_time"])
        tables = data["tables"]
        ok = (
            data.get("format") == 1 and data["name"] == name
            and data["archive"] == name + ARCHIVE_SUFFIX and taken.tzinfo is not None
            and isinstance(data["archive_sha256"], str) and _HEX64.fullmatch(data["archive_sha256"])
            and isinstance(tables, dict) and tables
            and all(isinstance(t, str) and isinstance(v, dict) and set(v) == {"rows", "sha256"}
                    and type(v["rows"]) is int and v["rows"] >= 0
                    and isinstance(v["sha256"], str) and _HEX64.fullmatch(v["sha256"])
                    for t, v in tables.items()))
    except (KeyError, TypeError, ValueError):
        return None
    return data if ok else None


def manifests(store, invalid=None):
    """Valid manifests in the store; names of malformed ones go to `invalid`."""
    found = []
    for filename in store.names():
        if not filename.endswith(MANIFEST_SUFFIX):
            continue
        try:
            with store.open(filename) as f:
                raw = f.read(MAX_MANIFEST_BYTES + 1)
            data = json.loads(raw) if len(raw) <= MAX_MANIFEST_BYTES else None
        except (OSError, ValueError):
            data = None
        manifest = validate_manifest(filename, data)
        if manifest is None:
            if invalid is not None:
                invalid.append(filename)
        else:
            found.append(manifest)
    return found


def orphan_archives(store, verified, now):
    """Archives with no valid manifest whose name dates them past ORPHAN_GRACE."""
    names = {m["name"] for m in verified}
    orphans = []
    for filename in store.names():
        if not filename.endswith(ARCHIVE_SUFFIX):
            continue
        name = filename[: -len(ARCHIVE_SUFFIX)]
        taken = _backup_time(name)
        if taken is not None and name not in names and now - taken > ORPHAN_GRACE:
            orphans.append(filename)
    return sorted(orphans)


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


def require_openssl3(env):
    """CMS AES-GCM needs OpenSSL 3; macOS's /usr/bin/openssl is LibreSSL."""
    out = subprocess.run(["openssl", "version"], env=env, capture_output=True, text=True)
    if out.returncode or not out.stdout.startswith("OpenSSL 3."):
        raise BackupError("openssl_unsupported")


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
        spool = private_dir(spool)
        require_openssl3(env)
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
                    try:
                        store.delete(name + ARCHIVE_SUFFIX)  # unverified; never kept
                    except Exception:
                        pass  # left as an orphan, swept after ORPHAN_GRACE
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
                    invalid = []
                    verified = manifests(store, invalid)
                    if name not in {m["name"] for m in verified}:
                        raise BackupError("upload_verify_failed")
                    keep, delete = retention_plan(verified, completed)
                    for old in sorted(delete):
                        # Manifest first: an interruption leaves an orphan archive,
                        # never a manifest whose archive is gone.
                        store.delete(old + MANIFEST_SUFFIX)
                        store.delete(old + ARCHIVE_SUFFIX)
                    swept = orphan_archives(store, verified, completed)
                    for orphan in swept:
                        store.delete(orphan)
                except BackupError:
                    raise
                except Exception:
                    # The new backup is verified and stored; storage growth must alert.
                    raise BackupError("retention_failed") from None
                # Malformed manifests are never acted on; they are reported for a human.
                report.update(status="ok", retained=len(keep), deleted=sorted(delete),
                              orphans_swept=swept, invalid_manifests=sorted(invalid))
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
    try:
        store = LocalStore(args.store)
    except BackupError as exc:
        report = {"status": "failed", "error_code": exc.code}
    else:
        report = run_backup(os.environ.get("TEST_DATABASE_URL", ""), args.recipient, args.spool,
                            store)
    print(json.dumps(report, indent=2))
    return {"ok": 0, "skipped_overlap": 3}.get(report["status"], 1)


if __name__ == "__main__":
    sys.exit(main())
