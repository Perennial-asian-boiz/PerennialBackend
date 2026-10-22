"""M3 scheduled logical backup and the timed restore drill; synthetic keys and data only."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

import psycopg
import pytest

OPS = Path(__file__).resolve().parents[2] / "database" / "ops"
sys.path.insert(0, str(OPS))

NOW = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)


def _entry(name, age):
    return {"name": name, "snapshot_time": (NOW - age).isoformat()}


def test_retention_keeps_a_day_of_snapshots_then_one_per_day_for_a_week():
    from backup import retention_plan
    entries = [
        _entry("recent-a", timedelta(minutes=5)),
        _entry("recent-b", timedelta(hours=23)),
        _entry("yesterday-early", timedelta(hours=30)),  # 2026-10-21 06:00
        _entry("yesterday-late", timedelta(hours=25)),   # 2026-10-21 11:00
        _entry("six-days", timedelta(days=6)),
        _entry("eight-days", timedelta(days=8)),
    ]
    keep, delete = retention_plan(entries, NOW)
    # recent-b is also 2026-10-21's newest, so it is that day's daily copy.
    assert keep == {"recent-a", "recent-b", "six-days"}
    assert delete == {"yesterday-early", "yesterday-late", "eight-days"}


def test_retention_never_deletes_the_newest_verified_backup():
    from backup import retention_plan
    keep, delete = retention_plan([_entry("old", timedelta(days=30)),
                                   _entry("older", timedelta(days=31))], NOW)
    assert keep == {"old"} and delete == {"older"}


def test_backup_rejects_unsafe_sources_before_connecting(tmp_path):
    from backup import LocalStore, run_backup
    report = run_backup("postgresql://user@remote.example/perennial_test", tmp_path / "cert.pem",
                        tmp_path / "spool", LocalStore(tmp_path / "store"))
    assert report["status"] == "failed" and report["error_code"] == "unsafe_source"
    assert LocalStore(tmp_path / "store").names() == []


@pytest.fixture(scope="module")
def keypair(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic-backup-keys")
    for name in ("", "other-"):
        subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
             "-subj", "/CN=synthetic-backup-test", "-keyout", str(root / f"{name}key.pem"),
             "-out", str(root / f"{name}cert.pem")], check=True, capture_output=True)
    return root


@pytest.fixture
def loaded(engine, fixture_path):
    from src.ingestion.importer import CollectionOutcome, import_collection
    payload = json.loads(fixture_path("ark_holdings.json").read_text())
    assert import_collection(engine, CollectionOutcome.success(
        "ark_holdings", "fixture", payload["holdings"])).succeeded
    return engine.url.render_as_string(hide_password=False)


def _restore_targets(engine):
    from sqlalchemy import text
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT count(*) FROM pg_database WHERE datname LIKE 'perennial_test_restore_%'"
        )).scalar_one()


@pytest.mark.db
def test_backup_is_encrypted_verified_and_restores_within_rto(engine, loaded, keypair, tmp_path):
    from backup import LocalStore, run_backup
    from restore_drill import run_backup_drill
    store = LocalStore(tmp_path / "store")
    report = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)
    assert report["status"] == "ok", report
    assert report["schema_revision"] and report["snapshot_age_seconds"] >= 0
    name = report["name"]
    assert store.names() == [name + ".dump.cms", name + ".manifest.json"]
    assert list((tmp_path / "spool").iterdir()) == []  # nothing left beside the store
    archive = (store.root / (name + ".dump.cms")).read_bytes()
    assert not archive.startswith(b"PGDMP")  # pg_dump's custom-format magic, never stored
    manifest_text = (store.root / (name + ".manifest.json")).read_text()
    manifest = json.loads(manifest_text)
    assert manifest["tables"]["ark_holdings"]["rows"] > 0
    assert manifest["storage_receipt"]["sha256"] == manifest["archive_sha256"]
    assert str(engine.url.port) not in manifest_text and "postgresql" not in manifest_text

    drill = run_backup_drill(loaded, store, keypair / "key.pem", tmp_path / "drill")
    assert drill["verified"] is True and drill["target_dropped"] is True
    assert drill["backup"] == name and drill["tables"] == manifest["tables"]
    assert drill["within_rto"] is True and drill["rto_target_seconds"] == 3600
    for step in ("retrieve", "decrypt", "restore_and_verify"):
        assert drill[step + "_seconds"] >= 0
    assert _restore_targets(engine) == 0


@pytest.mark.db
def test_tampered_or_wrongly_keyed_archive_is_never_restored(engine, loaded, keypair, tmp_path):
    from backup import LocalStore, run_backup
    from restore_drill import run_backup_drill
    store = LocalStore(tmp_path / "store")
    name = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)["name"]

    with pytest.raises(RuntimeError, match="did not decrypt"):
        run_backup_drill(loaded, store, keypair / "other-key.pem", tmp_path / "wrong-key")
    assert not (tmp_path / "wrong-key" / "database.dump").exists()

    path = store.root / (name + ".dump.cms")
    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 0xFF
    path.write_bytes(bytes(data))
    with pytest.raises(RuntimeError, match="checksum"):
        run_backup_drill(loaded, store, keypair / "key.pem", tmp_path / "tampered")

    # Even with a manifest rewritten to match, AES-GCM authentication rejects it.
    from backup import sha256_file
    manifest_path = store.root / (name + ".manifest.json")
    manifest = json.loads(manifest_path.read_text())
    manifest["archive_sha256"] = sha256_file(path)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError, match="did not decrypt"):
        run_backup_drill(loaded, store, keypair / "key.pem", tmp_path / "forged")
    assert not (tmp_path / "forged" / "database.dump").exists()
    assert _restore_targets(engine) == 0


@pytest.mark.db
def test_overlapping_backup_is_skipped_not_successful(engine, loaded, keypair, tmp_path):
    from backup import BACKUP_LOCK, LocalStore, run_backup
    from restore_drill import test_connection
    store = LocalStore(tmp_path / "store")
    with psycopg.connect(**test_connection(loaded), autocommit=True) as other:
        other.execute("SELECT pg_advisory_lock(%s)", (BACKUP_LOCK,))
        report = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)
    assert report["status"] == "skipped_overlap"
    assert report["error_code"] == "backup_in_progress"
    assert store.names() == []


@pytest.mark.db
def test_failed_upload_keeps_the_last_verified_backup(engine, loaded, keypair, tmp_path):
    from backup import LocalStore, run_backup
    store = LocalStore(tmp_path / "store")
    first = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)
    before = store.names()

    class FailingStore(LocalStore):
        def put(self, name, path):
            raise OSError("synthetic upload outage https://store.invalid/SECRET")

    report = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool",
                        FailingStore(tmp_path / "store"))
    assert report["status"] == "failed" and report["error_code"] == "upload_failed"
    assert "SECRET" not in json.dumps(report)
    assert store.names() == before and first["status"] == "ok"
    assert list((tmp_path / "spool").iterdir()) == []


@pytest.mark.db
def test_backup_applies_retention_to_stale_verified_sets(engine, loaded, keypair, tmp_path):
    from backup import LocalStore, run_backup
    store = LocalStore(tmp_path / "store")
    for name, age in (("perennial-stale-a", 9), ("perennial-stale-b", 10)):
        taken = (datetime.now(timezone.utc) - timedelta(days=age)).isoformat()
        (store.root / f"{name}.dump.cms").write_bytes(b"synthetic")
        (store.root / f"{name}.manifest.json").write_text(
            json.dumps({"name": name, "snapshot_time": taken}))
    report = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)
    assert report["status"] == "ok"
    assert report["deleted"] == ["perennial-stale-a", "perennial-stale-b"]
    assert store.names() == [report["name"] + ".dump.cms", report["name"] + ".manifest.json"]


@pytest.mark.db
def test_backup_and_drill_command_lines(engine, loaded, keypair, tmp_path):
    env = dict(os.environ, TEST_DATABASE_URL=loaded)
    store = tmp_path / "store"
    backup = subprocess.run(
        [sys.executable, str(OPS / "backup.py"), "--synthetic-only", "--recipient",
         str(keypair / "cert.pem"), "--spool", str(tmp_path / "spool"), "--store", str(store)],
        env=env, capture_output=True, text=True)
    assert backup.returncode == 0, backup.stdout
    assert json.loads(backup.stdout)["status"] == "ok"
    drill = subprocess.run(
        [sys.executable, str(OPS / "restore_drill.py"), "--synthetic-only", "--from-store",
         str(store), "--key", str(keypair / "key.pem"), "--output", str(tmp_path / "drill")],
        env=env, capture_output=True, text=True)
    assert drill.returncode == 0, drill.stderr
    report = json.loads(drill.stdout)
    assert report["verified"] and report["within_rto"] and report["target_dropped"]
    assert json.loads((tmp_path / "drill" / "report.json").read_text()) == report

    with psycopg.connect(loaded.replace("postgresql+psycopg", "postgresql"), autocommit=True) as other:
        from backup import BACKUP_LOCK
        other.execute("SELECT pg_advisory_lock(%s)", (BACKUP_LOCK,))
        skipped = subprocess.run(
            [sys.executable, str(OPS / "backup.py"), "--synthetic-only", "--recipient",
             str(keypair / "cert.pem"), "--spool", str(tmp_path / "spool"), "--store",
             str(store)], env=env, capture_output=True, text=True)
    assert skipped.returncode == 3
    assert json.loads(skipped.stdout)["status"] == "skipped_overlap"


@pytest.mark.db
def test_truncated_dump_fails_even_though_encryption_succeeds(
    engine, loaded, keypair, tmp_path, monkeypatch
):
    from backup import LocalStore, run_backup
    fake = tmp_path / "bin"
    fake.mkdir()
    (fake / "pg_dump").write_text("#!/bin/sh\nprintf 'PGDMP-partial'\nexit 1\n")
    (fake / "pg_dump").chmod(0o755)
    monkeypatch.setenv("PATH", f"{fake}:{os.environ['PATH']}")
    store = LocalStore(tmp_path / "store")
    report = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)
    assert report["status"] == "failed" and report["error_code"] == "dump_failed"
    assert store.names() == [] and list((tmp_path / "spool").iterdir()) == []
