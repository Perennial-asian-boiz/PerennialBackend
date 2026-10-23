"""M3 scheduled logical backup and the timed restore drill; synthetic keys and data only."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import psycopg
import pytest

OPS = Path(__file__).resolve().parents[2] / "database" / "ops"
sys.path.insert(0, str(OPS))

NOW = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)
HEX = "0" * 64


def _entry(name, age):
    return {"name": name, "snapshot_time": (NOW - age).isoformat()}


def _name(taken, suffix="aaaaaa"):
    return f"perennial-{taken:%Y%m%dT%H%M%SZ}-{suffix}"


def _write_set(store, taken, suffix="aaaaaa", **overrides):
    """A well-formed stored backup set (synthetic archive bytes)."""
    name = _name(taken, suffix)
    (store.root / f"{name}.dump.cms").write_bytes(b"synthetic")
    manifest = {"format": 1, "name": name, "archive": f"{name}.dump.cms",
                "snapshot_time": taken.isoformat(), "archive_sha256": HEX,
                "tables": {"ark_holdings": {"rows": 1, "sha256": HEX}}, **overrides}
    (store.root / f"{name}.manifest.json").write_text(json.dumps(manifest))
    return name


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


# ── SEC-BACKUP-01: names from manifests and stores never escape the store ──

@pytest.mark.parametrize("name", [
    "../outside", "/etc/passwd", "a/b", "..", ".", "", ".hidden", "x" * 200, None, "a\x00b",
])
def test_store_refuses_names_that_are_not_plain_basenames(tmp_path, name):
    from backup import LocalStore
    store = LocalStore(tmp_path / "store")
    (tmp_path / "src").write_bytes(b"synthetic")
    for call in (lambda: store.put(name, tmp_path / "src"), lambda: store.delete(name),
                 lambda: store.get(name, tmp_path / "dest"), lambda: store.open(name)):
        with pytest.raises(ValueError):
            call()
    assert not (tmp_path / "dest").exists()


def test_store_ignores_and_never_follows_symlinks(tmp_path):
    from backup import LocalStore
    store = LocalStore(tmp_path / "store")
    victim = tmp_path / "victim.dump.cms"
    victim.write_bytes(b"sentinel")
    link = _name(NOW - timedelta(days=30)) + ".dump.cms"
    (store.root / link).symlink_to(victim)
    assert store.names() == []
    for call in (lambda: store.delete(link), lambda: store.open(link),
                 lambda: store.put(link, victim)):
        with pytest.raises(ValueError):
            call()
    assert victim.read_bytes() == b"sentinel" and (store.root / link).is_symlink()


def test_store_tightens_existing_directories_and_refuses_a_symlinked_root(tmp_path):
    from backup import BackupError, LocalStore
    loose = tmp_path / "loose"
    loose.mkdir(mode=0o755)
    LocalStore(loose)
    assert loose.stat().st_mode & 0o777 == 0o700
    (tmp_path / "linked").symlink_to(loose)
    with pytest.raises(BackupError, match="unsafe_directory"):
        LocalStore(tmp_path / "linked")


@pytest.mark.parametrize("bad", [
    {"name": "../outside/victim"},
    {"name": "/tmp/victim"},
    {"archive": "../outside/victim.dump.cms"},
    {"name": "perennial-20260101T000000Z-bbbbbb"},  # disagrees with its filename
    {"snapshot_time": "yesterday"},
    {"snapshot_time": "2026-10-01T00:00:00"},  # no timezone
    {"archive_sha256": "../x"},
    {"tables": {"t": {"rows": "1", "sha256": HEX}}},
    {"tables": {}},
    {"format": 2},
])
def test_malformed_manifests_are_never_acted_on(tmp_path, bad):
    from backup import LocalStore, manifests
    store = LocalStore(tmp_path / "store")
    name = _write_set(store, NOW - timedelta(days=30), **bad)
    invalid = []
    assert manifests(store, invalid) == []
    assert invalid == [name + ".manifest.json"]


@pytest.mark.db
def test_retention_and_drill_cannot_reach_outside_the_store(engine, loaded, keypair, tmp_path):
    from backup import LocalStore, run_backup
    from restore_drill import run_backup_drill
    outside = tmp_path / "outside"
    outside.mkdir()
    for f in ("victim.dump.cms", "victim.manifest.json"):
        (outside / f).write_text("sentinel")
    store = LocalStore(tmp_path / "store")
    old = NOW - timedelta(days=30)
    traversal = _write_set(store, old, "aaaaaa", name="../outside/victim")
    absolute = _write_set(store, old, "bbbbbb", archive=str(outside / "victim.dump.cms"))

    report = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)
    assert report["status"] == "ok", report
    assert report["invalid_manifests"] == sorted([absolute + ".manifest.json",
                                                  traversal + ".manifest.json"])
    assert sorted(p.name for p in outside.iterdir()) == ["victim.dump.cms",
                                                         "victim.manifest.json"]
    assert (store.root / (traversal + ".manifest.json")).exists()  # left for a human

    # The drill only ever selects a validated manifest, so the bad ones are invisible.
    with pytest.raises(ValueError):
        run_backup_drill(loaded, store, keypair / "key.pem", tmp_path / "pick",
                         manifest_name="../outside/victim")
    only_bad = LocalStore(tmp_path / "only-bad")
    _write_set(only_bad, old, "cccccc", archive="../outside/victim.dump.cms")
    with pytest.raises(RuntimeError, match="No verified backup manifest"):
        run_backup_drill(loaded, only_bad, keypair / "key.pem", tmp_path / "bad-drill")
    assert all(p.read_text() == "sentinel" for p in outside.iterdir())
    assert sorted(p.name for p in (tmp_path / "bad-drill").iterdir()) == ["report.json"]


# ── BK-2: orphan archives are swept; nothing else is ──

@pytest.mark.db
def test_orphan_archives_are_swept_after_the_grace_period(engine, loaded, keypair, tmp_path):
    from backup import LocalStore, run_backup
    store = LocalStore(tmp_path / "store")
    now = datetime.now(timezone.utc)
    stale = _name(now - timedelta(hours=2), "dddddd") + ".dump.cms"
    fresh = _name(now - timedelta(minutes=10), "eeeeee") + ".dump.cms"
    for f in (stale, fresh, "unrelated.dump.cms", "notes.txt"):
        (store.root / f).write_bytes(b"synthetic")
    report = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)
    assert report["status"] == "ok" and report["orphans_swept"] == [stale]
    assert sorted(store.names()) == sorted([
        fresh, "notes.txt", "unrelated.dump.cms",
        report["name"] + ".dump.cms", report["name"] + ".manifest.json"])


@pytest.fixture(scope="module")
def keypair(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic-backup-keys")
    for name in ("", "other-"):
        subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
             "-subj", "/CN=synthetic-backup-test", "-keyout", str(root / f"{name}key.pem"),
             "-out", str(root / f"{name}cert.pem")], check=True, capture_output=True)
    subprocess.run(
        ["openssl", "pkey", "-in", str(root / "key.pem"), "-aes-256-cbc",
         "-passout", "pass:synthetic-passphrase", "-out", str(root / "locked-key.pem")],
        check=True, capture_output=True)
    return root


@pytest.fixture
def loaded(engine, fixture_path):
    from src.ingestion.importer import CollectionOutcome, import_collection
    payload = json.loads(fixture_path("ark_holdings.json").read_text())
    assert import_collection(engine, CollectionOutcome.success(
        "ark_holdings", "fixture", payload["holdings"])).succeeded
    return engine.url.render_as_string(hide_password=False)


def _exists(engine, database):
    from sqlalchemy import text
    with engine.connect() as conn:
        return conn.execute(text("SELECT count(*) FROM pg_database WHERE datname = :n"),
                            {"n": database}).scalar_one() == 1


def _target_gone(engine, output):
    """This drill's own target only: the test server is shared with other runs."""
    report = json.loads((output / "report.json").read_text())
    return "target_database" not in report or not _exists(engine, report["target_database"])


def _no_plaintext(output):
    # Only the report survives a drill: no dump, no encrypted copy, no PGDMP bytes.
    assert sorted(p.name for p in output.iterdir()) == ["report.json"]
    assert b"PGDMP" not in (output / "report.json").read_bytes()


CONNECTION_KEYS = {"host", "hostaddr", "port", "user", "username", "password", "passfile",
                   "dbname", "database", "dsn", "url", "conninfo", "sslmode"}


def _no_connection_details(manifest_text, url):
    """The manifest names nothing about the database connection.

    Nothing here can match by chance: a bare port number can occur in the
    archive sha256, sizes and timestamps, and a bare user, password, database or
    host name can occur inside the backup's own name (the documented local user
    is "perennial"), so those are only matched as whole JSON values or in forms
    that need an "@" or ":" no manifest field has.
    """
    def walk(value):
        if isinstance(value, dict):
            for k, v in value.items():
                yield "key", k.lower()
                yield from walk(v)
        elif isinstance(value, list):
            for v in value:
                yield from walk(v)
        elif isinstance(value, str):
            yield "value", value.lower()

    lowered = manifest_text.lower()
    found = ["scheme"] if re.search(r"postgres(ql)?(\+\w+)?://", lowered) else []
    pieces = [(marker, marker) for marker in ("host=", "port=", "user=", "password=", "dbname=")]
    pieces += [("url", url.render_as_string(hide_password=False)),
               ("masked url", url.render_as_string()), ("host:port", f"{url.host}:{url.port}"),
               ("user:password@", f"{url.username}:{url.password}@"), ("@host", f"@{url.host}")]
    # Labels only, so a failure never prints the credential itself.
    found += [label for label, piece in pieces if piece.lower() in lowered]
    whole = {str(v).lower(): label for label, v in [
        ("host", url.host), ("user", url.username), ("password", url.password),
        ("database", url.database)] if v}
    for kind, item in walk(json.loads(manifest_text)):
        if kind == "key" and item in CONNECTION_KEYS:
            found.append(f"key {item}")
        elif kind == "value" and item in whole:
            found.append(whole[item])
    assert found == []


@pytest.mark.parametrize("raw", [
    # CI (run 40020378732: the archive sha256 held CI's port)
    "postgresql+psycopg://perennial_test:synthetic_ci_only@127.0.0.1:5432/perennial_test_0a1b2c3d",
    # development/backend/.env.example: user and password are the prefix of every backup name
    "postgresql+psycopg://perennial:perennial@localhost:5433/perennial_test",
])
def test_connection_check_ignores_chance_matches_but_catches_real_leaks(raw):
    from sqlalchemy.engine import make_url
    url = make_url(raw)
    port = str(url.port)
    digest = port + "a" * 60
    name = f"perennial-20261023T201405Z-{port}ab"
    manifest = {"format": 1, "name": name, "archive": name + ".dump.cms",
                "snapshot_time": f"2026-10-23T20:14:05.{port}12+00:00",
                "archive_bytes": int(port + "1"), "archive_sha256": digest,
                "recipient_sha256_fingerprint": HEX, "pg_dump_version": "PostgreSQL 17.7",
                "storage_receipt": {"store": "local-stub", "name": name + ".dump.cms",
                                    "bytes": int(port + "1"), "sha256": digest},
                "tables": {"ark_holdings": {"rows": int(port), "sha256": digest}}}
    text = json.dumps(manifest, indent=2)
    assert port in text  # the bare-port check failed here
    _no_connection_details(text, url)

    leaks = [
        {"note": url.render_as_string(hide_password=False)},
        {"note": url.render_as_string()},
        {"note": f"postgresql://elsewhere/{url.database}x"},
        {"note": f"restored from {url.host}:{port}"},
        {"note": f"host={url.host} port={port} dbname=x"},
        {"note": f"{url.username}:{url.password}@"},
        {"note": f"via @{url.host}"},
        {"note": url.password},
        {"note": url.username.upper()},
        {"source": [url.database]},
        {"origin": url.host},
        {"port": url.port},
        {"storage_receipt": {**manifest["storage_receipt"], "Host": "x"}},
        {"tables": {"ark_holdings": {"rows": 1, "sha256": HEX, "dsn": "x"}}},
    ]
    for leak in leaks:
        with pytest.raises(AssertionError):
            _no_connection_details(json.dumps({**manifest, **leak}, indent=2), url)


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
    _no_connection_details(manifest_text, engine.url)

    drill = run_backup_drill(loaded, store, keypair / "key.pem", tmp_path / "drill")
    assert drill["verified"] is True and drill["target_dropped"] is True
    assert drill["backup"] == name and drill["tables"] == manifest["tables"]
    assert drill["within_rto"] is True and drill["rto_target_seconds"] == 3600
    for step in ("retrieve", "decrypt_restore_and_verify"):
        assert drill[step + "_seconds"] >= 0
    _no_plaintext(tmp_path / "drill")
    assert _target_gone(engine, tmp_path / "drill")


# ── SEC-BACKUP-02: no plaintext survives a drill on any path ──

@pytest.mark.db
def test_no_plaintext_survives_failed_or_interrupted_drills(
    engine, loaded, keypair, tmp_path, monkeypatch
):
    import restore_drill
    from backup import LocalStore, run_backup
    store = LocalStore(tmp_path / "store")
    name = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)["name"]

    # Verification failure after a successful restore.
    manifest_path = store.root / (name + ".manifest.json")
    good = manifest_path.read_text()
    manifest = json.loads(good)
    manifest["tables"]["ark_holdings"]["rows"] += 1
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError, match="differ"):
        restore_drill.run_backup_drill(loaded, store, keypair / "key.pem", tmp_path / "verify")
    _no_plaintext(tmp_path / "verify")
    assert _target_gone(engine, tmp_path / "verify")
    manifest_path.write_text(good)

    # Interruption while pg_restore is consuming the decrypted stream.
    real_popen = restore_drill.subprocess.Popen
    started = []

    def interrupting(args, **kwargs):
        proc = real_popen(args, **kwargs)
        started.append(proc)
        if args[0] == "pg_restore":
            real_wait = proc.wait

            def wait(*a, **k):  # one Ctrl-C while the restore is running
                proc.wait = real_wait
                raise KeyboardInterrupt
            proc.wait = wait
        return proc

    monkeypatch.setattr(restore_drill.subprocess, "Popen", interrupting)
    with pytest.raises(KeyboardInterrupt):
        restore_drill.run_backup_drill(loaded, store, keypair / "key.pem", tmp_path / "interrupt")
    monkeypatch.setattr(restore_drill.subprocess, "Popen", real_popen)
    pipeline = [p for p in started if p.args[0] == "pg_restore" or p.args[:2] == ["openssl", "cms"]]
    assert len(pipeline) == 2 and all(proc.poll() is not None for proc in pipeline)
    _no_plaintext(tmp_path / "interrupt")
    assert _target_gone(engine, tmp_path / "interrupt")


@pytest.mark.db
def test_passphrase_reaches_only_the_decryption_child(
    engine, loaded, keypair, tmp_path, monkeypatch
):
    import restore_drill
    from backup import LocalStore, run_backup
    store = LocalStore(tmp_path / "store")
    run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)
    seen = {}
    real_popen = restore_drill.subprocess.Popen

    def recording(args, **kwargs):
        seen[args[0]] = (args, kwargs.get("env") or {})
        return real_popen(args, **kwargs)

    monkeypatch.setenv("BACKUP_KEY_PASSPHRASE", "synthetic-passphrase")
    monkeypatch.setattr(restore_drill.subprocess, "Popen", recording)
    drill = restore_drill.run_backup_drill(loaded, store, keypair / "locked-key.pem",
                                           tmp_path / "drill")
    assert drill["verified"] is True
    assert seen["openssl"][1]["BACKUP_KEY_PASSPHRASE"] == "synthetic-passphrase"
    assert "BACKUP_KEY_PASSPHRASE" not in seen["pg_restore"][1]
    assert not any("synthetic-passphrase" in a for args, _ in seen.values() for a in args)
    _no_plaintext(tmp_path / "drill")


@pytest.mark.db
def test_tampered_or_wrongly_keyed_archive_is_never_restored(engine, loaded, keypair, tmp_path):
    from backup import LocalStore, run_backup, sha256_file
    from restore_drill import run_backup_drill
    store = LocalStore(tmp_path / "store")
    name = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)["name"]

    with pytest.raises(RuntimeError, match="did not decrypt"):
        run_backup_drill(loaded, store, keypair / "other-key.pem", tmp_path / "wrong-key")
    _no_plaintext(tmp_path / "wrong-key")
    assert _target_gone(engine, tmp_path / "wrong-key")

    path = store.root / (name + ".dump.cms")
    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 0xFF
    path.write_bytes(bytes(data))
    with pytest.raises(RuntimeError, match="checksum"):
        run_backup_drill(loaded, store, keypair / "key.pem", tmp_path / "tampered")
    _no_plaintext(tmp_path / "tampered")

    # Even with a manifest rewritten to match, AES-GCM authentication rejects it.
    manifest_path = store.root / (name + ".manifest.json")
    manifest = json.loads(manifest_path.read_text())
    manifest["archive_sha256"] = sha256_file(path)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError, match="did not decrypt"):
        run_backup_drill(loaded, store, keypair / "key.pem", tmp_path / "forged")
    _no_plaintext(tmp_path / "forged")
    assert _target_gone(engine, tmp_path / "forged")


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


# ── BK-3: the stored-object checksum is what decides ──

@pytest.mark.db
def test_mismatched_storage_receipt_is_not_a_backup(engine, loaded, keypair, tmp_path):
    from backup import LocalStore, run_backup
    store = LocalStore(tmp_path / "store")
    run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)
    before = store.names()

    class CorruptingStore(LocalStore):
        def put(self, name, path):
            receipt = super().put(name, path)
            return dict(receipt, sha256="f" * 64)  # the provider stored other bytes

    report = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool",
                        CorruptingStore(tmp_path / "store"))
    assert report["status"] == "failed" and report["error_code"] == "upload_verify_failed"
    assert store.names() == before  # unverified archive removed, no manifest, old set intact


@pytest.mark.db
def test_backup_applies_retention_to_stale_verified_sets(engine, loaded, keypair, tmp_path):
    from backup import LocalStore, run_backup
    store = LocalStore(tmp_path / "store")
    now = datetime.now(timezone.utc)
    stale = [_write_set(store, now - timedelta(days=9), "aaaaaa"),
             _write_set(store, now - timedelta(days=10), "bbbbbb")]
    report = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)
    assert report["status"] == "ok"
    assert report["deleted"] == sorted(stale)
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
    _no_plaintext(tmp_path / "drill")

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


@pytest.mark.db
def test_libressl_is_refused_rather_than_failing_later(
    engine, loaded, keypair, tmp_path, monkeypatch
):
    from backup import LocalStore, run_backup
    fake = tmp_path / "bin"
    fake.mkdir()
    (fake / "openssl").write_text("#!/bin/sh\necho 'LibreSSL 3.3.6'\n")
    (fake / "openssl").chmod(0o755)
    monkeypatch.setenv("PATH", f"{fake}:{os.environ['PATH']}")
    store = LocalStore(tmp_path / "store")
    report = run_backup(loaded, keypair / "cert.pem", tmp_path / "spool", store)
    assert report["status"] == "failed" and report["error_code"] == "openssl_unsupported"
    assert store.names() == []
