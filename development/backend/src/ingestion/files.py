"""
JSON-file entry point: turn an existing fetcher output file into a
CollectionOutcome for the shared importer.

Completeness boundary. The current fetchers write their JSON file even after
swallowing request errors, so a file by itself cannot prove that the
collection which produced it was complete; importing a partial file would
replace the latest good snapshot with a partial one. Therefore:

  * mode="file" (legacy fetcher output) is rejected with
    `collection_unattested` unless the caller passes attest_complete=True
    (CLI: --attest-complete), stating they know that collection finished
    without errors. Without it, nothing is imported and the latest good batch
    stays current.
  * mode="fixture" (synthetic/sanitized test inputs) is self-attesting for
    non-empty inputs.
  * An empty record list always requires attest_complete=True
    (`empty_collection_unattested` otherwise), in either mode.
  * A short-interest file with a non-empty `errors` list is rejected
    (`collection_reported_errors`) even when attested.
  * Live collection (src/ingestion/collectors.py) decides completeness from
    observed per-request results instead and never needs attestation.

Only regular files are read (no symlinks, FIFOs or devices), at most
MAX_FILE_BYTES are read before parsing, and errors are reported as fixed codes.
"""

import json
import os
import stat
from pathlib import Path
from typing import Any, Optional, Tuple, Union

from src.ingestion.importer import CollectionOutcome
from src.ingestion.schemas import RECORDS_KEY

MAX_FILE_BYTES = 25 * 1024 * 1024
FILE_MODES = ("file", "fixture")


def read_json_bounded(
    path: Union[str, Path], max_bytes: int = MAX_FILE_BYTES
) -> Tuple[Any, Optional[str]]:
    """
    Parse a regular JSON file of at most max_bytes. Returns (data, None) or
    (None, code) with code in file_unreadable / input_too_large / malformed_json.
    The size limit is enforced on the bytes actually read, not only on stat().
    """
    fd: Optional[int]
    try:
        fd = os.open(
            str(path),
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
        )
    except OSError:
        return None, "file_unreadable"
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            return None, "file_unreadable"
        with os.fdopen(fd, "rb") as handle:
            fd = None
            raw = handle.read(max_bytes + 1)
    except OSError:
        return None, "file_unreadable"
    finally:
        if fd is not None:
            os.close(fd)
    if len(raw) > max_bytes:
        return None, "input_too_large"
    try:
        return json.loads(raw.decode("utf-8")), None
    except (
        ValueError,
        RecursionError,
    ):  # JSONDecodeError and UnicodeDecodeError are ValueErrors
        return None, "malformed_json"


def read_fetcher_file(
    source: str,
    path: Union[str, Path],
    mode: str = "file",
    attest_complete: bool = False,
) -> CollectionOutcome:
    if mode not in FILE_MODES:
        raise ValueError(f"file imports use mode 'file' or 'fixture', not {mode!r}")
    path = Path(path)

    def fail(code: str, **diag: Any) -> CollectionOutcome:
        return CollectionOutcome.failure(source, mode, code, diagnostics=diag or None)

    data, problem = read_json_bounded(path)
    if problem:
        return fail(problem)

    key = RECORDS_KEY[source]
    if not isinstance(data, dict) or not isinstance(data.get(key), list):
        return fail("malformed_input")
    if source == "short_interest" and data.get("errors"):
        return fail("collection_reported_errors")
    records = data[key]
    if not records and not attest_complete:
        return fail("empty_collection_unattested")
    if mode == "file" and not attest_complete:
        return fail("collection_unattested", input_count=len(records))
    envelope = {k: v for k, v in data.items() if k != key}
    return CollectionOutcome.success(source, mode, records, envelope=envelope)
