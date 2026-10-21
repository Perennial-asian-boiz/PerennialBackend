"""
Perennial — Shared Path Resolution
File: services/consensus_watchlist/paths.py

PURPOSE:
    Single source of truth for where the pipeline reads and writes.

    Every module used to compute the repo root with a hardcoded parent
    count (parents[4], parents[5], parents[6]) or by matching the literal
    directory name "project-perennial". Both break silently the moment the
    tree is restructured or the repo is renamed — which is exactly what
    happened moving to PerennialBackend. This module searches upward for a
    real marker instead, so nothing depends on how deep a file sits.

USAGE:
    From any module under consensus_watchlist/ (including fetchers/ and
    scheduler/), bootstrap the import like this:

        import sys
        from pathlib import Path
        sys.path.insert(0, str(next(
            p for p in Path(__file__).resolve().parents
            if p.name == "consensus_watchlist"
        )))
        from paths import ENV_PATH, LOCAL_DATA_DIR
"""

import sys
from pathlib import Path
from typing import Optional


def use_utf8_console() -> None:
    """
    The pipeline's progress output uses emoji and arrows. When output is
    captured rather than shown in a real terminal (VS Code "Run Code", a log
    file, the scheduler), Windows falls back to cp1252, which cannot encode
    them — the first one crashes the run before its JSON is saved. Called at
    import below, so every module that imports paths gets it for free.
    """
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


use_utf8_console()

# consensus_watchlist/ — found by name, so fetchers/ and scheduler/ resolve
# to the same anchor as consensus.py itself.
PACKAGE_DIR = next(
    p for p in Path(__file__).resolve().parents if p.name == "consensus_watchlist"
)


def find_repo_root() -> Path:
    """
    Walks up from consensus_watchlist/ looking for the repo root.

    Markers, in the order they are checked at each level:
      1. a .git directory          — a real clone
      2. development/backend/      — a source export with no git metadata

    Falls back to the historical layout depth if neither is found, so a
    stripped-down copy still lands somewhere sane rather than raising.
    """
    for parent in PACKAGE_DIR.parents:
        if (parent / ".git").is_dir():
            return parent
        if (parent / "development" / "backend").is_dir():
            return parent
    return PACKAGE_DIR.parents[3]


REPO_ROOT = find_repo_root()

# Credentials. Matches .env.example's location and the .gitignore rule.
ENV_PATH = REPO_ROOT / "development" / "backend" / ".env"

# Every fetcher writes here; consensus.py reads all four files back out.
LOCAL_DATA_DIR = REPO_ROOT / "development" / "database" / "local_data"


def local_data_dir() -> Path:
    """Returns LOCAL_DATA_DIR, creating it if this is a fresh clone."""
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    return LOCAL_DATA_DIR


def input_file(filename: str) -> Optional[Path]:
    """Returns the path to a pipeline input file, or None if it hasn't been fetched yet."""
    path = local_data_dir() / filename
    return path if path.exists() else None
