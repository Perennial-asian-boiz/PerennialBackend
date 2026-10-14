"""Parallel branches must not fork the migration history.

Alembic only warns when two files share a revision id and then silently loads
one of them, so a duplicate id would change the schema without failing. These
checks turn that, and a second head, into a CI failure on the later PR.
"""

import ast
import warnings
from collections import Counter
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "database" / "alembic.ini"
VERSIONS = ALEMBIC_INI.parent / "migrations" / "versions"


def _revision_ids(versions: Path) -> list[str]:
    ids = []
    for path in sorted(versions.glob("*.py")):
        for node in ast.parse(path.read_text()).body:
            target = node.target if isinstance(node, ast.AnnAssign) else None
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
            if (
                isinstance(target, ast.Name)
                and target.id == "revision"
                and isinstance(node.value, ast.Constant)
            ):
                ids.append(node.value.value)
    return ids


def _duplicates(versions: Path) -> list[str]:
    return sorted(i for i, n in Counter(_revision_ids(versions)).items() if n > 1)


def _heads(script_location: Path) -> list[str]:
    script = ScriptDirectory(
        str(script_location), version_locations=[str(script_location / "versions")]
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return list(script.get_heads())


def test_every_migration_file_declares_a_revision():
    files = list(VERSIONS.glob("*.py"))
    assert files
    assert len(_revision_ids(VERSIONS)) == len(files)


def test_revision_ids_are_unique():
    assert _duplicates(VERSIONS) == []


def test_migration_history_has_one_head():
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    assert len(script.get_heads()) == 1


@pytest.fixture
def forked_history(tmp_path: Path) -> Path:
    versions = tmp_path / "versions"
    versions.mkdir()
    body = "down_revision = '0003'\ndef upgrade(): pass\ndef downgrade(): pass\n"
    (versions / "a_0004_gdelt.py").write_text("revision = '0004'\n" + body)
    (versions / "b_0004_auth.py").write_text('revision: str = "0004"\n' + body)
    (versions / "c_0003.py").write_text(
        "revision = '0003'\ndown_revision = None\n"
        "def upgrade(): pass\ndef downgrade(): pass\n"
    )
    return tmp_path


def test_guard_detects_duplicate_revision_ids(forked_history: Path):
    assert _duplicates(forked_history / "versions") == ["0004"]


def test_guard_detects_a_second_head(forked_history: Path):
    assert len(_heads(forked_history)) == 2
