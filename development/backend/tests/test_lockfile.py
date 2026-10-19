"""The production install path is pinned and hash checked."""

import re
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_ci_installs_hashed_dev_lock_with_runtime_dependencies():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    runtime = project["dependencies"]
    dev = project["optional-dependencies"]["dev"]
    assert runtime and dev
    lock = (ROOT / "requirements-dev.lock").read_text()
    for requirement in runtime + dev:
        name, version = requirement.split("==", 1)
        pattern = rf"(?m)^{re.escape(name.lower())}=={re.escape(version)}\s*\\"
        assert re.search(pattern, lock, re.IGNORECASE), requirement
    assert lock.count("--hash=sha256:") >= len(runtime) + len(dev)
    assert "# WARNING:" not in lock
    lines = lock.splitlines()
    pinned = [index for index, line in enumerate(lines) if re.match(r"^[A-Za-z][^\s]*==", line)]
    assert pinned
    for index in pinned:
        assert lines[index].endswith("\\")
        assert lines[index + 1].strip().startswith("--hash=sha256:")
    workflow = (ROOT / ".github/workflows/tests.yml").read_text()
    assert "pip install --require-hashes -r requirements-dev.lock" in workflow
