# ADR 0001: Hash-checked Python dependency locks

Date: 2026-10-09

Status: proposed for P2-C

## Decision

Declare exact runtime pins in `project.dependencies` and development pins in the
`dev` optional dependency group of `pyproject.toml`. Generate both
`requirements.lock` and `requirements-dev.lock` with `pip-tools==7.6.2` on the
repository's Python 3.12 interpreter using `pip-compile --generate-hashes`.
CI installs the development lock with pip's `--require-hashes` option and runs
`pip-audit` against that lock. Dependabot checks Python dependencies weekly.

## Why

The runtime lock can be used by a production image without test and lint tools.
The development lock includes runtime and development packages, including all
transitive dependencies. Hash checking makes a version-only replacement fail
installation. Existing `requirements*.txt` files remain thin compatibility
entry points to the generated locks.

## Regeneration

On Python 3.12, install `pip-tools==7.6.2`, then run:

```sh
python -m piptools compile --generate-hashes --allow-unsafe -o requirements.lock pyproject.toml
python -m piptools compile --generate-hashes --allow-unsafe --extra dev -o requirements-dev.lock pyproject.toml
```

The runtime lock also uses `--allow-unsafe` (since S1-12): textstat requires
setuptools, so the runtime lock must pin it, with hashes, to install with
`--require-hashes`.

Review both diffs and rerun the synthetic suite and dependency audit before
committing an update. Regeneration is platform sensitive; CI verifies the
Linux install from the committed lock.
