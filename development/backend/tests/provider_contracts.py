"""Offline JSON shape extraction and comparison. No requests or credentials.

Paths use JSON Pointer escaping, plus ~2 for literal *; /* means list items.
Null is an explicit type; optional means absent in some observed parent objects.
Empty lists have no inferred item schema. Shapes are observations, not a claim
that unobserved variants are forbidden. Values are never retained in reports.
Object keys are retained: live inputs still require scrubbing/auditor review.
"""

import argparse
import json
from pathlib import Path
from typing import Any, TypedDict


class Node(TypedDict):
    types: list[str]
    optional: bool


Shape = dict[str, Node]


class Drift(TypedDict):
    path: str
    kind: str
    expected: Node | None
    observed: Node | None


def _kind(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return "object"
    raise ValueError("non_json_value")


def extract_shape(payload: Any) -> Shape:
    """Aggregate all rows, ignoring order, lengths and scalar values."""
    result: Shape = {}

    def visit(values: list[Any], path: str, optional: bool = False) -> None:
        result[path] = {
            "types": sorted({_kind(v) for v in values}),
            "optional": optional,
        }
        objects = [v for v in values if isinstance(v, dict)]
        for key in sorted({k for obj in objects for k in obj}):
            present = [obj[key] for obj in objects if key in obj]
            escaped = key.replace("~", "~0").replace("/", "~1").replace("*", "~2")
            visit(present, f"{path}/{escaped}", len(present) < len(objects))
        items = [item for v in values if isinstance(v, list) for item in v]
        if items:
            visit(items, f"{path}/*")

    visit([payload], "$")
    return result


def compare_shapes(expected: Shape, observed: Shape) -> list[Drift]:
    """Stable structured diff; unknown empty-list items are never called removed."""
    changes: list[Drift] = []
    unknown = []
    for path in expected:
        if path.endswith("/*") and path not in observed:
            parent = observed.get(path[:-2])
            if parent and "array" in parent["types"]:
                unknown.append(path)
    for path in sorted(expected.keys() | observed.keys()):
        before, after = expected.get(path), observed.get(path)
        if before == after:
            continue
        if any(path.startswith(p + "/") for p in unknown):
            continue
        kind = (
            "unobserved"
            if path in unknown
            else "added"
            if before is None
            else "removed"
            if after is None
            else "changed"
        )
        changes.append(
            {"path": path, "kind": kind, "expected": before, "observed": after}
        )
    return changes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args()
    try:
        expected = extract_shape(json.loads(args.baseline.read_text()))
        observed = extract_shape(json.loads(args.candidate.read_text()))
        changes = compare_shapes(expected, observed)
    except (OSError, ValueError, RecursionError):
        print(json.dumps({"error": "invalid_contract_input"}))
        return 2
    print(json.dumps(changes, indent=2, sort_keys=True))
    return int(bool(changes))


if __name__ == "__main__":
    raise SystemExit(main())
