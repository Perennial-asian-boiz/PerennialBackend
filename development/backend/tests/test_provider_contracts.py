"""Synthetic contract harness behavior; never calls providers."""

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

HARNESS = Path(__file__).with_name("provider_contracts.py")
FIXTURES = Path(__file__).parent / "fixtures" / "providers"


def load_harness():
    assert HARNESS.exists(), "P1-C shape extractor is missing on baseline"
    spec = importlib.util.spec_from_file_location("provider_contracts", HARNESS)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_shape_aggregates_every_row_and_separates_null_missing_bool():
    harness = load_harness()
    shape = harness.extract_shape([{"a": 1, "b": True}, {"a": None}, {"a": 2.5}])
    assert shape["$"]["types"] == ["array"]
    assert shape["$/*/a"] == {"types": ["integer", "null", "number"], "optional": False}
    assert shape["$/*/b"] == {"types": ["boolean"], "optional": True}
    assert shape == harness.extract_shape(
        [{"a": 2.5}, {"a": None}, {"b": False, "a": 7}]
    )


@pytest.mark.parametrize(
    "before,after,path,kind",
    [
        ({"a": 1}, {}, "$/a", "removed"),
        ({}, {"a": 1}, "$/a", "added"),
        ({"a": 1}, {"a": "1"}, "$/a", "changed"),
        ({"a": "x"}, {"a": None}, "$/a", "changed"),
        ([], {}, "$", "changed"),
        ([{"a": 1}], [{"a": 1}, {}], "$/*/a", "changed"),
        ([{"a": 1}], [], "$/*", "unobserved"),
    ],
)
def test_structured_drift(before, after, path, kind):
    harness = load_harness()
    diff = harness.compare_shapes(
        harness.extract_shape(before), harness.extract_shape(after)
    )
    assert any(d["path"] == path and d["kind"] == kind for d in diff)
    assert all(set(d) == {"path", "kind", "expected", "observed"} for d in diff)


def test_values_and_array_order_do_not_drift():
    harness = load_harness()
    assert (
        harness.compare_shapes(
            harness.extract_shape([{"a": "secret-one"}]),
            harness.extract_shape([{"a": "secret-two"}]),
        )
        == []
    )
    assert "secret" not in json.dumps(harness.extract_shape({"a": "secret-one"}))


def test_paths_escape_object_keys_and_distinguish_array_items():
    harness = load_harness()
    shape = harness.extract_shape({"a/b": {"~": 1}, "*": [True]})
    assert "$/a~1b/~0" in shape
    assert "$/~2/*" in shape


def test_synthetic_fixture_inventory_and_existing_validators(monkeypatch):
    harness = load_harness()
    from datetime import date

    from src.ingestion import collectors
    from src.services.consensus_watchlist.fetchers import insider

    expected = {
        "fmp/senate.json",
        "fmp/house.json",
        "ark/holdings.json",
        "finnhub/insider_transactions.json",
        "nasdaq/short_interest.json",
    }
    # The fixture adapter's input (M4 MC-A), not a provider response.
    adapter_inputs = {"market_caps/quotes.json"}
    assert {str(p.relative_to(FIXTURES)) for p in FIXTURES.glob("*/*.json")} == (
        expected | adapter_inputs
    )
    payloads = {name: json.loads((FIXTURES / name).read_text()) for name in expected}
    for name in ("fmp/senate.json", "fmp/house.json"):
        assert payloads[name]
        assert all(
            set(collectors._FMP_KEYS) <= set(r)
            and collectors._string_fields_ok(r, collectors._FMP_KEYS)
            for r in payloads[name]
        )
    assert collectors._ark_fund_ok(payloads["ark/holdings.json"]["holdings"]) is None
    assert (
        collectors._short_interest_kind(payloads["nasdaq/short_interest.json"])
        == "rows"
    )
    monkeypatch.setattr(insider, "lookback_cutoff", lambda: date(2026, 1, 1))
    buys, code, _ = insider.parse_finnhub_buys(
        "ZZAA", payloads["finnhub/insider_transactions.json"], "popular_stable"
    )
    assert code is None and len(buys) == 1 and buys[0]["value"] == 100000
    for payload in payloads.values():
        assert (
            harness.compare_shapes(
                harness.extract_shape(payload), harness.extract_shape(payload)
            )
            == []
        )


def test_cli_reports_drift_and_nonzero_without_values(tmp_path):
    load_harness()
    baseline = tmp_path / "baseline.json"
    candidate = tmp_path / "candidate.json"
    baseline.write_text('{"a": 1}')
    candidate.write_text('{"a": "SYNTHETIC_SECRET"}')
    result = subprocess.run(
        [sys.executable, str(HARNESS), str(baseline), str(candidate)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert json.loads(result.stdout)[0]["path"] == "$/a"
    assert "SYNTHETIC_SECRET" not in result.stdout + result.stderr
    candidate.write_text('{"a": 2}')
    result = subprocess.run(
        [sys.executable, str(HARNESS), str(baseline), str(candidate)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0 and json.loads(result.stdout) == []


def test_cli_invalid_json_fails_safely(tmp_path):
    load_harness()
    invalid = tmp_path / "invalid.json"
    invalid.write_text("SYNTHETIC_SECRET")
    result = subprocess.run(
        [sys.executable, str(HARNESS), str(invalid), str(invalid)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert json.loads(result.stdout) == {"error": "invalid_contract_input"}
    assert "SYNTHETIC_SECRET" not in result.stdout + result.stderr


def test_workflow_is_manual_only_and_has_no_live_capture():
    workflow = (
        Path(__file__).resolve().parents[3] / ".github/workflows/provider-contracts.yml"
    )
    assert workflow.exists(), "P1-C manual workflow is missing on baseline"
    text = workflow.read_text()
    assert "workflow_dispatch:" in text and "schedule:" not in text
    assert "secrets." not in text and "issues: write" not in text
    assert "test_provider_contracts.py" in text
