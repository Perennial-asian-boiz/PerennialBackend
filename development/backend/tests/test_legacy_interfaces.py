"""P2-E: legacy names, the cron.py aliases, and the typed adapter interfaces."""

import inspect

import pytest
from src.ingestion import adapters, collectors

FETCHERS = ("ark", "fmp", "insider", "short_interest")


@pytest.mark.parametrize("name", FETCHERS)
def test_cron_run_alias_is_the_legacy_entry_point(name):
    # scheduler/cron.py calls <fetcher>.run() and cannot change until M7.
    module = collectors._fetcher(name)
    assert module.run is module.legacy_run


ADAPTERS = {
    "ark": adapters.ArkAdapter,
    "fmp": adapters.FmpAdapter,
    "insider": adapters.InsiderAdapter,
    "short_interest": adapters.ShortInterestAdapter,
}


def _members(protocol):
    return {name for name in vars(protocol) if not name.startswith("_")}


@pytest.mark.parametrize("name", FETCHERS)
def test_fetcher_module_satisfies_its_adapter_protocol(name):
    # collectors load fetchers by file, so this is the runtime half of the
    # static check in src/ingestion/adapters.py.
    assert isinstance(collectors._fetcher(name), ADAPTERS[name])


@pytest.mark.parametrize("name", FETCHERS)
def test_every_public_fetcher_function_is_shared_or_legacy(name):
    module = collectors._fetcher(name)
    functions = {
        attr
        for attr, value in vars(module).items()
        if inspect.isfunction(value)
        and value.__module__ == module.__name__
        and not attr.startswith("_")
        and attr != "run"
    }
    shared = functions & _members(ADAPTERS[name])
    assert functions - shared == {f for f in functions if f.startswith("legacy_")}
    assert not any(f.startswith("legacy_") for f in _members(ADAPTERS[name]))


def test_collectors_registry_matches_the_collector_protocol():
    assert set(collectors.COLLECTORS) == {
        "congress_trades",
        "ark_holdings",
        "insider_trades",
        "short_interest",
    }
    for collector in collectors.COLLECTORS.values():
        params = inspect.signature(collector).parameters
        assert params["session"].default is None
        assert "sleep" in params
