"""P2-E: legacy names, the cron.py aliases, and the typed adapter interfaces."""

import pytest
from src.ingestion import collectors

FETCHERS = ("ark", "fmp", "insider", "short_interest")


@pytest.mark.parametrize("name", FETCHERS)
def test_cron_run_alias_is_the_legacy_entry_point(name):
    # scheduler/cron.py calls <fetcher>.run() and cannot change until M7.
    module = collectors._fetcher(name)
    assert module.run is module.legacy_run
