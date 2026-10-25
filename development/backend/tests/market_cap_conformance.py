"""Conformance every market-cap adapter must pass (M4 MCA-5b). Synthetic inputs only.

A new provider adapter adds a case to CONFORMANCE_CASES in
test_market_caps_source.py. It must call fetch through `session` (a fake
transport the test supplies) and never reach the network from the test.
"""

import re
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal

import pytest

ADAPTER_NAME = re.compile(r"[a-z0-9_]{1,32}")
CURRENCY = re.compile(r"[A-Z]{3}")
HTTP_CODE = re.compile(r"http_[1-5][0-9]{2}")


def allowed_error_code(code):
    """missing_credential, or a unit failure code the run diagnostics keep."""
    from src.ingestion.diagnostics import FAILURE_CODES

    return isinstance(code, str) and (
        code == "missing_credential"
        or code in FAILURE_CODES
        or HTTP_CODE.fullmatch(code) is not None
    )


def check_quote(quote):
    from src.ingestion.market_caps import MarketCapQuote

    assert isinstance(quote, MarketCapQuote)
    value = quote.value
    assert value is None or (
        isinstance(value, Decimal) and value.is_finite() and value > 0
    )
    assert isinstance(quote.currency, str) and CURRENCY.fullmatch(quote.currency)
    assert isinstance(quote.issuer_level, bool)
    assert quote.observed_at is None or (
        isinstance(quote.observed_at, datetime) and quote.observed_at.tzinfo is not None
    )
    for label in (quote.sector, quote.industry):
        assert label is None or isinstance(label, str)


def check_adapter(adapter, symbols, *, failing=None, session=None):
    """Run the conformance checks against one adapter.

    `symbols` must include at least one symbol the adapter has no value for.
    `failing` is the same adapter set up to fail (for example a fake session
    answering 429); it must raise MarketCapAdapterError with an allowed code.
    """
    from src.ingestion.market_caps import MarketCapAdapterError

    assert isinstance(adapter.name, str) and ADAPTER_NAME.fullmatch(adapter.name)
    assert adapter.collection_mode in ("live", "fixture")
    slept = []
    symbols = list(symbols)

    first = adapter.fetch(symbols, session=session, sleep=slept.append)
    assert isinstance(first, Mapping)
    # Exactly the requested canonical symbols: none dropped, none added or respelled.
    assert set(first) == set(symbols)
    for quote in first.values():
        if quote is not None:
            check_quote(quote)
    assert any(q is None for q in first.values()), "include a symbol without a value"
    assert any(q is not None for q in first.values()), "include a symbol with a value"

    # Deterministic for the same inputs, whatever the request order.
    again = adapter.fetch(list(reversed(symbols)), session=session, sleep=slept.append)
    assert dict(again) == dict(first)
    assert all(isinstance(s, (int, float)) and s >= 0 for s in slept)

    if failing is not None:
        with pytest.raises(MarketCapAdapterError) as caught:
            failing.fetch(symbols, session=session, sleep=slept.append)
        assert allowed_error_code(caught.value.code)
    return first
