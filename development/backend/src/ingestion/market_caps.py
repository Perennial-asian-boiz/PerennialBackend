"""
Market caps as a tracked source (M4 MC-A): the adapter interface, the fixture
adapter and the provider-neutral collector.

A provider is added by writing one MarketCapAdapter and registering it in
ADAPTERS, the only place a provider is named. collect() plans the request,
validates the adapter's quotes and shapes them into market_caps records; the
shared importer stores them as a batch with run lineage. run_pipeline
calls it once per run when an adapter is configured (MC-B); production keeps
the legacy FMP profile path while settings.market_cap_adapter is None.

Policy, as in src/ingestion/collectors.py:
  * The plan is the publication's candidate set (ranking.candidate_symbols)
    over explicit upstream {trades, holdings} rows: Congress purchase tickers
    plus ARK holdings, sorted and unique. Malformed rows are missing_upstream,
    an empty plan is no_input_tickers and a plan over MAX_PLANNED_REQUESTS is
    plan_too_large. Nothing is ever truncated.
  * Any failure fails the whole collection with collection_failed, so no
    partial batch is stored: an adapter error (its code is the unit code, filed
    under every planned ticker, since one call covers them all), a response
    whose symbols differ from the plan, or an invalid quote. A missing
    credential is its own run code. Exceptions other than MarketCapAdapterError
    propagate to collect_source, which maps them without their text.
  * A None quote is a stored row without a value: the provider has none. Such
    a row asserts nothing else, so it is USD (as the legacy path stores it) and
    not issuer-level.
  * Values follow publication._caps: finite, above 0 and below 1e22, rounded
    half-even to cents. Only USD. observed_at must be timezone-aware and not
    in the future. An invalid sector or industry label becomes NULL,
    including one with a control or format character.
  * A fixture adapter can only record a fixture run (MCA-3g); an adapter of
    any other non-live mode is refused too. collect_source relabels the
    outcome's mode from its record_mode, so this layer refuses the adapter
    before fetch, whatever the caller does with the outcome. A live adapter
    records only live runs (adapter_mode_mismatch otherwise).
"""

import logging
import time
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal, Protocol

import requests

from src.ingestion.collectors import (
    MAX_PLANNED_REQUESTS,
    _fetcher,
    _get_json,
    _Tracker,
    _upstream_row_ok,
)
from src.ingestion.files import read_json_bounded
from src.ingestion.finnhub_profile import PROFILE_URL, MalformedProfile, parse_profile
from src.ingestion.importer import CollectionOutcome
from src.ingestion.schemas import normalize_symbol
from src.pipeline.ranking import candidate_symbols
from src.settings import Settings

logger = logging.getLogger(__name__)

SOURCE = "market_caps"
UNIT_KIND = "market-cap tickers"
MAX_VALUE = Decimal("1e22")  # NUMERIC(24, 2)
CENT = Decimal("0.01")
CURRENCY = "USD"
FUTURE_TOLERANCE = timedelta(minutes=5)  # as publication._time_ok
FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "tests"
    / "fixtures"
    / "providers"
    / "market_caps"
    / "quotes.json"
)
_FILE_CODES = {
    "file_unreadable": "unreadable_file",
    "input_too_large": "file_too_large",
    "malformed_json": "malformed_file",
}


@dataclass(frozen=True)
class MarketCapQuote:
    """One symbol's market cap as an adapter reports it; collect() validates it."""

    value: Decimal | None
    currency: str
    issuer_level: bool
    observed_at: datetime | None
    sector: str | None
    industry: str | None


class MarketCapAdapterError(Exception):
    """An adapter failure. Only the code is kept; messages are never stored.

    The code is missing_credential, or a unit failure code the run diagnostics
    keep (diagnostics.FAILURE_CODES or http_NNN); any other code is stored as
    "failed".
    """

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class MarketCapAdapter(Protocol):
    @property
    def name(self) -> str:
        """The provider id stored in each row: ^[a-z0-9_]{1,32}$."""
        ...

    @property
    def collection_mode(self) -> Literal["live", "fixture"]: ...

    def fetch(
        self,
        symbols: Sequence[str],
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> Mapping[str, MarketCapQuote | None]:
        """A quote (None: no value) for exactly the canonical symbols requested.

        The adapter maps its own provider spellings and keys the result by the
        symbols it was given. It raises MarketCapAdapterError on any failure.
        """
        ...


class FixtureMarketCapAdapter:
    """Serves a synthetic JSON fixture and never touches the network (demo, tests).

    The file is {"quotes": {SYMBOL: {"value": "<decimal>" | null, "currency",
    "issuer_level", "observed_at": ISO 8601 | null, "sector", "industry"}}}.
    Unlisted symbols get None. With fail_with, every fetch raises that code.
    """

    name = "fixture"
    collection_mode: Literal["live", "fixture"] = "fixture"

    def __init__(self, path: Path | None = None, *, fail_with: str | None = None):
        self.path = FIXTURE_PATH if path is None else path
        self.fail_with = fail_with

    def fetch(
        self,
        symbols: Sequence[str],
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> dict[str, MarketCapQuote | None]:
        if self.fail_with is not None:
            raise MarketCapAdapterError(self.fail_with)
        quotes = self._quotes()
        return {symbol: quotes.get(symbol) for symbol in symbols}

    def _quotes(self) -> dict[str, MarketCapQuote]:
        parsed, problem = read_json_bounded(self.path)
        if problem:
            raise MarketCapAdapterError(_FILE_CODES.get(problem, "unreadable_file"))
        raw = parsed.get("quotes") if isinstance(parsed, dict) else None
        if not isinstance(raw, dict):
            raise MarketCapAdapterError("malformed_file")
        try:
            return {symbol: _fixture_quote(item) for symbol, item in raw.items()}
        except (KeyError, TypeError, ValueError, InvalidOperation):
            raise MarketCapAdapterError("malformed_file") from None


def _fixture_quote(item: Any) -> MarketCapQuote:
    value, observed = item["value"], item["observed_at"]
    if value is not None and not isinstance(value, str):
        raise ValueError("fixture values are decimal strings")
    observed_at = None if observed is None else datetime.fromisoformat(observed)
    if observed_at is not None and observed_at.tzinfo is None:
        raise ValueError("fixture observed_at must be timezone-aware")
    return MarketCapQuote(
        value=None if value is None else Decimal(value),
        currency=item["currency"],
        issuer_level=item["issuer_level"],
        observed_at=observed_at,
        sector=item["sector"],
        industry=item["industry"],
    )


class FinnhubMarketCapAdapter:
    """Live market caps from Finnhub's free company profile (60 calls a minute).

    One request per symbol, REQUEST_DELAY apart, with the key in a header. The
    symbol sent is insider.provider_symbol's spelling (BRK.B -> BRK-B), the
    same mapping the insider collector uses. Values are company-wide
    (issuer_level) and USD only. These have no value: an unknown symbol, a
    listing with no US symbol, a profile in another currency, and a 403 (not
    on the free plan). A 403 for more than MAX_403_SHARE of the plan, or for
    all of it, is a key or plan problem and fails, like FMP's 402 (decision 84).
    Any other failure fails the fetch with its unit code.
    """

    name = "finnhub"
    collection_mode: Literal["live", "fixture"] = "live"
    REQUEST_DELAY = 1.1  # as insider.REQUEST_DELAY: the free tier is 60/minute
    MAX_403_SHARE = 0.25

    def __init__(self, api_key: str | None) -> None:
        self.api_key = api_key

    def fetch(
        self,
        symbols: Sequence[str],
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> dict[str, MarketCapQuote | None]:
        key = self.api_key
        if not key or key.lower().startswith("your_"):
            raise MarketCapAdapterError("missing_credential")
        insider = _fetcher("insider")
        owned = session is None
        session = session or requests.Session()
        quotes: dict[str, MarketCapQuote | None] = {}
        refused = other_currency = 0
        try:
            for index, symbol in enumerate(symbols):
                provider_symbol = insider.provider_symbol(symbol)
                if provider_symbol is None:  # no US listing to ask about
                    quotes[symbol] = None
                    continue
                if index:
                    sleep(self.REQUEST_DELAY)
                resp = _get_json(
                    session,
                    PROFILE_URL,
                    params={"symbol": provider_symbol},
                    headers={"X-Finnhub-Token": key},
                    sleep=sleep,
                )
                if not resp.ok and resp.code == "http_403":
                    refused += 1
                    quotes[symbol] = None
                    continue
                if not resp.ok:
                    raise MarketCapAdapterError(resp.code or "failed")
                try:
                    profile = parse_profile(resp.data, provider_symbol)
                except MalformedProfile:
                    raise MarketCapAdapterError("malformed_response") from None
                if profile is None or profile.market_cap is None:
                    quotes[symbol] = None
                elif profile.currency != CURRENCY:
                    other_currency += 1
                    quotes[symbol] = None
                else:
                    quotes[symbol] = MarketCapQuote(
                        value=profile.market_cap,
                        currency=CURRENCY,
                        issuer_level=True,
                        observed_at=None,
                        sector=None,
                        industry=profile.industry,
                    )
        finally:
            if owned:
                session.close()
        if refused and (
            refused == len(symbols) or refused > self.MAX_403_SHARE * len(symbols)
        ):
            logger.warning("market caps finnhub_403=%d of %d", refused, len(symbols))
            raise MarketCapAdapterError("http_403")
        if refused or other_currency:
            logger.info(
                "market caps unavailable: finnhub_403=%d non_usd=%d of %d",
                refused,
                other_currency,
                len(symbols),
            )
        return quotes


def _fixture_adapter(settings: Settings) -> MarketCapAdapter:
    return FixtureMarketCapAdapter()


def _finnhub_adapter(settings: Settings) -> MarketCapAdapter:
    key = settings.finnhub_api_key
    return FinnhubMarketCapAdapter(None if key is None else key.get_secret_value())


ADAPTERS: dict[str, Callable[[Settings], MarketCapAdapter]] = {
    "fixture": _fixture_adapter,
    "finnhub": _finnhub_adapter,
}


# ── collector ────────────────────────────────────────────


def _failure(code: str, tracker: _Tracker | None = None) -> CollectionOutcome:
    diagnostics = tracker.diagnostics() if tracker is not None else None
    return CollectionOutcome.failure(SOURCE, "live", code, diagnostics=diagnostics)


def _canonical(row: Any) -> bool:
    symbol = row.get("symbol")
    try:
        return isinstance(symbol, str) and normalize_symbol(symbol) == symbol
    except ValueError:
        return False


def _plan(upstream: Any) -> list[str] | None:
    """The publication's candidates over {trades, holdings}; None if malformed."""
    if not isinstance(upstream, dict):
        return None
    rows: dict[str, Sequence[dict[str, Any]]] = {}
    for key, source in (("trades", "congress_trades"), ("holdings", "ark_holdings")):
        value = upstream.get(key)
        if not isinstance(value, list) or not all(
            _upstream_row_ok(key, row) and _canonical(row) for row in value
        ):
            return None
        rows[source] = value
    return candidate_symbols(rows)


class _Invalid(Exception):
    def __init__(self, code: str = "malformed_response") -> None:
        super().__init__(code)
        self.code = code


def _value(raw: Any) -> Decimal | None:
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, (Decimal, int, float)):
        raise _Invalid
    try:
        value = Decimal(str(raw)) if isinstance(raw, float) else Decimal(raw)
        if not value.is_finite() or not 0 < value < MAX_VALUE:
            raise _Invalid
        value = value.quantize(CENT, rounding=ROUND_HALF_EVEN)
    except InvalidOperation:
        raise _Invalid from None
    if not 0 < value < MAX_VALUE:  # e.g. 0.004 rounds to 0.00
        raise _Invalid
    return value


def _observed_at(raw: Any, now: datetime) -> datetime | None:
    if raw is None:
        return None
    if not isinstance(raw, datetime) or raw.utcoffset() is None:
        raise _Invalid
    if raw > now + FUTURE_TOLERANCE:
        raise _Invalid
    return raw


def _label(raw: Any, max_length: int) -> str | None:
    """As runner._classification: optional labels; bad ones become null.

    A label with a control, format or other Unicode "C" character is bad too:
    NUL cannot be stored, and the others would be stored verbatim.
    """
    if not isinstance(raw, str):
        return None
    label = raw.strip()
    if not 0 < len(label) <= max_length:
        return None
    if any(unicodedata.category(char).startswith("C") for char in label):
        return None
    return label


def _record(symbol: str, quote: Any, provider: str, now: datetime) -> dict[str, Any]:
    """One market_caps record for `symbol`, or _Invalid with the unit code."""
    record: dict[str, Any] = {
        "ticker": symbol,
        "provider": provider,
        "value": None,
        "value_currency": CURRENCY,
        "issuer_level": False,
        "observed_at": None,
        "sector": None,
        "industry": None,
    }
    if quote is None:
        return record
    if not isinstance(quote, MarketCapQuote) or not isinstance(quote.currency, str):
        raise _Invalid
    if quote.currency != CURRENCY:
        raise _Invalid("currency_conflict")
    if not isinstance(quote.issuer_level, bool):
        raise _Invalid
    record.update(
        value=_value(quote.value),
        issuer_level=quote.issuer_level,
        observed_at=_observed_at(quote.observed_at, now),
        sector=_label(quote.sector, 64),
        industry=_label(quote.industry, 128),
    )
    return record


def collect(
    adapter: MarketCapAdapter,
    *,
    upstream: Any,
    record_mode: str = "live",
    session: requests.Session | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> CollectionOutcome:
    """Collect the candidates' market caps through one adapter.

    record_mode is the mode collect_source will record the run in. The
    outcome's own mode is always "live", as collect_source requires.
    """
    # MCA-3g: a fixture adapter only records fixture runs; any unknown mode fails closed.
    mode = adapter.collection_mode
    if mode != "live" and not (mode == "fixture" and record_mode == "fixture"):
        return _failure("fixture_adapter_not_allowed")
    # MC-B: symmetric, so a live adapter never records a fixture, file or derived run.
    if mode == "live" and record_mode != "live":
        return _failure("adapter_mode_mismatch")
    plan = _plan(upstream)
    if plan is None:
        return _failure("missing_upstream")
    if not plan:
        return _failure("no_input_tickers")
    if len(plan) > MAX_PLANNED_REQUESTS:
        return _failure("plan_too_large")

    tracker = _Tracker(UNIT_KIND, planned=len(plan))
    try:
        quotes = adapter.fetch(plan, session=session, sleep=sleep)
    except MarketCapAdapterError as exc:
        if exc.code == "missing_credential":
            return _failure("missing_credential")
        for symbol in plan:
            tracker.fail(symbol, exc.code)
        return _failure("collection_failed", tracker)
    if not isinstance(quotes, Mapping):
        for symbol in plan:
            tracker.fail(symbol, "malformed_response")
        return _failure("collection_failed", tracker)

    now = datetime.now(timezone.utc)
    records = []
    for symbol in plan:
        if symbol not in quotes:
            tracker.fail(symbol, "malformed_response")
            continue
        try:
            records.append(_record(symbol, quotes[symbol], adapter.name, now))
        except _Invalid as invalid:
            tracker.fail(symbol, invalid.code)
            continue
        tracker.ok()
    planned = set(plan)
    for key in quotes:
        if key not in planned:  # extra or respelled: the response is not the plan's
            tracker.fail(key if isinstance(key, str) else "?", "malformed_response")
    if tracker.failure_count:
        return _failure("collection_failed", tracker)

    envelope = {
        "provider": adapter.name,
        "fetched_at": now.isoformat(),
        "total_symbols": len(records),
        "symbols_with_value": sum(1 for r in records if r["value"] is not None),
    }
    return CollectionOutcome.success(
        SOURCE,
        "live",
        records,
        diagnostics=tracker.diagnostics(),
        envelope=envelope,
        coverage={"complete": True, "scope": "selected_universe"},
    )
