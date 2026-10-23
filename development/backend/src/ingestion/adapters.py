"""
Typed interfaces between the database collectors and the fetcher modules.

collectors.py loads each fetcher by file, as scheduler/cron.py does, so the
import itself is untyped. These protocols name exactly what the collectors
use from each module: request constants and the strict, shared parsers.
Legacy-only functions (legacy_*) are deliberately absent. The TYPE_CHECKING
block makes mypy check the real modules against them.
"""

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
    runtime_checkable,
)

import requests

from src.ingestion.importer import CollectionOutcome

Row = Dict[str, Any]


class Collector(Protocol):
    """One live collection for one source; provider failures become outcomes.

    Callers pass arguments by keyword: collect_congress takes api_key second.
    """

    def __call__(
        self,
        *,
        session: Optional[requests.Session] = ...,
        sleep: Callable[[float], None] = ...,
    ) -> CollectionOutcome: ...


@runtime_checkable
class ArkAdapter(Protocol):
    @property
    def ARK_API_URL(self) -> str: ...
    @property
    def ARK_FUNDS(self) -> Mapping[str, str]: ...
    @property
    def REQUEST_DELAY(self) -> float: ...
    def parse_holdings(self, all_raw: Dict[str, List[Row]]) -> List[Row]: ...


@runtime_checkable
class FmpAdapter(Protocol):
    @property
    def BASE_URL(self) -> str: ...
    @property
    def FMP_API_KEY(self) -> Optional[str]: ...
    @property
    def MAX_PAGES(self) -> int: ...
    def parse_trades(
        self, raw_data: List[Row], *, preserve_provenance: bool = ...
    ) -> List[Row]: ...
    def deduplicate_proven_repeats(self, trades: List[Row]) -> List[Row]: ...
    def filter_recent(self, trades: List[Row], years: int = ...) -> List[Row]: ...


@runtime_checkable
class InsiderAdapter(Protocol):
    @property
    def FINNHUB_API_KEY(self) -> str: ...
    @property
    def FINNHUB_URL(self) -> str: ...
    @property
    def LOCAL_DATA_DIR(self) -> Path: ...
    @property
    def LOOKBACK_DAYS(self) -> int: ...
    @property
    def MAX_CONGRESS_TICKERS(self) -> int: ...
    @property
    def MIN_BUY_VALUE(self) -> int: ...
    @property
    def REQUEST_DELAY(self) -> float: ...
    @property
    def SOURCE_NAME(self) -> str: ...
    def lookback_cutoff(self) -> date: ...
    def provider_symbol(self, ticker: str) -> Optional[str]: ...
    def parse_finnhub_buys(
        self, ticker: str, raw: Any, bucket: str
    ) -> Tuple[List[Row], Optional[str], Optional[str]]: ...


@runtime_checkable
class ShortInterestAdapter(Protocol):
    @property
    def HEADERS(self) -> Dict[str, str]: ...
    @property
    def LOCAL_DATA_DIR(self) -> Path: ...
    @property
    def NASDAQ_URL(self) -> str: ...
    @property
    def REQUEST_DELAY(self) -> float: ...
    @property
    def REQUEST_TIMEOUT(self) -> int: ...
    def normalize_ticker(self, ticker: str) -> Optional[str]: ...
    def parse_short_interest_strict(self, ticker: str, raw: Row) -> Optional[Row]: ...


if TYPE_CHECKING:
    # Resolved through mypy_path (pyproject.toml), as cron.py imports them.
    import ark
    import fmp
    import insider
    import short_interest

    _ark: ArkAdapter = ark
    _fmp: FmpAdapter = fmp
    _insider: InsiderAdapter = insider
    _short_interest: ShortInterestAdapter = short_interest
