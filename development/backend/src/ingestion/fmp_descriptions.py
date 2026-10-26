"""
Company descriptions from FMP's existing profile call (S1-12, decision 75 (a)).

Staging only (Q10), still gated by decision 3's terms review. Decision 1 froze
new FMP adapter work, so this makes exactly the request
runner.collect_market_caps already makes (GET /stable/profile with symbol and
apikey, the same credential) and reads one more field of the response,
`description`. No new endpoint, parameter or budget logic.

Every failure is a DescriptionAdapterError with a fixed code: _get_json's
transport codes (http_NNN, timeout, invalid_json, ...), malformed_response for
a body that isn't one profile of the requested symbol, missing_credential
before any request, and unexpected_error for anything else. The key travels
only in the query string (FMP's API), and never reaches an error, its chained
cause or context, a log line or the stored source link.
"""

from typing import Any

import requests

from src.ingestion.collectors import _fetcher, _get_json
from src.ingestion.company_summaries import (
    Description,
    DescriptionAdapterError,
    DescriptionNotEntitled,
)

PROFILE_URL = "https://financialmodelingprep.com/stable/profile"
# Decision 78 (a): the source link is FMP's public home page for every symbol,
# because its per-company page only loads after logging in. No credential.
SOURCE_URL = "https://financialmodelingprep.com/"
SOURCE_NAME = "Financial Modeling Prep"
REQUEST_TIMEOUT = 10  # as collect_market_caps


def _transport_code(code: str | None) -> str:
    """_get_json's code, reduced to [a-z0-9_]: request_error:<Name> has no detail kept."""
    if not code or code.startswith("request_error:"):
        return "request_failed"
    return code


class FmpProfileDescriptions:
    """A DescriptionAdapter over FMP's /stable/profile response."""

    source_name = SOURCE_NAME

    def __init__(
        self, api_key: str | None = None, *, session: requests.Session | None = None
    ) -> None:
        self._api_key = api_key
        self._session = session

    def __repr__(self) -> str:
        return "FmpProfileDescriptions()"  # never the key

    def _key(self) -> str:
        key = self._api_key
        if key is None:
            key = _fetcher("fmp").FMP_API_KEY  # collect_market_caps's credential
        if (
            not isinstance(key, str)
            or not key.strip()
            or key.lower().startswith("your_")
        ):
            raise DescriptionAdapterError("missing_credential")
        return key

    def fetch(self, symbol: str) -> Description | None:
        code = None
        try:
            return self._fetch(symbol)
        except DescriptionAdapterError:
            raise
        except Exception:  # nothing but a fixed code may leave the adapter
            code = "unexpected_error"
        # Raised outside the except block: no __cause__ or __context__ to leak.
        raise DescriptionAdapterError(code)

    def _fetch(self, symbol: str) -> Description | None:
        key = self._key()
        if self._session is None:
            self._session = requests.Session()
        response = _get_json(
            self._session,
            PROFILE_URL,
            params={"symbol": symbol, "apikey": key},
            timeout=REQUEST_TIMEOUT,
        )
        if not response.ok and response.code == "http_402":
            # Premium-only on this plan (decision 84): this symbol only.
            raise DescriptionNotEntitled("http_402")
        if not response.ok:
            raise DescriptionAdapterError(_transport_code(response.code))
        payload: Any = response.data
        if not isinstance(payload, list) or len(payload) > 1:
            raise DescriptionAdapterError("malformed_response")
        if not payload:
            return None  # FMP has no profile for the symbol
        item = payload[0]
        if not isinstance(item, dict) or item.get("symbol") != symbol:
            raise DescriptionAdapterError("malformed_response")
        text = item.get("description")
        if text is None:
            return None
        if not isinstance(text, str):
            raise DescriptionAdapterError("malformed_response")
        if not text.strip():
            return None
        # Verbatim: selecting the excerpt is the job's.
        return Description(text=text, url=SOURCE_URL)
