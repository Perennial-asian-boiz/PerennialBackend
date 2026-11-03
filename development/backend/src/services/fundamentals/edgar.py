"""SEC EDGAR companyfacts: annual line items per company.

SEC fair-access rules: at most 10 requests a second, and a User-Agent that names
a contact (Settings.sec_user_agent). Retries, size caps and JSON validation come
from the shared collectors._get_json.

S1-12 (ED-2, ED-6) adds a company's submissions list and raw archive documents
(10-K HTML) through the same paced _get. Market caps add the public float on
the 10-K cover (dei:EntityPublicFloat), one small companyconcept request.
"""

import time
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Mapping, TypeGuard
from urllib.parse import urlsplit

import requests
from src.ingestion.collectors import _get_json

TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
PUBLIC_FLOAT_URL = "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik:010d}/dei/EntityPublicFloat.json"
ARCHIVE_HOST = "www.sec.gov"
ARCHIVE_PATH = "/Archives/edgar/data/"
# Bounds the HTML parsing work per 10-K (F1). The download itself is still
# bounded by the shared collectors.MAX_RESPONSE_BYTES (50 MiB).
MAX_DOCUMENT_BYTES = 20 * 1024 * 1024
MIN_REQUEST_INTERVAL_SECONDS = 0.12
ANNUAL_FORMS = frozenset({"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"})
FISCAL_YEARS_KEPT = 6  # 3-year growth, last year's trend and the 5-year chart

# line item -> (us-gaap concepts, ifrs-full concepts); per fiscal year, the first
# concept that reports a value wins, because companies change concepts over time.
CONCEPTS: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "revenue": (
        (
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
            "SalesRevenueNet",
            "SalesRevenueGoodsNet",
        ),
        ("Revenue", "RevenueFromContractsWithCustomers"),
    ),
    "revenue_net_of_interest": (("RevenuesNetOfInterestExpense",), ()),
    "gross_profit": (("GrossProfit",), ("GrossProfit",)),
    "cost_of_revenue": (
        ("CostOfRevenue", "CostOfGoodsAndServicesSold", "CostOfGoodsSold"),
        ("CostOfSales",),
    ),
    "operating_income": (
        ("OperatingIncomeLoss",),
        ("ProfitLossFromOperatingActivities",),
    ),
    "pretax_income": (
        (
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
        ),
        ("ProfitLossBeforeTax",),
    ),
    "net_income": (
        ("NetIncomeLoss", "ProfitLoss"),
        ("ProfitLossAttributableToOwnersOfParent", "ProfitLoss"),
    ),
    "operating_cash_flow": (
        ("NetCashProvidedByUsedInOperatingActivities",),
        ("CashFlowsFromUsedInOperatingActivities",),
    ),
    "assets": (("Assets",), ("Assets",)),
    "current_assets": (("AssetsCurrent",), ("CurrentAssets",)),
    "current_liabilities": (("LiabilitiesCurrent",), ("CurrentLiabilities",)),
    "liabilities": (("Liabilities",), ("Liabilities",)),
    "long_term_debt": (
        (
            "LongTermDebtNoncurrent",
            "LongTermDebt",
            "LongTermDebtAndCapitalLeaseObligations",
        ),
        ("NoncurrentPortionOfNoncurrentBorrowings", "LongtermBorrowings"),
    ),
    "equity": (
        ("StockholdersEquity",),
        ("EquityAttributableToOwnersOfParent", "Equity"),
    ),
    "diluted_shares": (
        ("WeightedAverageNumberOfDilutedSharesOutstanding",),
        ("AdjustedWeightedAverageShares",),
    ),
    "noninterest_expense": (("NoninterestExpense",), ()),
}
# Duration items cover one fiscal year; the rest are balances at year end.
FLOW_ITEMS = frozenset(
    {
        "revenue",
        "revenue_net_of_interest",
        "gross_profit",
        "cost_of_revenue",
        "operating_income",
        "pretax_income",
        "net_income",
        "operating_cash_flow",
        "diluted_shares",
        "noninterest_expense",
    }
)


def user_agent_ok(agent: str | None) -> TypeGuard[str]:
    """False for a missing or blank User-Agent, or one naming example.com (the
    .env.example placeholder, you@example.com), which must never go out as SEC's
    contact."""
    return (
        agent is not None and agent.strip() != "" and "example.com" not in agent.lower()
    )


class EdgarError(RuntimeError):
    """A request failed; carries a short code, never response text."""


@dataclass(frozen=True)
class PublicFloat:
    """The USD market value of shares held by non-affiliates, from the 10-K cover.

    as_of is the date it was measured: the last business day of the company's
    second fiscal quarter, so a value is up to about 20 months old before the
    next 10-K replaces it.
    """

    value: Decimal
    as_of: date
    filed: date
    form: str


def latest_public_float(data: Any) -> PublicFloat | None:
    """The latest-measured USD public float in a companyconcept response, or None.

    Latest by measurement date, then filing date (an amendment wins). None
    when there is no USD value, or the latest is zero or less: a company that
    listed after its measurement date reports 0, and an older value would
    describe a different company size. A malformed response raises.
    """
    units = data.get("units") if isinstance(data, dict) else None
    if not isinstance(units, dict):
        raise EdgarError("public_float_malformed")
    entries = units.get("USD", [])
    if not isinstance(entries, list):
        raise EdgarError("public_float_malformed")
    best: PublicFloat | None = None
    for entry in entries:
        try:
            raw = entry["val"]
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                raise ValueError
            candidate = PublicFloat(
                value=Decimal(str(raw)),
                as_of=date.fromisoformat(entry["end"]),
                filed=date.fromisoformat(entry["filed"]),
                form=str(entry.get("form", "")),
            )
        except (KeyError, TypeError, ValueError, InvalidOperation):
            raise EdgarError("public_float_malformed") from None
        if not candidate.value.is_finite():
            raise EdgarError("public_float_malformed")
        if best is None or (candidate.as_of, candidate.filed) > (
            best.as_of,
            best.filed,
        ):
            best = candidate
    if best is None or best.value <= 0:
        return None
    return best


@dataclass(frozen=True)
class AnnualValue:
    fiscal_year_end: date
    line_item: str
    value: float
    unit: str
    concept: str
    form: str
    filed: date


def _annual_entries(entries: Iterable[Any], flow: bool) -> dict[date, dict[str, Any]]:
    """Latest-filed annual value per fiscal year end; flow items must span ~1 year."""
    out: dict[date, dict[str, Any]] = {}
    for e in entries:
        if not isinstance(e, dict) or e.get("form") not in ANNUAL_FORMS:
            continue
        if e.get("fp") != "FY" or not isinstance(e.get("val"), (int, float)):
            continue
        try:
            end = date.fromisoformat(e["end"])
            filed = date.fromisoformat(e["filed"])
            if flow:
                days = (end - date.fromisoformat(e["start"])).days
                if not 350 <= days <= 380:
                    continue
        except (KeyError, TypeError, ValueError):
            continue
        if isinstance(e["val"], bool):
            continue
        prev = out.get(end)
        if prev is None or filed > prev["filed"]:
            out[end] = {"val": float(e["val"]), "filed": filed, "form": e["form"]}
    return out


def annual_values(
    facts: Mapping[str, Any], keep: int = FISCAL_YEARS_KEPT
) -> list[AnnualValue]:
    """Every line item's annual values for the latest `keep` fiscal years.

    Money items use USD when reported, otherwise the filing currency; shares use
    the "shares" unit. One unit per line item, so a series never mixes currencies.
    """
    found: list[AnnualValue] = []
    for item, (gaap, ifrs) in CONCEPTS.items():
        merged: dict[date, AnnualValue] = {}
        unit_used: str | None = None
        for taxonomy, names in (("us-gaap", gaap), ("ifrs-full", ifrs)):
            for name in names:
                units = facts.get(taxonomy, {}).get(name, {}).get("units", {})
                if not isinstance(units, dict) or not units:
                    continue
                if item == "diluted_shares":
                    unit = "shares"
                else:
                    unit = unit_used or ("USD" if "USD" in units else next(iter(units)))
                if unit not in units or (unit != "shares" and len(unit) != 3):
                    continue
                unit_used = unit
                for end, e in _annual_entries(units[unit], item in FLOW_ITEMS).items():
                    merged.setdefault(
                        end,
                        AnnualValue(
                            end, item, e["val"], unit, name, e["form"], e["filed"]
                        ),
                    )
        found.extend(merged[end] for end in sorted(merged)[-keep:])
    return found


class EdgarClient:
    def __init__(
        self,
        session: requests.Session,
        user_agent: str,
        *,
        interval: float = MIN_REQUEST_INTERVAL_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.session, self.headers = session, {"User-Agent": user_agent}
        self.interval, self.monotonic, self.sleep = interval, monotonic, sleep
        self._last: float | None = None

    def _get(self, url: str, allowed: tuple[int, ...] = (), raw: bool = False) -> Any:
        if self._last is not None:
            wait = self.interval - (self.monotonic() - self._last)
            if wait > 0:
                self.sleep(wait)
        self._last = self.monotonic()
        response = _get_json(
            self.session, url, headers=self.headers, allowed_status=allowed, raw=raw
        )
        if not response.ok:
            raise EdgarError(response.code or "request_failed")
        return response

    def _ticker_rows(self) -> list[dict[str, Any]]:
        data = self._get(TICKER_MAP_URL).data
        if not isinstance(data, dict):
            raise EdgarError("ticker_map_malformed")
        return [
            row
            for row in data.values()
            if isinstance(row, dict) and isinstance(row.get("ticker"), str)
        ]

    def ticker_map(self) -> dict[str, int]:
        """SEC's ticker -> CIK map. Share-class punctuation uses '-' (BRK-B)."""
        out: dict[str, int] = {}
        for row in self._ticker_rows():
            cik = row.get("cik_str")
            if isinstance(cik, int) and not isinstance(cik, bool):
                out.setdefault(row["ticker"].upper(), cik)
        return out

    def ticker_titles(self) -> dict[str, str]:
        """SEC's ticker -> company title as filed, from the same list (decision 92).

        A blank or non-string title is left out.
        """
        out: dict[str, str] = {}
        for row in self._ticker_rows():
            title = row.get("title")
            if isinstance(title, str) and title.strip():
                out.setdefault(row["ticker"].upper(), title)
        return out

    def company_facts(self, cik: int) -> dict[str, Any] | None:
        """The company's facts, or None when SEC has none for this CIK (404)."""
        response = self._get(FACTS_URL.format(cik=cik), allowed=(404,))
        if response.status == 404:
            return None
        facts = response.data.get("facts") if isinstance(response.data, dict) else None
        if not isinstance(facts, dict):
            raise EdgarError("facts_malformed")
        return facts

    def public_float(self, cik: int) -> PublicFloat | None:
        """The company's latest public float, or None when SEC has none (404)."""
        response = self._get(PUBLIC_FLOAT_URL.format(cik=cik), allowed=(404,))
        if response.status == 404:
            return None
        return latest_public_float(response.data)

    def submissions(self, cik: int) -> dict[str, Any] | None:
        """The company's submissions list, or None when SEC has none for this CIK (404)."""
        response = self._get(SUBMISSIONS_URL.format(cik=cik), allowed=(404,))
        if response.status == 404:
            return None
        if not isinstance(response.data, dict):
            raise EdgarError("submissions_malformed")
        return response.data

    def document(self, url: str) -> bytes | None:
        """An archive document's raw bytes; None when it's gone (404) or over a cap.

        Only https://www.sec.gov/Archives/edgar/data/ URLs without "." or ".."
        path segments are fetched; anything else is a bug in the caller and is
        refused before any request.
        """
        parts = urlsplit(url)
        if (
            parts.scheme != "https"
            or parts.netloc != ARCHIVE_HOST
            or not parts.path.startswith(ARCHIVE_PATH)
            or any(segment in (".", "..") for segment in parts.path.split("/"))
            or parts.query
            or parts.fragment
        ):
            raise EdgarError("invalid_url")
        # A 404 or an oversized body is a permanent per-filing condition, not a failure.
        try:
            response = self._get(url, allowed=(404,), raw=True)
        except EdgarError as exc:
            if exc.args == ("response_too_large",):
                return None
            raise
        body: bytes | None = response.data
        if response.status == 404 or body is None or len(body) > MAX_DOCUMENT_BYTES:
            return None
        return body
