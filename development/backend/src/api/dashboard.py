"""Map the published ranking payload to the frontend dashboard contract."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict

DashboardSentiment = Literal["Positive", "Neutral", "Watch"]

UNCLASSIFIED = "Unclassified"
# The frontend's own sector list: it filters the dashboard by exact name.
FRONTEND_SECTORS = (
    "Technology",
    "Healthcare",
    "Financials",
    "Industrials",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Utilities",
    "Materials",
    "Communication Services",
    "Real Estate",
)
_SECTOR_NAMES = {
    **{sector: sector for sector in FRONTEND_SECTORS},
    # FMP's names for the profile sectors (company_profiles.sector).
    "Financial Services": "Financials",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Basic Materials": "Materials",
    # GICS names that differ from the frontend's.
    "Information Technology": "Technology",
    "Health Care": "Healthcare",
}


def dashboard_sector(value: str | None) -> str:
    """A stored profile sector as one of the frontend's sectors, or Unclassified."""
    if not isinstance(value, str):
        return UNCLASSIFIED
    return _SECTOR_NAMES.get(value, UNCLASSIFIED)


@dataclass(frozen=True)
class Display:
    """What the database says about a ticker: securities.name and the profile sector."""

    name: str | None
    sector: str | None


class DashboardCompanyResponse(BaseModel):
    """Display-ready company data consumed by the frontend dashboard."""

    model_config = ConfigDict(from_attributes=True)

    name: str
    ticker: str
    sector: str
    price: float | None
    change_percent: float | None
    confidence_score: float | None
    sentiment: DashboardSentiment
    reason: str | None


class DashboardResponse(BaseModel):
    """Published dashboard data exposed by ``GET /api/watchlist``."""

    schema_version: str
    generated_at: str
    affordable_growing: list[DashboardCompanyResponse]
    popular_stable: list[DashboardCompanyResponse]


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    return float(value)


def _reason(bucket: str, signals: Mapping[str, Any], warnings: list[Any]) -> str:
    active_signals = [
        name.replace("_", " ").title() for name, value in signals.items() if value
    ]
    bucket_label = (
        "growth" if bucket == "affordable_growing" else "stability and popularity"
    )
    if active_signals:
        reason = f"Published {bucket_label} signals: {', '.join(active_signals)}."
    else:
        reason = f"Published {bucket_label} ranking data is available."
    if warnings:
        reason += " Review the published data warnings before making a decision."
    return reason


def _ticker(record: Mapping[str, Any]) -> str:
    return str(record.get("ticker") or "").strip().upper()


def company_from_record(
    record: Mapping[str, Any],
    bucket: str,
    display: Mapping[str, Display] | None = None,
) -> DashboardCompanyResponse:
    """Convert one internal ranking record without changing its source data.

    With ``display`` (the database lookup), name and sector come from the
    security and its profile, and a ticker it lacks is Unclassified. Without
    it, a record-carried name and sector are kept.
    """

    ticker = _ticker(record)
    if display is None:
        name = str(record.get("name") or ticker)
        sector = str(record.get("sector") or UNCLASSIFIED)
    else:
        found = display.get(ticker)
        name = (found.name if found else None) or ticker
        sector = dashboard_sector(found.sector if found else None)
    signals = record.get("signals")
    if not isinstance(signals, Mapping):
        signals = {}
    warnings = record.get("warnings")
    if not isinstance(warnings, list):
        warnings = []

    ark_signal = signals.get("ark")
    ark_price = (
        ark_signal.get("share_price") if isinstance(ark_signal, Mapping) else None
    )
    has_warning = bool(warnings)
    has_signal = any(value is not None for value in signals.values())

    return DashboardCompanyResponse(
        name=name,
        ticker=ticker,
        sector=sector,
        price=_number(record.get("price", ark_price)),
        change_percent=_number(record.get("change_percent")),
        confidence_score=_number(record.get("confidence_score")),
        sentiment="Watch" if has_warning else ("Positive" if has_signal else "Neutral"),
        reason=_reason(bucket, signals, warnings),
    )


DASHBOARD_BUCKETS = ("affordable_growing", "popular_stable")


def publication_tickers(publication: Mapping[str, Any] | None) -> list[str]:
    """The tickers the dashboard shows, from both of its buckets."""
    output = publication.get("output") if publication is not None else None
    if not isinstance(output, Mapping):
        return []
    tickers = {
        _ticker(record)
        for bucket in DASHBOARD_BUCKETS
        if isinstance(output.get(bucket), list)
        for record in output[bucket]
        if isinstance(record, Mapping)
    }
    return sorted(t for t in tickers if t)


def dashboard_response_from_publication(
    publication: Mapping[str, Any] | None,
    display: Mapping[str, Display] | None = None,
) -> DashboardResponse:
    """Build a stable response from the current publication, including no-data state."""

    if publication is None:
        return DashboardResponse(
            schema_version="1.0",
            generated_at="",
            affordable_growing=[],
            popular_stable=[],
        )

    output = publication.get("output")
    if not isinstance(output, Mapping):
        output = {}

    def bucket_records(name: str) -> list[DashboardCompanyResponse]:
        records = output.get(name)
        if not isinstance(records, list):
            return []
        return [
            company_from_record(record, name, display)
            for record in records
            if isinstance(record, Mapping)
        ]

    generated_at = output.get("generated_at") or publication.get("created_at")
    return DashboardResponse(
        schema_version=str(output.get("schema_version") or "1.0"),
        generated_at=str(generated_at or ""),
        affordable_growing=bucket_records("affordable_growing"),
        popular_stable=bucket_records("popular_stable"),
    )
