"""Map the published ranking payload to the frontend dashboard contract."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict

DashboardSentiment = Literal["Positive", "Neutral", "Watch"]


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


def company_from_record(
    record: Mapping[str, Any], bucket: str
) -> DashboardCompanyResponse:
    """Convert one internal ranking record without changing its source data."""

    ticker = str(record.get("ticker") or "").strip().upper()
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
        name=str(record.get("name") or ticker),
        ticker=ticker,
        sector=str(record.get("sector") or "Unclassified"),
        price=_number(record.get("price", ark_price)),
        change_percent=_number(record.get("change_percent")),
        confidence_score=_number(record.get("confidence_score")),
        sentiment="Watch" if has_warning else ("Positive" if has_signal else "Neutral"),
        reason=_reason(bucket, signals, warnings),
    )


def dashboard_response_from_publication(
    publication: Mapping[str, Any] | None,
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
            company_from_record(record, name)
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
