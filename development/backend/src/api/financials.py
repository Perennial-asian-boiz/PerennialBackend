"""S2-04: a security's yearly SEC EDGAR figures, for GET /v1/securities/{symbol}/financials.

Serves every fiscal year in the security's newest fundamentals batch, newest
first. Only successful imports store rows, so that batch is the latest good data.
The route reads only the nine fundamentals columns perennial_api is granted
(decision 113), never concept or row_number.

Computed here, each null when an input is missing, the two units differ or the
divisor is <= 0:

* revenue_growth: this year's revenue / the next-older year's - 1, when that year
  ended about a year earlier (365 +/- 20 days, as the fundamentals score);
* net_margin: net income / revenue;
* debt_to_equity: long-term debt / equity.

A bank (the fundamentals score's is_bank) uses revenue net of interest, as the
score does, and revenue_basis names the line item served as revenue.
"""

from datetime import date
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.engine import Connection

from src.db import models as m
from src.services.fundamentals.scoring import is_bank

SOURCE = "SEC EDGAR"
SERVED_ITEMS = (
    "revenue",
    "net_income",
    "long_term_debt",
    "equity",
    "operating_cash_flow",
)
# Exactly provision_roles.API_COLUMNS["fundamentals"] (src can't import ops).
COLUMNS = (
    "id",
    "batch_id",
    "security_id",
    "fiscal_year_end",
    "line_item",
    "value",
    "unit",
    "form",
    "filed",
)
YEAR_TOLERANCE_DAYS = 20  # as the fundamentals score's _year_before
PLACES = 6

Figure = tuple[Decimal, str]  # (value, unit)


class Amount(BaseModel):
    value: float
    unit: str = Field(description="As filed: an ISO currency code, e.g. USD")


class FinancialYear(BaseModel):
    fiscal_year_end: str
    form: str = Field(description="The form of the year's latest filed figure")
    filed: str = Field(description="When that figure was filed")
    revenue: Amount | None = Field(
        description="Revenue net of interest for a bank (see revenue_basis)"
    )
    net_income: Amount | None
    long_term_debt: Amount | None
    equity: Amount | None
    operating_cash_flow: Amount | None
    revenue_growth: float | None = Field(
        description="Against the next-older year, if it ended about a year earlier"
    )
    net_margin: float | None
    debt_to_equity: float | None


class FinancialsResponse(BaseModel):
    api_version: str = Field(examples=["v1"])
    symbol: str = Field(description="The canonical ticker")
    source: str = Field(examples=[SOURCE])
    status: str = Field(description="ok, or unavailable when nothing is stored")
    batch_id: int | None = Field(description="The fundamentals batch served")
    revenue_basis: str | None = Field(
        description="revenue, or revenue_net_of_interest for a bank; null when unavailable"
    )
    years: list[FinancialYear] = Field(
        description="Newest first; empty when unavailable"
    )


UNAVAILABLE: dict[str, Any] = {
    "source": SOURCE,
    "status": "unavailable",
    "batch_id": None,
    "revenue_basis": None,
    "years": [],
}


def _amount(figure: Figure | None) -> dict[str, Any] | None:
    return None if figure is None else {"value": float(figure[0]), "unit": figure[1]}


def _ratio(top: Figure | None, bottom: Figure | None) -> Decimal | None:
    if top is None or bottom is None or top[1] != bottom[1] or bottom[0] <= 0:
        return None
    return top[0] / bottom[0]


def _served(value: Decimal | None) -> float | None:
    return None if value is None else float(round(value, PLACES))


def served(conn: Connection, security_id: int) -> tuple[dict[str, Any], str]:
    """(body without api_version and symbol, ETag). Stored batches never change, so
    the batch id is the ETag."""
    f = m.fundamentals
    batch = conn.execute(
        select(func.max(f.c.batch_id)).where(f.c.security_id == security_id)
    ).scalar()
    if batch is None:
        return dict(UNAVAILABLE), '"v1-financials-0"'
    rows = conn.execute(
        select(*(f.c[name] for name in COLUMNS)).where(
            f.c.batch_id == batch, f.c.security_id == security_id
        )
    ).all()
    figures: dict[str, dict[date, Figure]] = {}
    filings: dict[date, tuple[date, str]] = {}
    for row in rows:
        end = row.fiscal_year_end
        figures.setdefault(row.line_item, {})[end] = (row.value, row.unit)
        filings[end] = max(
            filings.get(end, (row.filed, row.form)), (row.filed, row.form)
        )

    lines = {
        item: {d: float(v) for d, (v, _) in s.items()} for item, s in figures.items()
    }
    basis = (
        "revenue_net_of_interest"
        if is_bank(lines) and figures.get("revenue_net_of_interest")
        else "revenue"
    )
    revenue = figures.get(basis, {})
    ends = sorted(filings, reverse=True)
    years = []
    for i, end in enumerate(ends):
        prior = ends[i + 1] if i + 1 < len(ends) else None
        prior_revenue = (
            revenue.get(prior)
            if prior is not None
            and abs((end - prior).days - 365) <= YEAR_TOLERANCE_DAYS
            else None
        )
        item = {name: figures.get(name, {}).get(end) for name in SERVED_ITEMS}
        item["revenue"] = revenue.get(end)
        growth = _ratio(item["revenue"], prior_revenue)
        filed, form = filings[end]
        years.append(
            {
                "fiscal_year_end": end.isoformat(),
                "form": form,
                "filed": filed.isoformat(),
                **{name: _amount(item[name]) for name in SERVED_ITEMS},
                "revenue_growth": _served(None if growth is None else growth - 1),
                "net_margin": _served(_ratio(item["net_income"], item["revenue"])),
                "debt_to_equity": _served(
                    _ratio(item["long_term_debt"], item["equity"])
                ),
            }
        )
    body = {
        "source": SOURCE,
        "status": "ok",
        "batch_id": int(batch),
        "revenue_basis": basis,
        "years": years,
    }
    return body, f'"v1-financials-{batch}"'
