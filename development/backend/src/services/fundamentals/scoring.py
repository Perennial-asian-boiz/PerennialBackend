"""The fundamentals score (pattern evaluation), docs/work-items/pattern-evaluation-design.md.

Pure functions over one security's annual line items: no network, no database.
Every limit and rule parameter is part of ScoreRule and stored in the derived
batch's envelope, so a stored score can always be explained and replayed.
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping

Series = Mapping[date, float]
Lines = Mapping[str, Series]

# measure: (weight, value worth 0 points, value worth 100 points). Lower is better
# when the first limit is larger than the second (the efficiency ratio).
GENERAL_LIMITS = {
    "revenue_growth_3y": (0.50, 0.00, 0.40),
    "operating_margin": (0.30, -0.20, 0.30),
    "operating_margin_trend": (0.20, -0.10, 0.10),
}
BANK_LIMITS = {
    "revenue_growth_3y": (0.50, 0.00, 0.25),
    "return_on_equity": (0.30, 0.00, 0.20),
    "efficiency_ratio": (0.20, 0.80, 0.50),
}
MEASURES = (
    "revenue_growth_3y",
    "operating_margin",
    "operating_margin_trend",
    "return_on_equity",
    "efficiency_ratio",
)


@dataclass(frozen=True)
class ScoreRule:
    version: str = "fundamentals-v1"
    general: Mapping[str, tuple[float, float, float]] = field(
        default_factory=lambda: dict(GENERAL_LIMITS)
    )
    bank: Mapping[str, tuple[float, float, float]] = field(
        default_factory=lambda: dict(BANK_LIMITS)
    )
    min_measures: int = 2
    max_margin: float = 1.0  # margins beyond +/-100% are not meaningful
    growth_years: int = 3

    def envelope(self) -> dict[str, Any]:
        def limits(spec: Mapping[str, tuple[float, float, float]]) -> list[str]:
            return [f"{k}:{w}:{lo}:{hi}" for k, (w, lo, hi) in spec.items()]

        return {
            "score_version": self.version,
            "general_limits": limits(self.general),
            "bank_limits": limits(self.bank),
            "min_measures": self.min_measures,
            "max_margin": self.max_margin,
        }


def points(value: float, lo: float, hi: float) -> float:
    """value on 0-100 between lo (0 points) and hi (100 points), capped at both ends."""
    share = (value - lo) / (hi - lo)
    return round(100 * min(1.0, max(0.0, share)), 1)


def combine(
    measures: Mapping[str, float | None], spec: Mapping[str, tuple[float, float, float]]
) -> tuple[float, dict[str, dict[str, float]]]:
    """Weighted score over the present measures; a missing measure's weight is spread
    over the rest (DECISIONS.md rule 4). Callers ensure at least one is present."""
    present = {k: v for k, v in measures.items() if v is not None}
    total = sum(spec[k][0] for k in present)
    parts = {}
    for k, v in present.items():
        weight, lo, hi = spec[k]
        parts[k] = {"value": v, "points": points(v, lo, hi), "weight": weight / total}
    return round(sum(p["points"] * p["weight"] for p in parts.values()), 1), parts


def _year_before(series: Series, end: date, years: int) -> date | None:
    for d in series:
        if abs((end - d).days - 365 * years) <= 20:
            return d
    return None


def growth(series: Series, end: date, years: int) -> float | None:
    start = _year_before(series, end, years)
    if start is None or series[start] <= 0 or series[end] <= 0:
        return None
    return float((series[end] / series[start]) ** (1 / years)) - 1


def profit_series(lines: Lines, end: date) -> tuple[Series, str | None]:
    """Operating income, or pre-tax income when operating income isn't reported for
    the latest year. One source for all years, so the trend compares like with like."""
    if end in lines.get("operating_income", {}):
        return lines["operating_income"], "operating_income"
    if end in lines.get("pretax_income", {}):
        return lines["pretax_income"], "pretax_income"
    return {}, None


def general_measures(
    lines: Lines, end: date, rule: ScoreRule
) -> tuple[dict[str, float | None], str | None]:
    rev = lines["revenue"]
    profit, source = profit_series(lines, end)
    prev = _year_before(rev, end, 1)
    margin = profit[end] / rev[end] if end in profit and rev[end] > 0 else None
    prev_margin = None
    if prev is not None and prev in profit and rev[prev] > 0:
        prev_margin = profit[prev] / rev[prev]
    trend = None
    if margin is not None and prev_margin is not None:
        if max(abs(margin), abs(prev_margin)) <= rule.max_margin:
            trend = margin - prev_margin
    growth_3y = growth(rev, end, rule.growth_years)
    if margin is not None and margin < -rule.max_margin:
        # Costs over twice revenue: growth from such a small base flatters (CRSP).
        growth_3y = None
    if margin is not None and margin > rule.max_margin:
        margin = None  # non-operating gains booked as income, not an operating margin
    return {
        "revenue_growth_3y": growth_3y,
        "operating_margin": margin,
        "operating_margin_trend": trend,
    }, source


def bank_measures(lines: Lines, end: date, rule: ScoreRule) -> dict[str, float | None]:
    rev = bank_revenue(lines)
    ni, eq = lines.get("net_income", {}), lines.get("equity", {})
    expense = lines.get("noninterest_expense", {})
    prev = _year_before(eq, end, 1)
    roe = None
    if end in ni and end in eq and prev is not None:
        average = (eq[end] + eq[prev]) / 2
        roe = ni[end] / average if average > 0 else None
    efficiency = expense[end] / rev[end] if end in expense and rev[end] > 0 else None
    return {
        "revenue_growth_3y": growth(rev, end, rule.growth_years),
        "return_on_equity": roe,
        "efficiency_ratio": efficiency,
    }


def gross_margin(lines: Lines, end: date) -> float | None:
    rev = lines.get("revenue", {}).get(end)
    if not rev:
        return None
    gp = lines.get("gross_profit", {}).get(end)
    if gp is not None:
        return gp / rev
    cost = lines.get("cost_of_revenue", {}).get(end)
    return (rev - cost) / rev if cost is not None else None


def piotroski(lines: Lines, end: date) -> tuple[int | None, int]:
    """F-Score over the latest fiscal year vs the one before: (score, signals known).
    Absent long-term debt counts as 0 when total liabilities are reported."""
    rev = lines.get("revenue", {})
    prev = _year_before(rev, end, 1)
    if prev is None:
        return None, 0

    def get(item: str, year: date) -> float | None:
        value = lines.get(item, {}).get(year)
        if (
            value is None
            and item == "long_term_debt"
            and year in lines.get("liabilities", {})
        ):
            return 0.0
        return value

    def ratio(a: str, b: str, year: date) -> float | None:
        x, y = get(a, year), get(b, year)
        return x / y if x is not None and y not in (None, 0) else None

    def better(
        now: float | None, before: float | None, lower: bool = False
    ) -> bool | None:
        if now is None or before is None:
            return None
        return now < before if lower else now > before

    roa = ratio("net_income", "assets", end)
    ocf, ni = get("operating_cash_flow", end), get("net_income", end)
    shares, shares_prev = get("diluted_shares", end), get("diluted_shares", prev)
    signals = [
        None if roa is None else roa > 0,
        None if ocf is None else ocf > 0,
        better(roa, ratio("net_income", "assets", prev)),
        None if ocf is None or ni is None else ocf > ni,
        better(
            ratio("long_term_debt", "assets", end),
            ratio("long_term_debt", "assets", prev),
            lower=True,
        ),
        better(
            ratio("current_assets", "current_liabilities", end),
            ratio("current_assets", "current_liabilities", prev),
        ),
        None if shares is None or shares_prev is None else shares <= shares_prev,
        better(gross_margin(lines, end), gross_margin(lines, prev)),
        better(ratio("revenue", "assets", end), ratio("revenue", "assets", prev)),
    ]
    known = [s for s in signals if s is not None]
    return (sum(known) if known else None), len(known)


def bank_revenue(lines: Lines) -> Series:
    """Revenue net of interest expense. Some banks (BAC) report it only as Revenues."""
    return lines.get("revenue_net_of_interest") or lines.get("revenue", {})


def is_bank(lines: Lines) -> bool:
    """Bank model when the filings report non-interest expense for the latest revenue year."""
    rev, expense = bank_revenue(lines), lines.get("noninterest_expense", {})
    return bool(rev) and bool(expense) and max(rev) == max(expense)


def score_security(
    ticker: str, lines: Lines, rule: ScoreRule = ScoreRule()
) -> dict[str, Any] | None:
    """One security's fundamental_scores record, or None when it has no annual data."""
    if not any(lines.values()):
        return None
    bank = is_bank(lines)
    model = "bank" if bank else "general"
    rev = bank_revenue(lines) if bank else lines.get("revenue", {})
    record: dict[str, Any] = {
        "ticker": ticker,
        "model": model,
        "score": None,
        "missing": [],
        "piotroski_signals": 0,
        "score_version": rule.version,
        **{m: None for m in MEASURES},
    }
    if not rev or rev[max(rev)] <= 0:
        record["fiscal_year_end"] = max(rev).isoformat() if rev else None
        record["missing"] = ["no_revenue"]
        return record
    end = max(rev)
    record["fiscal_year_end"] = end.isoformat()
    if bank:
        measures, source = bank_measures(lines, end, rule), None
    else:
        measures, source = general_measures(lines, end, rule)
        record["gross_margin"] = gross_margin(lines, end)
        record["piotroski_score"], record["piotroski_signals"] = piotroski(lines, end)
    record["profit_source"] = source
    record.update(
        {k: (round(v, 6) if v is not None else None) for k, v in measures.items()}
    )
    if growth(rev, end, rule.growth_years) is None:
        record["missing"].append("insufficient_history")
    if sum(v is not None for v in measures.values()) < rule.min_measures:
        record["missing"].insert(0, "not_enough_data")
        return record
    record["score"], _ = combine(measures, rule.bank if bank else rule.general)
    return record


def breakdown(
    record: Mapping[str, Any], rule: ScoreRule = ScoreRule()
) -> dict[str, dict[str, float]]:
    """Points and effective weight per measure for a stored record (the breakdown screen)."""
    spec = rule.bank if record.get("model") == "bank" else rule.general
    measures = {
        k: (float(record[k]) if record.get(k) is not None else None) for k in spec
    }
    if not any(v is not None for v in measures.values()):
        return {}
    return combine(measures, spec)[1]


def key_reasons(record: Mapping[str, Any]) -> list[str]:
    """Plain-language reasons for the "Why This Company" screen."""
    reasons = []

    def value(k: str) -> float | None:
        return float(record[k]) if record.get(k) is not None else None

    if (g := value("revenue_growth_3y")) is not None:
        reasons.append(
            f"Revenue {'grew' if g >= 0 else 'shrank'} {abs(g) * 100:.0f}% a year over the last 3 years."
        )
    if (m := value("operating_margin")) is not None:
        kind = (
            "pre-tax profit"
            if record.get("profit_source") == "pretax_income"
            else "operating profit"
        )
        if m >= 0:
            reasons.append(
                f"Keeps {m * 100:.0f} cents of every sales dollar as {kind}."
            )
        else:
            loss = kind.replace("profit", "loss")
            reasons.append(
                f"Spends more than it earns: {loss} of {abs(m) * 100:.0f} cents per sales dollar."
            )
    if (t := value("operating_margin_trend")) is not None:
        reasons.append(
            f"Profitability {'improved' if t >= 0 else 'worsened'} {abs(t) * 100:.0f} points from last year."
        )
    if (r := value("return_on_equity")) is not None:
        reasons.append(f"Earns {r * 100:.0f}% a year on shareholders' money.")
    if (e := value("efficiency_ratio")) is not None:
        reasons.append(f"Spends {e * 100:.0f} cents to earn each dollar of revenue.")
    if (gm := value("gross_margin")) is not None and record.get("model") == "general":
        reasons.append(f"Gross margin: {gm * 100:.0f}%.")
    return reasons
