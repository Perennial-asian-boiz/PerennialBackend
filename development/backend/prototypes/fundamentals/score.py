"""Prototype fundamentals score (pattern evaluation) from SEC EDGAR annual reports.

Implements docs/work-items/pattern-evaluation-design.md, version 1. Prototype only:
no database, no migration. Writes fundamentals_scores.json next to this file and
prints a summary, including the Spearman rank correlation with the Piotroski F-Score.

    cd development/backend/prototypes/fundamentals
    ../../../../.venv/bin/python score.py

Needs SEC_USER_AGENT in development/backend/.env.
"""
import json
import time
from datetime import date, datetime, timezone

import requests

from edgar_probe import HERE, UA, currency, series

SCORE_VERSION = "fundamentals-v1.1-prototype"
MIN_MEASURES = 2  # fewer present measures than this: not scored ("not_enough_data")
MAX_MARGIN = 1.0  # operating margins beyond +/-100% are not meaningful for the rules below
# measure: (weight, value scoring 0 points, value scoring 100 points). Lower-is-better when lo > hi.
GENERAL = {
    "revenue_growth_3y": (0.50, 0.00, 0.40),
    "operating_margin": (0.30, -0.20, 0.30),
    "operating_margin_trend": (0.20, -0.10, 0.10),
}
BANK = {
    "revenue_growth_3y": (0.50, 0.00, 0.25),
    "return_on_equity": (0.30, 0.00, 0.20),
    "efficiency_ratio": (0.20, 0.80, 0.50),
}
# Tickers whose SEC ticker map points at a successor entity without history (M4 identity work).
IDENTITY_UNRESOLVED = {"XOM"}
ALL_METRICS = ["revenue", "gross_profit", "cost_of_revenue", "operating_income", "pretax_income", "net_income",
               "operating_cash_flow", "assets", "current_assets", "current_liabilities", "long_term_debt",
               "liabilities", "diluted_shares", "equity", "noninterest_expense", "revenue_net_of_interest"]


# ── pure scoring ──────────────────────────────────────────


def points(value, lo, hi):
    """Map value linearly onto 0-100 between lo (0 points) and hi (100 points), capped at both ends."""
    share = (value - lo) / (hi - lo)
    return round(100 * min(1.0, max(0.0, share)), 1)


def combine(measures, spec):
    """Weighted score over the measures present; missing measures' weight is spread over the rest."""
    present = {k: v for k, v in measures.items() if v is not None}
    if not present:
        return None, {}
    total = sum(spec[k][0] for k in present)
    parts = {}
    for k, v in present.items():
        weight, lo, hi = spec[k]
        parts[k] = {"value": round(v, 4), "points": points(v, lo, hi), "weight": round(weight / total, 4)}
    return round(sum(p["points"] * p["weight"] for p in parts.values()), 1), parts


def year_before(ends, end, years):
    target = date.fromisoformat(end)
    for e in ends:
        if abs((target - date.fromisoformat(e)).days - 365 * years) <= 20:
            return e
    return None


def cagr(values, end, years=3):
    start = year_before(sorted(values), end, years)
    if start is None or values[start] <= 0 or values[end] <= 0:
        return None
    return (values[end] / values[start]) ** (1 / years) - 1


def profit_series(d, end):
    """Operating income, or pre-tax income from continuing operations when operating income isn't
    reported for the latest year. One source for all years, so the trend compares like with like."""
    if end in d["operating_income"]:
        return d["operating_income"], "operating_income"
    if end in d["pretax_income"]:
        return d["pretax_income"], "pretax_income"
    return {}, None


def general_measures(d, end):
    rev = d["revenue"]
    op, _ = profit_series(d, end)
    prev = year_before(sorted(rev), end, 1)
    margin = op[end] / rev[end] if end in op and rev[end] > 0 else None
    prev_margin = op[prev] / rev[prev] if prev and prev in op and rev[prev] > 0 else None
    # A trend from a margin beyond +/-100% (revenue near zero) is noise; a margin above +100% means
    # non-operating gains were booked as income, so it isn't a real operating margin.
    trend = None
    if margin is not None and prev_margin is not None and max(abs(margin), abs(prev_margin)) <= MAX_MARGIN:
        trend = margin - prev_margin
    if margin is not None and margin > MAX_MARGIN:
        margin = None
    return {"revenue_growth_3y": cagr(rev, end), "operating_margin": margin, "operating_margin_trend": trend}


def bank_measures(d, end):
    rev, ni, eq, nie = d["revenue_net_of_interest"], d["net_income"], d["equity"], d["noninterest_expense"]
    prev = year_before(sorted(eq), end, 1)
    roe = None
    if end in ni and end in eq and prev:
        avg_equity = (eq[end] + eq[prev]) / 2
        roe = ni[end] / avg_equity if avg_equity > 0 else None
    efficiency = nie[end] / rev[end] if end in nie and rev.get(end, 0) > 0 else None
    return {"revenue_growth_3y": cagr(rev, end), "return_on_equity": roe, "efficiency_ratio": efficiency}


def piotroski(d, end):
    """F-Score signals for the latest fiscal year vs the one before. Absent long-term debt counts as 0
    when total liabilities are reported. Returns (score, signals_known)."""
    prev = year_before(sorted(d["revenue"]), end, 1)
    if prev is None:
        return None, 0

    def get(metric, year):
        value = d[metric].get(year)
        if value is None and metric == "long_term_debt" and year in d["liabilities"]:
            return 0.0
        return value

    def ratio(a, b, year):
        x, y = get(a, year), get(b, year)
        return x / y if x is not None and y not in (None, 0) else None

    def gross(year):
        rev, gp, cost = d["revenue"].get(year), d["gross_profit"].get(year), d["cost_of_revenue"].get(year)
        if not rev:
            return None
        return gp / rev if gp is not None else ((rev - cost) / rev if cost is not None else None)

    roa, roa_prev = ratio("net_income", "assets", end), ratio("net_income", "assets", prev)
    ocf, ni = get("operating_cash_flow", end), get("net_income", end)
    lev, lev_prev = ratio("long_term_debt", "assets", end), ratio("long_term_debt", "assets", prev)
    cur, cur_prev = ratio("current_assets", "current_liabilities", end), ratio("current_assets", "current_liabilities", prev)
    sh, sh_prev = get("diluted_shares", end), get("diluted_shares", prev)
    gm, gm_prev = gross(end), gross(prev)
    turn, turn_prev = ratio("revenue", "assets", end), ratio("revenue", "assets", prev)
    signals = [
        None if roa is None else roa > 0,
        None if ocf is None else ocf > 0,
        None if roa is None or roa_prev is None else roa > roa_prev,
        None if ocf is None or ni is None else ocf > ni,
        None if lev is None or lev_prev is None else lev < lev_prev,
        None if cur is None or cur_prev is None else cur > cur_prev,
        None if sh is None or sh_prev is None else sh <= sh_prev,
        None if gm is None or gm_prev is None else gm > gm_prev,
        None if turn is None or turn_prev is None else turn > turn_prev,
    ]
    known = [s for s in signals if s is not None]
    return sum(known), len(known)


def key_reasons(model, parts, gross_margin, profit_source=None):
    reasons = []
    p = {k: v["value"] for k, v in parts.items()}
    if "revenue_growth_3y" in p:
        g = p["revenue_growth_3y"]
        reasons.append(f"Revenue {'grew' if g >= 0 else 'shrank'} {abs(g) * 100:.0f}% a year over the last 3 years.")
    if "operating_margin" in p:
        m = p["operating_margin"]
        kind = "pre-tax profit" if profit_source == "pretax_income" else "operating profit"
        reasons.append(f"Keeps {m * 100:.0f} cents of every sales dollar as {kind}." if m >= 0
                       else f"Spends more than it earns: {kind.replace('profit', 'loss')} of {abs(m) * 100:.0f} cents per sales dollar.")
    if "operating_margin_trend" in p:
        t = p["operating_margin_trend"] * 100
        reasons.append(f"Profitability {'improved' if t >= 0 else 'worsened'} {abs(t):.0f} points from last year.")
    if "return_on_equity" in p:
        reasons.append(f"Earns {p['return_on_equity'] * 100:.0f}% a year on shareholders' money.")
    if "efficiency_ratio" in p:
        reasons.append(f"Spends {p['efficiency_ratio'] * 100:.0f} cents to earn each dollar of revenue.")
    if model == "general" and gross_margin is not None:
        reasons.append(f"Gross margin: {gross_margin * 100:.0f}%.")
    return reasons


def spearman(xs, ys):
    def ranks(v):
        order = sorted(range(len(v)), key=lambda i: v[i])
        r = [0.0] * len(v)
        i = 0
        while i < len(order):
            j = i
            while j + 1 < len(order) and v[order[j + 1]] == v[order[i]]:
                j += 1
            for k in range(i, j + 1):
                r[order[k]] = (i + j) / 2 + 1
            i = j + 1
        return r
    rx, ry = ranks(xs), ranks(ys)
    mx, my = sum(rx) / len(rx), sum(ry) / len(ry)
    cov = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    var = (sum((a - mx) ** 2 for a in rx) * sum((b - my) ** 2 for b in ry)) ** 0.5
    return cov / var if var else None


# ── per-company evaluation ────────────────────────────────


def evaluate(d, money_currency):
    """Score one company from its annual series. Returns the result record (without ticker)."""
    is_bank = bool(d["noninterest_expense"]) and bool(d["revenue_net_of_interest"]) and \
        max(d["noninterest_expense"]) == max(d["revenue_net_of_interest"])
    model = "bank" if is_bank else "general"
    rev = d["revenue_net_of_interest"] if is_bank else d["revenue"]
    if not rev:
        has_annual = any(d[m] for m in ("assets", "net_income", "equity"))
        return {"model": model, "score": None, "missing": ["no_revenue" if has_annual else "no_annual_report"]}
    end = max(rev)
    if rev[end] <= 0:
        return {"model": model, "score": None, "missing": ["no_revenue"], "fiscal_year_end": end}
    measures = bank_measures(d, end) if is_bank else general_measures(d, end)
    missing = []
    if measures["revenue_growth_3y"] is None:
        missing.append("insufficient_history")
    if sum(v is not None for v in measures.values()) < MIN_MEASURES:
        return {"model": model, "score": None, "fiscal_year_end": end, "missing": ["not_enough_data"] + missing}
    score, parts = combine(measures, BANK if is_bank else GENERAL)
    profit_source = None if is_bank else profit_series(d, end)[1]
    rv, gp, cost = rev[end], d["gross_profit"].get(end), d["cost_of_revenue"].get(end)
    gross_margin = gp / rv if gp is not None else ((rv - cost) / rv if cost is not None else None)
    f_score, f_known = (None, 0) if is_bank else piotroski(d, end)
    history = [{"fiscal_year_end": e, "revenue": rev[e]} for e in sorted(rev)[-5:]]
    return {
        "model": model, "score": score, "fiscal_year_end": end, "currency": money_currency,
        "parts": parts, "missing": missing, "gross_margin": round(gross_margin, 4) if gross_margin is not None else None,
        "key_reasons": key_reasons(model, parts, gross_margin, profit_source), "revenue_history": history,
        "profit_source": profit_source,
        "piotroski": {"score": f_score, "signals_known": f_known},
    }


def main():
    tickers = json.loads((HERE / "tickers.json").read_text())
    sec = requests.get("https://www.sec.gov/files/company_tickers.json", headers=UA, timeout=30).json()
    cik = {r["ticker"].upper(): r["cik_str"] for r in sec.values()}
    results = {}
    for t in tickers:
        if t in IDENTITY_UNRESOLVED:
            results[t] = {"model": None, "score": None, "missing": ["identity_unresolved"]}
            continue
        if t not in cik:
            results[t] = {"model": None, "score": None, "missing": ["not_sec_filer"]}
            continue
        time.sleep(0.15)  # SEC allows 10 requests a second
        resp = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik[t]:010d}.json", headers=UA, timeout=60)
        if resp.status_code == 404:
            results[t] = {"model": None, "score": None, "missing": ["no_annual_report"]}
            continue
        resp.raise_for_status()
        facts = resp.json().get("facts", {})
        d = {m: series(facts, m)[0] for m in ALL_METRICS}
        results[t] = evaluate(d, currency(facts, "revenue"))

    payload = {
        "score_version": SCORE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "limits": {"general": GENERAL, "bank": BANK},
        "companies": results,
    }
    (HERE / "fundamentals_scores.json").write_text(json.dumps(payload, indent=1))
    report(results)


def report(results):
    scored = {t: r for t, r in results.items() if r.get("score") is not None}
    print(f"{len(results)} companies: {len(scored)} scored "
          f"({sum(r['model'] == 'bank' for r in scored.values())} bank model), {len(results) - len(scored)} not scored")
    reasons = {}
    for t, r in results.items():
        if r.get("score") is None:
            reasons.setdefault(r["missing"][0], []).append(t)
    for code, ts in sorted(reasons.items()):
        print(f"  {code}: {', '.join(ts)}")
    print(f"\n{'rank':>4} {'ticker':6} {'model':7} {'score':>5}  {'growth3y':>8} {'op/ROE':>7} {'trend/eff':>9}  piotroski")
    ordered = sorted(scored.items(), key=lambda x: -x[1]["score"])
    for i, (t, r) in enumerate(ordered, 1):
        p = r["parts"]
        def cell(k):
            return f"{p[k]['value'] * 100:.0f}%" if k in p else "-"
        second = cell("operating_margin") if r["model"] == "general" else cell("return_on_equity")
        third = cell("operating_margin_trend") if r["model"] == "general" else cell("efficiency_ratio")
        f = r["piotroski"]
        fs = f"{f['score']}/{f['signals_known']}" if f["score"] is not None else "-"
        print(f"{i:>4} {t:6} {r['model']:7} {r['score']:>5}  {cell('revenue_growth_3y'):>8} {second:>7} {third:>9}  {fs}")
    full = [(r["score"], r["piotroski"]["score"]) for r in scored.values()
            if r["model"] == "general" and r["piotroski"]["signals_known"] == 9]
    if len(full) >= 5:
        rho = spearman([a for a, _ in full], [b for _, b in full])
        print(f"\nSpearman rank correlation with Piotroski F-Score: {rho:.2f} "
              f"({len(full)} general-model companies with all 9 signals)")


if __name__ == "__main__":
    main()
