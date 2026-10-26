"""Pattern evaluation: EDGAR parsing, the score rules, and both ingestion runs. Synthetic only."""

from datetime import date, timedelta

import pytest
from sqlalchemy import insert, select
from src.db import models
from src.db.queries import batch_rows
from src.ingestion import collectors
from src.ingestion.schemas import FundamentalScore
from src.services.fundamentals import edgar, job
from src.services.fundamentals.edgar import EdgarClient, EdgarError, annual_values
from src.services.fundamentals.job import (
    Universe,
    collect_fundamentals,
    run_fundamentals,
)
from src.services.fundamentals.scoring import (
    ScoreRule,
    breakdown,
    combine,
    key_reasons,
    piotroski,
    points,
    score_security,
)

YEARS = [date(2022, 12, 31), date(2023, 12, 31), date(2024, 12, 31), date(2025, 12, 31)]


def fact(val, end, *, flow=True, form="10-K", fp="FY", filed=None):
    entry = {"val": val, "end": end.isoformat(), "form": form, "fp": fp,
             "filed": (filed or end + timedelta(days=60)).isoformat()}
    if flow:
        entry["start"] = (end - timedelta(days=364)).isoformat()
    return entry


def facts(concepts, *, taxonomy="us-gaap", unit="USD", flow=True):
    return {taxonomy: {name: {"units": {unit: [fact(v, y, flow=flow) for y, v in zip(YEARS, values)]}}
                       for name, values in concepts.items()}}


def series(values):
    return dict(zip(YEARS, values))


GENERAL_FACTS = facts({"Revenues": [100, 120, 150, 172.8], "OperatingIncomeLoss": [-10, 0, 15, 30]})
BANK_FACTS = facts({"RevenuesNetOfInterestExpense": [80, 90, 100, 110], "NetIncomeLoss": [8, 9, 10, 12],
                    "NoninterestExpense": [50, 55, 60, 66]})
BANK_FACTS["us-gaap"]["StockholdersEquity"] = {"units": {"USD": [fact(100, y, flow=False) for y in YEARS]}}


# ── vocabulary ────────────────────────────────────────────


def test_fundamentals_sources_are_appended_without_moving_anything():
    from src.ingestion.identity import CANONICAL_ONLY_SOURCES
    from src.pipeline.lifecycle import SOURCE_LOCK_KEYS

    before = models.WATCHLIST_SOURCES + models.SUBJECTLESS_SOURCES + models.PINNED_ROW_SOURCES
    # Later sources may follow (append only); fundamentals keep their positions.
    assert models.SOURCES[: len(before) + 2] == before + ("fundamentals", "fundamental_scores")
    assert not set(models.FUNDAMENTALS_SOURCES) & set(models.WATCHLIST_SOURCES)
    assert (SOURCE_LOCK_KEYS["fundamentals"], SOURCE_LOCK_KEYS["fundamental_scores"]) == (170018, 170019)
    assert len(set(SOURCE_LOCK_KEYS.values())) == len(SOURCE_LOCK_KEYS)
    assert set(models.FUNDAMENTALS_SOURCES) <= CANONICAL_ONLY_SOURCES


# ── EDGAR parsing ─────────────────────────────────────────


def test_annual_values_keep_annual_full_year_latest_filed_entries():
    rev = [
        fact(100, YEARS[3]),
        fact(90, YEARS[3], fp="Q3", form="10-Q"),  # quarterly: ignored
        {**fact(25, YEARS[3]), "start": (YEARS[3] - timedelta(days=90)).isoformat()},  # 3-month span
        fact(101, YEARS[3], filed=YEARS[3] + timedelta(days=400)),  # restated later: wins
    ]
    values = annual_values({"us-gaap": {"Revenues": {"units": {"USD": rev}}}})
    assert [(v.line_item, v.value, v.concept) for v in values] == [("revenue", 101.0, "Revenues")]


def test_concepts_merge_per_year_and_ifrs_filers_keep_their_currency():
    gaap = {"us-gaap": {
        "SalesRevenueNet": {"units": {"USD": [fact(80, YEARS[0])]}},
        "Revenues": {"units": {"USD": [fact(100, YEARS[1])]}},
    }}
    assert [(v.fiscal_year_end, v.concept) for v in annual_values(gaap)] == [
        (YEARS[0], "SalesRevenueNet"), (YEARS[1], "Revenues")]
    ifrs = facts({"Revenue": [1, 2, 3, 4]}, taxonomy="ifrs-full", unit="TWD")
    assert {v.unit for v in annual_values(ifrs)} == {"TWD"}


def test_only_the_latest_six_fiscal_years_are_kept():
    many = [date(2014 + n, 12, 31) for n in range(12)]
    raw = {"us-gaap": {"Revenues": {"units": {"USD": [fact(n, y) for n, y in enumerate(many)]}}}}
    assert [v.fiscal_year_end.year for v in annual_values(raw)] == list(range(2020, 2026))


def test_client_paces_requests_sends_contact_and_treats_404_as_no_data(monkeypatch):
    calls, clock, slept = [], [0.0], []

    def get(session, url, *, headers, allowed_status):
        calls.append((url, headers["User-Agent"]))
        return collectors._Response(True, status=404) if "CIK0000000002" in url else collectors._Response(
            True, status=200, data={"facts": {"us-gaap": {}}})

    monkeypatch.setattr(edgar, "_get_json", get)
    client = EdgarClient(object(), "Perennial test team@example.com", interval=0.1,
                         monotonic=lambda: clock[0], sleep=slept.append)
    assert client.company_facts(1) == {"us-gaap": {}}
    assert client.company_facts(2) is None
    assert slept == [0.1] and all(ua == "Perennial test team@example.com" for _, ua in calls)


def test_client_failures_carry_only_a_code(monkeypatch):
    monkeypatch.setattr(edgar, "_get_json", lambda *a, **k: collectors._Response(False, code="http_503"))
    with pytest.raises(EdgarError, match="^http_503$"):
        EdgarClient(object(), "ua", interval=0).company_facts(1)


# ── score rules ───────────────────────────────────────────


def test_points_are_linear_and_capped_with_lower_is_better_limits():
    assert points(0.20, 0, 0.40) == 50.0 and points(-1, 0, 0.4) == 0 and points(9, 0, 0.4) == 100
    assert points(0.65, 0.80, 0.50) == 50.0


def test_missing_measures_spread_their_weight_over_the_rest():
    score, parts = combine({"revenue_growth_3y": 0.40, "operating_margin": None,
                            "operating_margin_trend": 0.0}, ScoreRule().general)
    assert score == 85.7 and round(sum(p["weight"] for p in parts.values()), 9) == 1


def test_general_model_score_matches_the_hand_calculation():
    record = score_security("ZZGN", {"revenue": series([100, 120, 150, 172.8]),
                                     "operating_income": series([-10, 0, 15, 30])})
    # growth 20% -> 50, margin 17.4% -> 74.7, trend +7.4 points -> 86.8; weighted 64.8
    assert (record["model"], record["score"], record["missing"]) == ("general", 64.8, [])
    assert record["profit_source"] == "operating_income"
    FundamentalScore.model_validate(record)


def test_bank_model_uses_growth_roe_and_efficiency():
    record = score_security("ZZBK", {
        "revenue_net_of_interest": series([80, 90, 100, 110]), "net_income": series([8, 9, 10, 12]),
        "equity": series([100] * 4), "noninterest_expense": series([50, 55, 60, 66])})
    assert (record["model"], record["score"]) == ("bank", 53.7)
    assert (record["return_on_equity"], record["efficiency_ratio"]) == (0.12, 0.6)


def test_a_bank_reporting_only_revenues_still_gets_the_bank_model():
    record = score_security("ZZBA", {
        "revenue": series([80, 90, 100, 110]), "net_income": series([8, 9, 10, 12]),
        "equity": series([100] * 4), "noninterest_expense": series([50, 55, 60, 66])})
    assert (record["model"], record["score"]) == ("bank", 53.7)


def test_a_single_measure_is_not_enough_to_score():
    record = score_security("ZZ", {"revenue": series([100, 120, 150, 172.8])})
    assert record["score"] is None and record["missing"] == ["not_enough_data"]
    FundamentalScore.model_validate(record)


def test_pretax_income_stands_in_when_operating_income_is_not_reported():
    record = score_security("ZZ", {"revenue": series([100, 120, 150, 172.8]),
                                   "pretax_income": series([5, 8, 12, 20])})
    assert record["score"] is not None and record["profit_source"] == "pretax_income"
    assert "pre-tax profit" in key_reasons(record)[1]


def test_margins_beyond_100_percent_drop_the_trend_and_positive_outliers():
    gains = score_security("ZZ", {"revenue": series([1, 1, 1, 2]),
                                  "operating_income": series([0, 0, 70, 150])})
    assert gains["operating_margin"] is None and gains["operating_margin_trend"] is None


def test_growth_from_a_tiny_base_is_not_scored_when_costs_exceed_twice_revenue():
    record = score_security("ZZCR", {"revenue": series([1, 2, 3, 4]),
                                     "operating_income": series([-50, -60, -70, -80])})
    assert record["revenue_growth_3y"] is None and record["operating_margin"] == -20.0
    assert record["score"] is None and record["missing"][0] == "not_enough_data"


def test_no_revenue_and_short_history_are_explained():
    assert score_security("ZZ", {"assets": series([1, 2, 3, 4]),
                                 "revenue": series([0, 0, 0, 0])})["missing"] == ["no_revenue"]
    short = score_security("ZZ", {"revenue": {YEARS[2]: 10, YEARS[3]: 12},
                                  "operating_income": {YEARS[2]: 1, YEARS[3]: 2}})
    assert short["score"] is not None and short["missing"] == ["insufficient_history"]
    assert score_security("ZZ", {}) is None


def test_piotroski_counts_absent_debt_as_zero_when_liabilities_are_reported():
    lines = {
        "revenue": {YEARS[2]: 100, YEARS[3]: 130}, "net_income": {YEARS[2]: 5, YEARS[3]: 10},
        "assets": {YEARS[2]: 100, YEARS[3]: 110}, "operating_cash_flow": {YEARS[2]: 8, YEARS[3]: 15},
        "current_assets": {YEARS[2]: 50, YEARS[3]: 60}, "current_liabilities": {YEARS[2]: 40, YEARS[3]: 40},
        "liabilities": {YEARS[2]: 30, YEARS[3]: 30}, "diluted_shares": {YEARS[2]: 10, YEARS[3]: 10},
        "gross_profit": {YEARS[2]: 40, YEARS[3]: 55},
    }
    assert piotroski(lines, YEARS[3]) == (8, 9)  # leverage flat at 0: the only failed signal


def test_breakdown_recomputes_points_and_weights_from_a_stored_record():
    record = score_security("ZZGN", {"revenue": series([100, 120, 150, 172.8]),
                                     "operating_income": series([-10, 0, 15, 30])})
    parts = breakdown(record)
    assert {k: p["points"] for k, p in parts.items()} == {
        "revenue_growth_3y": 50.0, "operating_margin": 74.7, "operating_margin_trend": 86.8}


# ── collection (no database) ─────────────────────────────


class Client:
    def __init__(self, tickers, facts_by_cik, failing=()):
        self.tickers, self.facts_by_cik, self.failing = tickers, facts_by_cik, set(failing)
        self.session = None

    def ticker_map(self):
        return dict(self.tickers)

    def company_facts(self, cik):
        if cik in self.failing:
            raise EdgarError("http_503")
        return self.facts_by_cik.get(cik)


def test_collection_records_line_items_and_counts_companies_without_data():
    client = Client({"ZZGN": 1, "ZZND": 3}, {1: GENERAL_FACTS})
    outcome = collect_fundamentals(client, Universe(["ZZGN", "ZZNC", "ZZND"], {}, {}))
    assert outcome.succeeded and len(outcome.records) == 8
    assert outcome.envelope["no_cik"] == ["ZZNC"] and outcome.envelope["no_annual_data"] == ["ZZND"]
    assert outcome.envelope["with_data"] == 1


def test_a_seeded_cik_wins_over_the_sec_ticker_map():
    client = Client({"XOM": 9}, {34088: GENERAL_FACTS})  # ticker map points at a new entity
    outcome = collect_fundamentals(client, Universe(["XOM"], {}, {"XOM": 34088}))
    assert len(outcome.records) == 8


def test_any_company_transport_failure_fails_the_batch_with_codes_only():
    client = Client({"ZZGN": 1, "ZZBK": 2}, {1: GENERAL_FACTS, 2: BANK_FACTS}, failing={2})
    outcome = collect_fundamentals(client, Universe(["ZZBK", "ZZGN"], {}, {}))
    assert not outcome.succeeded and outcome.error_code == "partial_collection"
    assert outcome.diagnostics["failures"] == [{"unit": "ZZBK", "code": "http_503"}]


# ── both runs through the ingestion contract (database) ──


def seed(engine, *symbols):
    with engine.begin() as conn:
        for symbol in symbols:
            sid = conn.execute(insert(models.securities).values(symbol=symbol)
                               .returning(models.securities.c.id)).scalar_one()
            conn.execute(insert(models.security_identifiers).values(
                scheme="ticker", value=symbol, security_id=sid))


@pytest.mark.db
def test_collect_then_derive_with_lineage_and_identical_reruns_reuse_batches(engine, monkeypatch):
    seed(engine, "ZZGN", "ZZBK", "ZZNC", "ZZND")
    monkeypatch.setattr(job, "current_universe",
                        lambda conn: Universe(["ZZBK", "ZZGN", "ZZNC", "ZZND"], {}, {}))
    client = Client({"ZZGN": 1, "ZZBK": 2, "ZZND": 3}, {1: GENERAL_FACTS, 2: BANK_FACTS})

    first = run_fundamentals(engine, client)
    assert first.succeeded
    lines, scores = first.results["fundamentals"], first.results["fundamental_scores"]
    with engine.connect() as conn:
        assert len(batch_rows(conn, "fundamentals", lines.batch_id)) == 8 + 16
        stored = {r["symbol"]: r for r in batch_rows(conn, "fundamental_scores", scores.batch_id)}
        dependency = conn.execute(select(models.run_dependencies.c.batch_id).where(
            models.run_dependencies.c.run_id == scores.run_id)).scalar_one()
    assert {s: (r["model"], float(r["score"])) for s, r in stored.items()} == {
        "ZZBK": ("bank", 53.7), "ZZGN": ("general", 64.8)}
    assert dependency == lines.batch_id

    again = run_fundamentals(engine, client)
    assert again.results["fundamentals"].reused_batch and again.results["fundamental_scores"].reused_batch


@pytest.mark.db
def test_a_failed_collection_keeps_the_last_good_batch_for_scoring(engine, monkeypatch):
    seed(engine, "ZZGN", "ZZBK")
    monkeypatch.setattr(job, "current_universe", lambda conn: Universe(["ZZBK", "ZZGN"], {}, {}))
    good = run_fundamentals(engine, Client({"ZZGN": 1, "ZZBK": 2}, {1: GENERAL_FACTS, 2: BANK_FACTS}))
    bad = run_fundamentals(engine, Client({"ZZGN": 1, "ZZBK": 2}, {1: GENERAL_FACTS}, failing={2}))
    assert bad.results["fundamentals"].error_code == "partial_collection"
    assert bad.results["fundamental_scores"].succeeded
    assert bad.results["fundamental_scores"].batch_id == good.results["fundamental_scores"].batch_id


@pytest.mark.db
def test_no_published_watchlist_is_a_missing_upstream_failure(engine, monkeypatch):
    monkeypatch.setattr(job, "current_universe", lambda conn: None)
    run = run_fundamentals(engine, Client({}, {}))
    assert run.results["fundamentals"].error_code == "missing_upstream"
    assert run.results["fundamental_scores"].error_code == "missing_upstream"
