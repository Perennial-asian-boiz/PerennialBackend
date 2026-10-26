"""S1-03 news ingestion: Finnhub parsing, match-v1, coverage, and duplicates. Synthetic only."""

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError

from src.db import models
from src.ingestion import collectors
from src.services.news import finnhub, job
from src.services.news.finnhub import FinnhubError, FinnhubNewsClient, RawArticle, parse_articles
from src.services.fundamentals.job import Universe
from src.services.news.job import collect_news, run_news
from src.services.news.matching import match_rule, name_variants

TODAY = date(2026, 10, 26)
T0 = 1792944049  # 2026-10-26 UTC


def item(fid, headline, summary="", source="Yahoo", stamp=T0):
    return {"id": fid, "headline": headline, "summary": summary, "source": source,
            "url": f"https://finnhub.io/api/news?id={fid}", "datetime": stamp, "related": "X"}


def article(fid, headline, summary=""):
    return RawArticle(fid, headline, summary, "Yahoo", f"https://finnhub.io/api/news?id={fid}",
                      datetime.fromtimestamp(T0, tz=timezone.utc))


# ── Finnhub client ───────────────────────────────────────


def test_parse_keeps_complete_articles_and_counts_the_rest():
    payload = [item(1, " Nvidia rises "), item(2, ""), {"id": "x"}, item(3, "No time", stamp=0),
               {**item(4, "No outlet"), "source": ""}]
    articles, skipped = parse_articles(payload)
    assert [(a.finnhub_id, a.headline, a.outlet) for a in articles] == [
        (1, "Nvidia rises", "Yahoo"), (4, "No outlet", "unknown")]
    assert skipped == 3 and articles[0].published_at.tzinfo == timezone.utc
    with pytest.raises(FinnhubError):
        parse_articles({"error": "x"})


def test_client_sends_the_key_in_a_header_and_paces_requests(monkeypatch):
    seen, clock, slept = [], [0.0], []

    def get(session, url, *, params, headers):
        seen.append((params, headers))
        return collectors._Response(True, status=200, data=[])

    monkeypatch.setattr(finnhub, "_get_json", get)
    client = FinnhubNewsClient(object(), "secret", interval=1.1,
                               monotonic=lambda: clock[0], sleep=slept.append)
    client.company_news("NVDA", date(2026, 10, 19), TODAY)
    client.company_news("RKLB", date(2026, 10, 19), TODAY)
    assert slept == [1.1]
    assert all("token" not in p and h == {"X-Finnhub-Token": "secret"} for p, h in seen)
    assert seen[0][0] == {"symbol": "NVDA", "from": "2026-10-19", "to": "2026-10-26"}


def test_client_errors_carry_only_a_code(monkeypatch):
    monkeypatch.setattr(finnhub, "_get_json", lambda *a, **k: collectors._Response(False, code="http_429"))
    with pytest.raises(FinnhubError, match="^http_429$"):
        FinnhubNewsClient(object(), "k", interval=0).company_news("NVDA", TODAY, TODAY)


# ── match-v1 (AC 3) ──────────────────────────────────────


def test_name_variants_drop_legal_suffixes_generic_tails_and_common_words():
    assert name_variants(["Palantir Technologies Inc."]) == {"palantir technologies", "palantir"}
    assert name_variants(["Block, Inc."]) == set()
    assert name_variants(["Morgan Stanley", None, ""]) == {"morgan stanley"}


@pytest.mark.parametrize("headline,symbol,names,expected", [
    ("Why $RKLB jumped today", "RKLB", [], "ticker"),
    ("RKLB shares climb after launch", "RKLB", [], "ticker"),
    ("Funds buy RKLBX notes", "RKLB", [], None),            # not a whole word
    ("Ms. Smith joins the board", "MS", [], None),          # short tickers need $ or an exchange
    ("Shares rise (NYSE: MS) after results", "MS", [], "ticker"),
    ("Morgan Stanley beats estimates", "MS", ["Morgan Stanley"], "name"),
    ("Rocket Lab wins a contract", "RKLB", ["Rocket Lab Corp"], "name"),
    ("A block of shares changed hands", "XYZ", ["Block, Inc."], None),
    ("Stocks rally as tech gains", "NVDA", ["NVIDIA Corp"], None),
])
def test_match_rule(headline, symbol, names, expected):
    assert match_rule(headline, "", symbol, name_variants(names)) == expected


def test_the_summary_can_carry_the_match():
    assert match_rule("Space stocks to watch", "Rocket Lab leads the group",
                      "RKLB", name_variants(["Rocket Lab"])) == "name"


# ── collection: coverage and new pairs (AC 1, 2, 5) ──────


class Client:
    def __init__(self, by_symbol, failing=()):
        self.by_symbol, self.failing = by_symbol, set(failing)
        self.session = None

    def company_news(self, symbol, start, end):
        if symbol in self.failing:
            raise FinnhubError("http_429")
        return list(self.by_symbol.get(symbol, [])), 0


NAMES = {"RKLB": ["Rocket Lab"], "NVDA": ["NVIDIA Corp"], "ABSI": ["Absci Corp"]}


def test_collection_keeps_matched_new_pairs_and_records_coverage_per_ticker():
    client = Client({
        "RKLB": [article(1, "Rocket Lab wins", "summary"), article(1, "Rocket Lab wins", "summary"),
                 article(2, "Space roundup")],
        "NVDA": [article(3, "Nvidia unveils a chip"), article(4, "NVDA rises")],
        "ABSI": [article(5, "Biotech stocks drift")],
    })
    got = collect_news(client, ["ABSI", "NVDA", "RKLB"], NAMES, {"NVDA": {3}}, TODAY)
    assert got.outcome.succeeded
    assert [(r["ticker"], r["finnhub_id"], r["match_rule"], r["content_retrieved"])
            for r in got.outcome.records] == [
        ("NVDA", 4, "ticker", "headline_only"), ("RKLB", 1, "name", "headline_and_summary")]
    assert {c.symbol: (c.status, c.articles_found, c.articles_new) for c in got.coverage} == {
        "ABSI": ("no_coverage", 0, 0), "NVDA": ("covered", 2, 1), "RKLB": ("covered", 1, 1)}
    record = got.outcome.records[0]
    assert set(record) >= {"url", "headline", "outlet", "published_at", "ticker"}
    assert len(record["content_hash"]) == 64


def test_a_rate_limited_ticker_is_logged_and_the_run_continues():
    got = collect_news(Client({"RKLB": [article(1, "Rocket Lab wins")]}, failing={"NVDA"}),
                       ["NVDA", "RKLB"], NAMES, {}, TODAY)
    assert got.outcome.succeeded and got.outcome.coverage == {"complete": False, "scope": "selected_universe"}
    assert {c.symbol: (c.status, c.error_code) for c in got.coverage} == {
        "NVDA": ("error", "http_429"), "RKLB": ("covered", None)}
    assert got.outcome.envelope["errors"] == ["NVDA"]


def test_every_ticker_failing_fails_the_batch():
    got = collect_news(Client({}, failing={"NVDA"}), ["NVDA"], NAMES, {}, TODAY)
    assert not got.outcome.succeeded and got.outcome.error_code == "collection_failed"
    assert got.coverage[0].status == "error"


# ── database: duplicates and coverage rows (AC 4, 5) ─────


def seed(engine, *symbols):
    with engine.begin() as conn:
        for symbol in symbols:
            sid = conn.execute(insert(models.securities).values(symbol=symbol, name=NAMES.get(symbol, [None])[0])
                               .returning(models.securities.c.id)).scalar_one()
            conn.execute(insert(models.security_identifiers).values(
                scheme="ticker", value=symbol, security_id=sid))


def counts(engine):
    with engine.connect() as conn:
        articles = conn.execute(select(models.news_articles)).mappings().all()
        coverage = conn.execute(select(models.news_coverage)).mappings().all()
    return articles, coverage


@pytest.mark.db
def test_rerunning_stores_no_duplicate_and_records_coverage_each_run(engine, monkeypatch):
    seed(engine, "RKLB", "NVDA", "ABSI")
    monkeypatch.setattr(job, "current_universe", lambda conn: Universe(["ABSI", "NVDA", "RKLB"], {}, {}))
    client = Client({"RKLB": [article(1, "Rocket Lab wins")], "NVDA": [article(2, "NVDA rises")]})

    first = run_news(engine, client, TODAY)
    assert first.succeeded and first.accepted_count == 2
    client.by_symbol["RKLB"].append(article(9, "Rocket Lab delays a launch"))
    second = run_news(engine, client, TODAY)
    assert second.succeeded and second.accepted_count == 1  # only the new article

    articles, coverage = counts(engine)
    assert sorted(a["finnhub_id"] for a in articles) == [1, 2, 9]
    assert len(coverage) == 6  # 3 tickers x 2 runs
    assert sorted(c["status"] for c in coverage if c["run_id"] == second.run_id) == [
        "covered", "covered", "no_coverage"]

    third = run_news(engine, client, TODAY)
    assert third.succeeded and third.accepted_count == 0
    assert len(counts(engine)[0]) == 3


@pytest.mark.db
def test_the_database_itself_rejects_a_duplicate_article_for_a_security(engine, monkeypatch):
    seed(engine, "RKLB")
    monkeypatch.setattr(job, "current_universe", lambda conn: Universe(["RKLB"], {}, {}))
    run_news(engine, Client({"RKLB": [article(1, "Rocket Lab wins")]}), TODAY)
    row = dict(counts(engine)[0][0])
    row.pop("id")
    row["row_number"] = 2
    with pytest.raises(IntegrityError), engine.begin() as conn:
        conn.execute(insert(models.news_articles).values(**row))


@pytest.mark.db
def test_errors_are_recorded_per_ticker_and_no_universe_is_missing_upstream(engine, monkeypatch):
    seed(engine, "RKLB", "NVDA")
    monkeypatch.setattr(job, "current_universe", lambda conn: Universe(["NVDA", "RKLB"], {}, {}))
    result = run_news(engine, Client({"RKLB": [article(1, "Rocket Lab wins")]}, failing={"NVDA"}), TODAY)
    assert result.succeeded
    coverage = {c["error_code"] for c in counts(engine)[1]}
    assert coverage == {None, "http_429"}

    monkeypatch.setattr(job, "current_universe", lambda conn: None)
    assert run_news(engine, Client({}), TODAY).error_code == "missing_upstream"
