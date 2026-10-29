"""S1-12 ED-9, spec-first: stored summaries from another source are cleared at
the start of every run (decision 93, E1 (b), typed by Bryan 2026-10-28).

Spec: docs/work-items/s1-12-edgar-descriptions-packet.md (ED-9, ED-8) on
docs/s1-12-edgar-packet @ 2d52f06, and the coordinator's row of 2026-10-29.

What is pinned:
- summarize(engine, adapter) first clears, in its own transaction committed
  before any fetch, every stored summary whose summary_source_name is set and
  differs from the adapter's source name: the 7 summary columns become NULL
  (status NULL is "not yet decided"). Every profile, not only the members of
  this run.
- So FMP text stops showing even when the SEC run then fails (an adapter error
  or any other exception), and the run's own writes still land on a cleared
  member (it is re-decided in the same run).
- A row from the adapter's own source ("SEC EDGAR") is never touched by the
  clear: the stale-good rule still governs it. A row with no source name (an
  "unavailable" outcome, or never decided) shows no text and isn't touched
  either, so a second run clears nothing.
- The clear comes after the source-name check, and in the CLI after the
  SEC_USER_AGENT check (ED-8: missing_credential is "before any request and
  before any write (including ED-9's clear)"). A refused run changes nothing.
- The coverage line gains cleared_other_source=<n>, a count only.

Every database here is the test database; no provider is called (fake adapters,
and the CLI tests fake requests.Session.get with FakeSec).
"""

import hashlib
import json
import logging
import re

import pytest
import requests
from sqlalchemy import text
from test_collectors import FakeResponse
from test_read_api import groups, login  # noqa: F401  (groups: fixture)
from test_s1_12_edgar_adapter import FakeSec, company, doc_url
from test_s1_12_ingestion import EASY, members, stored

FMP = "Financial Modeling Prep"
FMP_URL = "https://financialmodelingprep.com/"
SEC = "SEC EDGAR"
SEC_URL = "https://www.sec.gov/Archives/edgar/data/1/000000000126000001/zz.htm"
UA = "Synthetic Research ops@perennial.test"
SUMMARY_COLUMNS = (
    "summary",
    "summary_grade",
    "summary_status",
    "summary_source_name",
    "summary_source_url",
    "summary_as_of",
    "description_digest",
)


def cs():
    from src.ingestion import company_summaries

    return company_summaries


class Adapter:
    """A description source by name; a value that is an Exception is raised."""

    def __init__(self, source_name, descriptions, url, on_fetch=None):
        self.source_name, self.url = source_name, url
        self.descriptions, self.calls = dict(descriptions), []
        self.on_fetch = on_fetch

    def fetch(self, symbol):
        if self.on_fetch is not None:
            self.on_fetch(symbol)
        self.calls.append(symbol)
        value = self.descriptions.get(symbol)
        if isinstance(value, Exception):
            raise value
        if value is None:
            return None
        return cs().Description(text=value, url=self.url)


TEXT = " ".join(EASY)
LATER = " ".join(EASY[1:])  # a different description with a passing window


def fmp(engine, *symbols):
    """Store an FMP-sourced ok summary on each symbol's profile, as FMP runs did.

    Written directly (as the owner), not by an FMP-named summarize run: with
    ED-9 that run would itself clear every other source's rows."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE perennial.company_profiles p SET summary = :summary,"
                " summary_grade = 1.5, summary_status = 'ok',"
                " summary_source_name = :name, summary_source_url = :url,"
                " summary_as_of = now(), description_digest = :digest"
                " FROM perennial.securities s"
                " WHERE s.company_profile_id = p.id AND s.symbol = ANY(:symbols)"
            ),
            {
                "summary": " ".join(EASY[:4]),
                "name": FMP,
                "url": FMP_URL,
                "digest": hashlib.sha256(b"synthetic fmp").hexdigest(),
                "symbols": list(symbols),
            },
        )
    for symbol in symbols:
        row = stored(engine, symbol)
        assert (row["summary_status"], row["summary_source_name"]) == ("ok", FMP)


def sec(engine, descriptions):
    """Store SEC EDGAR outcomes (ok or unavailable) on these symbols' profiles."""
    symbols = sorted(descriptions)
    result = cs().summarize(
        engine, Adapter(SEC, descriptions, SEC_URL), symbols=symbols
    )
    assert result.succeeded


def cleared(row):
    return all(row[column] is None for column in SUMMARY_COLUMNS)


def profile(engine, symbol):
    """The non-summary columns of the symbol's profile."""
    with engine.connect() as conn:
        return dict(
            conn.execute(
                text(
                    "SELECT p.legal_name, p.sector, p.industry, p.cik"
                    " FROM perennial.company_profiles p JOIN perennial.securities s"
                    " ON s.company_profile_id = p.id WHERE s.symbol = :s"
                ),
                {"s": symbol},
            )
            .mappings()
            .one()
        )


# ── what is cleared ──────────────────────────────────────────


@pytest.mark.db
def test_fmp_rows_on_a_member_and_a_non_member_are_cleared(engine):
    members(engine, "ZZFA", "ZZFB")
    fmp(engine, "ZZFA", "ZZFB")
    other = profile(engine, "ZZFB")
    result = cs().summarize(
        engine, Adapter(SEC, {"ZZFA": TEXT}, SEC_URL), symbols=["ZZFA"]
    )
    assert result.succeeded
    assert cleared(stored(engine, "ZZFB")), "a non-member's FMP text is still stored"
    assert profile(engine, "ZZFB") == other  # only the summary columns
    # The member is cleared and then decided by this same run.
    row = stored(engine, "ZZFA")
    assert row["summary_status"] == "ok"
    assert (row["summary_source_name"], row["summary_source_url"]) == (SEC, SEC_URL)
    assert (result.with_summary, result.unavailable) == (1, 0)


@pytest.mark.db
def test_a_member_whose_new_description_has_no_window_ends_unavailable(engine):
    members(engine, "ZZFC")
    fmp(engine, "ZZFC")
    result = cs().summarize(
        engine, Adapter(SEC, {"ZZFC": None}, SEC_URL), symbols=["ZZFC"]
    )
    assert result.succeeded
    row = stored(engine, "ZZFC")
    assert row["summary_status"] == "unavailable"
    assert row["summary"] is None and row["summary_source_name"] is None


@pytest.mark.db
def test_any_other_source_name_is_cleared_too(engine):
    members(engine, "ZZFD")
    cs().summarize(
        engine,
        Adapter(
            "Synthetic Profiles", {"ZZFD": TEXT}, "https://profiles.example.com/zz"
        ),
        symbols=["ZZFD"],
    )
    assert stored(engine, "ZZFD")["summary_status"] == "ok"
    assert cs().summarize(engine, Adapter(SEC, {}, SEC_URL), symbols=[]).succeeded
    assert cleared(stored(engine, "ZZFD"))


@pytest.mark.db
def test_rows_from_the_adapters_own_source_are_untouched(engine):
    members(engine, "ZZSA", "ZZSB", "ZZSC")
    sec(engine, {"ZZSA": TEXT, "ZZSB": TEXT, "ZZSC": None})
    before = {s: stored(engine, s) for s in ("ZZSA", "ZZSB", "ZZSC")}
    assert before["ZZSC"]["summary_status"] == "unavailable"
    # ZZSA is a member with an unchanged description (skipped); ZZSB and ZZSC
    # aren't members of this run.
    result = cs().summarize(
        engine, Adapter(SEC, {"ZZSA": TEXT}, SEC_URL), symbols=["ZZSA"]
    )
    assert result.succeeded
    after = {s: stored(engine, s) for s in before}
    assert after == before  # xmin included: no row rewritten


@pytest.mark.db
def test_a_row_with_no_source_name_is_untouched(engine):
    members(engine, "ZZNA", "ZZNB")
    fmp_unavailable = Adapter(FMP, {"ZZNA": None}, FMP_URL)
    assert cs().summarize(engine, fmp_unavailable, symbols=["ZZNA"]).succeeded
    before = {s: stored(engine, s) for s in ("ZZNA", "ZZNB")}
    assert before["ZZNA"]["summary_status"] == "unavailable"
    assert before["ZZNB"]["summary_status"] is None  # never decided
    assert cs().summarize(engine, Adapter(SEC, {}, SEC_URL), symbols=[]).succeeded
    assert {s: stored(engine, s) for s in before} == before


# ── when the clear runs ──────────────────────────────────────


@pytest.mark.db
def test_the_clear_is_committed_before_the_first_fetch(engine):
    members(engine, "ZZCA", "ZZCB")
    fmp(engine, "ZZCA", "ZZCB")
    seen = []

    def look(symbol):
        # A separate connection sees only committed rows.
        with engine.connect() as conn:
            seen.append(
                conn.execute(
                    text(
                        "SELECT count(*) FROM perennial.company_profiles"
                        " WHERE summary_source_name = :n"
                    ),
                    {"n": FMP},
                ).scalar_one()
            )

    adapter = Adapter(SEC, {"ZZCA": TEXT}, SEC_URL, on_fetch=look)
    assert cs().summarize(engine, adapter, symbols=["ZZCA"]).succeeded
    assert seen == [0]


@pytest.mark.db
@pytest.mark.parametrize("position", ["first", "last"])
def test_an_adapter_error_still_leaves_no_fmp_row(engine, position):
    members(engine, "ZZEA", "ZZEB", "ZZEC", "ZZED")
    sec(engine, {"ZZED": TEXT})
    fmp(engine, "ZZEA", "ZZEB", "ZZEC")
    sec_before = stored(engine, "ZZED")
    error = cs().DescriptionAdapterError("http_503")
    descriptions = (
        {"ZZEA": error, "ZZEB": TEXT, "ZZED": LATER}
        if position == "first"
        else {"ZZEA": TEXT, "ZZEB": TEXT, "ZZED": error}
    )
    result = cs().summarize(
        engine, Adapter(SEC, descriptions, SEC_URL), symbols=sorted(descriptions)
    )
    assert not result.succeeded and result.error_code
    for symbol in ("ZZEA", "ZZEB", "ZZEC"):  # members and a non-member
        assert cleared(stored(engine, symbol)), symbol
    assert stored(engine, "ZZED") == sec_before  # stale-good, xmin included


@pytest.mark.db
def test_an_unexpected_exception_still_leaves_no_fmp_row(engine):
    members(engine, "ZZUA", "ZZUB")
    sec(engine, {"ZZUB": TEXT})
    fmp(engine, "ZZUA")
    sec_before = stored(engine, "ZZUB")
    adapter = Adapter(SEC, {"ZZUA": RuntimeError("synthetic"), "ZZUB": LATER}, SEC_URL)
    with pytest.raises(RuntimeError):
        cs().summarize(engine, adapter, symbols=["ZZUA", "ZZUB"])
    assert cleared(stored(engine, "ZZUA"))
    assert stored(engine, "ZZUB") == sec_before


@pytest.mark.db
def test_an_invalid_source_name_clears_nothing(engine):
    members(engine, "ZZIA")
    fmp(engine, "ZZIA")
    before = stored(engine, "ZZIA")
    for bad in ("", " SEC EDGAR", "SEC\x00EDGAR", "x" * 129):
        result = cs().summarize(
            engine, Adapter(bad, {"ZZIA": TEXT}, SEC_URL), symbols=["ZZIA"]
        )
        assert not result.succeeded and result.error_code == "invalid_source_name"
    assert stored(engine, "ZZIA") == before


# ── the coverage line ────────────────────────────────────────


@pytest.mark.db
def test_the_coverage_line_counts_the_cleared_rows(engine, caplog):
    members(engine, "ZZLA", "ZZLB", "ZZLC")
    sec(engine, {"ZZLC": TEXT})
    fmp(engine, "ZZLA", "ZZLB")
    caplog.set_level(logging.DEBUG)

    def coverage():
        lines = [
            r.getMessage() for r in caplog.records if "with_summary=" in r.getMessage()
        ]
        assert len(lines) == 1, lines
        return lines[0]

    caplog.clear()
    adapter = Adapter(SEC, {"ZZLA": TEXT}, SEC_URL)
    assert cs().summarize(engine, adapter, symbols=["ZZLA"]).succeeded
    first = coverage()
    assert re.search(r"\bcleared_other_source=2\b", first), first
    for message in (r.getMessage() for r in caplog.records):
        for sentence in EASY:
            assert sentence not in message, message
        assert FMP_URL not in message and SEC_URL not in message
    caplog.clear()
    assert cs().summarize(engine, adapter, symbols=["ZZLA"]).succeeded
    second = coverage()
    assert re.search(r"\bcleared_other_source=0\b", second), second


# ── the CLI (ED-8): the importer login, and a refused run ────


@pytest.fixture
def fake_sec(monkeypatch):
    fake = FakeSec()
    monkeypatch.setattr(
        requests.Session, "get", lambda self, url, **kwargs: fake.get(url, **kwargs)
    )
    from src.ingestion import collectors

    monkeypatch.setattr(collectors, "MAX_HTTP_RETRIES", 0)
    monkeypatch.delenv("PERENNIAL_ENV", raising=False)
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    return fake


@pytest.fixture
def importer_url(engine, groups):  # noqa: F811
    with login(engine, groups, members=("importer",), connect=True) as (
        _,
        name,
        password,
    ):
        yield engine.url.set(username=name, password=password).render_as_string(
            hide_password=False
        )


def cli_main(monkeypatch, symbols, argv=("summaries",)):
    from src.ingestion import cli

    monkeypatch.setattr(cs(), "consensus_members", lambda engine: list(symbols))
    return cli.main(list(argv))


@pytest.mark.db
def test_the_importer_login_clears_and_the_run_succeeds(
    engine, importer_url, fake_sec, monkeypatch, capsys
):
    members(engine, "ZZKA", "ZZKB")
    fmp(engine, "ZZKA", "ZZKB")
    company(fake_sec, "ZZKA", 111)
    monkeypatch.setenv("DATABASE_URL", importer_url)
    monkeypatch.setenv("SEC_USER_AGENT", UA)
    assert cli_main(monkeypatch, ["ZZKA"]) == 0
    capsys.readouterr()
    assert cleared(stored(engine, "ZZKB"))
    row = stored(engine, "ZZKA")
    assert (row["summary_source_name"], row["summary_source_url"]) == (
        SEC,
        doc_url(111),
    )


@pytest.mark.db
def test_a_failed_sec_run_through_the_cli_still_clears(
    engine, importer_url, fake_sec, monkeypatch, capsys
):
    members(engine, "ZZKC")
    fmp(engine, "ZZKC")
    company(fake_sec, "ZZKC", 112)
    fake_sec.subs[112] = FakeResponse(503, {"message": "synthetic provider body"})
    monkeypatch.setenv("DATABASE_URL", importer_url)
    monkeypatch.setenv("SEC_USER_AGENT", UA)
    assert cli_main(monkeypatch, ["ZZKC"]) != 0
    assert set(json.loads(capsys.readouterr().out)) == {"error_code"}
    assert cleared(stored(engine, "ZZKC"))


@pytest.mark.db
@pytest.mark.parametrize(
    "agent",
    [None, "", "   ", "Perennial research you@example.com"],
    ids=["unset", "empty", "blank", "env_example_placeholder"],
)
def test_a_refused_run_clears_nothing(
    engine, importer_url, fake_sec, monkeypatch, capsys, agent
):
    members(engine, "ZZKD", "ZZKE")
    fmp(engine, "ZZKD", "ZZKE")
    company(fake_sec, "ZZKD", 113)
    before = {s: stored(engine, s) for s in ("ZZKD", "ZZKE")}
    monkeypatch.setenv("DATABASE_URL", importer_url)
    if agent is None:
        monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    else:
        monkeypatch.setenv("SEC_USER_AGENT", agent)
    assert cli_main(monkeypatch, ["ZZKD"]) != 0
    assert json.loads(capsys.readouterr().out) == {"error_code": "missing_credential"}
    assert fake_sec.requests == []
    assert {s: stored(engine, s) for s in before} == before  # xmin included
