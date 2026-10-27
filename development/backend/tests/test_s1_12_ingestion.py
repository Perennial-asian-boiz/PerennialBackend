"""S1-12 ingestion, spec-first: CS-1, CS-2, CS-3 and CS-7, with the checker's
N1, N2 and the ok-status coupling note.

Spec: docs/work-items/s1-stories-plan.md (S1-12) on docs/s1-stories-plan @
25d2590, with decision 72: verbatim sentence selection, else "Summary not
available" (Q13 (a) then (e)), no model (Q9 declined), consensus members only
(Q11), the provider profile through the provider-neutral adapter (Q5). The
storage is migration 0012 on feat/s1-12-summary-api @ 01e411c.

The seam (one module, src.ingestion.company_summaries):
- split_sentences(text) -> list[str]; select_excerpt(description) -> an object
  with .text and .grade, or None; SELECTOR_VERSION, a constant in the skip key.
- Description(text, url) is what an adapter returns; DescriptionAdapterError(code)
  is how it fails. An adapter has .source_name and .fetch(symbol) -> Description
  or None. The tests use a fake adapter only: no provider is ever called.
- consensus_members(engine) -> the current publication's entry tickers.
- summarize(engine, adapter, *, symbols=None) -> a result with .succeeded,
  .error_code, .members, .with_summary and .unavailable; symbols defaults to
  consensus_members(engine).

The synthetic sentences were graded with textstat 0.7.13 while these tests were
written: EASY ones are about grade -2 to 0, HARD ones about 34, and every mixed
window used below is above 15. A test reads textstat only to check the stored
grade and the pin.
"""

import hashlib
import re
import socket
import sys
from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from test_read_api import REPO, feed, publish_run, seed_security

EASY = [
    "The firm makes tools.",
    "It sells them in shops.",
    "Its staff work in one town.",
    "The shops are near homes.",
    "It has a small team.",
    "Most of its tools are for gardens.",
]
HARD = [
    "The corporation manufactures sophisticated semiconductor instrumentation, metrology"
    " equipment, and computational lithography infrastructure for multinational"
    " manufacturing organizations.",
    "Additionally, the organization provides comprehensive consulting, implementation,"
    " integration, and maintenance capabilities supporting enterprise digital"
    " transformation initiatives internationally.",
]
URL = "https://profiles.example.com/zz/profile"
SOURCE = "Synthetic Profiles"


def cs():
    from src.ingestion import company_summaries

    return company_summaries


def joined(sentences):
    return " ".join(sentences)


def normalized(value):
    return " ".join(value.split())


# CS-2's cases: description -> the excerpt (None: summary_unavailable).
SELECTION = {
    "readable_lead": (joined(EASY), joined(EASY[:4])),
    "hard_lead_readable_middle": (joined(HARD + EASY[:4]), joined(EASY[:4])),
    "three_then_hard": (joined(EASY[:3] + [HARD[0]] + EASY[3:4]), joined(EASY[:3])),
    # Document order: the start moves only after 4, 3 and 2 have failed there, so a
    # passing 3 at the start beats a passing 4 further on.
    "three_first_beats_a_later_four": (
        joined(EASY[:3] + [HARD[0]] + EASY[3:] + ["They last a long time."]),
        joined(EASY[:3]),
    ),
    "hard_then_two": (joined([HARD[1]] + EASY[:2]), joined(EASY[:2])),
    "exactly_two": (joined(EASY[:2]), joined(EASY[:2])),
    "nothing_readable": (joined(HARD), None),
    "single_sentence": (EASY[0], None),
    "empty": ("", None),
    "whitespace_only": ("  \n\t ", None),
}


# ── CS-2 and N2: the splitter ────────────────────────────────


@pytest.mark.parametrize(
    "description, sentences",
    [
        ("It sells parts. It has a team.", ["It sells parts.", "It has a team."]),
        ("It grew fast! Did it slow? No.", ["It grew fast!", "Did it slow?", "No."]),
        ("It sells parts.\nIt has a team.", ["It sells parts.", "It has a team."]),
        ("It sells parts.   It has a team.", ["It sells parts.", "It has a team."]),
        # N2: a digit or an opening quote starts a sentence too.
        (
            "It sells parts. 3M also sells parts.",
            ["It sells parts.", "3M also sells parts."],
        ),
        (
            'It sells parts. "Our goal is simple," it says.',
            ["It sells parts.", '"Our goal is simple," it says.'],
        ),
        (
            "It sells parts. “Our goal is simple,” it says.",
            ["It sells parts.", "“Our goal is simple,” it says."],
        ),
        (
            "It sells parts. 'Our goal is simple,' it says.",
            ["It sells parts.", "'Our goal is simple,' it says."],
        ),
        # N2: a final sentence with no terminal punctuation still counts.
        (
            "It sells parts. It has a small team",
            ["It sells parts.", "It has a small team"],
        ),
        ("It has a small team", ["It has a small team"]),
        # The abbreviations textstat mis-splits, and others no split happens after.
        (
            "It has plants in the U.S. Midwest. It sells tools.",
            ["It has plants in the U.S. Midwest.", "It sells tools."],
        ),
        (
            "Synthetic Parts Inc. Group owns it. It sells tools.",
            ["Synthetic Parts Inc. Group owns it.", "It sells tools."],
        ),
        (
            "Sales rose 3.5 percent. It sells tools.",
            ["Sales rose 3.5 percent.", "It sells tools."],
        ),
        (
            "It sells approx. three parts. It has a team.",
            ["It sells approx. three parts.", "It has a team."],
        ),
        (
            "It sells parts.It has a team.",
            ["It sells parts.It has a team."],
        ),  # no whitespace
        ("", []),
    ],
    ids=[
        "period",
        "bang_and_question",
        "newline",
        "spaces",
        "digit",
        "straight_double_quote",
        "curly_quote",
        "single_quote",
        "final_unpunctuated",
        "only_unpunctuated",
        "us",
        "inc",
        "decimal",
        "lowercase_after",
        "no_space",
        "empty",
    ],
)
def test_n2_the_fixed_splitter(description, sentences):
    assert cs().split_sentences(description) == sentences


def test_cs2_the_splitter_is_ours_not_textstats():
    import inspect

    source = inspect.getsource(cs().split_sentences)
    assert "textstat" not in source


# ── CS-2: windows of 4, then 3, then 2, in document order ────


@pytest.mark.parametrize("case", sorted(SELECTION))
def test_cs2_the_first_passing_window_is_taken_verbatim(case):
    description, expected = SELECTION[case]
    excerpt = cs().select_excerpt(description)
    if expected is None:
        assert excerpt is None
    else:
        assert excerpt is not None and excerpt.text == expected
        assert float(excerpt.grade) <= 13.0  # MAX_GRADE, 8 until 2026-10-26


def test_cs2_a_none_description_is_unavailable():
    assert cs().select_excerpt(None) is None


def test_cs2_the_window_is_joined_with_single_spaces():
    messy = "  " + "\n\n".join(EASY[:2]) + "   \t" + EASY[2] + "\n"
    assert cs().select_excerpt(messy).text == joined(EASY[:3])


def test_cs2_the_grade_is_textstats_flesch_kincaid_of_the_stored_text():
    import textstat

    excerpt = cs().select_excerpt(SELECTION["readable_lead"][0])
    assert (
        abs(float(excerpt.grade) - textstat.flesch_kincaid_grade(excerpt.text)) < 0.005
    )


def test_cs2_textstat_is_pinned_exactly_in_pyproject_and_both_locks():
    pyproject = (REPO / "pyproject.toml").read_text()
    pinned = re.search(r'"textstat==([0-9][0-9.]*)"', pyproject)
    assert pinned, "textstat is not an exact (==) runtime dependency"
    for lock in ("requirements.lock", "requirements-dev.lock"):
        content = (REPO / lock).read_text()
        assert re.search(
            rf"(?m)^textstat=={re.escape(pinned.group(1))} \\\n\s+--hash=sha256:",
            content,
        ), lock


# ── CS-3: nothing invented, no model ─────────────────────────


@pytest.mark.parametrize("case", sorted(SELECTION))
def test_cs3_every_excerpt_is_a_substring_of_the_description(case):
    description, _ = SELECTION[case]
    excerpt = cs().select_excerpt(description)
    if excerpt is not None:
        assert normalized(excerpt.text) in normalized(description)


MODEL_PACKAGES = ("anthropic", "openai", "transformers", "torch", "llama")


def test_cs3_no_model_package_in_src_or_either_lock():
    src = REPO / "development" / "backend" / "src"
    for path in sorted(src.rglob("*.py")):
        source = path.read_text()
        for name in MODEL_PACKAGES:
            assert not re.search(rf"(?m)^\s*(import|from)\s+{name}", source), (
                path,
                name,
            )
    for lock in ("requirements.lock", "requirements-dev.lock"):
        content = (REPO / lock).read_text().lower()
        for name in MODEL_PACKAGES:
            assert not re.search(rf"(?m)^{name}[a-z0-9_.-]*==", content), (lock, name)


# ── the fake adapter and the stored row ──────────────────────


class FakeAdapter:
    """Descriptions by symbol; a value that is an Exception is raised."""

    source_name = SOURCE

    def __init__(self, descriptions):
        self.descriptions, self.calls = dict(descriptions), []

    def fetch(self, symbol):
        self.calls.append(symbol)
        value = self.descriptions.get(symbol)
        if isinstance(value, Exception):
            raise value
        if value is None:
            return None
        if isinstance(value, tuple):
            body, url = value
            return cs().Description(text=body, url=url)
        return cs().Description(text=value, url=URL)


def members(engine, *symbols):
    for symbol in symbols:
        seed_security(
            engine,
            symbol,
            name=f"Synthetic {symbol}",
            profile={"sector": "Industrials"},
        )
    return list(symbols)


def stored(engine, symbol):
    with engine.connect() as conn:
        return (
            conn.execute(
                text(
                    "SELECT p.summary, p.summary_grade, p.summary_status, p.summary_source_name,"
                    " p.summary_source_url, p.summary_as_of, p.description_digest, p.xmin::text AS xmin"
                    " FROM perennial.company_profiles p JOIN perennial.securities s"
                    " ON s.company_profile_id = p.id WHERE s.symbol = :s"
                ),
                {"s": symbol},
            )
            .mappings()
            .one()
        )


def run(engine, adapter, symbols):
    return cs().summarize(engine, adapter, symbols=symbols)


# ── CS-1: ingestion stores the excerpt with its source ───────


@pytest.mark.db
def test_cs1_a_passing_window_is_stored_with_its_source(engine):
    import textstat

    symbols = members(engine, "ZZSA")
    before = datetime.now(timezone.utc)
    result = run(engine, FakeAdapter({"ZZSA": joined(EASY)}), symbols)
    after = datetime.now(timezone.utc)
    assert result.succeeded and result.error_code is None
    row = stored(engine, "ZZSA")
    assert row["summary"] == joined(EASY[:4])
    assert row["summary_status"] == "ok"
    assert (
        abs(float(row["summary_grade"]) - textstat.flesch_kincaid_grade(row["summary"]))
        < 0.006
    )
    assert (row["summary_source_name"], row["summary_source_url"]) == (SOURCE, URL)
    assert (
        row["summary_as_of"].tzinfo is not None
        and before <= row["summary_as_of"] <= after
    )
    assert re.fullmatch(r"[0-9a-f]{64}", row["description_digest"])


@pytest.mark.db
@pytest.mark.parametrize(
    "description",
    [joined(HARD), EASY[0], "", None],
    ids=["no_passing_window", "single_sentence", "empty", "no_description"],
)
def test_cs1_no_window_or_no_description_is_unavailable_and_counted(
    engine, description
):
    symbols = members(engine, "ZZSB")
    result = run(engine, FakeAdapter({"ZZSB": description}), symbols)
    assert result.succeeded
    row = stored(engine, "ZZSB")
    assert row["summary_status"] == "unavailable"
    assert row["summary"] is None and row["summary_grade"] is None
    assert result.unavailable == 1 and result.with_summary == 0


@pytest.mark.db
def test_cs1_a_provider_error_fails_the_run_and_touches_no_row(engine):
    symbols = members(engine, "ZZEA", "ZZEB", "ZZEC")
    first = {"ZZEA": joined(EASY), "ZZEB": joined(EASY), "ZZEC": joined(EASY)}
    assert run(engine, FakeAdapter(first), symbols).succeeded
    before = {s: stored(engine, s) for s in symbols}
    failing = {
        "ZZEA": joined(EASY[1:]),  # changed: would be rewritten if the run went on
        "ZZEB": cs().DescriptionAdapterError("http_503"),
        "ZZEC": joined(EASY[2:]),
    }
    result = run(engine, FakeAdapter(failing), symbols)
    assert not result.succeeded and result.error_code
    assert {
        s: stored(engine, s) for s in symbols
    } == before  # xmin included: no row rewritten


@pytest.mark.db
def test_cs1_an_unchanged_description_is_skipped_and_no_row_is_rewritten(engine):
    symbols = members(engine, "ZZDA", "ZZDB")
    descriptions = {"ZZDA": joined(EASY), "ZZDB": joined(HARD)}
    run(engine, FakeAdapter(descriptions), symbols)
    before = {s: stored(engine, s) for s in symbols}
    again = run(engine, FakeAdapter(descriptions), symbols)
    assert again.succeeded
    assert {s: stored(engine, s) for s in symbols} == before
    assert (again.with_summary, again.unavailable) == (
        1,
        1,
    )  # coverage is the stored state


@pytest.mark.db
def test_cs1_a_changed_description_is_processed_again(engine):
    symbols = members(engine, "ZZCA")
    run(engine, FakeAdapter({"ZZCA": joined(EASY)}), symbols)
    first = stored(engine, "ZZCA")
    run(engine, FakeAdapter({"ZZCA": joined(EASY[2:])}), symbols)
    second = stored(engine, "ZZCA")
    assert second["summary"] == joined(EASY[2:6])
    assert second["description_digest"] != first["description_digest"]


@pytest.mark.db
def test_cs1_stale_good_is_cleared_when_the_new_description_has_no_window(engine):
    symbols = members(engine, "ZZGA")
    run(engine, FakeAdapter({"ZZGA": joined(EASY)}), symbols)
    assert stored(engine, "ZZGA")["summary_status"] == "ok"
    result = run(engine, FakeAdapter({"ZZGA": joined(HARD)}), symbols)
    assert result.succeeded
    row = stored(engine, "ZZGA")
    assert row["summary_status"] == "unavailable"
    assert row["summary"] is None and row["summary_grade"] is None


@pytest.mark.db
def test_n1_bumping_the_selector_version_reprocesses_stored_rows(engine, monkeypatch):
    symbols = members(engine, "ZZVA")
    descriptions = {"ZZVA": joined(HARD)}
    run(engine, FakeAdapter(descriptions), symbols)
    skipped = stored(engine, "ZZVA")
    run(engine, FakeAdapter(descriptions), symbols)
    assert stored(engine, "ZZVA") == skipped  # the same version: skipped
    monkeypatch.setattr(cs(), "SELECTOR_VERSION", str(cs().SELECTOR_VERSION) + "-tuned")
    run(engine, FakeAdapter(descriptions), symbols)
    assert stored(engine, "ZZVA")["xmin"] != skipped["xmin"], (
        "a tuned selector skipped the row"
    )


@pytest.mark.db
def test_cs1_only_the_given_members_are_touched(engine):
    members(engine, "ZZMA", "ZZNM")
    outside = stored(engine, "ZZNM")
    adapter = FakeAdapter({"ZZMA": joined(EASY), "ZZNM": joined(EASY)})
    run(engine, adapter, ["ZZMA"])
    assert adapter.calls == ["ZZMA"]
    assert stored(engine, "ZZNM") == outside


@pytest.mark.db
def test_cs1_members_default_to_the_consensus_list(engine, monkeypatch):
    members(engine, "ZZQA")
    seen = []

    def consensus(given):
        seen.append(given)
        return ["ZZQA"]

    monkeypatch.setattr(cs(), "consensus_members", consensus)
    adapter = FakeAdapter({"ZZQA": joined(EASY)})
    result = cs().summarize(engine, adapter)
    assert seen == [engine] and adapter.calls == ["ZZQA"] and result.members == 1


@pytest.mark.db
def test_q11_consensus_members_are_the_current_publications_entries(
    engine, fixture_path
):
    from src.api.read import _current_publication_id, _envelope, _records

    publish_run(engine, feed(fixture_path))
    with engine.connect() as conn:
        _, output = _envelope(conn, _current_publication_id(conn))
        expected = sorted({r["ticker"] for r in _records(output) if r.get("ticker")})
    assert expected, "the fixture publication has no entries"
    assert list(cs().consensus_members(engine)) == expected


@pytest.mark.db
@pytest.mark.parametrize(
    "url",
    ["http://profiles.example.com/zz", "javascript:alert(1)", ""],
    ids=["http", "javascript", "empty"],
)
def test_cs1_a_non_https_source_url_is_never_stored(engine, url):
    symbols = members(engine, "ZZUA")
    run(engine, FakeAdapter({"ZZUA": (joined(EASY), url)}), symbols)
    row = stored(engine, "ZZUA")
    assert row["summary_status"] != "ok" and row["summary_source_url"] is None


# ── the checker's coupling note: ok needs every part ─────────

OK_PARTS = {
    "summary": "'" + joined(EASY[:2]) + "'",
    "summary_grade": "1.5",
    "summary_source_name": f"'{SOURCE}'",
    "summary_source_url": f"'{URL}'",
    "summary_as_of": "now()",
    "description_digest": "'" + hashlib.sha256(b"synthetic").hexdigest() + "'",
}


@pytest.mark.db
@pytest.mark.parametrize("missing", sorted(OK_PARTS))
def test_status_ok_requires_every_stored_part(engine, missing):
    members(engine, "ZZOK")
    profile = (
        "(SELECT company_profile_id FROM perennial.securities WHERE symbol = 'ZZOK')"
    )
    parts = {c: ("NULL" if c == missing else v) for c, v in OK_PARTS.items()}
    assignments = ", ".join(f"{c} = {v}" for c, v in parts.items())
    with pytest.raises(DBAPIError) as refused, engine.begin() as conn:
        conn.execute(
            text(
                f"UPDATE perennial.company_profiles SET summary_status = 'ok',"
                f" {assignments} WHERE id = {profile}"
            )
        )
    assert refused.value.orig.sqlstate == "23514"  # a CHECK


@pytest.mark.db
def test_status_ok_with_every_part_is_accepted(engine):
    members(engine, "ZZOL")
    profile = (
        "(SELECT company_profile_id FROM perennial.securities WHERE symbol = 'ZZOL')"
    )
    assignments = ", ".join(f"{c} = {v}" for c, v in OK_PARTS.items())
    with engine.begin() as conn:
        conn.execute(
            text(
                f"UPDATE perennial.company_profiles SET summary_status = 'ok',"
                f" {assignments} WHERE id = {profile}"
            )
        )


# ── CS-3: no network but the description source ─────────────


@pytest.mark.db
def test_cs3_the_job_makes_no_network_call_of_its_own(engine, monkeypatch):
    symbols = members(engine, "ZZNA")
    attempts = []

    def refuse(*args, **kwargs):
        attempts.append(args[:1])
        raise OSError("synthetic: no network")

    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(socket.socket, "connect", refuse)
    import requests

    monkeypatch.setattr(requests.Session, "request", refuse)
    before = set(sys.modules)
    result = run(engine, FakeAdapter({"ZZNA": joined(EASY)}), symbols)
    loaded = set(sys.modules) - before
    assert result.succeeded and attempts == []
    assert not {m for m in loaded if m.split(".")[0] in MODEL_PACKAGES}


# ── CS-7: the coverage report ────────────────────────────────


@pytest.mark.db
def test_cs7_each_run_reports_coverage_without_company_text(engine, caplog):
    import logging

    symbols = members(engine, "ZZRA", "ZZRB", "ZZRC")
    descriptions = {
        "ZZRA": joined(EASY),
        "ZZRB": joined(HARD + EASY[:4]),
        "ZZRC": joined(HARD),
    }
    caplog.set_level(logging.DEBUG)
    caplog.clear()
    result = run(engine, FakeAdapter(descriptions), symbols)
    assert (result.members, result.with_summary, result.unavailable) == (3, 2, 1)
    messages = [r.getMessage() for r in caplog.records]
    report = [m for m in messages if "members=3" in m]
    assert report, messages
    assert "with_summary=2" in report[0] and "unavailable=1" in report[0]
    for message in messages:
        for sentence in EASY + HARD:
            assert sentence not in message, message
        assert URL not in message


@pytest.mark.db
def test_cs7_coverage_counts_add_up_to_the_members(engine):
    symbols = members(engine, "ZZTA", "ZZTB")
    result = run(engine, FakeAdapter({"ZZTA": joined(EASY), "ZZTB": None}), symbols)
    assert result.succeeded and result.members == 2
    assert result.with_summary + result.unavailable == result.members


def test_the_module_has_a_selector_version():
    assert isinstance(cs().SELECTOR_VERSION, str) and cs().SELECTOR_VERSION
