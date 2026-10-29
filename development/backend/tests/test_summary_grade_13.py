"""S1-12 summaries: the readability limit (grade 13 on 2026-10-26, 16 since 2026-10-28).

Amended 2026-10-28 (decision 89, with the switch to SEC EDGAR descriptions):
MAX_GRADE is 16.0. The cases below now pin 16 where they pinned 13; the
re-check rules (2) are unchanged, and SELECTOR_VERSION stays s1-12-v2.

On staging's real data only 3 of 50 consensus members passed at grade 8
(company descriptions read around grade 16). Bryan, S1-12's owner, raised
MAX_GRADE from 8 to 13; S1-12 AC2 changes with it.

Pinned here:
1. MAX_GRADE == 16.0 (was 13.0). A window grading above 8 and at most 16 is an
   ok summary; above 16 stays unavailable; exactly 16 passes (grade <= MAX_GRADE).
2. A re-run re-checks rows decided under the old limit. The description digest
   includes MAX_GRADE itself, so the limit and the skip key can't drift, and
   SELECTOR_VERSION is bumped (s1-12-v1 -> s1-12-v2). A row stored "unavailable"
   under the old digest is reprocessed by the next run with the same
   description, with one fetch per member and no extra one.
3. Unchanged: verbatim selection and the stored source (the existing
   test_s1_12_ingestion, test_s1_12_summaries_cli and test_fmp_402 suites pin
   the rest: stale-good on fetch errors and 402s, counts-only output).

MID's windows were graded with textstat 0.7.8 (the locked version) while these
tests were written: every window of MID is between 9.6 and 11.4, its opening
four 10.3; TOO_HARD's only window is 18.3; BETWEEN's opening four is 14.8 and
its first window at or below 13 starts at its third sentence (11.3). The
fixture tests below re-check that.
"""

import hashlib
import json

import pytest
from sqlalchemy import text
from test_s1_12_ingestion import (
    SOURCE,
    URL,
    FakeAdapter,
    cs,
    joined,
    members,
    run,
    stored,
)

MID = [
    "Revenue comes mainly from equipment sales and maintenance contracts.",
    "The business operates distribution centers in several regions.",
    "Most products are assembled at facilities in the United States.",
    "The company was founded in Ohio and employs about two thousand people.",
]
TOO_HARD = [
    "The company designs measurement equipment used by industrial factories.",
    "Its customers include manufacturers, universities, and government agencies.",
]
BETWEEN = [*TOO_HARD, MID[0], MID[1]]


def windows(sentences):
    return [
        joined(sentences[start : start + size])
        for start in range(len(sentences))
        for size in (4, 3, 2)
        if start + size <= len(sentences)
    ]


# ── The fixtures grade where this file says ──────────────


def test_fixture_every_mid_window_is_above_8_and_at_most_13():
    import textstat

    grades = [textstat.flesch_kincaid_grade(w) for w in windows(MID)]
    assert all(8.5 < g < 12.5 for g in grades), grades


def test_fixture_too_hard_is_above_16():
    import textstat

    assert [textstat.flesch_kincaid_grade(w) for w in windows(TOO_HARD)] == [
        pytest.approx(18.27, abs=0.01)
    ]


def test_fixture_between_opens_between_13_and_16_with_a_later_window_under_13():
    import textstat

    assert textstat.flesch_kincaid_grade(joined(BETWEEN)) == pytest.approx(14.8, abs=0.05)
    assert textstat.flesch_kincaid_grade(joined(BETWEEN[2:])) == pytest.approx(11.33, abs=0.05)
    above_13 = [joined(BETWEEN[s : s + n]) for s, n in ((0, 3), (0, 2), (1, 3), (1, 2))]
    assert all(textstat.flesch_kincaid_grade(w) > 13.5 for w in above_13)


# ── 1: the limit ─────────────────────────────────────────


def test_max_grade_is_16():
    assert cs().MAX_GRADE == 16.0


def test_a_window_between_grade_13_and_16_now_wins_over_a_later_one():
    """At 13 the first passing window was BETWEEN[2:]; at 16 it is the opening four."""
    excerpt = cs().select_excerpt(joined(BETWEEN))
    assert excerpt is not None
    assert excerpt.text == joined(BETWEEN)
    assert 13.0 < float(excerpt.grade) <= 16.0


def test_a_window_between_grade_8_and_13_is_now_selected_verbatim():
    description = joined(MID)
    excerpt = cs().select_excerpt(description)
    assert excerpt is not None
    assert excerpt.text == joined(MID)  # the first window, the longest size first
    assert excerpt.text in description
    assert 8.0 < float(excerpt.grade) <= 16.0


def test_above_16_is_still_unavailable():
    assert cs().select_excerpt(joined(TOO_HARD)) is None


@pytest.mark.parametrize("grade,selected", [(16.0, True), (16.01, False)])
def test_exactly_16_passes(monkeypatch, grade, selected):
    import textstat

    monkeypatch.setattr(textstat, "flesch_kincaid_grade", lambda _text: grade)
    excerpt = cs().select_excerpt(joined(MID))
    assert (excerpt is not None) is selected
    if selected:
        assert float(excerpt.grade) == 16.0


# ── 2: rows decided under the old limit are re-checked ───


def test_the_selector_version_is_bumped():
    assert cs().SELECTOR_VERSION == "s1-12-v2"


def old_digest(description):
    """The digest S1-12 stored before this change (selector s1-12-v1, no limit in it)."""
    key = {
        "selector_version": "s1-12-v1",
        "source_name": SOURCE,
        "url": URL,
        "text": description,
    }
    encoded = json.dumps(key, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8", "surrogatepass")).hexdigest()


@pytest.mark.db
def test_a_row_stored_unavailable_under_grade_8_becomes_ok_on_the_next_run(engine):
    """Staging's state: rows the grade-8 run decided, with the old digest."""
    import textstat

    symbols = members(engine, "ZZMID")
    description = joined(MID)
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE perennial.company_profiles p"
                " SET summary_status = 'unavailable', description_digest = :d"
                " FROM perennial.securities s"
                " WHERE s.company_profile_id = p.id AND s.symbol = 'ZZMID'"
            ),
            {"d": old_digest(description)},
        )
    adapter = FakeAdapter({"ZZMID": description})
    result = run(engine, adapter, symbols)
    assert result.succeeded
    assert adapter.calls == ["ZZMID"]  # the run's one fetch, nothing extra
    row = stored(engine, "ZZMID")
    assert row["summary_status"] == "ok"
    assert row["summary"] == joined(MID)
    assert row["summary"] in description  # verbatim
    assert (row["summary_source_name"], row["summary_source_url"]) == (SOURCE, URL)
    assert (
        abs(float(row["summary_grade"]) - textstat.flesch_kincaid_grade(row["summary"]))
        < 0.006
    )
    assert row["description_digest"] != old_digest(description)
    assert (result.with_summary, result.unavailable) == (1, 0)


@pytest.mark.db
def test_the_digest_includes_the_limit_itself(engine, monkeypatch):
    """Same selector version, same description: only the limit changes, and the
    stored decision is re-checked. A limit outside the digest would skip it."""
    symbols = members(engine, "ZZLIM")
    description = {"ZZLIM": joined(MID)}
    limit = cs().MAX_GRADE
    monkeypatch.setattr(cs(), "MAX_GRADE", 8.0)
    first = run(engine, FakeAdapter(description), symbols)
    assert first.succeeded
    assert stored(engine, "ZZLIM")["summary_status"] == "unavailable"
    monkeypatch.setattr(cs(), "MAX_GRADE", limit)
    adapter = FakeAdapter(description)
    second = run(engine, adapter, symbols)
    assert second.succeeded
    assert adapter.calls == ["ZZLIM"]
    assert stored(engine, "ZZLIM")["summary_status"] == "ok"
    assert (second.with_summary, second.unavailable) == (1, 0)


@pytest.mark.db
def test_an_ok_row_under_the_new_limit_is_still_skipped_on_an_unchanged_run(engine):
    symbols = members(engine, "ZZSKIP")
    description = {"ZZSKIP": joined(MID)}
    run(engine, FakeAdapter(description), symbols)
    before = stored(engine, "ZZSKIP")
    assert before["summary_status"] == "ok"
    run(engine, FakeAdapter(description), symbols)
    assert stored(engine, "ZZSKIP") == before  # xmin included: not rewritten
