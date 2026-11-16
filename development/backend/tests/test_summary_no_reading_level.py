"""S1-12 summaries: no reading-level limit (Bryan, 2026-11-16).

The Flesch–Kincaid limit (grade 8, then 13 on 2026-10-26 and 16 on 2026-10-28)
left companies without a summary whenever their 10-K had no window at or below
it. It is gone: the first storable window of 2–4 sentences is the summary.

Pinned here:
1. There is no MAX_GRADE. A window is selected whatever textstat grades it.
2. The grade is still recorded (ck_company_profiles_summary_ok_complete needs
   one on every ok row), kept inside the column's Numeric(5, 2).
3. SELECTOR_VERSION is s1-12-v3 and the digest no longer has max_grade, so the
   next run re-checks every row the grade-16 selector decided, with one fetch
   per member. A row it stored "unavailable" becomes ok.
4. Unchanged: verbatim selection, the stored source, and skipping an unchanged
   row (test_s1_12_ingestion pins the rest).

TOO_HARD's only window was graded 18.3 with textstat 0.7.8 (the locked
version): above every limit there has been.
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


def test_fixture_too_hard_is_above_the_last_limit():
    import textstat

    assert textstat.flesch_kincaid_grade(joined(TOO_HARD)) > 16.0


# ── 1: no limit ──────────────────────────────────────────


def test_there_is_no_max_grade():
    assert not hasattr(cs(), "MAX_GRADE")


def test_a_window_above_grade_16_is_selected_verbatim():
    excerpt = cs().select_excerpt(joined(TOO_HARD))
    assert excerpt is not None
    assert excerpt.text == joined(TOO_HARD)
    assert float(excerpt.grade) > 16.0


def test_the_opening_window_wins_over_an_easier_later_one():
    """Under a limit, TOO_HARD at the start pushed the window to a later start."""
    excerpt = cs().select_excerpt(joined(TOO_HARD + MID))
    assert excerpt is not None
    assert excerpt.text == joined(TOO_HARD + MID[:2])


@pytest.mark.parametrize("grade", [-50.0, 16.01, 400.0])
def test_any_grade_is_selected(monkeypatch, grade):
    import textstat

    monkeypatch.setattr(textstat, "flesch_kincaid_grade", lambda _text: grade)
    excerpt = cs().select_excerpt(joined(MID))
    assert excerpt is not None and excerpt.text == joined(MID)
    assert float(excerpt.grade) == grade


# ── 2: the grade is recorded, inside the column ──────────


@pytest.mark.db
@pytest.mark.parametrize(
    "grade,kept", [(18.27, 18.27), (12345.6, 999.99), (-12345.6, -999.99)]
)
def test_the_stored_grade_fits_the_column(engine, monkeypatch, grade, kept):
    import textstat

    monkeypatch.setattr(textstat, "flesch_kincaid_grade", lambda _text: grade)
    symbols = members(engine, "ZZFIT")
    result = run(engine, FakeAdapter({"ZZFIT": joined(TOO_HARD)}), symbols)
    assert result.succeeded
    row = stored(engine, "ZZFIT")
    assert row["summary_status"] == "ok" and row["summary"] == joined(TOO_HARD)
    assert float(row["summary_grade"]) == pytest.approx(kept)


# ── 3: rows the grade-16 selector decided are re-checked ─


def test_the_selector_version_is_bumped():
    assert cs().SELECTOR_VERSION == "s1-12-v3"


def old_digest(description):
    """The digest the grade-16 selector stored (s1-12-v2, with max_grade)."""
    key = {
        "selector_version": "s1-12-v2",
        "max_grade": 16.0,
        "source_name": SOURCE,
        "url": URL,
        "text": description,
    }
    encoded = json.dumps(key, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8", "surrogatepass")).hexdigest()


@pytest.mark.db
def test_a_row_stored_unavailable_under_grade_16_becomes_ok_on_the_next_run(engine):
    """Staging's state: rows the grade-16 run decided, with the old digest."""
    import textstat

    symbols = members(engine, "ZZHARD")
    description = joined(TOO_HARD)
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE perennial.company_profiles p"
                " SET summary_status = 'unavailable', description_digest = :d"
                " FROM perennial.securities s"
                " WHERE s.company_profile_id = p.id AND s.symbol = 'ZZHARD'"
            ),
            {"d": old_digest(description)},
        )
    adapter = FakeAdapter({"ZZHARD": description})
    result = run(engine, adapter, symbols)
    assert result.succeeded
    assert adapter.calls == ["ZZHARD"]  # the run's one fetch, nothing extra
    row = stored(engine, "ZZHARD")
    assert row["summary_status"] == "ok"
    assert row["summary"] == description  # verbatim
    assert (row["summary_source_name"], row["summary_source_url"]) == (SOURCE, URL)
    assert (
        abs(float(row["summary_grade"]) - textstat.flesch_kincaid_grade(row["summary"]))
        < 0.006
    )
    assert row["description_digest"] != old_digest(description)
    assert (result.with_summary, result.unavailable) == (1, 0)


# ── 4: unchanged ─────────────────────────────────────────


@pytest.mark.db
def test_an_ok_row_is_still_skipped_on_an_unchanged_run(engine):
    symbols = members(engine, "ZZSKIP")
    description = {"ZZSKIP": joined(TOO_HARD)}
    run(engine, FakeAdapter(description), symbols)
    before = stored(engine, "ZZSKIP")
    assert before["summary_status"] == "ok"
    run(engine, FakeAdapter(description), symbols)
    assert stored(engine, "ZZSKIP") == before  # xmin included: not rewritten
