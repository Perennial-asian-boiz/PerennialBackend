"""Auditor F1 on fix/sec-names @ d85204e: bound the stored SEC title, spec-first.

SEC's title goes into securities.name, which /api/watchlist and
/v1/securities/{symbol} serve. Today any non-blank string is stored as filed;
the auditor's probe passed NUL plus an ANSI escape, a 100,000-character title,
zero-width and right-to-left-override characters, and "<script>".

Pinned (the coordinator's row of 2026-10-29, mirroring company_summaries'
_printable rule and its cap):
- A title containing any control (Cc) or format (Cf) character, or longer than
  200 characters, is skipped. It is not stored, not cleaned and not cut: the
  security's name stays NULL.
- run()'s result, and main()'s stdout counts, gain titles_skipped: the
  empty-named securities whose SEC title was skipped. Those aren't counted in
  not_in_sec_list, which still counts securities with no SEC entry.
- A title of exactly 200 characters is stored. Ordinary punctuation is stored as
  filed: & < > ' " , . - ( ) and non-ASCII letters. The frontend escapes text,
  so a "<" in a real company name is the frontend's to render, not ours to
  refuse.
- Other securities in the same run are filled as before.
"""

import json

import pytest
from test_sec_names import (  # noqa: F401  (no_retries, sec_get: fixtures)
    UA,
    FakeList,
    client,
    names,
    no_retries,
    owner_url,
    sec_get,
    security,
    seed_names,
)

CAP = 200

BAD = [
    pytest.param("SYNTHETIC\x00 CORP", id="nul"),
    pytest.param("SYNTHETIC \x1b[31mCORP\x1b[0m", id="ansi_escape"),
    pytest.param("SYNTHETIC\nCORP", id="newline"),
    pytest.param("SYNTHETIC\tCORP", id="tab"),
    pytest.param("SYNTHETIC CORP\x7f", id="del"),
    pytest.param("SYNTHETIC\x85CORP", id="c1_next_line"),
    pytest.param("SYNTHETIC​CORP", id="zero_width_space"),
    pytest.param("SYNTHETIC‍CORP", id="zero_width_joiner"),
    pytest.param("SYNTHETIC ‮CORP", id="rtl_override"),
    pytest.param("SYNTHETIC ⁦CORP⁩", id="bidi_isolate"),
    pytest.param("﻿SYNTHETIC CORP", id="bom"),
    pytest.param("SYNTHETIC­CORP", id="soft_hyphen"),
    pytest.param("S" * (CAP + 1), id="cap_plus_one"),
    pytest.param("SYNTHETIC CORP " * 6667, id="about_100000_chars"),
]

GOOD = [
    pytest.param("SYNTHETIC AT&T-LIKE INC", id="ampersand"),
    pytest.param('SYNTHETIC <HOLDINGS> & "PARTNERS", L.P.', id="angle_brackets_quotes"),
    pytest.param("SYNTHETIC O'BRIEN (U.S.) CO.", id="apostrophe_parens"),
    pytest.param("Synthétique Société Anonyme", id="non_ascii_letters"),
    pytest.param("S" * CAP, id="exactly_the_cap"),
]


@pytest.mark.db
@pytest.mark.parametrize("title", BAD)
def test_a_control_format_or_overlong_title_is_skipped(engine, no_retries, title):  # noqa: F811
    security(engine, "ZZBD")
    security(engine, "ZZOK")
    fake = FakeList([("ZZBD", title), ("ZZOK", "SYNTHETIC GOOD CO")])
    counts = seed_names().run(engine, client(fake))
    # Not stored, not cleaned, not cut: NULL.
    assert names(engine) == {"ZZBD": None, "ZZOK": "SYNTHETIC GOOD CO"}
    assert counts["titles_skipped"] == 1
    assert counts["names_filled"] == 1
    assert counts["not_in_sec_list"] == 0


@pytest.mark.db
@pytest.mark.parametrize("title", GOOD)
def test_ordinary_punctuation_and_the_cap_itself_are_stored_as_filed(
    engine,
    no_retries,  # noqa: F811
    title,
):
    security(engine, "ZZGD")
    counts = seed_names().run(engine, client(FakeList([("ZZGD", title)])))
    assert names(engine) == {"ZZGD": title}
    assert counts["names_filled"] == 1 and counts["titles_skipped"] == 0


@pytest.mark.db
def test_the_counts_keep_skipped_and_missing_apart(engine, no_retries):  # noqa: F811
    for symbol in ("ZZB1", "ZZB2", "ZZOK", "IBIT"):
        security(engine, symbol)
    security(engine, "ZZNM", name="Reviewed Name")  # has a name: never a candidate
    rows = [
        ("ZZB1", "SYNTHETIC\x00 ONE"),
        ("ZZB2", "S" * (CAP + 1)),
        ("ZZOK", "SYNTHETIC GOOD CO"),
        ("ZZNM", "SYNTHETIC‮ NAMED"),
    ]
    counts = seed_names().run(engine, client(FakeList(rows)))
    assert counts["names_filled"] == 1
    assert counts["titles_skipped"] == 2  # ZZNM already named: not a skip
    assert counts["not_in_sec_list"] == 1  # IBIT
    assert names(engine)["ZZNM"] == "Reviewed Name"
    assert all(isinstance(v, int) and not isinstance(v, bool) for v in counts.values())


@pytest.mark.db
def test_main_prints_the_skip_count_and_no_title(engine, sec_get, monkeypatch, capsys):  # noqa: F811
    security(engine, "ZZB1")
    security(engine, "ZZOK")
    sec_get.rows = [("ZZB1", "SYNTHETIC\x1b[2J BAD"), ("ZZOK", "SYNTHETIC GOOD CO")]
    monkeypatch.setenv("OPS_DATABASE_URL", owner_url(engine))
    monkeypatch.setenv("SEC_USER_AGENT", UA)
    assert seed_names().main() == 0
    out = capsys.readouterr()
    counts = json.loads(out.out)
    assert counts["titles_skipped"] == 1 and counts["names_filled"] == 1
    for output in (out.out, out.err):
        assert "SYNTHETIC" not in output and "\x1b" not in output and UA not in output
    assert names(engine) == {"ZZB1": None, "ZZOK": "SYNTHETIC GOOD CO"}
