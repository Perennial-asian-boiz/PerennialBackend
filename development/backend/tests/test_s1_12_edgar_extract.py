"""S1-12 EDGAR, ED-3: Item 1 ("Business") prose from a 10-K's HTML, spec-first.

Spec: docs/work-items/s1-12-edgar-descriptions-packet.md @ 2d52f06 (decisions
89-91). The seam is a pure function, sec_descriptions.item_1_text(html) -> str |
None, using the standard library's html.parser:
1. Drop script, style, head, title, ix:header and any element whose inline style
   has display:none, with everything inside it.
2. p, div, li, h1-h6, tr, table and br end a block; a row's cells are joined
   with one space; whitespace (U+00A0 included) collapses; empty blocks drop.
3. A heading is a block of at most 120 characters. Start:
   `items?\\s*1(\\s*(and|&|,)\\s*2)?\\s*[.:\\-–—]?\\s*business\\b.*`; end:
   `item\\s*(1a|1b|1c|2|3)\\b.*`, both case-insensitive full matches. Table rows
   count as headings.
4. Each start runs to the first end after it; the section with the most prose
   wins (a table of contents has none). No start, or no end after any start: None.
5. Prose: blocks outside tables ending in . ! or ?, optionally followed by closing
   quotes or a bracket, and without U+FFFD.
6. Kept blocks are joined with "\\n\\n", whole blocks only, at most
   MAX_DESCRIPTION_CHARS (100,000) in all. No prose: None.
Decoding (step 7) is fetch's and is tested in test_s1_12_edgar_adapter.py.
Synthetic HTML only.
"""

import pytest

ITEM_1 = "<p>Item 1. Business</p>"
ITEM_1A = "<p>Item 1A. Risk Factors</p><p>Risks are described here.</p>"


def extract(*parts):
    from src.ingestion.sec_descriptions import item_1_text

    return item_1_text("<html><body>" + "".join(parts) + "</body></html>")


def paras(*sentences):
    return "".join(f"<p>{s}</p>" for s in sentences)


def test_the_section_between_item_1_and_item_1a_is_its_prose_blocks():
    assert (
        extract(
            "<p>Cover page text.</p>",
            ITEM_1,
            paras("Acme makes garden tools.", "It sells them in shops."),
            ITEM_1A,
        )
        == "Acme makes garden tools.\n\nIt sells them in shops."
    )


@pytest.mark.parametrize(
    "heading",
    [
        "Item 1. Business",
        "ITEM 1. BUSINESS",
        "Item 1: Business",
        "Item 1 - Business",
        "Item 1 — Business",
        "Item 1 – Business",
        "Item 1Business",
        "Items 1 and 2. Business and Properties",
        "Items 1 &amp; 2. Business and Properties",
        "Items 1, 2. Business and Properties",
        "Item&nbsp;1.&nbsp;Business",
        "Item 1. Business Overview",
    ],
)
def test_start_heading_forms(heading):
    assert (
        extract(f"<p>{heading}</p>", paras("Acme makes tools."), ITEM_1A)
        == "Acme makes tools."
    )


@pytest.mark.parametrize(
    "end",
    [
        "Item 1A. Risk Factors",
        "ITEM 1B. UNRESOLVED STAFF COMMENTS",
        "Item 1C. Cybersecurity",
        "Item 2. Properties",
        "Item 3. Legal Proceedings",
    ],
)
def test_end_heading_forms(end):
    assert (
        extract(ITEM_1, paras("Acme makes tools."), f"<p>{end}</p>", paras("After."))
        == "Acme makes tools."
    )


@pytest.mark.parametrize(
    "not_an_end", ["Item 10. Directors", "Item 20. Exhibits", "Item 4. Mine Safety"]
)
def test_other_items_dont_end_the_section(not_an_end):
    assert extract(
        ITEM_1, paras("First."), f"<p>{not_an_end}</p>", paras("Second."), ITEM_1A
    ) == ("First.\n\nSecond.")


def test_a_table_of_contents_loses_to_the_real_section():
    toc = (
        "<table>"
        "<tr><td>Item 1.</td><td>Business</td><td>3</td></tr>"
        "<tr><td>Item 1A.</td><td>Risk Factors</td><td>9</td></tr>"
        "<tr><td>Item 2.</td><td>Properties</td><td>20</td></tr>"
        "</table>"
    )
    assert (
        extract(
            toc,
            "<p>Forward-looking statements are discussed here.</p>",
            ITEM_1,
            paras("Acme makes tools.", "It has two plants."),
            ITEM_1A,
        )
        == "Acme makes tools.\n\nIt has two plants."
    )


@pytest.mark.parametrize("long_first", [True, False], ids=["long_first", "long_last"])
def test_the_longest_prose_section_wins_whatever_its_position(long_first):
    short = "<p>Item 1. Business</p><p>See below.</p><p>Item 1A. Risk Factors</p>"
    long = (
        ITEM_1
        + paras("Acme makes tools for many gardens.", "It has two plants.")
        + "<p>Item 2. Properties</p>"
    )
    parts = (long, short) if long_first else (short, long)
    assert extract(*parts) == "Acme makes tools for many gardens.\n\nIt has two plants."


def test_a_heading_laid_out_as_a_two_cell_table_row():
    heading = "<table><tr><td>ITEM 1.</td><td>BUSINESS</td></tr></table>"
    end = "<table><tr><td>ITEM 1A.</td><td>RISK FACTORS</td></tr></table>"
    assert (
        extract(heading, paras("Acme makes tools."), end, paras("After."))
        == "Acme makes tools."
    )


def test_hidden_inline_xbrl_header_is_ignored():
    hidden = (
        '<div style="display:none"><ix:header>'
        + ITEM_1
        + paras("Hidden sentence one is long enough to win.", "Hidden sentence two.")
        + ITEM_1A
        + "</ix:header></div>"
    )
    assert extract(hidden, ITEM_1, paras("Acme makes tools."), ITEM_1A) == (
        "Acme makes tools."
    )


def test_ix_header_alone_is_dropped_with_its_descendants():
    hidden = (
        "<ix:header><ix:hidden>"
        + ITEM_1
        + paras("Hidden sentence one is long enough to win.", "Hidden sentence two.")
        + ITEM_1A
        + "</ix:hidden></ix:header>"
    )
    assert extract(hidden, ITEM_1, paras("Acme makes tools."), ITEM_1A) == (
        "Acme makes tools."
    )


@pytest.mark.parametrize(
    "style", ["display:none", "display: none", "color:red; display: none;"]
)
def test_display_none_elements_are_dropped_with_their_descendants(style):
    hidden = f'<div style="{style}"><p>Hidden <b>bold</b> sentence.</p></div>'
    assert (
        extract(ITEM_1, paras("Acme makes tools."), hidden, paras("Visible."), ITEM_1A)
        == "Acme makes tools.\n\nVisible."
    )


def test_script_style_head_and_title_never_reach_the_text():
    from src.ingestion.sec_descriptions import item_1_text

    html = (
        "<html><head><title>Item 1. Business</title>"
        "<style>Headed style sentence.</style></head><body>"
        + ITEM_1
        + "<script>Scripted sentence.</script>"
        + paras("Acme makes tools.")
        + "<style>Styled sentence.</style>"
        + "<title>Titled sentence.</title>"
        + "<p>Kept <script>Inline scripted.</script>sentence.</p>"
        + ITEM_1A
        + "</body></html>"
    )
    assert item_1_text(html) == "Acme makes tools.\n\nKept sentence."


def test_footers_and_table_of_contents_links_between_paragraphs_drop():
    assert (
        extract(
            ITEM_1,
            paras("Acme makes tools."),
            "<p>Acme Corp | 2025 Form 10-K | 3</p>",
            '<p><a href="#toc">Table of Contents</a></p>',
            "<hr/>",
            paras("It has two plants."),
            ITEM_1A,
        )
        == "Acme makes tools.\n\nIt has two plants."
    )


def test_a_subheading_is_not_part_of_the_first_sentence():
    assert (
        extract(
            ITEM_1,
            "<p><b>Company Background</b></p>",
            paras("Acme makes tools."),
            "<h3>Products</h3>",
            paras("It sells rakes."),
            ITEM_1A,
        )
        == "Acme makes tools.\n\nIt sells rakes."
    )


def test_a_cross_reference_index_10k_with_no_item_1_heading_is_none():
    assert (
        extract(
            "<p>Form 10-K Cross-Reference Index</p>",
            "<table><tr><td>Business</td><td>Pages 4-20</td></tr></table>",
            paras("Our business is described on the pages listed above."),
            ITEM_1A,
        )
        is None
    )


def test_item_1_with_no_end_heading_after_it_is_none():
    assert extract(ITEM_1, paras("Acme makes tools.", "It sells them.")) is None


def test_an_end_heading_only_before_the_start_is_none():
    assert extract(ITEM_1A, ITEM_1, paras("Acme makes tools.")) is None


def test_a_section_with_no_prose_is_none():
    assert extract(ITEM_1, "<p>Overview</p><p>Page 3</p>", ITEM_1A) is None


def test_a_block_over_120_characters_is_not_a_heading():
    long_heading = "Item 1. Business " + "x" * 104  # 121 characters
    assert len(long_heading) == 121
    assert (
        extract(f"<p>{long_heading}</p>", paras("Acme makes tools."), ITEM_1A) is None
    )
    exact = "Item 1. Business " + "x" * 103  # 120 characters
    assert extract(f"<p>{exact}</p>", paras("Acme makes tools."), ITEM_1A) == (
        "Acme makes tools."
    )


def test_prose_ends_in_sentence_punctuation_with_optional_closing_marks():
    assert (
        extract(
            ITEM_1,
            paras(
                "Acme makes tools.",
                "Will it grow?",
                "It will!",
                'Its motto is "built to last."',
                "(Founded in Ohio.)",
                "Its motto is “built to last.”",
                "No ending punctuation here",
                "Ends in a colon:",
            ),
            ITEM_1A,
        )
        == "Acme makes tools.\n\nWill it grow?\n\nIt will!\n\n"
        'Its motto is "built to last."\n\n(Founded in Ohio.)\n\n'
        "Its motto is “built to last.”"
    )


@pytest.mark.parametrize("bullet", ["1.", "3.1.", "(2).", "\u2014.", "* * *.", "1.)"])
def test_a_block_without_a_letter_is_not_prose(bullet):
    """Checker note on 441f00b: a lone Word bullet block such as "1." ends in
    sentence punctuation, but it isn't a sentence; prose needs at least one letter."""
    assert (
        extract(
            ITEM_1,
            f"<p>{bullet}</p>",
            paras("Acme makes tools."),
            f"<p>{bullet}</p>",
            paras("It sells rakes."),
            ITEM_1A,
        )
        == "Acme makes tools.\n\nIt sells rakes."
    )


def test_a_numbered_sentence_with_letters_is_still_prose():
    assert extract(
        ITEM_1, paras("1. Acme makes tools.", "\u00c9tude sells rakes."), ITEM_1A
    ) == ("1. Acme makes tools.\n\n\u00c9tude sells rakes.")


def test_blocks_inside_tables_are_never_prose():
    assert (
        extract(
            ITEM_1,
            "<table><tr><td>This cell reads like a sentence.</td></tr>"
            "<tr><td><p>So does this paragraph in a table.</p></td></tr></table>",
            paras("Acme makes tools."),
            ITEM_1A,
        )
        == "Acme makes tools."
    )


def test_a_block_containing_the_replacement_character_is_dropped():
    assert (
        extract(
            ITEM_1,
            paras("Acme makes tools.", "It earned �5 million.", "It has staff."),
            ITEM_1A,
        )
        == "Acme makes tools.\n\nIt has staff."
    )


def test_inline_tags_dont_split_and_whitespace_collapses():
    assert (
        extract(
            ITEM_1,
            "<p>Acme\n   makes <b>garden</b>&nbsp;<i>tools</i> for   homes.</p>",
            ITEM_1A,
        )
        == "Acme makes garden tools for homes."
    )


@pytest.mark.parametrize("tag", ["p", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6"])
def test_block_elements_end_a_block(tag):
    assert (
        extract(ITEM_1, f"<{tag}>First.</{tag}><{tag}>Second.</{tag}>", ITEM_1A)
        == "First.\n\nSecond."
    )


def test_br_ends_a_block():
    assert extract(ITEM_1, "<p>First.<br>Second.<br/>Third.</p>", ITEM_1A) == (
        "First.\n\nSecond.\n\nThird."
    )


def test_character_references_are_converted():
    assert (
        extract(
            ITEM_1, "<p>Smith &amp; Sons&#8217; tools &#x2014; since 1900.</p>", ITEM_1A
        )
        == "Smith & Sons’ tools — since 1900."
    )


def test_prose_over_the_cap_keeps_whole_blocks_only():
    from src.ingestion.company_summaries import MAX_DESCRIPTION_CHARS

    assert MAX_DESCRIPTION_CHARS == 100_000
    blocks = [f"Paragraph {i:03d} " + "word " * 196 + "ends here." for i in range(120)]
    assert all(len(b) == 1004 for b in blocks)
    text = extract(ITEM_1, paras(*blocks), ITEM_1A)
    assert text is not None
    assert len(text) <= MAX_DESCRIPTION_CHARS
    kept = text.split("\n\n")
    assert kept == blocks[: len(kept)]  # whole blocks, in order
    assert (
        len(text) + 2 + len(blocks[len(kept)]) > MAX_DESCRIPTION_CHARS
    )  # as many as fit


def test_a_document_of_only_markup_or_empty_is_none():
    from src.ingestion.sec_descriptions import item_1_text

    assert item_1_text("") is None
    assert item_1_text("<html><body></body></html>") is None
