"""
Company descriptions from SEC EDGAR: the latest 10-K's Item 1 (S1-12,
decisions 89-91; packet docs/work-items/s1-12-edgar-descriptions-packet.md).

A DescriptionAdapter for company_summaries. The job still picks a verbatim 2–4
sentence window (decision 72, ADR 0005: no model); this only supplies the text,
Item 1 ("Business") prose, and a link to the filing.

Per symbol, through EdgarClient's paced _get (User-Agent is the only header):
  * CIK (ED-1): a seeded company_profiles.cik wins; otherwise SEC's ticker map
    (share classes as BRK-B), fetched at most once per adapter and only when a
    symbol has no seed. No CIK: None.
  * Filing (ED-2): filings.recent only; the exact form "10-K" with the greatest
    filingDate, ties to the greatest accessionNumber (10-K/A, 10-KT, 20-F and
    40-F never count). A 10-K row with an unreadable date or accession, or a
    non-string primaryDocument, is skipped (F1). No well-formed 10-K, a
    submissions 404, or a filing older than MAX_FILING_AGE_DAYS (from today's
    UTC date): None.
  * Link (ED-6): https://www.sec.gov/Archives/edgar/data/<cik>/<accession
    without dashes>/<primaryDocument>. A primaryDocument that isn't a plain
    .htm/.html filename is None, so no path from a response can change host or
    directory.
  * Text (ED-3): item_1_text over the document, decoded as UTF-8 (BOM allowed)
    or else cp1252 with replacement. A 404, a document over
    edgar.MAX_DOCUMENT_BYTES (20 MiB), or no Item 1 prose is None.

Errors (ED-4): every failure leaves as DescriptionAdapterError with a fixed
code: the transport's (http_NNN, timeout, deadline_exceeded, invalid_json,
request_error:* reduced to request_failed), ticker_map_malformed,
submissions_malformed (no filings.recent, or a missing or ragged column), or
unexpected_error for anything else, raised with no
chained cause or context. Nothing from a filing, the User-Agent or a URL is
logged here.
"""

import re
from collections.abc import Mapping
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Engine

from src.db.models import company_profiles, securities
from src.ingestion.company_summaries import (
    MAX_DESCRIPTION_CHARS,
    Description,
    DescriptionAdapterError,
)
from src.services.fundamentals.edgar import EdgarClient, EdgarError

SOURCE_NAME = "SEC EDGAR"
# E2 is open: 550 days (about 18 months) is the packet's recommendation.
MAX_FILING_AGE_DAYS = 550
ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{folder}/{document}"
MAX_HEADING_CHARS = 120

_ACCESSION = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_ISO_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_PRIMARY_DOCUMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}")
_START_HEADING = re.compile(
    r"items?\s*1(\s*(and|&|,)\s*2)?\s*[.:\-–—]?\s*business\b.*", re.IGNORECASE
)
_END_HEADING = re.compile(r"item\s*(1a|1b|1c|2|3)\b.*", re.IGNORECASE)
_PROSE_END = re.compile(r"[.!?][\"'”’)\]]*$")
_WHITESPACE = re.compile(r"[\s ]+")

# ── ED-3: Item 1 prose from the 10-K's HTML ─────────────

_DROPPED = frozenset({"script", "style", "head", "title", "ix:header"})
_BLOCKS = frozenset(
    {"p", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6", "tr", "table", "br"}
)
_CELLS = frozenset({"td", "th"})
# No end tag, so never the root of a dropped subtree.
_VOID = frozenset(
    {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta"}
    | {"source", "track", "wbr"}
)


def _hidden(attrs: list[tuple[str, str | None]]) -> bool:
    return any(
        name == "style"
        and value is not None
        and "display:none" in _WHITESPACE.sub("", value.lower())
        for name, value in attrs
    )


class _Blocks(HTMLParser):
    """The document's text blocks, each with whether it came from inside a table."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.blocks: list[tuple[str, bool]] = []
        self._text: list[str] = []
        self._tables = 0
        self._dropping: str | None = None  # the dropped subtree's root tag
        self._depth = 0  # open elements of that tag inside it

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._dropping is not None:
            self._depth += tag == self._dropping
            return
        if tag not in _VOID and (tag in _DROPPED or _hidden(attrs)):
            self._dropping, self._depth = tag, 1
            return
        self._boundary(tag, opening=True)

    def handle_endtag(self, tag: str) -> None:
        if self._dropping is not None:
            if tag == self._dropping:
                self._depth -= 1
                if not self._depth:
                    self._dropping = None
            return
        self._boundary(tag, opening=False)

    def handle_data(self, data: str) -> None:
        if self._dropping is None:
            self._text.append(data)

    def close(self) -> None:
        super().close()
        self._flush()

    def _boundary(self, tag: str, opening: bool) -> None:
        if tag in _BLOCKS:
            self._flush()  # before the table count changes
            if tag == "table":
                self._tables = self._tables + 1 if opening else max(0, self._tables - 1)
        elif tag in _CELLS:
            self._text.append(" ")  # a row's cells are joined with one space

    def _flush(self) -> None:
        text = _WHITESPACE.sub(" ", "".join(self._text)).strip()
        self._text = []
        if text:
            self.blocks.append((text, self._tables > 0))


def _heading(pattern: re.Pattern[str], text: str) -> bool:
    return len(text) <= MAX_HEADING_CHARS and pattern.fullmatch(text) is not None


def _prose(text: str, in_table: bool) -> bool:
    """A sentence-ending block outside tables with a letter in it.

    Drops headings, page footers, TOC links and numbering such as "1." or "* * *.".
    """
    return (
        not in_table
        and any(c.isalpha() for c in text)
        and "�" not in text
        and _PROSE_END.search(text) is not None
    )


def item_1_text(html: str) -> str | None:
    """Item 1's prose blocks joined with blank lines, at most MAX_DESCRIPTION_CHARS.

    Each start heading's section runs to the first end heading after it; the
    section with the most prose wins (a table of contents has none). No start,
    no end after any start, or no prose: None.
    """
    parser = _Blocks()
    parser.feed(html)
    parser.close()
    blocks = parser.blocks
    # The first end heading at or after each index, in one backwards pass.
    next_end: list[int | None] = [None] * (len(blocks) + 1)
    for i in range(len(blocks) - 1, -1, -1):
        next_end[i] = i if _heading(_END_HEADING, blocks[i][0]) else next_end[i + 1]
    best: list[str] = []
    best_chars = 0
    for i, (text, _) in enumerate(blocks):
        end = next_end[i + 1]
        if end is None or not _heading(_START_HEADING, text):
            continue
        prose = [t for t, in_table in blocks[i + 1 : end] if _prose(t, in_table)]
        chars = sum(len(t) for t in prose)
        if chars > best_chars:
            best, best_chars = prose, chars
    kept: list[str] = []
    total = 0
    for text in best:  # whole blocks only, so a sentence is never cut
        total += len(text) + (2 if kept else 0)
        if total > MAX_DESCRIPTION_CHARS:
            break
        kept.append(text)
    return "\n\n".join(kept) or None


def _decode(body: bytes) -> str:
    try:
        return body.decode("utf-8-sig")
    except UnicodeDecodeError:
        return body.decode("cp1252", errors="replace")


# ── ED-2: the latest 10-K ────────────────────────────────


def _latest_10k(submissions: Mapping[str, Any]) -> tuple[date, str, str] | None:
    """(filingDate, accessionNumber, primaryDocument) of the newest exact 10-K."""
    filings = submissions.get("filings")
    recent = filings.get("recent") if isinstance(filings, dict) else None
    if not isinstance(recent, dict):
        raise EdgarError("submissions_malformed")
    keys = ("form", "filingDate", "accessionNumber", "primaryDocument")
    columns = [c for c in (recent.get(k) for k in keys) if isinstance(c, list)]
    if len(columns) != len(keys) or len({len(c) for c in columns}) != 1:
        raise EdgarError("submissions_malformed")  # a missing or ragged array
    best: tuple[date, str, str] | None = None
    for form, filed, accession, document in zip(*columns):
        # A malformed 10-K row is skipped (F1): one bad row in a company's
        # history must not fail every future all-or-nothing run.
        if not (
            form == "10-K"
            and isinstance(filed, str)
            and _ISO_DATE.fullmatch(filed)
            and isinstance(accession, str)
            and _ACCESSION.fullmatch(accession)
            and isinstance(document, str)
        ):
            continue
        try:
            day = date.fromisoformat(filed)
        except ValueError:  # an impossible date such as 2025-02-30
            continue
        if best is None or (day, accession) > best[:2]:
            best = (day, accession, document)
    return best


def _document_ok(name: str) -> bool:
    return _PRIMARY_DOCUMENT.fullmatch(name) is not None and name.lower().endswith(
        (".htm", ".html")
    )


def _transport_code(code: str | None) -> str:
    """EdgarError's code, reduced to [a-z0-9_]: request_error:<Name> keeps no detail."""
    if not code or code.startswith("request_error:"):
        return "request_failed"
    return code


class SecTenKDescriptions:
    """A DescriptionAdapter over the latest 10-K's Item 1 on SEC EDGAR."""

    source_name = SOURCE_NAME

    def __init__(self, client: EdgarClient, seeded_ciks: Mapping[str, int]) -> None:
        self._client = client
        self._seeded = dict(seeded_ciks)
        self._ticker_map: dict[str, int] | None = None

    def __repr__(self) -> str:
        return "SecTenKDescriptions()"  # never the User-Agent

    def fetch(self, symbol: str) -> Description | None:
        try:
            return self._fetch(symbol)
        except EdgarError as exc:
            code = _transport_code(exc.args[0] if exc.args else None)
        except Exception:  # nothing but a fixed code may leave the adapter
            code = "unexpected_error"
        # Raised outside the except block: no __cause__ or __context__ to leak.
        raise DescriptionAdapterError(code)

    def _cik(self, symbol: str) -> int | None:
        if symbol in self._seeded:
            return self._seeded[symbol]
        if self._ticker_map is None:
            self._ticker_map = self._client.ticker_map()
        return self._ticker_map.get(symbol.replace(".", "-"))  # SEC writes BRK-B

    def _fetch(self, symbol: str) -> Description | None:
        cik = self._cik(symbol)
        if cik is None:
            return None
        submissions = self._client.submissions(cik)
        if submissions is None:
            return None
        chosen = _latest_10k(submissions)
        if chosen is None:
            return None
        filed, accession, document = chosen
        age = (datetime.now(timezone.utc).date() - filed).days
        if age > MAX_FILING_AGE_DAYS or not _document_ok(document):
            return None
        url = ARCHIVE_URL.format(
            cik=cik, folder=accession.replace("-", ""), document=document
        )
        body = self._client.document(url)
        if body is None:
            return None
        text = item_1_text(_decode(body))
        if text is None:
            return None
        return Description(text=text, url=url)


def seeded_ciks(engine: Engine) -> dict[str, int]:
    """{symbol: CIK} for every security whose company profile has a CIK (ED-1).

    The same join as the fundamentals job's universe, not limited to a
    publication. Share classes on one profile share its CIK.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            select(securities.c.symbol, company_profiles.c.cik)
            .join(
                company_profiles,
                securities.c.company_profile_id == company_profiles.c.id,
            )
            .where(company_profiles.c.cik.is_not(None))
        ).tuples()
        return {symbol: int(cik) for symbol, cik in rows if str(cik).isdigit()}
