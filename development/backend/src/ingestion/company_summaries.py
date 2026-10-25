"""
Company summaries at ingestion (S1-12; ADR 0005: made here, never per request,
with no model of any kind).

For each consensus-list member (Q11) the job fetches a description through a
provider-neutral DescriptionAdapter and stores, on the member's company
profile, the first window of 2–4 consecutive sentences that reads at
Flesch–Kincaid grade 8 or below (decision 72: verbatim sentence selection,
falling back to "Summary not available"). Only the excerpt is stored, with the
source's name and https link; the description itself never is.

Policy:
  * Sentences come from split_sentences, our fixed splitter (textstat's splits
    "Inc." and "U.S."). Windows are tried in document order: at each start, 4,
    then 3, then 2 sentences. A window is stored verbatim, its sentences joined
    with single spaces (runs of whitespace inside a sentence become one space;
    no word changes). A window with a control or format character, or longer
    than the summary column allows, never passes.
  * The run is all-or-nothing: every description is fetched before anything is
    written, and a DescriptionAdapterError fails the run with no row touched
    (stale-good). Other exceptions propagate, also before any write.
  * No description, an empty one, no passing window, an oversized description
    or a link that isn't https is a per-security "unavailable" outcome. The run
    still succeeds. A member without a company profile has nowhere to store a
    summary: it isn't fetched and counts as unavailable.
  * Skip key: description_digest is the SHA-256 of the description as fetched
    together with its link, the source's name and SELECTOR_VERSION. A profile
    whose digest is unchanged and that already has an outcome isn't rewritten,
    so bumping SELECTOR_VERSION reprocesses every row (checker N1). A changed
    description is processed again; when it has no passing window the stored
    summary is cleared, because it no longer matches its source.
  * Each run logs one coverage line (CS-7): counts only, never company text or
    links.
"""

import hashlib
import json
import logging
import re
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

import textstat
from sqlalchemy import and_, select, update
from sqlalchemy.engine import Engine

from src.db.models import (
    company_profiles,
    current_publication,
    publications,
    securities,
)
from src.ingestion.diagnostics import failure_code

logger = logging.getLogger(__name__)

# Part of every description_digest: change it whenever selection changes.
SELECTOR_VERSION = "s1-12-v1"
MAX_GRADE = 8.0
WINDOW_SIZES = (4, 3, 2)
MAX_SUMMARY_CHARS = 4000  # ck_company_profiles_summary_not_blank
MAX_SOURCE_NAME_CHARS = 128  # company_profiles.summary_source_name
MAX_DESCRIPTION_CHARS = 100_000  # bounds the grading work per description
MAX_URL_CHARS = 2048
# The same rule as ck_company_profiles_summary_source_url_https.
_HTTPS_URL = re.compile(r"https://[A-Za-z0-9.-]+(:[0-9]{1,5})?([/?#][^\s]*)?")
CHANNEL = "production"
BUCKETS = ("popular_stable", "affordable_growing", "unresolved")

# ── sentences and windows ────────────────────────────────

# . ! or ? (and any closing quotes or bracket), whitespace, then a capital, a
# digit or an opening quote: a sentence boundary unless an abbreviation ends there.
_BOUNDARY = re.compile(r"[.!?][\"'”’)]*(?=\s+[A-Z0-9\"“'‘])")
_OPENERS = "\"'“‘("
# Dotted initials and acronyms: "U.S.", "e.g.", "J.".
_INITIALS = re.compile(r"(?:[A-Za-z]\.)+")
ABBREVIATIONS = frozenset(
    {
        "Inc.", "Corp.", "Co.", "Ltd.", "Bros.", "Mr.", "Mrs.", "Ms.", "Dr.",
        "Prof.", "St.", "Jr.", "Sr.", "Mt.", "Ft.", "vs.", "approx.", "Dept.",
        "Jan.", "Feb.", "Mar.", "Apr.", "Aug.", "Sept.", "Oct.", "Nov.", "Dec.",
    }
)  # fmt: skip


def _abbreviation(text: str, period: int) -> bool:
    """Whether the word ending at text[period] (a '.') is an abbreviation."""
    start = period
    while start > 0 and not text[start - 1].isspace():
        start -= 1
    word = text[start : period + 1].lstrip(_OPENERS)
    return word in ABBREVIATIONS or _INITIALS.fullmatch(word) is not None


def split_sentences(text: str) -> list[str]:
    """Our fixed splitter (CS-2, N2); each sentence is stripped, none is empty."""
    sentences, start = [], 0
    for boundary in _BOUNDARY.finditer(text):
        if text[boundary.start()] == "." and _abbreviation(text, boundary.start()):
            continue
        sentence = text[start : boundary.end()].strip()
        if sentence:
            sentences.append(sentence)
        start = boundary.end()
    rest = text[start:].strip()
    if rest:
        sentences.append(rest)
    return sentences


@dataclass(frozen=True)
class Excerpt:
    text: str
    grade: float  # textstat.flesch_kincaid_grade(text); stored rounded to cents


def _printable(text: str) -> bool:
    return not any(unicodedata.category(char).startswith("C") for char in text)


def select_excerpt(description: str | None) -> Excerpt | None:
    """The first passing window of 2–4 consecutive sentences, verbatim; else None."""
    if not isinstance(description, str) or len(description) > MAX_DESCRIPTION_CHARS:
        return None
    sentences = [" ".join(s.split()) for s in split_sentences(description)]
    for start in range(len(sentences)):
        for size in WINDOW_SIZES:
            window = sentences[start : start + size]
            if len(window) < size:
                continue
            text = " ".join(window)
            if len(text) > MAX_SUMMARY_CHARS or not _printable(text):
                continue
            grade = float(textstat.flesch_kincaid_grade(text))
            if grade <= MAX_GRADE:
                return Excerpt(text=text, grade=grade)
    return None


# ── the adapter seam ─────────────────────────────────────


@dataclass(frozen=True)
class Description:
    """A company description as a source states it, and the https link to it."""

    text: str
    url: str


class DescriptionAdapterError(Exception):
    """A description source's failure. Only the code is kept; messages never are."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class DescriptionAdapter(Protocol):
    @property
    def source_name(self) -> str:
        """Shown with every summary as "Source: <name>"."""
        ...

    def fetch(self, symbol: str) -> Description | None:
        """The description for a canonical symbol, or None when there is none.

        Raises DescriptionAdapterError on any failure.
        """
        ...


@dataclass(frozen=True)
class SummaryResult:
    """One run. On failure the counts are zero and nothing was written."""

    succeeded: bool
    error_code: str | None
    members: int
    with_summary: int
    unavailable: int


def consensus_members(engine: Engine) -> list[str]:
    """The current production publication's entry tickers, sorted and unique (Q11)."""
    with engine.connect() as conn:
        output = conn.execute(
            select(publications.c.output)
            .join(
                current_publication,
                current_publication.c.publication_id == publications.c.id,
            )
            .where(current_publication.c.channel == CHANNEL)
        ).scalar_one_or_none()
    if not isinstance(output, dict):
        return []
    tickers = {
        record["ticker"]
        for bucket in BUCKETS
        for record in output.get(bucket) or ()
        if isinstance(record, dict)
        and isinstance(record.get("ticker"), str)
        and record["ticker"]
    }
    return sorted(tickers)


# ── the job ──────────────────────────────────────────────


def _source_name(adapter: DescriptionAdapter) -> str | None:
    name = getattr(adapter, "source_name", None)
    if not isinstance(name, str) or name != name.strip():
        return None
    if not 0 < len(name) <= MAX_SOURCE_NAME_CHARS or not _printable(name):
        return None
    return name


def _https(url: str) -> bool:
    return len(url) <= MAX_URL_CHARS and _HTTPS_URL.fullmatch(url) is not None


def _digest(source_name: str, description: Description | None) -> str:
    key = {
        "selector_version": SELECTOR_VERSION,
        "source_name": source_name,
        "url": description.url if description else None,
        "text": description.text if description else None,
    }
    encoded = json.dumps(key, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8", "surrogatepass")).hexdigest()


def _outcome(
    source_name: str, description: Description | None, now: datetime
) -> dict[str, Any]:
    """The company_profiles values for one fetched description."""
    excerpt = None
    if description is not None and _https(description.url):
        excerpt = select_excerpt(description.text)
    if excerpt is None:
        return {
            "summary": None,
            "summary_grade": None,
            "summary_status": "unavailable",
            "summary_source_name": None,
            "summary_source_url": None,
            "summary_as_of": now,
        }
    assert description is not None
    return {
        "summary": excerpt.text,
        "summary_grade": round(excerpt.grade, 2),
        "summary_status": "ok",
        "summary_source_name": source_name,
        "summary_source_url": description.url,
        "summary_as_of": now,
    }


def _failed(code: str) -> SummaryResult:
    return SummaryResult(False, code, 0, 0, 0)


def summarize(
    engine: Engine,
    adapter: DescriptionAdapter,
    *,
    symbols: Sequence[str] | None = None,
) -> SummaryResult:
    """Fetch, select and store summaries for `symbols` (default: the consensus list)."""
    source_name = _source_name(adapter)
    if source_name is None:
        logger.warning("company summaries failed: invalid source name; nothing written")
        return _failed("invalid_source_name")
    members = sorted(set(consensus_members(engine) if symbols is None else symbols))
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                securities.c.symbol,
                company_profiles.c.id,
                company_profiles.c.description_digest,
                company_profiles.c.summary_status,
            )
            .join(
                company_profiles,
                company_profiles.c.id == securities.c.company_profile_id,
            )
            .where(securities.c.symbol.in_(members))
        ).all()
    profile_of = {row.symbol: row.id for row in rows}
    stored = {row.id: (row.description_digest, row.summary_status) for row in rows}

    # Fetch everything first: a failure must leave every row as it was.
    fetched: dict[int, Description | None] = {}
    for symbol in members:
        profile = profile_of.get(symbol)
        if profile is None or profile in fetched:  # share classes share a profile
            continue
        try:
            description = adapter.fetch(symbol)
        except DescriptionAdapterError as exc:
            code = failure_code(exc.code)
            logger.warning(
                "company summaries failed symbol=%s code=%s; nothing written",
                symbol,
                code,
            )
            return _failed("collection_failed")
        if description is not None and not (
            isinstance(description, Description)
            and isinstance(description.text, str)
            and isinstance(description.url, str)
        ):
            logger.warning(
                "company summaries failed symbol=%s code=malformed_response;"
                " nothing written",
                symbol,
            )
            return _failed("collection_failed")
        fetched[profile] = description

    now = datetime.now(timezone.utc)
    rewritten = 0
    with engine.begin() as conn:
        for profile, description in fetched.items():
            digest = _digest(source_name, description)
            old_digest, old_status = stored[profile]
            if digest == old_digest and old_status is not None:
                continue  # unchanged and already decided: no work, no write
            values = _outcome(source_name, description, now)
            # Compare-and-set: a concurrent run that already stored a newer
            # outcome for this profile wins.
            result = conn.execute(
                update(company_profiles)
                .where(
                    and_(
                        company_profiles.c.id == profile,
                        company_profiles.c.description_digest.is_not_distinct_from(
                            old_digest
                        ),
                        company_profiles.c.summary_status.is_not_distinct_from(
                            old_status
                        ),
                    )
                )
                .values(**values, description_digest=digest)
            )
            rewritten += result.rowcount
        statuses: dict[int, str | None] = {
            row.id: row.summary_status
            for row in conn.execute(
                select(company_profiles.c.id, company_profiles.c.summary_status).where(
                    company_profiles.c.id.in_(set(profile_of.values()))
                )
            )
        }

    with_summary = sum(
        1
        for symbol in members
        if symbol in profile_of and statuses.get(profile_of[symbol]) == "ok"
    )
    result_counts = SummaryResult(
        True, None, len(members), with_summary, len(members) - with_summary
    )
    logger.info(
        "company summaries members=%s with_summary=%s unavailable=%s"
        " no_profile=%s fetched=%s rewritten=%s selector=%s",
        result_counts.members,
        result_counts.with_summary,
        result_counts.unavailable,
        len(members) - len(profile_of),
        len(fetched),
        rewritten,
        SELECTOR_VERSION,
    )
    return result_counts
