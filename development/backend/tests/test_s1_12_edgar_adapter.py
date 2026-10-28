"""S1-12 EDGAR adapter, ED-1, ED-2 and ED-4 to ED-7, ED-10, spec-first.

Spec: docs/work-items/s1-12-edgar-descriptions-packet.md @ 2d52f06 (decisions
89-91). The seams (proposed names from the packet):
- src.ingestion.sec_descriptions: SecTenKDescriptions(client, seeded_ciks) with
  source_name "SEC EDGAR" and fetch(symbol) -> Description | None, raising only
  DescriptionAdapterError; MAX_FILING_AGE_DAYS (E2 is open, so the tests read
  the constant, never 550); item_1_text (test_s1_12_edgar_extract.py);
  seeded_ciks(engine) -> {symbol: int(cik)}.
- EdgarClient (src/services/fundamentals/edgar.py) gains submissions(cik) and
  document(url), both through _get and its pacing.
The real EdgarClient and collectors._get_json run against a fake session (no
SEC call); retries are off. Dates are UTC: the filing-age limit counts days back
from today's UTC date.
"""

import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import pytest
import requests
from test_collectors import FakeResponse

UA = "Synthetic Research ops@perennial.test"
MAP_URL = "https://www.sec.gov/files/company_tickers.json"
ARCHIVE = "https://www.sec.gov/Archives/edgar/data/"
ACCESSION = "0000320193-25-000079"
DOCUMENT = "zzaa-20250927.htm"
PROSE = [
    "The firm makes tools.",
    "It sells them in shops.",
    "Its staff work in one town.",
    "The shops are near homes.",
]
TENK_HTML = (
    "<html><body><p>Item 1. Business</p>"
    + "".join(f"<p>{s}</p>" for s in PROSE)
    + "<p>Item 1A. Risk Factors</p><p>Risks are described here.</p></body></html>"
)


def sd():
    from src.ingestion import sec_descriptions

    return sec_descriptions


def today():
    return datetime.now(timezone.utc).date()


def ago(days):
    return (today() - timedelta(days=days)).isoformat()


def sub_url(cik):
    return f"https://data.sec.gov/submissions/CIK{cik:010d}.json"


def doc_url(cik, accession=ACCESSION, document=DOCUMENT):
    return f"{ARCHIVE}{cik}/{accession.replace('-', '')}/{document}"


def entry(form="10-K", filed=None, accession=ACCESSION, document=DOCUMENT):
    return {
        "form": form,
        "filingDate": filed if filed is not None else ago(30),
        "accessionNumber": accession,
        "primaryDocument": document,
    }


def submissions(*entries, name="SYNTHETIC CO"):
    keys = ("accessionNumber", "filingDate", "reportDate", "form", "primaryDocument")
    recent = {k: [] for k in keys}
    for e in entries:
        for k in keys:
            recent[k].append(e.get(k, e["filingDate"] if k == "reportDate" else None))
    return {"name": name, "filings": {"recent": recent, "files": []}}


class Clock:
    def __init__(self):
        self.now = 1000.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        assert seconds >= 0
        self.now += seconds


class FakeSec:
    """requests.Session.get over SEC's map, submissions and archive; records each request."""

    def __init__(self, clock=None):
        self.tickers = {}  # ticker -> cik
        self.subs = {}  # cik -> dict body or FakeResponse
        self.docs = {}  # url -> bytes or FakeResponse
        self.map_answer = None  # a FakeResponse overriding the map
        self.raise_on = {}  # url prefix -> exception instance
        self.requests = []
        self.clock = clock

    def get(self, url, params=None, headers=None, timeout=None, stream=False):
        assert stream and timeout
        self.requests.append(
            (url, params, dict(headers or {}), self.clock.now if self.clock else None)
        )
        for prefix, exc in self.raise_on.items():
            if url.startswith(prefix):
                raise exc
        if url == MAP_URL:
            if self.map_answer is not None:
                return self.map_answer
            body = {
                str(i): {"cik_str": cik, "ticker": t, "title": f"Synthetic {t}"}
                for i, (t, cik) in enumerate(self.tickers.items())
            }
            return FakeResponse(200, body)
        if url.startswith("https://data.sec.gov/submissions/CIK"):
            cik = int(url.rsplit("CIK", 1)[1].split(".")[0])
            value = self.subs.get(cik)
            if value is None:
                return FakeResponse(404, {"message": "not found"})
            return (
                value if isinstance(value, FakeResponse) else FakeResponse(200, value)
            )
        value = self.docs.get(url)
        if value is None:
            return FakeResponse(404, raw=b"<html>Not found</html>")
        return (
            value if isinstance(value, FakeResponse) else FakeResponse(200, raw=value)
        )

    def urls(self):
        return [r[0] for r in self.requests]


@pytest.fixture(autouse=True)
def no_retries(monkeypatch):
    from src.ingestion import collectors

    monkeypatch.setattr(collectors, "MAX_HTTP_RETRIES", 0)


def adapter(fake, seeded=None, clock=None):
    from src.services.fundamentals.edgar import EdgarClient

    clock = clock or fake.clock or Clock()
    client = EdgarClient(fake, UA, monotonic=clock.monotonic, sleep=clock.sleep)
    return sd().SecTenKDescriptions(client, dict(seeded or {}))


def company(fake, symbol, cik, *entries, html=TENK_HTML, mapped=True):
    """One company: in the map (unless mapped=False), its submissions, its 10-K."""
    if mapped:
        fake.tickers[symbol.replace(".", "-")] = cik
    fake.subs[cik] = submissions(*(entries or (entry(),)))
    for e in entries or (entry(),):
        if isinstance(e["accessionNumber"], str) and isinstance(
            e["primaryDocument"], str
        ):
            fake.docs[doc_url(cik, e["accessionNumber"], e["primaryDocument"])] = (
                html.encode("utf-8") if isinstance(html, str) else html
            )


def error_code(call):
    from src.ingestion.company_summaries import DescriptionAdapterError

    with pytest.raises(DescriptionAdapterError) as caught:
        call()
    return caught.value


# ── The adapter's shape ──────────────────────────────────


def test_source_name_is_sec_edgar():
    assert sd().SecTenKDescriptions.source_name == "SEC EDGAR"
    assert adapter(FakeSec()).source_name == "SEC EDGAR"


def test_the_age_limit_is_a_named_positive_constant():
    limit = sd().MAX_FILING_AGE_DAYS
    assert isinstance(limit, int) and not isinstance(limit, bool) and limit > 0


def test_the_new_failure_codes_are_in_the_safe_vocabulary():
    from src.ingestion.diagnostics import FAILURE_CODES, failure_code

    for code in (
        "submissions_malformed",
        "ticker_map_malformed",
        "request_failed",
        "unexpected_error",
    ):
        assert code in FAILURE_CODES
        assert failure_code(code) == code


# ── ED-1: the CIK ────────────────────────────────────────


def test_a_seeded_cik_wins_over_the_ticker_map():
    fake = FakeSec()
    company(fake, "ZZAA", 111, mapped=False)
    fake.tickers["ZZAA"] = 222
    fake.subs[222] = submissions(entry())
    description = adapter(fake, {"ZZAA": 111}).fetch("ZZAA")
    assert MAP_URL not in fake.urls()
    assert sub_url(111) in fake.urls() and sub_url(222) not in fake.urls()
    assert description.url == doc_url(111)


def test_without_a_seed_the_map_is_read_with_dashed_share_classes():
    fake = FakeSec()
    company(fake, "BRK.B", 333)
    assert "BRK-B" in fake.tickers
    description = adapter(fake).fetch("BRK.B")
    assert fake.urls()[:2] == [MAP_URL, sub_url(333)]
    assert description is not None and description.url == doc_url(333)


def test_the_map_is_fetched_at_most_once_per_adapter():
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    company(fake, "ZZAB", 112)
    descriptions = adapter(fake)
    descriptions.fetch("ZZAA")
    descriptions.fetch("ZZAB")
    assert fake.urls().count(MAP_URL) == 1


def test_no_map_request_when_every_symbol_is_seeded():
    fake = FakeSec()
    company(fake, "ZZAA", 111, mapped=False)
    company(fake, "ZZAB", 112, mapped=False)
    descriptions = adapter(fake, {"ZZAA": 111, "ZZAB": 112})
    assert descriptions.fetch("ZZAA") is not None
    assert descriptions.fetch("ZZAB") is not None
    assert MAP_URL not in fake.urls()


def test_no_cik_is_none_with_no_further_request():
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    descriptions = adapter(fake)
    assert descriptions.fetch("ZZNONE") is None
    assert fake.urls() == [MAP_URL]
    assert descriptions.fetch("ZZNONE2") is None
    assert fake.urls() == [MAP_URL]


# ── ED-2: which filing ───────────────────────────────────


def test_the_newest_10k_by_filing_date_is_chosen():
    fake = FakeSec()
    old = entry(filed=ago(400), accession="0000000111-24-000001", document="old.htm")
    new = entry(filed=ago(30), accession="0000000111-25-000001", document="new.htm")
    mid = entry(filed=ago(200), accession="0000000111-25-000000", document="mid.htm")
    company(fake, "ZZAA", 111, old, new, mid)
    description = adapter(fake, {"ZZAA": 111}).fetch("ZZAA")
    assert description.url == doc_url(111, "0000000111-25-000001", "new.htm")
    assert [u for u in fake.urls() if u.startswith(ARCHIVE)] == [description.url]


def test_a_filing_date_tie_goes_to_the_greatest_accession():
    fake = FakeSec()
    low = entry(filed=ago(30), accession="0000000111-25-000001", document="low.htm")
    high = entry(filed=ago(30), accession="0000000111-25-000009", document="high.htm")
    company(fake, "ZZAA", 111, high, low)
    assert adapter(fake, {"ZZAA": 111}).fetch("ZZAA").url == doc_url(
        111, "0000000111-25-000009", "high.htm"
    )


def test_a_newer_amendment_loses_to_the_10k():
    fake = FakeSec()
    tenk = entry(filed=ago(60), accession="0000000111-25-000001", document="tenk.htm")
    amend = entry(
        form="10-K/A",
        filed=ago(10),
        accession="0000000111-25-000002",
        document="amend.htm",
    )
    company(fake, "ZZAA", 111, tenk, amend)
    assert adapter(fake, {"ZZAA": 111}).fetch("ZZAA").url == doc_url(
        111, "0000000111-25-000001", "tenk.htm"
    )


@pytest.mark.parametrize(
    "form", ["20-F", "40-F", "10-KT", "10-K/A", "10-Q", "10-k", "10-K405"]
)
def test_no_exact_10k_is_none_without_a_document_request(form):
    fake = FakeSec()
    company(fake, "ZZAA", 111, entry(form=form))
    assert adapter(fake, {"ZZAA": 111}).fetch("ZZAA") is None
    assert fake.urls() == [sub_url(111)]


def test_no_filings_at_all_is_none():
    fake = FakeSec()
    fake.subs[111] = submissions()
    assert adapter(fake, {"ZZAA": 111}).fetch("ZZAA") is None


def test_a_submissions_404_is_none():
    fake = FakeSec()
    assert adapter(fake, {"ZZAA": 111}).fetch("ZZAA") is None
    assert fake.urls() == [sub_url(111)]


def test_the_age_limit_keeps_exactly_the_limit_and_drops_one_day_more():
    limit = sd().MAX_FILING_AGE_DAYS
    fake = FakeSec()
    company(fake, "ZZAA", 111, entry(filed=ago(limit)))
    assert adapter(fake, {"ZZAA": 111}).fetch("ZZAA") is not None
    fake = FakeSec()
    company(fake, "ZZAA", 111, entry(filed=ago(limit + 1)))
    assert adapter(fake, {"ZZAA": 111}).fetch("ZZAA") is None
    assert fake.urls() == [sub_url(111)]  # no document request


def test_the_age_limit_is_read_from_the_constant(monkeypatch):
    monkeypatch.setattr(sd(), "MAX_FILING_AGE_DAYS", 10)
    fake = FakeSec()
    company(fake, "ZZAA", 111, entry(filed=ago(11)))
    assert adapter(fake, {"ZZAA": 111}).fetch("ZZAA") is None


def test_only_filings_recent_is_read():
    fake = FakeSec()
    company(fake, "ZZAA", 111, entry(form="10-Q"))
    fake.subs[111]["filings"]["files"] = [
        {"name": "CIK0000000111-submissions-001.json"}
    ]
    assert adapter(fake, {"ZZAA": 111}).fetch("ZZAA") is None
    assert fake.urls() == [sub_url(111)]


def _malformed(body):
    def ragged(b):
        b["filings"]["recent"]["form"].append("10-K")
        return b

    cases = {
        "not_an_object": lambda b: [b],
        "no_filings": lambda b: {"name": "X"},
        "no_recent": lambda b: {"name": "X", "filings": {"files": []}},
        "recent_not_an_object": lambda b: {"name": "X", "filings": {"recent": []}},
        "ragged_arrays": ragged,
        "form_not_a_list": lambda b: {
            **b,
            "filings": {"recent": {**b["filings"]["recent"], "form": "10-K"}},
        },
        "missing_primary_document": lambda b: {
            **b,
            "filings": {
                "recent": {
                    k: v
                    for k, v in b["filings"]["recent"].items()
                    if k != "primaryDocument"
                }
            },
        },
    }
    return cases


@pytest.mark.parametrize(
    "case",
    [
        "not_an_object",
        "no_filings",
        "no_recent",
        "recent_not_an_object",
        "ragged_arrays",
        "form_not_a_list",
        "missing_primary_document",
    ],
)
def test_a_malformed_submissions_body_is_an_error(case):
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    fake.subs[111] = FakeResponse(200, _malformed(None)[case](fake.subs[111]))
    assert error_code(lambda: adapter(fake, {"ZZAA": 111}).fetch("ZZAA")).code == (
        "submissions_malformed"
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("filingDate", "2025/01/31"),
        ("filingDate", "2025-02-30"),
        ("filingDate", None),
        ("accessionNumber", "0000320193-25-79"),
        ("accessionNumber", "../../0000320193-25-000079"),
        ("accessionNumber", 320193),
        ("primaryDocument", 5),
    ],
)
def test_a_chosen_field_of_the_wrong_type_or_format_is_an_error(field, value):
    fake = FakeSec()
    company(fake, "ZZAA", 111, {**entry(), field: value})
    assert error_code(lambda: adapter(fake, {"ZZAA": 111}).fetch("ZZAA")).code == (
        "submissions_malformed"
    )


# ── ED-6: the link ───────────────────────────────────────


def test_the_link_is_the_primary_document_on_www_sec_gov():
    fake = FakeSec()
    company(
        fake,
        "ZZAA",
        320193,
        entry(accession="0000320193-25-000079", document="zzaa-20250927.htm"),
        mapped=False,
    )
    description = adapter(fake, {"ZZAA": 320193}).fetch("ZZAA")
    assert description.url == (
        "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/zzaa-20250927.htm"
    )
    from src.ingestion.company_summaries import _https

    assert _https(description.url)


@pytest.mark.parametrize(
    "document",
    [
        "../secret.htm",
        "a/b.htm",
        "zzaa.pdf",
        "zzaa.txt",
        "zzaa.htm?x=1",
        "zzaa.htm#top",
        "https://evil.example/zzaa.htm",
        ".hidden.htm",
        "-dash.htm",
        "zz aa.htm",
        "",
        "z" * 197 + ".htm",  # 201 characters
    ],
)
def test_a_primary_document_failing_the_filename_rule_is_none(document):
    fake = FakeSec()
    company(fake, "ZZAA", 111, entry(document=document))
    assert adapter(fake, {"ZZAA": 111}).fetch("ZZAA") is None
    assert fake.urls() == [sub_url(111)]


@pytest.mark.parametrize("document", ["z" * 196 + ".htm", "ZZ_a-1.2.html", "a.htm"])
def test_filenames_the_rule_allows(document):
    fake = FakeSec()
    company(fake, "ZZAA", 111, entry(document=document))
    assert adapter(fake, {"ZZAA": 111}).fetch("ZZAA").url == doc_url(
        111, document=document
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://data.sec.gov/Archives/edgar/data/111/000000011125000001/a.htm",
        "http://www.sec.gov/Archives/edgar/data/111/000000011125000001/a.htm",
        "https://www.sec.gov/files/company_tickers.json",
        "https://www.sec.gov.evil.example/Archives/edgar/data/111/a.htm",
        "https://evil.example/https://www.sec.gov/Archives/edgar/data/111/a.htm",
        "https://user@www.sec.gov/Archives/edgar/data/111/a.htm",
    ],
)
def test_document_refuses_anything_but_the_edgar_archive_without_a_request(url):
    from src.services.fundamentals.edgar import EdgarClient, EdgarError

    fake = FakeSec()
    clock = Clock()
    client = EdgarClient(fake, UA, monotonic=clock.monotonic, sleep=clock.sleep)
    with pytest.raises(EdgarError) as caught:
        client.document(url)
    assert caught.value.args == ("invalid_url",)
    assert fake.requests == []


def test_document_returns_the_raw_bytes():
    from src.services.fundamentals.edgar import EdgarClient

    fake = FakeSec()
    url = doc_url(111)
    fake.docs[url] = b"\x00raw bytes, not json"
    clock = Clock()
    client = EdgarClient(fake, UA, monotonic=clock.monotonic, sleep=clock.sleep)
    assert client.document(url) == b"\x00raw bytes, not json"
    assert client.document(doc_url(111, document="missing.htm")) is None  # 404


def test_document_over_the_body_cap_is_none():
    from src.ingestion import collectors
    from src.services.fundamentals.edgar import EdgarClient

    fake = FakeSec()
    url = doc_url(111)
    fake.docs[url] = FakeResponse(
        200,
        raw=b"<html></html>",
        headers={"Content-Length": str(collectors.MAX_RESPONSE_BYTES + 1)},
    )
    clock = Clock()
    assert (
        EdgarClient(fake, UA, monotonic=clock.monotonic, sleep=clock.sleep).document(
            url
        )
        is None
    )


def test_submissions_reads_the_padded_cik_url():
    from src.services.fundamentals.edgar import EdgarClient, EdgarError

    fake = FakeSec()
    fake.subs[320193] = {"name": "SYNTHETIC", "filings": {"recent": {}}}
    clock = Clock()
    client = EdgarClient(fake, UA, monotonic=clock.monotonic, sleep=clock.sleep)
    assert client.submissions(320193) == fake.subs[320193]
    assert fake.urls() == ["https://data.sec.gov/submissions/CIK0000320193.json"]
    assert client.submissions(999) is None  # 404
    fake.subs[5] = FakeResponse(200, ["not", "an", "object"])
    with pytest.raises(EdgarError) as caught:
        client.submissions(5)
    assert caught.value.args == ("submissions_malformed",)


# ── ED-4: errors versus None ─────────────────────────────


@pytest.mark.parametrize("status", [403, 429, 500, 503])
@pytest.mark.parametrize("endpoint", ["map", "submissions", "document"])
def test_transport_failures_are_errors_with_their_code(endpoint, status):
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    failing = FakeResponse(status, {"message": "synthetic SEC body"})
    if endpoint == "map":
        fake.map_answer = failing
    elif endpoint == "submissions":
        fake.subs[111] = failing
    else:
        fake.docs[doc_url(111)] = failing
    err = error_code(lambda: adapter(fake).fetch("ZZAA"))
    assert err.code == f"http_{status}"


@pytest.mark.parametrize(
    "exc,code",
    [
        (requests.Timeout("synthetic"), "timeout"),
        (requests.ConnectionError("synthetic"), "request_failed"),
        (requests.exceptions.SSLError("synthetic"), "request_failed"),
        (requests.exceptions.InvalidURL("synthetic"), "request_failed"),
    ],
)
@pytest.mark.parametrize(
    "prefix",
    [MAP_URL, "https://data.sec.gov/submissions/", ARCHIVE],
    ids=["map", "submissions", "document"],
)
def test_request_exceptions_are_fixed_codes(prefix, exc, code):
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    fake.raise_on[prefix] = exc
    assert error_code(lambda: adapter(fake).fetch("ZZAA")).code == code


def test_invalid_json_on_submissions_is_an_error():
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    fake.subs[111] = FakeResponse(200, raw=b"{not json")
    assert (
        error_code(lambda: adapter(fake, {"ZZAA": 111}).fetch("ZZAA")).code
        == "invalid_json"
    )


@pytest.mark.parametrize("body", [["a", "list"], "text", 7])
def test_a_malformed_ticker_map_is_an_error(body):
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    fake.map_answer = FakeResponse(200, body)
    assert (
        error_code(lambda: adapter(fake).fetch("ZZAA")).code == "ticker_map_malformed"
    )


@pytest.mark.parametrize(
    "answer",
    [
        FakeResponse(404, raw=b"gone"),
        FakeResponse(
            200, raw=b"<html></html>", headers={"Content-Length": str(10**12)}
        ),
    ],
    ids=["document_404", "document_too_large"],
)
def test_a_permanent_document_condition_is_none_not_an_error(answer):
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    fake.docs[doc_url(111)] = answer
    assert adapter(fake, {"ZZAA": 111}).fetch("ZZAA") is None


@pytest.mark.parametrize(
    "html",
    [
        "<html><body><p>Form 10-K Cross-Reference Index</p><p>See pages 4-20.</p></body></html>",
        "<html><body><p>Item 1. Business</p><p>Overview</p><p>Item 1A. Risk Factors</p></body></html>",
        "",
    ],
    ids=["no_item_1", "no_prose", "empty"],
)
def test_a_document_without_item_1_prose_is_none(html):
    fake = FakeSec()
    company(fake, "ZZAA", 111, html=html)
    assert adapter(fake, {"ZZAA": 111}).fetch("ZZAA") is None


def test_any_other_exception_is_unexpected_error_with_nothing_chained():
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    fake.raise_on["https://data.sec.gov/submissions/"] = KeyError(
        "synthetic secret detail"
    )
    err = error_code(lambda: adapter(fake, {"ZZAA": 111}).fetch("ZZAA"))
    assert err.code == "unexpected_error"
    assert err.__cause__ is None and err.__context__ is None
    assert "synthetic secret detail" not in repr(err.args)


def test_every_error_leaves_as_a_description_adapter_error_without_context():
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    fake.subs[111] = FakeResponse(503, {"message": "synthetic SEC body"})
    err = error_code(lambda: adapter(fake, {"ZZAA": 111}).fetch("ZZAA"))
    assert err.__cause__ is None and err.__context__ is None
    assert UA not in str(err.args) and "synthetic SEC body" not in str(err.args)


# ── ED-3 step 7 and ED-5: decoding, and the description ──


def test_the_description_is_item_1_text_of_the_document():
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    description = adapter(fake, {"ZZAA": 111}).fetch("ZZAA")
    assert description.text == sd().item_1_text(TENK_HTML)
    assert description.text == "\n\n".join(PROSE)


def test_utf8_with_a_bom_decodes():
    fake = FakeSec()
    company(
        fake,
        "ZZAA",
        111,
        html=b"\xef\xbb\xbf"
        + TENK_HTML.replace("tools.", "tools’ parts.").encode("utf-8"),
    )
    text = adapter(fake, {"ZZAA": 111}).fetch("ZZAA").text
    assert text.startswith("The firm makes tools’ parts.")
    assert "﻿" not in text


def test_cp1252_bytes_decode_when_utf8_fails():
    fake = FakeSec()
    html = TENK_HTML.replace("tools.", "tools’ parts.").encode("cp1252")
    assert b"\x92" in html
    company(fake, "ZZAA", 111, html=html)
    assert (
        adapter(fake, {"ZZAA": 111})
        .fetch("ZZAA")
        .text.startswith("The firm makes tools’ parts.")
    )


def test_undecodable_bytes_drop_only_their_block():
    fake = FakeSec()
    # 0x81 is undefined in cp1252, so it decodes to U+FFFD, and its block drops.
    html = TENK_HTML.replace("It sells them in shops.", "It sells \x81 them.").encode(
        "latin-1"
    )
    company(fake, "ZZAA", 111, html=html)
    text = adapter(fake, {"ZZAA": 111}).fetch("ZZAA").text
    assert text == "\n\n".join([PROSE[0], *PROSE[2:]])


def test_the_jobs_selection_takes_a_verbatim_window_of_it():
    from src.ingestion.company_summaries import select_excerpt

    fake = FakeSec()
    company(fake, "ZZAA", 111)
    description = adapter(fake, {"ZZAA": 111}).fetch("ZZAA")
    excerpt = select_excerpt(description.text)
    assert excerpt is not None and excerpt.text == " ".join(PROSE)


# ── ED-7 and ED-10: requests, pacing, headers, hosts, logs ──


def test_request_count_pacing_headers_and_hosts(caplog):
    from src.services.fundamentals.edgar import MIN_REQUEST_INTERVAL_SECONDS

    clock = Clock()
    fake = FakeSec(clock)
    company(fake, "ZZAA", 111)
    company(fake, "ZZAB", 112)
    company(fake, "ZZAC", 113, entry(form="20-F"))
    caplog.set_level(logging.DEBUG)
    descriptions = adapter(fake, clock=clock)
    for symbol in ("ZZAA", "ZZAB", "ZZAC"):
        descriptions.fetch(symbol)
    # 1 map + 3 submissions + 2 documents (ZZAC has no 10-K)
    assert len(fake.requests) == 6
    times = [t for *_, t in fake.requests]
    assert all(
        b - a >= MIN_REQUEST_INTERVAL_SECONDS - 1e-9 for a, b in zip(times, times[1:])
    )
    for url, params, headers, _ in fake.requests:
        assert headers == {"User-Agent": UA}
        assert not params
        parts = urlsplit(url)
        assert parts.scheme == "https" and parts.hostname in {
            "www.sec.gov",
            "data.sec.gov",
        }
        assert not parts.query and not parts.username
    logged = "".join(r.getMessage() for r in caplog.records)
    for secret_or_text in (UA, *PROSE, ARCHIVE, "SYNTHETIC CO"):
        assert secret_or_text not in logged


def test_errors_log_no_filing_text_or_contact(caplog):
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    fake.docs[doc_url(111)] = FakeResponse(503, {"message": "synthetic SEC body"})
    caplog.set_level(logging.DEBUG)
    error_code(lambda: adapter(fake, {"ZZAA": 111}).fetch("ZZAA"))
    logged = "".join(r.getMessage() for r in caplog.records)
    assert (
        UA not in logged
        and "synthetic SEC body" not in logged
        and ARCHIVE not in logged
    )


# ── With the database: seeded CIKs and the job ───────────


@pytest.mark.db
def test_seeded_ciks_maps_every_security_whose_profile_has_a_cik(engine):
    from sqlalchemy import insert, select
    from src.db import models
    from test_read_api import seed_security

    seed_security(engine, "ZZGA", profile={"cik": "0000320193", "sector": "Technology"})
    with engine.begin() as conn:
        profile = conn.execute(
            select(models.securities.c.company_profile_id).where(
                models.securities.c.symbol == "ZZGA"
            )
        ).scalar_one()
        conn.execute(
            insert(models.securities).values(symbol="ZZGB", company_profile_id=profile)
        )
    seed_security(engine, "ZZNC", profile={"sector": "Energy"})  # profile without a CIK
    seed_security(engine, "ZZNP")  # no profile
    assert dict(sd().seeded_ciks(engine)) == {"ZZGA": 320193, "ZZGB": 320193}


def _members(engine, *symbols, cik=None):
    from test_read_api import seed_security

    for i, symbol in enumerate(symbols):
        profile = {"sector": "Industrials"}
        if cik is not None:
            profile["cik"] = f"{cik + i:010d}"
        seed_security(engine, symbol, name=f"Synthetic {symbol}", profile=profile)
    return list(symbols)


def _stored(engine, symbol):
    from sqlalchemy import text

    with engine.connect() as conn:
        return (
            conn.execute(
                text(
                    "SELECT p.summary, p.summary_status, p.summary_source_name,"
                    " p.summary_source_url, p.description_digest, p.xmin::text AS xmin"
                    " FROM perennial.company_profiles p JOIN perennial.securities s"
                    " ON s.company_profile_id = p.id WHERE s.symbol = :s"
                ),
                {"s": symbol},
            )
            .mappings()
            .one()
        )


@pytest.mark.db
def test_the_job_stores_a_verbatim_excerpt_credited_to_sec_edgar(engine):
    from src.ingestion.company_summaries import summarize

    symbols = _members(engine, "ZZAA", "ZZAB")
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    company(fake, "ZZAB", 112, entry(form="20-F"))
    result = summarize(engine, adapter(fake), symbols=symbols)
    assert result.succeeded
    assert (result.with_summary, result.unavailable) == (1, 1)
    row = _stored(engine, "ZZAA")
    assert row["summary_status"] == "ok"
    assert row["summary"] == " ".join(PROSE)
    assert (row["summary_source_name"], row["summary_source_url"]) == (
        "SEC EDGAR",
        doc_url(111),
    )
    assert _stored(engine, "ZZAB")["summary_status"] == "unavailable"


@pytest.mark.db
def test_share_classes_on_one_profile_cost_one_submissions_request(engine):
    from sqlalchemy import insert, select
    from src.db import models
    from src.ingestion.company_summaries import summarize

    _members(engine, "ZZGA")
    with engine.begin() as conn:
        profile = conn.execute(
            select(models.securities.c.company_profile_id).where(
                models.securities.c.symbol == "ZZGA"
            )
        ).scalar_one()
        conn.execute(
            insert(models.securities).values(symbol="ZZGB", company_profile_id=profile)
        )
    fake = FakeSec()
    company(fake, "ZZGA", 111)
    fake.tickers["ZZGB"] = 111
    assert summarize(engine, adapter(fake), symbols=["ZZGA", "ZZGB"]).succeeded
    assert fake.urls().count(sub_url(111)) == 1


@pytest.mark.db
def test_a_stored_fmp_row_is_re_decided_by_the_first_edgar_run(engine):
    """No SELECTOR_VERSION bump: the source name and URL already change the digest."""
    from src.ingestion.company_summaries import SELECTOR_VERSION, Description, summarize

    assert SELECTOR_VERSION == "s1-12-v2"

    class FmpLike:
        source_name = "Financial Modeling Prep"

        def fetch(self, symbol):
            return Description(
                text=" ".join(PROSE), url="https://financialmodelingprep.com/"
            )

    symbols = _members(engine, "ZZFM")
    assert summarize(engine, FmpLike(), symbols=symbols).succeeded
    before = _stored(engine, "ZZFM")
    assert before["summary_source_name"] == "Financial Modeling Prep"
    fake = FakeSec()
    company(fake, "ZZFM", 111)
    assert summarize(engine, adapter(fake), symbols=symbols).succeeded
    after = _stored(engine, "ZZFM")
    assert after["xmin"] != before["xmin"]
    assert after["description_digest"] != before["description_digest"]
    assert (after["summary_source_name"], after["summary_source_url"]) == (
        "SEC EDGAR",
        doc_url(111),
    )


@pytest.mark.db
def test_an_sec_error_fails_the_run_and_writes_nothing(engine):
    from src.ingestion.company_summaries import summarize

    symbols = _members(engine, "ZZAA", "ZZAB")
    fake = FakeSec()
    company(fake, "ZZAA", 111)
    company(fake, "ZZAB", 112)
    assert summarize(engine, adapter(fake), symbols=symbols).succeeded
    before = {s: _stored(engine, s) for s in symbols}
    fake.subs[112] = FakeResponse(403, {"message": "blocked"})
    fake.docs[doc_url(111)] = (TENK_HTML.replace("tools.", "rakes.")).encode()
    result = summarize(engine, adapter(fake), symbols=symbols)
    assert not result.succeeded
    assert {s: _stored(engine, s) for s in symbols} == before
