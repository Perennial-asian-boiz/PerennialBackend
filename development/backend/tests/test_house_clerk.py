"""House Clerk PTR fetcher: index, both table layouts, honest coverage, cache, fmp.py merge."""

import io
import json
import zipfile
from datetime import date, timedelta

import pytest

from src.ingestion import collectors

NO_SLEEP = lambda seconds: None  # noqa: E731
LABEL_NEW = "F\x00\x00\x00\x00\x00 S\x00\x00\x00\x00\x00: New"
LABEL_SUB = "S\x00\x00\x00\x00\x00\x00\x00\x00\x00 O\x00: Synthetic IRA"
HEADER = "ID Owner Asset Transaction\nType\nDate Notification\nDate\nAmount Cap.\nGains >\n$200?"
FOOTER = "* For the complete list of asset type abbreviations, please visit https://fd.house.gov/."


@pytest.fixture
def hc():
    return collectors._fetcher("house_clerk")


def filing(doc_id="20000001", filed="2026-09-01"):
    return {"doc_id": doc_id, "year": filed[:4], "name": "Synthetic Member",
            "district": "ZZ01", "filing_date": filed}


def ptr_text(*rows):
    """A PTR's extracted text: header, rows each followed by sub-row labels, footer."""
    body = "\n".join(f"{row}\n{LABEL_NEW}\n{LABEL_SUB}" for row in rows)
    return (f"P T R\nName: Hon. Synthetic Member\n{HEADER}\n{body}\n"
            f"Filing ID #20000001\n{FOOTER}\nI CERTIFY ...")


def index_zip(rows):
    xml = "<FinancialDisclosure>" + "".join(
        "<Member>" + "".join(f"<{k}>{v}</{k}>" for k, v in r.items()) + "</Member>"
        for r in rows
    ) + "</FinancialDisclosure>"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("2026FD.xml", xml)
        z.writestr("2026FD.txt", "ignored")
    return buf.getvalue()


def index_row(doc_id, filed="09/01/2026", kind="P", last="Member"):
    return {"Prefix": "Hon.", "Last": last, "First": "Synthetic", "Suffix": "",
            "FilingType": kind, "StateDst": "ZZ01", "Year": "2026",
            "FilingDate": filed, "DocID": doc_id}


# ── Index ────────────────────────────────────────────────


def test_index_keeps_ptrs_only(hc):
    rows = [index_row("20000001"), index_row("20000002", kind="C"),
            index_row("8000003", filed="08/15/2026", last="Paper")]
    filings = hc.parse_index(index_zip(rows))
    assert [f["doc_id"] for f in filings] == ["20000001", "8000003"]
    assert filings[0] == {"doc_id": "20000001", "year": "2026", "name": "Synthetic Member",
                          "district": "ZZ01", "filing_date": "2026-09-01"}


@pytest.mark.parametrize("blob", [b"not a zip", index_zip([index_row("x1")]),
                                  index_zip([index_row("20000001", filed="2026-09-01")])])
def test_unusable_index_raises(hc, blob):
    with pytest.raises(ValueError):
        hc.parse_index(blob)


# ── Parsing ──────────────────────────────────────────────


def test_row_with_ticker_before_deal(hc):
    text = ptr_text("SP Berkshire Hathaway Inc. New\nCommon Stock (BRK.B) [ST]\n"
                    "P 08/07/202609/01/2026$1,001 - $15,000")
    trades, stats = hc.parse_ptr_text(text, filing())
    assert stats == {"stock_rows": 1, "parsed_rows": 1, "skipped_rows": 0}
    t = trades[0]
    assert t["ticker"] == "BRK.B" and t["trade_type"] == "Purchase"
    assert t["asset_description"] == "Berkshire Hathaway Inc. New Common Stock"
    assert t["amount_range"] == "$1,001 - $15,000"
    assert (t["transaction_date"], t["disclosure_date"], t["days_to_disclose"]) == (
        "2026-08-07", "2026-09-01", 25)
    assert t["chamber"] == "House" and t["asset_type"] == "Stock"
    assert t["source_link"].endswith("/ptr-pdfs/2026/20000001.pdf")


def test_row_with_deal_before_wrapped_asset_name(hc):
    # When the asset name wraps, the deal columns align with its first line.
    text = ptr_text("UnitedHealth Group Incorporated P 03/19/2026 04/07/2026 $1,001 - $15,000\n"
                    "Common Stock (UNH) [ST]")
    trades, stats = hc.parse_ptr_text(text, filing())
    assert stats["parsed_rows"] == 1
    assert trades[0]["ticker"] == "UNH"
    assert trades[0]["asset_description"] == "UnitedHealth Group Incorporated Common Stock"


def test_wrapped_amount_and_trade_types(hc):
    text = ptr_text(
        "Chubb Limited Common Stock (CB)\n[ST]\nS 08/19/202609/01/2026$15,001 -\n$50,000",
        "IBM Corp Common Stock (IBM) [ST]\nS (partial) 08/24/2026 09/01/2026 $1,001 - $15,000",
        "Merck Common Stock (MRK) [ST]\nE 08/19/2026 09/01/2026 Over $50,000,000",
    )
    trades, _ = hc.parse_ptr_text(text, filing())
    assert [(t["ticker"], t["trade_type"], t["amount_range"]) for t in trades] == [
        ("CB", "Sale", "$15,001 - $50,000"),
        ("IBM", "Sale (Partial)", "$1,001 - $15,000"),
        ("MRK", "Exchange", "Over $50,000,000"),
    ]


def test_non_stock_rows_are_skipped_not_counted_as_missing(hc):
    text = ptr_text(
        "US Treasury Note 06/30/27 [GS] P 07/01/2026 07/13/2026 $15,001 - $50,000",
        "Synthetic Growth Fund (ZZFDX) [MF]\nP 07/01/2026 07/13/2026 $1,001 - $15,000",
        "Synthetic Corp Common Stock (ZZAA) [ST]\nP 07/01/2026 07/13/2026 $1,001 - $15,000",
    )
    trades, stats = hc.parse_ptr_text(text, filing())
    assert [t["ticker"] for t in trades] == ["ZZAA"]
    assert stats == {"stock_rows": 1, "parsed_rows": 1, "skipped_rows": 0}


def test_deal_between_ticker_and_wrapped_code(hc):
    # Only "[ST]" wrapped; the deal columns sit between the ticker and the code.
    text = ptr_text(
        "BlackRock, Inc. Common Stock (BLK) P 11/05/2025 12/09/2025 $1,001 - $15,000\n[ST]",
        "Franco-Nevada Corporation (FNV) P 01/27/2026 02/11/2026 $15,001 -\n[ST] $50,000",
    )
    trades, stats = hc.parse_ptr_text(text, filing())
    assert stats == {"stock_rows": 2, "parsed_rows": 2, "skipped_rows": 0}
    assert [(t["ticker"], t["amount_range"]) for t in trades] == [
        ("BLK", "$1,001 - $15,000"), ("FNV", "$15,001 - $50,000")]
    assert trades[0]["asset_description"] == "BlackRock, Inc. Common Stock"


def test_preferred_share_tickers_are_skipped_and_counted(hc, monkeypatch):
    # "T$A" cannot pass the database ticker schema; it is accounted for, not lost.
    text = ptr_text(
        "AT&T Inc. 5.000% Perpetual Preferred\nStock, Series A (T$A) [ST]\n"
        "S 05/11/2026 06/17/2026 $15,001 -\n$50,000",
        "Synthetic Corp Common Stock (ZZAA) [ST]\nP 05/11/2026 06/17/2026 $1,001 - $15,000",
    )
    trades, stats = hc.parse_ptr_text(text, filing())
    assert [t["ticker"] for t in trades] == ["ZZAA"]
    assert stats == {"stock_rows": 2, "parsed_rows": 1, "skipped_rows": 1}
    monkeypatch.setattr(hc, "extract_text", lambda pdf: text)
    assert hc.parse_report(b"%PDF", filing())["status"] == hc.STATUS_PARSED


def test_unparseable_stock_row_is_counted(hc, monkeypatch):
    text = ptr_text("Synthetic Corp Common Stock [ST]\nP 07/01/2026 07/13/2026 $1,001 - $15,000",
                    "Other Corp Common Stock (ZZBB) [ST]\nP 07/01/2026 07/13/2026 $1,001 - $15,000")
    monkeypatch.setattr(hc, "extract_text", lambda pdf: text)
    report = hc.parse_report(b"%PDF", filing())
    assert report["status"] == hc.STATUS_PARTIAL
    assert (report["stock_rows"], report["parsed_rows"]) == (2, 1)


def test_scanned_and_unreadable_reports(hc, monkeypatch):
    monkeypatch.setattr(hc, "extract_text", lambda pdf: "  \n ")
    assert hc.parse_report(b"%PDF", filing())["status"] == hc.STATUS_SCANNED

    def broken(pdf):
        raise ValueError("bad xref")

    monkeypatch.setattr(hc, "extract_text", broken)
    report = hc.parse_report(b"%PDF", filing())
    assert report["status"] == hc.STATUS_FAILED and report["trades"] == []


def minimal_pdf(text):
    """A one-page PDF with a Helvetica text layer and a valid xref table."""
    stream = "BT /F1 9 Tf 20 760 Td 11 TL " + " ".join(
        f"({line.replace('(', chr(92) + '(').replace(')', chr(92) + ')')}) Tj T*"
        for line in text.split("\n")) + " ET"
    objects = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        "/Resources << /Font << /F1 5 0 R >> >> >>",
        f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out, offsets = "%PDF-1.4\n", []
    for i, obj in enumerate(objects, 1):
        offsets.append(len(out.encode("latin-1")))
        out += f"{i} 0 obj\n{obj}\nendobj\n"
    xref = len(out.encode("latin-1"))
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n"
    out += "".join(f"{o:010d} 00000 n \n" for o in offsets)
    out += f"trailer\n<< /Root 1 0 R /Size {len(objects) + 1} >>\nstartxref\n{xref}\n%%EOF\n"
    return out.encode("latin-1")


def test_real_pdf_text_layer_parses(hc):
    text = ptr_text("Synthetic Corp Common Stock (ZZAA) [ST]\nP 07/01/2026 07/13/2026 $1,001 - $15,000")
    report = hc.parse_report(minimal_pdf(text.replace("\x00", " ")), filing())
    assert report["status"] == hc.STATUS_PARSED
    assert [t["ticker"] for t in report["trades"]] == ["ZZAA"]


# ── Collection ───────────────────────────────────────────


class FakeResponse:
    def __init__(self, status=200, body=b""):
        self.status_code = status
        self.body = body

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self.body), chunk_size):
            yield self.body[i : i + chunk_size]

    def close(self):
        pass


class FakeSession:
    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    def get(self, url, headers=None, timeout=None, stream=False):
        assert stream and timeout and headers["User-Agent"]
        self.calls.append(url)
        for suffix, resp in self.routes.items():
            if url.endswith(suffix):
                return resp() if callable(resp) else resp
        return FakeResponse(404)


@pytest.fixture
def clerk(hc, monkeypatch, tmp_path):
    """Two recent PTRs (one e-filed, one scanned) and one outside the lookback."""
    today = date.today()
    recent = (today - timedelta(days=10)).strftime("%m/%d/%Y")
    old = (today - timedelta(days=400)).strftime("%m/%d/%Y")
    rows = [index_row("20000001", recent), index_row("8000002", recent),
            index_row("20000003", old)]
    years = {today.year, (today - timedelta(days=hc.LOOKBACK_DAYS)).year}
    routes = {f"{y}FD.zip": FakeResponse(body=index_zip(rows)) for y in years}
    routes["20000001.pdf"] = FakeResponse(body=b"efiled")
    routes["8000002.pdf"] = FakeResponse(body=b"scanned")
    text = ptr_text("Synthetic Corp Common Stock (ZZAA) [ST]\n"
                    f"P {recent} {recent} $1,001 - $15,000")
    monkeypatch.setattr(hc, "extract_text", lambda pdf: text if pdf == b"efiled" else "")
    return routes, tmp_path / "cache.json"


def test_collect_reads_recent_ptrs_and_reports_coverage(hc, clerk):
    routes, cache = clerk
    session = FakeSession(routes)
    result = hc.collect(session=session, cache_path=cache, sleep=NO_SLEEP)
    assert result["status"] == "success"
    assert [t["ticker"] for t in result["trades"]] == ["ZZAA"]
    cov = result["coverage"]
    assert (cov["reports"], cov["parsed"], cov["scanned"], cov["failed"]) == (2, 1, 1, 0)
    assert cov["scanned_reports"] == ["8000002"]
    assert not any(u.endswith("20000003.pdf") for u in session.calls)  # outside lookback


def test_cached_reports_are_not_downloaded_again(hc, clerk):
    routes, cache = clerk
    hc.collect(session=FakeSession(routes), cache_path=cache, sleep=NO_SLEEP)
    second = FakeSession(routes)
    result = hc.collect(session=second, cache_path=cache, sleep=NO_SLEEP)
    assert not any(u.endswith(".pdf") for u in second.calls)
    assert result["coverage"]["downloaded_this_run"] == 0
    assert [t["ticker"] for t in result["trades"]] == ["ZZAA"]


def test_stale_parser_version_reparses(hc, clerk, monkeypatch):
    routes, cache = clerk
    hc.collect(session=FakeSession(routes), cache_path=cache, sleep=NO_SLEEP)
    monkeypatch.setattr(hc, "PARSER_VERSION", hc.PARSER_VERSION + 1)
    again = FakeSession(routes)
    hc.collect(session=again, cache_path=cache, sleep=NO_SLEEP)
    assert sum(u.endswith(".pdf") for u in again.calls) == 2


def test_failed_downloads_are_not_cached_and_fail_the_run(hc, clerk):
    routes, cache = clerk
    routes["20000001.pdf"] = FakeResponse(503)
    result = hc.collect(session=FakeSession(routes), cache_path=cache, sleep=NO_SLEEP)
    # One of two reports unanswered is under MIN_USABLE_SUCCESS_RATE.
    assert result["status"] == hc.STATUS_FAILED
    assert result["coverage"]["failed_reports"] == ["20000001"]
    assert "20000001" not in json.loads(cache.read_text())["reports"]


def test_unusable_index_fails_without_report_requests(hc, clerk):
    routes, cache = clerk
    for key in [k for k in routes if k.endswith("FD.zip")]:
        routes[key] = FakeResponse(500)
    session = FakeSession(routes)
    result = hc.collect(session=session, cache_path=cache, sleep=NO_SLEEP)
    assert result["status"] == hc.STATUS_FAILED and result["trades"] == []
    assert not any(u.endswith(".pdf") for u in session.calls)


def test_oversized_report_is_a_failure(hc, clerk, monkeypatch):
    routes, cache = clerk
    monkeypatch.setattr(hc, "MAX_PDF_BYTES", 3)
    result = hc.collect(session=FakeSession(routes), cache_path=cache, sleep=NO_SLEEP)
    assert result["coverage"]["failed"] == 2


# ── fmp.py merge ─────────────────────────────────────────


def fmp_item(chamber, symbol):
    return {"symbol": symbol, "firstName": "FMP", "lastName": chamber, "type": "Purchase",
            "amount": "$1,001 - $15,000", "transactionDate": date.today().isoformat(),
            "disclosureDate": date.today().isoformat(), "assetDescription": symbol,
            "assetType": "Stock", "district": "", "link": "", "_chamber": chamber}


@pytest.fixture
def fmp(monkeypatch, tmp_path):
    module = collectors._fetcher("fmp")
    monkeypatch.setattr(module, "FMP_API_KEY", "synthetic-key")
    monkeypatch.setattr(module, "LOCAL_DATA_DIR", tmp_path)
    monkeypatch.setattr(module, "legacy_fetch_chamber_trades", lambda c, **kw: [
        fmp_item(c.capitalize(), "ZZSN" if c == "senate" else "ZZFH")])
    return module


def clerk_trade():
    return {"politician_name": "Synthetic Member", "chamber": "House", "ticker": "ZZHC",
            "asset_description": "Synthetic", "asset_type": "Stock", "trade_type": "Purchase",
            "amount_range": "$1,001 - $15,000", "transaction_date": date.today().isoformat(),
            "disclosure_date": date.today().isoformat(), "days_to_disclose": 0,
            "district": "ZZ01", "source_link": "https://example.invalid/1.pdf",
            "fetched_at": "2026-10-21T00:00:00+00:00"}


def test_fmp_takes_house_rows_from_clerk(fmp, hc, monkeypatch, tmp_path):
    monkeypatch.setattr(hc, "collect", lambda: {
        "status": "success", "trades": [clerk_trade()], "coverage": {"reports": 1}})
    fmp.run()
    out = json.loads((tmp_path / "trades_congress.json").read_text())
    assert sorted(t["ticker"] for t in out["trades"]) == ["ZZHC", "ZZSN"]
    assert out["house_source"] == "house_clerk"
    assert out["house_clerk"] == {"status": "success", "reports": 1}
    assert "Clerk of the House" in out["source"]


def test_fmp_keeps_its_house_rows_when_clerk_fails(fmp, hc, monkeypatch, tmp_path):
    monkeypatch.setattr(hc, "collect", lambda: {
        "status": "failed", "trades": [], "coverage": {"error": "index_unusable:2026"}})
    fmp.run()
    out = json.loads((tmp_path / "trades_congress.json").read_text())
    assert sorted(t["ticker"] for t in out["trades"]) == ["ZZFH", "ZZSN"]
    assert out["house_source"] == "fmp"
    assert out["house_clerk"]["status"] == "failed"
    assert out["source"] == "Financial Modeling Prep (FMP)"


def test_merged_file_still_imports_and_feeds_consensus(fmp, hc, monkeypatch, tmp_path):
    from src.ingestion.files import read_fetcher_file
    from src.services.consensus_watchlist import consensus

    monkeypatch.setattr(hc, "collect", lambda: {
        "status": "success", "trades": [clerk_trade()], "coverage": {"reports": 1}})
    fmp.run()
    path = tmp_path / "trades_congress.json"
    outcome = read_fetcher_file("congress_trades", path, mode="file", attest_complete=True)
    assert outcome.succeeded and len(outcome.records) == 2
    assert "ZZHC" in consensus.load_congress_signals(path)


# ── Database collector (collect_congress) ────────────────


@pytest.fixture
def clerk_http(hc, monkeypatch, tmp_path):
    """House Clerk index with one e-filed and one scanned recent PTR, via collectors' session."""
    from test_collectors import FakeResponse as HttpResponse
    from test_collectors import FakeSession as HttpSession

    recent = (date.today() - timedelta(days=10)).strftime("%m/%d/%Y")
    index = index_zip([index_row("20000001", recent), index_row("8000002", recent)])
    text = ptr_text("Synthetic Corp Common Stock (ZZAA) [ST]\n"
                    f"P {recent} {recent} $1,001 - $15,000")
    monkeypatch.setattr(hc, "extract_text", lambda pdf: text if pdf == b"efiled" else "")
    monkeypatch.setattr(hc, "LOCAL_DATA_DIR", tmp_path)
    routes = {"FD.zip": index, "20000001.pdf": b"efiled", "8000002.pdf": b"scanned"}

    def handler(url, params):
        for suffix, body in routes.items():
            if url.endswith(suffix):
                return HttpResponse(raw=body) if isinstance(body, bytes) else body
        return HttpResponse(status=404, body={})

    return lambda: HttpSession(handler), routes


def test_collector_house_only_needs_no_fmp_key(clerk_http):
    make_session, _ = clerk_http
    session = make_session()
    outcome = collectors.collect_congress(
        session, sleep=NO_SLEEP, house_source="house_clerk", include_fmp_senate=False
    )
    assert outcome.succeeded, outcome.diagnostics
    assert not any("financialmodelingprep" in url for url, _ in session.calls)
    assert [(r["ticker"], r["chamber"], r["data_source"]) for r in outcome.records] == [
        ("ZZAA", "House", "house_clerk")]
    d = outcome.diagnostics
    assert (d["house_reports"], d["house_reports_scanned"], d["failed"]) == (2, 1, 0)
    assert outcome.envelope["chambers"] == ["House"]
    assert outcome.envelope["house_source"] == "house_clerk"


def test_collector_reuses_cached_reports(clerk_http):
    make_session, _ = clerk_http
    collectors.collect_congress(make_session(), sleep=NO_SLEEP, house_source="house_clerk",
                                include_fmp_senate=False)
    again = make_session()
    outcome = collectors.collect_congress(again, sleep=NO_SLEEP, house_source="house_clerk",
                                          include_fmp_senate=False)
    assert outcome.succeeded and len(outcome.records) == 1
    assert not any(url.endswith(".pdf") for url, _ in again.calls)
    assert outcome.diagnostics["house_reports_downloaded"] == 0


def test_collector_fails_on_partly_parsed_report_instead_of_dropping_trades(
    hc, clerk_http, monkeypatch
):
    make_session, _ = clerk_http
    broken = ptr_text("Synthetic Corp Common Stock [ST]\nP 07/01/2026 07/13/2026 $1,001 - $15,000")
    monkeypatch.setattr(hc, "extract_text", lambda pdf: broken if pdf == b"efiled" else "")
    outcome = collectors.collect_congress(make_session(), sleep=NO_SLEEP,
                                          house_source="house_clerk", include_fmp_senate=False)
    assert not outcome.succeeded
    assert outcome.diagnostics["failures"] == [
        {"unit": "house report 20000001", "code": "parse_error"}]


@pytest.mark.parametrize("unit_suffix,unit_name", [
    ("FD.zip", "house index"),
    ("20000001.pdf", "house report 20000001"),
])
def test_collector_fails_fast_on_a_failed_clerk_request(clerk_http, unit_suffix, unit_name):
    from test_collectors import FakeResponse as HttpResponse

    make_session, routes = clerk_http
    routes[unit_suffix] = HttpResponse(status=403, body={})
    outcome = collectors.collect_congress(make_session(), sleep=NO_SLEEP,
                                          house_source="house_clerk", include_fmp_senate=False)
    assert not outcome.succeeded and outcome.coverage is None
    [failure] = outcome.diagnostics["failures"]
    assert failure["unit"].startswith(unit_name) and failure["code"] == "http_403"


def test_collector_adds_fmp_senate_when_enabled(clerk_http):
    from test_collectors import FakeResponse as HttpResponse

    make_session, routes = clerk_http
    routes["senate-latest"] = HttpResponse(body=[{
        "symbol": "ZZSN", "firstName": "Synthetic", "lastName": "Senator", "type": "Purchase",
        "amount": "$1,001 - $15,000", "transactionDate": date.today().isoformat(),
        "disclosureDate": date.today().isoformat(), "assetDescription": "Synthetic",
        "assetType": "Stock", "district": "", "link": ""}])
    session = make_session()
    outcome = collectors.collect_congress(session, api_key="synthetic", sleep=NO_SLEEP,
                                          house_source="house_clerk", include_fmp_senate=True)
    assert outcome.succeeded, outcome.diagnostics
    assert sorted((r["ticker"], r["chamber"]) for r in outcome.records) == [
        ("ZZAA", "House"), ("ZZSN", "Senate")]
    assert not any(url.endswith("house-latest") for url, _ in session.calls)
    assert outcome.envelope["chambers"] == ["Senate", "House"]


def test_collector_settings_choose_the_sources(clerk_http):
    """Defaults: House from the Clerk, Senate from FMP's single free page."""
    from src.settings import Settings
    from test_collectors import FakeResponse as HttpResponse

    make_session, routes = clerk_http
    routes["senate-latest"] = HttpResponse(body=[])
    settings = Settings(
        database_url="postgresql+psycopg://u@127.0.0.1/x_test", fmp_api_key="synthetic"
    )
    assert (settings.congress_house_source, settings.congress_include_fmp_senate) == (
        "house_clerk", True)
    session = make_session()
    outcome = collectors.collect_congress(session, sleep=NO_SLEEP, settings=settings)
    assert outcome.succeeded and outcome.envelope["chambers"] == ["Senate", "House"]
    fmp_pages = [p.get("page") for url, p in session.calls if "financialmodelingprep" in url]
    assert fmp_pages == [0]  # Senate only, page 0 only
    # Clerk rows never look like a full FMP page to the history gap check.
    assert outcome.diagnostics["house_rows_served"] == 0


def test_house_only_settings_need_no_fmp_key(clerk_http):
    from src.settings import Settings

    make_session, _ = clerk_http
    settings = Settings(
        database_url="postgresql+psycopg://u@127.0.0.1/x_test",
        congress_include_fmp_senate=False,
    )
    session = make_session()
    outcome = collectors.collect_congress(session, sleep=NO_SLEEP, settings=settings)
    assert outcome.succeeded and outcome.envelope["chambers"] == ["House"]
    assert not any("financialmodelingprep" in url for url, _ in session.calls)


def test_collector_without_settings_keeps_fmp_both_chambers():
    """Callers that pass neither settings nor arguments get the original FMP collection."""
    outcome = collectors.collect_congress(api_key="")
    assert outcome.error_code == "missing_credential"  # FMP is still required


# ── Candidate cap (ranking v3, legacy consensus) ─────────


def congress_row(symbol, member, day, row_number, source="house_clerk"):
    return {"symbol": symbol, "politician_name": member, "transaction_date": day,
            "trade_type": "Purchase", "data_source": source, "row_number": row_number}


def test_v3_caps_congress_candidates_by_buyer_strength():
    from src.pipeline import ranking

    d1, d2 = date(2026, 9, 1), date(2026, 9, 20)
    rows = {
        "congress_trades": [
            congress_row("ZZAA", "A", d1, 1), congress_row("ZZAA", "B", d1, 2),  # 2 buyers
            congress_row("ZZBB", "A", d1, 3), congress_row("ZZBB", "A", d2, 4),  # 1 buyer, 2 buys
            congress_row("ZZCC", "C", d2, 5),                                    # 1 buyer, newer
            congress_row("ZZDD", "D", d1, 6),                                    # 1 buyer, older
        ],
        "ark_holdings": [{"symbol": "ZZEE"}],
    }

    def capped(n):
        return ranking.candidate_symbols(rows, config={"max_congress_candidates": n})

    assert capped(1) == ["ZZAA", "ZZEE"]
    assert capped(2) == ["ZZAA", "ZZBB", "ZZEE"]
    assert capped(3) == ["ZZAA", "ZZBB", "ZZCC", "ZZEE"]
    assert ranking.candidate_symbols(rows) == ["ZZAA", "ZZBB", "ZZCC", "ZZDD", "ZZEE"]
    # Released versions keep their uncapped rule, so their publications replay.
    assert ranking.candidate_symbols(
        rows, version="watchlist-v2", config={"max_congress_candidates": 1}
    ) == ["ZZAA", "ZZBB", "ZZCC", "ZZDD", "ZZEE"]


def test_legacy_consensus_keeps_strongest_congress_candidates():
    from src.services.consensus_watchlist import consensus

    signals = {
        "ZZAA": {"distinct_buyer_count": 2, "buy_count": 2,
                 "most_recent_transaction_date": "2026-09-01"},
        "ZZBB": {"distinct_buyer_count": 1, "buy_count": 2,
                 "most_recent_transaction_date": "2026-09-20"},
        "ZZCC": {"distinct_buyer_count": 1, "buy_count": 1,
                 "most_recent_transaction_date": "2026-09-20"},
        "ZZDD": {"distinct_buyer_count": 1, "buy_count": 1,
                 "most_recent_transaction_date": None},
    }
    assert consensus.top_congress_candidates(signals, limit=3) == ["ZZAA", "ZZBB", "ZZCC"]
    assert consensus.top_congress_candidates(signals)[-1] == "ZZDD"


# ── Trades dated after their own disclosure ──────────────


def dated_row(symbol, member, traded, disclosed, row_number):
    return {**congress_row(symbol, member, traded, row_number),
            "disclosure_date": disclosed, "amount_label": "$1,001 - $15,000"}


def test_v4_ignores_purchases_dated_after_their_disclosure():
    """A SONY-style typo (traded 2026-12-26, disclosed 2026-02-09) is not a 'most recent' buy."""
    from datetime import datetime, timezone
    from decimal import Decimal

    from src.pipeline import ranking

    good = dated_row("ZZAA", "A", date(2026, 1, 20), date(2026, 2, 9), 1)
    typo = dated_row("ZZAA", "B", date(2026, 12, 26), date(2026, 2, 9), 2)
    only_typo = dated_row("ZZBB", "C", date(2026, 12, 26), date(2026, 2, 9), 3)
    rows = {"congress_trades": [good, typo, only_typo], "ark_holdings": [],
            "insider_trades": [], "short_interest": []}

    assert ranking.candidate_symbols(rows) == ["ZZAA"]
    caps = {"ZZAA": Decimal("20000000000"), "ZZBB": Decimal("20000000000")}
    out = ranking.calculate(rows, caps, datetime(2026, 10, 24, tzinfo=timezone.utc))
    [record] = out["popular_stable"]
    signal = record["signals"]["congress"]
    assert record["ticker"] == "ZZAA"
    assert (signal["buy_count"], signal["distinct_buyer_count"]) == (1, 1)
    assert signal["most_recent_transaction_date"] == "2026-01-20"

    # Released v3 keeps its rule, so its publications replay unchanged.
    v3 = ranking.calculate(rows, caps, datetime(2026, 10, 24, tzinfo=timezone.utc),
                           version="watchlist-v3")
    v3_signal = {r["ticker"]: r["signals"]["congress"] for r in v3["popular_stable"]}
    assert v3_signal["ZZAA"]["most_recent_transaction_date"] == "2026-12-26"
    assert "ZZBB" in v3_signal


def test_legacy_consensus_skips_purchases_dated_after_disclosure(tmp_path):
    from src.services.consensus_watchlist import consensus

    path = tmp_path / "trades_congress.json"
    path.write_text(json.dumps({"trades": [
        {"ticker": "ZZAA", "trade_type": "Purchase", "politician_name": "A",
         "transaction_date": "2026-01-20", "disclosure_date": "2026-02-09"},
        {"ticker": "ZZAA", "trade_type": "Purchase", "politician_name": "B",
         "transaction_date": "2026-12-26", "disclosure_date": "2026-02-09"},
        {"ticker": "ZZBB", "trade_type": "Purchase", "politician_name": "C",
         "transaction_date": "2026-12-26", "disclosure_date": "2026-02-09"},
    ]}))
    signals = consensus.load_congress_signals(path)
    assert set(signals) == {"ZZAA"}
    assert signals["ZZAA"]["buy_count"] == 1
    assert signals["ZZAA"]["most_recent_transaction_date"] == "2026-01-20"


def test_bad_dates_and_late_filings_are_counted(hc, clerk_http, monkeypatch):
    make_session, _ = clerk_http
    recent = date.today() - timedelta(days=10)
    disclosed = recent.strftime("%m/%d/%Y")
    late = (recent - timedelta(days=60)).strftime("%m/%d/%Y")
    after = (recent + timedelta(days=5)).strftime("%m/%d/%Y")
    text = ptr_text(
        f"Synthetic Late Corp Common Stock (ZZLT) [ST]\nP {late} {disclosed} $1,001 - $15,000",
        f"Synthetic Typo Corp Common Stock (ZZTY) [ST]\nP {after} {disclosed} $1,001 - $15,000",
    )
    monkeypatch.setattr(hc, "extract_text", lambda pdf: text if pdf == b"efiled" else "")
    outcome = collectors.collect_congress(make_session(), sleep=NO_SLEEP,
                                          house_source="house_clerk", include_fmp_senate=False)
    assert outcome.succeeded and len(outcome.records) == 2  # both stored as filed
    assert outcome.diagnostics["house_trades_dated_after_disclosure"] == 1
