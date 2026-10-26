"""S1-12: the FMP profile description adapter (decision 75 (a)), spec-first.

The description comes from FMP's existing /stable/profile response, the request
runner.collect_market_caps already makes, staging only (Q10) and still gated by
decision 3. Decision 1 froze new FMP adapter work, so these tests pin the
request to collect_market_caps's own shape: no new endpoint, no new params.

The seam: src.ingestion.fmp_descriptions.FmpProfileDescriptions(api_key=None, *,
session=None), a DescriptionAdapter (src.ingestion.company_summaries). With no
api_key it reads the credential collect_market_caps reads (the FMP fetcher's
FMP_API_KEY). Fake HTTP only: test_collectors' FakeSession, the layer the other
provider tests use. No real FMP call is ever made.
"""

import logging
import re
from urllib.parse import urlsplit

import pytest
import requests
from test_collectors import FakeResponse, FakeSession

KEY = "synthetic-fmp-key-0123456789"
PROFILE_URL = "https://financialmodelingprep.com/stable/profile"
# Decision 78 (a): the source link is FMP's public home page for every symbol
# (the per-company page only loads after logging in to FMP).
HOME_URL = "https://financialmodelingprep.com/"
DESCRIPTION = (
    "Synthetic Widgets makes small tools for homes.  It sells them in shops.\n"
    "Its staff work in one town."
)
# 0012's ck_company_profiles_summary_source_url, in Python.
STORABLE_URL = re.compile(r"https://[A-Za-z0-9.-]+(:[0-9]{1,5})?([/?#]\S*)?")


def profile(symbol="ZZAA", **fields):
    item = {
        "symbol": symbol,
        "companyName": f"Synthetic {symbol}",
        "currency": "USD",
        "marketCap": 1234567890,
        "sector": "Industrials",
        "industry": "Tools & Accessories",
        "website": "https://widgets.example.com",
        "description": DESCRIPTION,
    }
    item.update(fields)
    return [item]


def ok(body):
    return lambda url, params: FakeResponse(200, body)


def adapter(handler, api_key=KEY):
    from src.ingestion.fmp_descriptions import FmpProfileDescriptions

    session = FakeSession(handler)
    return FmpProfileDescriptions(api_key, session=session), session


@pytest.fixture
def no_retries(monkeypatch):
    """_get_json reads MAX_HTTP_RETRIES at call time: no real sleeps between retries."""
    from src.ingestion import collectors

    monkeypatch.setattr(collectors, "MAX_HTTP_RETRIES", 0)


# ── the existing request, unchanged ──────────────────────────


def test_the_request_is_collect_market_caps_own_profile_call():
    from src.pipeline.runner import collect_market_caps

    reference = FakeSession(ok(profile()))
    collect_market_caps(["ZZAA"], session=reference, api_key=KEY)
    descriptions, session = adapter(ok(profile()))
    descriptions.fetch("ZZAA")
    assert session.calls == reference.calls  # one GET, the same URL and params
    assert session.calls == [(PROFILE_URL, {"symbol": "ZZAA", "apikey": KEY})]


def test_one_fetch_is_one_request():
    descriptions, session = adapter(ok(profile()))
    descriptions.fetch("ZZAA")
    descriptions.fetch("ZZAA")
    assert len(session.calls) == 2  # no hidden second call per symbol


def test_without_an_api_key_it_uses_collect_market_caps_credential(monkeypatch):
    from src.ingestion.collectors import _fetcher
    from src.ingestion.fmp_descriptions import FmpProfileDescriptions

    monkeypatch.setattr(_fetcher("fmp"), "FMP_API_KEY", KEY)
    session = FakeSession(ok(profile()))
    FmpProfileDescriptions(session=session).fetch("ZZAA")
    assert session.calls == [(PROFILE_URL, {"symbol": "ZZAA", "apikey": KEY})]


# ── what it returns ──────────────────────────────────────────


def test_source_name_is_the_providers_name():
    descriptions, _ = adapter(ok(profile()))
    assert descriptions.source_name == "Financial Modeling Prep"
    assert len(descriptions.source_name) <= 128  # company_profiles.summary_source_name


def test_the_description_is_returned_verbatim():
    from src.ingestion.company_summaries import Description

    raw = "  " + DESCRIPTION + " \n"  # surrounding whitespace too: nothing is trimmed
    descriptions, _ = adapter(ok(profile(description=raw)))
    found = descriptions.fetch("ZZAA")
    assert isinstance(found, Description)
    assert found.text == raw  # summarizing is the job's, not the adapter's


@pytest.mark.parametrize("symbol", ["ZZAA", "BRK.B", "ZZ-B"])
def test_the_source_url_is_fmps_home_page_for_every_symbol(symbol):
    descriptions, _ = adapter(ok(profile(symbol)))
    url = descriptions.fetch(symbol).url
    assert url == HOME_URL  # decision 78 (a)
    parts = urlsplit(url)
    assert parts.scheme == "https" and parts.hostname
    assert STORABLE_URL.fullmatch(url) and len(url) <= 2048
    assert "apikey" not in url.lower() and KEY not in url
    assert symbol not in url and "%" not in url


@pytest.mark.parametrize(
    "body",
    [
        [],  # FMP has no profile for the symbol
        profile(description=None),
        profile(description=""),
        profile(description="   \n\t "),
        [{k: v for k, v in profile()[0].items() if k != "description"}],
    ],
    ids=["no_profile", "null", "empty", "whitespace_only", "missing_field"],
)
def test_no_description_is_none(body):
    descriptions, _ = adapter(ok(body))
    assert descriptions.fetch("ZZAA") is None


# ── failures: a fixed code, no key, no body ──────────────────

BODY_TEXT = "synthetic provider body text"


def _raise(error):
    def handler(url, params):
        raise error

    return handler


FAILURES = {
    "http_401": lambda url, params: FakeResponse(401, {"Error Message": BODY_TEXT}),
    "http_403": lambda url, params: FakeResponse(403, {"Error Message": BODY_TEXT}),
    "http_429_rate_limited": lambda url, params: FakeResponse(
        429, {"Error Message": BODY_TEXT}
    ),
    "http_500": lambda url, params: FakeResponse(500, {"Error Message": BODY_TEXT}),
    "timeout": _raise(requests.Timeout(BODY_TEXT)),
    "connection": _raise(requests.ConnectionError(BODY_TEXT)),
    "invalid_json": lambda url, params: FakeResponse(
        200, raw=b"<html>" + BODY_TEXT.encode()
    ),
    "object_not_list": ok({"symbol": "ZZAA", "description": BODY_TEXT}),
    "two_items": ok(profile() + profile()),
    "item_not_object": ok([BODY_TEXT]),
    "mismatched_symbol": ok(profile("ZZBB", description=BODY_TEXT)),
    "description_not_text": ok(profile(description=["not", "text"])),
}


@pytest.mark.parametrize("case", sorted(FAILURES))
def test_a_provider_failure_is_a_fixed_code_without_key_or_body(no_retries, case):
    from src.ingestion.company_summaries import DescriptionAdapterError

    descriptions, _ = adapter(FAILURES[case])
    with pytest.raises(DescriptionAdapterError) as caught:
        descriptions.fetch("ZZAA")
    error = caught.value
    assert isinstance(error.code, str) and re.fullmatch(r"[a-z0-9_]{1,64}", error.code)
    for shown in (str(error), repr(error), repr(error.args), error.code):
        assert KEY not in shown and BODY_TEXT not in shown, case
    assert error.__cause__ is None or (
        KEY not in str(error.__cause__) and BODY_TEXT not in str(error.__cause__)
    )


@pytest.mark.parametrize(
    "key",
    [None, "", "  ", "your_fmp_key_here"],
    ids=["none", "empty", "blank", "placeholder"],
)
def test_a_missing_key_is_a_fixed_code_and_no_request(monkeypatch, key):
    from src.ingestion.collectors import _fetcher
    from src.ingestion.company_summaries import DescriptionAdapterError
    from src.ingestion.fmp_descriptions import FmpProfileDescriptions

    monkeypatch.setattr(_fetcher("fmp"), "FMP_API_KEY", None)
    session = FakeSession(ok(profile()))
    with pytest.raises(DescriptionAdapterError) as caught:
        FmpProfileDescriptions(key, session=session).fetch("ZZAA")
    assert caught.value.code == "missing_credential"
    assert session.calls == []


def test_the_key_is_never_logged(caplog, no_retries):
    from src.ingestion.company_summaries import DescriptionAdapterError

    caplog.set_level(logging.DEBUG)
    caplog.clear()
    adapter(ok(profile()))[0].fetch("ZZAA")
    with pytest.raises(DescriptionAdapterError):
        adapter(FAILURES["http_500"])[0].fetch("ZZAA")
    for record in caplog.records:
        message = record.getMessage()
        assert KEY not in message and BODY_TEXT not in message, record.name


# ── through the job: no false success ────────────────────────


@pytest.mark.db
def test_the_job_fails_and_writes_nothing_when_fmp_fails(engine, no_retries):
    from sqlalchemy import text
    from src.ingestion.company_summaries import summarize
    from test_read_api import seed_security

    seed_security(
        engine, "ZZAA", name="Synthetic ZZAA", profile={"sector": "Industrials"}
    )
    descriptions, _ = adapter(FAILURES["http_500"])
    result = summarize(engine, descriptions, symbols=["ZZAA"])
    assert not result.succeeded and result.error_code
    with engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT summary_status FROM perennial.company_profiles p JOIN perennial.securities s"
                    " ON s.company_profile_id = p.id WHERE s.symbol = 'ZZAA'"
                )
            ).scalar()
            is None
        )


@pytest.mark.db
def test_the_job_stores_an_fmp_excerpt_with_the_safe_url(engine):
    from sqlalchemy import text
    from src.ingestion.company_summaries import summarize
    from test_read_api import seed_security

    seed_security(
        engine, "ZZAA", name="Synthetic ZZAA", profile={"sector": "Industrials"}
    )
    descriptions, _ = adapter(ok(profile()))
    result = summarize(engine, descriptions, symbols=["ZZAA"])
    assert result.succeeded
    with engine.connect() as conn:
        row = (
            conn.execute(
                text(
                    "SELECT summary_status, summary_source_name, summary_source_url"
                    " FROM perennial.company_profiles p JOIN perennial.securities s"
                    " ON s.company_profile_id = p.id WHERE s.symbol = 'ZZAA'"
                )
            )
            .mappings()
            .one()
        )
    assert row["summary_status"] == "ok"
    assert row["summary_source_name"] == "Financial Modeling Prep"
    assert row["summary_source_url"] == HOME_URL
    assert (
        "apikey" not in row["summary_source_url"].lower()
        and KEY not in row["summary_source_url"]
    )
