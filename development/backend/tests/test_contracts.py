"""Database-free tests: configuration, validation, canonical hashing, redaction, diagnostics, files."""

import copy
import json
import os
import traceback
from datetime import date
from decimal import Decimal

import pytest

from src.db.config import DatabaseConfigError, get_database_url, parse_database_url
from src.ingestion.canonical import HASH_VERSION, canonicalize
from src.ingestion.diagnostics import render_summary, safe_diagnostics
from src.ingestion.envelope import sanitize_envelope
from src.ingestion.files import read_fetcher_file
from src.ingestion.importer import ImportFailure, validate_records
from src.ingestion.redaction import redact
from src.ingestion.schemas import RECORDS_KEY

SECRET = "SYNTHETIC_SECRET_7f3a"


def load(fixture_path, source):
    name = {
        "congress_trades": "trades_congress.json",
        "ark_holdings": "ark_holdings.json",
        "insider_trades": "trades_insider.json",
        "short_interest": "short_interest.json",
    }[source]
    return json.loads(fixture_path(name).read_text())


def records(fixture_path, source):
    return load(fixture_path, source)[RECORDS_KEY[source]]


def batch_hash(source, raw, **kw):
    return canonicalize(source, validate_records(source, raw), **kw).content_hash


# ── config ───────────────────────────────────────────────

@pytest.mark.parametrize("raw", [
    None, "", f"mysql://u:{SECRET}@h/db", f"postgresql+psycopg://u:{SECRET}@h:{SECRET}/db",
    f"postgresql+psycopg://u:{SECRET}@/db", f"{SECRET}://x", f"not a url {SECRET}",
])
def test_config_errors_never_echo_url(raw):
    with pytest.raises(DatabaseConfigError) as info:
        parse_database_url(raw)
    rendered = "".join(traceback.format_exception(info.type, info.value, info.tb))
    assert SECRET not in rendered


def test_config_accepts_plain_scheme_and_env_file(tmp_path):
    assert parse_database_url("postgresql://u:p@localhost:5433/db").drivername == "postgresql+psycopg"
    env = tmp_path / ".env"
    env.write_text("DATABASE_URL=postgresql+psycopg://a:b@localhost:5433/perennial\n")
    url = get_database_url(environ={}, env_path=env)
    assert (url.host, url.port, url.database) == ("localhost", 5433, "perennial")
    with pytest.raises(DatabaseConfigError):
        get_database_url(environ={}, env_path=tmp_path / "missing.env")


@pytest.mark.parametrize("raw,ok", [
    ("postgresql+psycopg://u:p@127.0.0.1:55439/perennial_test", True),
    ("postgresql+psycopg://u:p@localhost:55439/perennial_test?sslmode=disable", True),
    ("postgresql+psycopg://u:p@db.example.invalid:5432/perennial_test", False),
    ("postgresql+psycopg://u:p@127.0.0.1:55439/perennial", False),
    ("postgresql+psycopg://u:p@127.0.0.1:55439/perennial_test?host=db.example.invalid", False),
    ("postgresql+psycopg://u:p@127.0.0.1:55439/perennial_test?hostaddr=192.0.2.1", False),
    ("postgresql+psycopg://u:p@127.0.0.1:55439/perennial_test?dbname=production", False),
    ("postgresql+psycopg://u:p@127.0.0.1:55439/perennial_test?service=prod", False),
    ("postgresql+psycopg://u:p@127.0.0.1:55439/perennial_test?port=5432", False),
])
def test_test_database_guard(raw, ok):
    from tests.conftest import check_test_server_url

    url = parse_database_url(raw)
    if ok:
        check_test_server_url(url)
    else:
        with pytest.raises(ValueError):
            check_test_server_url(url)


# ── validation: dates, numbers, nulls, tickers ───────────

def test_fixture_values_normalize(fixture_path):
    congress = validate_records("congress_trades", records(fixture_path, "congress_trades"))
    assert congress[2].ticker == "ZZBB"
    assert congress[2].disclosure_date is None and congress[2].source_link is None
    assert congress[2].district == "ZZ01" and congress[0].district is None
    assert congress[0].transaction_date == date(2026, 8, 14)
    assert congress[3].ticker == "ZZB.B"

    ark = validate_records("ark_holdings", records(fixture_path, "ark_holdings"))
    assert ark[0].funds == ["ARKK", "ARKW"]
    assert ark[0].total_weight == Decimal("10.30000000")
    assert ark[1].share_price is None  # upstream 0.0 default means unknown
    assert ark[2].total_weight == Decimal("101.25")  # no 0-100 cap

    insider = validate_records("insider_trades", records(fixture_path, "insider_trades"))
    assert insider[0].value == Decimal("25000.10") and insider[2].shares == Decimal("200000.5")

    si = validate_records("short_interest", records(fixture_path, "short_interest"))
    assert si[0].short_interest_shares == 2926673 and si[0].average_daily_volume == 1500000
    assert si[1].average_daily_volume is None and si[1].days_to_cover is None
    assert len(si[0].history) == 2


@pytest.mark.parametrize("source,field,value,error_type", [
    ("congress_trades", "transaction_date", "08/14/2026", "value_error"),
    ("congress_trades", "transaction_date", "", "value_error"),
    ("congress_trades", "transaction_date", "2026-02-30", "value_error"),
    ("congress_trades", "politician_name", "   ", "value_error"),
    ("congress_trades", "ticker", "N/A", "value_error"),
    ("congress_trades", "ticker", "AB C", "value_error"),
    ("congress_trades", "source_link", "https://user:pw@example.invalid/x", "value_error"),
    ("congress_trades", "source_link", "https://example.invalid?apikey=x", "value_error"),
    ("congress_trades", "source_link", "https://example.invalid/#access_token=x", "value_error"),
    ("congress_trades", "source_link", "https://example.invalid/ptr/1#SYNTHETIC_OPAQUE_TOKEN", "value_error"),
    ("congress_trades", "source_link", "https://example.invalid/ptr/1#", "value_error"),
    ("congress_trades", "source_link", "javascript:alert(1)", "value_error"),
    ("congress_trades", "district", "X" * 21, "value_error"),
    ("ark_holdings", "total_weight", "1e100", "value_error"),
    ("ark_holdings", "total_weight", float("nan"), "value_error"),
    ("ark_holdings", "total_weight", True, "value_error"),
    ("ark_holdings", "share_price", -1, "value_error"),
    ("ark_holdings", "fund_count", 3, "value_error"),
    ("insider_trades", "value", "abc", "value_error"),
    ("short_interest", "settlement_date", None, "value_error"),
    ("short_interest", "short_interest", 1.5, "value_error"),
])
def test_invalid_field_fails_whole_batch(fixture_path, source, field, value, error_type):
    raw = records(fixture_path, source)
    raw[-1] = dict(raw[-1], **{field: value})
    with pytest.raises(ImportFailure) as info:
        validate_records(source, raw)
    assert info.value.code == "validation_failed"
    first = info.value.diagnostics["errors"][0]
    assert first["record"] == len(raw) - 1


@pytest.mark.parametrize("ticker,symbol,exchange", [
    ("RKLB UQ", "RKLB", "NASDAQ"),
    ("dkng uw", "DKNG", "NASDAQ"),
    ("ZZAA UN", "ZZAA", "NYSE"),
    ("ZZAA", "ZZAA", None),
])
def test_us_exchange_suffix_maps_to_base_symbol(fixture_path, ticker, symbol, exchange):
    raw = records(fixture_path, "ark_holdings")[:1]
    raw[0]["ticker"] = ticker
    parsed = validate_records("ark_holdings", raw)[0]
    assert (parsed.ticker, parsed.exchange) == (symbol, exchange)


@pytest.mark.parametrize("ticker,exchange", [
    ("6618 HK", None),       # foreign listing: unsupported universe
    ("ZZAA LN", None),
    ("ZZAA UQ", "NYSE"),     # suffix contradicts an explicit exchange
    ("ZZAA UQ UQ", None),
])
def test_unsupported_suffixes_are_rejected(fixture_path, ticker, exchange):
    raw = records(fixture_path, "ark_holdings")[:1]
    raw[0]["ticker"] = ticker
    if exchange:
        raw[0]["exchange"] = exchange
    with pytest.raises(ImportFailure):
        validate_records("ark_holdings", raw)


def test_suffixed_and_plain_duplicate_is_rejected(fixture_path):
    raw = records(fixture_path, "ark_holdings")[:1]
    raw.append(dict(raw[0], ticker="ZZAA UQ"))
    with pytest.raises(ImportFailure) as info:
        validate_records("ark_holdings", raw)
    assert info.value.diagnostics["errors"][0]["type"] == "duplicate_security"


def test_blank_optional_text_becomes_null(fixture_path):
    raw = records(fixture_path, "congress_trades")
    for key in ("district", "chamber", "asset_type", "asset_description", "source_link", "amount_range"):
        raw[0][key] = ""
    parsed = validate_records("congress_trades", raw)[0]
    assert parsed.district is None and parsed.amount_label is None and parsed.source_link is None


def test_history_exponent_is_bounded(fixture_path):
    raw = records(fixture_path, "short_interest")
    raw[0]["history"][0]["days_to_cover"] = "1e100000"
    with pytest.raises(ImportFailure):
        validate_records("short_interest", raw)


def test_duplicate_security_rejected_for_one_row_sources(fixture_path):
    raw = records(fixture_path, "ark_holdings")
    raw.append(copy.deepcopy(raw[0]))
    with pytest.raises(ImportFailure) as info:
        validate_records("ark_holdings", raw)
    assert info.value.diagnostics["errors"][0]["type"] == "duplicate_security"


# ── canonical hash ───────────────────────────────────────

def test_hash_ignores_fetched_at_and_order_but_keeps_multiplicity(fixture_path):
    raw = records(fixture_path, "congress_trades")
    base = batch_hash("congress_trades", raw)

    restamped = copy.deepcopy(raw)
    for r in restamped:
        r["fetched_at"] = "2030-01-01T00:00:00+00:00"
    assert batch_hash("congress_trades", restamped) == base
    assert batch_hash("congress_trades", list(reversed(raw))) == base

    deduped = [r for i, r in enumerate(raw) if i != 1]  # drop one of the repeated rows
    assert batch_hash("congress_trades", deduped) != base

    changed = copy.deepcopy(raw)
    changed[4]["amount_range"] = "$15,001 - $50,000"
    assert batch_hash("congress_trades", changed) != base


def test_hash_numeric_text_is_canonical(fixture_path):
    raw = records(fixture_path, "ark_holdings")
    as_text = copy.deepcopy(raw)
    as_text[2]["total_weight"] = "101.2500"
    as_text[2]["share_price"] = "7.50"
    assert batch_hash("ark_holdings", as_text) == batch_hash("ark_holdings", raw)


def test_hash_includes_version_and_source_as_of():
    batch = canonicalize("insider_trades", [])
    assert batch.payload["hash_version"] == HASH_VERSION
    dated = canonicalize("insider_trades", [], source_as_of=date(2026, 9, 30))
    assert dated.content_hash != batch.content_hash
    assert canonicalize("insider_trades", [], source_as_of=date(2026, 9, 30)).content_hash == dated.content_hash


def test_row_order_is_canonical(fixture_path):
    raw = records(fixture_path, "insider_trades")
    a = canonicalize("insider_trades", validate_records("insider_trades", raw))
    b = canonicalize("insider_trades", validate_records("insider_trades", list(reversed(raw))))
    assert [r.canonical() for r in a.records] == [r.canonical() for r in b.records]


# ── payload envelope ─────────────────────────────────────

def test_envelope_is_allowlisted_and_sanitized(fixture_path):
    env = load(fixture_path, "congress_trades")
    env.pop("trades")
    env["endpoints"].append(f"https://x.invalid/v1?apikey={SECRET}")
    env["note"] = SECRET
    env["unexpected"] = {"token": SECRET}
    clean = sanitize_envelope("congress_trades", env)
    assert set(clean) <= {"source", "endpoints", "fetched_at", "total_trades"}
    assert SECRET not in json.dumps(clean)
    si = sanitize_envelope("short_interest", {"errors": [f"boom {SECRET}"], "source": "nasdaq"})
    assert si == {"source": "nasdaq", "errors_count": 1}


def test_payload_keeps_history_and_envelope(fixture_path):
    data = load(fixture_path, "short_interest")
    batch = canonicalize("short_interest", validate_records("short_interest", data["records"]),
                         envelope={k: v for k, v in data.items() if k != "records"})
    assert batch.payload["records"][0]["history"][1]["settlement_date"] == "2026-08-29"
    assert batch.payload["envelope"]["schema_version"] == "1.0"
    assert "fetched_at" not in json.dumps(batch.payload["records"])


# ── redaction and diagnostics ────────────────────────────

@pytest.mark.parametrize("text", [
    f"https://h.invalid/p?apikey={SECRET}",
    f"https://h.invalid?credential={SECRET}",
    f"https://h.invalid#{SECRET}",
    f"https://user:{SECRET}@h.invalid/x",
    f'{{"apikey": "{SECRET}"}}',
    f"token={SECRET}",
    f"Authorization: Bearer {SECRET}",
])
def test_redact(text):
    assert SECRET not in redact(text)


def test_safe_diagnostics_tolerates_malformed_shapes():
    for bad in ({"unit_kind": []}, {"errors": [{"type": {}}]}, {"errors": [{"field": []}]},
                {"failures": [{"unit": {}, "code": []}]}, {"exception": {"name": {}}},
                {"attempted": 10**30}, {"errors": "x"}, [], "x"):
        assert SECRET not in json.dumps(safe_diagnostics(bad))
    assert safe_diagnostics({"exception": {"name": SECRET}}) == {"exception": {"name": "Exception"}}
    assert safe_diagnostics({"exception": {"name": "IntegrityError", "sqlstate": "23505"}}) == {
        "exception": {"name": "IntegrityError", "sqlstate": "23505"}}


def test_safe_diagnostics_is_closed():
    diag = safe_diagnostics({
        "input_count": 3,
        "authorization": SECRET,
        "errors": [{"record": 1, "field": SECRET, "type": SECRET, "input": SECRET},
                   {"record": 2, "field": "history.0.days_to_cover", "type": "value_error"}],
        "failures": [{"unit": f"x?{SECRET}", "code": SECRET}, {"unit": "ZZAA", "code": "http_503"},
                     {"unit": "senate page 1", "code": "request_error:ConnectionError"}],
        "exception": {"name": "OperationalError", "sqlstate": "08006", "message": SECRET},
        "unit_kind": SECRET,
    })
    blob = json.dumps(diag)
    assert SECRET not in blob
    assert diag["errors"][1] == {"record": 2, "field": "history.0.days_to_cover", "type": "value_error"}
    assert diag["failures"][1:] == [{"unit": "ZZAA", "code": "http_503"},
                                    {"unit": "senate page 1", "code": "request_error:ConnectionError"}]
    assert SECRET not in render_summary("partial_collection", diag)


# ── file entry point attestation ─────────────────────────

def test_file_mode_requires_attestation(fixture_path, tmp_path):
    path = fixture_path("ark_holdings.json")
    assert read_fetcher_file("ark_holdings", path, mode="fixture").succeeded
    unattested = read_fetcher_file("ark_holdings", path, mode="file")
    assert not unattested.succeeded and unattested.error_code == "collection_unattested"
    assert read_fetcher_file("ark_holdings", path, mode="file", attest_complete=True).succeeded

    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps({"holdings": []}))
    assert read_fetcher_file("ark_holdings", empty, mode="fixture").error_code == "empty_collection_unattested"
    assert read_fetcher_file("ark_holdings", empty, mode="fixture", attest_complete=True).succeeded

    si = load(fixture_path, "short_interest")
    si["errors"] = [f"request failed {SECRET}"]
    bad = tmp_path / "si.json"
    bad.write_text(json.dumps(si))
    outcome = read_fetcher_file("short_interest", bad, mode="file", attest_complete=True)
    assert outcome.error_code == "collection_reported_errors"

    big = tmp_path / "big.json"
    big.write_text(json.dumps({"holdings": [{"ticker": "ZZAA"}] * 10}))
    from src.ingestion.files import read_json_bounded

    assert read_json_bounded(big, max_bytes=20) == (None, "input_too_large")
    link = tmp_path / "link.json"
    link.symlink_to(path)
    assert read_fetcher_file("ark_holdings", link, mode="fixture").error_code == "file_unreadable"
    fifo = tmp_path / "fifo.json"
    os.mkfifo(fifo)
    assert read_fetcher_file("ark_holdings", fifo, mode="fixture").error_code == "file_unreadable"
    assert read_fetcher_file("ark_holdings", tmp_path, mode="fixture").error_code == "file_unreadable"

    junk = tmp_path / "junk.json"
    junk.write_text("{" * 100000)
    assert read_fetcher_file("ark_holdings", junk, mode="fixture").error_code == "malformed_json"


def test_pinned_engine_overrides_pg_environment(monkeypatch):
    from sqlalchemy import event

    from tests.conftest import pinned_engine

    assert not [n for n in os.environ if n.startswith("PG")]  # scrubbed at conftest import
    monkeypatch.setenv("PGHOSTADDR", "192.0.2.123")  # documentation address; never contacted
    url = parse_database_url("postgresql+psycopg://u:p@localhost:55439/perennial_test")
    engine = pinned_engine(url)
    captured = {}

    @event.listens_for(engine, "do_connect")
    def capture(dialect, conn_rec, cargs, cparams):
        captured.update(cparams)
        raise ConnectionAbortedError("captured; no socket opened")

    with pytest.raises(Exception):
        engine.connect()
    # An explicit hostaddr parameter takes precedence over libpq's PGHOSTADDR default.
    assert captured["hostaddr"] == "127.0.0.1" and captured["host"] == "localhost"
    engine.dispose()
