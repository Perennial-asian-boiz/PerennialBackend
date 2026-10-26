"""FMP optional fields may be null (fixtures/providers/fmp/house.json has
"district": null). parse_trades used to call .strip() on them and the whole
Congress collection failed with parse_error."""

import json
from pathlib import Path

import pytest
from sqlalchemy import select

from src.db import models
from src.ingestion import collectors
from src.ingestion.canonical import canonicalize
from src.ingestion.importer import import_collection, validate_records

PROVIDERS = Path(__file__).resolve().parent / "fixtures" / "providers" / "fmp"
OPTIONAL = ("district", "link", "assetType", "assetDescription", "amount")


def fmp():
    return collectors._fetcher("fmp")


def item(**overrides):
    base = {
        "symbol": "ZZAA",
        "transactionDate": "2026-10-01",
        "disclosureDate": "2026-10-02",
        "firstName": "Synthetic",
        "lastName": "Person",
        "assetDescription": "Synthetic Company",
        "assetType": "Stock",
        "type": "Purchase",
        "amount": "$1,001 - $15,000",
        "district": "ZZ01",
        "link": "https://example.invalid/filing",
        "_chamber": "house",
    }
    base.update(overrides)
    return {k: v for k, v in base.items() if v is not ...}


@pytest.mark.parametrize("key", OPTIONAL)
@pytest.mark.parametrize("missing", [None, ...], ids=["null", "absent"])
def test_null_or_absent_optional_fields_parse_as_empty(key, missing):
    [trade] = fmp().parse_trades([item(**{key: missing})])
    field = {"district": "district", "link": "source_link", "assetType": "asset_type",
             "assetDescription": "asset_description", "amount": "amount_range"}[key]
    assert trade[field] == ""


def test_empty_and_null_store_and_hash_identically():
    from_null = fmp().parse_trades([item(district=None, link=None)])
    from_empty = fmp().parse_trades([item(district="", link="")])
    for trades in (from_null, from_empty):
        for t in trades:
            t.pop("fetched_at")
    [a] = validate_records("congress_trades", from_null)
    [b] = validate_records("congress_trades", from_empty)
    assert a.district is None and a.source_link is None
    assert canonicalize("congress_trades", [a]).content_hash == canonicalize(
        "congress_trades", [b]
    ).content_hash


def test_a_null_required_field_fails_validation_not_the_collection():
    trades = fmp().parse_trades([item(type=None)])  # no AttributeError
    with pytest.raises(Exception) as caught:
        validate_records("congress_trades", trades)
    assert getattr(caught.value, "code", None) == "validation_failed"


class _Response:
    def __init__(self, body):
        self.status_code = 200
        self._raw = json.dumps(body).encode()
        self.headers = {"Content-Type": "application/json", "Content-Length": str(len(self._raw))}
        self.ok = True

    def iter_content(self, chunk_size=1, decode_unicode=False):
        for i in range(0, len(self._raw), chunk_size or 1):
            yield self._raw[i : i + (chunk_size or 1)]

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class _FixtureSession:
    """Serves the P1-C FMP fixtures unmodified (house.json has "district": null)."""

    def get(self, url, params=None, **kwargs):
        chamber = url.rsplit("/", 1)[1].split("-")[0]
        first = int((params or {}).get("page", 0)) == 0
        body = json.loads((PROVIDERS / f"{chamber}.json").read_text()) if first else []
        return _Response(body)

    def close(self):
        pass


@pytest.mark.db
def test_house_fixture_with_null_district_collects_and_imports(engine):
    raw = json.loads((PROVIDERS / "house.json").read_text())
    assert any(row.get("district") is None for row in raw)  # the case under test

    outcome = collectors.collect_congress(
        _FixtureSession(), api_key="synthetic-key", sleep=lambda s: None
    )
    assert outcome.succeeded, outcome.error_code
    result = import_collection(engine, outcome)
    assert result.succeeded
    with engine.connect() as conn:
        districts = conn.execute(
            select(models.congress_trades.c.district).where(
                models.congress_trades.c.batch_id == result.batch_id
            )
        ).scalars().all()
    assert None in districts
