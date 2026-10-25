"""S1-12, the API side only: CS-4, CS-5 and the storage the API reads.

Spec: docs/work-items/s1-stories-plan.md (S1-12), on docs/s1-stories-plan @
25d2590: Q9 declined, so there's no rewrite, no summary_method and no method
field. Not ingestion (CS-1 to CS-3 and CS-7 wait on Q13) and not the page (CS-6,
Jaden's repo). Synthetic summaries only.

Readings this file pins (each is a contract choice for the API):
- The summary is security.summary, beside sector and industry (which also come
  from company_profiles), and the key is always present: an object or null.
- summary.as_of uses the route's existing timestamp form: UTC isoformat, +00:00.
- The API role may read the five columns it serves from (summary,
  summary_status, summary_source_name, summary_source_url, summary_as_of), but
  not summary_grade or description_digest, which it doesn't need.
- A summary is never served with a URL that isn't https: either the database
  refuses the row or the API serves summary: null.
"""

import json
import socket
import sys
from pathlib import Path

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from test_read_api import (  # noqa: F401  (api, groups: pytest fixtures)
    OPS,
    REPO,
    _denied,
    _router_app,
    _v1_openapi,
    api,
    as_role,
    canonical,
    client,
    groups,
    login,
    seed_security,
)

SUMMARY_COLUMNS = {
    "summary": {"text"},
    "summary_grade": {"numeric"},
    "summary_status": {"text", "character varying"},
    "summary_source_name": {"text", "character varying"},
    "summary_source_url": {"text", "character varying"},
    "summary_as_of": {"timestamp with time zone"},
    "description_digest": None,  # the SHA-256, in whatever form the migration picks
}
SERVED_COLUMNS = (
    "summary",
    "summary_status",
    "summary_source_name",
    "summary_source_url",
    "summary_as_of",
)
TEXT = (
    "Synthetic Widgets makes small parts for home tools. It sells them to stores in "
    "many states. The company was started in a small town."
)
STORED = {
    "summary": TEXT,
    "summary_grade": 5.2,
    "summary_status": "ok",
    "summary_source_name": "Synthetic Filings",
    "summary_source_url": "https://filings.example.com/zzsw/profile",
    "summary_as_of": "2026-10-20 12:00:00+00",
    "description_digest": "0" * 64,
}
SERVED = {
    "text": TEXT,
    "source": {
        "name": "Synthetic Filings",
        "url": "https://filings.example.com/zzsw/profile",
    },
    "as_of": "2026-10-20T12:00:00+00:00",
}


def with_summary(engine, symbol="ZZSW", **overrides):
    """A security whose profile carries a stored summary (raw SQL: the columns
    are the migration's, not yet the model's)."""
    values = {**STORED, **overrides}
    seed_security(
        engine, symbol, name="Synthetic Widgets", profile={"sector": "Industrials"}
    )
    assignments = ", ".join(f"{c} = :{c}" for c in values)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"UPDATE perennial.company_profiles SET {assignments} WHERE id ="
                " (SELECT company_profile_id FROM perennial.securities WHERE symbol = :symbol)"
            ),
            {**values, "symbol": symbol},
        )


def summary_of(api, symbol="ZZSW"):  # noqa: F811
    response = api.get(f"/v1/securities/{symbol}")
    assert response.status_code == 200, response.text
    return response, response.json()["security"]["summary"]


# ── storage: the columns the API reads, and its grant ────────


@pytest.mark.db
def test_company_profiles_has_the_summary_columns_all_nullable(engine):
    with engine.connect() as conn:
        found = {
            name: (data_type, nullable)
            for name, data_type, nullable in conn.execute(
                text(
                    "SELECT column_name, data_type, is_nullable FROM information_schema.columns"
                    " WHERE table_schema = 'perennial' AND table_name = 'company_profiles'"
                )
            )
        }
    assert "summary_method" not in found  # dropped with Q9 (25d2590)
    for column, types in SUMMARY_COLUMNS.items():
        assert column in found, column
        data_type, nullable = found[column]
        assert nullable == "YES", column
        if types is not None:
            assert data_type in types, (column, data_type)


@pytest.mark.db
@pytest.mark.parametrize("column, value", [("summary_status", "maybe")])
def test_status_takes_only_its_values(engine, column, value):
    seed_security(engine, "ZZCK", profile={"sector": "Industrials"})
    profile = (
        "(SELECT company_profile_id FROM perennial.securities WHERE symbol = 'ZZCK')"
    )
    with engine.begin() as conn:
        for good in {
            "summary_status": ("ok", "unavailable", None),
        }[column]:
            conn.execute(
                text(
                    f"UPDATE perennial.company_profiles SET {column} = :v"
                    f" WHERE id = {profile}"
                ),
                {"v": good},
            )
    with pytest.raises(DBAPIError), engine.begin() as conn:
        conn.execute(
            text(
                f"UPDATE perennial.company_profiles SET {column} = :v"
                f" WHERE id = {profile}"
            ),
            {"v": value},
        )


@pytest.mark.db
def test_the_api_grant_covers_the_served_summary_columns_only(engine, groups):  # noqa: F811
    sys.path.insert(0, str(OPS))
    from provision_roles import API_COLUMNS

    assert set(API_COLUMNS["company_profiles"]) == {
        "id",
        "sector",
        "industry",
        *SERVED_COLUMNS,
    }
    with as_role(engine, groups["api"]) as conn:
        conn.execute(
            text(
                f"SELECT {', '.join(SERVED_COLUMNS)} FROM perennial.company_profiles"
                " LIMIT 0"
            )
        )
        _denied(conn, "SELECT summary_grade FROM perennial.company_profiles")
        _denied(conn, "SELECT description_digest FROM perennial.company_profiles")


# ── CS-5: the contract ───────────────────────────────────────


@pytest.mark.db
def test_cs5_a_stored_summary_is_served_with_its_source(engine, api):  # noqa: F811
    with_summary(engine)
    response, summary = summary_of(api)
    assert summary == SERVED
    assert response.headers["cache-control"] == "no-cache"
    assert "etag" not in response.headers  # CS-5: none today
    assert response.content == canonical(response.content)
    for hidden in ("summary_grade", "description_digest", "0" * 64, "5.2"):
        assert hidden not in response.text


@pytest.mark.db
@pytest.mark.parametrize(
    "case",
    [
        "no_profile",
        "no_summary",
        "unavailable_with_text_left",
        "unavailable_without_text",
    ],
)
def test_cs5_summary_is_null_when_there_is_none_or_it_is_unavailable(engine, api, case):  # noqa: F811
    if case == "no_profile":
        seed_security(engine, "ZZSW", name="Synthetic Widgets")
    elif case == "no_summary":
        seed_security(
            engine, "ZZSW", name="Synthetic Widgets", profile={"sector": "Industrials"}
        )
    elif case == "unavailable_with_text_left":
        with_summary(engine, summary_status="unavailable")
    else:
        with_summary(
            engine,
            summary_status="unavailable",
            summary=None,
            summary_source_name=None,
            summary_source_url=None,
        )
    response, summary = summary_of(api)
    assert "summary" in response.json()["security"] and summary is None
    assert TEXT not in response.text


@pytest.mark.db
@pytest.mark.parametrize(
    "url",
    [
        "http://filings.example.com/zzsw",
        "javascript:alert(1)",
        "//filings.example.com/zzsw",
        "ftp://filings.example.com/zzsw",
        "https:filings.example.com",
        " https://filings.example.com/zzsw",
    ],
    ids=[
        "http",
        "javascript",
        "scheme_relative",
        "ftp",
        "no_authority",
        "leading_space",
    ],
)
def test_cs5_a_summary_is_never_served_with_a_non_https_url(engine, api, url):  # noqa: F811
    with engine.connect() as conn:  # a missing column must not pass as a refusal
        assert conn.execute(
            text(
                "SELECT 1 FROM information_schema.columns WHERE table_schema = 'perennial'"
                " AND table_name = 'company_profiles' AND column_name = 'summary_source_url'"
            )
        ).scalar(), "no summary_source_url column"
    try:
        with_summary(engine, summary_source_url=url)
    except DBAPIError as refused:
        assert refused.orig.sqlstate.startswith("23"), (
            refused.orig.sqlstate
        )  # a constraint
        return  # the database refuses it: as good
    response, summary = summary_of(api)
    assert summary is None, summary
    assert url.strip() not in response.text


@pytest.mark.db
def test_cs5_the_summary_key_is_on_every_security(engine, api):  # noqa: F811
    seed_security(engine, "ZZPL", name="Synthetic Plain")
    security = api.get("/v1/securities/ZZPL").json()["security"]
    assert "summary" in security and security["summary"] is None


@pytest.mark.db
def test_cs5_if_an_etag_is_ever_added_it_covers_summary_as_of(engine, api):  # noqa: F811
    with_summary(engine)
    first = api.get("/v1/securities/ZZSW")
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE perennial.company_profiles SET summary_as_of = summary_as_of"
                " + interval '1 day' WHERE summary IS NOT NULL"
            )
        )
    second = api.get("/v1/securities/ZZSW")
    assert second.json()["security"]["summary"]["as_of"] == "2026-10-21T12:00:00+00:00"
    if "etag" in first.headers or "etag" in second.headers:
        assert first.headers.get("etag") != second.headers.get("etag")


def _summary_schema():
    document = _v1_openapi(_router_app())
    security = document["schemas"]["Security"]["properties"]["summary"]
    options = security.get("anyOf", [security])
    assert {"type": "null"} in options, "summary must be nullable"
    (ref,) = [o for o in options if o != {"type": "null"}]
    name = ref["$ref"].rsplit("/", 1)[1] if "$ref" in ref else None
    return document, (document["schemas"][name] if name else ref)


def test_cs5_openapi_documents_the_summary_object():
    document, schema = _summary_schema()
    assert set(schema["properties"]) == {"text", "source", "as_of"}
    assert set(schema.get("required", ())) == {"text", "source", "as_of"}
    assert "rewritten" not in json.dumps(schema)
    source = schema["properties"]["source"]
    if "$ref" in source:
        source = document["schemas"][source["$ref"].rsplit("/", 1)[1]]
    assert set(source["properties"]) == {"name", "url"}
    assert "Security" in document["schemas"] and "summary" in document["schemas"][
        "Security"
    ].get("required", ()), "summary is always present (null when there is none)"


def test_cs5_the_snapshot_includes_the_summary():
    snapshot = json.loads(
        (Path(__file__).parent / "fixtures" / "api" / "openapi_v1.json").read_text()
    )
    assert "summary" in snapshot["schemas"]["Security"]["properties"]


def test_cs5_api_md_documents_the_summary():
    content = (REPO / "docs" / "technical-specs" / "API.md").read_text()
    for term in (
        '"summary"',
        '"source"',
        '"as_of"',
        "https",
        '"summary": null',
    ):
        assert term in content, term


# ── CS-4: no request-time work ───────────────────────────────


@pytest.mark.db
def test_cs4_the_route_reads_stored_columns_only(engine, groups, monkeypatch):  # noqa: F811
    """No provider or model call: no Python socket opens (libpq's own connection is
    C, so it isn't one), no HTTP client is used, no summary library is imported,
    and every statement is a read that takes the summary from company_profiles."""
    with_summary(engine)
    attempts = []

    def refuse(*args, **kwargs):
        attempts.append(args[:1])
        raise OSError("synthetic: no network at request time")

    with login(engine, groups) as (api_engine, _, _), client(api_engine, groups) as c:
        statements = []
        event.listen(
            api_engine,
            "before_cursor_execute",
            lambda conn, cursor, statement, *rest: statements.append(statement),
        )
        monkeypatch.setattr(socket, "create_connection", refuse)
        monkeypatch.setattr(socket.socket, "connect", refuse)
        import requests

        monkeypatch.setattr(requests.Session, "request", refuse)
        before = set(sys.modules)
        response = c.get("/v1/securities/ZZSW")
        loaded = set(sys.modules) - before
    assert (
        response.status_code == 200 and response.json()["security"]["summary"] == SERVED
    )
    assert attempts == []
    assert not {
        m
        for m in loaded
        if m.split(".")[0] in ("anthropic", "textstat", "openai", "httpx")
    }
    reads = [s for s in statements if "company_profiles" in s]
    assert reads and any("summary" in s for s in reads)
    for statement in statements:
        assert statement.lstrip().split(None, 1)[0].upper() in (
            "SELECT",
            "BEGIN",
            "SET",
            "SHOW",
        ), statement[:80]


def test_cs4_no_api_module_imports_a_model_or_readability_library():
    api_dir = REPO / "development" / "backend" / "src" / "api"
    for path in sorted(api_dir.rglob("*.py")):
        source = path.read_text()
        for name in ("anthropic", "textstat"):
            assert f"import {name}" not in source and f"from {name}" not in source, (
                path,
                name,
            )
