"""
Closed vocabulary for what an ingestion run may record about itself.

Everything persisted in ingestion_runs.error_code / error_summary /
diagnostics passes through this module:

  * error_code must be one of ERROR_SUMMARIES; anything else becomes
    "collection_failed".
  * error_summary is rendered here from the code's fixed template plus
    sanitized diagnostics. Text supplied by callers is never stored.
  * diagnostics keep only: known counts (<= 1e9); a known unit kind; validation errors
    as {record index, field path built from known model fields, pydantic
    error type}; collection failures as {unit shaped like a ticker / fund /
    "<chamber> page N", code from a fixed set}; an exception class name from
    the builtin/SQLAlchemy/psycopg/requests vocabulary (else "Exception") and
    a SQLSTATE. Values outside these sets are replaced by "?" or dropped.

So no input value, payload text, URL or exception message can reach the
database through a run record. The residual is that a unit is a ticker-shaped
string (<= 20 chars of A-Z0-9./-) taken from the collection plan.
"""

import json
import re
import typing
from typing import Any, Dict, List, Optional

import requests
from pydantic_core import core_schema

from src.ingestion.schemas import RECORD_MODELS, ShortInterestHistory

MAX_ENTRIES = 20
MAX_BYTES = 8192  # half the column CHECK; jsonb::text renders larger than json.dumps

ERROR_SUMMARIES = {
    "worker_abandoned": "Worker heartbeat expired.",
    "collection_failed": "collection did not complete",
    "partial_collection": "collection was incomplete; some units failed",
    "missing_credential": "a required provider credential is not configured",
    "no_input_tickers": "no candidate tickers available (upstream fetcher files missing or empty)",
    "parse_error": "fetcher output could not be parsed",
    "file_unreadable": "input file does not exist or cannot be read",
    "input_too_large": "input exceeds the size or record limit",
    "malformed_json": "input is not valid JSON",
    "malformed_input": "input does not have the expected structure",
    "collection_reported_errors": "input reports collection errors",
    "empty_collection_unattested": "empty input imported without --attest-complete",
    "collection_unattested": "fetcher output file imported without --attest-complete",
    "validation_failed": "one or more records failed validation",
    "validation_error": "input could not be validated",
    "security_conflict": "a ticker conflicts with an existing or in-batch security mapping",
    "missing_upstream": "upstream rows or fetcher files are missing or malformed",
    "plan_too_large": "too many tickers planned; collection refused rather than truncated",
    "database_error": "database write failed",
    "internal_error": "unexpected error; nothing was written",
}

UNIT_KINDS = frozenset({
    "ARK funds", "Congress requests", "insider tickers", "short-interest tickers", "upstream files",
})
_COUNT_KEYS = ("input_count", "error_count", "planned", "attempted", "completed", "failed", "not_attempted")

_OWN_ERROR_TYPES = {"not_an_object", "duplicate_security", "conflicting_exchange"}
VALIDATION_ERROR_TYPES = frozenset(typing.get_args(core_schema.ErrorType)) | _OWN_ERROR_TYPES

FAILURE_CODES = frozenset({
    "timeout", "invalid_json", "response_too_large", "empty_or_malformed_holdings",
    "malformed_holding", "malformed_response", "exchange_conflict", "currency_conflict",
    "parse_error", "missing_file", "unreadable_file", "malformed_file", "file_too_large",
    "pagination_exhausted", "deadline_exceeded",
})
_REQUEST_ERRORS = frozenset(
    name for name, obj in vars(requests.exceptions).items()
    if isinstance(obj, type) and issubclass(obj, Exception)
)
_HTTP_CODE = re.compile(r"^http_[1-5][0-9]{2}$")
_UNIT = re.compile(r"^(?:[A-Z0-9][A-Z0-9./-]{0,19}|(?:senate|house) page [0-9]{1,2}|senate watcher|trades_congress\.json|ark_holdings\.json)$")
MAX_COUNT = 10**9


def _exception_classes() -> frozenset:
    """Class names that may be stored: builtins, SQLAlchemy, psycopg, requests. Others -> 'Exception'."""
    import builtins

    import psycopg
    import sqlalchemy.exc

    names = set()
    for module in (builtins, sqlalchemy.exc, psycopg, psycopg.errors, requests.exceptions):
        for name, obj in vars(module).items():
            if isinstance(obj, type) and issubclass(obj, BaseException) and not name.startswith("_"):
                names.add(name)
    return frozenset(names)


EXCEPTION_CLASSES = _exception_classes()
_SQLSTATE = re.compile(r"^[0-9A-Z]{5}$")


def _field_names() -> frozenset:
    names = set()
    for model in list(RECORD_MODELS.values()) + [ShortInterestHistory]:
        for name, info in model.model_fields.items():
            names.add(name)
            if info.alias:
                names.add(info.alias)
    return frozenset(names)


FIELD_NAMES = _field_names()


def _member(value: Any, allowed) -> bool:
    return isinstance(value, str) and value in allowed


def safe_code(value: Any) -> str:
    return value if _member(value, ERROR_SUMMARIES) else "collection_failed"


def failure_code(value: Any) -> str:
    if not isinstance(value, str):
        return "failed"
    if value in FAILURE_CODES or _HTTP_CODE.match(value):
        return value
    kind, _, name = value.partition(":")
    if kind == "request_error" and name in _REQUEST_ERRORS:
        return value
    return "failed"


def _field_path(value: Any) -> str:
    if not isinstance(value, str) or len(value) > 120:
        return "?"
    if value == "":
        return ""
    parts = value.split(".")
    if all(p.isdigit() and len(p) <= 6 or p in FIELD_NAMES for p in parts):
        return value
    return "?"


def _count(value: Any) -> Optional[int]:
    ok = isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= MAX_COUNT
    return value if ok else None


def safe_diagnostics(diag: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(diag, dict):
        return None
    out: Dict[str, Any] = {}
    for key in _COUNT_KEYS:
        count = _count(diag.get(key))
        if count is not None:
            out[key] = count
    if _member(diag.get("unit_kind"), UNIT_KINDS):
        out["unit_kind"] = diag["unit_kind"]
    truncated = diag.get("truncated") is True
    errors = diag.get("errors")
    if isinstance(errors, list):
        truncated |= len(errors) > MAX_ENTRIES
        out["errors"] = [
            {
                "record": _count(e.get("record")),
                "field": _field_path(e.get("field")),
                "type": e["type"] if _member(e.get("type"), VALIDATION_ERROR_TYPES) else "invalid",
            }
            for e in errors[:MAX_ENTRIES] if isinstance(e, dict)
        ]
    failures = diag.get("failures")
    if isinstance(failures, list):
        truncated |= len(failures) > MAX_ENTRIES
        out["failures"] = [
            {
                "unit": f["unit"] if isinstance(f.get("unit"), str) and _UNIT.match(f["unit"]) else "?",
                "code": failure_code(f.get("code")),
            }
            for f in failures[:MAX_ENTRIES] if isinstance(f, dict)
        ]
    exc = diag.get("exception")
    if isinstance(exc, dict) and "name" in exc:
        out["exception"] = {"name": exc["name"] if _member(exc["name"], EXCEPTION_CLASSES) else "Exception"}
        if isinstance(exc.get("sqlstate"), str) and _SQLSTATE.match(exc["sqlstate"]):
            out["exception"]["sqlstate"] = exc["sqlstate"]
    if truncated:
        out["truncated"] = True
    if len(json.dumps(out)) > MAX_BYTES:
        return {"truncated": True}
    return out or None


def exception_diagnostics(exc: BaseException) -> Dict[str, Any]:
    orig = getattr(exc, "orig", None)
    return {"exception": {
        "name": type(exc).__name__,
        "sqlstate": getattr(orig, "sqlstate", None) or getattr(exc, "sqlstate", None),
    }}


def render_summary(code: str, diag: Optional[Dict[str, Any]]) -> str:
    """Fixed template for `code` plus details taken only from sanitized diagnostics."""
    parts: List[str] = [ERROR_SUMMARIES[code]]
    diag = diag or {}
    errors = diag.get("errors") or []
    if errors:
        first = errors[0]
        parts.append(
            f"{diag.get('error_count', len(errors))} error(s) in {diag.get('input_count', '?')} "
            f"record(s); first: record {first['record']} field '{first['field']}' ({first['type']})"
        )
    failures = diag.get("failures") or []
    if failures:
        shown = ", ".join(f"{f['unit']} ({f['code']})" for f in failures[:5])
        if "attempted" in diag:
            parts.append(
                f"{diag.get('failed', len(failures))} of {diag['attempted']} "
                f"{diag.get('unit_kind', 'units')} failed: {shown}"
            )
        else:
            parts.append(shown)
    exc = diag.get("exception")
    if exc:
        parts.append(exc["name"] + (f" (SQLSTATE {exc['sqlstate']})" if "sqlstate" in exc else ""))
    return "; ".join(parts)
