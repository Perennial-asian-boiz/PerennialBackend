"""JSON diagnostics with scoped correlation IDs and credential-free errors.

Configure once in the process entry point. Use constant event messages, never
request parameters or exception strings; log_exception() describes errors safely.
Context follows asyncio tasks; use copy_context() when starting worker threads.
"""

import json
import logging
import re
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.ingestion.redaction import describe_exception, redact

# Any scheme://... token; paths can carry secrets as well as queries.
_URL = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^\s'\"<>]+")

_context: ContextVar[dict[str, str | int | None]] = ContextVar(
    "log_context", default={}
)


@contextmanager
def log_context(**fields: str | int | None) -> Iterator[None]:
    """Bind run_id/source/publication_id, restoring outer values even on failure."""
    if fields.keys() - {"run_id", "source", "publication_id"}:
        raise ValueError("unsupported log context field")
    token = _context.set({**_context.get(), **fields})
    try:
        yield
    finally:
        _context.reset(token)


def _safe(text: str) -> str:
    """Drop whole URLs, then mask secret-looking pairs; logs never carry URLs."""
    return redact(_URL.sub("<url>", text))


class JsonFormatter(logging.Formatter):
    """Emit one JSON line; exclude arbitrary extras, tracebacks and request data."""

    def format(self, record: logging.LogRecord) -> str:
        context = _context.get()
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": _safe(record.getMessage()),
            **{
                key: _safe(value) if isinstance(value, str) else value
                for key in ("run_id", "source", "publication_id")
                for value in (context.get(key),)
            },
        }
        if record.exc_info and record.exc_info[1] is not None:
            payload["exception"] = describe_exception(record.exc_info[1])
        return json.dumps(payload)


class _StderrHandler(logging.StreamHandler[Any]):
    def emit(self, record: logging.LogRecord) -> None:
        # Resolve stderr at emission time, including embedded CLI invocations.
        self.stream = sys.stderr
        super().emit(record)


def configure_logging() -> None:
    """Install one JSON stderr handler, preserving handlers owned by the host."""
    root = logging.getLogger()
    if not any(isinstance(handler, _StderrHandler) for handler in root.handlers):
        handler = _StderrHandler()
        handler.setFormatter(JsonFormatter())
        root.addHandler(handler)
    root.setLevel(logging.INFO)


def log_exception(logger: logging.Logger, event: str, exc: BaseException) -> None:
    """Keep exception messages and tracebacks out of every handler's records."""
    logger.error("%s: %s", event, describe_exception(exc))


def create_logger(log_file: str) -> logging.Logger:
    """Compatibility for existing file-log callers; no filesystem work at import."""
    directory = Path(__file__).resolve().parents[2] / "logs"
    directory.mkdir(exist_ok=True)
    path = directory / Path(log_file).name
    logger = logging.getLogger(f"perennial.file.{path.stem}")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.FileHandler(path, encoding="utf-8")
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    return logger
