"""Plain console lines for interactive scripts; Ruff T20 bans print() in src/.

stdout_logger() and stderr_logger() write each message exactly as given, with no
prefix or JSON, so a script's console output reads the same as with print().
The stream is looked up when a line is written, so redirection still works.
Pass extra={"end": ""} to leave the line open, e.g. for a progress counter that
redraws itself with "\\r".
"""

import logging
import sys


class _ConsoleHandler(logging.Handler):
    def __init__(self, stream_name: str) -> None:
        super().__init__()
        self.stream_name = stream_name

    def emit(self, record: logging.LogRecord) -> None:
        try:
            stream = getattr(sys, self.stream_name)
            stream.write(record.getMessage() + getattr(record, "end", "\n"))
            stream.flush()
        except Exception:
            self.handleError(record)


def _console(stream_name: str) -> logging.Logger:
    logger = logging.getLogger(f"perennial.console.{stream_name}")
    if not logger.handlers:
        logger.addHandler(_ConsoleHandler(stream_name))
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def stdout_logger() -> logging.Logger:
    return _console("stdout")


def stderr_logger() -> logging.Logger:
    return _console("stderr")
