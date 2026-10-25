"""S1-11: US equity market status (``GET /v1/market-status``).

Spec: docs/work-items/s1-stories-plan.md (S1-11). The NYSE calendar (XNYS) comes
from exchange_calendars (Q8): holidays and early closes are the library's, never
the hand-kept holiday list in settings. Keep its pin current: unscheduled closures
arrive only in library releases. No database and no request input: the answer depends on
``now()`` alone, computed in UTC and shown in New York time, so the process time
zone has no say (MS-4).
"""

import functools
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import exchange_calendars

EXCHANGE = "XNYS"
ET = ZoneInfo("America/New_York")
REGULAR_CLOSE_HOUR = 16
MAX_AGE_SECONDS = 15


def now() -> datetime:
    """The current time, aware, in UTC (the tests' clock seam)."""
    return datetime.now(timezone.utc)


@functools.lru_cache(maxsize=1)
def _calendar(built_on: date) -> Any:
    """XNYS from about 20 years back to a year past ``built_on``, rebuilt daily
    so a long-running process never reaches the end of its calendar."""
    with warnings.catch_warnings():
        # The library's own pandas 3.1 deprecation; nothing for this module to fix.
        warnings.filterwarnings(
            "ignore", "Series.values returning an ndarray", DeprecationWarning
        )
        return exchange_calendars.get_calendar(
            EXCHANGE, end=built_on + timedelta(days=366)
        )


@dataclass(frozen=True)
class Session:
    open: datetime
    close: datetime

    @property
    def early_close(self) -> bool:
        return self.close.astimezone(ET).hour < REGULAR_CLOSE_HOUR


def _session(calendar: Any, label: Any) -> Session:
    return Session(
        open=calendar.session_open(label).to_pydatetime(),
        close=calendar.session_close(label).to_pydatetime(),
    )


def _clock(moment: datetime) -> str:
    """9:30 a.m., 1:00 p.m.: the note's wording for a New York time."""
    local = moment.astimezone(ET)
    hour = local.hour % 12 or 12
    return f"{hour}:{local.minute:02d} {'a.m.' if local.hour < 12 else 'p.m.'}"


def _note(session: Session) -> str:
    early = " (early close)" if session.early_close else ""
    return (
        f"Regular hours, {_clock(session.open)}–{_clock(session.close)} ET{early}."
        " Some brokers allow extended-hours trading."
    )


def _iso_et(moment: datetime) -> str:
    return moment.astimezone(ET).isoformat(timespec="seconds")


def status(at: datetime) -> tuple[dict[str, Any], int]:
    """The status at ``at`` (aware) and its Cache-Control max-age in seconds.

    A session is open from its open (inclusive) to its close (exclusive).
    ``early_close`` and ``note`` describe the session ``next_close`` belongs to.
    """
    at = at.astimezone(timezone.utc)
    calendar = _calendar(datetime.now(timezone.utc).date())
    today = at.astimezone(ET).date()
    if calendar.is_session(today):
        label = calendar.date_to_session(today)
        current = _session(calendar, label)
        if at >= current.close:
            label = calendar.next_session(label)
    else:
        label = calendar.date_to_session(today, direction="next")
    session = _session(calendar, label)
    is_open = session.open <= at < session.close
    if is_open:
        next_open = _session(calendar, calendar.next_session(label)).open
        transition = session.close
    else:
        next_open = session.open
        transition = session.open
    body = {
        "exchange": EXCHANGE,
        "is_open": is_open,
        "session": "regular" if is_open else "closed",
        "label": "Market Open" if is_open else "Market Closed",
        "note": _note(session),
        "as_of": at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "next_open": _iso_et(next_open),
        "next_close": _iso_et(session.close),
        "early_close": session.early_close,
    }
    # Whole seconds to the next open or close, so a cached answer never spans one.
    remaining = (transition - at) // timedelta(seconds=1)
    return body, max(1, min(MAX_AGE_SECONDS, remaining))
