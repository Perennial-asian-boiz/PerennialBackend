"""Basic per-client rate limiting for the public API (M5 R1 addendum, RL-*).

Design: docs/work-items/m5-read-api-design-note.md, "Addendum (2026-10-24)".

- One token bucket per (kind, client key), kept in memory per app instance:
  ``read`` for R1's routes, ``auth`` for #22's signup and forgot-password, and
  (#22 auth hardening) ``credential``, ``session`` and ``refresh`` for the other
  account routes, and ``email``, each client's share of the app's email budget.
- Buckets are GCRA timestamps, so refill needs no background work. A bucket
  table holds at most ``max_clients`` keys per kind and evicts the least
  recently used, so rotating addresses can't grow memory.
- Clients are keyed from ``request.client`` as uvicorn's ProxyHeadersMiddleware
  leaves it; nothing here reads X-Forwarded-For.
- Rejections log once per key per Retry-After window, then a count, and at
  most 60 lines a minute in all. No line carries an address, path or header.
"""

import ipaddress
import logging
import math
import re
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Literal

logger = logging.getLogger(__name__)

Kind = Literal["read", "auth", "credential", "session", "refresh", "email"]
KINDS: tuple[Kind, ...] = ("read", "auth", "credential", "session", "refresh", "email")
LOG_LINES_PER_MINUTE = 60
_EPSILON = 1e-9  # float slack: a token due "exactly now" is available
_IPV4_PORT = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}):\d+")
_BRACKETED = re.compile(r"\[([^\]]+)\](?::\d+)?")
_POSITIVE = re.compile(r"[1-9][0-9]*")


class RateLimitConfigError(ValueError):
    """Invalid rate-limit settings. Fixed text: the offending value is never echoed."""

    code = "rate_limit_config_invalid"

    def __init__(self) -> None:
        super().__init__(f"{self.code}: the rate-limit settings are not valid")


def _trusted(entry: str) -> str:
    """One validated proxy entry, parsed exactly as uvicorn parses it (strictly)."""
    try:
        if "/" in entry:
            ipaddress.ip_network(entry)
        else:
            ipaddress.ip_address(entry)
    except ValueError:
        raise RateLimitConfigError() from None
    return entry


@dataclass(frozen=True)
class RateLimitConfig:
    """Limits and proxy trust (RL-11). Every limit is a positive integer.

    trusted_proxies lists every proxy hop in front of the API, by IP or CIDR.
    "*" is refused: trust_all_proxies is the only way to trust every peer.
    """

    burst: int = 30
    per_minute: int = 60
    auth_burst: int = 5
    auth_per_hour: int = 30
    # #22 auth hardening (decisions 39-44, L6): the other account routes.
    credential_burst: int = 10
    credential_per_hour: int = 60
    session_burst: int = 30
    session_per_minute: int = 60
    refresh_burst: int = 10
    refresh_per_hour: int = 60
    # Keep the API-side email ceiling aligned with this project's 60/hour
    # Supabase SMTP limit. Flow and client ceilings may not exceed the total.
    email_budget_per_hour: int = 60
    email_budget_recovery_per_hour: int = 60
    email_budget_signup_per_hour: int = 60
    email_budget_per_client_per_hour: int = 60
    max_clients: int = 10000
    trusted_proxies: tuple[str, ...] = ()
    trust_all_proxies: bool = False
    clock: Callable[[], float] = field(
        default=time.monotonic, compare=False, repr=False
    )

    def __post_init__(self) -> None:
        limits = (
            self.burst,
            self.per_minute,
            self.auth_burst,
            self.auth_per_hour,
            self.credential_burst,
            self.credential_per_hour,
            self.session_burst,
            self.session_per_minute,
            self.refresh_burst,
            self.refresh_per_hour,
            self.email_budget_per_hour,
            self.email_budget_recovery_per_hour,
            self.email_budget_signup_per_hour,
            self.email_budget_per_client_per_hour,
            self.max_clients,
        )
        if any(type(v) is not int or v <= 0 for v in limits):
            raise RateLimitConfigError()
        total = self.email_budget_per_hour
        # Keep all subordinate budgets at or below the app-wide ceiling.
        if (
            self.email_budget_recovery_per_hour > total
            or self.email_budget_signup_per_hour > total
            or self.email_budget_per_client_per_hour > total
        ):
            raise RateLimitConfigError()
        if not isinstance(self.trusted_proxies, tuple) or not isinstance(
            self.trust_all_proxies, bool
        ):
            raise RateLimitConfigError()
        for entry in self.trusted_proxies:
            if not isinstance(entry, str):
                raise RateLimitConfigError()
            _trusted(entry)

    @classmethod
    def from_environ(cls, environ: Mapping[str, str]) -> "RateLimitConfig":
        """API_* settings; unset means the default, anything invalid refuses."""

        def limit(name: str, default: int) -> int:
            raw = environ.get(name)
            if raw is None:
                return default
            if not _POSITIVE.fullmatch(raw.strip()):
                raise RateLimitConfigError()
            return int(raw.strip())

        raw_proxies = environ.get("API_TRUSTED_PROXIES", "")
        proxies = tuple(p.strip() for p in raw_proxies.split(",") if p.strip())
        trust_all = environ.get("API_TRUST_ALL_PROXIES", "").strip()
        if trust_all not in ("", "0", "1"):
            raise RateLimitConfigError()
        return cls(
            burst=limit("API_RATE_LIMIT_BURST", cls.burst),
            per_minute=limit("API_RATE_LIMIT_PER_MINUTE", cls.per_minute),
            auth_burst=limit("API_AUTH_RATE_LIMIT_BURST", cls.auth_burst),
            auth_per_hour=limit("API_AUTH_RATE_LIMIT_PER_HOUR", cls.auth_per_hour),
            credential_burst=limit(
                "API_CREDENTIAL_RATE_LIMIT_BURST", cls.credential_burst
            ),
            credential_per_hour=limit(
                "API_CREDENTIAL_RATE_LIMIT_PER_HOUR", cls.credential_per_hour
            ),
            session_burst=limit("API_SESSION_RATE_LIMIT_BURST", cls.session_burst),
            session_per_minute=limit(
                "API_SESSION_RATE_LIMIT_PER_MINUTE", cls.session_per_minute
            ),
            refresh_burst=limit("API_REFRESH_RATE_LIMIT_BURST", cls.refresh_burst),
            refresh_per_hour=limit(
                "API_REFRESH_RATE_LIMIT_PER_HOUR", cls.refresh_per_hour
            ),
            email_budget_per_hour=limit(
                "API_EMAIL_BUDGET_PER_HOUR", cls.email_budget_per_hour
            ),
            email_budget_recovery_per_hour=limit(
                "API_EMAIL_BUDGET_RECOVERY_PER_HOUR", cls.email_budget_recovery_per_hour
            ),
            email_budget_signup_per_hour=limit(
                "API_EMAIL_BUDGET_SIGNUP_PER_HOUR", cls.email_budget_signup_per_hour
            ),
            email_budget_per_client_per_hour=limit(
                "API_EMAIL_BUDGET_PER_CLIENT_PER_HOUR",
                cls.email_budget_per_client_per_hour,
            ),
            max_clients=limit("API_RATE_LIMIT_MAX_CLIENTS", cls.max_clients),
            trusted_proxies=proxies,
            trust_all_proxies=trust_all == "1",
        )

    def trusted_hosts(self) -> list[str]:
        """ProxyHeadersMiddleware's trusted_hosts: empty trusts no one."""
        return ["*"] if self.trust_all_proxies else list(self.trusted_proxies)


def _ip_key(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str:
    if isinstance(address, ipaddress.IPv6Address):
        if address.ipv4_mapped is not None:  # a dual-stack listener's IPv4 client
            return f"ip:{address.ipv4_mapped}"
        prefix = int(address) >> 64 << 64  # one host commonly holds a whole /64
        return f"net:{ipaddress.IPv6Network((prefix, 64))}"
    return f"ip:{address}"


def client_key(host: str | None) -> str:
    """The bucket key for request.client's host, normalized (RL-6, RL-6b).

    Parse first; only if that fails strip "[v6]", "[v6]:port" or "a.b.c.d:port"
    and parse again. A bare IPv6 address is never stripped.
    """
    if host is None:
        return "unknown"
    for candidate in (host, _stripped(host)):
        if candidate is None:
            continue
        try:
            return _ip_key(ipaddress.ip_address(candidate))
        except ValueError:
            continue
    return f"host:{host[:64]}"


def _stripped(host: str) -> str | None:
    if match := _BRACKETED.fullmatch(host):
        return match.group(1)
    if match := _IPV4_PORT.fullmatch(host):
        return match.group(1)
    return None


@dataclass
class _Bucket:
    tat: float  # GCRA theoretical arrival time
    log_until: float = -math.inf  # end of the current per-key log window
    held: int = 0  # rejections not logged since the last line


class RateLimiter:
    """Thread-safe token buckets for one app instance."""

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._clock = config.clock
        self._lock = threading.Lock()
        self._buckets: dict[Kind, OrderedDict[str, _Bucket]] = {
            kind: OrderedDict() for kind in KINDS
        }
        # (interval seconds, burst) per kind
        share = config.email_budget_per_client_per_hour
        self._limits: dict[Kind, tuple[float, int]] = {
            "read": (60 / config.per_minute, config.burst),
            "auth": (3600 / config.auth_per_hour, config.auth_burst),
            "credential": (3600 / config.credential_per_hour, config.credential_burst),
            "session": (60 / config.session_per_minute, config.session_burst),
            "refresh": (3600 / config.refresh_per_hour, config.refresh_burst),
            "email": (3600 / share, share),
        }
        self._log_window_start = -math.inf
        self._log_lines = 0
        self._log_suppressed = 0
        # event -> (start of its one-line-a-minute window, events since the line)
        self._counted: dict[str, tuple[float, int]] = {}

    def size(self, kind: Kind) -> int:
        with self._lock:
            return len(self._buckets[kind])

    def check(self, kind: Kind, key: str) -> int | None:
        """Spend one token: None if allowed, else the whole seconds to wait (>= 1)."""
        interval, burst = self._limits[kind]
        lines: list[tuple[str, tuple[object, ...]]] = []
        with self._lock:
            now = self._clock()
            buckets = self._buckets[kind]
            bucket = buckets.get(key)
            if bucket is None:
                bucket = buckets[key] = _Bucket(tat=now)
                if len(buckets) > self.config.max_clients:
                    buckets.popitem(last=False)  # the least recently used
            else:
                buckets.move_to_end(key)
            arrival = max(bucket.tat, now) + interval
            excess = arrival - now - burst * interval
            if excess <= _EPSILON:
                bucket.tat = arrival
                return None
            retry_after = max(1, math.ceil(excess - _EPSILON))
            if now < bucket.log_until:
                bucket.held += 1
            else:
                lines = self._log_line(kind, bucket.held, now)
                bucket.held, bucket.log_until = 0, now + retry_after
        for message, args in lines:  # logged outside the lock
            logger.warning(message, *args)
        return retry_after

    def refund(self, kind: Kind, key: str) -> None:
        """Give back one token (AH-9f): never past the present, so never above full."""
        interval, _ = self._limits[kind]
        with self._lock:
            bucket = self._buckets[kind].get(key)
            if bucket is not None:
                bucket.tat = max(bucket.tat - interval, self._clock())

    def log_event(self, message: str, *args: object) -> None:
        """One fixed-text line, under RL-13's global cap (AH-5a)."""
        with self._lock:
            lines = self._capped(message, args, self._clock())
        for line, line_args in lines:
            logger.warning(line, *line_args)

    def count_event(self, event: str) -> None:
        """Counted, not logged one by one (AH-9h): at most one line a minute per
        event, carrying how many happened, under RL-13's global cap."""
        lines: list[tuple[str, tuple[object, ...]]] = []
        with self._lock:
            now = self._clock()
            start, count = self._counted.get(event, (-math.inf, 0))
            count += 1
            if now - start >= 60:
                lines = self._capped("%s count=%d", (event, count), now)
                start, count = now, 0
            self._counted[event] = (start, count)
        for line, line_args in lines:
            logger.warning(line, *line_args)

    def _log_line(
        self, kind: Kind, held: int, now: float
    ) -> list[tuple[str, tuple[object, ...]]]:
        """Lines to write, under the global cap. The caller holds the lock."""
        return self._capped("rate_limited kind=%s held_back=%d", (kind, held), now)

    def _capped(
        self, message: str, args: tuple[object, ...], now: float
    ) -> list[tuple[str, tuple[object, ...]]]:
        """At most 60 lines a minute in all, then one suppressed-count line."""
        lines: list[tuple[str, tuple[object, ...]]] = []
        if now - self._log_window_start >= 60:
            if self._log_suppressed:
                lines.append(
                    ("rate_limited_suppressed lines=%d", (self._log_suppressed,))
                )
            self._log_window_start, self._log_lines, self._log_suppressed = now, 0, 0
        if self._log_lines < LOG_LINES_PER_MINUTE:
            self._log_lines += 1
            lines.append((message, args))
        else:
            self._log_suppressed += 1
        return lines
