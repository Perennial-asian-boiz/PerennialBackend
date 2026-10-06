"""
Credential-free error text.

Stored error summaries and CLI output are built from error codes and
exception class names wherever possible. When free text is unavoidable it
passes through redact(), which strips URL credentials and query strings and
masks secret-looking key/value pairs, then truncates.
"""

import re

_URL = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^\s'\"<>]+")
_SECRET_PAIR = re.compile(
    r"(?i)\b(api[_-]?key|apikey|token|access[_-]?token|secret|password|passwd|pwd|"
    r"authorization|signature|sig|key)\b(['\"]?\s*[=:]\s*['\"]?(?:bearer\s+)?)([^\s&'\",;}]+)"
)
_BEARER = re.compile(r"(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]+")


def _strip_url(match: "re.Match[str]") -> str:
    """Keep scheme, host and path; drop userinfo, query and fragment."""
    url = match.group(0)
    scheme, _, rest = url.partition("://")
    # The authority ends at the first '/', '?' or '#', whichever comes first.
    end = min((i for i in (rest.find(c) for c in "/?#") if i >= 0), default=len(rest))
    authority, tail = rest[:end], rest[end:]
    if "@" in authority:
        authority = "***@" + authority.rsplit("@", 1)[1]
    path = tail.split("?", 1)[0].split("#", 1)[0]
    return f"{scheme}://{authority}{path}"


def redact(text: object, limit: int = 300) -> str:
    out = _URL.sub(_strip_url, str(text))
    out = _SECRET_PAIR.sub(lambda m: f"{m.group(1)}{m.group(2)}***", out)
    out = _BEARER.sub(lambda m: f"{m.group(1)}***", out)
    out = " ".join(out.split())
    if len(out) > limit:
        out = out[: limit - 3] + "..."
    return out


def describe_exception(exc: BaseException) -> str:
    """Class name plus SQLSTATE when available; never the exception message."""
    name = type(exc).__name__
    orig = getattr(exc, "orig", None)
    sqlstate = getattr(orig, "sqlstate", None) or getattr(exc, "sqlstate", None)
    if sqlstate:
        return f"{name} (SQLSTATE {sqlstate})"
    return name
