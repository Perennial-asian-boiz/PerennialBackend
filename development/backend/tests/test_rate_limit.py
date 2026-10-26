"""M5 R1 addendum: basic rate limiting for the public reads (RL-1 to RL-15b).

Spec: docs/work-items/m5-read-api-design-note.md, "Addendum (2026-10-24)".
The auth bucket (signup, forgot-password; RB-5d) and the integrated #22 app
(RL-15, the #22 half of RL-15b) are tested on #22's fix branch.

Requests go through raw ASGI calls so a test can set any peer (including none)
and any X-Forwarded-For chain; uvicorn's ProxyHeadersMiddleware, added by
read.install, does the proxy handling exactly as in production. A fake clock
drives refill. The API runs as a correctly shaped login (test_read_api).
"""

import asyncio
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest
from sqlalchemy import event
from test_read_api import (  # noqa: F401  (groups is a pytest fixture)
    _router_app,
    _v1_openapi,
    feed,
    groups,
    login,
    publish_run,
)

pytestmark = pytest.mark.db

PEER = ("198.51.100.1", 40000)
RATE_LIMITED = {"detail": "too many requests; retry later", "code": "rate_limited"}


class Clock:
    """A monotonic clock the test moves by hand."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def config(clock=None, **kwargs):
    from src.api.read import RateLimitConfig

    return RateLimitConfig(clock=clock or Clock(), **kwargs)


@contextmanager
def api_app(engine, groups, **limits):  # noqa: F811
    """create_app() as a correctly shaped login, with these rate limits."""
    from src.api.read import create_app

    with login(engine, groups) as (api_engine, _, _):
        rate_limit = limits.pop("rate_limit", None) or config(**limits)
        yield create_app(api_engine, api_role=groups["api"], rate_limit=rate_limit), api_engine


def call(app, path="/v1/watchlist", *, client=PEER, headers=()):
    """One GET through the ASGI app; returns (status, headers dict, body bytes)."""
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers],
        "client": client,
        "server": ("testserver", 80),
    }
    sent = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    asyncio.run(app(scope, receive, send))
    start = next(m for m in sent if m["type"] == "http.response.start")
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    hdrs = {k.decode().lower(): v.decode() for k, v in start["headers"]}
    return start["status"], hdrs, body


def drain(app, n, **kwargs):
    """n requests; returns their statuses."""
    return [call(app, **kwargs)[0] for _ in range(n)]


def limited(app, **kwargs):
    return call(app, **kwargs)[0] == 429


def shares_bucket(app, a, b, burst):
    """Drain client a, then ask whether client b is limited too."""
    assert 429 not in drain(app, burst, client=a)
    assert limited(app, client=a)
    return limited(app, client=b)


# ── RL-1, RL-2, RL-3: under, over and refill ────────────


def test_under_the_limit_responses_are_unchanged_and_each_app_starts_empty(engine, groups):  # noqa: F811
    with api_app(engine, groups, burst=2) as (app, _):
        assert drain(app, 2) == [404, 404]  # no publication: R1's normal 404
        assert limited(app)
    with api_app(engine, groups, burst=2) as (fresh, _):
        assert call(fresh)[0] == 404  # a new app instance has no buckets


def test_over_the_limit_is_a_canonical_429_without_cache_or_etag(engine, fixture_path, groups):  # noqa: F811
    publish_run(engine, feed(fixture_path))
    with api_app(engine, groups, burst=3) as (app, _):
        assert drain(app, 3) == [200, 200, 200]
        status, headers, body = call(app)
    assert status == 429
    assert json.loads(body) == RATE_LIMITED
    assert body == json.dumps(RATE_LIMITED, sort_keys=True, separators=(",", ":")).encode()
    assert re.fullmatch(r"[1-9][0-9]*", headers["retry-after"])
    assert headers["cache-control"] == "no-store"
    assert "etag" not in headers
    assert b"publication" not in body


def test_retry_after_is_whole_seconds_rounded_up_and_refill_restores_access(engine, groups):  # noqa: F811
    clock = Clock()
    with api_app(engine, groups, clock=clock, burst=1, per_minute=6) as (app, _):  # 1 per 10 s
        assert call(app)[0] == 404
        status, headers, _ = call(app)
        assert status == 429 and headers["retry-after"] == "10"
        clock.advance(5.5)
        status, headers, _ = call(app)
        assert status == 429 and headers["retry-after"] == "5"  # 4.5 s rounds up, not down
        clock.advance(4.4)
        status, headers, _ = call(app)
        assert status == 429 and headers["retry-after"] == "1"  # 0.1 s rounds up to 1
        clock.advance(0.1)
        assert call(app)[0] == 404
        for _ in range(50):  # at the refill rate, indefinitely
            clock.advance(10)
            assert call(app)[0] == 404


@pytest.mark.parametrize("burst, per_minute, retry_after", [(3, 1, "60"), (5, 120, "1")])
def test_burst_and_refill_set_through_settings_take_effect(
    engine, groups, monkeypatch, burst, per_minute, retry_after  # noqa: F811
):
    """Decision 33's condition: the limits are adjustable through configuration."""
    from src.api.read import create_app

    monkeypatch.setenv("API_RATE_LIMIT_BURST", str(burst))
    monkeypatch.setenv("API_RATE_LIMIT_PER_MINUTE", str(per_minute))
    with login(engine, groups) as (api_engine, _, _):
        app = create_app(api_engine, api_role=groups["api"])
        assert drain(app, burst) == [404] * burst  # the configured burst, exactly
        status, headers, _ = call(app)
        assert status == 429
        assert headers["retry-after"] == retry_after  # the configured refill rate


def test_max_clients_set_through_settings_takes_effect(engine, groups, monkeypatch):  # noqa: F811
    from src.api.read import create_app

    monkeypatch.setenv("API_RATE_LIMIT_BURST", "1")
    monkeypatch.setenv("API_RATE_LIMIT_MAX_CLIENTS", "1")
    with login(engine, groups) as (api_engine, _, _):
        app = create_app(api_engine, api_role=groups["api"])
        first = ("198.51.100.1", 1)
        assert call(app, client=first)[0] == 404 and limited(app, client=first)
        call(app, client=("198.51.100.2", 1))  # with room for one, this evicts the first
        assert call(app, client=first)[0] == 404


def test_default_limits_are_burst_30_refilling_60_a_minute():
    from src.api.read import RateLimitConfig

    defaults = RateLimitConfig()
    assert (defaults.burst, defaults.per_minute) == (30, 60)
    assert (defaults.auth_burst, defaults.auth_per_hour) == (5, 30)
    assert (
        defaults.email_budget_per_hour,
        defaults.email_budget_recovery_per_hour,
        defaults.email_budget_signup_per_hour,
        defaults.email_budget_per_client_per_hour,
    ) == (60, 60, 60, 60)
    assert defaults.max_clients == 10000
    assert defaults.trusted_proxies == () and defaults.trust_all_proxies is False


def test_default_email_client_share_allows_60_then_refills_once_per_minute():
    from src.api.rate_limit import RateLimitConfig, RateLimiter

    limiter = RateLimiter(RateLimitConfig())
    for _ in range(60):
        assert limiter.check("email", "test-client") is None
    assert limiter.check("email", "test-client") == 60


# ── RL-4, RL-5, RL-6, RL-6b: keying ─────────────────────


def test_two_clients_have_independent_buckets(engine, groups):  # noqa: F811
    with api_app(engine, groups, burst=2) as (app, _):
        assert not shares_bucket(app, ("198.51.100.1", 1), ("198.51.100.2", 1), 2)


@pytest.mark.parametrize(
    "a, b, shared",
    [
        ("2001:db8::1", "2001:db8::2", True),  # one /64, parsed as is (RL-D)
        ("2001:db8::1", "2001:db8:1::1", False),  # different /64s
        ("::ffff:198.51.100.1", "198.51.100.1", True),  # IPv4-mapped is its IPv4
        ("::ffff:198.51.100.1", "::ffff:198.51.100.2", False),  # never ::/64
        ("198.51.100.1:5678", "198.51.100.1:5679", True),  # ip:port strips the port
        ("198.51.100.1:5678", "198.51.100.1", True),
        ("[2001:db8::1]:443", "2001:db8::1", True),  # bracketed with a port
        ("[2001:db8::1]", "2001:db8::5", True),
        ("[2001:db8::1]:443", "2001:db8:1::1", False),
    ],
)
def test_addresses_are_normalized_before_keying(engine, groups, a, b, shared):  # noqa: F811
    with api_app(engine, groups, burst=2) as (app, _):
        assert shares_bucket(app, (a, 1), (b, 1), 2) is shared


def test_a_missing_peer_shares_one_unknown_bucket_and_is_never_exempt(engine, groups):  # noqa: F811
    with api_app(engine, groups, burst=2) as (app, _):
        assert drain(app, 2, client=None) == [404, 404]
        assert call(app, client=None)[0] == 429
        assert call(app, client=("198.51.100.9", 1))[0] == 404  # IPs are not "unknown"


def test_non_ip_hosts_are_literal_keys_truncated_to_64_characters(engine, groups):  # noqa: F811
    stem = "x" * 64
    with api_app(engine, groups, burst=2) as (app, _):
        assert shares_bucket(app, (stem + "-first", 1), (stem + "-second", 1), 2)
        assert not shares_bucket(app, ("testclient", 1), ("otherclient", 1), 2)


CLIENT, OTHER, OUTER, INNER = "198.51.100.7", "198.51.100.8", "10.0.0.1", "10.0.0.2"


def chain(client):
    """What the inner proxy forwards: client, then the outer proxy, appended in order."""
    return [("X-Forwarded-For", f"{client}, {OUTER}")]


def test_a_clients_own_forwarded_header_is_ignored_without_trusted_proxies(engine, groups):  # noqa: F811
    with api_app(engine, groups, burst=2) as (app, _):
        peer = ("203.0.113.9", 1)
        assert drain(app, 2, client=peer, headers=[("X-Forwarded-For", "6.6.6.6")]) == [404, 404]
        # Spoofing another address doesn't buy a new bucket.
        assert limited(app, client=peer, headers=[("X-Forwarded-For", "6.6.6.7")])


def test_with_every_hop_listed_clients_are_keyed_by_their_own_address(engine, groups):  # noqa: F811
    with api_app(engine, groups, burst=2, trusted_proxies=(OUTER, INNER)) as (app, _):
        inner = (INNER, 1)
        assert drain(app, 2, client=inner, headers=chain(CLIENT)) == [404, 404]
        assert limited(app, client=inner, headers=chain(CLIENT))
        assert call(app, client=inner, headers=chain(OTHER))[0] == 404
        # Entries a client prepends are skipped: the key is the rightmost untrusted one.
        spoofed = [("X-Forwarded-For", f"6.6.6.6, {CLIENT}, {OUTER}")]
        assert limited(app, client=inner, headers=spoofed)


def test_with_only_the_inner_hop_listed_every_client_collapses_onto_the_outer_hop(
    engine, groups  # noqa: F811
):
    """Documents the misconfiguration the setting must avoid: list every hop."""
    with api_app(engine, groups, burst=2, trusted_proxies=(INNER,)) as (app, _):
        inner = (INNER, 1)
        assert drain(app, 2, client=inner, headers=chain(CLIENT)) == [404, 404]
        assert limited(app, client=inner, headers=chain(OTHER))


def test_a_forwarded_header_from_an_untrusted_peer_is_ignored(engine, groups):  # noqa: F811
    with api_app(engine, groups, burst=2, trusted_proxies=(OUTER, INNER)) as (app, _):
        stranger = ("203.0.113.50", 1)
        assert drain(app, 2, client=stranger, headers=chain(CLIENT)) == [404, 404]
        assert limited(app, client=stranger, headers=chain(OTHER))
        assert call(app, client=(INNER, 1), headers=chain(CLIENT))[0] == 404


def test_trust_all_keys_by_the_first_forwarded_entry(engine, groups):  # noqa: F811
    """What API_TRUST_ALL_PROXIES does (the checker's probe): the client controls the key."""
    with api_app(engine, groups, burst=2, trust_all_proxies=True) as (app, _):
        peer = ("203.0.113.9", 1)
        first = [("X-Forwarded-For", "6.6.6.6, 203.0.113.9")]
        assert drain(app, 2, client=peer, headers=first) == [404, 404]
        assert limited(app, client=("192.0.2.1", 1), headers=[("X-Forwarded-For", "6.6.6.6")])
        assert call(app, client=peer, headers=[("X-Forwarded-For", "6.6.6.7, 203.0.113.9")])[0] == 404


# ── RL-7, RL-8, RL-9, RL-10, RL-14: cost and safety ─────


def test_a_limited_request_does_no_database_work(engine, fixture_path, groups, monkeypatch):  # noqa: F811
    from src.api import read

    publish_run(engine, feed(fixture_path))
    with api_app(engine, groups, burst=1) as (app, api_engine):
        assert call(app)[0] == 200
        helper_calls, statements = [], []
        real = read.read_transaction

        @contextmanager
        def recording(*args, **kwargs):
            helper_calls.append(args)
            with real(*args, **kwargs) as conn:
                yield conn

        monkeypatch.setattr(read, "read_transaction", recording)
        listener = lambda *a: statements.append(a[2])  # noqa: E731
        event.listen(api_engine, "before_cursor_execute", listener)
        try:
            for path in ("/v1/watchlist", "/v1/securities/ZZAA", "/v1/securities/BR$K"):
                assert call(app, path)[0] == 429, path
        finally:
            event.remove(api_engine, "before_cursor_execute", listener)
    assert helper_calls == [] and statements == []


def test_conditional_requests_and_errors_cost_a_token(engine, fixture_path, groups):  # noqa: F811
    pub = publish_run(engine, feed(fixture_path))
    tag = [("If-None-Match", f'"v1-{pub}"')]
    with api_app(engine, groups, burst=3) as (app, _):
        assert call(app, headers=tag)[0] == 304
        assert call(app, "/v1/securities/ZZNONE")[0] == 404
        assert call(app, "/v1/securities/BR$K")[0] == 422
        status, headers, body = call(app, headers=tag)
    assert status == 429 and body and "etag" not in headers  # 429, not 304


def test_concurrent_requests_get_exactly_burst_successes(engine, groups):  # noqa: F811
    with api_app(engine, groups, burst=5) as (app, _):  # the clock never moves
        with ThreadPoolExecutor(max_workers=8) as pool:
            statuses = list(pool.map(lambda _: call(app)[0], range(24)))
    assert statuses.count(404) == 5 and statuses.count(429) == 19


def test_memory_is_bounded_and_eviction_is_least_recently_used(engine, groups):  # noqa: F811
    with api_app(engine, groups, burst=1, max_clients=3) as (app, _):
        hammering, quiet = ("198.51.100.1", 1), ("198.51.100.2", 1)
        assert call(app, client=quiet)[0] == 404 and limited(app, client=quiet)
        assert call(app, client=hammering)[0] == 404
        for i in range(10, 18):  # 8 more clients; the hammering one keeps going
            assert limited(app, client=hammering)
            call(app, client=(f"198.51.100.{i}", 1))
            assert app.state.rate_limiter.size("read") <= 3
        assert limited(app, client=hammering)  # most recently used: never evicted
        assert call(app, client=quiet)[0] == 404  # evicted: back to a full bucket


def test_a_limited_route_without_the_installer_fails_closed(engine, groups, monkeypatch):  # noqa: F811
    from fastapi import FastAPI
    from src.api import read

    calls = []
    real = read.read_transaction

    @contextmanager
    def recording(*args, **kwargs):
        calls.append(args)
        with real(*args, **kwargs) as conn:
            yield conn

    monkeypatch.setattr(read, "read_transaction", recording)
    with login(engine, groups) as (api_engine, _, _):
        app = FastAPI()
        app.state.engine = api_engine
        app.include_router(read.router)  # as #22's app.py would, but without install()
        status, _, body = call(app)
    # No error handlers on a bare app, so only the status and the fixed code are pinned.
    assert status == 503 and b"rate_limiter_missing" in body
    assert calls == []


def test_create_app_has_exactly_one_proxy_middleware(engine, groups):  # noqa: F811
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

    with api_app(engine, groups) as (app, _):
        proxies = [m for m in app.user_middleware if m.cls is ProxyHeadersMiddleware]
    assert len(proxies) == 1


# ── RL-11: configuration refused at startup ──────────────


BAD_SETTINGS = [
    ("API_RATE_LIMIT_BURST", "0"),
    ("API_RATE_LIMIT_BURST", "-1"),
    ("API_RATE_LIMIT_BURST", "1.5"),
    ("API_RATE_LIMIT_BURST", "lots"),
    ("API_RATE_LIMIT_PER_MINUTE", "0"),
    ("API_AUTH_RATE_LIMIT_BURST", "0"),
    ("API_AUTH_RATE_LIMIT_PER_HOUR", "-5"),
    ("API_TRUSTED_PROXIES", "*"),
    ("API_TRUSTED_PROXIES", "10.0.0.1, proxy.internal"),
    ("API_TRUSTED_PROXIES", "10.0.0.0/33"),
    ("API_TRUSTED_PROXIES", "10.0.0.1, *"),
]


def start(api_engine, groups):  # noqa: F811
    """create_app() and its startup, whichever of the two validates."""
    from fastapi.testclient import TestClient
    from src.api.read import create_app

    with TestClient(create_app(api_engine, api_role=groups["api"])):
        pass


def test_invalid_limits_and_proxy_entries_refuse_at_startup_with_one_fixed_error(
    engine, groups, monkeypatch  # noqa: F811
):
    errors = set()
    with login(engine, groups) as (api_engine, _, _):
        for name, value in BAD_SETTINGS:
            with monkeypatch.context() as env:
                env.setenv(name, value)
                with pytest.raises(Exception) as caught:
                    start(api_engine, groups)
            assert "proxy.internal" not in str(caught.value)
            errors.add((type(caught.value), str(caught.value)))
        assert len(errors) == 1, errors  # one fixed error: nothing specific is echoed
        with monkeypatch.context() as env:  # valid lists and the explicit switch start
            env.setenv("API_TRUSTED_PROXIES", "10.0.0.1, 192.0.2.0/24, 2001:db8::/32")
            start(api_engine, groups)
            env.delenv("API_TRUSTED_PROXIES")
            env.setenv("API_TRUST_ALL_PROXIES", "1")
            start(api_engine, groups)


# ── RL-12: docs and the OpenAPI snapshot ─────────────────


def test_every_read_route_documents_429():
    paths = _v1_openapi(_router_app())["paths"]
    for path in ("/v1/watchlist", "/v1/securities/{symbol}", "/v1/market-status"):
        assert "429" in paths[path]["get"]["responses"], path


def test_api_document_covers_rate_limiting():
    from test_read_api import REPO

    content = (REPO / "docs" / "technical-specs" / "API.md").read_text()
    for term in (
        "429", "Retry-After", "rate_limited", "API_RATE_LIMIT_BURST", "API_RATE_LIMIT_PER_MINUTE",
        "API_AUTH_RATE_LIMIT_BURST", "API_AUTH_RATE_LIMIT_PER_HOUR", "API_RATE_LIMIT_MAX_CLIENTS",
        "API_TRUSTED_PROXIES", "API_TRUST_ALL_PROXIES", "--no-proxy-headers", "X-Forwarded-For",
        "worker",
    ):
        assert term in content, term


# ── RL-13: logs are capped per key and globally ──────────


def limit_lines(caplog, word):
    return [r.getMessage() for r in caplog.records if re.search(rf"\b{word}\b", r.getMessage())]


def test_rejections_log_once_per_key_then_a_count(engine, groups, caplog):  # noqa: F811
    caplog.set_level(logging.DEBUG)
    clock = Clock()
    peer = ("198.51.100.77", 1)
    with api_app(engine, groups, clock=clock, burst=1, per_minute=6) as (app, _):
        caplog.clear()
        assert call(app, client=peer)[0] == 404
        assert drain(app, 5, client=peer) == [429] * 5
        assert len(limit_lines(caplog, "rate_limited")) == 1  # the first rejection only
        clock.advance(11)  # past Retry-After
        assert call(app, client=peer)[0] == 404
        assert call(app, client=peer)[0] == 429
    lines = limit_lines(caplog, "rate_limited")
    assert len(lines) == 2 and re.search(r"\b4\b", lines[1])  # the 4 held back
    for line in limit_lines(caplog, r"rate_limited\w*"):
        for leak in ("198.51.100.77", "/v1/", "testserver"):
            assert leak not in line


def test_rejection_lines_are_capped_at_60_a_minute_across_keys(engine, groups, caplog):  # noqa: F811
    caplog.set_level(logging.DEBUG)
    clock = Clock()
    with api_app(engine, groups, clock=clock, burst=1) as (app, _):
        caplog.clear()
        for i in range(70):  # 70 keys, one first rejection each, in one minute
            peer = (f"2001:db8:{i:x}::1", 1)  # /64 rotation
            assert call(app, client=peer)[0] == 404 and limited(app, client=peer)
        assert len(limit_lines(caplog, "rate_limited")) == 60
        assert limit_lines(caplog, "rate_limited_suppressed") == []
        clock.advance(61)
        peer = ("2001:db8:ffff::1", 1)
        assert call(app, client=peer)[0] == 404 and limited(app, client=peer)
    suppressed = limit_lines(caplog, "rate_limited_suppressed")
    assert len(suppressed) == 1 and re.search(r"\b10\b", suppressed[0])
    assert all("2001:db8" not in line for line in limit_lines(caplog, r"rate_limited\w*"))


# ── Edge cases found after the build (9b1cf64) ───────────


def test_literal_keys_never_share_a_real_bucket(engine, groups):  # noqa: F811
    """Key namespaces: a peer named "unknown", or an IP-shaped literal, is its own bucket."""
    with api_app(engine, groups, burst=2) as (app, _):
        assert not shares_bucket(app, None, ("unknown", 1), 2)
        # "2001:db8::/64" is not an address: a literal, never the real /64's bucket.
        assert not shares_bucket(app, ("2001:db8::1", 1), ("2001:db8::/64", 1), 2)
        assert not shares_bucket(app, ("198.51.100.1", 1), ("198.51.100.1/32", 1), 2)


@pytest.mark.parametrize("entry", ["10.0.0.1/24", "2001:db8::1/64", "10.0.0.0/8, 192.0.2.1/16"])
def test_proxy_networks_with_host_bits_are_refused(entry, groups, engine, monkeypatch):  # noqa: F811
    from src.api.read import RateLimitConfigError, create_app

    monkeypatch.setenv("API_TRUSTED_PROXIES", entry)
    with login(engine, groups) as (api_engine, _, _):
        with pytest.raises(RateLimitConfigError):
            create_app(api_engine, api_role=groups["api"])
        monkeypatch.setenv("API_TRUSTED_PROXIES", "10.0.0.0/24, 2001:db8::/64")
        create_app(api_engine, api_role=groups["api"])


@pytest.mark.parametrize("value, ok", [("1", True), ("0", True), ("yes", False),
                                       ("true", False), ("2", False), ("", True)])  # empty means unset
def test_trust_all_accepts_only_1_or_0(value, ok, engine, groups, monkeypatch):  # noqa: F811
    from src.api.read import RateLimitConfigError, create_app

    monkeypatch.setenv("API_TRUST_ALL_PROXIES", value)
    with login(engine, groups) as (api_engine, _, _):
        if ok:
            create_app(api_engine, api_role=groups["api"])
        else:
            with pytest.raises(RateLimitConfigError):
                create_app(api_engine, api_role=groups["api"])


def test_trust_all_0_trusts_no_one(engine, groups, monkeypatch):  # noqa: F811
    from src.api.read import create_app

    monkeypatch.setenv("API_TRUST_ALL_PROXIES", "0")
    monkeypatch.setenv("API_RATE_LIMIT_BURST", "2")
    with login(engine, groups) as (api_engine, _, _):
        app = create_app(api_engine, api_role=groups["api"])
        peer = ("203.0.113.9", 1)
        assert drain(app, 2, client=peer, headers=[("X-Forwarded-For", "6.6.6.6")]) == [404, 404]
        assert limited(app, client=peer, headers=[("X-Forwarded-For", "6.6.6.7")])


def test_an_evicted_key_starts_again_without_its_held_count(engine, groups, caplog):  # noqa: F811
    caplog.set_level(logging.DEBUG)
    clock = Clock()
    a, b = ("198.51.100.1", 1), ("198.51.100.2", 1)
    with api_app(engine, groups, clock=clock, burst=1, per_minute=6, max_clients=1) as (app, _):
        caplog.clear()
        assert call(app, client=a)[0] == 404
        assert drain(app, 4, client=a) == [429] * 4  # one line, three held back
        assert call(app, client=b)[0] == 404  # evicts a, its bucket and its log state
        assert call(app, client=a)[0] == 404  # a full bucket again
        assert call(app, client=a)[0] == 429  # a new first rejection
    lines = limit_lines(caplog, "rate_limited")
    assert len(lines) == 2
    assert not re.search(r"\b3\b", lines[1]), lines[1]  # the held count did not survive


def test_a_missing_limiter_inside_create_app_has_r1s_error_shape(engine, groups):  # noqa: F811
    with api_app(engine, groups) as (app, _):
        del app.state.rate_limiter
        status, _, body = call(app)
    assert status == 503
    assert json.loads(body) == {"detail": json.loads(body)["detail"], "code": "rate_limiter_missing"}
    assert body == json.dumps(json.loads(body), sort_keys=True, separators=(",", ":")).encode()


def test_a_rate_that_does_not_divide_a_minute_is_exact(engine, groups):  # noqa: F811
    """RLB-T1: the float slack lets a token due exactly now through, indefinitely."""
    clock = Clock()
    with api_app(engine, groups, clock=clock, burst=1, per_minute=7) as (app, _):
        assert call(app)[0] == 404
        status, headers, _ = call(app)
        assert status == 429 and headers["retry-after"] == "9"  # ceil(60 / 7)
        clock.advance(60 / 7)
        assert call(app)[0] == 404
        for _ in range(200):  # exactly at the refill rate, no drift
            clock.advance(60 / 7)
            assert call(app)[0] == 404


@pytest.mark.parametrize("fields", [
    {"per_minute": 0}, {"burst": 0}, {"auth_per_hour": 0}, {"auth_burst": 0},
    {"max_clients": 0}, {"per_minute": -1}, {"per_minute": 1.5}, {"burst": True},
    {"trusted_proxies": ("*",)}, {"trusted_proxies": ("10.0.0.1/24",)},
])
def test_a_config_built_directly_refuses_with_the_fixed_error(fields):
    """RLB-T2: never ZeroDivisionError or a TypeError deeper in."""
    from src.api.read import RateLimitConfig, RateLimitConfigError

    with pytest.raises(RateLimitConfigError) as caught:
        RateLimitConfig(**fields)
    assert str(caught.value) == "rate_limit_config_invalid: the rate-limit settings are not valid"
