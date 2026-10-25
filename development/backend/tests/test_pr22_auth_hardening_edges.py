"""#22 auth hardening: the post-build edge pass at fix/pr22-auth-hardening 904a9cc.

Spec: the AH packet (docs/m5-pr22-fix-packet @ a8309c2), Part 1 with the S1
amendment. test_pr22_auth_hardening.py pins the criteria; these pin what a build
makes possible to get wrong beyond them: races, error paths, the email budget under
concurrency and across app instances, the per-client share behind a trusted proxy,
and paths that send nothing yet could still spend a share. Fake provider only.
"""

import threading

import pytest
from sqlalchemy import text

from test_pr22_auth_hardening import (  # noqa: F401  (api, pr22_roles: fixtures)
    EMAIL,
    ROOMY,
    SEND_REFUSED,
    START_AGAIN,
    UNIFORM,
    WAIT,
    age_challenge,
    api,
    assert_send_refused,
    blocking,
    budget,
    budgeted,
    challenges,
    confirmed_user,
    hardened,
    login,
    open_hashes,
    peer,
    post_forgot,
    post_login,
    post_signup,
    pr22_roles,
    roomy,
    run,
    seed_challenge,
    sent,
    session,
    session_id_of,
    sessions,
    users,
    verify,
)

pytestmark = pytest.mark.db


def test_two_concurrent_correct_codes_approve_exactly_one_session(roomy, engine):  # noqa: F811
    """Both reserve an attempt and both pass the provider; the finish serializes on
    the email lock, so one wins and the other loses cleanly (400, local logout)."""
    client, provider, _ = roomy
    confirmed_user(provider)
    challenge_id = login(client)
    barrier = threading.Barrier(2, timeout=WAIT)
    provider.hooks["verify_code"] = lambda *a, **k: barrier.wait()
    threads = [run(verify, client, challenge_id) for _ in range(2)]
    for thread, _ in threads:
        thread.join(WAIT * 2)
    statuses = sorted(result[0].status_code for _, result in threads)
    assert statuses == [200, 400], statuses
    approved = [s for s in sessions(engine, provider.users[EMAIL]["id"]) if s.revoked_at is None]
    assert len(approved) == 1
    loser = next(result[0] for _, result in threads if result[0].status_code == 400)
    assert loser.json()["detail"] == START_AGAIN
    local = [c for c in provider.named("logout") if c.kwargs.get("scope") == "local"]
    assert len(local) == 1 and session_id_of(local[0].args[0]) != str(approved[0].session_id)


def test_a_logout_during_an_in_flight_login_leaves_its_code_unusable(roomy, engine):  # noqa: F811
    """Revoke consumes the reserved challenge while its send is in flight; the login
    still answers, but its code can't approve a session afterwards."""
    client, provider, _ = roomy
    body, headers = session(client, provider)
    arrived, release = threading.Event(), threading.Event()
    provider.hooks["send_code"] = blocking(arrived, release, lambda email, flow: flow == "login")
    pending = run(login, client)
    try:
        assert arrived.wait(WAIT)
        assert client.post("/v1/auth/logout", headers=headers).status_code == 204
    finally:
        release.set()
    pending[0].join(WAIT)
    assert pending[1], "the in-flight login didn't finish"
    assert verify(client, pending[1][0]).status_code == 400
    assert all(s.revoked_at is not None for s in sessions(engine, body["user"]["id"]))


def _another_address(provider, monkeypatch):
    def hook(token):  # Supabase's identity for this session names someone else
        provider.tokens[token] = {**provider.tokens[token], "email": "other@example.com"}

    provider.hooks["identity"] = hook
    return 400, {"detail": "Invalid verification code."}, 1  # the counted 400


def _identity_refused(provider, monkeypatch):
    from src.api.supabase_auth import AuthProviderError

    def hook(token):
        raise AuthProviderError(401)

    provider.hooks["identity"] = hook
    error = AuthProviderError(401)
    return error.status, {"detail": error.message, "code": error.code, "field_errors": {}}, 0


def _malformed_body(provider, monkeypatch):
    from src.api.supabase_auth import AuthProviderError

    real = provider.verify_code
    monkeypatch.setattr(provider, "verify_code",  # a session, but no refresh token
                        lambda *a, **k: {"access_token": real(*a, **k)["access_token"]})
    error = AuthProviderError()
    return error.status, {"detail": error.message, "code": error.code, "field_errors": {}}, 0


@pytest.mark.parametrize("logout_fails", [False, True], ids=["logout_ok", "logout_fails"])
@pytest.mark.parametrize("refusal", [_another_address, _identity_refused, _malformed_body],
                         ids=["email_mismatch", "identity_refused", "malformed_body"])
def test_a_session_supabase_issued_for_a_refused_verify_gets_one_local_logout(
    api, engine, monkeypatch, refusal, logout_fails  # noqa: F811
):
    """Supabase accepted the code, so a session exists there; verify-otp refuses it
    anyway. Like the lost-race path, it logs that session out once, locally and with
    no connection held, and answers exactly as before whether or not that works."""
    from src.api.supabase_auth import AuthProviderError

    client, provider, _ = api
    confirmed_user(provider)
    challenge_id = login(client)
    status, detail, attempts = refusal(provider, monkeypatch)
    if logout_fails:

        def unavailable(*args, **kwargs):
            raise AuthProviderError(503)

        provider.hooks["logout"] = unavailable
    response = verify(client, challenge_id)
    assert (response.status_code, response.json()) == (status, detail)
    assert [r.attempts for r in challenges(engine, EMAIL)] == [attempts]
    assert sessions(engine, provider.users[EMAIL]["id"]) == []
    issued = provider.named("verify_code")[-1].result["access_token"]
    logouts = [(c.args[0] == issued, c.kwargs.get("scope"), c.checked_out)
               for c in provider.named("logout")]
    assert logouts == [(True, "local", 0)], logouts


PROXY = ("10.0.0.1", 4000)


def _through_proxy(app, forwarded):
    from fastapi.testclient import TestClient

    return TestClient(app, client=PROXY, headers={"X-Forwarded-For": forwarded})


def test_behind_a_trusted_proxy_each_forwarded_client_has_its_own_share(
    engine, pr22_roles, monkeypatch  # noqa: F811
):
    env = {**ROOMY, **budget(share=1), "API_TRUSTED_PROXIES": PROXY[0]}
    with hardened(engine, pr22_roles, monkeypatch, env=env) as (_, provider, app):
        emails = users(provider, 4)
        one, two = _through_proxy(app, "198.51.100.10"), _through_proxy(app, "198.51.100.11")
        login(one, emails[0])
        login(two, emails[1])  # the proxy's address isn't the key, so this isn't refused
        assert_send_refused(post_login(one, emails[2]))
        assert len(sent(provider)) == 2


def test_behind_a_trusted_proxy_a_failed_send_refunds_the_forwarded_client(
    engine, pr22_roles, monkeypatch  # noqa: F811
):
    from src.api.supabase_auth import AuthProviderError

    env = {**ROOMY, **budget(share=1), "API_TRUSTED_PROXIES": PROXY[0]}
    with hardened(engine, pr22_roles, monkeypatch, env=env) as (_, provider, app):
        emails = users(provider, 2)
        failures = []

        def fail_once(*args, **kwargs):
            if not failures:
                failures.append(1)
                raise AuthProviderError()

        provider.hooks["send_code"] = fail_once
        mine = _through_proxy(app, "198.51.100.12")
        assert post_login(mine, emails[0]).status_code == 503
        login(mine, emails[1])  # the refunded share is this client's, not the proxy's


def test_requests_refused_before_sending_spend_no_share(engine, pr22_roles, monkeypatch):  # noqa: F811
    """An unconfirmed login (403), signup of a registered address (409) and the
    per-email cap (429) all refuse before the reserve step: the share stays whole. (The
    total is raised: the 60 seeded sends that reach the cap count app-wide too.)"""
    with budgeted(engine, pr22_roles, monkeypatch, total=100, share=3) as (_, provider, app):
        emails = users(provider, 4)
        FakeProviderSignup = type(provider).__mro__[1].signup
        FakeProviderSignup(provider, "unconfirmed@example.com", "Password123", "Test Person")
        seed_challenge(engine, "login", email=emails[3], send_count=60, consumed=True)
        mine = peer(app, 1)
        assert post_login(mine, "unconfirmed@example.com").status_code == 403
        assert post_signup(mine, emails[0]).status_code == 409
        capped = post_login(mine, emails[3])
        assert capped.status_code == 429 and capped.json()["detail"] != SEND_REFUSED
        for email in emails[:3]:  # all three of the share's sends are still there
            login(mine, email)
        assert_send_refused(post_login(mine, emails[1]))


def test_one_short_concurrent_mixed_flows_on_two_instances_send_exactly_one_email(
    engine, pr22_roles, monkeypatch  # noqa: F811
):
    """The reserve step is held open between its count and its insert, so without
    the app-wide budget lock two of these would both see room and both send."""
    import time

    from fastapi.testclient import TestClient

    from src.api import accounts
    from src.api.app import create_app

    with budgeted(engine, pr22_roles, monkeypatch, total=5, recovery=2, signup=2, share=1) as (
        _, provider, app
    ):
        second = create_app(api_role=pr22_roles["api"])
        second.dependency_overrides[accounts.get_provider] = lambda: provider
        seed_challenge(engine, "login", email="earlier@example.com", send_count=4, consumed=True)
        emails = users(provider, 3)
        real = accounts.reserve_email

        def held_open(*args, **kwargs):  # widen count-to-insert, still in the transaction
            key = real(*args, **kwargs)
            time.sleep(0.3)
            return key

        monkeypatch.setattr(accounts, "reserve_email", held_open)
        other = lambda n: TestClient(second, client=(f"198.51.100.{n}", 5000))  # noqa: E731
        threads = [run(post_login, peer(app, 1), emails[0]),
                   run(post_forgot, other(2), emails[1]),
                   run(post_signup, peer(app, 3), "fresh@example.com"),
                   run(post_login, other(4), emails[2])]
        for thread, _ in threads:
            thread.join(WAIT * 2)
        assert len(sent(provider)) == 1, [c.name for c in sent(provider)]
        for _, result in threads:
            response = result[0]
            assert response.status_code in (200, 201) or (
                response.status_code == 429 and response.json()["detail"] == SEND_REFUSED
            ), response.text


def test_a_first_send_that_raises_anything_releases_the_row_and_the_share(
    engine, pr22_roles, monkeypatch  # noqa: F811
):
    with budgeted(engine, pr22_roles, monkeypatch, share=1) as (_, provider, app):
        emails = users(provider, 2)
        failures = []

        def crash_once(*args, **kwargs):
            if not failures:
                failures.append(1)
                raise RuntimeError("synthetic transport failure")

        provider.hooks["send_code"] = crash_once
        mine = peer(app, 1)
        with pytest.raises(RuntimeError):
            post_login(mine, emails[0])
        assert open_hashes(engine, emails[0]) == set()
        login(mine, emails[1])  # the share came back


def test_supabases_429_on_a_resend_still_counts_and_spends_the_share(
    engine, pr22_roles, monkeypatch  # noqa: F811
):
    from src.api.supabase_auth import AuthProviderError

    with budgeted(engine, pr22_roles, monkeypatch, share=2) as (_, provider, app):
        emails = users(provider, 2)
        mine = peer(app, 1)
        challenge_id = login(mine, emails[0])
        age_challenge(engine, challenge_id, sent_seconds=61)

        def too_many(*args, **kwargs):
            raise AuthProviderError(429, "over_email_send_rate_limit")

        provider.hooks["send_code"] = too_many
        assert_send_refused(mine.post("/v1/auth/resend-otp",
                                      json={"email": emails[0], "challenge_id": challenge_id}))
        provider.hooks.pop("send_code")
        assert [r.send_count for r in challenges(engine, emails[0])] == [2]
        assert_send_refused(post_login(mine, emails[1]))  # both shares spent


def test_forgot_password_answers_uniformly_when_the_reset_link_flow_is_exhausted(
    engine, pr22_roles, monkeypatch  # noqa: F811
):
    """A full end-to-end recovery at the defaults, then a stranger's requests: the
    owner's link still works once accepted, and every later answer is uniform."""
    with budgeted(engine, pr22_roles, monkeypatch) as (client, provider, app):
        from test_pr22_auth_hardening import recovery_session

        session(client, provider)
        _, headers = recovery_session(client, provider)
        stranger = peer(app, 9)
        for _ in range(3):
            response = post_forgot(stranger, EMAIL)
            assert response.status_code == 200 and response.json() == UNIFORM
        assert client.post("/v1/auth/reset-password", headers=headers,
                           json={"new_password": "Newpassword123"}).status_code == 200
        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM app.verified_sessions"
                                     " WHERE revoked_at IS NULL")).scalar() == 0
