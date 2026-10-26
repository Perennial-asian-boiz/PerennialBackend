"""#22: the email confirm URL may sit under a path prefix (GitHub Pages).

The frontend is served from /PerennialFrontEnd/, so its confirm page is
https://perennial-asian-boiz.github.io/PerennialFrontEnd/auth/confirm. The rule
(db-coordinator, 2026-10-25): the path is /auth/confirm, optionally after one or
more non-empty prefix segments of RFC 3986 unreserved characters (ASCII letters,
digits, - . _ ~). Refused: . and .. segments, empty segments, percent-encoding, a
trailing slash, anything after "confirm", ";" parameters and backslashes. Every
existing check stays, and redirect_to is the configured URL unchanged.
"""

from urllib.parse import parse_qs, urlencode
from uuid import uuid4

import pytest
from test_pr22_auth_hardening import (  # noqa: F401  (supabase, pr22_roles: fixtures)
    EMAIL,
    UNIFORM,
    _Response,
    pr22_roles,
    seed_challenge,
    supabase,
)

PAGES = "https://perennial-asian-boiz.github.io/PerennialFrontEnd/auth/confirm"
HOST = "https://perennial-asian-boiz.github.io"


def _confirmation(monkeypatch, url, app_env="production"):
    from src.api.supabase_auth import SupabaseAuth

    monkeypatch.setenv("APP_ENV", app_env)
    if url is None:
        monkeypatch.delenv("AUTH_EMAIL_REDIRECT_URL", raising=False)
    else:
        monkeypatch.setenv("AUTH_EMAIL_REDIRECT_URL", url)
    provider = SupabaseAuth("https://example.supabase.co", "synthetic-key")
    return provider.confirmation_path("signup")


ACCEPTED = {
    "github_pages": (PAGES, "production"),
    "no_prefix_as_today": ("https://app.example.com/auth/confirm", "production"),
    "two_segment_prefix": (f"{HOST}/Front-End/v1.2_x~y/auth/confirm", "production"),
    "local_dev_default": (None, "development"),
    "local_dev_with_the_pages_prefix": (
        "http://localhost:5173/PerennialFrontEnd/auth/confirm",
        "development",
    ),
}


@pytest.mark.parametrize("case", sorted(ACCEPTED))
def test_an_accepted_confirm_url_is_sent_to_supabase_unchanged(monkeypatch, case):
    url, app_env = ACCEPTED[case]
    path = _confirmation(monkeypatch, url, app_env)
    expected = url or "http://127.0.0.1:5173/auth/confirm"
    assert path == "signup?" + urlencode({"redirect_to": expected})


REFUSED = {
    # the path rule
    "dot_segment": f"{HOST}/PerennialFrontEnd/./auth/confirm",
    "dot_dot_segment": f"{HOST}/PerennialFrontEnd/../auth/confirm",
    "dot_dot_first": f"{HOST}/../auth/confirm",
    "dot_prefix": f"{HOST}/./auth/confirm",
    "empty_leading_segment": f"{HOST}//auth/confirm",
    "empty_middle_segment": f"{HOST}/PerennialFrontEnd//auth/confirm",
    "percent_in_prefix": f"{HOST}/Perennial%46rontEnd/auth/confirm",
    "percent_in_confirm": f"{HOST}/PerennialFrontEnd/auth/confir%6D",
    "encoded_slash": f"{HOST}/PerennialFrontEnd%2Fauth/confirm",
    "trailing_slash": f"{HOST}/PerennialFrontEnd/auth/confirm/",
    "trailing_slash_no_prefix": f"{HOST}/auth/confirm/",
    "suffix_on_confirm": f"{HOST}/PerennialFrontEnd/auth/confirmx",
    "segment_after_confirm": f"{HOST}/PerennialFrontEnd/auth/confirm/x",
    "params_in_prefix": f"{HOST}/PerennialFrontEnd;v=1/auth/confirm",
    "params_on_confirm": f"{HOST}/PerennialFrontEnd/auth/confirm;x",
    "backslash_in_prefix": f"{HOST}/Perennial\\FrontEnd/auth/confirm",
    "backslash_separator": f"{HOST}/PerennialFrontEnd\\auth/confirm",
    "sub_delim_in_prefix": f"{HOST}/Perennial+FrontEnd/auth/confirm",
    "colon_in_prefix": f"{HOST}/a:b/auth/confirm",
    "at_in_prefix": f"{HOST}/a@b/auth/confirm",
    "space_in_prefix": f"{HOST}/Perennial FrontEnd/auth/confirm",
    "non_ascii_letter_in_prefix": f"{HOST}/Pérennial/auth/confirm",
    "different_case_auth": f"{HOST}/PerennialFrontEnd/Auth/confirm",
    "no_auth_segment": f"{HOST}/PerennialFrontEnd/confirm",
    # the existing checks, now on a prefixed URL
    "http_pages_outside_development": PAGES.replace("https://", "http://"),
    "no_hostname": "https:///PerennialFrontEnd/auth/confirm",
    "username": "https://someone@perennial-asian-boiz.github.io/PerennialFrontEnd/auth/confirm",
    "password": "https://someone:pw@perennial-asian-boiz.github.io/PerennialFrontEnd/auth/confirm",
    "query": PAGES + "?next=1",
    "fragment": PAGES + "#top",
}


@pytest.mark.parametrize("case", sorted(REFUSED))
def test_a_refused_confirm_url_fails_closed(monkeypatch, case):
    from src.api.supabase_auth import AuthProviderError

    with pytest.raises(AuthProviderError) as refused:
        _confirmation(monkeypatch, REFUSED[case])
    assert refused.value.status == 503


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost:5173/PerennialFrontEnd/auth/confirm",  # local, but not dev
        "http://example.com/PerennialFrontEnd/auth/confirm",
    ],
    ids=["http_local_outside_development", "http_non_local_outside_development"],
)
def test_http_with_a_prefix_is_refused_outside_development(monkeypatch, url):
    from src.api.supabase_auth import AuthProviderError

    with pytest.raises(AuthProviderError):
        _confirmation(monkeypatch, url)


def test_http_non_local_with_a_prefix_is_refused_in_development(monkeypatch):
    from src.api.supabase_auth import AuthProviderError

    with pytest.raises(AuthProviderError):
        _confirmation(monkeypatch, PAGES.replace("https://", "http://"), "development")


def test_an_unset_url_is_still_refused_outside_development(monkeypatch):
    from src.api.supabase_auth import AuthProviderError

    with pytest.raises(AuthProviderError):
        _confirmation(monkeypatch, None, "production")


# ── through the routes, with the real SupabaseAuth over the fake requests layer ──


def _redirects(fake, supabase_path):
    """redirect_to of each request to supabase_path (e.g. "signup")."""
    return [
        parse_qs(path.split("?", 1)[1]).get("redirect_to")
        for method, path in fake.requests
        if method == "POST" and path.split("?")[0] == supabase_path
    ]


@pytest.mark.db
def test_signup_sends_the_github_pages_redirect(supabase, monkeypatch):  # noqa: F811
    client, fake, _, _ = supabase
    monkeypatch.setenv("AUTH_EMAIL_REDIRECT_URL", PAGES)
    email = "new-person@example.com"

    def created():
        fake.registered.add(email)  # Supabase now has the address
        return _Response(200, {"id": str(uuid4()), "email": email, "identities": [{}]})

    fake.answers[("POST", "signup")] = created
    response = client.post(
        "/v1/auth/signup",
        json={"email": email, "full_name": "New Person", "password": "Password123"},
    )
    assert response.status_code == 201, response.text
    assert _redirects(fake, "signup") == [[PAGES]]


@pytest.mark.db
def test_resend_confirmation_sends_the_github_pages_redirect(
    supabase,  # noqa: F811
    engine,
    monkeypatch,
):
    client, fake, _, _ = supabase
    monkeypatch.setenv("AUTH_EMAIL_REDIRECT_URL", PAGES)
    email = "pending@example.com"
    challenge_id = seed_challenge(engine, "signup", email=email)  # sent 5 min ago
    fake.answers[("POST", "resend")] = _Response(200, {})
    response = client.post(
        "/v1/auth/resend-confirmation",
        json={"email": email, "challenge_id": challenge_id},
    )
    assert response.status_code == 200, response.text
    assert _redirects(fake, "resend") == [[PAGES]]


@pytest.mark.db
def test_forgot_password_sends_the_github_pages_redirect(supabase, monkeypatch):  # noqa: F811
    client, fake, _, _ = supabase
    monkeypatch.setenv("AUTH_EMAIL_REDIRECT_URL", PAGES)
    fake.answers[("POST", "recover")] = _Response(200, {})
    response = client.post("/v1/auth/forgot-password", json={"email": EMAIL})
    assert response.status_code == 200 and response.json() == UNIFORM
    assert _redirects(fake, "recover") == [[PAGES]]
