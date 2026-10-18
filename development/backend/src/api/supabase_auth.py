"""Small Supabase Auth REST client. No admin/service-role key is required.

JWT signature validation is delegated to the project's authenticated /user
endpoint on every request. Claims are inspected only after that succeeds.
This supports Supabase signing-key rotation without custom cryptography.
"""

import base64
import json
import math
import os
import time
from dataclasses import dataclass, field
from urllib.parse import urlencode, urlsplit
from uuid import UUID

import requests


class AuthProviderError(Exception):
    def __init__(self, status=503, code="unavailable", *, reasons=()):
        self.status = status
        self.code = code
        self.field_errors = {}
        self.message = {
            401: "Invalid email, password, or session.",
            429: "Too many authentication requests. Please try again later.",
            409: "This email is already registered. Please sign in instead.",
            422: "We couldn't validate these details. Please try again or contact support.",
        }.get(status, "Authentication service is unavailable. Please try again later.")
        # App-owned copy only: provider messages can contain credentials or user input.
        if code == "weak_password":
            hints = {
                "length": "Choose a longer password.",
                "characters": "Include uppercase and lowercase letters, a number, and a symbol.",
                "pwned": "This password is known to be compromised. Choose a unique password you haven't used elsewhere.",
            }
            self.message = " ".join(hints[key] for key in hints if key in reasons)
            if not self.message:
                self.message = "This password is too easy to guess. Choose a longer, unique password."
            self.field_errors = {"password": self.message}
        elif code == "email_address_invalid":
            self.message = (
                "This email address cannot be used. Enter another valid email address."
            )
            self.field_errors = {"email": self.message}
        elif code == "email_exists":
            self.field_errors = {"email": self.message}
        elif code == "email_not_confirmed":
            self.message = "Please confirm your email using the link in your inbox before signing in."
        elif code == "email_address_not_authorized":
            self.message = "Email verification isn't fully set up yet. Please try again later or contact support."
        elif code == "over_email_send_rate_limit":
            self.message = "Too many verification emails have been requested. Please wait before trying again."
        elif code in {"signup_disabled", "email_provider_disabled"}:
            self.message = (
                "Account registration is currently unavailable. Please try again later."
            )
        super().__init__(self.message)


@dataclass
class SupabaseAuth:
    url: str
    key: str = field(repr=False)

    @classmethod
    def from_environment(cls):
        url = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
        key = os.getenv("SUPABASE_PUBLISHABLE_KEY", "").strip()
        parsed = urlsplit(url)
        local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        development = os.getenv("APP_ENV", "development") == "development"
        if (
            not key
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path
            or (
                parsed.scheme != "https"
                and not (local and development and parsed.scheme == "http")
            )
            or "\n" in key
            or "\r" in key
        ):
            raise AuthProviderError()
        return cls(url, key)

    def request(self, method, path, payload=None, token=None):
        headers = {"apikey": self.key, "Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            with requests.request(
                method,
                f"{self.url}/auth/v1/{path}",
                json=payload,
                headers=headers,
                timeout=(3.05, 10),
                allow_redirects=False,
            ) as response:
                if response.status_code == 204:
                    return {}
                try:
                    body = response.json()
                except ValueError:
                    raise AuthProviderError() from None
                if response.status_code not in (200, 201):
                    code = body.get("error_code") if isinstance(body, dict) else None
                    if not isinstance(code, str):
                        code = None
                    if response.status_code == 429:
                        raise AuthProviderError(
                            429,
                            "over_email_send_rate_limit"
                            if code == "over_email_send_rate_limit"
                            else "rate_limited",
                        )
                    # Allowlisted codes affect UX; never relay raw provider text.
                    if code == "email_not_confirmed":
                        raise AuthProviderError(401, "email_not_confirmed")
                    if code in {"user_already_exists", "email_exists"}:
                        raise AuthProviderError(409, "email_exists")
                    if code == "weak_password":
                        weak = body.get("weak_password")
                        reasons = (
                            weak.get("reasons") if isinstance(weak, dict) else None
                        )
                        raise AuthProviderError(
                            422,
                            code,
                            reasons=reasons if isinstance(reasons, list) else (),
                        )
                    if code in {"email_address_invalid", "validation_failed"}:
                        raise AuthProviderError(422, code)
                    if code in {
                        "email_address_not_authorized",
                        "signup_disabled",
                        "email_provider_disabled",
                    }:
                        raise AuthProviderError(503, code)
                    if response.status_code in (400, 401, 403, 422):
                        raise AuthProviderError(401)
                    raise AuthProviderError()
                if not isinstance(body, dict):
                    raise AuthProviderError()
                return body
        except requests.RequestException:
            raise AuthProviderError() from None

    def confirmation_path(self, path):
        redirect = os.getenv("AUTH_EMAIL_REDIRECT_URL", "").strip()
        if not redirect:
            if os.getenv("APP_ENV", "development") != "development":
                raise AuthProviderError()
            redirect = "http://127.0.0.1:5173/auth/confirm"
        parsed = urlsplit(redirect)
        local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        if (
            not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path != "/auth/confirm"
            or (
                parsed.scheme != "https"
                and not (
                    local
                    and os.getenv("APP_ENV", "development") == "development"
                    and parsed.scheme == "http"
                )
            )
        ):
            raise AuthProviderError()
        return path + "?" + urlencode({"redirect_to": redirect})

    def signup(self, email, password, full_name):
        body = self.request(
            "POST",
            self.confirmation_path("signup"),
            {"email": email, "password": password, "data": {"full_name": full_name}},
        )
        if body.get("access_token"):
            # Email confirmation must be enabled; never silently skip signup OTP.
            self.logout(body["access_token"], scope="local")
            raise AuthProviderError()
        # Supabase may deliberately return a sanitized user for a duplicate.
        # Never present that response as a newly created account.
        if body.get("identities") == []:
            raise AuthProviderError(409, "email_exists")
        try:
            UUID(body["id"])
            if (
                body["email"].casefold() != email.casefold()
                or not isinstance(body["identities"], list)
                or not body["identities"]
            ):
                raise ValueError()
        except (KeyError, ValueError, TypeError, AttributeError):
            raise AuthProviderError() from None
        return body

    def check_password(self, email, password):
        body = self.request(
            "POST", "token?grant_type=password", {"email": email, "password": password}
        )
        token = body.get("access_token")
        if not isinstance(token, str) or not isinstance(body.get("user"), dict):
            raise AuthProviderError()
        # Password checking must not return a usable session to the browser.
        self.logout(token, scope="local")
        return body["user"]

    def send_code(self, email, flow):
        if flow == "signup":
            return self.request(
                "POST",
                self.confirmation_path("resend"),
                {"email": email, "type": "signup"},
            )
        if flow == "recovery":
            return self.request("POST", "recover", {"email": email})
        return self.request("POST", "otp", {"email": email, "create_user": False})

    def verify_code(self, email, code, flow):
        return self.request(
            "POST",
            "verify",
            {
                "email": email,
                "token": code,
                "type": flow if flow in {"signup", "recovery"} else "email",
            },
        )

    def refresh(self, refresh_token):
        return self.request(
            "POST", "token?grant_type=refresh_token", {"refresh_token": refresh_token}
        )

    def logout(self, token, *, scope="global"):
        return self.request("POST", f"logout?scope={scope}", token=token)

    def change_password(self, token, new_password):
        return self.request("PUT", "user", {"password": new_password}, token=token)

    def identity(self, token):
        if not isinstance(token, str) or not 1 <= len(token) <= 16384:
            raise AuthProviderError(401)
        user = self.request("GET", "user", token=token)
        try:
            # /user above has already validated the signature at the issuer.
            parts = token.split(".")
            if len(parts) != 3 or not all(parts):
                raise ValueError()
            claims = json.loads(
                base64.b64decode(
                    parts[1] + "=" * (-len(parts[1]) % 4), altchars=b"-_", validate=True
                )
            )
            audience = claims.get("aud")
            expires = claims["exp"]
            not_before = claims.get("nbf", 0)
            if (
                claims["iss"] != f"{self.url}/auth/v1"
                or not (
                    audience == "authenticated"
                    or isinstance(audience, list)
                    and "authenticated" in audience
                )
                or isinstance(expires, bool)
                or not isinstance(expires, (int, float))
                or not math.isfinite(expires)
                or expires <= time.time()
                or isinstance(not_before, bool)
                or not isinstance(not_before, (int, float))
                or not math.isfinite(not_before)
                or not_before > time.time()
                or claims.get("role") != "authenticated"
                or not user.get("email_confirmed_at")
                or not isinstance(user.get("email"), str)
                or not user["email"]
                or user.get("is_anonymous")
                or UUID(claims["sub"]) != UUID(user["id"])
            ):
                raise ValueError()
            return user, UUID(claims["session_id"])
        except (KeyError, ValueError, TypeError, AttributeError):
            raise AuthProviderError(401) from None
