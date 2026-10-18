"""OTP delivery through Resend. Credentials and codes stay on the backend."""

import os

import requests

RESEND_EMAIL_URL = "https://api.resend.com/emails"


class EmailDeliveryError(RuntimeError):
    """A fixed-message error that never contains provider responses or secrets."""


def send_verification_email(email: str, code: str, *, expires_minutes: int) -> bool:
    """Return True after provider acceptance; False only in explicit local mode.

    A missing configuration or provider failure never falls back to returning
    the code. The caller must roll back the pending account/code transaction.
    """
    provider = os.getenv("EMAIL_PROVIDER", "resend").strip().casefold()
    environment = os.getenv("APP_ENV", "development").strip().casefold()
    if provider == "development" and environment == "development":
        return False
    if provider != "resend":
        raise EmailDeliveryError("Email delivery is not configured.")

    api_key = os.getenv("RESEND_API_KEY", "").strip()
    sender = os.getenv("EMAIL_FROM", "").strip()
    if not api_key or not sender or "\r" in api_key or "\n" in api_key:
        raise EmailDeliveryError("Email delivery is not configured.")

    # No automatic retries: a timed-out request may already have been accepted.
    # Never log request bodies, authorization headers, or provider responses.
    try:
        with requests.post(
            RESEND_EMAIL_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "from": sender,
                "to": [email],
                "subject": "Your Perennial verification code",
                "text": (
                    f"Your Perennial verification code is: {code}\n\n"
                    f"This code expires in {expires_minutes} minutes and can only "
                    "be used once. Do not share it with anyone.\n\n"
                    "If you did not request this code, you can ignore this email."
                ),
            },
            timeout=(3.05, 10),
            allow_redirects=False,
        ) as response:
            if response.status_code not in (200, 201):
                raise EmailDeliveryError("Unable to send verification email.")
            body = response.json()
            if not isinstance(body, dict) or not isinstance(body.get("id"), str):
                raise EmailDeliveryError("Unable to send verification email.")
            if not body["id"].strip():
                raise EmailDeliveryError("Unable to send verification email.")
    except (requests.RequestException, ValueError):
        raise EmailDeliveryError("Unable to send verification email.") from None
    return True
