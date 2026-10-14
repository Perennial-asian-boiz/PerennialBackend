"""Small, dependency-free helpers for passwords, OTPs, and sessions."""

import hashlib
import hmac
import secrets


_PBKDF2_ITERATIONS = 310_000


def hash_secret(secret: str) -> str:
    """Hash a password or verification code with a random salt."""
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", secret.encode("utf-8"), salt, _PBKDF2_ITERATIONS
    )
    return f"pbkdf2_sha256${_PBKDF2_ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_secret(secret: str, encoded: str) -> bool:
    """Constant-time verification for a value produced by hash_secret."""
    try:
        algorithm, iterations, salt_hex, digest_hex = encoded.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        expected = hashlib.pbkdf2_hmac(
            "sha256",
            secret.encode("utf-8"),
            bytes.fromhex(salt_hex),
            int(iterations),
        )
        return hmac.compare_digest(expected.hex(), digest_hex)
    except (TypeError, ValueError):
        return False


def new_otp() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


def new_session_token() -> str:
    return secrets.token_urlsafe(32)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()
