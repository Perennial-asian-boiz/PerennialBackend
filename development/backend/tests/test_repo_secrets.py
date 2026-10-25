"""S1-AC3: no secrets in the tree; credentials load from .env.

Keeps S1-01 AC3 true ("no API key or Supabase service-role key is found; all
credentials load from .env"), per docs/work-items/s1-stories-plan.md (S1-AC3).
Every tracked file except the lock files is scanned for credential-shaped
strings. A JWT-shaped string is allowed only in a synthetic fixture listed in
JWT_FIXTURES with the SHA-256 of its exact bytes; nothing else has an exception.
A finding names the file, line and kind, never the matched text.

The planted cases build their sentinels at run time, so this file never holds a
string the scan would flag.
"""

import base64
import hashlib
import json
import re
import subprocess
from pathlib import Path
from urllib.parse import parse_qsl

import pytest

ROOT = Path(__file__).resolve().parents[3]

PATTERNS = {
    "supabase_secret_key": re.compile(rb"sb_secret_\w{8,}"),
    "supabase_publishable_key": re.compile(rb"sb_publishable_\w{8,}"),
    "jwt": re.compile(rb"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"),
    "anthropic_key": re.compile(rb"sk-ant-[A-Za-z0-9_-]{8,}"),
    "aws_access_key_id": re.compile(rb"AKIA[0-9A-Z]{16}"),
}

# Synthetic fixtures that must contain a JWT-shaped string: repo path -> the
# SHA-256 of the file's bytes. Any change to such a file needs a new review here.
JWT_FIXTURES: dict[str, str] = {}

# What an .env.example may put where a credential goes: your_fmp_key_here, or an
# upper-case stand-in such as ENCODED_PASSWORD or [YOUR-PASSWORD] (letters only).
PLACEHOLDER = re.compile(r"your_[a-z0-9_]+_here|\[?[A-Z]+(?:[_-][A-Z]+)*\]?|<[^<>]+>")
# A credential by its name, numbered forms (KEY_2) included: a setting such as
# FMP_API_KEY or DB_PASS, or a URL query parameter such as apikey or token.
CREDENTIAL_NAME = re.compile(
    r"(KEY|TOKEN|SECRET|PASSWORD|_PASS|PWD|CREDENTIALS?)(_?\d+)?$"
)
LOOPBACK = {"localhost", "127.0.0.1", "[::1]"}


def tracked(root):
    listed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z"], capture_output=True, check=True
    ).stdout
    return [path for path in listed.decode().split("\0") if path]


def scan(root, jwt_fixtures):
    """(path, line, kind) for each credential-shaped string in a tracked file."""
    findings = []
    for path in tracked(root):
        if path.endswith(".lock"):
            continue
        file = Path(root, path)
        if not file.is_file():  # deleted from the work tree, still in the index
            continue
        data = file.read_bytes()
        fixture = jwt_fixtures.get(path) == hashlib.sha256(data).hexdigest()
        for kind, pattern in PATTERNS.items():
            if kind == "jwt" and fixture:
                continue
            for match in pattern.finditer(data):
                findings.append((path, data.count(b"\n", 0, match.start()) + 1, kind))
    return findings


def _url_password(value):
    """(password, host) from a URL's authority, or None without one. Parsed by
    hand: urlsplit rejects a bracketed placeholder such as [YOUR-PASSWORD]."""
    authority = re.split(r"[/?#]", value.split("://", 1)[1], maxsplit=1)[0]
    userinfo, at, hostport = authority.rpartition("@")
    if not at or ":" not in userinfo:
        return None
    host = re.match(r"\[[^\]]*\]|[^:]*", hostport).group(0)  # [::1] keeps its brackets
    return userinfo.split(":", 1)[1], host


def _credential(name):
    return bool(CREDENTIAL_NAME.search(name.upper().replace("-", "_")))


def _query_credentials(value):
    """Values of a URL's credential-named query parameters (FMP's ?apikey=...)."""
    query = value.partition("?")[2].partition("#")[0]
    return [v for k, v in parse_qsl(query, keep_blank_values=True) if _credential(k)]


def env_example_problems(text):
    """(line, setting) for each credential an .env.example gives a real value:
    a credential-named setting that isn't empty or a placeholder, a URL whose
    password isn't one (a loopback URL's development login is allowed), or a URL
    query parameter named like a credential that isn't empty or a placeholder."""
    problems = []
    for number, line in enumerate(text.splitlines(), 1):
        setting = re.match(r"\s*(?:#\s*)?(?:export\s+)?([A-Z][A-Z0-9_]*)=(.*)$", line)
        if not setting:
            continue
        name, value = setting.group(1), setting.group(2).strip().strip("\"'")
        if "://" in value:
            login = _url_password(value)
            real_login = (
                login
                and login[1] not in LOOPBACK
                and not PLACEHOLDER.fullmatch(login[0])
            )
            real_query = any(
                v and not PLACEHOLDER.fullmatch(v) for v in _query_credentials(value)
            )
            if real_login or real_query:
                problems.append((number, name))
        elif value and _credential(name) and not PLACEHOLDER.fullmatch(value):
            problems.append((number, name))
    return problems


# ── the repository ───────────────────────────────────────────


def test_no_tracked_file_holds_a_credential():
    assert tracked(ROOT), "git ls-files listed nothing: not a checkout?"
    findings = scan(ROOT, JWT_FIXTURES)
    assert findings == [], f"credential-shaped strings (path, line, kind): {findings}"


def test_every_jwt_fixture_is_tracked_unchanged_and_needed():
    files = set(tracked(ROOT))
    for path, digest in JWT_FIXTURES.items():
        assert path in files, f"{path} is allowlisted but not tracked"
        data = (ROOT / path).read_bytes()
        assert hashlib.sha256(data).hexdigest() == digest, f"{path} changed"
        assert PATTERNS["jwt"].search(data), f"{path} no longer needs its entry"


def test_env_files_are_ignored_and_only_examples_are_tracked():
    env_files = [
        path for path in tracked(ROOT) if re.fullmatch(r"\.env(\..+)?", Path(path).name)
    ]
    assert env_files and all(Path(p).name == ".env.example" for p in env_files), (
        env_files
    )
    for path in (".env", ".env.local", "development/backend/.env"):
        ignored = subprocess.run(["git", "-C", str(ROOT), "check-ignore", "-q", path])
        assert ignored.returncode == 0, f"{path} is not ignored"


def test_env_examples_name_settings_without_real_values():
    examples = [p for p in tracked(ROOT) if Path(p).name == ".env.example"]
    for path in examples:
        text = (ROOT / path).read_text()
        assert re.search(r"(?m)^\s*[A-Z][A-Z0-9_]*=", text), f"{path} names nothing"
        assert env_example_problems(text) == [], path


# ── planted sentinels, in a temporary repository ─────────────


def _b64(value):
    return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()


def _service_role_jwt():
    claims = {"iss": "supabase", "ref": "synthetic", "role": "service_role"}
    return ".".join(
        [_b64({"alg": "HS256", "typ": "JWT"}), _b64(claims), "c3ludGhldGlj"]
    )


SENTINELS = {
    "supabase_secret_key": lambda: "sb_" + "secret_" + "Synthetic0Sentinel0Value",
    "supabase_publishable_key": lambda: "sb_" + "publishable_" + "Synthetic0Sentinel",
    "jwt": _service_role_jwt,
    "anthropic_key": lambda: "sk-" + "ant-" + "api03-" + "Synthetic0Sentinel0Value",
    "aws_access_key_id": lambda: "AK" + "IA" + "SYNTHETIC0000000",
}


def _repo(tmp_path, files):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    for path, content in files.items():
        file = tmp_path / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(content if isinstance(content, bytes) else content.encode())
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True)
    return tmp_path


@pytest.mark.parametrize("kind", sorted(SENTINELS))
def test_a_planted_credential_is_found_without_echoing_it(tmp_path, kind):
    value = SENTINELS[kind]()
    root = _repo(
        tmp_path, {"clean.py": "x = 1\n", "src/config.py": f'\n\nKEY = "{value}"\n'}
    )
    findings = scan(root, {})
    assert findings == [("src/config.py", 3, kind)]
    assert value not in repr(findings)


@pytest.mark.parametrize(
    "path",
    ["docs/notes.md", ".github/workflows/ci.yml", "development/backend/.env.example"],
)
def test_a_planted_credential_is_found_in_any_tracked_file(tmp_path, path):
    root = _repo(
        tmp_path, {path: "SUPABASE_SECRET_KEY=" + SENTINELS["supabase_secret_key"]()}
    )
    assert scan(root, {}) == [(path, 1, "supabase_secret_key")]


def test_a_planted_credential_is_found_in_a_binary_file(tmp_path):
    payload = b"\x00\xff\x89PNG" + SENTINELS["aws_access_key_id"]().encode() + b"\x00"
    root = _repo(tmp_path, {"assets/logo.png": payload})
    assert scan(root, {}) == [("assets/logo.png", 1, "aws_access_key_id")]


def test_only_lock_files_are_skipped(tmp_path):
    key = SENTINELS["anthropic_key"]()
    root = _repo(
        tmp_path,
        {"requirements.lock": key, "requirements.lock.txt": key, "lock.py": key},
    )
    assert sorted(scan(root, {})) == [
        ("lock.py", 1, "anthropic_key"),
        ("requirements.lock.txt", 1, "anthropic_key"),
    ]


def test_an_allowlisted_jwt_fixture_passes_only_with_its_exact_bytes(tmp_path):
    fixture = f'TOKEN = "{_service_role_jwt()}"\n'.encode()
    digest = hashlib.sha256(fixture).hexdigest()
    path = "development/backend/tests/fixtures/jwt.py"
    assert scan(_repo(tmp_path / "same", {path: fixture}), {path: digest}) == []
    changed = fixture + b"# edited\n"
    assert scan(_repo(tmp_path / "edited", {path: changed}), {path: digest}) == [
        (path, 1, "jwt")
    ]
    elsewhere = "development/backend/tests/fixtures/other.py"
    assert scan(_repo(tmp_path / "moved", {elsewhere: fixture}), {path: digest}) == [
        (elsewhere, 1, "jwt")
    ]


def test_the_jwt_allowlist_never_excuses_another_credential(tmp_path):
    path = "fixtures/jwt.py"
    fixture = f'TOKEN = "{_service_role_jwt()}"\nKEY = "{SENTINELS["supabase_secret_key"]()}"\n'
    allow = {path: hashlib.sha256(fixture.encode()).hexdigest()}
    assert scan(_repo(tmp_path, {path: fixture}), allow) == [
        (path, 2, "supabase_secret_key")
    ]


NOT_CREDENTIALS = """\
# Settings: SUPABASE_SECRET_KEY takes a key starting `sb_secret_`; Claude keys start `sk-ant-`.
prefix = "sb_secret_" + key      # building one at run time
short = "sb_secret_abc1234"       # seven characters after the prefix
pattern = rb"sb_publishable_\\w{8,}"
header = "eyJ"                     # a JWT's first three characters
shape = "header.payload.signature"
two_parts = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0"
aws = "AKIA" + "lowercase_is_not_an_id"
hash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
FMP_API_KEY=your_fmp_key_here
"""


def test_no_false_positives_on_prefixes_shapes_and_placeholders(tmp_path):
    root = _repo(
        tmp_path, {"docs/keys.md": NOT_CREDENTIALS, ".env.example": NOT_CREDENTIALS}
    )
    assert scan(root, {}) == []


# ── .env.example values ──────────────────────────────────────

GOOD_EXAMPLE = """\
# Local development only
FMP_API_KEY=your_fmp_key_here
ANTHROPIC_API_KEY=
DATABASE_URL=postgresql://postgres.PROJECT_REF:[YOUR-PASSWORD]@POOLER_HOST:5432/postgres?sslmode=require
SUPABASE_DB_PASSWORD=""
API_DATABASE_URL=postgresql://perennial_api.PROJECT_REF:ENCODED_PASSWORD@POOLER_HOST:5432/postgres
SUPABASE_URL=https://PROJECT_REF.supabase.co
SUPABASE_PUBLISHABLE_KEY=
AUTH_EMAIL_REDIRECT_URL=http://127.0.0.1:5173/auth/confirm
APP_ENV=development
CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173
# DATABASE_URL=postgresql+psycopg://perennial:perennial@localhost:5433/perennial
# TEST_DATABASE_URL=postgresql+psycopg://perennial_test:perennial_test@127.0.0.1:5432/perennial_test
"""


def test_env_example_placeholders_and_loopback_urls_are_fine():
    assert env_example_problems(GOOD_EXAMPLE) == []


@pytest.mark.parametrize(
    "line",
    [
        "FMP_API_KEY=abc123def456ghi789",
        "TWITTER_BEARER_TOKEN=AAAAAAAAAAAAAAAAAAAAAsynthetic",
        "# YOUTUBE_API_KEY=AIzaSyntheticSentinelValue",
        "SUPABASE_SECRET_KEY=your-key",
        "FMP_API_KEY=ABC123DEF456",
        'SUPABASE_DB_PASSWORD="Synthetic9Password"',
        "DATABASE_URL=postgresql://postgres.abcdefghij:Synthetic9Pw@pooler.example.com:5432/postgres",
        "API_DATABASE_URL=postgresql://perennial_api.ref:[Synthetic9Pw]@POOLER_HOST:5432/postgres",
        "# TEST_DATABASE_URL=postgresql://u:p@10.0.0.5/perennial_test",
    ],
    ids=[
        "key",
        "token",
        "commented_key",
        "not_the_placeholder_form",
        "upper_case_with_digits",
        "quoted_password",
        "remote_url_password",
        "bracketed_real_password",
        "commented_remote_url",
    ],
)
def test_a_real_looking_value_in_an_env_example_is_refused(line):
    name = re.match(r"\s*(?:#\s*)?([A-Z0-9_]+)=", line).group(1)
    number = len(GOOD_EXAMPLE.splitlines()) + 1
    assert env_example_problems(GOOD_EXAMPLE + line + "\n") == [(number, name)]


@pytest.mark.parametrize(
    "line",
    [
        "FMP_URL=https://financialmodelingprep.com/api/v3/quote/ZZAA?apikey=abc123def456",
        "FMP_URL=https://provider.example.com/v3?symbol=ZZAA&api_key=Synthetic9Key",
        "FEED_URL=https://feed.example.com/rss?api-key=Synthetic9Key",
        "WEBHOOK_URL=https://hooks.example.com/in?token=Synthetic9Token#frag",
        "# SIGNER_URL=https://signer.example.com/?client_secret=Synthetic9Secret",
        "DB_URL=postgresql://localhost/dev?password=Synthetic9Password",
        "DB_PASS=Synthetic9Password",
        "MAIL_PWD=Synthetic9Password",
        "GOOGLE_CREDENTIALS=Synthetic9Credentials",
        "FMP_API_KEY_2=abc123def456",
        "FINNHUB_API_KEY2=abc123def456",
        "export FINNHUB_API_KEY=abc123def456",
        "# export YOUTUBE_API_KEY=AIzaSyntheticSentinelValue",
    ],
    ids=[
        "fmp_apikey_query",
        "api_key_query",
        "hyphenated_query",
        "token_query",
        "commented_secret_query",
        "loopback_query_password",
        "pass",
        "pwd",
        "credentials",
        "numbered_key",
        "numbered_key_no_underscore",
        "export",
        "commented_export",
    ],
)
def test_t1_t3_a_credential_in_a_query_a_new_name_form_or_an_export_is_refused(line):
    name = re.match(r"\s*(?:#\s*)?(?:export\s+)?([A-Z0-9_]+)=", line).group(1)
    number = len(GOOD_EXAMPLE.splitlines()) + 1
    assert env_example_problems(GOOD_EXAMPLE + line + "\n") == [(number, name)]


def test_t1_t3_placeholders_and_ordinary_names_stay_fine():
    example = """\
FMP_URL=https://financialmodelingprep.com/api/v3/quote?apikey=your_fmp_key_here
FEED_URL=https://feed.example.com/rss?api_key=&format=json
FMP_URL_2=https://financialmodelingprep.com/api/v3/quote?apikey=FMP_API_KEY
SEARCH_URL=https://search.example.com/?q=keyword&sort_by=date&page=2
export FMP_API_KEY=your_fmp_key_here
DB_PASS=
FMP_API_KEY_2=
RLS_BYPASS=true
PASSWORD_MIN_LENGTH_HINT=see docs
"""
    assert env_example_problems(example) == []


def test_ipv6_loopback_development_logins_are_fine():
    assert (
        env_example_problems("DATABASE_URL=postgresql://dev:dev@[::1]:5432/dev\n") == []
    )
