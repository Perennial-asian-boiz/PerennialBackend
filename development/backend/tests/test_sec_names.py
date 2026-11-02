"""Company names from SEC's ticker list (decision 92 (b); Q2-Q4 (a); typed by
Bryan 2026-10-28), spec-first.

Staging's securities.name is empty for every security, so the dashboard shows
tickers. Decision 92 (b): an owner-run step, like seed_identity.py, fills NULL
or empty securities.name from SEC's company_tickers.json "title".

Proposed interface (adopt it, or send the tests back):
- development/database/ops/seed_names.py, run by hand as the schema owner:
    SEC_USER_AGENT=... OPS_DATABASE_URL=... python development/database/ops/seed_names.py
  - run(engine, client) -> dict of int counts, with at least names_filled and
    not_in_sec_list (empty-named securities SEC's list has no title for).
    client is an EdgarClient. How it reads the titles (a new method beside
    ticker_map, say) isn't pinned: these tests fake requests.Session.get, so
    the real EdgarClient runs and no SEC call is made.
  - main() -> exit code. OPS_DATABASE_URL is required, as for seed_identity.
    SEC_USER_AGENT follows the summaries CLI's rule: unset, blank, or containing
    example.com prints {"error_code": "missing_credential"} and exits 1, before
    any request or write. Output is counts only.

Pinned rules:
- Only a NULL or blank name is filled; a non-empty name is never overwritten,
  not even one written while the step runs.
- The title is stored as filed: no title-casing ("SYNTHETIC CORP" stays).
- A share class is looked up with SEC's dash spelling (ZZB.B -> ZZB-B).
- A security missing from SEC's list (IBIT, ARKY and BYDDY on staging) keeps
  its NULL name, and a blank or non-string title isn't used.
- Only securities.name changes. Rerunning fills nothing.
- One request per run, to SEC's list, with only the User-Agent header.
- A failed request writes nothing and prints no provider text.
- No importer grant change: the importer still may not UPDATE securities.name.
- The API keeps name and ticker separate (Q4 (a): the frontend composes
  "name-ticker").
- Never scheduled.
"""

import json
import sys
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import requests
from pr22_support import pr22_roles  # noqa: F401  (pytest fixture)
from sqlalchemy import insert, select, text
from src.db import models
from test_collectors import FakeResponse
from test_dashboard_sector import companies, dashboard, publish, record
from test_read_api import groups  # noqa: F401  (pytest fixture)
from test_s1_12_edgar_adapter import MAP_URL, Clock

REPO = Path(__file__).resolve().parents[3]
OPS = REPO / "development" / "database" / "ops"
UA = "Synthetic Research ops@perennial.test"


def seed_names():
    if str(OPS) not in sys.path:
        sys.path.insert(0, str(OPS))
    import seed_names

    return seed_names


class FakeList:
    """requests.Session.get for SEC's ticker list; records each request."""

    def __init__(self, rows, answer=None, on_request=None):
        self.rows, self.answer, self.on_request = rows, answer, on_request
        self.requests = []

    def body(self):
        return {
            str(i): {"cik_str": 1000 + i, "ticker": ticker, "title": title}
            for i, (ticker, title) in enumerate(self.rows)
        }

    def get(self, url, params=None, headers=None, timeout=None, stream=False):
        self.requests.append((url, params, dict(headers or {})))
        if self.on_request is not None:
            self.on_request()
        if self.answer is not None:
            return self.answer
        assert url == MAP_URL, url
        return FakeResponse(200, self.body())


@pytest.fixture
def no_retries(monkeypatch):
    from src.ingestion import collectors

    monkeypatch.setattr(collectors, "MAX_HTTP_RETRIES", 0)
    monkeypatch.delenv("PERENNIAL_ENV", raising=False)  # never read a local .env


def client(fake):
    from src.services.fundamentals.edgar import EdgarClient

    session = requests.Session()
    session.get = fake.get
    clock = Clock()
    return EdgarClient(session, UA, monotonic=clock.monotonic, sleep=clock.sleep)


def security(engine, symbol, name=None, **fields):
    with engine.begin() as conn:
        conn.execute(
            insert(models.securities).values(symbol=symbol, name=name, **fields)
        )


def snapshot(engine):
    """Every securities row, every column, and its xmin."""
    with engine.connect() as conn:
        return {
            row["symbol"]: dict(row)
            for row in conn.execute(
                text("SELECT s.*, s.xmin::text AS xmin FROM perennial.securities s")
            ).mappings()
        }


def names(engine):
    with engine.connect() as conn:
        return dict(
            conn.execute(
                select(models.securities.c.symbol, models.securities.c.name)
            ).all()
        )


ROWS = [
    ("ZZNA", "SYNTHETIC NVIDIA-LIKE CORP"),
    ("ZZAT", "SYNTHETIC AT&T-LIKE INC."),
    ("ZZB-B", "SYNTHETIC HOLDINGS INC"),
    ("ZZMX", "Synthetic Mixed Case, Inc."),
    ("ZZKP", "SYNTHETIC KEPT CO"),
    ("ZZBL", "   "),
]


# ── which names are filled ───────────────────────────────────


@pytest.mark.db
def test_null_and_blank_names_are_filled_as_filed(engine, no_retries):
    security(engine, "ZZNA")
    security(engine, "ZZAT", name="")
    security(engine, "ZZMX", name="   ")
    counts = seed_names().run(engine, client(FakeList(ROWS)))
    assert names(engine) == {
        "ZZNA": "SYNTHETIC NVIDIA-LIKE CORP",
        "ZZAT": "SYNTHETIC AT&T-LIKE INC.",
        "ZZMX": "Synthetic Mixed Case, Inc.",
    }
    assert counts["names_filled"] == 3
    assert all(isinstance(v, int) and not isinstance(v, bool) for v in counts.values())


@pytest.mark.db
def test_a_share_class_is_looked_up_with_secs_dash(engine, no_retries):
    security(engine, "ZZB.B")
    seed_names().run(engine, client(FakeList(ROWS)))
    assert names(engine)["ZZB.B"] == "SYNTHETIC HOLDINGS INC"


@pytest.mark.db
def test_a_non_empty_name_is_never_overwritten(engine, no_retries):
    security(engine, "ZZKP", name="Reviewed Kept Name")
    security(engine, "ZZNA", name="x")
    before = snapshot(engine)
    counts = seed_names().run(engine, client(FakeList(ROWS)))
    assert snapshot(engine) == before  # xmin included: no row rewritten
    assert counts["names_filled"] == 0


@pytest.mark.db
def test_a_name_written_while_the_step_runs_is_kept(engine, no_retries):
    security(engine, "ZZKP")

    def reviewer_names_it():
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE perennial.securities SET name = 'Reviewed' WHERE symbol = 'ZZKP'"
                )
            )

    seed_names().run(engine, client(FakeList(ROWS, on_request=reviewer_names_it)))
    assert names(engine)["ZZKP"] == "Reviewed"


@pytest.mark.db
def test_a_security_missing_from_secs_list_keeps_a_null_name(engine, no_retries):
    for symbol in ("IBIT", "ARKY", "BYDDY"):
        security(engine, symbol)
    security(engine, "ZZNA")
    counts = seed_names().run(engine, client(FakeList(ROWS)))
    assert names(engine) == {
        "IBIT": None,
        "ARKY": None,
        "BYDDY": None,
        "ZZNA": "SYNTHETIC NVIDIA-LIKE CORP",
    }
    assert counts["names_filled"] == 1
    assert counts["not_in_sec_list"] == 3


@pytest.mark.db
@pytest.mark.parametrize("title", ["", "   ", None, 123, ["SYNTHETIC"]])
def test_a_blank_or_non_string_title_is_not_used(engine, no_retries, title):
    security(engine, "ZZBT")
    before = snapshot(engine)
    seed_names().run(engine, client(FakeList([("ZZBT", title)])))
    assert snapshot(engine) == before


@pytest.mark.db
def test_only_the_name_column_changes(engine, no_retries):
    security(
        engine,
        "ZZNA",
        exchange="NASDAQ",
        currency="USD",
        security_type="common",
        country="US",
        is_active=True,
    )
    before = snapshot(engine)["ZZNA"]
    seed_names().run(engine, client(FakeList(ROWS)))
    after = snapshot(engine)["ZZNA"]
    changed = {k for k in before if before[k] != after[k]} - {"xmin"}
    assert changed == {"name"}


@pytest.mark.db
def test_a_rerun_fills_nothing(engine, no_retries):
    security(engine, "ZZNA")
    security(engine, "IBIT")
    seed_names().run(engine, client(FakeList(ROWS)))
    before = snapshot(engine)
    counts = seed_names().run(engine, client(FakeList(ROWS)))
    assert counts["names_filled"] == 0
    assert snapshot(engine) == before


# ── the request ──────────────────────────────────────────────


@pytest.mark.db
def test_one_request_to_secs_list_with_only_the_user_agent(engine, no_retries):
    for symbol in ("ZZNA", "ZZAT", "ZZB.B", "IBIT"):
        security(engine, symbol)
    fake = FakeList(ROWS)
    seed_names().run(engine, client(fake))
    assert len(fake.requests) == 1
    url, params, headers = fake.requests[0]
    assert url == MAP_URL and not params
    assert urlsplit(url).query == ""
    assert headers == {"User-Agent": UA}


@pytest.mark.db
@pytest.mark.parametrize(
    "answer",
    [
        FakeResponse(503, {"message": "synthetic provider body"}),
        FakeResponse(403, {"message": "synthetic provider body"}),
        FakeResponse(200, ["synthetic provider body"]),
    ],
    ids=["http_503", "http_403", "not_an_object"],
)
def test_a_failed_or_malformed_list_writes_nothing(engine, no_retries, answer):
    security(engine, "ZZNA")
    before = snapshot(engine)
    step = seed_names()  # outside raises: a missing module is not the failure
    with pytest.raises(Exception) as caught:
        step.run(engine, client(FakeList(ROWS, answer=answer)))
    assert "synthetic provider body" not in str(caught.value)
    assert snapshot(engine) == before


# ── main(): the owner's command ──────────────────────────────


@pytest.fixture
def sec_get(monkeypatch, no_retries):
    """requests.Session.get answers from a FakeList; returns it."""
    fake = FakeList(ROWS)
    monkeypatch.setattr(
        requests.Session, "get", lambda self, url, **kwargs: fake.get(url, **kwargs)
    )
    monkeypatch.setattr(sys, "argv", ["seed_names.py"])
    return fake


def owner_url(engine):
    return engine.url.render_as_string(hide_password=False)


@pytest.mark.db
def test_main_fills_names_and_prints_counts_only(engine, sec_get, monkeypatch, capsys):
    security(engine, "ZZNA")
    security(engine, "IBIT")
    monkeypatch.setenv("OPS_DATABASE_URL", owner_url(engine))
    monkeypatch.setenv("SEC_USER_AGENT", UA)
    assert seed_names().main() == 0
    out = capsys.readouterr()
    counts = json.loads(out.out)
    assert counts["names_filled"] == 1 and counts["not_in_sec_list"] == 1
    assert all(isinstance(v, int) for v in counts.values()), counts
    for output in (out.out, out.err):
        assert UA not in output
        assert "SYNTHETIC" not in output
        assert owner_url(engine) not in output
    assert names(engine)["ZZNA"] == "SYNTHETIC NVIDIA-LIKE CORP"
    assert sec_get.requests and all(
        h == {"User-Agent": UA} for _, _, h in sec_get.requests
    )


@pytest.mark.db
@pytest.mark.parametrize(
    "agent",
    [
        None,
        "",
        "   ",
        "Perennial research you@example.com",
        "Synthetic admin@EXAMPLE.COM",
    ],
    ids=["unset", "empty", "blank", "env_example_placeholder", "example_domain"],
)
def test_main_refuses_a_missing_or_placeholder_user_agent(
    engine, sec_get, monkeypatch, capsys, agent
):
    security(engine, "ZZNA")
    before = snapshot(engine)
    monkeypatch.setenv("OPS_DATABASE_URL", owner_url(engine))
    if agent is None:
        monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    else:
        monkeypatch.setenv("SEC_USER_AGENT", agent)
    assert seed_names().main() == 1
    assert json.loads(capsys.readouterr().out) == {"error_code": "missing_credential"}
    assert sec_get.requests == []
    assert snapshot(engine) == before


@pytest.mark.db
def test_main_refuses_without_an_explicit_owner_url(
    engine, sec_get, monkeypatch, capsys
):
    security(engine, "ZZNA")
    before = snapshot(engine)
    monkeypatch.delenv("OPS_DATABASE_URL", raising=False)
    monkeypatch.setenv("SEC_USER_AGENT", UA)
    assert seed_names().main() == 1
    assert sec_get.requests == []
    assert snapshot(engine) == before


@pytest.mark.db
def test_main_on_a_failed_request_exits_1_and_prints_no_provider_text(
    engine, sec_get, monkeypatch, capsys
):
    security(engine, "ZZNA")
    before = snapshot(engine)
    sec_get.answer = FakeResponse(503, {"message": "synthetic provider body"})
    monkeypatch.setenv("OPS_DATABASE_URL", owner_url(engine))
    monkeypatch.setenv("SEC_USER_AGENT", UA)
    assert seed_names().main() == 1
    out = capsys.readouterr()
    for output in (out.out, out.err):
        assert "synthetic provider body" not in output and UA not in output
    assert snapshot(engine) == before


# ── grants, the API and scheduling ───────────────────────────


@pytest.mark.db
def test_the_importer_still_may_not_update_names(engine, groups):  # noqa: F811
    with engine.connect() as conn:

        def may(column):
            return conn.execute(
                text(
                    "SELECT has_column_privilege(:r, 'perennial.securities', :c, 'UPDATE')"
                ),
                {"r": groups["importer"], "c": column},
            ).scalar_one()

        assert may("exchange") and may("currency")  # the check can see a grant
        assert not may("name")


@pytest.mark.db
def test_the_dashboard_serves_the_filed_name_and_the_ticker_separately(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
    no_retries,
):
    security(engine, "ZZNA")
    security(engine, "IBIT")
    seed_names().run(engine, client(FakeList(ROWS)))
    publish(engine, [record("ZZNA")], [record("IBIT")])
    with dashboard(engine, pr22_roles, monkeypatch) as (_, api):
        served = companies(api.get("/api/watchlist").json())
    assert (served["ZZNA"]["name"], served["ZZNA"]["ticker"]) == (
        "SYNTHETIC NVIDIA-LIKE CORP",
        "ZZNA",
    )
    assert (served["IBIT"]["name"], served["IBIT"]["ticker"]) == ("IBIT", "IBIT")


def test_the_step_is_never_scheduled():
    backend = REPO / "development" / "backend" / "src"
    automated = [
        backend / "pipeline" / "runner.py",
        *sorted(
            (backend / "services" / "consensus_watchlist" / "scheduler").rglob("*.py")
        ),
        *sorted((REPO / ".github" / "workflows").glob("*.y*ml")),
    ]
    assert automated
    for path in automated:
        assert "seed_names" not in path.read_text(), path.name
    for path in sorted((REPO / "development").rglob("*")):
        if path.is_file() and (
            path.name.startswith(("Dockerfile", "Procfile"))
            or path.suffix in (".service", ".timer", ".cron")
        ):
            assert "seed_names" not in path.read_text(errors="ignore"), path
