"""Demo mode: the real pipeline against recorded provider fixtures, demo DB only.

The production collectors run unchanged; HTTP is answered from the P1-C
synthetic provider fixtures (tests/fixtures/providers), with every fixture date
shifted so the newest is yesterday, so freshness rules pass without being
relaxed. Runs are recorded as collection_mode 'fixture' and publish only through
publish(demo=True), which the marked local demo database alone accepts
(pipeline/demo_guard.py). Provider keys are never read: the demo passes
placeholders that no provider ever sees.
"""

import copy
import json
import re
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator
from unittest import mock

import requests
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.pipeline.demo_guard import DEMO_MARKER, DEMO_MODES, check_demo_database

FIXTURES = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "providers"
PLACEHOLDER_KEY = "demo-replay-not-a-key"
DEMO_MARKET_CAP = 20_000_000_000
_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_US = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")


def _dates(value: Any) -> Iterator[date]:
    if isinstance(value, dict):
        for v in value.values():
            yield from _dates(v)
    elif isinstance(value, list):
        for v in value:
            yield from _dates(v)
    elif isinstance(value, str):
        if m := _ISO.match(value):
            yield date(int(m[1]), int(m[2]), int(m[3]))
        elif m := _US.match(value):
            yield date(int(m[3]), int(m[1]), int(m[2]))


def _shift(value: Any, delta: timedelta) -> Any:
    if isinstance(value, dict):
        return {k: _shift(v, delta) for k, v in value.items()}
    if isinstance(value, list):
        return [_shift(v, delta) for v in value]
    if isinstance(value, str):
        if m := _ISO.match(value):
            return (date(int(m[1]), int(m[2]), int(m[3])) + delta).isoformat()
        if m := _US.match(value):
            return (date(int(m[3]), int(m[1]), int(m[2])) + delta).strftime("%m/%d/%Y")
    return value


class _Response:
    """Enough of requests.Response for the collectors' streamed reader."""

    def __init__(self, body: Any, status: int = 200):
        self.status_code = status
        self._raw = json.dumps(body).encode()
        self.headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(self._raw)),
        }
        self.content, self.text, self.ok = self._raw, self._raw.decode(), status < 400

    def iter_content(
        self, chunk_size: int = 1, decode_unicode: bool = False
    ) -> Iterator[bytes]:
        step = chunk_size or 1
        for i in range(0, len(self._raw), step):
            yield self._raw[i : i + step]

    def json(self, **kwargs: Any) -> Any:
        return json.loads(self._raw)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"http {self.status_code}")

    def close(self) -> None:
        pass

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


@dataclass
class DemoReplay:
    """Serves the fixtures by URL. outage/thin take provider names:
    'fmp', 'ark', 'finnhub', 'nasdaq'. outage answers 404 (collection fails);
    thin answers with no rows for that provider (an empty batch)."""

    today: date = field(default_factory=lambda: datetime.now(timezone.utc).date())
    outage: set[str] = field(default_factory=set)
    thin: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        raw = {
            name: json.loads((FIXTURES / path).read_text())
            for name, path in {
                "senate": "fmp/senate.json",
                "house": "fmp/house.json",
                "ark": "ark/holdings.json",
                "finnhub": "finnhub/insider_transactions.json",
                "nasdaq": "nasdaq/short_interest.json",
            }.items()
        }
        latest = max(d for payload in raw.values() for d in _dates(payload))
        delta = (self.today - timedelta(days=1)) - latest
        self.payloads = {name: _shift(payload, delta) for name, payload in raw.items()}

    def get(self, url: str, params: Any = None, **kwargs: Any) -> _Response:
        params = params or {}
        provider, body = self._route(url, params)
        if provider in self.outage:
            return _Response({}, status=404)
        if provider in self.thin:
            body = self._empty(provider, body)
        return _Response(body)

    def _route(self, url: str, params: dict[str, Any]) -> tuple[str, Any]:
        if "financialmodelingprep.com" in url and url.endswith("/profile"):
            symbol = params["symbol"]
            return "fmp", [
                {"symbol": symbol, "marketCap": DEMO_MARKET_CAP, "currency": "USD"}
            ]
        if "financialmodelingprep.com" in url and url.endswith("-latest"):
            chamber = url.rsplit("/", 1)[1].split("-")[0]
            if int(params.get("page", 0)) != 0:
                return "fmp", []
            rows = copy.deepcopy(self.payloads[chamber])
            for row in rows:
                # Demo-only shim, reported as a finding for dev-sources: the P1-C
                # contract allows "district": null, but fmp.parse_trades calls
                # .strip() on it and the Congress collection fails (parse_error).
                if row.get("district") is None:
                    row["district"] = ""
            return "fmp", rows
        if "arkfunds.io" in url:
            return "ark", {
                "symbol": params["symbol"],
                **copy.deepcopy(self.payloads["ark"]),
            }
        if "finnhub.io" in url:
            symbol = params["symbol"]
            body = copy.deepcopy(self.payloads["finnhub"])
            body["symbol"] = symbol
            for row in body["data"]:
                row["symbol"] = symbol
            return "finnhub", body
        if "api.nasdaq.com" in url:
            return "nasdaq", copy.deepcopy(self.payloads["nasdaq"])
        raise ValueError("demo replay has no fixture for this endpoint")

    @staticmethod
    def _empty(provider: str, body: Any) -> Any:
        if provider == "finnhub":
            return {**body, "data": []}
        return body  # other providers' thin variants are not needed by the walkthrough


@contextmanager
def replaying(replay: DemoReplay) -> Iterator[None]:
    """Route every requests.Session.get to the replay; drop provider pacing."""
    from src.ingestion import collectors

    def get(
        session: requests.Session, url: str, params: Any = None, **kwargs: Any
    ) -> _Response:
        return replay.get(url, params=params, **kwargs)

    def forbidden(
        session: requests.Session, method: str, url: str, *a: Any, **k: Any
    ) -> None:
        raise RuntimeError("demo mode makes no network requests")

    with ExitStack() as stack:
        stack.enter_context(mock.patch.object(requests.Session, "get", get))
        stack.enter_context(mock.patch.object(requests.Session, "request", forbidden))
        for name in ("ark", "insider", "short_interest"):
            module = collectors._fetcher(name)
            if hasattr(module, "REQUEST_DELAY"):
                stack.enter_context(mock.patch.object(module, "REQUEST_DELAY", 0))
        yield


def demo_settings(database_url: str) -> Any:
    from src.settings import Settings

    return Settings(
        database_url=SecretStr(database_url),
        fmp_api_key=SecretStr(PLACEHOLDER_KEY),
        finnhub_api_key=SecretStr(PLACEHOLDER_KEY),
    )


def run_demo(engine: Engine, replay: DemoReplay) -> int:
    """One full pipeline run against the replay, published into the demo DB."""
    from src.pipeline.runner import collect_market_caps, run_pipeline

    with replaying(replay):
        return run_pipeline(
            engine,
            settings=demo_settings(engine.url.render_as_string(hide_password=False)),
            demo=True,
            market_cap_collector=lambda symbols: collect_market_caps(
                symbols, api_key=PLACEHOLDER_KEY
            ),
        )


def create_demo_database(admin: Engine, name: str = "perennial_demo") -> None:
    """Create (if absent), mark and migrate a local demo database."""
    from alembic import command
    from alembic.config import Config

    from src.db.session import make_engine
    from src.pipeline.demo_guard import DEMO_DATABASE

    if not DEMO_DATABASE.fullmatch(name):
        raise ValueError("demo database names must match perennial_demo[_suffix]")
    with admin.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": name}
        ).first()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{name}"'))
        conn.execute(text(f"COMMENT ON DATABASE \"{name}\" IS '{DEMO_MARKER}'"))
    engine = make_engine(
        admin.url.set(database=name).render_as_string(hide_password=False),
        connect_args={"hostaddr": admin.url.host or "127.0.0.1"},
    )
    try:
        with engine.connect() as conn:
            check_demo_database(conn)
            cfg = Config(
                str(Path(__file__).resolve().parents[3] / "database" / "alembic.ini")
            )
            cfg.attributes["connection"] = conn
            command.upgrade(cfg, "head")
    finally:
        engine.dispose()


def walkthrough(engine: Engine, today: date | None = None) -> list[dict[str, Any]]:
    """The scripted demo; each step returns a compact summary (ids and codes only)."""
    from src.db.models import SOURCES, publication_events
    from src.db.queries import batch_rows, latest_successful_run
    from src.pipeline import publication
    from src.pipeline.health import health
    from src.pipeline.publication import (
        PublicationQualityError,
        current,
        publish,
        replay,
    )
    from src.pipeline.ranking import candidate_symbols
    from src.pipeline.runner import collect_market_caps

    with engine.connect() as conn:
        check_demo_database(conn)
    steps: list[dict[str, Any]] = []
    feed = DemoReplay(today=today) if today else DemoReplay()

    def events(pub_id: int) -> list[str]:
        with engine.connect() as conn:
            return [
                r.kind
                for r in conn.execute(
                    publication_events.select().where(
                        publication_events.c.publication_id == pub_id
                    )
                )
            ]

    first = run_demo(engine, feed)
    served = current(engine)
    assert served is not None
    steps.append({"step": "publish", "publication_id": first, "events": events(first)})
    steps.append(
        {
            "step": "current",
            "publication_id": served["id"],
            "demo_marker": served["ranking_config"].get("demo") is True,
            "tickers": sum(
                len(served["output"][b])
                for b in ("popular_stable", "affordable_growing")
            ),
        }
    )
    steps.append(
        {"step": "replay", "byte_identical": replay(engine, first) == served["output"]}
    )

    feed.outage = {"ark"}
    degraded = run_demo(engine, feed)
    feed.outage = set()
    steps.append(
        {
            "step": "source outage (ARK)",
            "publication_id": degraded,
            "events": events(degraded),
        }
    )

    feed.thin = {"finnhub"}
    try:
        run_demo(engine, feed)
        refused: dict[str, Any] = {"step": "quality refusal", "refused": False}
    except PublicationQualityError as exc:
        refused = {
            "step": "quality refusal",
            "refused": True,
            "error_code": exc.code,
            "checks": list(exc.checks),
        }
    feed.thin = set()
    steps.append(refused)

    with engine.connect() as conn:
        latest = {s: latest_successful_run(conn, s) for s in SOURCES}
        rows = {s: batch_rows(conn, s, r.batch_id) for s, r in latest.items() if r}
    with replaying(feed):
        caps = collect_market_caps(candidate_symbols(rows), api_key=PLACEHOLDER_KEY)
    overridden = publish(
        engine,
        {s: r.run_id for s, r in latest.items() if r},
        caps,
        demo=True,
        override_reason="demo: no qualifying insider buys in the recorded day",
        override_actor="demo-operator",
        override_checks=refused.get("checks") or [],
    )
    steps.append(
        {
            "step": "audited override",
            "publication_id": overridden,
            "events": events(overridden),
        }
    )

    rollback = getattr(publication, "rollback", None)
    if rollback is not None:  # arrives with the M3 code (PR #32)
        rollback(
            engine,
            first,
            actor="demo-operator",
            reason="demo: return to the first publication",
        )
        steps.append(
            {"step": "rollback", "publication_id": first, "events": events(first)}
        )
    else:
        steps.append(
            {"step": "rollback", "skipped": "rollback API not merged yet (PR #32)"}
        )

    report = health(engine, modes=DEMO_MODES)
    steps.append(
        {
            "step": "health",
            "severity": report.get("severity"),
            "issues": report["issues"],
        }
    )
    return steps
