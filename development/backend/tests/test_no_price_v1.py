"""No ARK share price on any API path (decision 95, L5 (a); checker F1 on
fix/dashboard-no-price @ 201b1ee), spec-first.

/api/watchlist already serves price null. But GET /v1/watchlist serves
publications.output and GET /v1/securities/{symbol} serves the record, and
both still carried signals.ark.share_price, which ranking.py publishes. The
coordinator's row of 2026-10-29 asks for both fixes:

(i) The root: a new publication's ARK signal has no share_price. The ranking
    module's rule is that a change is a new version, never an edit to an old
    one (ranking.py's docstring), and rollback requires an old publication to
    replay byte-identically under its stored ranking_version. So: a new
    default ranking VERSION whose ARK signal is v4's without share_price, and
    v1-v4 unchanged (they keep share_price).
(ii) Serve time: both /v1 paths drop signals.ark.share_price from stored
    publications that still have it (staging keeps older ones). Everything
    else is still served as stored, and the stored row isn't changed.
Plus: no stored share price value appears in any API response body.

Unchanged and not tested here: ARK ingest and the ark_holdings.share_price
column (the quality threshold still reads it).
"""

import json
from datetime import date
from decimal import Decimal

import pytest
from pr22_support import pr22_roles  # noqa: F401  (pytest fixture)
from sqlalchemy import select
from src.db import models
from test_dashboard_sector import dashboard, publish, record
from test_degraded_publishing import Feed, run
from test_ranking import GENERATED_AT, purchase, rows_for
from test_read_api import seed_security

OLD_VERSIONS = ("watchlist-v1", "watchlist-v2", "watchlist-v3", "watchlist-v4")
ARK_KEYS = {"funds", "fund_count", "total_weight"}
PRICES = ("12.75", "4321.987654", "98.765432")  # distinctive: no other field holds them


def keys(value):
    """Every dict key anywhere inside a JSON value."""
    if isinstance(value, dict):
        for key, inner in value.items():
            yield key
            yield from keys(inner)
    elif isinstance(value, list):
        for inner in value:
            yield from keys(inner)


def without_share_price(output):
    """A publication output (or record) with signals.ark.share_price removed."""
    copy = json.loads(json.dumps(output))

    def strip(rec):
        ark = (rec.get("signals") or {}).get("ark")
        if isinstance(ark, dict):
            ark.pop("share_price", None)

    if "signals" in copy:
        strip(copy)
    for bucket in ("popular_stable", "affordable_growing", "unresolved"):
        for rec in copy.get(bucket) or ():
            if isinstance(rec, dict):
                strip(rec)
    return copy


# ── (i) the ranking ──────────────────────────────────────────


def ark_rows():
    # ZZBB's purchase is dated after its disclosure: v4 ignores it, so the new
    # version must too (it is v4 in every other respect).
    typo = {
        **purchase("ZZBB", number=2),
        "transaction_date": date(2026, 10, 5),
        "disclosure_date": date(2026, 10, 2),
    }
    rows = rows_for([purchase(), typo])
    rows["ark_holdings"] = [
        dict(
            symbol="ZZAA",
            funds=["ARKK"],
            fund_count=1,
            total_weight=Decimal("2.5"),
            share_price=Decimal("30"),
        )
    ]
    return rows


def calculate(version=None):
    from src.pipeline import ranking

    rows = ark_rows()
    caps = {"ZZAA": Decimal("20000000000"), "ZZBB": Decimal("20000000000")}
    if version is None:
        return ranking.calculate(rows, caps, GENERATED_AT)
    return ranking.calculate(rows, caps, GENERATED_AT, version=version)


def ark_signal(output):
    [rec] = [
        r
        for b in ("popular_stable", "affordable_growing")
        for r in output[b]
        if r["ticker"] == "ZZAA"
    ]
    return rec["signals"]["ark"]


def test_a_new_ranking_version_is_the_default_and_the_old_ones_remain():
    from src.pipeline import ranking

    assert ranking.VERSION not in OLD_VERSIONS
    assert ranking.VERSION in ranking.VERSIONS
    assert set(OLD_VERSIONS) <= set(ranking.VERSIONS)


def test_the_default_versions_ark_signal_has_no_share_price():
    signal = ark_signal(calculate())
    assert set(signal) == ARK_KEYS
    assert signal["total_weight"] == Decimal("2.5") and signal["fund_count"] == 1


@pytest.mark.parametrize("version", OLD_VERSIONS)
def test_old_versions_still_publish_it_so_old_publications_replay(version):
    assert ark_signal(calculate(version))["share_price"] == Decimal("30")


def test_the_new_version_is_v4_without_the_share_price():
    new, old = calculate(), calculate("watchlist-v4")
    assert "ZZBB" not in json.dumps(old, default=str)  # v4's filter applied
    assert new["ranking_version"] != old["ranking_version"]
    old["ranking_version"] = new["ranking_version"]
    assert new == without_share_price(old)


# ── (i) through the pipeline, and rollback ───────────────────


def stored_output(engine, publication_id):
    with engine.connect() as conn:
        return conn.execute(
            select(models.publications.c.output).where(
                models.publications.c.id == publication_id
            )
        ).scalar_one()


def old_style_then_new(engine, fixture_path, monkeypatch):
    """A publication made the way publish() did before the fix (v4), then one now."""
    from src.pipeline import publication, ranking

    feed = Feed(fixture_path)
    with monkeypatch.context() as m:
        m.setattr(publication, "VERSION", "watchlist-v4")
        m.setattr(
            publication,
            "calculate",
            lambda *a, **k: ranking.calculate(*a, **{**k, "version": "watchlist-v4"}),
        )
        old = run(engine, feed)
    feed.change("congress_trades")
    new = run(engine, feed)
    assert old != new
    assert "share_price" in set(keys(stored_output(engine, old))), (
        "precondition: the fixture's ARK holdings reach the published records"
    )
    return old, new


@pytest.mark.db
def test_a_new_publication_carries_no_share_price(engine, fixture_path, monkeypatch):
    _, new = old_style_then_new(engine, fixture_path, monkeypatch)
    output = stored_output(engine, new)
    assert "share_price" not in set(keys(output))
    arks = [
        r["signals"]["ark"]
        for b in ("popular_stable", "affordable_growing", "unresolved")
        for r in output[b]
        if r["signals"].get("ark")
    ]
    assert arks and all(set(a) == ARK_KEYS for a in arks)


@pytest.mark.db
def test_an_old_publication_still_replays_and_can_be_rolled_back_to(
    engine, fixture_path, monkeypatch
):
    from src.pipeline.publication import current, replay, rollback

    old, new = old_style_then_new(engine, fixture_path, monkeypatch)
    assert replay(engine, old) == stored_output(engine, old)
    assert replay(engine, new) == stored_output(engine, new)
    rollback(engine, old, actor="synthetic-op", reason="synthetic rollback check")
    assert current(engine)["id"] == old


# ── (ii) serve time ──────────────────────────────────────────


def old_publication(engine):
    """A stored publication from before the fix: its ARK signals carry share_price."""
    ark = {"funds": ["ARKK"], "fund_count": 1, "total_weight": 2.5}
    affordable = [
        record(
            "ZZPA", signals={"ark": {**ark, "share_price": 12.75}, "congress": None}
        ),
        record("ZZPB", signals={"congress": {"buy_count": 1}, "ark": None}),
    ]
    popular = [
        record(
            "ZZPC",
            signals={"ark": {**ark, "share_price": 4321.987654}, "congress": None},
        )
    ]
    unresolved = [
        record(
            "ZZPD",
            signals={"ark": {**ark, "share_price": 98.765432}, "congress": None},
            warnings=["market_cap_unavailable"],
        )
    ]
    for r in (*affordable, *popular, *unresolved):
        seed_security(
            engine,
            r["ticker"],
            name=f"Synthetic {r['ticker']}",
            profile={"sector": "Energy"},
        )
    return publish_all(engine, affordable, popular, unresolved)


def publish_all(engine, affordable, popular, unresolved):
    """test_dashboard_sector.publish, with records in the unresolved bucket too."""
    from sqlalchemy import update

    output = publish(engine, affordable, popular)
    output["unresolved"] = unresolved
    with engine.begin() as conn:
        conn.execute(update(models.publications).values(output=output))
    return output


def canonical(content):
    return json.dumps(
        json.loads(content), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


@pytest.mark.db
def test_v1_watchlist_drops_a_stored_share_price(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    output = old_publication(engine)
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        response = client.get("/v1/watchlist")
    assert response.status_code == 200, response.text
    assert response.content == canonical(response.content)
    watchlist = response.json()["watchlist"]
    assert "share_price" not in set(keys(watchlist))
    for price in PRICES:
        assert price not in response.text
    # Everything else is served as stored.
    assert watchlist == without_share_price(output)
    zzpa = watchlist["affordable_growing"][0]
    assert zzpa["signals"]["ark"] == {
        "funds": ["ARKK"],
        "fund_count": 1,
        "total_weight": 2.5,
    }


@pytest.mark.db
@pytest.mark.parametrize("ticker", ["ZZPA", "ZZPC", "ZZPD"])
def test_v1_securities_drops_a_stored_share_price(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
    ticker,
):
    output = old_publication(engine)
    stored = next(
        r
        for b in ("affordable_growing", "popular_stable", "unresolved")
        for r in output[b]
        if r["ticker"] == ticker
    )
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        response = client.get(f"/v1/securities/{ticker}")
    assert response.status_code == 200, response.text
    assert response.content == canonical(response.content)
    entry = response.json()["current"]["entry"]
    assert entry == without_share_price(stored)
    assert "share_price" not in set(keys(response.json()))
    for price in PRICES:
        assert price not in response.text


@pytest.mark.db
def test_serving_leaves_the_stored_publication_unchanged(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    output = old_publication(engine)
    with engine.connect() as conn:
        pid = conn.execute(
            select(models.current_publication.c.publication_id)
        ).scalar_one()
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        client.get("/v1/watchlist")
        client.get("/v1/securities/ZZPA")
    assert stored_output(engine, pid) == output  # still has share_price


@pytest.mark.db
def test_no_api_response_body_contains_a_stored_share_price(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
):
    old_publication(engine)
    with dashboard(engine, pr22_roles, monkeypatch) as (app, client):
        # Every GET route the app documents (included routers aren't in app.routes).
        documented = client.get("/openapi.json").json()["paths"]
        paths = sorted(
            path for path, ops in documented.items() if "get" in ops and "{" not in path
        )
        paths += ["/openapi.json", "/docs", "/redoc"]
        paths += [f"/v1/securities/{t}" for t in ("ZZPA", "ZZPB", "ZZPC", "ZZPD")]
        assert "/api/watchlist" in paths and "/v1/watchlist" in paths
        bodies = {path: client.get(path).text for path in paths}
    for path, body in bodies.items():
        for price in PRICES:
            assert price not in body, path
        assert "share_price" not in body, path


@pytest.mark.db
def test_after_a_rollback_to_an_old_publication_v1_serves_no_share_price(
    engine,
    pr22_roles,  # noqa: F811
    monkeypatch,
    fixture_path,
):
    from src.pipeline.publication import rollback

    old, _ = old_style_then_new(engine, fixture_path, monkeypatch)
    rollback(engine, old, actor="synthetic-op", reason="synthetic rollback check")
    with dashboard(engine, pr22_roles, monkeypatch) as (_, client):
        body = client.get("/v1/watchlist").json()
    assert body["publication"]["id"] == old
    assert "share_price" not in set(keys(body))
    assert body["watchlist"] == without_share_price(stored_output(engine, old))
