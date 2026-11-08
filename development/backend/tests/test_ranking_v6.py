"""Ranking watchlist-v6: SEC's public float, labelled (decision 112, #90), spec-first.

Bryan approved decision 112 on #90 (2026-11-01). #98 stores SEC's public float
in market_caps under provider sec_public_float, but v5 would publish it as an
unlabelled market_cap. v6:

(a) A record drops market_cap and carries public_float plus public_float_as_of
    (the measurement date, from market_caps.observed_at). The output names its
    size_basis. Buckets and order are v5's.
(b) Weak values go in size_flags, never warnings (any warnings entry makes the
    dashboard row "Watch"): size_estimate_stale past 548 days, size_unavailable
    on unresolved rows. /v1's market_caps_unavailable counts v5's warning and
    v6's flag.
(c) The gate: a pinned publication uses v6 exactly when its adapter is
    sec_public_float; v6 refuses any other provider's rows, and v5 refuses
    SEC's. Settings refuse MARKET_CAP_ADAPTER=sec_public_float while no ranking
    version labels it (#98 F1/R1).
(d) The dashboard adds public_float, public_float_as_of and size_caution.

Not here: the candidate cap (C2) waits for Bryan's staging dry run.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from test_ranking import GENERATED_AT, purchase, rows_for

V6 = "watchlist-v6"
MEASURED = datetime(2026, 6, 30, tzinfo=timezone.utc)  # a fiscal Q2 end
CAPS = {
    "ZZAA": Decimal("20000000000"),  # popular_stable
    "ZZBB": Decimal("2000000000"),  # affordable_growing
    "ZZCC": None,  # unresolved
}


def ranked_rows():
    return rows_for([purchase("ZZAA"), purchase("ZZBB", number=2), purchase("ZZCC", number=3)])


def as_of(day=MEASURED):
    return {s: (None if v is None else day) for s, v in CAPS.items()}


def calculate(version=V6, **kwargs):
    from src.pipeline import ranking

    kwargs.setdefault("size_as_of", as_of())
    return ranking.calculate(ranked_rows(), CAPS, GENERATED_AT, version=version, **kwargs)


def records(output):
    return {
        r["ticker"]: r
        for b in ("popular_stable", "affordable_growing", "unresolved")
        for r in output[b]
    }


# ── (a) the label ────────────────────────────────────────


def test_v6_records_carry_the_public_float_and_its_as_of_date_not_a_market_cap():
    rec = records(calculate())["ZZAA"]
    assert "market_cap" not in rec
    assert rec["public_float"] == 20000000000.0
    assert rec["public_float_as_of"] == "2026-06-30"
    assert rec["size_flags"] == [] and rec["warnings"] == []


def test_the_as_of_date_is_the_utc_measurement_day():
    late = datetime(2026, 6, 30, 23, 30, tzinfo=timezone(timedelta(hours=-5)))
    rec = records(calculate(size_as_of=as_of(late)))["ZZAA"]
    assert rec["public_float_as_of"] == "2026-07-01"


def test_v6_output_names_its_size_basis_and_v5_does_not():
    assert calculate()["size_basis"] == "sec_public_float"
    assert calculate()["ranking_version"] == V6
    assert "size_basis" not in calculate("watchlist-v5")


def test_v6_buckets_and_order_are_v5s():
    def placement(output):
        return {
            b: [(r["ticker"], r["rank"]) for r in output[b]]
            for b in ("popular_stable", "affordable_growing", "unresolved")
        }

    assert placement(calculate()) == placement(calculate("watchlist-v5"))


def test_v6_has_no_ark_share_price():
    from src.pipeline import ranking

    rows = ranked_rows()
    rows["ark_holdings"] = [
        dict(symbol="ZZAA", funds=["ARKK"], fund_count=1,
             total_weight=Decimal("2.5"), share_price=Decimal("30"))
    ]
    out = ranking.calculate(rows, CAPS, GENERATED_AT, version=V6, size_as_of=as_of())
    assert records(out)["ZZAA"]["signals"]["ark"] == {
        "funds": ["ARKK"], "fund_count": 1, "total_weight": 2.5,
    }


def test_v5_is_unchanged_by_size_as_of():
    assert calculate("watchlist-v5") == calculate("watchlist-v5", size_as_of=None)
    rec = records(calculate("watchlist-v5"))["ZZAA"]
    assert rec["market_cap"] == 20000000000.0
    assert not {"public_float", "public_float_as_of", "size_flags"} & set(rec)


def test_a_v6_value_without_an_as_of_date_is_refused():
    with pytest.raises(ValueError, match="needs its as-of date"):
        calculate(size_as_of={})


# ── (b) weak values ──────────────────────────────────────


def test_an_unresolved_v6_record_flags_size_unavailable_and_warns_nothing():
    rec = records(calculate())["ZZCC"]
    assert rec["bucket"] == "unresolved"
    assert rec["public_float"] is None and rec["public_float_as_of"] is None
    assert rec["size_flags"] == ["size_unavailable"]
    assert rec["warnings"] == []


@pytest.mark.parametrize(("days", "stale"), [(548, False), (549, True)])
def test_a_float_older_than_548_days_is_flagged_stale_not_warned(days, stale):
    rec = records(calculate(size_as_of=as_of(GENERATED_AT - timedelta(days=days))))["ZZAA"]
    assert rec["size_flags"] == (["size_estimate_stale"] if stale else [])
    assert rec["warnings"] == []
    assert rec["bucket"] == "popular_stable"


def test_market_caps_unavailable_counts_v5s_warning_and_v6s_flag():
    from src.api.read import _size_unavailable

    v5, v6 = records(calculate("watchlist-v5")), records(calculate())
    assert _size_unavailable(v5["ZZCC"]) and _size_unavailable(v6["ZZCC"])
    assert not _size_unavailable(v5["ZZAA"]) and not _size_unavailable(v6["ZZAA"])
    stale = records(calculate(size_as_of=as_of(GENERATED_AT - timedelta(days=600))))
    assert not _size_unavailable(stale["ZZAA"])


def test_v1_serves_v6_records_as_stored():
    """SEC's public float is public domain: only market_cap is unlicensed."""
    from src.api.read import _without_unlicensed

    out = calculate()
    assert _without_unlicensed(out) == out


# ── (c) the gate ─────────────────────────────────────────


@pytest.mark.parametrize(
    ("provider", "version"),
    [("sec_public_float", V6), ("fixture", "watchlist-v5"), ("stub_live", "watchlist-v5"),
     (None, "watchlist-v5")],
)
def test_the_size_provider_picks_the_version(provider, version):
    from src.pipeline import ranking

    assert ranking.version_for(provider) == version
    assert ranking.VERSION == "watchlist-v5"  # the default for every other path


def settings(adapter):
    from src.settings import Settings

    return Settings(
        _env_file=None,
        database_url="postgresql+psycopg://user:synthetic@localhost/test",
        market_cap_adapter=adapter,
    )


def test_settings_accept_sec_public_float_now_that_v6_labels_it():
    assert settings("sec_public_float").market_cap_adapter == "sec_public_float"


def test_settings_refuse_sec_public_float_while_no_version_labels_it(monkeypatch):
    from pydantic import ValidationError
    from src.pipeline import ranking

    monkeypatch.setattr(ranking, "SIZE_BASIS", {})
    with pytest.raises(ValidationError, match="needs a ranking version that labels it"):
        settings("sec_public_float")
    assert settings("fixture").market_cap_adapter == "fixture"


# ── (d) the dashboard ────────────────────────────────────


def test_the_dashboard_shows_the_public_float_and_its_date():
    from src.api.dashboard import company_from_record

    row = company_from_record(records(calculate())["ZZAA"], "popular_stable", {})
    assert row.public_float == 20000000000.0
    assert row.public_float_as_of == "2026-06-30"
    assert row.size_caution is False
    assert row.sentiment == "Positive"


def test_a_stale_float_is_a_caution_not_a_watch():
    from src.api.dashboard import company_from_record

    out = calculate(size_as_of=as_of(GENERATED_AT - timedelta(days=600)))
    row = company_from_record(records(out)["ZZAA"], "popular_stable", {})
    assert row.size_caution is True
    assert row.sentiment == "Positive"


def test_a_v5_dashboard_row_has_no_public_float():
    from src.api.dashboard import company_from_record

    row = company_from_record(records(calculate("watchlist-v5"))["ZZAA"], "popular_stable", {})
    assert (row.public_float, row.public_float_as_of, row.size_caution) == (None, None, False)


# ── (c) the gate at publish (database) ───────────────────


def _publish_helpers():
    import test_market_caps_publish as helpers

    return helpers


@pytest.mark.db
def test_a_sec_public_float_publication_is_v6_and_replays(engine, fixture_path):
    from sqlalchemy import select
    from src.db import models
    from test_degraded_publishing import Feed, import_all

    h = _publish_helpers()
    measured = datetime.now(timezone.utc) - timedelta(days=30)
    runs = import_all(engine, Feed(fixture_path))
    h.cap_run(engine, {**h.every("20000000000.00"), "ZZDD": None},
              name="sec_public_float", observed_at=measured)
    pub = h.publish_pinned(engine, runs, market_cap_adapter="sec_public_float")

    out = h.output(engine, pub)
    with engine.connect() as conn:
        stored = conn.execute(
            select(models.publications.c.ranking_version).where(models.publications.c.id == pub)
        ).scalar_one()
    assert stored == out["ranking_version"] == V6
    assert out["size_basis"] == "sec_public_float"
    recs = h.records(out)
    assert recs["ZZAA"]["public_float"] == 20000000000.0
    assert recs["ZZAA"]["public_float_as_of"] == measured.date().isoformat()
    assert recs["ZZDD"]["size_flags"] == ["size_unavailable"]
    assert all("market_cap" not in r for r in recs.values())
    h.assert_replays(engine, pub)


@pytest.mark.db
def test_v6_refuses_rows_from_another_provider(engine, fixture_path):
    from src.pipeline.publication import PublicationError
    from test_degraded_publishing import Feed, import_all

    h = _publish_helpers()
    runs = import_all(engine, Feed(fixture_path))
    h.cap_run(engine, h.every("20000000000.00"))  # stub_live: a market cap
    with pytest.raises(PublicationError, match="provider does not match ranking version"):
        h.publish_pinned(engine, runs, market_cap_adapter="sec_public_float")


@pytest.mark.db
def test_v5_refuses_sec_public_float_rows(engine, fixture_path):
    from src.pipeline.publication import PublicationError
    from test_degraded_publishing import Feed, import_all

    h = _publish_helpers()
    runs = import_all(engine, Feed(fixture_path))
    h.cap_run(engine, h.every("20000000000.00"), name="sec_public_float",
              observed_at=datetime.now(timezone.utc) - timedelta(days=30))
    with pytest.raises(PublicationError, match="provider does not match ranking version"):
        h.publish_pinned(engine, runs)


@pytest.mark.db
def test_v6_refuses_a_public_float_without_an_as_of_date(engine, fixture_path):
    from src.pipeline.publication import PublicationError
    from test_degraded_publishing import Feed, import_all

    h = _publish_helpers()
    runs = import_all(engine, Feed(fixture_path))
    h.cap_run(engine, h.every("20000000000.00"), name="sec_public_float")
    with pytest.raises(PublicationError, match="public float has no as-of date"):
        h.publish_pinned(engine, runs, market_cap_adapter="sec_public_float")


@pytest.mark.db
def test_the_pipeline_with_the_sec_adapter_publishes_v6(engine, fixture_path):
    from test_degraded_publishing import Feed
    from test_market_caps_source import quote

    h = _publish_helpers()
    measured = datetime.now(timezone.utc) - timedelta(days=30)

    class SecAdapter(h.CapAdapter):
        def fetch(self, symbols, *, session=None, sleep=None):
            return {s: quote("20000000000.00", observed_at=measured) for s in symbols}

    pub = h.new_run(engine, Feed(fixture_path), SecAdapter(name="sec_public_float"))
    out = h.output(engine, pub)
    assert out["ranking_version"] == V6
    assert all(r["public_float_as_of"] == measured.date().isoformat()
               for r in h.records(out).values())
    h.assert_replays(engine, pub)
