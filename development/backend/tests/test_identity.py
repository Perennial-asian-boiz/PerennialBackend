"""M4 security identity: migration backfill, aliases, seeds, routing, classification."""

import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy import insert, select, text
from sqlalchemy.exc import IntegrityError
from src.db.models import (
    company_profiles,
    market_cap_observations,
    metadata,
    securities,
    security_identifiers,
)
from src.ingestion.identity import (
    INACTIVE,
    UNRESOLVED,
    SeedConflict,
    apply_seeds,
    backfill_profile_classification,
    load_manifest,
    request_symbol,
    resolve_tickers,
)
from tests.conftest import alembic_config

pytestmark = pytest.mark.db

MANIFEST_PATH = Path(__file__).resolve().parents[2] / "database/seeds/identity.json"
T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)


@pytest.fixture
def manifest():
    return load_manifest(MANIFEST_PATH)


@pytest.fixture
def seeded(engine, manifest):
    with engine.begin() as conn:
        apply_seeds(conn, manifest)
    return engine


def sid(conn, symbol):
    return conn.execute(
        select(securities.c.id).where(securities.c.symbol == symbol)
    ).scalar_one()


def aliases(conn):
    return set(
        conn.execute(
            select(
                security_identifiers.c.scheme,
                security_identifiers.c.value,
                securities.c.symbol,
            ).join(securities)
        ).all()
    )


def test_migration_gives_existing_securities_their_canonical_alias(migrated_engine):
    with migrated_engine.connect() as conn:
        cfg = alembic_config(conn)
        # Downgrades refuse while Congress history exists, so start empty.
        names = ", ".join(f"perennial.{t.name}" for t in metadata.sorted_tables)
        conn.execute(text(f"TRUNCATE {names} RESTART IDENTITY CASCADE"))
        conn.commit()
        command.downgrade(cfg, "0006")
        conn.execute(
            text(
                "INSERT INTO perennial.securities (symbol, exchange) "
                "VALUES ('BRK.B', 'NYSE'), ('ZZAA', NULL)"
            )
        )
        conn.commit()
        command.upgrade(cfg, "head")
        conn.commit()
        assert aliases(conn) == {
            ("ticker", "BRK.B", "BRK.B"),
            ("ticker", "ZZAA", "ZZAA"),
        }
        row = conn.execute(select(securities).where(securities.c.symbol == "ZZAA"))
        assert row.mappings().one()["is_active"] is True
        conn.execute(text("TRUNCATE perennial.securities RESTART IDENTITY CASCADE"))
        conn.commit()


def test_alias_and_metadata_constraints(engine):
    with engine.begin() as conn:
        first = conn.execute(
            insert(securities).values(symbol="AAA").returning(securities.c.id)
        ).scalar_one()
        second = conn.execute(
            insert(securities).values(symbol="BBB").returning(securities.c.id)
        ).scalar_one()
        conn.execute(
            insert(security_identifiers).values(
                scheme="ticker", value="AAA", security_id=first
            )
        )
    bad = [
        (security_identifiers, dict(scheme="ticker", value="AAA", security_id=second)),
        (security_identifiers, dict(scheme="isin", value="X", security_id=first)),
        (security_identifiers, dict(scheme="ticker", value=" a ", security_id=first)),
        (security_identifiers, dict(scheme="ticker", value="", security_id=first)),
        (securities, dict(symbol="CCC", security_type="bond")),
        (securities, dict(symbol="CCC", country="us")),
        (securities, dict(symbol="CCC", figi="short")),
        (company_profiles, dict(cik="123")),
        (company_profiles, dict(legal_name="  ")),
    ]
    for table, values in bad:
        with pytest.raises(IntegrityError), engine.begin() as conn:
            conn.execute(insert(table).values(**values))
    with engine.begin() as conn:
        conn.execute(insert(securities).values(symbol="FIGI1", figi="BBG000000001"))
    with pytest.raises(IntegrityError), engine.begin() as conn:
        conn.execute(insert(securities).values(symbol="FIGI2", figi="BBG000000001"))


def test_seeds_are_idempotent_and_keep_share_classes_distinct(seeded, manifest):
    with seeded.begin() as conn:
        assert apply_seeds(conn, manifest) == {
            "securities_added": 0,
            "aliases_added": 0,
            "profiles_added": 0,
        }
        goog, googl = sid(conn, "GOOG"), sid(conn, "GOOGL")
        assert goog != googl
        profiles = conn.execute(
            select(securities.c.symbol, securities.c.company_profile_id).where(
                securities.c.symbol.in_(["GOOG", "GOOGL"])
            )
        ).all()
        assert len({p.company_profile_id for p in profiles}) == 1
        profile = conn.execute(select(company_profiles)).mappings().one()
        assert profile["legal_name"] == "Alphabet Inc."
        assert profile["insider_reporting_security_id"] == googl
        assert profile["cik"] is None
        assert (
            conn.execute(
                select(securities.c.figi).where(securities.c.figi.is_not(None))
            ).all()
            == []
        )
        assert ("ticker", "BRK-B", "BRK.B") in aliases(conn)


def test_seed_keeps_existing_ids_and_fills_only_missing_metadata(engine, manifest):
    with engine.begin() as conn:
        existing = conn.execute(
            insert(securities).values(symbol="RKLB").returning(securities.c.id)
        ).scalar_one()
        conn.execute(
            insert(security_identifiers).values(
                scheme="ticker", value="RKLB", security_id=existing
            )
        )
        apply_seeds(conn, manifest)
        row = (
            conn.execute(select(securities).where(securities.c.symbol == "RKLB"))
            .mappings()
            .one()
        )
    assert row["id"] == existing and row["exchange"] == "NASDAQ"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda m: m["aliases"].append(
            {"scheme": "ticker", "value": "BRK-B", "symbol": "GOOG"}
        ),
        lambda m: m["securities"].append({"symbol": "RKLB", "exchange": "NYSE"}),
        lambda m: m["profiles"].append(
            {"legal_name": "Other", "members": ["GOOG", "BRK.B"]}
        ),
        lambda m: m["profiles"].append(
            {"members": ["RKLB"], "insider_reporting_symbol": "GOOGL"}
        ),
        lambda m: m["aliases"].append(
            {"scheme": "ticker", "value": "X", "symbol": "UNKNOWN"}
        ),
    ],
)
def test_conflicting_seed_rolls_back_everything(engine, manifest, mutate):
    changed = copy.deepcopy(manifest)
    mutate(changed)
    with pytest.raises(SeedConflict), engine.begin() as conn:
        apply_seeds(conn, changed)
    with engine.connect() as conn:
        assert conn.execute(select(securities.c.id)).all() == []


def test_resolution_uses_provider_alias_then_canonical(seeded):
    with seeded.begin() as conn:
        brk = sid(conn, "BRK.B")
        ids, failures = resolve_tickers(
            conn, "congress_trades", ["BRK.B", "BRK-B", "GOOG", "NOPE"]
        )
        assert ids == {"BRK.B": brk, "BRK-B": brk, "GOOG": sid(conn, "GOOG")}
        assert failures == [("NOPE", UNRESOLVED)]
        # ARK "RKLB UQ" is normalized to RKLB/NASDAQ by validation before lookup.
        assert resolve_tickers(conn, "ark_holdings", ["RKLB"])[0] == {
            "RKLB": sid(conn, "RKLB")
        }
        conn.execute(
            securities.update()
            .where(securities.c.symbol == "RKLB")
            .values(is_active=False)
        )
        assert resolve_tickers(conn, "ark_holdings", ["RKLB"]) == (
            {},
            [("RKLB", INACTIVE)],
        )


def test_provider_alias_is_scoped_to_its_provider(engine):
    with engine.begin() as conn:
        a = conn.execute(
            insert(securities).values(symbol="AAA").returning(securities.c.id)
        ).scalar_one()
        b = conn.execute(
            insert(securities).values(symbol="BBB").returning(securities.c.id)
        ).scalar_one()
        conn.execute(
            insert(security_identifiers),
            [
                dict(scheme="ticker", value="AAA", security_id=a),
                dict(scheme="ticker", value="BBB", security_id=b),
                dict(scheme="ticker", value="FINNHUB:AAA", security_id=b),
            ],
        )
        assert resolve_tickers(conn, "insider_trades", ["AAA"])[0] == {"AAA": b}
        assert resolve_tickers(conn, "congress_trades", ["AAA"])[0] == {"AAA": a}


def test_stored_identifiers_reproduce_pr18_request_spellings(seeded):
    """The data replaces insider.provider_symbol for the three #18 cases."""
    from src.services.consensus_watchlist.fetchers.insider import provider_symbol

    with seeded.begin() as conn:
        for symbol in ("BRK.B", "GOOG", "GOOGL", "RKLB"):
            stored = request_symbol(conn, sid(conn, symbol), "FINNHUB")
            assert stored == provider_symbol(symbol)
        assert provider_symbol("RKLB UQ") == "RKLB"
        # Routing never changes other providers' spellings or merges securities.
        assert request_symbol(conn, sid(conn, "GOOG"), "NASDAQ") == "GOOG"
        assert request_symbol(conn, sid(conn, "BRK.B"), "NASDAQ") == "BRK.B"


def test_two_request_spellings_are_ambiguous(seeded):
    with seeded.begin() as conn:
        conn.execute(
            insert(security_identifiers).values(
                scheme="ticker", value="FINNHUB:BRKB", security_id=sid(conn, "BRK.B")
            )
        )
        with pytest.raises(LookupError):
            request_symbol(conn, sid(conn, "BRK.B"), "FINNHUB")


def observe(conn, symbol, sector, industry, at, observed=True):
    conn.execute(
        insert(market_cap_observations).values(
            security_id=sid(conn, symbol),
            value=1,
            currency="USD",
            provider="fmp",
            observed_at=at if observed else None,
            retrieved_at=at,
            sector=sector,
            industry=industry,
        )
    )


def test_classification_backfill_latest_wins_conflicts_stay_empty(seeded):
    with seeded.begin() as conn:
        observe(conn, "RKLB", "Old", "Old", T0)
        observe(conn, "RKLB", "Industrials", None, T0 + timedelta(days=1))
        observe(conn, "RKLB", None, "Aerospace", T0 + timedelta(days=2), False)
        observe(conn, "GOOG", "Communication", "Internet", T0)
        observe(conn, "GOOGL", "Technology", "Internet", T0)
        counts = backfill_profile_classification(conn)
        assert counts == {"profiles_added": 1, "fields_filled": 3, "conflicts": 1}
        rows = {
            r.symbol: (r.sector, r.industry)
            for r in conn.execute(
                select(
                    securities.c.symbol,
                    company_profiles.c.sector,
                    company_profiles.c.industry,
                ).join(
                    company_profiles,
                    company_profiles.c.id == securities.c.company_profile_id,
                )
            )
        }
        assert rows["RKLB"] == ("Industrials", "Aerospace")
        assert rows["GOOG"] == rows["GOOGL"] == (None, "Internet")
        # Stored values are never overwritten; reruns add nothing.
        observe(conn, "RKLB", "Changed", "Changed", T0 + timedelta(days=9))
        assert backfill_profile_classification(conn) == {
            "profiles_added": 0,
            "fields_filled": 0,
            "conflicts": 1,
        }


def test_seed_cli_refuses_without_explicit_url(monkeypatch, capsys):
    import sys

    sys.path.insert(0, str(MANIFEST_PATH.parents[1] / "ops"))
    import seed_identity

    monkeypatch.delenv("OPS_DATABASE_URL", raising=False)
    monkeypatch.setattr(sys, "argv", ["seed_identity.py"])
    assert seed_identity.main() == 1
    assert "OPS_DATABASE_URL" not in capsys.readouterr().out


def test_seed_cli_run_reports_counts_only(engine):
    import sys

    sys.path.insert(0, str(MANIFEST_PATH.parents[1] / "ops"))
    import seed_identity

    counts = seed_identity.run(engine, MANIFEST_PATH)
    assert counts["securities_added"] == 4 and counts["profiles_added"] == 1
    assert seed_identity.run(engine, MANIFEST_PATH)["aliases_added"] == 0
