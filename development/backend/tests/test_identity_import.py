"""M4 importer resolution: root registration, alias cases, failures, replay."""

import json
from pathlib import Path

import pytest
from sqlalchemy import func, insert, select
from src.db.models import (
    ingestion_runs,
    securities,
    security_identifiers,
    source_batches,
)
from src.ingestion.identity import apply_seeds, load_manifest
from src.ingestion.importer import CollectionOutcome, import_collection

pytestmark = pytest.mark.db

MANIFEST = Path(__file__).resolve().parents[2] / "database/seeds/identity.json"


@pytest.fixture
def seeded(engine):
    with engine.begin() as conn:
        apply_seeds(conn, load_manifest(MANIFEST))
    return engine


def ark(*tickers):
    return [
        {"ticker": t, "funds": ["ARKK"], "fund_count": 1, "total_weight": 1.0}
        for t in tickers
    ]


def congress(*tickers):
    return [
        {
            "politician_name": "Synthetic Member",
            "ticker": t,
            "trade_type": "Purchase",
            "transaction_date": "2026-08-14",
        }
        for t in tickers
    ]


def short_interest(*tickers):
    return [{"ticker": t, "settlement_date": "2026-09-15"} for t in tickers]


def imported(engine, source, records):
    return import_collection(
        engine, CollectionOutcome.success(source, "fixture", records)
    )


def security_ids(engine, source, batch_id):
    from src.db.models import SOURCE_TABLES

    table = SOURCE_TABLES[source]
    with engine.connect() as conn:
        return (
            conn.execute(
                select(securities.c.symbol)
                .join(table, table.c.security_id == securities.c.id)
                .where(table.c.batch_id == batch_id)
                .order_by(securities.c.symbol)
            )
            .scalars()
            .all()
        )


def totals(engine):
    with engine.connect() as conn:
        return tuple(
            conn.execute(select(func.count()).select_from(t)).scalar_one()
            for t in (securities, security_identifiers, source_batches)
        )


def diagnostics(engine, run_id):
    with engine.connect() as conn:
        return conn.execute(
            select(ingestion_runs.c.diagnostics).where(ingestion_runs.c.id == run_id)
        ).scalar_one()


def test_root_registers_unknown_ticker_with_alias_and_count(engine):
    first = imported(engine, "congress_trades", congress("NEWCO", "NEWCO"))
    assert first.succeeded
    assert diagnostics(engine, first.run_id) == {"securities_registered": 1}
    with engine.connect() as conn:
        assert conn.execute(
            select(security_identifiers.c.value)
            .join(securities)
            .where(securities.c.symbol == "NEWCO")
        ).scalars().all() == ["NEWCO"]
    again = imported(engine, "ark_holdings", ark("NEWCO"))
    assert again.succeeded and diagnostics(engine, again.run_id) is None
    assert totals(engine)[0] == 1


@pytest.mark.parametrize(
    "source,records",
    [
        ("short_interest", short_interest("ZZAA", "NEWCO")),
        (
            "insider_trades",
            [
                {
                    "ticker": "NEWCO",
                    "insider_name": "Synthetic",
                    "transaction_type": "P - Purchase",
                    "transaction_date": "2026-09-01",
                }
            ],
        ),
    ],
)
def test_derived_source_never_creates_identity(engine, source, records):
    assert imported(engine, "ark_holdings", ark("ZZAA")).succeeded
    before = totals(engine)
    result = imported(engine, source, records)
    assert not result.succeeded and result.error_code == "unresolved_security"
    assert result.error_summary.endswith("NEWCO (unresolved)")
    assert totals(engine) == before
    assert diagnostics(engine, result.run_id)["failures"] == [
        {"unit": "NEWCO", "code": "unresolved"}
    ]


def test_pr18_aliases_resolve_to_seeded_securities(seeded):
    before = totals(seeded)
    result = imported(seeded, "congress_trades", congress("BRK-B", "GOOG", "GOOGL"))
    assert result.succeeded and diagnostics(seeded, result.run_id) is None
    assert security_ids(seeded, "congress_trades", result.batch_id) == [
        "BRK.B",
        "GOOG",
        "GOOGL",
    ]
    # "RKLB UQ" is normalized to RKLB on NASDAQ, which agrees with the seed.
    rklb = imported(seeded, "ark_holdings", ark("RKLB UQ", "GOOG", "GOOGL"))
    assert rklb.succeeded
    assert security_ids(seeded, "ark_holdings", rklb.batch_id) == [
        "GOOG",
        "GOOGL",
        "RKLB",
    ]
    assert totals(seeded)[:2] == before[:2]


def test_suffix_exchange_must_agree_with_seed(seeded):
    result = imported(seeded, "ark_holdings", ark("RKLB UN"))
    assert result.error_code == "security_conflict"
    assert result.error_summary.endswith("RKLB (exchange_conflict)")


def test_two_spellings_of_one_security_cannot_share_a_one_row_batch(seeded):
    before = totals(seeded)
    result = imported(seeded, "ark_holdings", ark("BRK.B", "BRK-B"))
    assert result.error_code == "security_conflict"
    assert {f["code"] for f in diagnostics(seeded, result.run_id)["failures"]} == {
        "alias_collision"
    }
    assert totals(seeded) == before
    # Row-per-trade sources may carry both spellings; both land on one security.
    trades = imported(seeded, "congress_trades", congress("BRK.B", "BRK-B"))
    assert security_ids(seeded, "congress_trades", trades.batch_id) == [
        "BRK.B",
        "BRK.B",
    ]


def test_inactive_security_is_not_reregistered(seeded):
    with seeded.begin() as conn:
        conn.execute(
            securities.update()
            .where(securities.c.symbol == "RKLB")
            .values(is_active=False)
        )
    before = totals(seeded)
    result = imported(seeded, "ark_holdings", ark("RKLB", "NEWCO"))
    assert result.error_code == "unresolved_security"
    assert result.error_summary.endswith("RKLB (inactive_security)")
    assert totals(seeded) == before


def test_unresolved_failure_rolls_back_registration(engine):
    assert imported(engine, "ark_holdings", ark("OLDCO")).succeeded
    with engine.begin() as conn:
        conn.execute(
            securities.update()
            .where(securities.c.symbol == "OLDCO")
            .values(is_active=False)
        )
    before = totals(engine)
    assert not imported(engine, "congress_trades", congress("OLDCO", "NEWCO")).succeeded
    assert totals(engine) == before


def test_alias_added_after_publication_does_not_change_replay(engine, fixture_path):
    from src.pipeline.publication import current, publish, replay
    from test_pipeline import caps, inputs

    runs, batches = inputs(engine, fixture_path)
    publication = publish(engine, runs, caps(engine, batches))
    stored = current(engine)["output"]
    with engine.begin() as conn:
        zzaa = conn.execute(
            select(securities.c.id).where(securities.c.symbol == "ZZAA")
        ).scalar_one()
        conn.execute(
            insert(security_identifiers).values(
                scheme="ticker", value="FMP:ZZAA-X", security_id=zzaa
            )
        )
    assert replay(engine, publication) == stored
    assert json.dumps(replay(engine, publication), sort_keys=True) == json.dumps(
        stored, sort_keys=True
    )
