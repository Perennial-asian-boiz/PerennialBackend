"""Versioned Congress counting; v1 golden output comes from the f9c73aaa code."""
import json
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import insert, select

from src.db import models
from src.db.queries import batch_rows
from src.ingestion.importer import CollectionOutcome, import_collection
from src.pipeline import ranking
from src.pipeline.lifecycle import begin_run
from src.pipeline.publication import current, publish, replay

GENERATED_AT = datetime(2026, 10, 7, 9, tzinfo=timezone.utc)


def canonical_json_bytes(output):
    return json.dumps(output, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def purchase(symbol='ZZAA', provider='fmp', number=1, **kwargs):
    return dict(symbol=symbol, data_source=provider, row_number=number,
                politician_name='Synthetic Member One', trade_type='Purchase',
                transaction_date=date(2026, 10, 1), disclosure_date=date(2026, 10, 2),
                amount_label='$1,001 - $15,000', **kwargs)


def rows_for(trades):
    return {'congress_trades': trades, 'ark_holdings': [],
            'insider_trades': [], 'short_interest': []}


def calculate(trades, version='watchlist-v2'):
    rows = rows_for(trades)
    return ranking.calculate(rows, {s: Decimal('20000000000') for s in ranking.candidate_symbols(rows)},
                             GENERATED_AT, version=version)


def golden_trades():
    return [purchase(number=1), purchase(provider='senate_watcher', number=2),
            purchase(provider='senate_watcher', number=3),
            purchase('ZZBB', number=4), purchase('ZZBB', number=5)]


def test_v1_output_stays_equal_to_baseline_golden(fixture_path):
    """Generated with scratch-loaded baseline-20261007/src/pipeline/ranking.py."""
    expected = json.loads(fixture_path('ranking_v1_golden.json').read_text())
    assert canonical_json_bytes(calculate(golden_trades(), 'watchlist-v1')) == canonical_json_bytes(expected)


def test_v2_collapses_cross_provider_pair_without_mutating_rows():
    trades = [purchase(), purchase(provider='senate_watcher', number=2)]
    before = deepcopy(trades)
    output = calculate(trades)
    signal = output['popular_stable'][0]['signals']['congress']
    assert signal['buy_count'] == 1 and signal['distinct_buyer_count'] == 1
    assert trades == before


@pytest.mark.parametrize('providers, expected', [
    (['fmp', 'fmp'], 2),
    (['fmp', 'fmp', 'senate_watcher', 'senate_watcher', 'senate_watcher'], 3),
    ([None, None, 'fmp'], 2),
    (['fmp', 'senate_watcher', None], 1),
])
def test_v2_preserves_largest_single_provider_multiplicity(providers, expected):
    trades = [purchase(provider=provider, number=n) for n, provider in enumerate(providers, 1)]
    assert calculate(trades)['popular_stable'][0]['signals']['congress']['buy_count'] == expected


def test_v2_ties_use_lowest_row_number_and_normalized_key():
    first = purchase(provider='senate_watcher', number=1)
    first.update(politician_name=' Synthetic Member One ', trade_type=' PURCHASE ',
                 amount_label='first provider amount', disclosure_date=date(2026, 10, 3))
    second = purchase(number=2)
    second.update(amount_label='discarded amount', disclosure_date=date(2026, 10, 5))
    expected = calculate([first, second])
    assert calculate([second, first]) == expected
    signal = expected['popular_stable'][0]['signals']['congress']
    assert signal['buy_count'] == 1
    assert signal['amount_range'] == 'first provider amount'
    assert signal['most_recent_disclosure_date'] == '2026-10-03'


def test_v2_keeps_largest_provider_metadata_and_independent_trade_dates():
    trades = [purchase(number=1), purchase(provider='senate_watcher', number=2),
              purchase(provider='senate_watcher', number=3), purchase(number=4)]
    trades[0].update(amount_label='discarded', disclosure_date=date(2026, 10, 6))
    trades[1].update(amount_label='kept', disclosure_date=date(2026, 10, 4))
    trades[3].update(transaction_date=date(2026, 9, 30), disclosure_date=None)
    signal = calculate(trades)['popular_stable'][0]['signals']['congress']
    assert signal['buy_count'] == 3
    assert signal['amount_range'] == 'kept'
    assert signal['most_recent_disclosure_date'] == '2026-10-04'


def test_v2_removes_inflation_from_popular_stable_order():
    trades = [purchase(), purchase(provider='senate_watcher', number=2),
              purchase(provider='senate_watcher', number=3),
              purchase('ZZBB', number=4), purchase('ZZBB', number=5)]
    # Give AA three cross-provider observations of one trade, BB two real buys.
    trades[2]['data_source'] = None
    assert [r['ticker'] for r in calculate(trades, 'watchlist-v1')['popular_stable']] == ['ZZAA', 'ZZBB']
    assert [r['ticker'] for r in calculate(trades)['popular_stable']] == ['ZZBB', 'ZZAA']


def test_unknown_ranking_version_is_rejected():
    with pytest.raises(ValueError, match='unsupported ranking version'):
        calculate([], 'watchlist-v99')


def test_v2_keeps_ark_insider_and_short_interest_signals_unchanged():
    rows = rows_for([purchase()])
    rows['ark_holdings'] = [dict(symbol='ZZAA', funds=['ARKK'], fund_count=1,
                                total_weight=Decimal('2.5'), share_price=Decimal('30'))]
    rows['insider_trades'] = [dict(symbol='ZZAA', insider_name='Synthetic Insider',
                                 transaction_type='Purchase', value=Decimal('1000'),
                                 transaction_date=date(2026, 10, 1))]
    rows['short_interest'] = [dict(symbol='ZZAA', short_interest_shares=100,
                                  average_daily_volume=20, days_to_cover=Decimal('5'),
                                  settlement_date=date(2026, 9, 15))]
    caps = {'ZZAA': Decimal('20000000000')}
    old = ranking.calculate(rows, caps, GENERATED_AT, version='watchlist-v1')
    new = ranking.calculate(rows, caps, GENERATED_AT, version='watchlist-v2')
    old['ranking_version'] = new['ranking_version']
    assert old == new


def ranking_inputs(engine, records):
    runs, batches = {}, {}
    for source in models.SOURCES:
        dependencies = {s: batches[s] for s in ('congress_trades', 'ark_holdings')} if len(batches) >= 2 else {}
        run = begin_run(engine, source, 'live', dependencies=dependencies)
        result = import_collection(engine, CollectionOutcome.success(
            source, 'live', records if source == 'congress_trades' else [],
            coverage={'complete': True, 'scope': 'snapshot'},
            source_as_of=datetime.now(timezone.utc).date() if source == 'ark_holdings' else None), run_id=run)
        assert result.succeeded
        runs[source], batches[source] = result.run_id, result.batch_id
    return runs, batches


def raw_records(trades):
    return [dict(ticker=r['symbol'], politician_name=r['politician_name'],
                 trade_type=r['trade_type'], transaction_date=r['transaction_date'].isoformat(),
                 disclosure_date=r['disclosure_date'].isoformat() if r['disclosure_date'] else None,
                 amount_range=r['amount_label'], data_source=r['data_source'], chamber='Senate') for r in trades]


@pytest.mark.db
def test_v1_publication_replays_baseline_output_and_new_publication_is_v2(engine, fixture_path):
    """A stored historical v1 graph replays its independent baseline golden output."""
    expected = json.loads(fixture_path('ranking_v1_golden.json').read_text())
    runs, batches = ranking_inputs(engine, raw_records(golden_trades()))
    with engine.begin() as conn:
        old = conn.execute(insert(models.publications).values(created_at=GENERATED_AT,
            ranking_version='watchlist-v1', ranking_config=ranking.DEFAULT_CONFIG,
            output=expected).returning(models.publications.c.id)).scalar_one()
        conn.execute(insert(models.publication_sources), [dict(publication_id=old, source=s,
            run_id=runs[s], batch_id=batches[s]) for s in models.SOURCES])
        for security in conn.execute(select(models.securities)).mappings():
            observation = conn.execute(insert(models.market_cap_observations).values(
                security_id=security['id'], value=Decimal('20000000000'), currency='USD',
                provider='fmp', retrieved_at=GENERATED_AT).returning(models.market_cap_observations.c.id)).scalar_one()
            conn.execute(insert(models.publication_market_caps).values(publication_id=old,
                security_id=security['id'], observation_id=observation))
    assert canonical_json_bytes(replay(engine, old)) == canonical_json_bytes(expected)
    observations = {s: dict(value=Decimal('20000000000'), currency='USD', provider='fmp',
                            retrieved_at=datetime.now(timezone.utc)) for s in ('ZZAA', 'ZZBB')}
    new = publish(engine, runs, observations)
    saved = current(engine)
    assert saved['ranking_version'] == saved['output']['ranking_version'] == 'watchlist-v2'
    assert replay(engine, new) == saved['output']
    assert canonical_json_bytes(replay(engine, old)) == canonical_json_bytes(expected)
    with engine.connect() as conn:
        assert len(batch_rows(conn, 'congress_trades', batches['congress_trades'])) == 5
    assert saved['output']['popular_stable'][0]['signals']['congress']['buy_count'] == 2


@pytest.mark.db
def test_shared_legacy_golden_keeps_raw_pair_and_ranks_one_buy(engine, fixture_path):
    """Use step 4a's fab7188 golden and preserved pre-split Congress hash."""
    fixture = json.loads(fixture_path('legacy_json_golden_fab7188.json').read_text())
    runs, batches = ranking_inputs(engine, fixture['database']['records'])
    with engine.connect() as conn:
        rows = batch_rows(conn, 'congress_trades', batches['congress_trades'])
        pair = [r for r in rows if r['symbol'] == 'ZZAA']
        assert len(pair) == 2 and {r['data_source'] for r in pair} == {'fmp', 'senate_watcher'}
        stored_hash = conn.execute(select(models.source_batches.c.content_hash).where(
            models.source_batches.c.id == batches['congress_trades'])).scalar_one()
        assert stored_hash == fixture['database']['content_hash']
    observations = {s: dict(value=Decimal('20000000000'), currency='USD', provider='fmp',
                            retrieved_at=datetime.now(timezone.utc)) for s in ('ZZAA', 'ZZBB')}
    pub = publish(engine, runs, observations)
    saved = current(engine)
    signals = {r['ticker']: r['signals']['congress'] for r in saved['output']['popular_stable']}
    assert signals == fixture['expected']['congress_signals']
    assert signals['ZZAA']['buy_count'] == 1
    assert replay(engine, pub) == saved['output']
    with engine.connect() as conn:
        assert batch_rows(conn, 'congress_trades', batches['congress_trades']) == rows
