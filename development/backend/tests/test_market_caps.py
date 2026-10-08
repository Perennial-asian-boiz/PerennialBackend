"""The fifth provider stage must obey its overall budget and avoid partial success."""
import pytest
from src.pipeline import runner
from src.ingestion import collectors
from src.pipeline.publication import PublicationError


def test_market_cap_unit_receives_remaining_pipeline_deadline(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(runner.time, 'monotonic', lambda: clock[0])
    seen = []
    def get(session, url, **kwargs):
        seen.append(kwargs)
        clock[0] += 2
        return collectors._Response(True, data=[])
    monkeypatch.setattr(collectors, '_get_json', get)
    result = runner.collect_market_caps(['ZZAA', 'ZZBB'], session=object(), api_key='synthetic', max_seconds=5)
    assert [r['deadline_seconds'] for r in seen] == [5, 3]
    assert set(result) == {'ZZAA', 'ZZBB'}


def test_market_cap_overrun_cannot_return_success(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(runner.time, 'monotonic', lambda: clock[0])
    def get(*args, **kwargs):
        clock[0] = 6
        return collectors._Response(True, data=[])
    monkeypatch.setattr(collectors, '_get_json', get)
    with pytest.raises(PublicationError, match='deadline'):
        runner.collect_market_caps(['ZZAA'], session=object(), api_key='synthetic', max_seconds=5)
