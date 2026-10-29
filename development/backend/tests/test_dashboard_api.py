from datetime import datetime, timezone

from src.api.dashboard import company_from_record, dashboard_response_from_publication


def test_company_adapter_preserves_available_display_fields():
    company = company_from_record(
        {
            "ticker": "aapl",
            "name": "Apple",
            "sector": "Technology",
            "price": 225.5,
            "change_percent": 1.25,
            "confidence_score": 84,
            "signals": {"congress": {"buy_count": 2}, "insider": None},
            "warnings": [],
        },
        "popular_stable",
    )

    assert company.ticker == "AAPL"
    assert company.name == "Apple"
    assert company.sector == "Technology"
    assert company.price is None  # decision 95 (L5 a): no price is shown
    assert company.change_percent == 1.25
    assert company.confidence_score == 84
    assert company.sentiment == "Positive"


def test_company_adapter_uses_safe_fallbacks_for_current_ranking_payload():
    company = company_from_record(
        {
            "ticker": "ZZAA",
            "signals": {"congress": None, "ark": None},
            "warnings": ["market_cap_unavailable"],
        },
        "affordable_growing",
    )

    assert company.name == "ZZAA"
    assert company.sector == "Unclassified"
    assert company.price is None
    assert company.change_percent is None
    assert company.confidence_score is None
    assert company.sentiment == "Watch"


def test_dashboard_response_maps_both_published_buckets():
    response = dashboard_response_from_publication(
        {
            "created_at": datetime(2026, 10, 14, tzinfo=timezone.utc),
            "output": {
                "schema_version": "1.0",
                "generated_at": "2026-10-14T00:00:00+00:00",
                "affordable_growing": [
                    {"ticker": "PLTR", "signals": {}, "warnings": []}
                ],
                "popular_stable": [{"ticker": "MSFT", "signals": {}, "warnings": []}],
            },
        }
    )

    assert response.schema_version == "1.0"
    assert response.generated_at == "2026-10-14T00:00:00+00:00"
    assert [item.ticker for item in response.affordable_growing] == ["PLTR"]
    assert [item.ticker for item in response.popular_stable] == ["MSFT"]
