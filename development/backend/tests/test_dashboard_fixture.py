from src.api.dashboard_fixture import temporary_dashboard_response


def test_temporary_dashboard_matches_frontend_fixture_shape():
    response = temporary_dashboard_response()

    assert len(response.affordable_growing) == 33
    assert len(response.popular_stable) == 33
    assert {company.sector for company in response.affordable_growing} == {
        "Technology",
        "Financials",
        "Healthcare",
        "Industrials",
        "Consumer Discretionary",
        "Consumer Staples",
        "Energy",
        "Utilities",
        "Materials",
        "Communication Services",
        "Real Estate",
    }
    assert response.affordable_growing[0].ticker == "PLTR"
    assert response.popular_stable[0].ticker == "MSFT"
