"""Temporary dashboard data copied from the frontend fixture.

This is development/demo data only. Replace it by switching
``DASHBOARD_DATA_MODE`` to ``publication`` when the ingestion pipeline starts
publishing the enriched company metadata.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.api.dashboard import DashboardCompanyResponse, DashboardResponse

_SECTOR_SEEDS: dict[str, dict[str, list[tuple[str, str]]]] = {
    "Technology": {
        "affordable_growing": [
            ("Palantir", "PLTR"),
            ("CrowdStrike", "CRWD"),
            ("Snowflake", "SNOW"),
        ],
        "popular_stable": [
            ("Microsoft", "MSFT"),
            ("Apple", "AAPL"),
            ("NVIDIA", "NVDA"),
        ],
    },
    "Financials": {
        "affordable_growing": [
            ("SoFi Technologies", "SOFI"),
            ("Affirm", "AFRM"),
            ("Robinhood", "HOOD"),
        ],
        "popular_stable": [
            ("JPMorgan Chase", "JPM"),
            ("Visa", "V"),
            ("Goldman Sachs", "GS"),
        ],
    },
    "Healthcare": {
        "affordable_growing": [
            ("Pfizer", "PFE"),
            ("Viatris", "VTRS"),
            ("Hims & Hers Health", "HIMS"),
        ],
        "popular_stable": [
            ("Johnson & Johnson", "JNJ"),
            ("AbbVie", "ABBV"),
            ("Merck", "MRK"),
        ],
    },
    "Industrials": {
        "affordable_growing": [
            ("Blade Air Mobility", "BLDE"),
            ("GATX", "GATX"),
            ("RXO", "RXO"),
        ],
        "popular_stable": [
            ("Caterpillar", "CAT"),
            ("Honeywell", "HON"),
            ("GE Aerospace", "GE"),
        ],
    },
    "Consumer Discretionary": {
        "affordable_growing": [("Etsy", "ETSY"), ("Wayfair", "W"), ("Rivian", "RIVN")],
        "popular_stable": [
            ("Amazon", "AMZN"),
            ("Home Depot", "HD"),
            ("McDonald's", "MCD"),
        ],
    },
    "Consumer Staples": {
        "affordable_growing": [
            ("Kraft Heinz", "KHC"),
            ("Keurig Dr Pepper", "KDP"),
            ("General Mills", "GIS"),
        ],
        "popular_stable": [
            ("Procter & Gamble", "PG"),
            ("Coca-Cola", "KO"),
            ("PepsiCo", "PEP"),
        ],
    },
    "Energy": {
        "affordable_growing": [
            ("Occidental Petroleum", "OXY"),
            ("Plug Power", "PLUG"),
            ("Marathon Oil", "MRO"),
        ],
        "popular_stable": [
            ("Exxon Mobil", "XOM"),
            ("Chevron", "CVX"),
            ("ConocoPhillips", "COP"),
        ],
    },
    "Utilities": {
        "affordable_growing": [
            ("NRG Energy", "NRG"),
            ("AES", "AES"),
            ("Pinnacle West", "PNW"),
        ],
        "popular_stable": [
            ("Duke Energy", "DUK"),
            ("Southern Company", "SO"),
            ("American Electric Power", "AEP"),
        ],
    },
    "Materials": {
        "affordable_growing": [
            ("Alcoa", "AA"),
            ("Cleveland-Cliffs", "CLF"),
            ("Eldorado Gold", "EGO"),
        ],
        "popular_stable": [
            ("Linde", "LIN"),
            ("Sherwin-Williams", "SHW"),
            ("Nucor", "NUE"),
        ],
    },
    "Communication Services": {
        "affordable_growing": [
            ("Snap", "SNAP"),
            ("Roku", "ROKU"),
            ("Paramount Global", "PARA"),
        ],
        "popular_stable": [
            ("Alphabet", "GOOGL"),
            ("Meta Platforms", "META"),
            ("Netflix", "NFLX"),
        ],
    },
    "Real Estate": {
        "affordable_growing": [
            ("Opendoor", "OPEN"),
            ("UWM Holdings", "UWMC"),
            ("Redfin", "RDFN"),
        ],
        "popular_stable": [
            ("Prologis", "PLD"),
            ("American Tower", "AMT"),
            ("Realty Income", "O"),
        ],
    },
}


def _company(
    sector: str, name: str, ticker: str, index: int, bucket: str
) -> DashboardCompanyResponse:
    affordable = bucket == "affordable_growing"
    return DashboardCompanyResponse(
        name=name,
        ticker=ticker,
        sector=sector,
        price=round(24.5 + (index % 9) * 18.35, 2),
        change_percent=round(
            (0.42 + (index % 5) * 0.27) if affordable else (0.18 + (index % 4) * 0.16),
            2,
        ),
        confidence_score=(72 + (index * 5) % 16)
        if affordable
        else (78 + (index * 3) % 12),
        sentiment="Neutral" if index % 4 == 0 else "Positive",
        reason=(
            f"Growth signals are improving across {sector.lower()}."
            if affordable
            else f"Established market position in {sector.lower()}."
        ),
    )


def temporary_dashboard_response() -> DashboardResponse:
    """Return the current frontend fixture through the backend API contract."""

    buckets: dict[str, list[DashboardCompanyResponse]] = {
        "affordable_growing": [],
        "popular_stable": [],
    }
    for sector, seed in _SECTOR_SEEDS.items():
        for bucket, companies in seed.items():
            buckets[bucket].extend(
                _company(sector, name, ticker, index, bucket)
                for index, (name, ticker) in enumerate(companies)
            )

    return DashboardResponse(
        schema_version="1.0",
        generated_at=datetime.now(timezone.utc).isoformat(),
        affordable_growing=buckets["affordable_growing"],
        popular_stable=buckets["popular_stable"],
    )
