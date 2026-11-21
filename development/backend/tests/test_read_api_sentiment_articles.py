"""S3-02 AC3: GET /v1/securities/{symbol}/sentiment/articles. Synthetic rows only.

Spec: docs/technical-specs/SENTIMENT_PULSE.md §14.

Pinned:
* The same reading as /sentiment: the security's row in the newest ticker_sentiment
  batch. Its news items (never social) ranked by the weight the tally gave them,
  heaviest first, ties to the newer article; at most 5.
* Each article: rank, headline, outlet, url, published_at, label, weight (6 places)
  and share = its weight / every counted article's weight (4 places). count is the
  number of counted articles.
* 200 "unavailable" with no_reading / no_breakdown, like /sentiment; 404 unknown
  ticker, 422 invalid; no-cache, ETag, 304.
* Served as perennial_api with its column grants: never an article's summary,
  finnhub_id, match_rule or content_hash.
"""

import json

import pytest
from sqlalchemy import text
from test_read_api import (  # noqa: F401
    REPO,
    SNAPSHOT,
    _router_app,
    _v1_openapi,
    api,
    assert_error,
    groups,
    seed_security,
)
from test_read_api_sentiment import reading


def get(client, symbol, **headers):
    return client.get(f"/v1/securities/{symbol}/sentiment/articles", headers=headers)


# (label, weight): article n is published n days before T0 (n from 1)
SEVEN = [
    ("positive", "0.1"),
    ("negative", "0.9"),
    ("neutral", "0.3"),
    ("positive", "0.5"),
    ("positive", "0.05"),
    ("negative", "0.15"),
    ("neutral", "0.0"),
]


@pytest.mark.db
def test_the_five_heaviest_articles_heaviest_first(engine, api):  # noqa: F811
    reading(engine, "ZZAA", SEVEN, [("0.6", "positive"), ("0.4", "positive")])
    response = get(api, "ZZAA")
    assert response.status_code == 200
    body = response.json()
    assert (body["status"], body["tallied_on"], body["count"]) == ("ok", "2026-11-17", 7)
    assert [a["headline"] for a in body["articles"]] == [
        "Synthetic headline 2",
        "Synthetic headline 4",
        "Synthetic headline 3",
        "Synthetic headline 6",
        "Synthetic headline 1",
    ]
    first = body["articles"][0]
    assert set(first) == {
        "rank", "headline", "outlet", "url", "published_at", "label", "weight", "share"}
    assert first["rank"] == 1 and first["label"] == "negative" and first["outlet"] == "Yahoo"
    assert first["url"].startswith("https://finnhub.io/api/news?id=")
    assert first["published_at"] == "2026-11-15T12:00:00+00:00"
    assert first["weight"] == 0.9
    # 0.9 / (0.1+0.9+0.3+0.5+0.05+0.15+0.0 = 2.0): social weights don't count
    assert [a["share"] for a in body["articles"]] == [0.45, 0.25, 0.15, 0.075, 0.05]
    assert [a["rank"] for a in body["articles"]] == [1, 2, 3, 4, 5]


@pytest.mark.db
def test_fewer_than_five_articles_are_all_listed(engine, api):  # noqa: F811
    reading(engine, "ZZAF", [("positive", "0.2"), ("negative", "0.6")])
    body = get(api, "ZZAF").json()
    assert body["count"] == 2
    assert [a["headline"] for a in body["articles"]] == [
        "Synthetic headline 2", "Synthetic headline 1"]
    assert [a["share"] for a in body["articles"]] == [0.75, 0.25]


@pytest.mark.db
def test_equal_weights_go_to_the_newer_article(engine, api):  # noqa: F811
    reading(engine, "ZZAT", [("positive", "0.3")] * 3)
    titles = [a["headline"] for a in get(api, "ZZAT").json()["articles"]]
    assert titles == ["Synthetic headline 1", "Synthetic headline 2", "Synthetic headline 3"]


@pytest.mark.db
def test_weights_that_all_faded_to_zero_share_equally(engine, api):  # noqa: F811
    reading(engine, "ZZAZ", [("positive", "0"), ("negative", "0")])
    assert [a["share"] for a in get(api, "ZZAZ").json()["articles"]] == [0.5, 0.5]


@pytest.mark.db
def test_a_reading_with_only_social_posts_lists_no_articles(engine, api):  # noqa: F811
    reading(engine, "ZZAS", (), [("0.6", "positive")])
    body = get(api, "ZZAS").json()
    assert (body["status"], body["count"], body["articles"]) == ("ok", 0, [])


@pytest.mark.db
def test_the_newest_batch_is_served(engine, api):  # noqa: F811
    reading(engine, "ZZAN", [("positive", "0.9")])
    reading(engine, "ZZAN", [("negative", "0.4"), ("negative", "0.2")])
    body = get(api, "ZZAN").json()
    assert body["count"] == 2 and {a["label"] for a in body["articles"]} == {"negative"}


@pytest.mark.db
def test_no_reading_and_no_breakdown_are_200_unavailable(engine, api):  # noqa: F811
    seed_security(engine, "ZZAU")
    assert get(api, "ZZAU").json() == {
        "api_version": "v1", "symbol": "ZZAU", "status": "unavailable", "reason": "no_reading"}
    batch = reading(engine, "ZZAB", [("positive", "0.5")])
    with engine.begin() as conn:
        conn.execute(text(
            "DELETE FROM perennial.ticker_sentiment_items WHERE ticker_sentiment_id IN"
            " (SELECT id FROM perennial.ticker_sentiment WHERE batch_id = :b)"), {"b": batch})
    assert get(api, "ZZAB").json()["reason"] == "no_breakdown"


@pytest.mark.db
def test_nothing_else_about_an_article_is_served(engine, api):  # noqa: F811
    reading(engine, "ZZAX", [("positive", "0.5")])
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE perennial.news_articles SET summary = 'Secret synthetic summary'"
            " WHERE headline = 'Synthetic headline 1'"))
    body = get(api, "ZZAX").text
    for term in ("Secret synthetic summary", "content_hash", "finnhub_id", "match_rule",
                 "weights-v4", "finbert"):
        assert term not in body, term


# ── errors, ETag and 304 ─────────────────────────────────


@pytest.mark.db
def test_an_unknown_symbol_is_404_unknown_security(engine, api):  # noqa: F811
    assert_error(get(api, "ZZNONE"), 404, "unknown_security")


@pytest.mark.db
@pytest.mark.parametrize("bad", ["%00", "A" * 1000, "ZZ%0AAA"])
def test_an_invalid_symbol_is_422_and_never_echoed(engine, api, bad):  # noqa: F811
    response = get(api, bad)
    assert_error(response, 422, "invalid_request")
    assert "A" * 1000 not in response.text


@pytest.mark.db
def test_etag_and_304(engine, api):  # noqa: F811
    reading(engine, "ZZAE", [("positive", "0.5")])
    first = get(api, "ZZAE")
    etag = first.headers["etag"]
    assert first.headers["cache-control"] == "no-cache"
    cached = get(api, "ZZAE", **{"If-None-Match": etag})
    assert cached.status_code == 304 and cached.content == b""
    reading(engine, "ZZAE", [("negative", "0.5")])
    assert get(api, "ZZAE", **{"If-None-Match": etag}).status_code == 200


@pytest.mark.db
def test_the_body_is_canonical_json(engine, api):  # noqa: F811
    reading(engine, "ZZAJ", SEVEN)
    response = get(api, "ZZAJ")
    assert response.text == json.dumps(
        response.json(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


# ── the route, OpenAPI and docs ──────────────────────────


def test_the_route_is_a_documented_read_route():
    paths = _v1_openapi(_router_app())["paths"]
    operation = paths["/v1/securities/{symbol}/sentiment/articles"]["get"]
    assert {"200", "304", "404", "422", "429"} <= set(operation["responses"])


def test_the_response_schemas_are_named():
    schemas = _v1_openapi(_router_app())["schemas"]
    for name in ("SentimentArticlesResponse", "TopArticle"):
        assert name in schemas, name


def test_the_snapshot_includes_the_articles_route():
    """Regenerated in the same PR (UPDATE_API_SNAPSHOT=1)."""
    paths = json.loads(SNAPSHOT.read_text())["paths"]
    assert "/v1/securities/{symbol}/sentiment/articles" in paths


def test_api_md_documents_the_articles_route():
    content = (REPO / "docs" / "technical-specs" / "API.md").read_text()
    for term in ("/v1/securities/{symbol}/sentiment/articles", "heaviest first", "share"):
        assert term in content, term
