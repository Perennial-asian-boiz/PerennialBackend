"""S3-02 piece 1: GET /v1/securities/{symbol}/sentiment. Synthetic rows only.

Spec: docs/technical-specs/SENTIMENT_PULSE.md.

Pinned:
* The reading is the security's row in the newest ticker_sentiment batch; its
  ticker_sentiment_items (0026) are summed per channel. News shares are weighted
  (sum of a label's weights / sum of weights), social shares plain (count of a
  label / count), each to 4 places, with per-label counts.
* A channel with fewer than min_items (3) items is "not_enough_coverage" with
  shares null; the other channel is unaffected.
* 200 status "unavailable": no_reading (no batch, or no row for the security in
  the newest batch) and no_breakdown (a row without items). 404 unknown_security
  only for an unknown ticker, 422 for an invalid one. Cache-Control no-cache, an
  ETag that changes with the reading, 304 on If-None-Match.
* Served as perennial_api, reading only its granted columns: never a headline,
  URL, post id or score.
"""

import json
import secrets
from datetime import date, datetime, timedelta, timezone

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
from test_scores_schema import _batch, _insert

T0 = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)
TALLIED = date(2026, 11, 17)


def _security_id(engine, symbol, *alias):
    with engine.connect() as conn:
        found = conn.execute(
            text("SELECT id FROM perennial.securities WHERE symbol = :s"), {"s": symbol}
        ).scalar_one_or_none()
    return found if found is not None else seed_security(engine, symbol, *alias)


def _article(conn, security_id, n):
    batch = _batch(conn, "news_articles")
    fid = secrets.randbelow(10**12) + 1
    return _insert(
        conn,
        "news_articles",
        {
            "finnhub_id": fid,
            "url": f"https://finnhub.io/api/news?id={fid}",
            "headline": f"Synthetic headline {n}",
            "outlet": "Yahoo",
            "published_at": T0 - timedelta(days=n),
            "content_retrieved": "headline_only",
            "match_rule": "ticker",
            "content_hash": secrets.token_hex(32),
        },
        batch_id=batch,
        security_id=security_id,
    )


def _post(conn, security_id, n, score, label):
    batch = _batch(conn, "social_posts")
    return _insert(
        conn,
        "social_posts",
        {"platform": "bluesky", "post_id": f"post-{n}", "score": score, "label": label},
        batch_id=batch,
        security_id=security_id,
    )


def reading(engine, symbol, news=(), social=(), *alias, batch=None):
    """A ticker_sentiment row for `symbol` in a new (or the given) batch, with
    items: news as (label, weight), social as (score, label). Returns the batch."""
    security_id = _security_id(engine, symbol, *alias)
    with engine.begin() as conn:
        batch = batch or _batch(conn, "ticker_sentiment")
        counted = len(news) + len(social)
        row_number = conn.execute(
            text("SELECT count(*) + 1 FROM perennial.ticker_sentiment WHERE batch_id = :b"),
            {"b": batch},
        ).scalar_one()
        row = _insert(
            conn,
            "ticker_sentiment",
            {
                "row_number": row_number,
                "value": "0.1000" if counted else None,
                "plain_average": "0.1000" if counted else None,
                "article_count": counted,
                "news_count": len(news),
                "social_count": len(social),
                "first_at": T0 - timedelta(days=7) if counted else None,
                "last_at": T0 if counted else None,
                "weights_version": "weights-v4",
                "model_version": "finbert" if news else None,
                "social_sentiment": "0.0000",
                "tallied_on": TALLIED,
            },
            batch_id=batch,
            security_id=security_id,
        )
        for n, (label, weight) in enumerate(news, start=1):
            conn.execute(
                text(
                    "INSERT INTO perennial.ticker_sentiment_items (ticker_sentiment_id, kind,"
                    " news_article_id, score, label, weight, item_at) VALUES"
                    " (:r, 'news', :a, 0.5, :l, :w, :at)"
                ),
                {"r": row, "a": _article(conn, security_id, n), "l": label, "w": weight,
                 "at": T0 - timedelta(days=n)},
            )
        for n, (score, label) in enumerate(social, start=1):
            conn.execute(
                text(
                    "INSERT INTO perennial.ticker_sentiment_items (ticker_sentiment_id, kind,"
                    " social_post_id, score, label, weight, item_at) VALUES"
                    " (:r, 'social', :p, :s, :l, 0.18, :at)"
                ),
                {"r": row, "p": _post(conn, security_id, n, score, label), "s": score,
                 "l": label, "at": datetime(2026, 11, 16, 23, 59, 59, tzinfo=timezone.utc)},
            )
    return batch


def get(client, symbol, **headers):
    return client.get(f"/v1/securities/{symbol}/sentiment", headers=headers)


NEWS = [("positive", "0.5"), ("positive", "0.1"), ("neutral", "0.3"), ("negative", "0.1")]
SOCIAL = [("0.6", "positive"), ("-0.5", "negative"), ("0.0", "neutral"), ("0.3", "positive")]


# ── the breakdown ────────────────────────────────────────


@pytest.mark.db
def test_news_is_weighted_and_social_is_plain(engine, api):  # noqa: F811
    reading(engine, "ZZSA", NEWS, SOCIAL)
    response = get(api, "ZZSA")
    assert response.status_code == 200
    assert response.json() == {
        "api_version": "v1",
        "symbol": "ZZSA",
        "status": "ok",
        "tallied_on": "2026-11-17",
        "min_items": 3,
        "news": {
            "status": "ok",
            "weighted": True,
            "count": 4,
            "first_at": "2026-11-13T12:00:00+00:00",
            "last_at": "2026-11-16T12:00:00+00:00",
            "counts": {"positive": 2, "neutral": 1, "negative": 1},
            # 0.6 / 1.0, 0.3 / 1.0, 0.1 / 1.0: not 2/4, 1/4, 1/4
            "shares": {"positive": 0.6, "neutral": 0.3, "negative": 0.1},
        },
        "social": {
            "status": "ok",
            "weighted": False,
            "count": 4,
            "first_at": "2026-11-16T23:59:59+00:00",
            "last_at": "2026-11-16T23:59:59+00:00",
            "counts": {"positive": 2, "neutral": 1, "negative": 1},
            "shares": {"positive": 0.5, "neutral": 0.25, "negative": 0.25},
        },
    }


@pytest.mark.db
def test_shares_are_rounded_to_four_places_and_sum_to_one(engine, api):  # noqa: F811
    reading(engine, "ZZSR", [("positive", "1"), ("neutral", "1"), ("negative", "1")])
    shares = get(api, "ZZSR").json()["news"]["shares"]
    assert shares == {"positive": 0.3333, "neutral": 0.3333, "negative": 0.3333}
    assert abs(sum(shares.values()) - 1) <= 0.0002


@pytest.mark.db
def test_coverage_is_judged_per_channel(engine, api):  # noqa: F811
    reading(engine, "ZZSC", NEWS[:3], SOCIAL[:2])
    body = get(api, "ZZSC").json()
    assert body["news"]["status"] == "ok" and body["news"]["count"] == 3
    assert body["social"] == {
        "status": "not_enough_coverage",
        "weighted": False,
        "count": 2,
        "first_at": "2026-11-16T23:59:59+00:00",
        "last_at": "2026-11-16T23:59:59+00:00",
        "counts": {"positive": 1, "neutral": 0, "negative": 1},
        "shares": None,
    }


@pytest.mark.db
def test_an_empty_channel_has_no_dates(engine, api):  # noqa: F811
    reading(engine, "ZZSE", NEWS)
    social = get(api, "ZZSE").json()["social"]
    assert social["count"] == 0 and social["first_at"] is None and social["last_at"] is None
    assert social["status"] == "not_enough_coverage" and social["shares"] is None


@pytest.mark.db
def test_news_whose_weights_all_faded_to_zero_falls_back_to_plain_shares(engine, api):  # noqa: F811
    reading(engine, "ZZSZ", [("positive", "0"), ("positive", "0"), ("negative", "0")])
    assert get(api, "ZZSZ").json()["news"]["shares"] == {
        "positive": 0.6667, "neutral": 0.0, "negative": 0.3333}


# ── which reading ────────────────────────────────────────


@pytest.mark.db
def test_the_newest_batch_is_served(engine, api):  # noqa: F811
    reading(engine, "ZZSN", NEWS)
    reading(engine, "ZZSN", [("negative", "1")] * 3)
    assert get(api, "ZZSN").json()["news"]["counts"] == {
        "positive": 0, "neutral": 0, "negative": 3}


@pytest.mark.db
def test_a_security_left_out_of_the_newest_batch_has_no_reading(engine, api):  # noqa: F811
    reading(engine, "ZZSO", NEWS)
    reading(engine, "ZZSP", NEWS)  # newer batch, without ZZSO
    assert get(api, "ZZSO").json() == {
        "api_version": "v1", "symbol": "ZZSO", "status": "unavailable", "reason": "no_reading"}


@pytest.mark.db
def test_a_reading_without_items_has_no_breakdown(engine, api):  # noqa: F811
    batch = reading(engine, "ZZSB", NEWS)
    with engine.begin() as conn:  # a row tallied before 0026: counted, but no items
        conn.execute(text(
            "DELETE FROM perennial.ticker_sentiment_items WHERE ticker_sentiment_id IN"
            " (SELECT id FROM perennial.ticker_sentiment WHERE batch_id = :b)"), {"b": batch})
    assert get(api, "ZZSB").json() == {
        "api_version": "v1", "symbol": "ZZSB", "status": "unavailable", "reason": "no_breakdown"}


@pytest.mark.db
def test_nothing_tallied_yet_is_200_no_reading_not_404(engine, api):  # noqa: F811
    seed_security(engine, "ZZSU")
    response = get(api, "ZZSU")
    assert response.status_code == 200
    assert response.json()["reason"] == "no_reading"


@pytest.mark.db
def test_an_alias_finds_the_canonical_security(engine, api):  # noqa: F811
    reading(engine, "ZZS.A", NEWS, (), "ZZS-A")
    body = get(api, "ZZS-A").json()
    assert body["symbol"] == "ZZS.A" and body["status"] == "ok"


@pytest.mark.db
def test_nothing_the_api_may_not_read_is_served(engine, api):  # noqa: F811
    reading(engine, "ZZSX", NEWS, SOCIAL)
    body = get(api, "ZZSX").text
    for term in ("Synthetic headline", "finnhub.io", "post-1", "weights-v4", "finbert"):
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
    reading(engine, "ZZST", NEWS)
    first = get(api, "ZZST")
    etag = first.headers["etag"]
    assert first.headers["cache-control"] == "no-cache"
    assert get(api, "ZZST").headers["etag"] == etag
    cached = get(api, "ZZST", **{"If-None-Match": etag})
    assert cached.status_code == 304 and cached.content == b""
    reading(engine, "ZZST", NEWS)
    fresh = get(api, "ZZST", **{"If-None-Match": etag})
    assert fresh.status_code == 200 and fresh.headers["etag"] != etag


@pytest.mark.db
def test_the_body_is_canonical_json(engine, api):  # noqa: F811
    reading(engine, "ZZSJ", NEWS)
    response = get(api, "ZZSJ")
    assert response.text == json.dumps(
        response.json(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


# ── the route, OpenAPI and docs ──────────────────────────


def test_the_route_is_a_documented_read_route():
    operation = _v1_openapi(_router_app())["paths"]["/v1/securities/{symbol}/sentiment"]["get"]
    assert {"200", "304", "404", "422", "429"} <= set(operation["responses"])


def test_the_response_schemas_are_named():
    schemas = _v1_openapi(_router_app())["schemas"]
    for name in ("SentimentResponse", "Channel", "LabelCounts", "LabelShares"):
        assert name in schemas, name


def test_the_snapshot_includes_the_sentiment_route():
    """Regenerated in the same PR (UPDATE_API_SNAPSHOT=1)."""
    assert "/v1/securities/{symbol}/sentiment" in json.loads(SNAPSHOT.read_text())["paths"]


def test_api_md_documents_the_sentiment_route():
    content = (REPO / "docs" / "technical-specs" / "API.md").read_text()
    for term in (
        "/v1/securities/{symbol}/sentiment",
        "not_enough_coverage",
        "no_reading",
        "no_breakdown",
        "min_items",
        "weighted",
    ):
        assert term in content, term
