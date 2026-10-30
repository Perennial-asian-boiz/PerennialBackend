"""Sprint 2 DB-1..DB-5 and TEST-5: migration 0018, its models, schemas and grants.

Spec: output/sprint-2/s2-stories-plan.md §3.1, §3.4 and §5 (a) and (e). The
ticker_sentiment contract is decision 108 (pinned exactly): PR #85's table at
fccd07f plus model_version, created by 0018. A row per security; an empty row
(no news or posts counted) has a NULL value. Synthetic rows only.

Where the plan leaves a column open, these tests pin the test maker's proposed
default (each marked "proposed"); the handoff lists them.

* company_scores stores §3.4's breakdown as columns, `<component>_<field>` for
  fundamentals, sentiment and event_exposure: value (0-100), nominal_weight,
  weight (effective), contribution, input_as_of, input_version, record_id.
  The record's table follows from the component (fundamental_scores,
  ticker_sentiment; event_exposure has none in score-v1).
* score_explanations names the score row it explains (company_score_id), so the
  API reaches it from current_scores -> company_scores without run_dependencies,
  and stores `reason` beside `status`.
* The API reads every company_scores column except watchlist_publication_id, and
  every score_explanations column except input_digest.
"""

import ast
import json
import re
import secrets
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError
from test_owner_migrations import (
    MIGRATIONS,
    head,
    migrate,
    provision,
    provisioned,
    scratch,
    version,
)
from test_read_api import (
    COLUMNS,
    FULL_TABLES,
    _denied,
    as_role,
    groups,  # noqa: F401  (a pytest fixture)
    login,
    seed_security,
)

NEW_SOURCES = ("ticker_sentiment", "company_scores", "score_explanations")
NEW_TABLES = (*NEW_SOURCES, "current_scores")
COMPONENTS = ("fundamentals", "sentiment", "event_exposure")
COMPONENT_COLUMNS = {
    "value": ("numeric(4,1)", False),
    "nominal_weight": ("numeric(5,4)", True),
    "weight": ("numeric(5,4)", False),
    "contribution": ("numeric(5,2)", False),
    "input_as_of": ("date", False),
    "input_version": ("character varying(32)", False),
    "record_id": ("bigint", False),
}
BATCH_AND_SECURITY = {
    "id": ("bigint", True),
    "batch_id": ("bigint", True),
    "security_id": ("bigint", True),
}
# {column: (format_type, NOT NULL)}
SHAPES = {
    "ticker_sentiment": {  # decision 108: #85's table plus model_version, exactly
        **BATCH_AND_SECURITY,
        "row_number": ("integer", True),
        "value": ("numeric(5,4)", False),
        "plain_average": ("numeric(5,4)", False),
        "article_count": ("integer", True),
        "news_count": ("integer", True),
        "social_count": ("integer", True),
        "first_at": ("timestamp with time zone", False),
        "last_at": ("timestamp with time zone", False),
        "weights_version": ("character varying(32)", True),
        "model_version": ("character varying(64)", False),
    },
    "company_scores": {
        **BATCH_AND_SECURITY,
        "status": ("character varying(16)", True),
        "unavailable_reason": ("character varying(32)", False),
        "score": ("numeric(4,1)", False),
        "score_version": ("character varying(32)", True),
        "missing": ("character varying(32)[]", True),
        "recommended": ("boolean", True),
        "watchlist_publication_id": ("bigint", False),
        **{
            f"{c}_{f}": shape
            for c in COMPONENTS
            for f, shape in COMPONENT_COLUMNS.items()
        },
    },
    "score_explanations": {
        **BATCH_AND_SECURITY,
        "company_score_id": ("bigint", True),  # proposed
        "status": ("character varying(16)", True),
        "reason": ("character varying(32)", False),  # proposed
        "template_version": ("character varying(32)", True),
        "claims": ("jsonb", True),
        "input_digest": ("character varying(64)", True),
    },
    "current_scores": {
        "channel": ("character varying(32)", True),
        "batch_id": ("bigint", True),
        "promoted_at": ("timestamp with time zone", True),
    },
}
# The ingestion contract's row_number is the builder's choice on the two derived tables.
OPTIONAL_ROW_NUMBER = {"company_scores", "score_explanations"}
SERVED = {
    "company_scores": tuple(
        c for c in SHAPES["company_scores"] if c != "watchlist_publication_id"
    ),
    "score_explanations": tuple(
        c for c in SHAPES["score_explanations"] if c != "input_digest"
    ),
}
WRITES = ("INSERT", "UPDATE", "DELETE", "TRUNCATE")
PUBLICATION = object()  # replaced by a real publication's id at insert time
FAILED_RUN = (
    "INSERT INTO perennial.ingestion_runs (source, status, collection_mode, started_at, finished_at,"
    " error_code) VALUES (:s, 'failed', 'derived', now(), now(), 'missing_upstream')"
)


def claim(order=1, component="fundamentals", **extra):
    return {
        "order": order,
        "text": "Revenue grew 18% a year over the last 3 years.",
        "component": component,
        "measure": "revenue_growth_3y",
        "record": {"table": "fundamental_scores", "id": 812, "batch_id": 5},
        "as_of": "2025-06-30",
        "value": "0.18",
        **extra,
    }


FIRST_AT = datetime(2026, 10, 29, 14, 5, tzinfo=timezone.utc)
LAST_AT = datetime(2026, 11, 4, 16, 30, tzinfo=timezone.utc)


def sentiment_row(**over):
    """A scored row: 12 news articles, no social posts."""
    return {
        "value": Decimal("0.3100"),
        "plain_average": Decimal("0.2800"),
        "article_count": 12,
        "news_count": 12,
        "social_count": 0,
        "first_at": FIRST_AT,
        "last_at": LAST_AT,
        "weights_version": "weights-v1",
        "model_version": "ProsusAI/finbert@synthetic0001",
        **over,
    }


def empty_sentiment_row(**over):
    """Nothing counted for the security in the window: sentiment is missing."""
    empty = {
        "value": None,
        "plain_average": None,
        "article_count": 0,
        "news_count": 0,
        "social_count": 0,
        "first_at": None,
        "last_at": None,
        "model_version": None,
    }
    return sentiment_row(**{**empty, **over})


def social_sentiment_row(**over):
    """Social posts only: no news article, so no model_version (decision 108)."""
    social = {
        "article_count": 5,
        "news_count": 0,
        "social_count": 5,
        "model_version": None,
    }
    return sentiment_row(**{**social, **over})


def score_row(**over):
    """§3.4's full example: 78.0 and 65.5 with event exposure missing -> 74.3."""
    row = {
        "status": "ok",
        "unavailable_reason": None,
        "score": Decimal("74.3"),
        "score_version": "score-v1",
        "missing": ["event_exposure"],
        "recommended": False,
        "watchlist_publication_id": None,
    }
    parts = {
        "fundamentals": (
            Decimal("78.0"),
            Decimal("0.6000"),
            Decimal("0.7059"),
            Decimal("55.06"),
            date(2025, 6, 30),
            "fundamentals-v1",
        ),
        "sentiment": (
            Decimal("65.5"),
            Decimal("0.2500"),
            Decimal("0.2941"),
            Decimal("19.26"),
            date(2026, 11, 4),
            "weights-v1",
        ),
        "event_exposure": (
            None,
            Decimal("0.1500"),
            Decimal("0.0000"),
            Decimal("0.00"),
            None,
            None,
        ),
    }
    for component, values in parts.items():
        for field, value in zip(
            (
                "value",
                "nominal_weight",
                "weight",
                "contribution",
                "input_as_of",
                "input_version",
            ),
            values,
        ):
            row[f"{component}_{field}"] = value
        row[f"{component}_record_id"] = None
    return {**row, **over}


def unavailable_row(**over):
    """§3.4's unavailable example: fundamentals only."""
    return score_row(
        **{
            "status": "unavailable",
            "unavailable_reason": "only_fundamentals",
            "score": None,
            "missing": ["sentiment", "event_exposure"],
            **{
                f"{c}_{f}": None
                for c in ("sentiment", "event_exposure")
                for f in (
                    "value",
                    "weight",
                    "contribution",
                    "input_as_of",
                    "input_version",
                )
            },
            "fundamentals_weight": None,
            "fundamentals_contribution": None,
            **over,
        }
    )


def explanation_row(**over):
    return {
        "status": "ok",
        "reason": None,
        "template_version": "explain-v1",
        "claims": [
            claim(1),
            claim(2, measure="operating_margin"),
            claim(3, component="sentiment"),
        ],
        "input_digest": "ab" * 32,
        **over,
    }


# ── DB-1: the migration file ─────────────────────────────


def _migration():
    found = sorted(MIGRATIONS.glob("*_0018_*.py"))
    assert len(found) == 1, found
    return found[0]


def _assigned(tree, name):
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == name for t in node.targets
        ):
            return node.value.value
    return None


def test_db1_migration_0018_follows_0017_and_is_the_single_head():
    path = _migration()
    assert re.fullmatch(r"\d{8}_0018_[a-z0-9_]+\.py", path.name), path.name
    tree = ast.parse(path.read_text())
    assert (_assigned(tree, "revision"), _assigned(tree, "down_revision")) == (
        "0018",
        "0017",
    )
    assert head() == "0018"


@pytest.mark.parametrize("function", ["upgrade", "downgrade"])
def test_db1_both_directions_set_a_local_lock_timeout(function):
    source = _migration().read_text()
    body = next(
        n
        for n in ast.parse(source).body
        if isinstance(n, ast.FunctionDef) and n.name == function
    )
    assert re.search(
        r"SET LOCAL lock_timeout = '5s'", ast.get_source_segment(source, body)
    )


# ── DB-1: the migrated tables ────────────────────────────


def _shape(conn, table):
    return {
        name: (kind, notnull)
        for name, kind, notnull in conn.execute(
            text(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull FROM pg_attribute a"
                " WHERE a.attrelid = to_regclass(:t) AND a.attnum > 0 AND NOT a.attisdropped"
            ),
            {"t": f"perennial.{table}"},
        )
    }


def _has_row_number(conn, table):
    return "row_number" in _shape(conn, table)


@pytest.mark.db
@pytest.mark.parametrize("table", NEW_TABLES)
def test_db1_columns_types_and_nullability(engine, table):
    with engine.connect() as conn:
        actual = _shape(conn, table)
    if table in OPTIONAL_ROW_NUMBER and "row_number" in actual:
        assert actual.pop("row_number") == ("integer", True)
    assert actual == SHAPES[table]


@pytest.mark.db
@pytest.mark.parametrize("table", NEW_SOURCES)
def test_db1_one_row_per_batch_and_security_and_a_security_index(engine, table):
    with engine.connect() as conn:
        found = inspect(conn)
        uniques = [
            set(u["column_names"])
            for u in found.get_unique_constraints(table, schema="perennial")
        ]
        indexes = [
            i["column_names"] for i in found.get_indexes(table, schema="perennial")
        ]
        foreign = {
            tuple(f["constrained_columns"]): (
                f["referred_table"],
                tuple(f["referred_columns"]),
            )
            for f in found.get_foreign_keys(table, schema="perennial")
        }
    assert {"batch_id", "security_id"} in uniques
    assert any(cols[0] == "security_id" for cols in indexes), indexes
    assert foreign[("batch_id",)] == ("source_batches", ("id",))
    assert foreign[("security_id",)] == ("securities", ("id",))


@pytest.mark.db
def test_db1_current_scores_is_a_pointer_per_channel_to_a_batch(engine):
    with engine.connect() as conn:
        found = inspect(conn)
        pk = found.get_pk_constraint("current_scores", schema="perennial")[
            "constrained_columns"
        ]
        foreign = {
            tuple(f["constrained_columns"]): (
                f["referred_table"],
                tuple(f["referred_columns"]),
            )
            for f in found.get_foreign_keys("current_scores", schema="perennial")
        }
    assert pk == ["channel"]
    assert foreign[("batch_id",)] == ("source_batches", ("id",))


@pytest.mark.db
def test_db1_explanations_reference_their_score_row(engine):  # proposed
    """Checker F1: one composite FK, (company_score_id, security_id) -> company_scores
    (id, security_id), backed by UNIQUE (id, security_id), so an explanation can only
    name a score of its own security. No single-column FK beside it."""
    with engine.connect() as conn:
        found = inspect(conn)
        foreign = [
            (
                tuple(f["constrained_columns"]),
                f["referred_table"],
                tuple(f["referred_columns"]),
            )
            for f in found.get_foreign_keys("score_explanations", schema="perennial")
        ]
        indexes = [
            i["column_names"]
            for i in found.get_indexes("score_explanations", schema="perennial")
        ]
        uniques = [
            tuple(u["column_names"])
            for u in found.get_unique_constraints("company_scores", schema="perennial")
        ]
    to_scores = [f for f in foreign if f[1] == "company_scores"]
    assert to_scores == [
        (("company_score_id", "security_id"), "company_scores", ("id", "security_id"))
    ]
    assert ("id", "security_id") in uniques
    assert any(cols[0] == "company_score_id" for cols in indexes), (
        indexes
    )  # supports the FK


def _cross_security(engine):
    """Two securities with a committed score each; returns (A's score id, A, B)."""
    with engine.begin() as conn:
        a, b = _security(conn, "ZZXA"), _security(conn, "ZZXB")
        batch = _batch(conn, "company_scores")
        score_a = _insert(
            conn, "company_scores", score_row(), batch_id=batch, security_id=a
        )
        _insert(conn, "company_scores", score_row(), batch_id=batch, security_id=b)
        explanations = _batch(conn, "score_explanations")
    return score_a, a, b, explanations


def _explain_as(engine, role, score_id, security_id, batch):
    """Insert one explanation as `role`; None, or the refusing SQLSTATE."""
    with as_role(engine, role) as conn:
        try:
            with conn.begin_nested():
                _insert(
                    conn,
                    "score_explanations",
                    {"company_score_id": score_id, **explanation_row()},
                    batch_id=batch,
                    security_id=security_id,
                )
        except DBAPIError as exc:
            return exc.orig.sqlstate
    return None


@pytest.mark.db
@pytest.mark.parametrize("role", ["owner", "importer"])
def test_db1_cross_security_explanation_refused(engine, groups, role):  # noqa: F811
    """Checker F1: B's explanation naming A's score would serve A's claims as B's."""
    score_a, _, b, batch = _cross_security(engine)
    assert _explain_as(engine, groups[role], score_a, b, batch) == "23503"


@pytest.mark.db
@pytest.mark.parametrize("role", ["owner", "importer"])
def test_db1_same_security_explanation_accepted(engine, groups, role):  # noqa: F811
    score_a, a, _, batch = _cross_security(engine)
    assert _explain_as(engine, groups[role], score_a, a, batch) is None


# Inserting rows straight into the tables: the CHECKs are the database's own guard.


def _batch(conn, source):
    return conn.execute(
        text(
            "INSERT INTO perennial.source_batches (source, content_hash, hash_version, record_count, payload)"
            " VALUES (:s, :h, 'v2', 0, '{}') RETURNING id"
        ),
        {"s": source, "h": secrets.token_hex(32)},
    ).scalar_one()


def _security(conn, symbol):
    return conn.execute(
        text("INSERT INTO perennial.securities (symbol) VALUES (:s) RETURNING id"),
        {"s": symbol},
    ).scalar_one()


def _insert(conn, table, row, *, batch_id, security_id):
    row = {**row, "batch_id": batch_id, "security_id": security_id}
    if _has_row_number(conn, table):
        row.setdefault("row_number", 1)
    values = []
    for column in row:
        if column == "claims":
            values.append("CAST(:claims AS jsonb)")
        elif column == "missing":
            values.append("CAST(:missing AS varchar[])")
        else:
            values.append(f":{column}")
    params = {
        **row,
        **({"claims": json.dumps(row["claims"])} if "claims" in row else {}),
    }
    return conn.execute(
        text(
            f"INSERT INTO perennial.{table} ({', '.join(row)}) VALUES ({', '.join(values)}) RETURNING id"
        ),
        params,
    ).scalar_one()


@pytest.fixture
def subjects(engine):
    """A security and one batch per new source, committed."""
    with engine.begin() as conn:
        found = {"security_id": _security(conn, "ZZSC")}
        found.update({source: _batch(conn, source) for source in NEW_SOURCES})
    return found


def _publication(conn):
    return conn.execute(
        text(
            "INSERT INTO perennial.publications (created_at, ranking_version, ranking_config, output)"
            " VALUES (now(), 'watchlist-v5', '{}', '{}') RETURNING id"
        )
    ).scalar_one()


def _store(engine, subjects, table, row):
    """Insert in its own transaction; returns None or the refusing SQLSTATE."""
    try:
        with engine.begin() as conn:
            if row.get("watchlist_publication_id") is PUBLICATION:
                row = {**row, "watchlist_publication_id": _publication(conn)}
            score_id = None
            if table == "score_explanations":
                score_id = _insert(
                    conn,
                    "company_scores",
                    score_row(),
                    batch_id=subjects["company_scores"],
                    security_id=subjects["security_id"],
                )
                row = {"company_score_id": score_id, **row}
            _insert(
                conn,
                table,
                row,
                batch_id=subjects[table],
                security_id=subjects["security_id"],
            )
            conn.rollback()
    except DBAPIError as exc:
        return exc.orig.sqlstate
    return None


ACCEPTED = {
    "sentiment_baseline": ("ticker_sentiment", sentiment_row()),
    "sentiment_minus_one": ("ticker_sentiment", sentiment_row(value=Decimal("-1"))),
    "sentiment_plus_one": ("ticker_sentiment", sentiment_row(value=Decimal("1"))),
    "sentiment_plain_average_bounds": (
        "ticker_sentiment",
        sentiment_row(plain_average=Decimal("-1")),
    ),
    "sentiment_one_article": (
        "ticker_sentiment",
        sentiment_row(article_count=1, news_count=1),
    ),
    "sentiment_empty": ("ticker_sentiment", empty_sentiment_row()),
    "sentiment_social_only": ("ticker_sentiment", social_sentiment_row()),
    "sentiment_news_and_social": (
        "ticker_sentiment",
        sentiment_row(article_count=12, news_count=9, social_count=3),
    ),
    "sentiment_one_instant": ("ticker_sentiment", sentiment_row(first_at=LAST_AT)),
    "score_ok": ("company_scores", score_row()),
    "score_zero": ("company_scores", score_row(score=Decimal("0.0"))),
    "score_hundred": ("company_scores", score_row(score=Decimal("100.0"))),
    "score_recommended_at_60": (
        "company_scores",
        score_row(
            score=Decimal("60.0"),
            recommended=True,
            watchlist_publication_id=PUBLICATION,
        ),
    ),
    "score_unavailable_only_fundamentals": ("company_scores", unavailable_row()),
    "score_unavailable_no_fundamentals": (
        "company_scores",
        unavailable_row(
            unavailable_reason="no_fundamentals",
            missing=list(COMPONENTS),
            fundamentals_value=None,
            fundamentals_input_as_of=None,
            fundamentals_input_version=None,
        ),
    ),
    "explanation_ok": ("score_explanations", explanation_row()),
    "explanation_one_claim": ("score_explanations", explanation_row(claims=[claim(1)])),
    "explanation_not_recommended": (
        "score_explanations",
        explanation_row(status="not_recommended", claims=[]),
    ),
    "explanation_insufficient": (
        "score_explanations",
        explanation_row(
            status="unavailable", reason="insufficient_supporting_data", claims=[]
        ),
    ),
    "explanation_score_unavailable": (
        "score_explanations",
        explanation_row(status="unavailable", reason="score_unavailable", claims=[]),
    ),
}
REFUSED = {
    # ticker_sentiment (decision 108): #85's three checks, plus model_version
    "sentiment_below_minus_one": (
        "ticker_sentiment",
        sentiment_row(value=Decimal("-1.0001")),
    ),
    "sentiment_above_one": ("ticker_sentiment", sentiment_row(value=Decimal("1.0001"))),
    "sentiment_plain_average_above_one": (
        "ticker_sentiment",
        sentiment_row(plain_average=Decimal("1.0001")),
    ),
    "sentiment_counts_dont_add_up": (
        "ticker_sentiment",
        sentiment_row(article_count=13),
    ),
    "sentiment_negative_count": (
        "ticker_sentiment",
        sentiment_row(article_count=12, news_count=13, social_count=-1),
    ),
    "sentiment_counted_without_a_value": (
        "ticker_sentiment",
        empty_sentiment_row(
            article_count=12, news_count=12, model_version="ProsusAI/finbert@x"
        ),
    ),
    "sentiment_a_value_with_nothing_counted": (
        "ticker_sentiment",
        sentiment_row(article_count=0, news_count=0, model_version=None),
    ),
    "sentiment_value_without_plain_average": (
        "ticker_sentiment",
        sentiment_row(plain_average=None),
    ),
    "sentiment_value_without_last_at": (
        "ticker_sentiment",
        sentiment_row(last_at=None),
    ),
    "sentiment_first_after_last": (
        "ticker_sentiment",
        sentiment_row(first_at=LAST_AT, last_at=FIRST_AT),
    ),
    "sentiment_news_without_model_version": (
        "ticker_sentiment",
        sentiment_row(model_version=None),
    ),
    "sentiment_model_version_without_news": (
        "ticker_sentiment",
        social_sentiment_row(model_version="ProsusAI/finbert@synthetic0001"),
    ),
    "sentiment_row_number_zero": ("ticker_sentiment", sentiment_row(row_number=0)),
    # company_scores: score 0-100, weights 0..1, status, missing
    "score_below_zero": ("company_scores", score_row(score=Decimal("-0.1"))),
    "score_above_hundred": ("company_scores", score_row(score=Decimal("100.1"))),
    "score_bad_status": ("company_scores", score_row(status="partial")),
    "score_unknown_missing": (
        "company_scores",
        score_row(missing=["event_exposure", "insider"]),
    ),
    "score_effective_weight_above_one": (
        "company_scores",
        score_row(fundamentals_weight=Decimal("1.0001")),
    ),
    "score_effective_weight_negative": (
        "company_scores",
        score_row(sentiment_weight=Decimal("-0.0001")),
    ),
    "score_nominal_weight_above_one": (
        "company_scores",
        score_row(fundamentals_nominal_weight=Decimal("1.0001")),
    ),
    "score_nominal_weight_negative": (
        "company_scores",
        score_row(event_exposure_nominal_weight=Decimal("-0.0001")),
    ),
    "score_component_value_above_hundred": (
        "company_scores",
        score_row(sentiment_value=Decimal("100.1")),
    ),
    "score_component_value_negative": (
        "company_scores",
        score_row(fundamentals_value=Decimal("-0.1")),
    ),
    "score_contribution_negative": (
        "company_scores",
        score_row(fundamentals_contribution=Decimal("-0.01")),
    ),
    "score_contribution_above_hundred": (
        "company_scores",
        score_row(sentiment_contribution=Decimal("100.01")),
    ),
    # proposed: status, score and reason agree; recommended needs ok, >= 60 and the watchlist it was judged on
    "score_ok_without_a_score": ("company_scores", score_row(score=None)),
    "score_ok_with_a_reason": (
        "company_scores",
        score_row(unavailable_reason="only_fundamentals"),
    ),
    "score_unavailable_with_a_score": (
        "company_scores",
        unavailable_row(score=Decimal("50.0")),
    ),
    "score_unavailable_without_a_reason": (
        "company_scores",
        unavailable_row(unavailable_reason=None),
    ),
    "score_unknown_reason": (
        "company_scores",
        unavailable_row(unavailable_reason="stale"),
    ),
    "score_recommended_below_60": (
        "company_scores",
        score_row(
            score=Decimal("59.9"),
            recommended=True,
            watchlist_publication_id=PUBLICATION,
        ),
    ),
    "score_recommended_unavailable": (
        "company_scores",
        unavailable_row(recommended=True, watchlist_publication_id=PUBLICATION),
    ),
    "score_recommended_without_its_watchlist": (
        "company_scores",
        score_row(score=Decimal("80.0"), recommended=True),
    ),
    # score_explanations
    "explanation_bad_status": ("score_explanations", explanation_row(status="pending")),
    "explanation_four_claims": (
        "score_explanations",
        explanation_row(claims=[claim(1), claim(2), claim(3), claim(4)]),
    ),
    "explanation_claims_not_an_array": (
        "score_explanations",
        explanation_row(claims={"order": 1}),
    ),
    "explanation_ok_without_claims": ("score_explanations", explanation_row(claims=[])),
    "explanation_not_recommended_with_claims": (
        "score_explanations",
        explanation_row(status="not_recommended"),
    ),
    "explanation_unavailable_without_reason": (
        "score_explanations",
        explanation_row(status="unavailable", claims=[]),
    ),
    "explanation_ok_with_reason": (
        "score_explanations",
        explanation_row(reason="score_unavailable"),
    ),
    "explanation_unknown_reason": (
        "score_explanations",
        explanation_row(status="unavailable", reason="later", claims=[]),
    ),
    "explanation_digest_not_hex": (
        "score_explanations",
        explanation_row(input_digest="AB" * 32),
    ),
    "explanation_digest_short": (
        "score_explanations",
        explanation_row(input_digest="ab" * 31),
    ),
}


@pytest.mark.db
@pytest.mark.parametrize("case", sorted(ACCEPTED))
def test_db1_checks_accept_valid_rows(engine, subjects, case):
    table, row = ACCEPTED[case]
    assert _store(engine, subjects, table, row) is None


@pytest.mark.db
@pytest.mark.parametrize("case", sorted(REFUSED))
def test_db1_checks_refuse_invalid_rows(engine, subjects, case):
    table, row = REFUSED[case]
    assert _store(engine, subjects, table, row) == "23514"


@pytest.mark.db
@pytest.mark.parametrize("table", NEW_SOURCES)
def test_db1_a_second_row_for_the_same_batch_and_security_is_refused(
    engine, subjects, table
):
    rows = {
        "ticker_sentiment": sentiment_row(),
        "company_scores": score_row(),
        "score_explanations": explanation_row(),
    }
    with pytest.raises(DBAPIError) as caught:
        with engine.begin() as conn:
            extra = {}
            if table == "score_explanations":
                extra["company_score_id"] = _insert(
                    conn,
                    "company_scores",
                    score_row(),
                    batch_id=subjects["company_scores"],
                    security_id=subjects["security_id"],
                )
            for number in (1, 2):
                row = {**rows[table], **extra}
                if _has_row_number(conn, table):
                    row["row_number"] = number
                _insert(
                    conn,
                    table,
                    row,
                    batch_id=subjects[table],
                    security_id=subjects["security_id"],
                )
    assert caught.value.orig.sqlstate == "23505"


@pytest.mark.db
def test_db1_an_explanation_for_a_missing_score_row_is_refused(
    engine, subjects
):  # proposed
    with pytest.raises(DBAPIError) as caught:
        with engine.begin() as conn:
            _insert(
                conn,
                "score_explanations",
                {"company_score_id": 2**62, **explanation_row()},
                batch_id=subjects["score_explanations"],
                security_id=subjects["security_id"],
            )
    assert caught.value.orig.sqlstate == "23503"


@pytest.mark.db
def test_db1_jsonb_claims_round_trip_unchanged(engine, subjects):
    row = explanation_row()
    with engine.begin() as conn:
        score_id = _insert(
            conn,
            "company_scores",
            score_row(),
            batch_id=subjects["company_scores"],
            security_id=subjects["security_id"],
        )
        new_id = _insert(
            conn,
            "score_explanations",
            {"company_score_id": score_id, **row},
            batch_id=subjects["score_explanations"],
            security_id=subjects["security_id"],
        )
        stored = conn.execute(
            text("SELECT claims FROM perennial.score_explanations WHERE id = :i"),
            {"i": new_id},
        ).scalar_one()
    assert stored == row["claims"]


@pytest.mark.db
def test_db1_source_vocabulary_gains_the_three_sources_and_keeps_the_old(engine):
    from src.db import models

    for source in (*NEW_SOURCES, *models.SOURCES):
        with engine.begin() as conn:
            _batch(conn, source)
            conn.execute(text(FAILED_RUN), {"s": source})
    for table in ("source_batches", "ingestion_runs"):
        with pytest.raises(DBAPIError) as caught:
            with engine.begin() as conn:
                if table == "source_batches":
                    _batch(conn, "current_scores")  # a pointer table, not a source
                else:
                    conn.execute(text(FAILED_RUN), {"s": "current_scores"})
        assert caught.value.orig.sqlstate == "23514"


# ── DB-2: models ─────────────────────────────────────────


def test_db2_sources_are_appended_after_news_without_moving_anything():
    from src.db import models

    before = (
        models.WATCHLIST_SOURCES
        + models.SUBJECTLESS_SOURCES
        + models.PINNED_ROW_SOURCES
        + models.FUNDAMENTALS_SOURCES
        + models.NEWS_SOURCES
    )
    assert models.SOURCES[: len(before) + 3] == before + NEW_SOURCES
    assert not set(NEW_SOURCES) & set(models.WATCHLIST_SOURCES)


def test_db2_vocabularies():
    from src.db import models

    assert models.SCORE_COMPONENTS == COMPONENTS  # the fixed component order (§3.2)
    assert models.SCORE_STATUSES == ("ok", "unavailable")
    assert models.EXPLANATION_STATUSES == ("ok", "not_recommended", "unavailable")


def test_db2_tables_are_registered():
    from src.db import models

    for source in NEW_SOURCES:
        assert models.SOURCE_TABLES[source].name == source
    assert "current_scores" not in models.SOURCE_TABLES  # a pointer, not a batch table
    assert models.metadata.tables["perennial.current_scores"] is models.current_scores
    from sqlalchemy.dialects.postgresql import JSONB

    assert isinstance(models.score_explanations.c.claims.type, JSONB)


def test_db2_lock_keys_and_canonical_resolution():
    from src.ingestion.identity import CANONICAL_ONLY_SOURCES
    from src.pipeline.lifecycle import SOURCE_LOCK_KEYS

    # The next unused keys after news_articles' 170020, in vocabulary order.
    assert [SOURCE_LOCK_KEYS[s] for s in NEW_SOURCES] == [170021, 170022, 170023]
    assert len(set(SOURCE_LOCK_KEYS.values())) == len(SOURCE_LOCK_KEYS)
    # Derived from securities the platform already resolved: canonical symbols only.
    assert set(NEW_SOURCES) <= CANONICAL_ONLY_SOURCES


# ── DB-3: ingestion schemas ──────────────────────────────


def test_db3_each_source_has_a_record_model_one_row_per_security_and_envelope_keys():
    from src.ingestion.envelope import ENVELOPE_KEYS
    from src.ingestion.schemas import ONE_ROW_PER_SECURITY, RECORD_MODELS

    for source in NEW_SOURCES:
        model = RECORD_MODELS[source]
        expected = set(SHAPES[source]) - {"id", "batch_id", "security_id", "row_number"}
        assert set(model.DB_COLUMNS) == expected, source
        assert source in ENVELOPE_KEYS
    assert set(NEW_SOURCES) <= ONE_ROW_PER_SECURITY


def _import(engine, source, records, envelope=None):
    from src.ingestion.importer import CollectionOutcome, import_collection

    outcome = CollectionOutcome.success(
        source,
        "derived",
        records,
        coverage={"complete": True, "scope": "derivation"},
        envelope=envelope,
    )
    return import_collection(engine, outcome)


def _json_ready(row):
    out = {}
    for key, value in row.items():
        if isinstance(value, Decimal):
            out[key] = str(value)
        elif isinstance(value, date):
            out[key] = value.isoformat()
        else:
            out[key] = value
    return out


def _stored(engine, table, batch_id):
    with engine.connect() as conn:
        rows = (
            conn.execute(
                text(
                    f"SELECT s.symbol, t.* FROM perennial.{table} t JOIN perennial.securities s"
                    " ON s.id = t.security_id WHERE t.batch_id = :b ORDER BY s.symbol"
                ),
                {"b": batch_id},
            )
            .mappings()
            .all()
        )
    return {r["symbol"]: dict(r) for r in rows}


def _same(stored, expected):
    return {k: stored[k] for k in expected} == expected


SENTIMENT_ENVELOPE = {
    "weights_version": "weights-v1",
    "window_days": 7,
    "universe": 3,
    "scored": 2,
    "no_data_count": 1,
    "news_used": 12,
    "social_used": 5,
    "social_batch_fresh": True,
    "publishers_unknown": 0,
}


def test_db3_ticker_sentiment_envelope_keys_are_85s_exactly():
    from src.ingestion.envelope import ENVELOPE_KEYS

    keys = ENVELOPE_KEYS["ticker_sentiment"]
    assert len(keys) == len(set(keys)) and set(keys) == set(SENTIMENT_ENVELOPE)


@pytest.mark.db
def test_db3_ticker_sentiment_imports_through_the_contract(engine):
    seed_security(engine, "ZZTA")
    seed_security(engine, "ZZTB")
    rows = {
        "ZZTA": sentiment_row(),
        "ZZTB": social_sentiment_row(value=Decimal("-0.5000")),
        "ZZTC": empty_sentiment_row(),
    }
    seed_security(engine, "ZZTC")
    result = _import(
        engine,
        "ticker_sentiment",
        [{"ticker": t, **_json_ready(r)} for t, r in rows.items()],
        envelope=SENTIMENT_ENVELOPE,
    )
    assert result.succeeded, (result.error_code, result.error_summary)
    stored = _stored(engine, "ticker_sentiment", result.batch_id)
    assert set(stored) == set(rows)
    for symbol, row in rows.items():
        assert _same(stored[symbol], row), symbol
    assert sorted(r["row_number"] for r in stored.values()) == [1, 2, 3]


@pytest.mark.db
def test_db3_company_scores_and_explanations_import_through_the_contract(engine):
    seed_security(engine, "ZZCA")
    seed_security(engine, "ZZCB")
    with engine.begin() as conn:
        publication = _publication(conn)
    scores = {
        "ZZCA": score_row(
            score=Decimal("74.3"),
            recommended=True,
            watchlist_publication_id=publication,
        ),
        "ZZCB": unavailable_row(),
    }
    result = _import(
        engine,
        "company_scores",
        [{"ticker": t, **_json_ready(r)} for t, r in scores.items()],
        envelope={"score_version": "score-v1"},
    )
    assert result.succeeded, (result.error_code, result.error_summary)
    stored = _stored(engine, "company_scores", result.batch_id)
    for symbol, row in scores.items():
        assert _same(stored[symbol], row), symbol

    explanations = {
        "ZZCA": explanation_row(company_score_id=stored["ZZCA"]["id"]),
        "ZZCB": explanation_row(
            company_score_id=stored["ZZCB"]["id"],
            status="unavailable",
            reason="score_unavailable",
            claims=[],
        ),
    }
    result = _import(
        engine,
        "score_explanations",
        [{"ticker": t, **r} for t, r in explanations.items()],
        envelope={"template_version": "explain-v1"},
    )
    assert result.succeeded, (result.error_code, result.error_summary)
    stored = _stored(engine, "score_explanations", result.batch_id)
    for symbol, row in explanations.items():
        assert _same(stored[symbol], row), symbol


INVALID_RECORDS = {
    "sentiment_value_above_one": (
        "ticker_sentiment",
        sentiment_row(value=Decimal("1.5")),
    ),
    "sentiment_plain_average_below_minus_one": (
        "ticker_sentiment",
        sentiment_row(plain_average=Decimal("-1.5")),
    ),
    "sentiment_counts_dont_add_up": (
        "ticker_sentiment",
        sentiment_row(article_count=13),
    ),
    "sentiment_negative_count": (
        "ticker_sentiment",
        sentiment_row(article_count=12, news_count=13, social_count=-1),
    ),
    "sentiment_counted_without_a_value": (
        "ticker_sentiment",
        empty_sentiment_row(
            article_count=12, news_count=12, model_version="ProsusAI/finbert@x"
        ),
    ),
    "sentiment_a_value_with_nothing_counted": (
        "ticker_sentiment",
        sentiment_row(article_count=0, news_count=0, model_version=None),
    ),
    "sentiment_first_after_last": (
        "ticker_sentiment",
        sentiment_row(first_at=LAST_AT, last_at=FIRST_AT),
    ),
    "sentiment_naive_last_at": (
        "ticker_sentiment",
        sentiment_row(last_at=datetime(2026, 11, 4, 16, 30)),
    ),
    "sentiment_no_weights_version": (
        "ticker_sentiment",
        sentiment_row(weights_version=""),
    ),
    "sentiment_news_without_model_version": (
        "ticker_sentiment",
        sentiment_row(model_version=None),
    ),
    "sentiment_model_version_without_news": (
        "ticker_sentiment",
        social_sentiment_row(model_version="ProsusAI/finbert@synthetic0001"),
    ),
    "score_above_hundred": ("company_scores", score_row(score=Decimal("100.1"))),
    "score_bad_status": ("company_scores", score_row(status="partial")),
    "score_unknown_missing": ("company_scores", score_row(missing=["insider"])),
    "score_repeated_missing": (
        "company_scores",
        score_row(missing=["event_exposure", "event_exposure"]),
    ),
    "score_weight_above_one": (
        "company_scores",
        score_row(sentiment_weight=Decimal("1.5")),
    ),
    "score_ok_without_a_score": ("company_scores", score_row(score=None)),
    "score_unavailable_with_a_score": (
        "company_scores",
        unavailable_row(score=Decimal("40.0")),
    ),
    "score_recommended_below_60": (
        "company_scores",
        score_row(score=Decimal("59.9"), recommended=True, watchlist_publication_id=1),
    ),
    "explanation_bad_status": (
        "score_explanations",
        explanation_row(status="pending", company_score_id=1),
    ),
    "explanation_four_claims": (
        "score_explanations",
        explanation_row(
            claims=[claim(1), claim(2), claim(3), claim(4)], company_score_id=1
        ),
    ),
    "explanation_claim_on_unknown_component": (
        "score_explanations",
        explanation_row(claims=[claim(1, component="insider")], company_score_id=1),
    ),
    "explanation_ok_without_claims": (
        "score_explanations",
        explanation_row(claims=[], company_score_id=1),
    ),
    "explanation_digest_not_hex": (
        "score_explanations",
        explanation_row(input_digest="xyz", company_score_id=1),
    ),
}


@pytest.mark.db
@pytest.mark.parametrize("case", sorted(INVALID_RECORDS))
def test_db3_invalid_records_fail_validation_before_the_database(engine, case):
    source, row = INVALID_RECORDS[case]
    seed_security(engine, "ZZIV")
    result = _import(engine, source, [{"ticker": "ZZIV", **_json_ready(row)}])
    assert (result.status, result.error_code) == ("failed", "validation_failed")
    with engine.connect() as conn:
        assert (
            conn.execute(text(f"SELECT count(*) FROM perennial.{source}")).scalar() == 0
        )


@pytest.mark.db
@pytest.mark.parametrize("source", NEW_SOURCES)
def test_db3_two_records_for_one_security_fail_validation(engine, source):
    rows = {
        "ticker_sentiment": sentiment_row(),
        "company_scores": score_row(),
        "score_explanations": explanation_row(company_score_id=1),
    }
    seed_security(engine, "ZZDU")
    record = {"ticker": "ZZDU", **_json_ready(rows[source])}
    result = _import(engine, source, [record, dict(record)])
    assert (result.status, result.error_code) == ("failed", "validation_failed")


# ── DB-4: grants (provision_roles) ───────────────────────


def _provision_roles():
    from test_owner_migrations import provision_roles

    return provision_roles()


def test_db4_policy_lists_the_new_tables():
    roles = _provision_roles()
    assert set(NEW_SOURCES) <= set(roles.IMMUTABLE)
    assert "current_scores" in roles.MUTABLE and "current_scores" not in roles.IMMUTABLE
    assert "current_scores" in roles.API_TABLES
    assert {t: set(roles.API_COLUMNS[t]) for t in SERVED} == {
        t: set(c) for t, c in SERVED.items()
    }
    for table in (
        "ticker_sentiment",
        "fundamental_scores",
        "fundamentals",
        "news_articles",
    ):
        assert table not in roles.API_TABLES and table not in roles.API_COLUMNS, table


def test_db4_read_api_test_expectations_match_the_policy():
    """test_read_api's exact-readable test compares against these constants."""
    assert "current_scores" in FULL_TABLES
    assert {t: set(COLUMNS[t]) for t in SERVED} == {
        t: set(c) for t, c in SERVED.items()
    }


# ── DB-5: what each role can do ──────────────────────────


def _privilege(conn, role, table, privilege):
    return conn.execute(
        text("SELECT has_table_privilege(:r, :t, :p)"),
        {"r": role, "t": f"perennial.{table}", "p": privilege},
    ).scalar()


def _any_column(conn, role, table, privilege):
    return conn.execute(
        text("SELECT has_any_column_privilege(:r, :t, :p)"),
        {"r": role, "t": f"perennial.{table}", "p": privilege},
    ).scalar()


@pytest.mark.db
def test_db5_api_reads_only_the_served_columns(engine, groups):  # noqa: F811
    with engine.connect() as conn:
        shapes = {t: _shape(conn, t) for t in SERVED}
    with as_role(engine, groups["api"]) as conn:
        conn.execute(text("SELECT * FROM perennial.current_scores LIMIT 0"))
        for table, columns in SERVED.items():
            conn.execute(
                text(f"SELECT {', '.join(columns)} FROM perennial.{table} LIMIT 0")
            )
            for column in sorted(set(shapes[table]) - set(columns)):
                _denied(conn, f"SELECT {column} FROM perennial.{table}")
            _denied(conn, f"SELECT * FROM perennial.{table}")
        for table in (
            "ticker_sentiment",
            "fundamental_scores",
            "fundamentals",
            "source_batches",
            "run_dependencies",
        ):
            _denied(conn, f"SELECT 1 FROM perennial.{table} LIMIT 1")


@pytest.mark.db
@pytest.mark.parametrize("table", NEW_TABLES)
def test_db5_api_cannot_write_the_new_tables(engine, groups, table):  # noqa: F811
    with engine.connect() as conn:
        for privilege in WRITES:
            assert not _privilege(conn, groups["api"], table, privilege), privilege
        for privilege in ("INSERT", "UPDATE"):
            assert not _any_column(conn, groups["api"], table, privilege), privilege
    with as_role(engine, groups["api"]) as conn:
        _denied(conn, f"DELETE FROM perennial.{table}")


@pytest.mark.db
def test_db5_importer_appends_and_moves_only_the_pointer(engine, groups):  # noqa: F811
    importer, reader = groups["importer"], groups["reader"]
    with engine.connect() as conn:
        for table in NEW_SOURCES:
            assert _privilege(conn, importer, table, "SELECT") and _privilege(
                conn, importer, table, "INSERT"
            )
            for privilege in ("UPDATE", "DELETE", "TRUNCATE"):
                assert not _privilege(conn, importer, table, privilege), (
                    table,
                    privilege,
                )
            assert not _any_column(conn, importer, table, "UPDATE"), table
        for privilege in ("SELECT", "INSERT", "UPDATE"):
            assert _privilege(conn, importer, "current_scores", privilege), privilege
        for privilege in ("DELETE", "TRUNCATE"):
            assert not _privilege(conn, importer, "current_scores", privilege), (
                privilege
            )
        for table in NEW_TABLES:
            assert _privilege(conn, reader, table, "SELECT"), table
            for privilege in WRITES:
                assert not _privilege(conn, reader, table, privilege), (
                    table,
                    privilege,
                )


@pytest.mark.db
def test_db5_an_api_login_reads_a_stored_score_and_nothing_behind_it(engine, groups):  # noqa: F811
    """Through a real login inheriting only the API group: the path §3.4's route takes."""
    with engine.begin() as conn:
        security_id = _security(conn, "ZZAP")
        batches = {s: _batch(conn, s) for s in NEW_SOURCES}
        _insert(
            conn,
            "ticker_sentiment",
            sentiment_row(),
            batch_id=batches["ticker_sentiment"],
            security_id=security_id,
        )
        score_id = _insert(
            conn,
            "company_scores",
            score_row(),
            batch_id=batches["company_scores"],
            security_id=security_id,
        )
        _insert(
            conn,
            "score_explanations",
            {"company_score_id": score_id, **explanation_row()},
            batch_id=batches["score_explanations"],
            security_id=security_id,
        )
        conn.execute(
            text(
                "INSERT INTO perennial.current_scores (channel, batch_id, promoted_at)"
                " VALUES ('production', :b, now())"
            ),
            {"b": batches["company_scores"]},
        )
    with login(engine, groups, connect=True) as (api_engine, _, _password):
        with api_engine.connect() as conn:
            score = conn.execute(
                text(
                    "SELECT s.id, s.score, s.missing FROM perennial.current_scores p"
                    " JOIN perennial.company_scores s ON s.batch_id = p.batch_id"
                    " WHERE p.channel = 'production' AND s.security_id = :s"
                ),
                {"s": security_id},
            ).one()
            assert (score.score, score.missing) == (Decimal("74.3"), ["event_exposure"])
            claims = conn.execute(
                text(
                    "SELECT claims FROM perennial.score_explanations WHERE company_score_id = :i"
                ),
                {"i": score.id},
            ).scalar_one()
            assert [c["order"] for c in claims] == [1, 2, 3]
            for statement in (
                "SELECT value FROM perennial.ticker_sentiment",
                "SELECT score FROM perennial.fundamental_scores",
                "SELECT input_digest FROM perennial.score_explanations",
                "SELECT watchlist_publication_id FROM perennial.company_scores",
                "UPDATE perennial.current_scores SET promoted_at = now()",
                "INSERT INTO perennial.current_scores (channel, batch_id, promoted_at)"
                " VALUES ('x', 1, now())",
            ):
                _denied(conn, statement)


# ── TEST-5: owner migrations, up and down ────────────────


def _tables(target):
    with target.connect() as conn:
        return set(
            conn.execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname = 'perennial'")
            ).scalars()
        )


def test_test5_owner_upgrades_downgrades_and_upgrades_again_on_a_fresh_database(engine):
    with scratch(engine) as (target, names):
        provisioned(target, names)
        assert version(target) == "0018"
        assert set(NEW_TABLES) <= _tables(target)
        migrate(target, "0017", owner=names["owner"], down=True)
        assert version(target) == "0017"
        assert not set(NEW_TABLES) & _tables(target)
        with pytest.raises(DBAPIError) as caught:  # the vocabulary is back to 0017's
            with target.begin() as conn:
                _batch(conn, "company_scores")
        assert caught.value.orig.sqlstate == "23514"
        with target.begin() as conn:
            _batch(conn, "news_articles")  # 0017's sources still accepted
        provision(target, names, "bootstrap")
        migrate(target, owner=names["owner"])
        provision(target, names, "grants")
        assert version(target) == "0018"
        assert set(NEW_TABLES) <= _tables(target)


LINEAGE = [(source, kind) for source in NEW_SOURCES for kind in ("batch", "run")]


@pytest.mark.parametrize("source,kind", LINEAGE)
def test_test5_downgrade_refuses_while_lineage_exists(engine, source, kind):
    with scratch(engine) as (target, names):
        provisioned(target, names)
        with target.begin() as conn:
            if kind == "batch":
                _batch(conn, source)
            else:
                conn.execute(text(FAILED_RUN), {"s": source})
        with pytest.raises(Exception, match="lineage"):
            migrate(target, "0017", owner=names["owner"], down=True)
        assert version(target) == "0018"
        assert set(NEW_TABLES) <= _tables(target)
        table = "source_batches" if kind == "batch" else "ingestion_runs"
        with target.connect() as conn:
            assert (
                conn.execute(
                    text(f"SELECT count(*) FROM perennial.{table} WHERE source = :s"),
                    {"s": source},
                ).scalar()
                == 1
            )
