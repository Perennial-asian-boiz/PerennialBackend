"""JOB-7 / AC 4: article_scoring_attempts, the scorer's retry state. Spec-first;
synthetic only.

Pinned:

* Migration 0021 (after 0020) creates perennial.article_scoring_attempts, a
  state table keyed on (news_article_id, model): no id, batch or row_number.
  attempts >= 1; last_error is a code from ARTICLE_SCORING_ERRORS, never
  exception or article text; updated_at defaults to now(). Deleting a news
  article deletes its retry rows.
* One row per article and model: a second is refused, another model version
  starts fresh.
* Grants: the importer SELECT, INSERT, UPDATE and DELETE (the scorer deletes a
  row once the article is scored); the reader SELECT; the API nothing. No other
  table gains DELETE.
* 0021's downgrade refuses, before any change, while any retry row exists; with
  none it drops the table and keeps the articles.
"""

import ast
import re

import pytest
from sqlalchemy import func, insert, select, text
from sqlalchemy.exc import IntegrityError
from src.db import models
from src.services.news.job import run_news
from test_credibility import (  # noqa: F401  (seeded is a fixture)
    NOW,
    NewsClient,
    seeded,
)
from test_d114_article_summary import (  # noqa: F401  (scratch is a fixture)
    article_count,
    item,
    migrate,
    parsed,
    scratch,
    seed_rklb,
    version,
)
from test_owner_migrations import MIGRATIONS, provision_roles
from test_read_api import groups  # noqa: F401  (a pytest fixture)

TABLE = "perennial.article_scoring_attempts"
T = models.article_scoring_attempts
MODEL = "ProsusAI/finbert@synthetic"


def an_article(engine) -> int:
    run_news(
        engine, NewsClient({"RKLB": parsed(item(1, "Rocket Lab wins", ""))}), NOW.date()
    )
    with engine.connect() as conn:
        return conn.execute(select(func.min(models.news_articles.c.id))).scalar_one()


def add(engine, **values):
    row = {"model": MODEL, "attempts": 1, "last_error": "failed", **values}
    with engine.begin() as conn:
        conn.execute(insert(T).values(**row))


def rows(engine):
    with engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(T)).scalar_one()


def exists(target):
    with target.connect() as conn:
        return conn.execute(text(f"SELECT to_regclass('{TABLE}')")).scalar() is not None


# ── the model ────────────────────────────────────────────


def test_it_is_a_state_table_keyed_on_article_and_model():
    assert [c.name for c in T.primary_key.columns] == ["news_article_id", "model"]
    assert {"id", "batch_id", "row_number"}.isdisjoint(T.c.keys())
    assert not any(c.nullable for c in T.c)
    (fk,) = T.c.news_article_id.foreign_keys
    assert (fk.target_fullname, fk.ondelete) == ("perennial.news_articles.id", "CASCADE")
    assert T.c.model.type.length == models.article_sentiment.c.model.type.length


def test_last_error_is_a_short_code_from_a_fixed_list():
    assert models.ARTICLE_SCORING_ERRORS == ("failed", "timeout", "import_failed")
    assert max(map(len, models.ARTICLE_SCORING_ERRORS)) <= T.c.last_error.type.length


# ── migration 0021: the file ─────────────────────────────


def _migration():
    found = sorted(MIGRATIONS.glob("*_0021_*.py"))
    assert len(found) == 1, found
    return found[0]


def _assigned(tree, name):
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == name for t in node.targets
        ):
            return node.value.value
    return None


def test_migration_0021_follows_0020():
    path = _migration()
    assert re.fullmatch(r"\d{8}_0021_[a-z0-9_]+\.py", path.name), path.name
    tree = ast.parse(path.read_text())
    assert (_assigned(tree, "revision"), _assigned(tree, "down_revision")) == (
        "0021",
        "0020",
    )


@pytest.mark.parametrize("function", ["upgrade", "downgrade"])
def test_both_directions_set_a_local_lock_timeout_first(function):
    source = _migration().read_text()
    body = next(
        n
        for n in ast.parse(source).body
        if isinstance(n, ast.FunctionDef) and n.name == function
    )
    first = ast.get_source_segment(source, body.body[0])
    assert first == "op.execute(\"SET LOCAL lock_timeout = '5s'\")"


# ── constraints ──────────────────────────────────────────


@pytest.mark.db
def test_a_valid_row_is_stored_with_updated_at_defaulted(engine, seeded):  # noqa: F811
    add(engine, news_article_id=an_article(engine))
    with engine.connect() as conn:
        row = conn.execute(select(T)).one()
    assert (row.attempts, row.last_error) == (1, "failed")
    assert row.updated_at is not None and row.updated_at.tzinfo is not None


@pytest.mark.db
@pytest.mark.parametrize(
    "bad",
    [
        {"attempts": 0},
        {"attempts": -1},
        {"last_error": "boom"},
        {"last_error": "Failed"},
        {"last_error": None},
        {"attempts": None},
        {"model": None},
    ],
)
def test_constraints_refuse_bad_rows(engine, seeded, bad):  # noqa: F811
    article = an_article(engine)
    with pytest.raises(IntegrityError):
        add(engine, news_article_id=article, **bad)
    assert rows(engine) == 0


@pytest.mark.db
def test_one_row_per_article_and_model_and_a_new_model_starts_fresh(
    engine,
    seeded,  # noqa: F811
):
    article = an_article(engine)
    add(engine, news_article_id=article)
    with pytest.raises(IntegrityError):
        add(engine, news_article_id=article, attempts=2)
    add(engine, news_article_id=article, model=f"{MODEL}+text2")
    assert rows(engine) == 2


@pytest.mark.db
def test_an_unknown_article_is_refused(engine, seeded):  # noqa: F811
    with pytest.raises(IntegrityError):
        add(engine, news_article_id=987654321)


@pytest.mark.db
def test_deleting_the_article_deletes_its_retry_rows(engine, seeded):  # noqa: F811
    article = an_article(engine)
    add(engine, news_article_id=article)
    with engine.begin() as conn:
        conn.execute(
            models.news_articles.delete().where(models.news_articles.c.id == article)
        )
    assert rows(engine) == 0


# ── grants ───────────────────────────────────────────────


def test_policy_lists_it_as_state_with_delete_and_nothing_for_the_api():
    roles = provision_roles()
    assert "article_scoring_attempts" in roles.MUTABLE
    assert "article_scoring_attempts" not in roles.IMMUTABLE + roles.REFERENCE
    assert roles.DELETABLE == ("article_scoring_attempts",)
    assert "article_scoring_attempts" not in roles.API_TABLES
    assert "article_scoring_attempts" not in roles.API_COLUMNS


@pytest.mark.db
def test_grants_on_the_table(engine, groups):  # noqa: F811
    allowed = {
        "importer": {"SELECT", "INSERT", "UPDATE", "DELETE"},
        "reader": {"SELECT"},
        "api": set(),
    }
    with engine.connect() as conn:
        for role, granted in allowed.items():
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"):
                assert (
                    conn.execute(
                        text("SELECT has_table_privilege(:r, :t, :p)"),
                        {"r": groups[role], "t": TABLE, "p": privilege},
                    ).scalar()
                    is (privilege in granted)
                ), (role, privilege)


@pytest.mark.db
def test_no_other_table_gains_delete(engine, groups):  # noqa: F811
    with engine.connect() as conn:
        deletable = set(
            conn.execute(
                text(
                    "SELECT c.relname FROM pg_class c JOIN pg_namespace n"
                    " ON n.oid = c.relnamespace WHERE n.nspname = 'perennial'"
                    " AND c.relkind IN ('r', 'p')"
                    " AND has_table_privilege(:r, c.oid, 'DELETE')"
                ),
                {"r": groups["importer"]},
            ).scalars()
        )
    assert deletable == {"article_scoring_attempts"}


# ── migration 0021: up and down on a database of its own ─


def scored_once_and_failed(target):
    seed_rklb(target)
    run_news(
        target,
        NewsClient({"RKLB": parsed(item(1, "Rocket Lab wins", ""))}),
        NOW.date(),
    )
    with target.connect() as conn:
        article = conn.execute(select(models.news_articles.c.id)).scalar_one()
    add(target, news_article_id=article, last_error="timeout")


@pytest.mark.db
def test_with_no_retry_rows_the_downgrade_drops_the_table_and_keeps_the_articles(
    scratch,  # noqa: F811
):
    migrate(scratch, "0021")
    scored_once_and_failed(scratch)
    with scratch.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE}"))
    migrate(scratch, "0020", down=True)
    assert version(scratch) == "0020" and not exists(scratch)
    assert article_count(scratch) == 1
    migrate(scratch, "0021")
    assert version(scratch) == "0021" and exists(scratch) and rows(scratch) == 0


@pytest.mark.db
def test_while_a_retry_row_exists_the_downgrade_refuses_and_changes_nothing(
    scratch,  # noqa: F811
):
    migrate(scratch, "0021")
    scored_once_and_failed(scratch)
    with pytest.raises(RuntimeError, match="retry state exists"):
        migrate(scratch, "0020", down=True)
    assert version(scratch) == "0021" and rows(scratch) == 1
