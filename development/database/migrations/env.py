"""Alembic environment. Online migrations only; the URL is never logged."""

from alembic import context

# development/backend is on sys.path via prepend_sys_path in alembic.ini.
from src.db.config import DatabaseConfigError
from src.db.models import SCHEMA, metadata
from src.db.session import make_engine
from src.ingestion.redaction import describe_exception

config = context.config
if config.config_file_name is not None:
    from logging.config import fileConfig

    fileConfig(config.config_file_name, disable_existing_loggers=False)


def run_migrations_online() -> None:
    # Tests pass an existing connection through config.attributes.
    connection = config.attributes.get("connection")
    if connection is not None:
        _run(connection)
        return
    # Fixed-text boundary for configuration/connection problems: no URL,
    # no driver message (which can echo host/user details).
    try:
        engine = make_engine()
    except DatabaseConfigError as exc:
        raise SystemExit(f"error: {exc}") from None
    except Exception as exc:
        raise SystemExit(f"error: database engine could not be created: {describe_exception(exc)}") from None
    try:
        try:
            conn = engine.connect()
        except Exception as exc:
            raise SystemExit(f"error: could not connect to the database: {describe_exception(exc)}") from None
        with conn:
            _run(conn)
    finally:
        engine.dispose()


def _run(connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=metadata,
        include_schemas=True,
        include_name=lambda name, type_, parent: type_ != "schema" or name == SCHEMA,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()
    # SQLAlchemy 2 connections autobegin; make sure DDL is committed.
    if connection.in_transaction():
        connection.commit()


if context.is_offline_mode():
    raise SystemExit("Offline (--sql) migrations are not supported; connect to a database.")
run_migrations_online()
