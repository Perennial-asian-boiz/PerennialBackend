"""Engine factory shared by the importer, CLI, and migrations."""

from typing import Any, Optional, Union

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine

from src.db.config import get_database_url, parse_database_url


def make_engine(url: Optional[Union[str, URL]] = None, **kwargs: Any) -> Engine:
    """
    Create an engine. hide_parameters keeps bound values (row contents) out of
    SQLAlchemy exception messages, so a failed insert cannot echo payload data.
    """
    if url is None:
        url = get_database_url()
    elif isinstance(url, str):
        url = parse_database_url(url)
    kwargs.setdefault("hide_parameters", True)
    kwargs.setdefault("pool_pre_ping", True)
    connect_args = dict(kwargs.pop("connect_args", {}))
    connect_args.setdefault("connect_timeout", 10)
    kwargs["connect_args"] = connect_args
    return create_engine(url, **kwargs)
