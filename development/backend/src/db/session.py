"""Engine factory shared by the importer, CLI, and migrations."""

from typing import Any, Literal, Optional, Union

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine

from src.db.config import get_database_url, parse_database_url

Workload = Literal["pipeline", "api", "migrate", "cli"]

_POOL = {
    "pipeline": (2, 4),
    "api": (5, 5),
    "migrate": (1, 0),
    "cli": (2, 4),
}
_STATEMENT_TIMEOUT_MS = {"pipeline": 60_000, "api": 5_000, "migrate": 0, "cli": 60_000}


def make_engine(
    url: Optional[Union[str, URL]] = None,
    *,
    workload: Workload = "cli",
    **kwargs: Any,
) -> Engine:
    """
    Create an engine. hide_parameters keeps bound values (row contents) out of
    SQLAlchemy exception messages, so a failed insert cannot echo payload data.
    """
    if url is None:
        url = get_database_url()
    elif isinstance(url, str):
        url = parse_database_url(url)
    if workload not in _POOL:
        raise ValueError("unknown database workload")
    kwargs.setdefault("hide_parameters", True)
    kwargs.setdefault("pool_pre_ping", True)
    kwargs.setdefault("pool_size", _POOL[workload][0])
    kwargs.setdefault("max_overflow", _POOL[workload][1])
    connect_args = dict(kwargs.pop("connect_args", {}))
    connect_args.setdefault("connect_timeout", 10)
    connect_args["application_name"] = f"perennial-{workload}"
    options = str(connect_args.get("options", "")).strip()
    connect_args["options"] = " ".join(
        part
        for part in (
            options,
            f"-c statement_timeout={_STATEMENT_TIMEOUT_MS[workload]}",
            "-c lock_timeout=5000",
            "-c idle_in_transaction_session_timeout=60000",
        )
        if part
    )
    kwargs["connect_args"] = connect_args
    return create_engine(url, **kwargs)
