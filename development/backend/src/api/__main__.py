"""``python -m src.api``: the one way to serve the integrated app (change 7).

uvicorn's access log is off (it prints raw request lines, tokens in auth links
included), its own proxy handling is off (``read.install`` adds the only proxy
middleware) and ``log_config=None`` leaves its loggers to the app's JSON
handler. API_HOST and API_PORT choose the listener (default 127.0.0.1:8000).
"""

import os

import uvicorn

from src.utils.logger import configure_logging


def main() -> None:
    configure_logging()
    uvicorn.run(
        "src.api.app:app",
        host=os.environ.get("API_HOST", "127.0.0.1"),
        port=int(os.environ.get("API_PORT", "8000")),
        proxy_headers=False,
        access_log=False,
        log_config=None,
    )


if __name__ == "__main__":
    main()
