import logging
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
LOG_DIR = BACKEND_DIR/"logs"
LOG_DIR.mkdir(exist_ok=True)

def create_logger(log_file):
    """ creates logger for subsequent scripts """
    logger = logging.getLogger("youtube")
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers if called multiple times
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"
    )

    handler = logging.FileHandler(f"logs/{log_file}", encoding="utf-8")
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger