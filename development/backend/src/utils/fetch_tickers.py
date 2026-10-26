import json
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent.parent
TICKER_FILE = SRC_DIR / "services" / "sentiment" / "tickers.json"


def _clean(entry) -> str | None:
    """Normalise one entry to an UPPERCASE ticker without '$', or None if unusable."""
    if isinstance(entry, dict):
        entry = entry.get("ticker") or entry.get("symbol")
    if not isinstance(entry, str):
        return None
    ticker = entry.strip().upper().lstrip("$")
    return ticker or None


def _read_ticker_list() -> list[str] | None:
    """
    Read the ticker source. Accepted shapes:
      ["AAPL", "MSFT"]
      {"tickers": ["AAPL", "MSFT"]}
      {"AAPL": {...}, "MSFT": {...}}              (keys are used as tickers)
      [{"ticker": "AAPL"}, {"symbol": "MSFT"}]

    Returns tickers in file order with duplicates removed, or None if the source
    is missing, unreadable, malformed or contains no tickers.
    """
    # ---- JSON file implementation (current) ----
    try:
        raw = json.loads(TICKER_FILE.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            raw = raw["tickers"] if "tickers" in raw else list(raw.keys())
        if not isinstance(raw, list):
            return None
        cleaned = (_clean(entry) for e in raw for entry in (e,))
        tickers = list(dict.fromkeys(t for t in cleaned if t))
    except (OSError, ValueError, KeyError, TypeError):
        # OSError: missing/unreadable file; ValueError: bad JSON or bad encoding
        return None
    return tickers or None

    # ---- Supabase implementation (later) ----
    # Needs a client created once at startup (e.g. a module-level `sb`).
    # rows = (await sb.table("ticker_config").select("tickers").eq("id", 1).execute()).data
    # if not rows:
    #     return None
    # return _normalise(rows[0]["tickers"])


def _read_ticker_map() -> dict[str, str] | None:
    """
    Read the ticker file as {company name: UPPERCASE ticker}.

    Expected file shape:
      {"NVIDIA": "NVDA", "Apple": "AAPL", ...}   (key = company name, value = ticker)

    Company names keep their original casing; tickers are uppercased and have any
    leading '$' removed. Entries whose name or ticker is unusable are skipped.
    Returns None if the file is missing, unreadable, not a JSON object, or has no
    usable entries.
    """
    try:
        raw = json.loads(TICKER_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(raw, dict):
        return None

    mapping = {}
    for name, symbol in raw.items():
        ticker = _clean(symbol)
        if isinstance(name, str) and name.strip() and ticker:
            mapping[name.strip()] = ticker
    return mapping or None


async def fetch_ticker_list() -> list[str] | None:
    """
    Ordered, de-duplicated list of tickers (for callers that care about order,
    e.g. round-robin processing). None if the source is unavailable right now.
    """
    return _read_ticker_list()


async def fetch_tickers() -> set[str] | None:
    """
    Return the current ticker set from the database.
    Return None if the source is unavailable or unreadable right now, so the
    watcher keeps the current set instead of wiping it.

    Returns a set of UPPERCASE tickers without "$", or None.
    """
    tickers = _read_ticker_list()
    return set(tickers) if tickers is not None else None


async def fetch_ticker_map() -> dict[str, str] | None:
    """
    Return the ticker file as {company name: UPPERCASE ticker}, e.g.
    {"NVIDIA": "NVDA", "Apple": "AAPL"}. None if the source is unavailable or
    has no usable entries.
    """
    return _read_ticker_map()
