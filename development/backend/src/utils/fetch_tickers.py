import json
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent.parent
TICKER_FILE = SRC_DIR / "services" / "sentiment" / "tickers.json"

async def fetch_tickers() -> set[str] | None:
    """
    Return the current ticker set from database.
    Return None if the source is unavailable or unreadable right now 
    the watcher will then keep the current set instead of wiping it

    returns a set of UPPERCASE tickers without "$", or None.
    """
    # ---- JSON file implementation (current) ----
    try:
        data = json.loads(TICKER_FILE.read_text())
        return {t.strip().upper().lstrip("$") for t in data["tickers"] if t and t.strip()}
    except (FileNotFoundError, json.JSONDecodeError, KeyError, TypeError):
        return None

    # ---- Supabase implementation (later) ----
    # Needs a client created once at startup (e.g. a module-level `sb`).
    # rows = (await sb.table("ticker_config").select("tickers").eq("id", 1).execute()).data
    # if not rows:
    #     return None
    # return {t.strip().upper().lstrip("$") for t in rows[0]["tickers"] if t}