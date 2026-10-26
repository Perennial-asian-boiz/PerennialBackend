"""Article-to-ticker matching, rule match-v1 (S1-03 AC 3).

Finnhub tags each article with the ticker it was requested for. The tag alone is
not trusted: an article is kept for a ticker only when its headline or summary
names the company, in this order:

1. ticker: "$NVDA" anywhere, or "NVDA" as an uppercase whole word. Tickers of one
   or two letters (F, MS, DE) need the "$" form or an exchange form such as
   "(NYSE: MS)", because they collide with ordinary words.
2. name: a variant of the company's name as a whole phrase, case-insensitive.
   Variants come from the legal name, alias names and security name, with legal
   suffixes removed (Inc, Corp, Holdings, ...) and then generic trailing words
   (Technologies, Therapeutics, ...). A single-word variant that is also an
   ordinary word in financial news (Block, Target, Square) never matches
   by itself.

Anything else is dropped. The rule is pure: no network, no database.
"""

import re
from collections.abc import Iterable

MATCH_RULE_VERSION = "match-v1"

LEGAL_SUFFIXES = frozenset(
    {
        "inc", "incorporated", "corp", "corporation", "co", "company", "ltd",
        "limited", "plc", "llc", "lp", "holdings", "holding", "group", "sa", "nv",
        "ag", "se", "class", "cl", "a", "b", "c", "adr", "ads", "sponsored", "spon",
        "the", "new", "com",
    }
)  # fmt: skip
GENERIC_TAIL = frozenset(
    {
        "technologies", "technology", "therapeutics", "pharmaceuticals", "systems",
        "platforms", "global", "markets", "international", "industries",
        "enterprises", "networks", "biosciences", "bio", "health", "aviation",
        "devices", "innovation", "defense", "security", "internet", "machines",
    }
)  # fmt: skip
# Single words too common to stand for a company on their own.
COMMON_WORDS = frozenset(
    {
        "block", "target", "gap", "circle", "beam", "toast", "compass", "prime",
        "generate", "adaptive", "aurora", "pacific", "scribe", "genius", "kodiak",
        "pony", "archer", "square", "shell", "unity", "general", "american",
        "united", "first", "national", "energy", "capital", "digital", "snow",
    }
)  # fmt: skip
_WORD = re.compile(r"[a-z0-9]+")


def normalize(text: str) -> str:
    return " ".join(_WORD.findall(text.lower()))


def name_variants(names: Iterable[str | None]) -> set[str]:
    """Normalized phrases that may stand for the company in a headline."""
    variants: set[str] = set()
    for raw in names:
        if not raw:
            continue
        words = [w for w in normalize(raw).split() if w not in LEGAL_SUFFIXES]
        while words:
            phrase = " ".join(words)
            if len(phrase) >= 3 and not (len(words) == 1 and phrase in COMMON_WORDS):
                variants.add(phrase)
            if len(words) == 1 or words[-1] not in GENERIC_TAIL:
                break
            words = words[:-1]
    return variants


def _ticker_pattern(symbol: str) -> re.Pattern[str]:
    t = re.escape(symbol.upper())
    exchange = (
        rf"\((?:NYSE|NASDAQ|Nasdaq|NYSEAMERICAN|NYSE American|AMEX|OTC)\s*:\s*{t}\)"
    )
    if len(symbol) <= 2:
        return re.compile(rf"\${t}\b|{exchange}")
    return re.compile(rf"\${t}\b|(?<![A-Za-z0-9$.]){t}(?![A-Za-z0-9])|{exchange}")


def match_rule(
    headline: str, summary: str, symbol: str, variants: set[str]
) -> str | None:
    """'ticker' or 'name' when the text names the company, else None."""
    text = f"{headline}\n{summary}"
    if _ticker_pattern(symbol).search(text):
        return "ticker"
    padded = f" {normalize(text)} "
    if any(f" {v} " in padded for v in variants):
        return "name"
    return None
