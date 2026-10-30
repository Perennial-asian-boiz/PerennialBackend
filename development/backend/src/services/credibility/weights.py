"""The committed source-weight table (source_weights.json).

Sources are publisher base domains (reuters.com) or social platforms (reddit).
A host matches its own entry or its nearest listed parent domain, so
finance.yahoo.com takes yahoo.com's weight. A source that matches nothing gets
the lowest weight listed (S1-05 AC 2).

The file also names the rest of the calculation: half_life_days, how fast an
item's weight fades with age, and relevance, the rule that decides whether an
article is about its ticker. Changing anything in the file changes the version,
which every ticker_sentiment row stores.
"""

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

WEIGHTS_FILE = Path(__file__).with_name("source_weights.json")
# The relevance rules this code implements. "headline-match-v1": S1-03's match-v1
# applied to the headline alone, so an article that named the company only in its
# summary is left out.
RELEVANCE_RULES = ("headline-match-v1",)


@dataclass(frozen=True)
class WeightTable:
    version: str
    weights: Mapping[str, float]
    half_life_days: float
    relevance: str

    @property
    def lowest(self) -> float:
        return min(self.weights.values())

    def weight_for(self, source: str | None) -> float:
        """The weight of a domain or platform; the lowest listed weight if unknown."""
        parts = (source or "").lower().split(".")
        for i in range(len(parts)):
            weight = self.weights.get(".".join(parts[i:]))
            if weight is not None:
                return weight
        return self.lowest


def parse_weights(data: Any) -> WeightTable:
    if not isinstance(data, dict) or not isinstance(data.get("version"), str):
        raise ValueError("weight table needs a version")
    version, tiers = data["version"].strip(), data.get("tiers")
    if not version or len(version) > 32 or not isinstance(tiers, dict) or not tiers:
        raise ValueError("weight table needs a short version and tiers")
    half_life, relevance = data.get("half_life_days"), data.get("relevance")
    if (
        not isinstance(half_life, (int, float))
        or isinstance(half_life, bool)
        or not math.isfinite(half_life)
        or half_life <= 0
    ):
        raise ValueError("half_life_days must be a positive number")
    if relevance not in RELEVANCE_RULES:
        raise ValueError(f"relevance must be one of {RELEVANCE_RULES}")
    weights: dict[str, float] = {}
    for tier in tiers.values():
        weight, sources = tier.get("weight"), tier.get("sources")
        if not isinstance(weight, (int, float)) or not 0 < weight <= 1:
            raise ValueError("each weight must be in (0, 1]")
        if not isinstance(sources, list) or not sources:
            raise ValueError("each tier needs sources")
        for source in sources:
            key = str(source).strip().lower()
            if not key or key in weights:
                raise ValueError("a source is empty or listed twice")
            weights[key] = float(weight)
    return WeightTable(version, weights, float(half_life), relevance)


def load_weights(path: Path = WEIGHTS_FILE) -> WeightTable:
    return parse_weights(json.loads(path.read_text()))
