"""Load theme_sector_map.json: which GDELT themes to watch and which outlets count as evidence."""
import json
import re
from dataclasses import dataclass
from pathlib import Path

MAP_PATH = Path(__file__).resolve().parent / "theme_sector_map.json"
THEME_NAME = re.compile(r"^[A-Z0-9_]{1,64}$")


@dataclass(frozen=True)
class ThemeMap:
    themes: dict[str, dict]
    evidence_domains: tuple[str, ...]

    def is_evidence_domain(self, domain: str | None) -> bool:
        return bool(domain) and any(
            domain == d or domain.endswith("." + d) for d in self.evidence_domains)


def load_theme_map(path: Path = MAP_PATH) -> ThemeMap:
    raw = json.loads(path.read_text(encoding="utf-8"))
    themes = raw.get("themes")
    if not isinstance(themes, dict) or not themes:
        raise ValueError("theme map has no themes")
    bad = [t for t in themes if not THEME_NAME.match(t)]
    if bad:
        raise ValueError(f"theme names must be GDELT theme codes: {bad}")
    domains = raw.get("evidence_domains", [])
    if not isinstance(domains, list) or not all(isinstance(d, str) and d for d in domains):
        raise ValueError("evidence_domains must be a list of domain names")
    return ThemeMap(themes, tuple(d.lower() for d in domains))
