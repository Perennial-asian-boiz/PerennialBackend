"""
Security identity (M4): resolve provider tickers through stored aliases, and
apply the reviewed seed manifest that creates those aliases.

Aliases live in security_identifiers as (scheme, value) -> security_id. For
scheme "ticker", a bare value is the canonical spelling ("BRK.B") and a value
with a fixed provider prefix is that provider's spelling ("FINNHUB:BRK-B").
The prefix comes from the source being imported, never from a record.

Resolution never creates a security or an alias and never calls a network
service. A ticker resolves to exactly one active security, or the caller gets
a fixed failure code for it. Seeds are applied by an operator (owner role)
through development/database/ops/seed_identity.py; conflicting ownership stops
the whole seed transaction instead of being overwritten.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import insert, select, update
from sqlalchemy.engine import Connection

from src.db.models import (
    IDENTIFIER_SCHEMES,
    SECURITY_TYPES,
    company_profiles,
    market_cap_observations,
    securities,
    security_identifiers,
)

# Fixed provider namespace for each source's incoming spellings.
SOURCE_PROVIDERS = {
    "congress_trades": "FMP",
    "ark_holdings": "ARK",
    "insider_trades": "FINNHUB",
    "short_interest": "NASDAQ",
}
PROVIDERS = frozenset({*SOURCE_PROVIDERS.values()})
# Sources whose adapters return results under the canonical symbols they were
# given (M4 market caps): no provider namespace, so only canonical tickers match.
CANONICAL_ONLY_SOURCES = frozenset({"market_caps"})
# Pattern evaluation plans from the published watchlist's canonical symbols.
CANONICAL_ONLY_SOURCES |= {"fundamentals", "fundamental_scores"}
# S1-03 news ingestion plans from the same published watchlist.
CANONICAL_ONLY_SOURCES |= {"news_articles"}
# Sprint 2 scores derive from securities the platform already resolved.
CANONICAL_ONLY_SOURCES |= {"ticker_sentiment", "company_scores", "score_explanations"}

UNRESOLVED = "unresolved"
INACTIVE = "inactive_security"
ALIAS_COLLISION = "alias_collision"


def provider_alias(provider: str, ticker: str) -> str:
    if provider not in PROVIDERS:
        raise ValueError("unknown provider")
    return f"{provider}:{ticker}"


def resolve_tickers(
    conn: Connection, source: str, tickers: Iterable[str]
) -> Tuple[Dict[str, int], List[Tuple[str, str]]]:
    """
    Map each normalized ticker to a security id: the source's provider alias
    first, then the canonical ticker (only the canonical ticker for
    CANONICAL_ONLY_SOURCES). Returns (ids, failures), where failures are
    (ticker, code) pairs in ticker order.
    """
    provider = None if source in CANONICAL_ONLY_SOURCES else SOURCE_PROVIDERS[source]
    wanted = sorted(set(tickers))
    if not wanted:
        return {}, []
    values = {provider_alias(provider, t): t for t in wanted} if provider else {}
    rows = conn.execute(
        select(
            security_identifiers.c.value,
            security_identifiers.c.security_id,
            securities.c.is_active,
        )
        .join(securities, securities.c.id == security_identifiers.c.security_id)
        .where(
            security_identifiers.c.scheme == "ticker",
            security_identifiers.c.value.in_([*values, *wanted]),
        )
    ).all()
    found = {row.value: (row.security_id, row.is_active) for row in rows}
    ids: Dict[str, int] = {}
    failures: List[Tuple[str, str]] = []
    for ticker in wanted:
        alias = found.get(provider_alias(provider, ticker)) if provider else None
        hit = alias or found.get(ticker)
        if hit is None:
            failures.append((ticker, UNRESOLVED))
        elif not hit[1]:
            failures.append((ticker, INACTIVE))
        else:
            ids[ticker] = hit[0]
    return ids, failures


def request_symbol(conn: Connection, security_id: int, provider: str) -> str:
    """
    The one spelling to send to `provider` for this security. For FINNHUB an
    issuer's insider_reporting_security_id routes the request (GOOG -> GOOGL);
    the requesting security keeps its own identity and attribution.
    """
    target = security_id
    if provider == "FINNHUB":
        routed = conn.execute(
            select(company_profiles.c.insider_reporting_security_id)
            .join(securities, securities.c.company_profile_id == company_profiles.c.id)
            .where(securities.c.id == security_id)
        ).scalar_one_or_none()
        target = routed or security_id
    prefix = provider_alias(provider, "")
    spellings = (
        conn.execute(
            select(security_identifiers.c.value).where(
                security_identifiers.c.security_id == target,
                security_identifiers.c.scheme == "ticker",
                security_identifiers.c.value.startswith(prefix, autoescape=True),
            )
        )
        .scalars()
        .all()
    )
    if len(spellings) > 1:
        raise LookupError("ambiguous provider request spelling")
    if spellings:
        return str(spellings[0])[len(prefix) :]
    symbol: str = conn.execute(
        select(securities.c.symbol).where(securities.c.id == target)
    ).scalar_one()
    return symbol


# ── seeds ────────────────────────────────────────────────

SEED_SECURITY_FIELDS = ("exchange", "currency", "name", "security_type", "country")


class SeedConflict(Exception):
    """The manifest disagrees with stored identity; nothing was written."""


def load_manifest(path: Path) -> Dict[str, Any]:
    manifest = json.loads(path.read_text())
    if not isinstance(manifest, dict) or set(manifest) - {
        "note",
        "securities",
        "aliases",
        "profiles",
    }:
        raise ValueError("seed manifest has unexpected keys")
    for item in manifest.get("securities", []):
        if item.get("security_type") not in (None, *SECURITY_TYPES):
            raise ValueError("seed security has an unknown security_type")
    for item in manifest.get("aliases", []):
        if item.get("scheme") not in IDENTIFIER_SCHEMES:
            raise ValueError("seed alias has an unknown scheme")
    return manifest


def _security_id(conn: Connection, symbol: str) -> int:
    found = conn.execute(
        select(securities.c.id).where(securities.c.symbol == symbol)
    ).scalar_one_or_none()
    if found is None:
        raise SeedConflict(f"seed references unknown security {symbol}")
    return int(found)


def _add_alias(conn: Connection, scheme: str, value: str, security_id: int) -> bool:
    owner = conn.execute(
        select(security_identifiers.c.security_id).where(
            security_identifiers.c.scheme == scheme,
            security_identifiers.c.value == value,
        )
    ).scalar_one_or_none()
    if owner is None:
        conn.execute(
            insert(security_identifiers).values(
                scheme=scheme, value=value, security_id=security_id
            )
        )
        return True
    if owner != security_id:
        raise SeedConflict(f"{scheme} alias {value} belongs to another security")
    return False


def apply_seeds(conn: Connection, manifest: Dict[str, Any]) -> Dict[str, int]:
    """Idempotently apply a manifest inside the caller's transaction."""
    counts = {"securities_added": 0, "aliases_added": 0, "profiles_added": 0}
    for item in manifest.get("securities", []):
        symbol = item["symbol"]
        wanted = {k: item[k] for k in SEED_SECURITY_FIELDS if item.get(k) is not None}
        row = (
            conn.execute(select(securities).where(securities.c.symbol == symbol))
            .mappings()
            .one_or_none()
        )
        if row is None:
            security_id = conn.execute(
                insert(securities)
                .values(symbol=symbol, **wanted)
                .returning(securities.c.id)
            ).scalar_one()
            counts["securities_added"] += 1
        else:
            security_id = row["id"]
            changes = {}
            for key, value in wanted.items():
                if row[key] is None:
                    changes[key] = value
                elif row[key] != value:
                    raise SeedConflict(f"security {symbol} has a different {key}")
            if changes:
                conn.execute(
                    update(securities)
                    .where(securities.c.id == security_id)
                    .values(**changes)
                )
        counts["aliases_added"] += _add_alias(conn, "ticker", symbol, security_id)
    for item in manifest.get("aliases", []):
        security_id = _security_id(conn, item["symbol"])
        counts["aliases_added"] += _add_alias(
            conn, item["scheme"], item["value"], security_id
        )
    for item in manifest.get("profiles", []):
        counts["profiles_added"] += _apply_profile(conn, item)
    return counts


def _apply_profile(conn: Connection, item: Dict[str, Any]) -> int:
    members = [_security_id(conn, s) for s in item["members"]]
    linked = set(
        conn.execute(
            select(securities.c.company_profile_id).where(securities.c.id.in_(members))
        ).scalars()
    )
    existing = linked - {None}
    if len(existing) > 1:
        raise SeedConflict("profile members already belong to different profiles")
    reporting = item.get("insider_reporting_symbol")
    reporting_id = _security_id(conn, reporting) if reporting else None
    if reporting_id is not None and reporting_id not in members:
        raise SeedConflict("insider reporting security must be a profile member")
    wanted = {k: item[k] for k in ("legal_name", "cik") if item.get(k) is not None}
    if reporting_id is not None:
        wanted["insider_reporting_security_id"] = reporting_id
    added = 0
    if existing:
        profile_id = existing.pop()
        row = (
            conn.execute(
                select(company_profiles).where(company_profiles.c.id == profile_id)
            )
            .mappings()
            .one()
        )
        changes = {}
        for key, value in wanted.items():
            if row[key] is None:
                changes[key] = value
            elif row[key] != value:
                raise SeedConflict(f"profile has a different {key}")
        if changes:
            conn.execute(
                update(company_profiles)
                .where(company_profiles.c.id == profile_id)
                .values(**changes)
            )
    else:
        profile_id = conn.execute(
            insert(company_profiles).values(**wanted).returning(company_profiles.c.id)
        ).scalar_one()
        added = 1
    conn.execute(
        update(securities)
        .where(securities.c.id.in_(members), securities.c.company_profile_id.is_(None))
        .values(company_profile_id=profile_id)
    )
    return added


def backfill_profile_classification(conn: Connection) -> Dict[str, int]:
    """
    Copy sector/industry from market_cap_observations into company_profiles.

    Each security's value is its latest observation with a non-null value,
    ordered by observed_at (falling back to retrieved_at). A security without a
    profile gets its own profile. Stored values are never overwritten. When the
    members of one profile disagree, or two equally recent observations
    disagree, nothing is written for that field and it is counted for review.
    """
    counts = {"profiles_added": 0, "fields_filled": 0, "conflicts": 0}
    rows = conn.execute(
        select(
            market_cap_observations.c.security_id,
            market_cap_observations.c.sector,
            market_cap_observations.c.industry,
            market_cap_observations.c.observed_at,
            market_cap_observations.c.retrieved_at,
        )
    ).all()
    latest: Dict[Tuple[int, str], Tuple[datetime, set[str]]] = {}
    for row in rows:
        at = row.observed_at or row.retrieved_at
        for column in ("sector", "industry"):
            value: Optional[str] = getattr(row, column)
            if value is None:
                continue
            key = (row.security_id, column)
            prior = latest.get(key)
            if prior is None or at > prior[0]:
                latest[key] = (at, {value})
            elif at == prior[0]:
                prior[1].add(value)
    by_profile: Dict[int, Dict[str, set[str]]] = {}
    for security_id in sorted({key[0] for key in latest}):
        profile_id = conn.execute(
            select(securities.c.company_profile_id).where(
                securities.c.id == security_id
            )
        ).scalar_one()
        if profile_id is None:
            profile_id = conn.execute(
                insert(company_profiles).values().returning(company_profiles.c.id)
            ).scalar_one()
            conn.execute(
                update(securities)
                .where(securities.c.id == security_id)
                .values(company_profile_id=profile_id)
            )
            counts["profiles_added"] += 1
        fields = by_profile.setdefault(profile_id, {})
        for column in ("sector", "industry"):
            if (security_id, column) in latest:
                fields.setdefault(column, set()).update(
                    latest[(security_id, column)][1]
                )
    for profile_id, fields in sorted(by_profile.items()):
        stored = (
            conn.execute(
                select(company_profiles).where(company_profiles.c.id == profile_id)
            )
            .mappings()
            .one()
        )
        changes = {}
        for column, values in fields.items():
            if stored[column] is not None:
                continue
            if len(values) != 1:
                counts["conflicts"] += 1
                continue
            changes[column] = next(iter(values))
        if changes:
            conn.execute(
                update(company_profiles)
                .where(company_profiles.c.id == profile_id)
                .values(**changes)
            )
            counts["fields_filled"] += len(changes)
    return counts
