"""
Canonical batch identity.

HASH_VERSION names the rule below; change it whenever the rule changes so
old and new hashes are never compared as if they meant the same thing.

Rule "v2" (v1 serialization plus canonical short-interest history field names,
quantized history ratios and retained Congress provider transaction identity):
  * Hash only validated business fields (SourceRecord.canonical()): no
    `fetched_at`, no top-level totals/notes/endpoints, no unknown keys.
  * Each record is serialized as JSON with sorted keys, compact separators,
    dates as YYYY-MM-DD and Decimals in normalized plain notation ("1.5", not
    "1.50" or "1.5E+0"), so 1, 1.0 and "1.00" hash the same.
  * Records are sorted by that serialization. Fetcher ordering is therefore
    ignored (a reordered but otherwise identical snapshot is the same batch),
    while multiplicity is kept: two identical trades stay two entries.
  * row_number is the 1-based position in this canonical order, so identical
    content always yields identical rows.
  * sha256 over {"hash_version", "source", "source_as_of", "records"}
    serialized the same way. source_as_of is a genuine provider date (or
    null), so two empty snapshots for different dates stay distinct.
    Retrieval/run timestamps (`fetched_at`) are never part of identity.

The stored payload is the identity document plus the sanitized fetcher
envelope (src/ingestion/envelope.py), which is informational only.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Sequence

from src.ingestion.envelope import sanitize_envelope
from src.ingestion.schemas import SourceRecord

HASH_VERSION = "v2"


def _dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class CanonicalBatch:
    source: str
    content_hash: str
    records: List[SourceRecord]  # in canonical order; row_number = index + 1
    payload: Dict[str, Any]


def canonicalize(
    source: str,
    records: Sequence[SourceRecord],
    source_as_of: Optional[date] = None,
    envelope: Optional[Dict[str, Any]] = None,
) -> CanonicalBatch:
    keyed = sorted(
        ((_dumps(r.canonical()), r) for r in records), key=lambda pair: pair[0]
    )
    identity = {
        "hash_version": HASH_VERSION,
        "source": source,
        "source_as_of": source_as_of.isoformat() if source_as_of else None,
        "records": [json.loads(text) for text, _ in keyed],
    }
    content_hash = hashlib.sha256(_dumps(identity).encode("utf-8")).hexdigest()
    payload = dict(identity, envelope=sanitize_envelope(source, envelope))
    return CanonicalBatch(
        source=source,
        content_hash=content_hash,
        records=[r for _, r in keyed],
        payload=payload,
    )
