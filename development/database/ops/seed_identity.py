"""Apply the reviewed M4 identity seeds, then backfill profile sector/industry.

Run after migrations and grants, as the schema owner, with OPS_DATABASE_URL:
    python development/database/ops/seed_identity.py [--manifest PATH]
One transaction: a seed conflict rolls everything back. Idempotent; reruns add
nothing. Output is counts only. No network lookups (FIGI enrichment is not here).
"""
import argparse
import json
import os
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

BACKEND = Path(__file__).resolve().parents[2] / "backend"
sys.path.insert(0, str(BACKEND))

from src.ingestion.identity import (  # noqa: E402
    SeedConflict,
    apply_seeds,
    backfill_profile_classification,
    load_manifest,
)

DEFAULT_MANIFEST = Path(__file__).resolve().parents[1] / "seeds" / "identity.json"


def run(engine, manifest_path):
    manifest = load_manifest(manifest_path)
    with engine.begin() as conn:
        counts = apply_seeds(conn, manifest)
        counts.update({f"backfill_{k}": v for k, v in backfill_profile_classification(conn).items()})
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()
    try:
        url = make_url(os.environ.get("OPS_DATABASE_URL", ""))
        if url.drivername not in {"postgresql", "postgresql+psycopg"} or not url.host or not url.database:
            raise ValueError("Expected explicit PostgreSQL OPS_DATABASE_URL")
        engine = create_engine(url.set(drivername="postgresql+psycopg"))
        counts = run(engine, args.manifest)
    except SeedConflict as exc:
        # Conflict text names only reviewed manifest symbols, never provider data.
        print(f"Identity seed refused: {exc}; transaction rolled back", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Identity seed failed ({type(exc).__name__}); transaction rolled back", file=sys.stderr)
        return 1
    print(json.dumps(counts, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
