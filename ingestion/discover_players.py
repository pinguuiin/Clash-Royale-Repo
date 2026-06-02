"""Entrypoint: discover top-ladder player tags.

Hits ``/locations/global/rankings/players`` and writes the cleaned tags to a
JSON file that ``pull_battlelogs.py`` then consumes. Kept separate from the
battlelog pull so the (cheap, single-request) seed step can be re-run
independently of the (long, rate-limited) fan-out step.

Usage:
    python -m ingestion.discover_players --limit 1000 --out data/raw/players.json
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from ingestion.client import ClashRoyaleClient
from ingestion.config import load_settings
from ingestion.parsers import parse_rankings

logger = logging.getLogger(__name__)

def discover_players(limit: int, client: ClashRoyaleClient | None = None) -> list[dict]:
    """Return the top ``limit``(limit=1000) ladder players as cleaned ranking rows."""
    owns_client = client is None
    client = client or ClashRoyaleClient()
    try:
        raw = client.get_top_players(limit=limit)
    finally:
        # only manage the lifecycle of local instances
        if owns_client:
            client.close()
    players = parse_rankings(raw)
    logger.info("discovered %d players (limit=%d)", len(players), limit)
    return players

def write_players(players: list[dict], out_path: Path) -> None:
    """Create parent dir if not exists, and write players to JSON file."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(players, indent=2), encoding="utf-8") # formatted json
    logger.info("wrote %d players to %s", len(players), out_path)

def main(argv: list[str] | None = None) -> int:
    """Parse arguments, set logging, fetch players data, and write to file."""
    
    # Use arguments for ``limit`` and ``output path``. better flexibility when test from cli
    parser = argparse.ArgumentParser(description="Discover top-ladder player tags.")
    parser.add_argument(
        "--limit", type=int, default=1000,
        help="Number of top players to fetch (API max is 1000).",
    )
    parser.add_argument(
        "--out", type=Path, default=None,
        help="Output JSON path (default: <CR_RAW_DIR>/players.json).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    # Get player data and write to a JSON file
    out_path = args.out or (load_settings().raw_dir / "players.json")
    players = discover_players(limit=args.limit) # could pass a fake client here for testing
    write_players(players, out_path)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
