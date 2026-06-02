"""Entrypoint: pull the card dimension.

Hits ``/cards`` and writes the cleaned card rows to a JSON file that the bronze
layer loads into ``dim_cards``. Like ``discover_players.py`` this is a cheap,
single-request seed step, kept separate from the long battlelog fan-out so it
can be re-run independently.

The card list is small (~120 rows) and slowly changing (Supercell adds a card
every few weeks), so each run does a full refresh: fetch all cards, overwrite
the file. The bronze ``dim_cards`` table is likewise truncate-and-reload — no
bookkeeping needed.

Usage:
    python -m ingestion.pull_cards --out data/raw/cards.json
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from ingestion.client import ClashRoyaleClient
from ingestion.config import load_settings
from ingestion.parsers import parse_cards

logger = logging.getLogger(__name__)

def fetch_cards(client: ClashRoyaleClient | None = None) -> list[dict]:
    """Return the full card dimension as cleaned rows."""
    owns_client = client is None
    client = client or ClashRoyaleClient()
    try:
        raw = client.get_cards()
    finally:
        # only manage the lifecycle of local instances
        if owns_client:
            client.close()
    cards = parse_cards(raw)
    logger.info("fetched %d cards", len(cards))
    return cards

def write_cards(cards: list[dict], out_path: Path) -> None:
    """Create parent dir if not exists, and write cards to JSON file."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(cards, indent=2), encoding="utf-8") # formatted json
    logger.info("wrote %d cards to %s", len(cards), out_path)

def main(argv: list[str] | None = None) -> int:
    """Parse arguments, set logging, fetch card dimension, and write to file."""
    parser = argparse.ArgumentParser(description="Pull the card dimension.")
    parser.add_argument(
        "--out", type=Path, default=None,
        help="Output JSON path (default: <CR_RAW_DIR>/cards.json).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    out_path = args.out or (load_settings().raw_dir / "cards.json")
    cards = fetch_cards()
    write_cards(cards, out_path)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
