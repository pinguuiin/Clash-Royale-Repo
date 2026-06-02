"""Entrypoint: discover player tags to feed the battlelog pull.

Fetch the top clans for a location, fan out to each clan's member list, and dedupe
the tags. Kept separate from the battlelog pull so this (cheaper) seed step can be
re-run independently of the long, rate-limited fan-out.

Usage:
    python -m ingestion.discover_players --max-clans 200 --out data/raw/players.json
    python -m ingestion.discover_players --location 57000000   # Europe only
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from ingestion.client import ClashRoyaleAPIError, ClashRoyaleClient
from ingestion.config import load_settings
from ingestion.parsers import parse_clan_members, parse_clan_rankings

logger = logging.getLogger(__name__)

def discover_players(
    location: str = "global",
    max_clans: int = 200,
    client: ClashRoyaleClient | None = None,
) -> list[dict]:
    """Return deduped player rows from the members of the top ``max_clans`` clans.

    A per-clan API failure is logged and skipped rather than aborting the run.
    Tags are deduped while preserving order, so a player is kept under the
    highest-ranked clan they appear in.
    """
    owns_client = client is None
    client = client or ClashRoyaleClient()
    try:
        clans = parse_clan_rankings(client.get_top_clans(location_id=location, limit=max_clans))
        logger.info("found %d clans in location=%s", len(clans), location)

        players: list[dict] = []
        seen: set[str] = set()
        for i, clan in enumerate(clans, start=1):
            clan_tag = clan["tag"]
            try:
                members = parse_clan_members(client.get_clan_members(clan_tag), clan_tag=clan_tag)
            except ClashRoyaleAPIError as exc:
                logger.warning("skipping clan %s: %s", clan_tag, exc)
                continue
            for m in members:
                if m["tag"] not in seen:
                    seen.add(m["tag"])
                    players.append(m)
            if i % 50 == 0 or i == len(clans):
                logger.info("progress: %d/%d clans, %d unique players", i, len(clans), len(players))
    finally:
        # only manage the lifecycle of local instances
        if owns_client:
            client.close()

    logger.info("discovered %d unique players from %d clans", len(players), len(clans))
    return players

def write_players(players: list[dict], out_path: Path) -> None:
    """Create parent dir if not exists, and write players to JSON file."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(players, indent=2), encoding="utf-8") # formatted json
    logger.info("wrote %d players to %s", len(players), out_path)

def main(argv: list[str] | None = None) -> int:
    """Parse arguments, set logging, discover players via top clans, and write to file."""
    parser = argparse.ArgumentParser(description="Discover player tags via top clans' members.")
    parser.add_argument(
        "--location", type=str, default="global",
        help="Location id to rank clans by (default: global; e.g. 57000000 = Europe).",
    )
    parser.add_argument(
        "--max-clans", type=int, default=200,
        help="Number of top clans to fan out over (API max is 1000).",
    )
    parser.add_argument(
        "--out", type=Path, default=None,
        help="Output JSON path (default: <CR_RAW_DIR>/players.json).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    # Get player data and write to a JSON file
    out_path = args.out or (load_settings().raw_dir / "players.json")
    try:
        players = discover_players(location=args.location, max_clans=args.max_clans)
    except ClashRoyaleAPIError as exc:
        # e.g. an invalid location id 404s on the clan-ranking fetch.
        parser.error(f"could not fetch clan rankings for location={args.location!r}: {exc}")
    if not players:
        # Fail loud if no players are discovered.
        parser.error(
            f"no players discovered (location={args.location!r}, max_clans={args.max_clans}) — "
            "check the location id and that its clan rankings are populated"
        )
    write_players(players, out_path)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
