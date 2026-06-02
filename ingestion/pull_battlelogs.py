"""Entrypoint: pull per-player battlelogs into a checkpointed JSON batch.

Design choices:
  - Checkpointing — each player's battlelog appends to a JSON file as it's
    pulled; crashes resume from the last completed player, not from zero.
  - Backoff — handled inside the HTTP client (exponential delay on 429).

Output layout (one batch per run, under <CR_RAW_DIR>):
    data/raw/<batch_id>/
        battles.jsonl      # newline-delimited JSON, one parsed battle per line
        _checkpoint.json   # {"batch_id", "completed_tags": [...]}

The bronze notebook reads the batch directory directly off the Unity Catalog
volume. Idempotency across batches (the same battle seen in two players' logs)
is left to the bronze layer.

Usage:
    python -m ingestion.pull_battlelogs --players-file data/raw/players.json
    python -m ingestion.pull_battlelogs --resume                 # newest batch
    python -m ingestion.pull_battlelogs --batch-id 20260530T0900 # specific batch
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from ingestion.client import ClashRoyaleAPIError, ClashRoyaleClient
from ingestion.config import load_settings
from ingestion.parsers import parse_battlelog

logger = logging.getLogger(__name__)

CHECKPOINT_FILE = "_checkpoint.json"
BATTLES_FILE = "battles.jsonl"

class Checkpoint:
    """Tracks which player tags are done and appends parsed battles to disk.

    The checkpoint is rewritten after every completed player, so an interrupted
    run resumes from the last fully-written player. Battles are flushed to the
    JSONL file before the tag is marked complete, so a tag is never recorded as
    done with its battles missing.
    """

    def __init__(self, batch_dir: Path):
        self.batch_dir = batch_dir
        self.checkpoint_path = batch_dir / CHECKPOINT_FILE
        self.battles_path = batch_dir / BATTLES_FILE
        self.completed_tags: set[str] = set()
        self.battle_count = 0
        self._load()

    def _load(self) -> None:
        """Load checkpoint from disk if it exists, otherwise create a new batch dir."""
        if self.checkpoint_path.exists():
            state = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
            self.completed_tags = set(state.get("completed_tags", []))
            self.battle_count = state.get("battle_count", 0)
            logger.info(
                "resuming batch %s: %d players already complete, %d battles on disk",
                self.batch_dir.name, len(self.completed_tags), self.battle_count,
            )
        else:
            self.batch_dir.mkdir(parents=True, exist_ok=True)

    def is_done(self, tag: str) -> bool:
        return tag in self.completed_tags

    def record(self, tag: str, battles: list[dict]) -> None:
        """Append a player's parsed battles, then mark the player complete."""
        if battles:
            with self.battles_path.open("a", encoding="utf-8") as fh:
                for battle in battles:
                    fh.write(json.dumps(battle, ensure_ascii=False) + "\n")
            self.battle_count += len(battles)
        self.completed_tags.add(tag)
        self._save()

    def _save(self) -> None:
        state = {
            "batch_id": self.batch_dir.name,
            "completed_tags": sorted(self.completed_tags),
            "battle_count": self.battle_count,
        }
        # Write-then-rename for an atomic checkpoint update.
        tmp = self.checkpoint_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        tmp.replace(self.checkpoint_path)

def _utc_to_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def load_player_tags(players_file: Path) -> list[str]:
    """Read tags from the discover_players output (or a plain list of tags)."""
    data = json.loads(players_file.read_text(encoding="utf-8"))
    tags: list[str] = []
    for item in data:
        if isinstance(item, str):
            tags.append(item)
        elif isinstance(item, dict) and item.get("tag"):
            tags.append(item["tag"])
    # De-duplicate while preserving rank order.
    seen: set[str] = set()
    ordered = [t for t in tags if not (t in seen or seen.add(t))] # add() returns None, = False
    return ordered

def _resolve_batch_dir(raw_dir: Path, batch_id: str | None, resume: bool) -> Path:
    """Pick the batch directory: explicit id, newest existing, or a new one."""
    if batch_id:
        return raw_dir / batch_id
    if resume:
        candidates = sorted(
            (p for p in raw_dir.glob("*/") if (p / CHECKPOINT_FILE).exists()),
            key=lambda p: p.stat().st_mtime,
        )
        if candidates:
            return candidates[-1]
        logger.warning("--resume given but no existing batch found; starting a new one")
    return raw_dir / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

def pull_battlelogs(
    tags: list[str],
    batch_dir: Path,
    client: ClashRoyaleClient | None = None,
) -> Checkpoint:
    """Fetch and persist battlelogs for ``tags`` into ``batch_dir``.

    Resumes automatically from any existing checkpoint in ``batch_dir``. A
    per-player API failure is logged and skipped (the tag is not marked done, so
    a later run retries it); the run as a whole is never aborted by one bad tag.
    """
    owns_client = client is None
    client = client or ClashRoyaleClient()
    ckpt = Checkpoint(batch_dir)

    pending = [t for t in tags if not ckpt.is_done(t)]
    logger.info(
        "pulling %d players (%d already done) into %s",
        len(pending), len(tags) - len(pending), batch_dir,
    )

    try:
        for i, tag in enumerate(pending, start=1):
            try:
                raw = client.get_battlelog(tag)
            except ClashRoyaleAPIError as exc:
                # 404 = deleted/renamed player; others = transient after retries.
                logger.warning("skipping %s: %s", tag, exc)
                continue

            battles = parse_battlelog(raw, source_tag=tag)
            fetched_at = _utc_to_iso_now()
            for b in battles:
                b["ingested_at"] = fetched_at  # lineage; side effect kept out of parsers
            ckpt.record(tag, battles)

            if i % 50 == 0 or i == len(pending):
                logger.info(
                    "progress: %d/%d players, %d battles total",
                    i, len(pending), ckpt.battle_count,
                )
    except KeyboardInterrupt:
        logger.warning("interrupted — checkpoint saved at %d players", len(ckpt.completed_tags))
        raise
    finally:
        if owns_client:
            client.close()

    return ckpt

def main(argv: list[str] | None = None) -> int:
    """Parse arguments, set logging, load player tags, pull battlelogs to resolved dir."""
    parser = argparse.ArgumentParser(description="Pull per-player battlelogs (checkpointed).")
    parser.add_argument(
        "--players-file", type=Path, default=None,
        help="JSON file of players from discover_players (default: <CR_RAW_DIR>/players.json).",
    )
    parser.add_argument(
        "--max-players", type=int, default=None,
        help="Cap the number of players pulled this run (default: all).",
    )
    parser.add_argument(
        "--batch-id", type=str, default=None,
        help="Batch directory name; reuse the same id to resume that batch.",
    )
    # acts as a bool and doesn't require a value
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume the newest existing batch instead of starting a new one.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    settings = load_settings()
    players_file = args.players_file or (settings.raw_dir / "players.json")
    if not players_file.exists():
        parser.error(
            f"{players_file} not found — run `python -m ingestion.discover_players` first."
        )

    tags = load_player_tags(players_file)
    if args.max_players is not None:
        tags = tags[: args.max_players]

    batch_dir = _resolve_batch_dir(settings.raw_dir, args.batch_id, args.resume)
    ckpt = pull_battlelogs(tags, batch_dir)
    logger.info(
        "done: %d players, %d battles in %s",
        len(ckpt.completed_tags), ckpt.battle_count, ckpt.battles_path,
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
