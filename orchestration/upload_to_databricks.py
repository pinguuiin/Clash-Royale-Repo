"""Entrypoint: upload ingested JSON into a Databricks Unity Catalog volume.

Mirrors every data file in the local directory (data/raw/) into a UC volume
path, preserving the relative layout. So:

    data/raw/players.json
    data/raw/cards.json
    data/raw/20260611T060000/battles.jsonl

lands as::

    /Volumes/workspace/clash/raw/players.json
    /Volumes/workspace/clash/raw/cards.json
    /Volumes/workspace/clash/raw/20260611T060000/battles.jsonl

The bronze notebook then reads the batch directories straight off the volume.

Auth is the standard Databricks SDK chain: ``DATABRICKS_HOST`` and
``DATABRICKS_TOKEN`` in the environment (set as GitHub secrets in CI).

Usage:
    python -m orchestration.upload_to_databricks
    python -m orchestration.upload_to_databricks --local-dir data/raw \
        --volume-dir /Volumes/workspace/clash/raw
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient

from ingestion.config import load_settings

logger = logging.getLogger(__name__)

DEFAULT_VOLUME_DIR = "/Volumes/workspace/clash/raw"

def iter_files(local_dir: Path) -> list[Path]:
    """Return every file under ``local_dir`` (recursively), sorted for stable logs."""
    return sorted(p for p in local_dir.rglob("*") if p.is_file())

def upload_tree(
    local_dir: Path,
    volume_dir: str,
    client: WorkspaceClient | None = None,
    overwrite: bool = True,
) -> int:
    """Mirror ``local_dir`` into ``volume_dir`` on a UC volume; return file count."""
    client = client or WorkspaceClient()
    volume_dir = volume_dir.rstrip("/")

    files = iter_files(local_dir)
    if not files:
        raise RuntimeError(
            f"no files found under {local_dir} — run the ingestion entrypoints first"
        )

    for local_path in files:
        rel = local_path.relative_to(local_dir).as_posix()
        remote_path = f"{volume_dir}/{rel}"
        with local_path.open("rb") as fh:
            client.files.upload(remote_path, fh, overwrite=overwrite)
        logger.info("uploaded %s -> %s", local_path, remote_path)

    logger.info("uploaded %d files to %s", len(files), volume_dir)
    return len(files)

def main(argv: list[str] | None = None) -> int:
    """Parse arguments, set logging, mirror the local raw dir into the UC volume."""
    parser = argparse.ArgumentParser(description="Upload ingested JSON to a UC volume.")
    parser.add_argument(
        "--local-dir", type=Path, default=None,
        help="Local directory to upload (default: <CR_RAW_DIR>).",
    )
    parser.add_argument(
        "--volume-dir", type=str, default=None,
        help=(
            "Target UC volume directory (default: $DATABRICKS_VOLUME or "
            f"{DEFAULT_VOLUME_DIR})."
        ),
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    local_dir = args.local_dir or load_settings().raw_dir
    if not local_dir.exists():
        parser.error(f"{local_dir} not found — nothing to upload")

    volume_dir = args.volume_dir or os.environ.get("DATABRICKS_VOLUME", DEFAULT_VOLUME_DIR)
    upload_tree(local_dir, volume_dir)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
