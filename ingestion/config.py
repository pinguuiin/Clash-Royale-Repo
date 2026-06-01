"""Environment-driven configuration for the ingestion layer.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Load a local .env if present
load_dotenv()

# RoyaleAPI proxy to resolve the dynamic-IP token lock.
DEFAULT_BASE_URL = "https://proxy.royaleapi.dev/v1"

@dataclass(frozen=True)
class Settings:
    """Immutable snapshot of ingestion configuration."""

    api_token: str
    base_url: str
    # Local storage dir for ingested JSON batches before being uploaded to the Unity Catalog volume.
    raw_dir: Path
    # HTTP behaviour
    request_timeout: float
    max_retries: int
    backoff_base: float
    backoff_cap: float
    # community consensus is roughly 5–10 requests per second sustained, with bursts allowed.
    requests_per_minute: int

    @property
    def auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_token}"}

def _get_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default

def _get_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default

def load_settings() -> Settings:
    """Build a Settings class from the current environment."""

    # Raises RuntimeError if the API token is missing
    token = os.environ.get("CR_API_TOKEN")
    if not token:
        raise RuntimeError(
            "CR_API_TOKEN is not set. Copy .env.example to .env and add your "
            "Clash Royale developer token (created against the RoyaleAPI proxy "
            "IP)."
        )

    return Settings(
        api_token=token,
        base_url=os.environ.get("CR_API_BASE_URL", DEFAULT_BASE_URL).rstrip("/"),
        raw_dir=Path(os.environ.get("CR_RAW_DIR", "data/raw")),
        request_timeout=_get_float("CR_REQUEST_TIMEOUT", 30.0),
        max_retries=_get_int("CR_MAX_RETRIES", 6),
        backoff_base=_get_float("CR_BACKOFF_BASE", 1.0),
        backoff_cap=_get_float("CR_BACKOFF_CAP", 60.0),
        requests_per_minute=_get_int("CR_REQUESTS_PER_MINUTE", 120),
    )
