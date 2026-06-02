"""Thin HTTP client over the Clash Royale API (via the RoyaleAPI proxy).

Responsibilities are deliberately narrow: send an authenticated GET, retry with
exponential backoff on rate limits / transient server errors, and hand back
decoded JSON. All *interpretation* of that JSON lives in ``parsers`` so it can
be unit-tested without the network.
"""

from __future__ import annotations

import logging
import random
import time
from urllib.parse import quote

import requests

from ingestion.config import Settings, load_settings

logger = logging.getLogger(__name__)

# HTTP statuses worth retrying: rate limit + transient upstream/proxy errors.
_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})

class ClashRoyaleAPIError(RuntimeError):
    """Raised when the API returns a non-retryable error or retries are exhausted."""

    def __init__(self, status_code: int, url: str, body: str):
        self.status_code = status_code
        self.url = url
        self.body = body
        super().__init__(f"GET {url} failed with {status_code}: {body[:200]}")

def encode_tag(tag: str) -> str:
    """URL-encode a player/clan tag.

    Tags start with ``#`` (e.g. ``#2PP``) must be percent-encoded to
    ``%23`` in the path. A leading ``#`` is optional in the input.
    """
    tag = tag.strip().upper()
    if not tag.startswith("#"):
        tag = "#" + tag
    return quote(tag, safe="")

class ClashRoyaleClient:
    """Authenticated, retrying GET client for the Clash Royale API."""

    def __init__(self, settings: Settings | None = None, session: requests.Session | None = None):
        self.settings = settings or load_settings()
        self.session = session or requests.Session()
        self.session.headers.update(self.settings.auth_header)
        # expected the API to return JSON
        self.session.headers.setdefault("Accept", "application/json")
        # Spacing between requests to stay under the per-minute rate limit.
        self._min_interval = (
            60.0 / self.settings.requests_per_minute
            if self.settings.requests_per_minute > 0
            else 0.0
        )
        self._last_request_at = 0.0

    # -- context manager -------------------------------------------------
    def __enter__(self) -> ClashRoyaleClient:
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def close(self) -> None:
        self.session.close()

    # -- core ------------------------------------------------------------
    def _throttle(self) -> None:
        """Sleep just enough to honour the configured requests-per-minute."""
        if self._min_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)

    def _backoff_seconds(self, attempt: int, retry_after: str | None) -> float:
        """Exponential backoff with full jitter, honouring Retry-After if given."""
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass  # Retry-After may be an HTTP date; fall back to exponential.
        delay = self.settings.backoff_base * (2 ** attempt)
        delay = min(delay, self.settings.backoff_cap)
        return random.uniform(0, delay)  # full jitter

    def get(self, path: str, params: dict | None = None) -> dict:
        """GET ``{base_url}{path}`` and return decoded JSON. Retry on failure."""
        url = f"{self.settings.base_url}/{path.lstrip('/')}"

        for attempt in range(self.settings.max_retries + 1):
            self._throttle()
            self._last_request_at = time.monotonic()
            try:
                resp = self.session.get(
                    url, params=params, timeout=self.settings.request_timeout
                )
            except requests.RequestException as exc:
                # Network-level failure (DNS, connection reset, timeout).
                # Raise error when reaches maximum retries; otherwise log, back off, and retry
                if attempt >= self.settings.max_retries:
                    raise ClashRoyaleAPIError(0, url, str(exc)) from exc
                delay = self._backoff_seconds(attempt, None)
                logger.warning(
                    "request error on %s (attempt %d/%d): %s — retrying in %.1fs",
                    url, attempt + 1, self.settings.max_retries, exc, delay,
                )
                time.sleep(delay)
                continue

            # Success
            if resp.status_code == 200:
                return resp.json()

            # HTTP error: same retry logic
            if resp.status_code in _RETRYABLE_STATUSES and attempt < self.settings.max_retries:
                delay = self._backoff_seconds(attempt, resp.headers.get("Retry-After"))
                logger.warning(
                    "HTTP %d on %s (attempt %d/%d) — backing off %.1fs",
                    resp.status_code, url, attempt + 1, self.settings.max_retries, delay,
                )
                time.sleep(delay)
                continue

            # Non-retryable, or retries exhausted.
            raise ClashRoyaleAPIError(resp.status_code, url, resp.text)

        # Loop exits only via return/raise above; this guards against logic drift.
        raise ClashRoyaleAPIError(0, url, "get() logic error")

    # -- typed endpoint helpers -----------------------------------------
    def get_top_clans(self, location_id: str = "global", limit: int = 1000) -> dict:
        """`/locations/{location_id}/rankings/clans` — top clans in a location."""
        return self.get(f"/locations/{location_id}/rankings/clans", params={"limit": limit})

    def get_clan_members(self, clan_tag: str) -> dict:
        """`/clans/{tag}/members` — current member list of a clan."""
        return self.get(f"/clans/{encode_tag(clan_tag)}/members")

    def get_battlelog(self, tag: str) -> list:
        """`/players/{tag}/battlelog` — last ~25 battles for a player.

        Returns a list (the battlelog endpoint responds with a bare JSON array).
        """
        return self.get(f"/players/{encode_tag(tag)}/battlelog")  # type: ignore[return-value]

    def get_cards(self) -> dict:
        """`/cards` — card dimension."""
        return self.get("/cards")
