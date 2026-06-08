"""Shared pytest fixtures: loaders for the raw-API-shaped JSON fixtures.

The files in ``tests/fixtures/`` mirror real ``/players/{tag}/battlelog`` entries,
including the edge cases the parsers must survive: a missing deck and a malformed
battle. They are fed straight into the pure functions in :mod:`ingestion.parsers`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures"

def _load(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))

@pytest.fixture
def battle_normal() -> dict:
    """A clean 1v1 ladder battle: both sides have a full 8-card deck."""
    return _load("battle_normal.json")

@pytest.fixture
def battle_missing_deck() -> dict:
    """A battle whose opponent has an empty ``cards`` array (deleted/empty deck)."""
    return _load("battle_missing_deck.json")

@pytest.fixture
def battle_malformed() -> dict:
    """A battle with a bad timestamp, null arena, non-int crowns, idless card,
    missing trophies, and an empty ``opponent`` list."""
    return _load("battle_malformed.json")
