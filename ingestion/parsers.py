"""Pure functions: raw API JSON -> flat, bronze-ready records.

Every transformation from raw JSON to row tuple is a side-effect-free function,
so pytest can verify them against fixture files.

Nothing in this module performs I/O, reads the clock, or mutates global state.
Given the same input it returns the same output. Malformed and incomplete
inputs are handled gracefully (no exceptions) so that a single bad battle never
aborts a whole ingestion run; the resulting nulls/empties are caught downstream
by the in-pipeline data-quality checks.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

# =========== Battlelog Parser =================================

def _first(participants) -> dict:
    """First participant of a team/opponent list, or ``{}`` if absent."""
    if isinstance(participants, list) and participants:
        return participants[0] if isinstance(participants[0], dict) else {}
    return {}

def _extract_card_ids(participant: dict) -> list[int]:
    """Pull the list of card ids from one team/opponent participant.

    Return ``[]`` for a missing or empty ``cards`` array, and drop any cards
    that lack an ``id``.
    """
    cards = participant.get("cards") or []
    return [c["id"] for c in cards if isinstance(c, dict) and c.get("id") is not None]

def _win_flag(player_crowns, opponent_crowns) -> bool | None:
    """True/False if both crown counts are present and comparable, else None."""
    if isinstance(player_crowns, int) and isinstance(opponent_crowns, int):
        return player_crowns > opponent_crowns
    return None

def make_battle_id(battle_time_raw: str | None, player_tag: str, opponent_tag: str) -> str:
    """Create deterministic, collision-resistant id for a battle.

    The API does not supply a battle id, so we derive a unique hash value from
    the fields that together identify a match: its timestamp and the two
    participants (order-independent). Missing player/opponent tags or battle time
    will be checked in the later stages.
    """
    parts = [battle_time_raw or "", *sorted([player_tag or "", opponent_tag or ""])]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()

def parse_battle_time(raw_time: str | None) -> str | None:
    """Normalize a Clash Royale battle timestamp to ISO-8601 UTC.

    Returns ``None`` for missing or malformed input rather than raising, so the
    battle is still ingested and the bad timestamp surfaces as a data-quality
    signal instead of crashing the parser. (Covered by the "malformed
    timestamp" code test)
    """
    if not raw_time or not isinstance(raw_time, str):
        return None
    try:
        dt = datetime.strptime(raw_time, "%Y%m%dT%H%M%S.%fZ") # e.g. "20230101T120000.000Z"
    except (ValueError, TypeError):
        return None
    return dt.replace(tzinfo=timezone.utc).isoformat()

def deck_hash(card_ids: list[int]) -> str | None:
    """Order-independent hash fingerprint of a deck, for grouping identical decks.

    Returns ``None`` when there are no cards (e.g. a battle with a missing
    deck), so callers can distinguish "no deck" from "some deck".
    """
    if not card_ids:
        return None
    canonical = ",".join(str(cid) for cid in sorted(card_ids))
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()

def parse_battle(raw: dict) -> dict | None:
    """Parse one raw battlelog entry into a flat bronze record.

    Returns ``None`` only when ``raw`` is not a dict at all — every other
    degenerate case (missing deck, missing trophies, malformed timestamp,
    deleted/empty opponent) is represented as nulls/empties in the output so the
    battle is still captured and assessed by the quality layer.

    The subject player is ``team[0]``; the opponent is ``opponent[0]``. 2v2 and
    other multi-participant modes keep only the first participant per side,
    which is sufficient for 1v1 ladder meta analysis (the project's scope).
    """
    if not isinstance(raw, dict):
        return None

    player = _first(raw.get("team"))
    opponent = _first(raw.get("opponent"))

    player_tag = player.get("tag")
    opponent_tag = opponent.get("tag")
    battle_time_raw = raw.get("battleTime")

    player_cards = _extract_card_ids(player)
    opponent_cards = _extract_card_ids(opponent)

    player_crowns = player.get("crowns")
    opponent_crowns = opponent.get("crowns")
    win = _win_flag(player_crowns, opponent_crowns)

    arena = raw.get("arena") or {}
    game_mode = raw.get("gameMode") or {}
    game_mode_name = game_mode.get("name")

    return {
        "battle_id": make_battle_id(battle_time_raw, player_tag or "", opponent_tag or ""),
        "battle_time": parse_battle_time(battle_time_raw),
        "battle_time_raw": battle_time_raw,
        "type": raw.get("type"),
        # Note this excludes Path of Legend ranked ("Ranked1v1_*")
        "is_ladder": game_mode_name == "Ladder",
        "game_mode": game_mode_name,
        "arena_id": arena.get("id"),
        "arena_name": arena.get("name"),
        # Subject player
        "player_tag": player_tag,
        "player_name": player.get("name"),
        "player_starting_trophies": player.get("startingTrophies"),
        "player_trophy_change": player.get("trophyChange"),
        "player_crowns": player_crowns,
        "player_cards": player_cards,
        "player_deck_hash": deck_hash(player_cards),
        # Opponent
        "opponent_tag": opponent_tag,
        "opponent_name": opponent.get("name"),
        "opponent_starting_trophies": opponent.get("startingTrophies"),
        "opponent_trophy_change": opponent.get("trophyChange"),
        "opponent_crowns": opponent_crowns,
        "opponent_cards": opponent_cards,
        "opponent_deck_hash": deck_hash(opponent_cards),
        # Derived
        "win": win,
    }

def parse_battlelog(raw_battles, source_tag: str | None = None) -> list[dict]:
    """Parse a full battlelog array, dropping only non-dict entries.

    ``source_tag`` (the player whose log this is) is attached to each record for
    lineage — useful when checkpointing per player and debugging coverage.
    """
    out: list[dict] = []
    for entry in raw_battles or []:
        parsed = parse_battle(entry)
        if parsed is None:
            continue
        if source_tag is not None:
            parsed["source_player_tag"] = source_tag
        out.append(parsed)
    return out

# =========== Clan / Member Parsers ===========================

def parse_clan_ranking(raw: dict) -> dict | None:
    """Parse one entry from a ``/rankings/clans`` response."""
    if not isinstance(raw, dict) or not raw.get("tag"):
        return None
    return {
        "tag": raw.get("tag"),
        "rank": raw.get("rank"),
        "members": raw.get("members"),
    }

def parse_clan_rankings(raw: dict) -> list[dict]:
    """Parse the ``{"items": [...]}`` clan-ranking payload into clean rows."""
    items = (raw or {}).get("items", [])
    return [c for c in (parse_clan_ranking(i) for i in items) if c is not None]

def parse_clan_member(raw: dict) -> dict | None:
    """Parse one entry from a ``/clans/{tag}/members`` response.

    These member rows are the player seed for the battlelog fan-out; ``tag`` is
    the only field downstream strictly needs, the rest are kept for context.
    """
    if not isinstance(raw, dict) or not raw.get("tag"):
        return None
    return {
        "tag": raw.get("tag"),
        "name": raw.get("name"),
        "trophies": raw.get("trophies"),
    }

def parse_clan_members(raw: dict, clan_tag: str | None = None) -> list[dict]:
    """Parse a clan members payload into clean rows.

    ``clan_tag`` (the clan this list came from) is attached to each row for
    lineage, since the member entries don't carry their own clan reference.
    """
    items = (raw or {}).get("items", [])
    out: list[dict] = []
    for item in items:
        member = parse_clan_member(item)
        if member is None:
            continue
        if clan_tag is not None:
            member["clan_tag"] = clan_tag
        out.append(member)
    return out

# =========== Card Parser =====================================

def parse_card(raw: dict) -> dict | None:
    """Parse one entry from the ``/cards`` dimension response.

    ``elixirCost`` and ``rarity`` are present on newer API responses but not all
    historical ones, so both are read defensively.
    """
    if not isinstance(raw, dict) or raw.get("id") is None:
        return None
    return {
        "card_id": raw.get("id"),
        "name": raw.get("name"),
        "rarity": raw.get("rarity"),
        "elixir_cost": raw.get("elixirCost"),
        "max_level": raw.get("maxLevel"),
    }

def parse_cards(raw: dict) -> list[dict]:
    """Parse the ``/cards`` payload into clean card-dimension rows."""
    items = (raw or {}).get("items", [])
    return [c for c in (parse_card(i) for i in items) if c is not None]
