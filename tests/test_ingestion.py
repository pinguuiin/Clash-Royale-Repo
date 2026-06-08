"""Unit tests for the clan, member, and card dimension parsers.

These feed the seed crawl (clan rankings -> members -> player tags) and the card
dimension. Like the battle parsers they are pure and tolerate malformed input by
dropping bad rows rather than raising.
"""

from __future__ import annotations

from ingestion.parsers import (
    parse_card,
    parse_cards,
    parse_clan_member,
    parse_clan_members,
    parse_clan_ranking,
    parse_clan_rankings,
)

# ========== clan rankings ================================================

def test_parse_clan_ranking_normal():
    row = parse_clan_ranking({"tag": "#CLAN", "rank": 1, "members": 50})
    assert row == {"tag": "#CLAN", "rank": 1, "members": 50}

def test_parse_clan_ranking_requires_tag():
    assert parse_clan_ranking({"rank": 1}) is None

def test_parse_clan_ranking_non_dict():
    assert parse_clan_ranking("nope") is None

def test_parse_clan_rankings_filters_bad_rows():
    payload = {"items": [{"tag": "#A", "rank": 1}, {"rank": 2}, {"tag": "#B", "rank": 3}]}
    out = parse_clan_rankings(payload)
    assert [r["tag"] for r in out] == ["#A", "#B"]

def test_parse_clan_rankings_empty_payload():
    assert parse_clan_rankings({}) == []
    assert parse_clan_rankings(None) == []

# ========== clan members (the player seed) ===============================

def test_parse_clan_member_normal():
    row = parse_clan_member({"tag": "#P", "name": "x", "trophies": 6000})
    assert row == {"tag": "#P", "name": "x", "trophies": 6000}

def test_parse_clan_member_requires_tag():
    assert parse_clan_member({"name": "x"}) is None

def test_parse_clan_members_attaches_clan_tag():
    payload = {"items": [{"tag": "#P1"}, {"tag": "#P2"}]}
    out = parse_clan_members(payload, clan_tag="#CLAN")
    assert all(m["clan_tag"] == "#CLAN" for m in out)

def test_parse_clan_members_omits_clan_tag_when_absent():
    out = parse_clan_members({"items": [{"tag": "#P1"}]})
    assert "clan_tag" not in out[0]

def test_parse_clan_members_filters_bad_rows():
    out = parse_clan_members({"items": [{"tag": "#P1"}, {"name": "no tag"}]})
    assert len(out) == 1

# ========== cards dimension ==============================================

def test_parse_card_normal():
    row = parse_card(
        {"id": 26000000, "name": "Knight", "rarity": "common", "elixirCost": 3, "maxLevel": 16}
    )
    assert row == {
        "card_id": 26000000,
        "name": "Knight",
        "rarity": "common",
        "elixir_cost": 3,
        "max_level": 16,
    }

def test_parse_card_requires_id():
    assert parse_card({"name": "Knight"}) is None

def test_parse_card_id_zero_is_kept():
    # id 0 is falsy but valid; the parser must check "is None", not truthiness.
    assert parse_card({"id": 0, "name": "Zero"}) is not None

def test_parse_card_defensive_optional_fields():
    # Older responses lack rarity / elixirCost; both read as None, no raise.
    row = parse_card({"id": 26000001, "name": "Archers"})
    assert row["rarity"] is None
    assert row["elixir_cost"] is None

def test_parse_cards_filters_bad_rows():
    payload = {"items": [{"id": 1, "name": "A"}, {"name": "no id"}, {"id": 2, "name": "B"}]}
    out = parse_cards(payload)
    assert [c["card_id"] for c in out] == [1, 2]

def test_parse_cards_empty_payload():
    assert parse_cards({}) == []
    assert parse_cards(None) == []
