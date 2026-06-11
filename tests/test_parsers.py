"""Unit tests for the battlelog parsers in :mod:`ingestion.parsers`.

These verify *that the code is correct* — the first of the project's two
reliability layers (the second being the in-pipeline data-quality checks). The
parsers are pure, so every case here runs deterministically with no I/O.
"""

from __future__ import annotations

from ingestion.parsers import (
    deck_hash,
    make_battle_id,
    parse_battle,
    parse_battle_time,
    parse_battlelog,
)

# ========== parse_battle_time ================================================

def test_parse_battle_time_normal():
    assert parse_battle_time("20230101T120000.000Z") == "2023-01-01T12:00:00+00:00"

def test_parse_battle_time_is_utc():
    assert parse_battle_time("20230101T120000.000Z").endswith("+00:00")

def test_parse_battle_time_malformed_returns_none():
    assert parse_battle_time("not-a-timestamp") is None

def test_parse_battle_time_none_returns_none():
    assert parse_battle_time(None) is None

def test_parse_battle_time_non_string_returns_none():
    assert parse_battle_time(20230101) is None

# ========== make_battle_id ===================================================

def test_make_battle_id_is_deterministic():
    a = make_battle_id("20230101T120000.000Z", "#AAA", "#BBB")
    b = make_battle_id("20230101T120000.000Z", "#AAA", "#BBB")
    assert a == b

def test_make_battle_id_is_order_independent_in_tags():
    """Either player seeing the battle in their log must yield the same id."""
    a = make_battle_id("20230101T120000.000Z", "#AAA", "#BBB")
    b = make_battle_id("20230101T120000.000Z", "#BBB", "#AAA")
    assert a == b

def test_make_battle_id_differs_on_time():
    a = make_battle_id("20230101T120000.000Z", "#AAA", "#BBB")
    b = make_battle_id("20230101T120001.000Z", "#AAA", "#BBB")
    assert a != b

def test_make_battle_id_handles_missing_tags():
    # Missing tags must not raise; uniqueness is enforced downstream.
    assert isinstance(make_battle_id(None, "", ""), str)

# ========== deck_hash ========================================================

def test_deck_hash_is_order_independent():
    assert deck_hash([3, 1, 2]) == deck_hash([1, 2, 3])

def test_deck_hash_distinguishes_decks():
    assert deck_hash([1, 2, 3]) != deck_hash([1, 2, 4])

def test_deck_hash_empty_is_none():
    assert deck_hash([]) is None

# ========== parse_battle — normal case =======================================

def test_parse_battle_normal_core_fields(battle_normal):
    rec = parse_battle(battle_normal)
    assert rec["player_tag"] == "#PPVL99U0R"
    assert rec["opponent_tag"] == "#PVLCGV9R"
    assert rec["battle_time"] == "2026-06-08T02:10:39+00:00"
    assert rec["arena_id"] == 54000031
    assert rec["game_mode"] == "Ladder"

def test_parse_battle_normal_is_ladder(battle_normal):
    assert parse_battle(battle_normal)["is_ladder"] is True

def test_parse_battle_normal_decks(battle_normal):
    rec = parse_battle(battle_normal)
    assert len(rec["player_cards"]) == 8
    assert len(rec["opponent_cards"]) == 8
    assert rec["player_deck_hash"] is not None
    assert rec["opponent_deck_hash"] is not None

def test_parse_battle_win_flag(battle_normal):
    # team crowns 2 > opponent crowns 0 -> win.
    assert parse_battle(battle_normal)["win"] is True

# ========== parse_battle — missing deck edge case ============================

def test_parse_battle_missing_deck_does_not_raise(battle_missing_deck):
    assert parse_battle(battle_missing_deck) is not None

def test_parse_battle_missing_deck_is_empty_not_null(battle_missing_deck):
    rec = parse_battle(battle_missing_deck)
    assert rec["opponent_cards"] == []
    assert rec["opponent_deck_hash"] is None  # distinguishes "no deck" from "some deck"

def test_parse_battle_missing_deck_keeps_player_side(battle_missing_deck):
    # A missing opponent deck must not corrupt the subject player's deck.
    assert len(parse_battle(battle_missing_deck)["player_cards"]) == 8

def test_parse_battle_missing_deck_tie_win_flag(battle_missing_deck):
    # 1 crown each -> not a win (and not None, since both crowns are present).
    assert parse_battle(battle_missing_deck)["win"] is False

# ========== parse_battle — malformed edge case ============================

def test_parse_battle_malformed_does_not_raise(battle_malformed):
    assert parse_battle(battle_malformed) is not None

def test_parse_battle_malformed_timestamp_is_null(battle_malformed):
    rec = parse_battle(battle_malformed)
    assert rec["battle_time"] is None
    assert rec["battle_time_raw"] == "not-a-timestamp"  # raw kept for inspection

def test_parse_battle_malformed_drops_idless_card(battle_malformed):
    # The card with no "id" is dropped; the two valid ones remain.
    assert parse_battle(battle_malformed)["player_cards"] == [26000000, 26000021]

def test_parse_battle_malformed_non_int_crowns_win_is_none(battle_malformed):
    # crowns == "two" is not comparable -> win is None, not a crash.
    assert parse_battle(battle_malformed)["win"] is None

def test_parse_battle_malformed_empty_opponent(battle_malformed):
    rec = parse_battle(battle_malformed)
    assert rec["opponent_tag"] is None
    assert rec["opponent_cards"] == []

def test_parse_battle_malformed_null_arena(battle_malformed):
    assert parse_battle(battle_malformed)["arena_id"] is None

# ========== parse_battle — non-dict input ====================================

def test_parse_battle_non_dict_returns_none():
    assert parse_battle(None) is None
    assert parse_battle("garbage") is None
    assert parse_battle([1, 2, 3]) is None

# ========== parse_battlelog ==================================================

def test_parse_battlelog_parses_all(battle_normal, battle_missing_deck):
    out = parse_battlelog([battle_normal, battle_missing_deck])
    assert len(out) == 2

def test_parse_battlelog_drops_non_dict_entries(battle_normal):
    out = parse_battlelog([battle_normal, None, "junk", 42])
    assert len(out) == 1

def test_parse_battlelog_attaches_source_tag(battle_normal):
    out = parse_battlelog([battle_normal], source_tag="#SEED")
    assert out[0]["source_player_tag"] == "#SEED"

def test_parse_battlelog_no_source_tag_omits_field(battle_normal):
    out = parse_battlelog([battle_normal])
    assert "source_player_tag" not in out[0]

def test_parse_battlelog_handles_none_input():
    assert parse_battlelog(None) == []
