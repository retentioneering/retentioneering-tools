"""Tests for widgets/_utils.py helpers."""

from retentioneering.widgets._utils import parse_diff, pattern_edges


class TestParseDiff:
    def test__three_element_segment_diff(self) -> None:
        assert parse_diff('["my_segment", "seg_1", "seg_2"]') == [
            "my_segment",
            "seg_1",
            "seg_2",
        ]

    def test__two_element_path_ids_diff(self) -> None:
        assert parse_diff('[["user_1"], ["user_2", "user_3"]]') == [
            ["user_1"],
            ["user_2", "user_3"],
        ]

    def test__accepts_list_input_not_just_json_string(self) -> None:
        assert parse_diff([["user_1"], ["user_2"]]) == [["user_1"], ["user_2"]]

    def test__empty_or_falsy_returns_none(self) -> None:
        assert parse_diff("") is None
        assert parse_diff(None) is None

    def test__wrong_length_returns_none(self) -> None:
        assert parse_diff('["only_one"]') is None
        assert parse_diff('["a", "b", "c", "d"]') is None

    def test__malformed_json_returns_none(self) -> None:
        assert parse_diff("not json") is None


class TestPatternEdges:
    """Whether the rendered strip reaches the path's own boundaries, which is
    what decides the serrated edges. Derived from the parsed pattern — the JS
    used to sniff the string, which misread a pattern that merely mentions a
    sentinel."""

    def test__no_pattern_starts_at_the_beginning_and_is_cut_at_the_end(self) -> None:
        assert pattern_edges(None) == (True, False)
        assert pattern_edges("") == (True, False)

    def test__start_anchored_pattern(self) -> None:
        assert pattern_edges("path_start->cart") == (True, False)

    def test__end_anchored_pattern(self) -> None:
        assert pattern_edges("cart->.*->path_end") == (False, True)

    def test__both_boundaries(self) -> None:
        assert pattern_edges("path_start->[^purchase]*->path_end") == (True, True)

    def test__mid_path_pattern_reaches_neither(self) -> None:
        assert pattern_edges("cart->.*->purchase") == (False, False)

    def test__a_sentinel_merely_mentioned_is_not_a_boundary(self) -> None:
        """The case a substring test gets wrong: the pattern contains
        "path_end", but ends on `purchase`."""
        assert pattern_edges("cart->[^path_end]*->purchase") == (False, False)

    def test__a_sentinel_inside_a_class_is_not_the_part_boundary(self) -> None:
        assert pattern_edges("[path_start|home]->cart") == (False, False)
