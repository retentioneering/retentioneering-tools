"""Tests for widgets/_utils.py's parse_diff helper."""

from retentioneering.widgets._utils import parse_diff


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
