"""Tests for module-level MCP server helpers."""

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.mcp.server import _apply_preprocessors, _find_unlinked_numbers


def get_stream() -> Eventstream:
    df = pd.DataFrame(
        [
            ["user_1", "view", "2020-01-01 00:00:00"],
            ["user_1", "noise", "2020-01-02 00:00:00"],
            ["user_1", "purchase", "2020-01-03 00:00:00"],
            ["user_2", "view", "2020-01-01 00:00:00"],
            ["user_2", "noise", "2020-01-02 00:00:00"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    return Eventstream(df)


class TestApplyPreprocessorsFilterEvents:
    def test__sql_form(self) -> None:
        stream = get_stream()

        result = _apply_preprocessors(
            stream,
            [
                {
                    "type": "filter_events",
                    "sql": "SELECT * FROM eventstream WHERE event != 'noise'",
                }
            ],
        )

        events = set(result.df["event"].astype(str))
        assert "noise" not in events
        assert {"view", "purchase"} <= events

    def test__keep_form(self) -> None:
        stream = get_stream()

        result = _apply_preprocessors(
            stream,
            [{"type": "filter_events", "keep": {"event": ["view"]}}],
        )

        assert set(result.df["event"].astype(str)) == {"view"}

    def test__drop_form(self) -> None:
        stream = get_stream()

        result = _apply_preprocessors(
            stream,
            [{"type": "filter_events", "drop": {"event": ["noise"]}}],
        )

        events = set(result.df["event"].astype(str))
        assert "noise" not in events


class TestFindUnlinkedNumbers:
    def test__multiplier_digits_before_sign_is_flagged(self) -> None:
        issues = _find_unlinked_numbers("Conversion grew 2.2× during the spike")

        assert any("2.2" in issue["number"] for issue in issues)

    def test__multiplier_sign_before_digits_is_flagged(self) -> None:
        issues = _find_unlinked_numbers("Conversion grew ×2.2 during the spike")

        assert any("2.2" in issue["number"] for issue in issues)

    def test__multiplier_with_nearby_link_is_ok(self) -> None:
        issues = _find_unlinked_numbers(
            "Conversion grew 2.2× during the spike [tab-0:transition graph]"
        )

        assert issues == []

    def test__backticked_multiplier_is_exempt(self) -> None:
        issues = _find_unlinked_numbers("Conversion grew `2.2×` during the spike")

        assert issues == []

    def test__edge_linked_only_on_source_endpoint_is_flagged(self) -> None:
        issues = _find_unlinked_numbers(
            "**[Mobile vs Desktop:shipping_details]** → purchase: 6.5% mobile"
        )

        assert any("Edge in plain text" in issue.get("hint", "") for issue in issues)

    def test__edge_fully_wrapped_in_one_link_is_ok(self) -> None:
        issues = _find_unlinked_numbers(
            "**[Overall Flow:review_order->purchase]** reaches 38% conversion"
        )

        assert issues == []
