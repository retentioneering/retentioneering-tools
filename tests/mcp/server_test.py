"""Tests for module-level MCP server helpers."""

import asyncio

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.mcp import list_tools, tools
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


class TestListTools:
    def test__lists_every_registered_tool_with_its_arguments(self) -> None:
        rows = list_tools()
        names = [row["name"] for row in rows]
        assert names[0] == "load_data" and "export_report" in names
        assert len(names) == len(set(names))
        by_name = {row["name"]: row for row in rows}
        assert by_name["playbook"]["parameters"] == ["scenario"]
        assert by_name["reset_base_stream"]["parameters"] == []
        # required arguments come first
        assert by_name["export_report"]["parameters"][:2] == ["title", "analysis"]
        assert all(row["description"] for row in rows)
        assert all("\n" not in row["description"] for row in rows)

    def test__works_under_a_running_event_loop(self) -> None:
        async def from_a_notebook() -> list[dict]:
            return list_tools()

        assert asyncio.run(from_a_notebook()) == list_tools()


class TestPlaybook:
    def test__a_scenario_is_text_and_the_index_is_a_dict(self) -> None:
        index = tools.playbook("")
        assert isinstance(index, dict) and index["scenarios"]
        recipe = tools.playbook(index["scenarios"][0])
        assert isinstance(recipe, str) and recipe
        assert tools.playbook("no such scenario") == index
