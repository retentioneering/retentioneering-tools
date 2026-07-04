"""Tests for module-level MCP server helpers."""

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.mcp.server import _apply_preprocessors


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

    def test__by_column_form_still_works(self) -> None:
        stream = get_stream()

        result = _apply_preprocessors(
            stream,
            [{"type": "filter_events", "column": "event", "values": ["view"]}],
        )

        assert set(result.df["event"].astype(str)) == {"view"}
