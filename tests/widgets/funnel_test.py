"""Tests for FunnelWidget's diff-mode group labels."""

import json

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.widgets.funnel import FunnelWidget


def _make_stream() -> Eventstream:
    df = pd.DataFrame(
        [
            ["user_1", "step1", "2020-01-01 00:00:00", "seg_1"],
            ["user_1", "step2", "2020-01-01 00:01:00", "seg_1"],
            ["user_2", "step1", "2020-01-01 00:00:00", "seg_1"],
            ["user_3", "step1", "2020-01-01 00:00:00", "seg_2"],
            ["user_3", "step2", "2020-01-01 00:01:00", "seg_2"],
            ["user_4", "step1", "2020-01-01 00:00:00", "seg_2"],
        ],
        columns=["user_id", "event", "timestamp", "my_segment"],
    )
    return Eventstream(df, {"segment_cols": ["my_segment"]})


class TestFunnelWidgetDiff:
    def test__segment_diff_uses_segment_values_as_labels(self) -> None:
        """Regression guard: (segment_col, value1, value2) must keep using
        the segment values as group labels, as before."""
        stream = _make_stream()
        widget = FunnelWidget(
            stream, steps=["step1", "step2"], diff=("my_segment", "seg_1", "seg_2")
        )

        assert widget.error == ""
        result = json.loads(widget.result)

        assert result["group1_label"] == "seg_1"
        assert result["group2_label"] == "seg_2"
        assert result["group1_total"] == 2
        assert result["group2_total"] == 2

    def test__path_ids_diff_uses_generic_group_labels(self) -> None:
        """diff=(path_ids1, path_ids2) must activate diff mode with generic
        "Group 1"/"Group 2" labels rather than leaking raw path IDs."""
        stream = _make_stream()
        widget = FunnelWidget(
            stream,
            steps=["step1", "step2"],
            diff=(["user_1", "user_2"], ["user_3", "user_4"]),
        )

        assert widget.error == ""
        result = json.loads(widget.result)

        assert result["group1_label"] == "Group 1"
        assert result["group2_label"] == "Group 2"
        assert result["group1_total"] == 2
        assert result["group2_total"] == 2
