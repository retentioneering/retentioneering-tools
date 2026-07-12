"""Tests for StepMatrixWidget's diff-mode per-group event counts."""

import json

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.widgets.step_matrix import StepMatrixWidget


def _make_stream() -> Eventstream:
    df = pd.DataFrame(
        [
            ["user_1", "A", "2020-01-01 00:00:00", "seg_1"],
            ["user_2", "B", "2020-01-02 00:00:00", "seg_1"],
            ["user_3", "C", "2020-01-01 00:00:00", "seg_2"],
            ["user_4", "D", "2020-01-01 00:00:00", "seg_3"],
            ["user_5", "E", "2020-01-02 00:00:00", "seg_3"],
        ],
        columns=["user_id", "event", "timestamp", "my_segment"],
    )
    return Eventstream(df, {"segment_cols": ["my_segment"]})


class TestStepMatrixWidgetDiff:
    def test__event_counts_g2_with_rest(self) -> None:
        """value2="<REST>" must pool every other segment value's per-event counts."""
        stream = _make_stream()
        widget = StepMatrixWidget(stream, diff=("my_segment", "seg_1", "<REST>"))

        assert widget.error == ""
        result = json.loads(widget.result)

        # group1 = seg_1 (user_1, user_2)
        assert result["event_counts_g1"] == {
            "A": 1,
            "B": 1,
            "path_start": 2,
            "path_end": 2,
        }
        # group2 = <REST> = seg_2 + seg_3 pooled (user_3, user_4, user_5)
        assert result["event_counts_g2"] == {
            "C": 1,
            "D": 1,
            "E": 1,
            "path_start": 3,
            "path_end": 3,
        }

    def test__event_counts_g2_with_literal_value(self) -> None:
        """Regression guard: non-<REST> diff values must keep working as before."""
        stream = _make_stream()
        widget = StepMatrixWidget(stream, diff=("my_segment", "seg_1", "seg_2"))

        assert widget.error == ""
        result = json.loads(widget.result)

        assert result["event_counts_g1"] == {
            "A": 1,
            "B": 1,
            "path_start": 2,
            "path_end": 2,
        }
        assert result["event_counts_g2"] == {
            "C": 1,
            "path_start": 1,
            "path_end": 1,
        }

    def test__path_ids_diff_activates_diff_mode(self) -> None:
        """diff=(path_ids1, path_ids2) must activate diff mode in the widget,
        not just in the headless step_matrix_data/step_sankey_data methods."""
        stream = _make_stream()
        widget = StepMatrixWidget(
            stream, diff=(["user_1", "user_2"], ["user_3", "user_4", "user_5"])
        )

        assert widget.error == ""
        result = json.loads(widget.result)

        assert result["event_counts_g1"] == {
            "A": 1,
            "B": 1,
            "path_start": 2,
            "path_end": 2,
        }
        assert result["event_counts_g2"] == {
            "C": 1,
            "D": 1,
            "E": 1,
            "path_start": 3,
            "path_end": 3,
        }
