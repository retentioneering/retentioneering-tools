"""Tests for the widgets' ``state_file`` persistence protocol."""

import json

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.widgets.funnel import FunnelWidget
from retentioneering.widgets.step_matrix import StepMatrixWidget
from retentioneering.widgets.transition_graph import TransitionGraphWidget


def _make_stream() -> Eventstream:
    df = pd.DataFrame(
        [
            ["user_1", "A", "2020-01-01 00:00:00", "seg_1"],
            ["user_1", "B", "2020-01-01 00:01:00", "seg_1"],
            ["user_2", "A", "2020-01-02 00:00:00", "seg_2"],
            ["user_2", "C", "2020-01-02 00:01:00", "seg_2"],
        ],
        columns=["user_id", "event", "timestamp", "my_segment"],
    )
    return Eventstream(df, {"segment_cols": ["my_segment"]})


class TestStateFileCreate:
    def test__creates_file_with_current_state(self, tmp_path) -> None:
        path = tmp_path / "matrix.json"
        widget = StepMatrixWidget(_make_stream(), max_steps=5, state_file=str(path))

        assert widget.error == ""
        assert path.exists()
        payload = json.loads(path.read_text())
        assert payload["widget_type"] == "step_matrix"
        assert payload["state"]["max_steps"] == 5
        assert set(payload["state"]) == set(StepMatrixWidget._persist_names)

    def test__no_state_file_means_no_persistence(self, tmp_path) -> None:
        widget = StepMatrixWidget(_make_stream())
        widget.max_steps = 7  # must not raise trying to save
        assert list(tmp_path.iterdir()) == []


class TestStateFileAutosave:
    def test__param_change_is_saved(self, tmp_path) -> None:
        path = tmp_path / "matrix.json"
        widget = StepMatrixWidget(_make_stream(), state_file=str(path))

        widget.max_steps = 4
        widget.sidebar_open = False

        state = json.loads(path.read_text())["state"]
        assert state["max_steps"] == 4
        assert state["sidebar_open"] is False

    def test__node_positions_are_saved(self, tmp_path) -> None:
        path = tmp_path / "graph.json"
        widget = TransitionGraphWidget(_make_stream(), state_file=str(path))

        positions = json.dumps({"A": {"x": 1, "y": 2}})
        widget.node_positions = positions

        state = json.loads(path.read_text())["state"]
        assert state["node_positions"] == positions


class TestStateFileRestore:
    def test__state_is_restored_from_file(self, tmp_path) -> None:
        path = tmp_path / "matrix.json"
        stream = _make_stream()

        first = StepMatrixWidget(stream, state_file=str(path))
        first.max_steps = 4
        first.diff = json.dumps(["my_segment", "seg_1", "seg_2"])

        second = StepMatrixWidget(stream, state_file=str(path))
        assert second.max_steps == 4
        assert second.diff == json.dumps(["my_segment", "seg_1", "seg_2"])
        assert second.error == ""
        assert second.result != "{}"

    def test__explicit_args_override_saved_state(self, tmp_path) -> None:
        path = tmp_path / "matrix.json"
        stream = _make_stream()

        first = StepMatrixWidget(stream, state_file=str(path))
        first.max_steps = 4
        first.sidebar_open = False

        second = StepMatrixWidget(stream, max_steps=8, state_file=str(path))
        assert second.max_steps == 8  # explicit arg wins
        assert second.sidebar_open is False  # non-overridden state survives
        # ... and the override is saved back to the file
        assert json.loads(path.read_text())["state"]["max_steps"] == 8

    def test__funnel_steps_are_restored(self, tmp_path) -> None:
        path = tmp_path / "funnel.json"
        stream = _make_stream()

        first = FunnelWidget(stream, steps=["A", "B"], state_file=str(path))
        assert first.error == ""

        second = FunnelWidget(stream, state_file=str(path))
        assert json.loads(second.steps) == ["A", "B"]
        assert second.error == ""

    def test__transition_graph_layout_is_restored(self, tmp_path) -> None:
        path = tmp_path / "graph.json"
        stream = _make_stream()
        positions = json.dumps({"A": {"x": 10, "y": 20}})

        first = TransitionGraphWidget(stream, state_file=str(path))
        first.node_positions = positions

        second = TransitionGraphWidget(stream, state_file=str(path))
        assert second.node_positions == positions

    def test__step_matrix_view_state_is_restored(self, tmp_path) -> None:
        path = tmp_path / "matrix.json"
        stream = _make_stream()
        visibility = json.dumps({"A": {"isHidden": True, "isPinned": False}})
        sort_state = json.dumps({"order": ["B", "A"], "lex_dir": "desc"})

        first = StepMatrixWidget(stream, state_file=str(path))
        first.event_visibility = visibility
        first.event_count_filter = "[1, 3]"
        first.matrix_value_filter = "0.05"
        first.sort_state = sort_state
        first.scroll_x = 120.5
        first.step_window = 7

        second = StepMatrixWidget(stream, state_file=str(path))
        assert second.event_visibility == visibility
        assert second.event_count_filter == "[1, 3]"
        assert second.matrix_value_filter == "0.05"
        assert second.sort_state == sort_state
        assert second.scroll_x == 120.5
        assert second.step_window == 7

    def test__step_sankey_filter_and_scroll_are_restored(self, tmp_path) -> None:
        from retentioneering.widgets.step_sankey import StepSankeyWidget

        path = tmp_path / "sankey.json"
        stream = _make_stream()

        first = StepSankeyWidget(stream, state_file=str(path))
        first.event_count_filter = "[2, 10]"
        first.scroll_x = 300.0

        second = StepSankeyWidget(stream, state_file=str(path))
        assert second.event_count_filter == "[2, 10]"
        assert second.scroll_x == 300.0

    def test__cluster_analysis_renames_and_tab_are_restored(self, tmp_path) -> None:
        from retentioneering.widgets.cluster_analysis import ClusterAnalysisWidget

        path = tmp_path / "clusters.json"
        stream = _make_stream()
        renames = json.dumps({"cluster_0": "power users"})

        first = ClusterAnalysisWidget(stream, state_file=str(path))
        first.cluster_renames = renames
        first.active_tab = "Silhouette"

        second = ClusterAnalysisWidget(stream, state_file=str(path))
        assert second.cluster_renames == renames
        assert second.active_tab == "Silhouette"

    def test__transition_graph_filters_and_viewport_are_restored(
        self, tmp_path
    ) -> None:
        path = tmp_path / "graph.json"
        stream = _make_stream()
        viewport = json.dumps({"zoom": 1.5, "pan": {"x": -40, "y": 12}})

        first = TransitionGraphWidget(stream, state_file=str(path))
        first.edge_filter = "[0.1, 0.8]"
        first.event_count_filter = "[2, 50]"
        first.viewport = viewport

        second = TransitionGraphWidget(stream, state_file=str(path))
        assert second.edge_filter == "[0.1, 0.8]"
        assert second.event_count_filter == "[2, 50]"
        assert second.viewport == viewport


class TestStateFileValidation:
    def test__widget_type_mismatch_raises(self, tmp_path) -> None:
        path = tmp_path / "state.json"
        stream = _make_stream()
        StepMatrixWidget(stream, state_file=str(path))

        with pytest.raises(ValueError, match="step_matrix"):
            FunnelWidget(stream, state_file=str(path))

    def test__invalid_json_raises(self, tmp_path) -> None:
        path = tmp_path / "state.json"
        path.write_text("not json at all")

        with pytest.raises(ValueError, match="not a valid widget state file"):
            StepMatrixWidget(_make_stream(), state_file=str(path))

    def test__unknown_state_keys_are_ignored(self, tmp_path) -> None:
        path = tmp_path / "state.json"
        path.write_text(
            json.dumps(
                {
                    "widget_type": "step_matrix",
                    "state": {"max_steps": 6, "bogus_key": "whatever"},
                }
            )
        )

        widget = StepMatrixWidget(_make_stream(), state_file=str(path))
        assert widget.max_steps == 6
