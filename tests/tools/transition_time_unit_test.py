import json

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError


def spans():
    rows = []
    origin = pd.Timestamp("2026-01-01")
    for user, (phase, wait) in enumerate([(100, 125), (950, 125), (250, 375)], 1):
        start = origin + pd.Timedelta(seconds=user, milliseconds=phase)
        rows.extend(
            [
                (user, "request", start),
                (user, "ready", start + pd.Timedelta(milliseconds=wait)),
            ]
        )
    return Eventstream(pd.DataFrame(rows, columns=["user_id", "event", "timestamp"]))


@pytest.mark.parametrize("metric, expected", [("time_median", 125), ("time_q95", 350)])
def test_millisecond_aggregation(metric, expected):
    result = spans().transition_graph_data(edge_weight=metric, time_unit="ms")
    assert result.loc["request", "ready"] == pd.Timedelta(milliseconds=expected)


def test_legacy_seconds_and_cross_boundary():
    stream = spans()
    pd.testing.assert_frame_equal(
        stream.transition_graph_data(edge_weight="time_median"),
        stream.transition_graph_data(edge_weight="time_median", time_unit="s"),
    )
    # The 125ms span at .950 crosses a second; the equally long .100 span does not.
    for user, expected in [(1, 0), (2, 1)]:
        one = stream.filter_events(keep={"user_id": [user]})
        assert one.transition_graph_data(edge_weight="time_median").loc[
            "request", "ready"
        ] == pd.Timedelta(seconds=expected)
        assert one.transition_graph_data(edge_weight="time_median", time_unit="ms").loc[
            "request", "ready"
        ] == pd.Timedelta(milliseconds=125)


def test_pattern_diff_and_counts():
    stream = spans()
    delta, first, second = stream.transition_graph_data(
        edge_weight="time_median",
        diff=([1], [3]),
        path_pattern="request->ready",
        time_unit="ms",
    )
    assert delta.loc["request", "ready"] == pd.Timedelta(milliseconds=-250)
    assert first.loc["request", "ready"] == pd.Timedelta(milliseconds=125)
    assert second.loc["request", "ready"] == pd.Timedelta(milliseconds=375)
    pd.testing.assert_frame_equal(
        stream.transition_graph_data(edge_weight="count"),
        stream.transition_graph_data(edge_weight="count", time_unit="ms"),
    )


def test_session_boundaries():
    frame = spans().df[["user_id", "event", "timestamp"]].copy()
    frame["session_id"] = frame.user_id
    frame["user_id"] = 1
    stream = Eventstream(frame, {"path_cols": ["user_id", "session_id"]})
    matrix = stream.transition_graph_data(
        edge_weight="time_median", path_col="session_id", time_unit="ms"
    )
    assert pd.isna(matrix.loc["ready", "request"])
    assert matrix.loc["request", "ready"] == pd.Timedelta(milliseconds=125)


def test_invalid_unit():
    with pytest.raises(InvalidParameterError, match="time_unit"):
        spans().transition_graph_data(edge_weight="time_median", time_unit="nanosecond")


def test_widget_export_compute_and_persistence(tmp_path):
    state = str(tmp_path / "graph.json")
    widget = spans().transition_graph(
        edge_weight="time_median", time_unit="ms", state_file=state
    )
    payload = widget._export_data()
    assert payload["time_unit"] == "ms"
    result = payload["result"]
    assert (
        result["values"][result["events"].index("request")][
            result["events"].index("ready")
        ]
        == 0.125
    )
    assert json.loads(widget.result) == widget._tool_transition_graph_data(
        {"time_unit": "ms"}
    )
    restored = spans().transition_graph(state_file=state)
    assert restored.time_unit == "ms"
    assert restored._export_data()["result"]["values"] == result["values"]
    restored.time_unit = "s"
    changed = json.loads(restored.result)
    assert (
        changed["values"][changed["events"].index("request")][
            changed["events"].index("ready")
        ]
        == 0
    )


def test_mcp_report_retains_resolution_and_seconds():
    from retentioneering.mcp import tools
    from retentioneering.mcp._report_session import ReportSession

    session = ReportSession(spans(), {})
    summary = tools.add_transition_graph(
        session, label="Load durations", edge_weight="time_median", time_unit="ms"
    )
    assert summary["time_unit"] == "ms"
    data = session.pending_tabs[0]["data"]
    assert data["time_unit"] == "ms"
    result = data["result"]
    assert (
        result["values"][result["events"].index("request")][
            result["events"].index("ready")
        ]
        == 0.125
    )


def test_widget_pattern_diff_and_explicit_state_override(tmp_path):
    state = str(tmp_path / "graph.json")
    widget = spans().transition_graph(
        edge_weight="time_median",
        time_unit="ms",
        diff=([1], [3]),
        path_pattern="request->ready",
        state_file=state,
    )
    result = json.loads(widget.result)
    i, j = result["events"].index("request"), result["events"].index("ready")
    assert result["values"][i][j] == -0.250
    assert result["group1"]["values"][i][j] == 0.125
    assert spans().transition_graph(state_file=state, time_unit="s").time_unit == "s"


def test_interactive_settings_retain_resolution_and_export(tmp_path):
    frame = spans().df[["user_id", "event", "timestamp"]].copy()
    frame["session_id"] = frame.user_id
    frame["cohort"] = frame.user_id.map({1: "fast", 2: "fast", 3: "slow"})
    stream = Eventstream(
        frame,
        {"path_cols": ["user_id", "session_id"], "segment_cols": ["cohort"]},
    )
    state = str(tmp_path / "graph.json")
    widget = stream.transition_graph(
        edge_weight="time_median", time_unit="ms", state_file=state
    )
    # These trait changes mirror the interactive sidebar's host.set calls.
    widget.edge_weight = "count"
    widget.path_col = "session_id"
    widget.edge_weight = "time_q95"
    widget.diff = json.dumps(["cohort", "fast", "slow"])
    result = json.loads(widget.result)
    i, j = result["events"].index("request"), result["events"].index("ready")
    assert result["values"][i][j] == -0.250
    assert result["group1"]["values"][i][j] == 0.125
    assert result["group2"]["values"][i][j] == 0.375
    restored = stream.transition_graph(state_file=state)
    assert restored.time_unit == "ms"
    assert restored.edge_weight == "time_q95"
    assert restored.path_col == "session_id"
    assert restored._export_data()["result"] == result
    export = tmp_path / "graph.html"
    restored.export_html(str(export))
    assert export.exists()
    widget.diff = ""
    widget.edge_weight = "time_median"
    assert widget.time_unit == "ms"
    assert widget._export_data()["result"]["values"][i][j] == 0.125
