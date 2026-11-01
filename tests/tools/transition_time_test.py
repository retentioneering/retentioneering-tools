import json

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream


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


def cell(result, a="request", b="ready"):
    return result["values"][result["events"].index(a)][result["events"].index(b)]


@pytest.mark.parametrize("metric, expected", [("time_median", 125), ("time_q95", 350)])
def test_sub_second_aggregation(metric, expected):
    result = spans().transition_graph_data(edge_weight=metric)
    assert result.loc["request", "ready"] == pd.Timedelta(milliseconds=expected)


@pytest.mark.parametrize("user", [1, 2])
def test_duration_does_not_depend_on_second_boundaries(user):
    # User 1's 125ms span stays inside one second; user 2's crosses a boundary.
    one = spans().filter_events(keep={"user_id": [user]})
    result = one.transition_graph_data(edge_weight="time_median")
    assert result.loc["request", "ready"] == pd.Timedelta(milliseconds=125)


def test_pattern_and_diff():
    delta, first, second = spans().transition_graph_data(
        edge_weight="time_median", diff=([1], [3]), path_pattern="request->ready"
    )
    assert delta.loc["request", "ready"] == pd.Timedelta(milliseconds=-250)
    assert first.loc["request", "ready"] == pd.Timedelta(milliseconds=125)
    assert second.loc["request", "ready"] == pd.Timedelta(milliseconds=375)


def test_session_boundaries():
    frame = spans().df[["user_id", "event", "timestamp"]].copy()
    frame["session_id"] = frame.user_id
    frame["user_id"] = 1
    stream = Eventstream(frame, {"path_cols": ["user_id", "session_id"]})
    matrix = stream.transition_graph_data(
        edge_weight="time_median", path_col="session_id"
    )
    assert pd.isna(matrix.loc["ready", "request"])
    assert matrix.loc["request", "ready"] == pd.Timedelta(milliseconds=125)


def test_widget_json_carries_fractional_seconds():
    widget = spans().transition_graph(edge_weight="time_median")
    assert cell(widget._export_data()["result"]) == 0.125
    widget.diff = json.dumps([[1], [3]])
    result = json.loads(widget.result)
    assert cell(result) == -0.25
    assert cell(result["group1"]) == 0.125
    assert cell(result["group2"]) == 0.375
