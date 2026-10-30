"""Unrelated typed segment labels must not change numeric metric computation."""

import pandas as pd
import pytest

from retentioneering import Eventstream


@pytest.mark.parametrize(
    "metric",
    [
        {"metric": "has_event", "metric_args": {"event": "buy"}},
        {"metric": "event_count", "metric_args": {"event": "buy"}},
        {"metric": "length"},
        {"metric": "duration"},
        {"metric": "first_event_time"},
        {"metric": "active_days"},
        {"metric": "active_days", "metric_args": {"active_events": ["buy"]}},
        {
            "metric": "time_between",
            "metric_args": {"start_event": "open", "end_event": "buy"},
        },
        {"metric": "column_sum", "metric_args": {"column": "amount"}},
        {"metric": "column_mean", "metric_args": {"column": "amount"}},
        {"metric": "column_last", "metric_args": {"column": "amount"}},
        {
            "metric": "event_value_sum",
            "metric_args": {"event": "buy", "value_col": "amount"},
        },
    ],
)
def test_unrelated_typed_categories_do_not_change_metric_values(metric):
    frame = pd.DataFrame(
        {
            "user_id": ["a", "a", "b", "b"],
            "event": ["open", "buy", "open", "buy"],
            "timestamp": pd.date_range("2026-01-01", periods=4, freq="h"),
            "amount": [None, 12.0, None, 7.0],
            "arm": [1, 1, "1", "1"],
        }
    )
    stream = Eventstream(frame, {"segment_cols": ["arm"], "custom_cols": ["amount"]})
    plain = Eventstream(frame.drop(columns="arm"), {"custom_cols": ["amount"]})
    pd.testing.assert_frame_equal(
        stream.get_metrics([metric]), plain.get_metrics([metric])
    )
    assert set(stream.df["arm"].unique()) == {1, "1"}


@pytest.mark.parametrize("level", [1, "1"])
@pytest.mark.parametrize("mode", ["any", "all", "event_share"])
def test_in_segment_preserves_typed_labels_and_null_denominators(level, mode):
    frame = pd.DataFrame(
        {
            "user_id": ["a", "a", "b", "b", "c", "c", "d", "d"],
            "event": ["open"] * 8,
            "timestamp": pd.date_range("2026-01-01", periods=8, freq="h"),
            "arm": [1, None, "1", None, 1, "1", None, None],
        }
    )
    stream = Eventstream(frame, {"segment_cols": ["arm"]})
    args = {"segment_name": "arm", "segment_level": level, "mode": mode}
    if mode == "event_share":
        args["threshold"] = 0.75
    values = stream.get_metrics([{"metric": "in_segment", "metric_args": args}]).iloc[
        :, 0
    ]
    target = "a" if isinstance(level, int) else "b"
    expected = {"a": 0, "b": 0, "c": 0, "d": 0}
    if mode != "event_share":
        expected[target] = 1
        expected["c"] = int(mode == "any")
    assert values.to_dict() == expected
    assert set(stream.df["arm"].dropna().unique()) == {1, "1"}
