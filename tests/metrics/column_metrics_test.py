"""Registry metrics over custom columns: column_sum, column_mean, column_last."""

import json

import numpy as np
import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidMetricConfigError
from retentioneering.metrics.metric_builder import VALID_METRICS


def _cfg(metric, column):
    return {"metric": metric, "metric_args": {"column": column}}


@pytest.fixture
def stream() -> Eventstream:
    df = pd.DataFrame(
        {
            "user_id": ["a", "a", "a", "b", "b", "c", "d", "d"],
            "event": ["view", "order", "order", "view", "view", "order", "x", "y"],
            # d's two events share a timestamp: row order decides which is last
            "timestamp": pd.to_datetime(
                [
                    "2024-01-01 10:00",
                    "2024-01-01 10:05",
                    "2024-01-02 09:00",
                    "2024-01-01 11:00",
                    "2024-01-01 11:01",
                    "2024-01-03 08:00",
                    "2024-01-04 08:00",
                    "2024-01-04 08:00",
                ]
            ),
            "revenue": [None, 10.0, 5.5, None, None, 7.0, 1.0, 2.0],
            "plan": ["free", None, "pro", None, None, "free", "a", "b"],
        }
    )
    return Eventstream(df)


def test_registered() -> None:
    assert {"column_sum", "column_mean", "column_last"} <= VALID_METRICS


def test_values_and_null_rules(stream) -> None:
    m = stream.get_metrics(
        [
            _cfg("column_sum", "revenue"),
            _cfg("column_mean", "revenue"),
            _cfg("column_last", "revenue"),
            _cfg("column_last", "plan"),
        ]
    )
    assert list(m.columns) == [
        "column_sum_revenue",
        "column_mean_revenue",
        "column_last_revenue",
        "column_last_plan",
    ]
    assert m.loc["a"].tolist() == [15.5, 7.75, 5.5, "pro"]
    # b has no value at all: the sum is 0, mean and last are absent (NaN), not 0
    assert m.loc["b", "column_sum_revenue"] == 0.0
    assert np.isnan(m.loc["b", "column_mean_revenue"])
    assert np.isnan(m.loc["b", "column_last_revenue"])
    assert pd.isna(m.loc["b", "column_last_plan"])
    assert m["column_sum_revenue"].dtype == float


def test_last_breaks_timestamp_ties_by_row_order(stream) -> None:
    m = stream.get_metrics(
        [_cfg("column_last", "plan"), _cfg("column_last", "revenue")]
    )
    assert m.loc["d", "column_last_plan"] == "b"
    assert m.loc["d", "column_last_revenue"] == 2.0


def test_column_must_be_custom(stream) -> None:
    with pytest.raises(InvalidMetricConfigError, match="Custom columns"):
        stream.get_metrics([_cfg("column_sum", "event")])
    with pytest.raises(InvalidMetricConfigError, match="custom column"):
        stream.get_metrics([_cfg("column_sum", "nope")])


def test_numeric_required_for_sum_and_mean(stream) -> None:
    for metric in ("column_sum", "column_mean"):
        with pytest.raises(InvalidMetricConfigError, match="numeric"):
            stream.get_metrics([_cfg(metric, "plan")])


def test_metric_args_shape(stream) -> None:
    with pytest.raises(InvalidMetricConfigError, match="requires 'column'"):
        stream.get_metrics([{"metric": "column_sum"}])
    with pytest.raises(InvalidMetricConfigError, match="only 'column'"):
        stream.get_metrics(
            [{"metric": "column_sum", "metric_args": {"col": "revenue"}}]
        )


def test_filter_paths_on_column_sum(stream) -> None:
    kept = stream.filter_paths(
        {
            "op": ">",
            "metric": "column_sum",
            "metric_args": {"column": "revenue"},
            "value": 6,
        }
    )
    assert sorted(kept.df["user_id"].unique()) == ["a", "c"]


def test_condition_tree_and_nan_leaf(stream) -> None:
    # A NaN mean never satisfies a comparison, so b is dropped by either branch.
    kept = stream.filter_paths(
        {
            "op": "or",
            "args": [
                {
                    "op": ">=",
                    "metric": "column_mean",
                    "metric_args": {"column": "revenue"},
                    "value": 7,
                },
                {
                    "op": "=",
                    "metric": "column_last",
                    "metric_args": {"column": "plan"},
                    "value": "b",
                },
            ],
        }
    )
    assert sorted(kept.df["user_id"].unique()) == ["a", "c", "d"]


def test_metric_bins_on_column_sum(stream) -> None:
    out = stream.add_segment(
        "spend",
        metric_bins={
            "metric": _cfg("column_sum", "revenue"),
            "edges": [1],
            "segment_levels": ["none", "paying"],
        },
    )
    assert out.get_path_segments("spend").to_dict() == {
        "a": "paying",
        "b": "none",
        "c": "paying",
        "d": "paying",
    }


def test_mean_undefined_goes_to_undefined_bin(stream) -> None:
    out = stream.add_segment(
        "avg", metric_bins={"metric": _cfg("column_mean", "revenue"), "edges": [6]}
    )
    assert out.get_path_segments("avg")["b"] == "undefined"


def test_segment_overview_and_composite(stream) -> None:
    seg = stream.add_segment(
        "tier", path_values={"a": "x", "b": "x", "c": "y", "d": "y"}
    )
    table = seg.segment_overview_data(
        "tier", metrics=[{**_cfg("column_sum", "revenue"), "agg": "mean"}]
    )
    frame = table if isinstance(table, pd.DataFrame) else table["table"]
    row = frame.loc[[i for i in frame.index if "column_sum_revenue" in str(i)][0]]
    assert row["x"] == pytest.approx(7.75)
    assert row["y"] == pytest.approx(5.0)


def test_mcp_style_config_round_trips(stream) -> None:
    configs = json.loads(json.dumps([_cfg("column_sum", "revenue")]))
    assert stream.get_metrics(configs)["column_sum_revenue"].sum() == pytest.approx(
        25.5
    )


def test_widgets_ship_the_custom_column_catalogue(stream) -> None:
    from retentioneering.widgets.segment_overview import SegmentOverviewWidget

    widget = SegmentOverviewWidget(stream)
    assert json.loads(widget.custom_cols) == {"revenue": True, "plan": False}
    assert widget._export_data()["custom_cols"] == {"revenue": True, "plan": False}
