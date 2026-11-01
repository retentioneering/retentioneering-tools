"""Column-name validation across the public Eventstream API (issue #235)."""

import inspect

import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.eventstream.ordering import EventOrderError
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import (
    InvalidParameterError,
    PreprocessingColumnNotFoundError,
)


LENGTH = {"metric": "length"}
CLUSTER_ARGS = {"n_clusters": 2, "random_state": 42}
PATH_CALLS = {
    "add_clusters": {
        "name": "cluster",
        "features": [LENGTH],
        "method_args": CLUSTER_ARGS,
    },
    "filter_paths": {"condition": {"op": ">", "metric": "length", "value": 0}},
    "get_metrics": {"metrics": [LENGTH]},
    "add_events": {"name": "marker", "source_event": "a"},
    "add_segment": {"name": "new_segment", "func": lambda df: ["x"] * len(df)},
    "collapse_events": {"loops": True},
    "to_daily_states": {"max_dormant_days": 0},
    "sample_paths": {"n": 1, "random_state": 42},
    "split_sessions": {"timeout": "1h", "session_col": "new_session"},
    "truncate_paths": {"start_anchor": "a", "end_anchor": "b"},
    "add_start_end_events": {},
    "transition_graph_data": {},
    "step_sankey_data": {},
    "step_matrix_data": {},
    "step_sankey": {},
    "step_matrix": {},
    "transition_graph": {},
    "funnel": {"steps": ["a", "b"]},
    "funnel_data": {"steps": ["a", "b"]},
    "get_conversion_rate": {"start_anchor": "a", "end_anchor": "b"},
    "segment_overview": {"segment_col": "plan", "metrics": [LENGTH]},
    "segment_overview_data": {"segment_col": "plan", "metrics": [LENGTH]},
    "cluster_analysis": {"features": [LENGTH], "method_args": CLUSTER_ARGS},
    "cluster_analysis_data": {"features": [LENGTH], "method_args": CLUSTER_ARGS},
    "get_metric_distribution": {
        "segment_col": "plan",
        "metric": LENGTH,
        "segment_level": "paid",
    },
}


@pytest.fixture
def stream():
    df = pd.DataFrame(
        {
            "user_id": ["u1"] * 5 + ["u2"] * 7,
            "session_id": ["s1"] * 3 + ["s2"] * 2 + ["s3"] * 4 + ["s4"] * 3,
            "event": ["a", "a", "b", "a", "b", "a", "a", "a", "b", "a", "a", "b"],
            "timestamp": pd.date_range("2024-01-01", periods=12, freq="min"),
            "revenue": [10, 5, 1, 20, 2, 30, 5, 5, 3, 40, 5, 4],
            "plan": ["paid"] * 5 + ["free"] * 7,
        }
    )
    return Eventstream(
        df,
        {
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["plan"],
            "custom_cols": ["revenue"],
        },
    )


def test_all_public_path_col_methods_are_covered():
    methods = {
        name
        for name, method in inspect.getmembers(Eventstream, inspect.isfunction)
        if not name.startswith("_")
        and "path_col" in inspect.signature(method).parameters
    }
    assert methods == set(PATH_CALLS)


@pytest.mark.parametrize("method", PATH_CALLS)
@pytest.mark.parametrize("path_col", ["usr", "revenue", ""])
def test_public_methods_reject_invalid_path_col(stream, method, path_col):
    # An existing custom column is still not an allowed path grain.
    with pytest.raises(InvalidParameterError) as exc:
        getattr(stream, method)(**PATH_CALLS[method], path_col=path_col)
    message = str(exc.value)
    assert "parameter 'path_col'" in message
    assert f"'{path_col}'" in message
    assert str(stream.schema.path_cols) in message


@pytest.mark.parametrize("method", PATH_CALLS)
@pytest.mark.parametrize("path_col", [None, "user_id", "session_id"])
def test_public_methods_accept_declared_path_grains(stream, method, path_col, request):
    if method == "to_daily_states" and path_col == "session_id":
        # On master too: daily states nulls the other path IDs and resets the
        # position per session, violating the primary path's ordering contract.
        # Keep this known limitation visible without broadening a validation fix.
        request.applymarker(
            pytest.mark.xfail(
                raises=EventOrderError,
                strict=True,
                reason="Pre-existing daily-states ordering failure at a secondary path grain",
            )
        )
    result = getattr(stream, method)(**PATH_CALLS[method], path_col=path_col)
    assert result is not None
    if method == "get_metrics":
        grain = path_col or stream.schema.path_col
        assert result.index.name == grain
        assert len(result) == stream.df[grain].nunique()
    if method == "funnel_data":
        grain = path_col or stream.schema.path_col
        assert result["steps"][0]["unique_paths"] == stream.df[grain].nunique()
    if hasattr(result, "error"):
        assert not result.error
    if hasattr(result, "close"):
        result.close()


@pytest.mark.parametrize(
    "method, kwargs",
    [
        ("add_events", {"name": "m", "anchor": "a"}),
        ("add_events", {"name": "m", "sql": "SELECT * FROM eventstream"}),
        ("add_events", {"name": "m", "churn": {"inactivity_days": 1}}),
        ("add_segment", {"name": "stage", "funnel_events": ["a", "b"]}),
        (
            "add_segment",
            {"name": "size", "metric_bins": {"metric": LENGTH, "edges": [10]}},
        ),
        ("add_segment", {"name": "revenue"}),  # promotion returns early
        ("funnel_data", {}),  # empty steps return early
        ("funnel", {}),
    ],
)
def test_path_col_is_validated_in_other_modes(stream, method, kwargs):
    with pytest.raises(InvalidParameterError, match="path_col"):
        getattr(stream, method)(**kwargs, path_col="usr")


@pytest.mark.parametrize("path_col", [None, "user_id", "session_id"])
def test_schema_resolves_declared_path_col(path_col):
    schema = EventstreamSchema(path_cols=["user_id", "session_id"])
    assert schema.resolve_path_col(path_col) == (path_col or "user_id")


AGG_CALLS = {
    "collapse_events": {"loops": True},
    "to_daily_states": {"max_dormant_days": 0},
}


def test_all_public_agg_methods_are_covered():
    methods = {
        name
        for name, method in inspect.getmembers(Eventstream, inspect.isfunction)
        if not name.startswith("_") and "agg" in inspect.signature(method).parameters
    }
    assert methods == set(AGG_CALLS)


@pytest.mark.parametrize("method", AGG_CALLS)
@pytest.mark.parametrize("path_col", [None, "session_id"])
def test_unknown_agg_column_raises(stream, method, path_col):
    with pytest.raises(PreprocessingColumnNotFoundError) as exc:
        getattr(stream, method)(
            **AGG_CALLS[method], path_col=path_col, agg={"revnue": "sum"}
        )
    message = str(exc.value)
    assert method in message
    assert "agg" in message
    assert "revnue" in message
    assert str(list(stream.df.columns)) in message


@pytest.mark.parametrize(
    "method, path_col",
    [
        ("collapse_events", None),
        ("collapse_events", "session_id"),
        ("to_daily_states", None),
    ],
)
@pytest.mark.parametrize(
    "column", ["event", "event_type", "path_col", "index", "subindex"]
)
def test_existing_excluded_agg_columns_remain_accepted(
    stream, method, path_col, column
):
    kwargs = {**AGG_CALLS[method], "path_col": path_col}
    if column == "path_col":
        column = path_col or stream.schema.path_col
    expected = getattr(stream, method)(**kwargs)
    actual = getattr(stream, method)(**kwargs, agg={column: "last"})
    pd.testing.assert_frame_equal(actual.df, expected.df)


@pytest.mark.parametrize(
    "method, path_col",
    [
        ("collapse_events", "user_id"),
        ("collapse_events", "session_id"),
        ("to_daily_states", "user_id"),
    ],
)
def test_valid_agg_sums_revenue_at_selected_grain(stream, method, path_col):
    result = getattr(stream, method)(
        **AGG_CALLS[method], path_col=path_col, agg={"revenue": "sum"}
    )
    expected = stream.df.groupby(path_col, observed=True)["revenue"].sum()
    actual = result.df.groupby(path_col, observed=True)["revenue"].sum()
    pd.testing.assert_series_equal(actual, expected, check_dtype=False)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"group_col": "plan"},
        {"bounds": {"start_event": "a", "end_event": "b"}, "name": "chunk"},
    ],
)
def test_collapse_other_modes_reject_unknown_agg_column(stream, kwargs):
    with pytest.raises(PreprocessingColumnNotFoundError, match="revnue"):
        stream.collapse_events(**kwargs, agg={"revnue": "sum"})


@pytest.mark.parametrize(
    "kwargs, column",
    [
        ({"group_col": "plan"}, "plan"),
        (
            {"bounds": {"start_event": "a", "end_event": "b"}, "name": "chunk"},
            "timestamp",
        ),
    ],
)
def test_collapse_other_modes_accept_excluded_agg_column(stream, kwargs, column):
    expected = stream.collapse_events(**kwargs)
    actual = stream.collapse_events(**kwargs, agg={column: "last"})
    pd.testing.assert_frame_equal(actual.df, expected.df)
