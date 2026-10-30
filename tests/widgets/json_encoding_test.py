"""Every widget's `result` is strict JSON: NaN/Infinity become null (A1.8)."""

import json

import numpy as np
import pytest

from retentioneering.datasets import load_ecom
from retentioneering.widgets._base import RetentioneeringWidget


def _strict(raw: str):
    def fail(token):
        raise AssertionError(f"non-finite token {token!r} in widget JSON")

    return json.loads(raw, parse_constant=fail)


def test_encode_result_replaces_non_finite() -> None:
    raw = {
        "a": float("nan"),
        "b": [1.0, float("inf"), np.float64("nan"), (2.0, -float("inf"))],
        "c": {"d": np.float64(0.5), "e": "NaN"},
    }
    encoded = RetentioneeringWidget._encode_result(raw)
    assert _strict(encoded) == {
        "a": None,
        "b": [1.0, None, None, [2.0, None]],
        "c": {"d": 0.5, "e": "NaN"},
    }


@pytest.fixture(scope="module")
def stream():
    return load_ecom()


def _widgets(stream):
    # time_between is undefined for paths without both events, so a segment
    # whose paths never purchase has a NaN mean: the case that used to emit NaN.
    nan_metric = {
        "metric": "time_between",
        "metric_args": {"start_event": "add_to_cart", "end_event": "purchase"},
        "agg": "mean",
    }
    df = stream.df
    buyers = set(df.loc[df["event"] == "purchase", "user_id"])
    never = [u for u in df["user_id"].unique() if u not in buyers][:5]
    seg = stream.add_segment(
        "buyer",
        path_values={
            u: ("no" if u in never else "yes") for u in df["user_id"].unique()
        },
    )
    return {
        "transition_graph": stream.transition_graph(),
        "step_matrix": stream.step_matrix(),
        "step_sankey": stream.step_sankey(),
        "funnel": stream.funnel(["catalog", "product_view", "add_to_cart", "purchase"]),
        "segment_overview": seg.segment_overview("buyer", metrics=[nan_metric]),
        "cluster_analysis": stream.cluster_analysis(
            features=[{"metric": "length"}, {"metric": "duration"}],
            method="kmeans",
            method_args={"n_clusters": 3},
        ),
    }


def test_every_widget_result_is_strict_json(stream) -> None:
    widgets = _widgets(stream)
    for name, widget in widgets.items():
        assert widget.result not in ("", "{}"), name
        _strict(widget.result)
        _strict(json.dumps(widget._export_data(), allow_nan=False))


def test_segment_overview_nan_cells_are_null(stream) -> None:
    widget = _widgets(stream)["segment_overview"]
    payload = _strict(widget.result)
    row = payload["metrics"].index("time_from_add_to_cart_to_purchase_mean")
    column = payload["segments"].index("no")
    # nobody in "no" purchased: the mean is absent, sent as null (not NaN, not 0)
    assert payload["values"][row][column] is None
    assert payload["values"][row][payload["segments"].index("yes")] > 0
