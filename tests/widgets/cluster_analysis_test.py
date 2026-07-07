"""Tests for ClusterAnalysisWidget's 'save clusters to eventstream' action."""

import json

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.widgets.cluster_analysis import ClusterAnalysisWidget


def _make_stream() -> Eventstream:
    df = pd.DataFrame(
        [
            ["user_1", "login", "2020-01-01 00:00:00"],
            ["user_1", "view", "2020-01-01 00:01:00"],
            ["user_2", "login", "2020-01-01 00:00:00"],
            ["user_2", "view", "2020-01-01 00:01:00"],
            ["user_2", "view", "2020-01-01 00:02:00"],
            ["user_2", "view", "2020-01-01 00:03:00"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    return Eventstream(df)


class TestClusterAnalysisWidgetSave:
    def test__chosen_params_reports_fixed_n_clusters(self) -> None:
        stream = _make_stream()
        widget = ClusterAnalysisWidget(
            stream, features=[{"metric": "length"}], n_clusters=2
        )

        assert json.loads(widget.chosen_params) == {"n_clusters": 2}

    def test__chosen_params_reports_grid_search_winner(self) -> None:
        # Needs enough distinct paths for KMeans(n_clusters=3) to be valid.
        df = pd.DataFrame(
            [
                ["user_1", "login", "2020-01-01 00:00:00"],
                ["user_2", "login", "2020-01-01 00:00:00"],
                ["user_2", "view", "2020-01-01 00:01:00"],
                ["user_3", "login", "2020-01-01 00:00:00"],
                ["user_3", "view", "2020-01-01 00:01:00"],
                ["user_3", "view", "2020-01-01 00:02:00"],
                ["user_4", "login", "2020-01-01 00:00:00"],
                ["user_4", "view", "2020-01-01 00:01:00"],
                ["user_4", "view", "2020-01-01 00:02:00"],
                ["user_4", "view", "2020-01-01 00:03:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)
        widget = ClusterAnalysisWidget(
            stream, features=[{"metric": "length"}], n_clusters=[2, 3]
        )
        assert widget.error == ""

        params = json.loads(widget.chosen_params)
        assert params["n_clusters"] in (2, 3)

    def test__save_mode_code_does_not_mutate_eventstream(self) -> None:
        stream = _make_stream()
        widget = ClusterAnalysisWidget(
            stream, features=[{"metric": "length"}], n_clusters=2
        )

        widget.save_segment_name = "cluster"
        widget.save_mode = "code"
        widget.save_trigger = "1"

        result = json.loads(widget.save_result)
        assert result["ok"] is True
        assert result["mode"] == "code"
        assert "stream = stream.add_clusters(" in result["code"]
        assert "n_clusters=2" in result["code"]

        # The eventstream itself must be untouched.
        assert "cluster" not in stream.schema.segment_cols
        assert "cluster" not in stream.df.columns

    def test__save_mode_code_includes_rename_chain(self) -> None:
        stream = _make_stream()
        widget = ClusterAnalysisWidget(
            stream, features=[{"metric": "length"}], n_clusters=2
        )

        widget.save_segment_name = "cluster"
        widget.save_rename = json.dumps({"cluster_0": "short"})
        widget.save_mode = "code"
        widget.save_trigger = "1"

        result = json.loads(widget.save_result)
        assert result["ok"] is True
        assert ".rename_segment_values(" in result["code"]
        assert "cluster_0" in result["code"]

    def test__save_mode_inplace_mutates_shared_eventstream(self) -> None:
        stream = _make_stream()
        widget = ClusterAnalysisWidget(
            stream, features=[{"metric": "length"}], n_clusters=2
        )

        widget.save_segment_name = "cluster"
        widget.save_rename = json.dumps({"cluster_0": "short", "cluster_1": "long"})
        widget.save_mode = "inplace"
        widget.save_trigger = "1"

        result = json.loads(widget.save_result)
        assert result["ok"] is True
        assert result["mode"] == "inplace"
        assert result["segment_name"] == "cluster"

        # `stream` is the exact same object passed to the widget - it must reflect
        # the new segment column without any reassignment.
        assert "cluster" in stream.schema.segment_cols
        assert set(stream.df["cluster"].unique().tolist()) <= {"short", "long"}

        # cached_property schema/fingerprint must not be stale.
        assert stream.schema.segment_cols == ["cluster"]

        # The widget's own catalogs must be refreshed too.
        assert "cluster" in json.loads(widget.segment_cols)

    def test__save_mode_inplace_without_rename(self) -> None:
        stream = _make_stream()
        widget = ClusterAnalysisWidget(
            stream, features=[{"metric": "length"}], n_clusters=2
        )

        widget.save_segment_name = "cluster"
        widget.save_mode = "inplace"
        widget.save_trigger = "1"

        result = json.loads(widget.save_result)
        assert result["ok"] is True
        assert any(
            c.startswith("cluster_") for c in stream.df["cluster"].unique().tolist()
        )

    def test__save_without_segment_name_reports_error(self) -> None:
        stream = _make_stream()
        widget = ClusterAnalysisWidget(
            stream, features=[{"metric": "length"}], n_clusters=2
        )

        widget.save_mode = "code"
        widget.save_trigger = "1"

        result = json.loads(widget.save_result)
        assert result["ok"] is False
        assert "error" in result

    def test__save_with_colliding_segment_name_reports_error(self) -> None:
        stream = _make_stream()
        widget = ClusterAnalysisWidget(
            stream, features=[{"metric": "length"}], n_clusters=2
        )
        widget.save_segment_name = "user_id"
        widget.save_mode = "inplace"
        widget.save_trigger = "1"

        result = json.loads(widget.save_result)
        assert result["ok"] is False


class TestClusterAnalysisWidgetDefaults:
    def test__default_features_and_metrics_use_the_all_events_wildcard(self) -> None:
        """Defaults must not enumerate every event - it's unreadable in generated
        code and unnecessary now that event_count/has_event support the wildcard."""
        stream = _make_stream()
        widget = ClusterAnalysisWidget(stream)

        assert json.loads(widget.features) == [{"metric": "event_count"}]
        assert json.loads(widget.overview_metrics) == [
            {"metric": "event_count", "agg": "mean"}
        ]
