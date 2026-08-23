"""Tests for ClusterAnalysisWidget's 'save clusters to eventstream' action."""

import json

import pandas as pd
import pytest

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
            stream, features=[{"metric": "length"}], method_args={"n_clusters": 2}
        )

        assert json.loads(widget.chosen_params) == {
            "method": "kmeans",
            "method_args": {"n_clusters": 2},
            "scaler": "minmax",
        }

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
            stream, features=[{"metric": "length"}], method_args={"n_clusters": [2, 3]}
        )
        assert widget.error == ""

        params = json.loads(widget.chosen_params)
        assert params["method_args"]["n_clusters"] in (2, 3)

    def test__hdbscan_args_reach_the_computation(self) -> None:
        """The widget has no sidebar fields for hdbscan, so `method_args` is the
        only way to configure it — it used to have no way at all."""
        stream = _make_stream()
        widget = ClusterAnalysisWidget(
            stream,
            features=[{"metric": "length"}],
            method="hdbscan",
            method_args={"min_cluster_size": 2},
        )

        assert widget.error == ""
        params = json.loads(widget.chosen_params)
        assert params["method"] == "hdbscan"
        assert params["method_args"]["min_cluster_size"] == 2

    def test__save_mutates_shared_eventstream(self) -> None:
        stream = _make_stream()
        widget = ClusterAnalysisWidget(
            stream, features=[{"metric": "length"}], method_args={"n_clusters": 2}
        )

        widget.save_segment_name = "cluster"
        widget.save_rename = json.dumps({"cluster_0": "short", "cluster_1": "long"})
        widget.save_trigger = "1"

        result = json.loads(widget.save_result)
        assert result["ok"] is True
        assert result["segment_name"] == "cluster"

        # `stream` is the exact same object passed to the widget - it must reflect
        # the new segment column without any reassignment.
        assert "cluster" in stream.schema.segment_cols
        assert set(stream.df["cluster"].unique().tolist()) <= {"short", "long"}

        # cached_property schema/fingerprint must not be stale.
        assert stream.schema.segment_cols == ["cluster"]

        # The widget's own catalogs must be refreshed too.
        assert "cluster" in json.loads(widget.segment_cols)

    def test__save_without_rename(self) -> None:
        stream = _make_stream()
        widget = ClusterAnalysisWidget(
            stream, features=[{"metric": "length"}], method_args={"n_clusters": 2}
        )

        widget.save_segment_name = "cluster"
        widget.save_trigger = "1"

        result = json.loads(widget.save_result)
        assert result["ok"] is True
        assert any(
            c.startswith("cluster_") for c in stream.df["cluster"].unique().tolist()
        )

    def test__save_without_segment_name_reports_error(self) -> None:
        stream = _make_stream()
        widget = ClusterAnalysisWidget(
            stream, features=[{"metric": "length"}], method_args={"n_clusters": 2}
        )

        widget.save_trigger = "1"

        result = json.loads(widget.save_result)
        assert result["ok"] is False
        assert "error" in result

    def test__save_with_colliding_segment_name_reports_error(self) -> None:
        stream = _make_stream()
        widget = ClusterAnalysisWidget(
            stream, features=[{"metric": "length"}], method_args={"n_clusters": 2}
        )
        widget.save_segment_name = "user_id"
        widget.save_trigger = "1"

        result = json.loads(widget.save_result)
        assert result["ok"] is False


class TestClusterAnalysisWidgetDefaults:
    def test__default_features_and_metrics_use_the_all_events_wildcard(self) -> None:
        """Defaults must not enumerate every event - it's unreadable in generated
        code and unnecessary now that event_count_bulk/has_event_bulk support the
        wildcard (event_count/has_event are strict single-event, no wildcard)."""
        stream = _make_stream()
        widget = ClusterAnalysisWidget(stream)

        assert json.loads(widget.features) == [{"metric": "event_count_bulk"}]
        assert json.loads(widget.overview_metrics) == [
            {"metric": "event_count_bulk", "agg": "mean"}
        ]


class TestClusterAnalysisStreamVarName:
    def test__infers_the_variable_name_used_at_the_call_site(self) -> None:
        es = _make_stream()
        widget = es.cluster_analysis(
            features=[{"metric": "length"}], method_args={"n_clusters": 2}
        )

        assert widget.stream_var_name == "es"

    def test__falls_back_to_stream_when_not_bound_to_a_variable(self) -> None:
        widget = _make_stream().cluster_analysis(
            features=[{"metric": "length"}], method_args={"n_clusters": 2}
        )

        assert widget.stream_var_name == "stream"

    def test__direct_widget_construction_defaults_to_stream(self) -> None:
        """ClusterAnalysisWidget(...) called directly (not via Eventstream.cluster_analysis)
        has no caller frame to inspect, so it must fall back to the default."""
        stream = _make_stream()
        widget = ClusterAnalysisWidget(
            stream, features=[{"metric": "length"}], method_args={"n_clusters": 2}
        )

        assert widget.stream_var_name == "stream"


def _grid_stream() -> Eventstream:
    """12 users with varying path lengths — enough distinct paths for a grid."""
    rows = []
    for i in range(12):
        uid = f"user_{i}"
        rows.append([uid, "login", "2020-01-01 00:00:00"])
        for j in range(i % 4):
            rows.append([uid, "view", f"2020-01-01 00:0{j + 1}:00"])
        if i % 3 == 0:
            rows.append([uid, "purchase", "2020-01-01 00:09:00"])
    return Eventstream(pd.DataFrame(rows, columns=["user_id", "event", "timestamp"]))


FEATURES = [
    {"metric": "length"},
    {"metric": "event_count", "metric_args": {"event": "view"}},
]


def _widget(**kwargs):
    return ClusterAnalysisWidget(
        _grid_stream(), features=FEATURES, method_args={"n_clusters": "2-4"}, **kwargs
    )


class TestClusterAnalysisWidgetGridSelection:
    def test__defaults_to_the_silhouette_winner(self) -> None:
        widget = _widget()
        sil = json.loads(widget.result)["silhouette"]
        assert widget.selected_params == ""
        assert sil["selected_index"] is None
        assert (
            json.loads(widget.chosen_params)["method_args"]
            == sil["params"][sil["best_index"]]
        )

    def test__selecting_a_point_rebuilds_the_overview_for_it(self) -> None:
        widget = _widget()
        widget.selected_params = json.dumps({"n_clusters": 4})

        result = json.loads(widget.result)
        assert len(result["overview"]["segments"]) == 4
        assert result["silhouette"]["selected_index"] == 2
        assert widget.error == ""

    def test__chosen_params_follows_the_selection(self) -> None:
        """The copy-pasteable code and the saved segment must describe the
        partition on screen, not the one that happened to win on score."""
        widget = _widget()
        widget.selected_params = json.dumps({"n_clusters": 4})
        assert json.loads(widget.chosen_params)["method_args"] == {"n_clusters": 4}

    def test__saving_materialises_the_selected_partition(self) -> None:
        widget = _widget()
        widget.selected_params = json.dumps({"n_clusters": 4})
        widget.save_segment_name = "picked"
        widget.save_trigger = "go"

        assert json.loads(widget.save_result).get("ok") is True
        assert len(widget._cluster_labels.unique()) == 4

    def test__the_winner_stays_reported_alongside_the_selection(self) -> None:
        widget = _widget()
        before = json.loads(widget.result)["silhouette"]["best_index"]
        widget.selected_params = json.dumps({"n_clusters": 4})
        after = json.loads(widget.result)["silhouette"]

        assert after["best_index"] == before
        assert after["selected_index"] != after["best_index"] or before == 2

    def test__stale_selection_falls_back_instead_of_erroring(self) -> None:
        """A selection restored from a state file can name a point of an older
        grid. That is a preference to drop, not a failure to show the user."""
        widget = _widget()
        widget.selected_params = json.dumps({"n_clusters": 99})

        assert widget.error == ""
        assert widget.selected_params == ""
        assert json.loads(widget.result)["overview"]["segments"]

    def test__apply_clears_a_selection_from_the_previous_grid(self) -> None:
        widget = _widget()
        widget.selected_params = json.dumps({"n_clusters": 4})
        widget.n_clusters = "2-3"
        widget.apply_trigger = "go"

        assert widget.selected_params == ""
        sil = json.loads(widget.result)["silhouette"]
        assert [p["n_clusters"] for p in sil["params"]] == [2, 3]

    def test__selection_is_persisted_in_widget_state(self) -> None:
        widget = _widget()
        widget.selected_params = json.dumps({"n_clusters": 4})
        assert "selected_params" in widget._persist_names


class TestClusterAnalysisWidgetNmfComponents:
    """`nmf_components` is a pipeline step every method accepts, so — like
    `scaler` — it belongs in the widget's signature and not only in the sidebar
    toggle. The widget used to expose it in the UI alone, which left
    `cluster_analysis()` unable to open on an NMF run its headless twin
    (`cluster_analysis_data`/`add_clusters`) could run."""

    def test__parameter_turns_the_sidebar_toggle_on(self) -> None:
        widget = _widget(nmf_components=2)

        assert widget.nmf_enabled is True
        assert widget.nmf_components == "2"
        assert widget.error == ""

    def test__omitting_it_leaves_nmf_off(self) -> None:
        widget = _widget()

        assert widget.nmf_enabled is False
        assert widget.nmf_components == ""
        assert "nmf" not in json.loads(widget.result)

    def test__decomposition_reaches_the_result_and_chosen_params(self) -> None:
        widget = _widget(nmf_components=2)

        nmf = json.loads(widget.result)["nmf"]
        assert len(nmf["H_matrix"]) == 2
        assert nmf["W_cluster_means"]
        assert json.loads(widget.chosen_params)["nmf_components"] == 2

    def test__a_list_runs_a_grid_over_the_components(self) -> None:
        widget = ClusterAnalysisWidget(
            _grid_stream(),
            features=FEATURES,
            method_args={"n_clusters": 3},
            nmf_components=[1, 2],
        )

        assert widget.nmf_components == "[1, 2]"
        sil = json.loads(widget.result)["silhouette"]
        assert [p["nmf_components"] for p in sil["params"]] == [1, 2]

    def test__select_can_name_a_grid_point_by_its_components(self) -> None:
        widget = ClusterAnalysisWidget(
            _grid_stream(),
            features=FEATURES,
            method_args={"n_clusters": 3},
            nmf_components=[1, 2],
            select={"n_clusters": 3, "nmf_components": 1},
        )

        assert widget.error == ""
        assert json.loads(widget.chosen_params)["nmf_components"] == 1

    def test__eventstream_method_passes_it_through(self) -> None:
        stream = _grid_stream()
        widget = stream.cluster_analysis(
            features=FEATURES, method_args={"n_clusters": 3}, nmf_components=2
        )

        assert widget.nmf_enabled is True
        assert json.loads(widget.chosen_params)["nmf_components"] == 2

    def test__an_unparsable_value_raises_instead_of_silently_skipping_nmf(
        self,
    ) -> None:
        with pytest.raises(ValueError, match="nmf_components"):
            _widget(nmf_components="two")

    def test__explicit_argument_wins_over_saved_state(self, tmp_path) -> None:
        path = tmp_path / "clusters.json"
        first = _widget(nmf_components=2, state_file=str(path))
        assert first.nmf_components == "2"

        restored = _widget(state_file=str(path))
        assert restored.nmf_enabled is True
        assert restored.nmf_components == "2"

        overridden = _widget(nmf_components=1, state_file=str(path))
        assert overridden.nmf_components == "1"
