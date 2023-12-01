from __future__ import annotations

import os
import time

from retentioneering.eventstream import Eventstream
from retentioneering.preprocessing_graph import PreprocessingGraph
from retentioneering.tooling import (
    Clusters,
    Cohorts,
    Funnel,
    Sequences,
    StatTests,
    StepMatrix,
    StepSankey,
    TransitionGraph,
)
from retentioneering.utils.tracker_analytics_tools import process_data

from .fixtures.common import (
    groups,
    groups_small_session_id,
    test_stream,
    test_stream_small,
)
from .fixtures.tools import *
from .fixtures.utils import set_local_tracker


def get_log(event_full_name: str) -> dict:
    time.sleep(1)

    logs = process_data(pd.read_csv(os.environ["RETE_TRACKER_CSV_PATH"]), only_calls=False).sort_values(by="index")
    main_log = logs[logs["event_full_name"] == f"{event_full_name}"].iloc[0]

    return {
        "args": main_log["args"],
        "performance_info": main_log["performance_before"],
    }


class TestToolsTracking:
    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_funnel_fit_full(self, test_stream: Eventstream, funnel_fit_full_params: dict) -> None:
        funnel = Funnel(test_stream)
        funnel.fit(**funnel_fit_full_params["args"])

        log = get_log("funnel_fit")

        assert log["args"] == funnel_fit_full_params["expected_args"]
        assert log["performance_info"] == funnel_fit_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_funnel_plot(
        self, test_stream: Eventstream, funnel_fit_full_params: dict, funnel_plot_params: dict
    ) -> None:
        funnel = Funnel(test_stream)
        funnel.fit(**funnel_fit_full_params["args"])
        funnel.plot()

        log = get_log("funnel_plot")

        assert log["args"] == funnel_plot_params["expected_args"]
        assert log["performance_info"] == funnel_plot_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_cohorts_fit_full(self, test_stream: Eventstream, cohorts_fit_full_params: dict) -> None:
        cohorts = Cohorts(test_stream)
        cohorts.fit(**cohorts_fit_full_params["args"])

        log = get_log("cohorts_fit")

        assert log["args"] == cohorts_fit_full_params["expected_args"]
        assert log["performance_info"] == cohorts_fit_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_cohorts_lineplot_full(
        self, test_stream: Eventstream, cohorts_fit_full_params: dict, cohorts_lineplot_full_params: dict
    ) -> None:
        cohorts = Cohorts(test_stream)
        cohorts.fit(**cohorts_fit_full_params["args"])
        cohorts.lineplot(**cohorts_lineplot_full_params["args"])

        log = get_log("cohorts_lineplot")

        assert log["args"] == cohorts_lineplot_full_params["expected_args"]
        assert log["performance_info"] == cohorts_lineplot_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_cohorts_heatmap_full(
        self, test_stream: Eventstream, cohorts_fit_full_params: dict, cohorts_heatmap_full_params: dict
    ) -> None:
        cohorts = Cohorts(test_stream)
        cohorts.fit(**cohorts_fit_full_params["args"])
        cohorts.heatmap(**cohorts_heatmap_full_params["args"])

        log = get_log("cohorts_heatmap")

        assert log["args"] == cohorts_heatmap_full_params["expected_args"]
        assert log["performance_info"] == cohorts_heatmap_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_step_matrix_fit_full(self, test_stream: Eventstream, step_matrix_fit_full_params: dict) -> None:
        step_matrix = StepMatrix(test_stream)
        step_matrix.fit(**step_matrix_fit_full_params["args"])

        log = get_log("step_matrix_fit")

        assert log["args"] == step_matrix_fit_full_params["expected_args"]
        assert log["performance_info"] == step_matrix_fit_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_step_matrix_plot(
        self, test_stream: Eventstream, step_matrix_fit_full_params: dict, step_matrix_plot_params: dict
    ) -> None:
        step_matrix = StepMatrix(test_stream)
        step_matrix.fit(**step_matrix_fit_full_params["args"])
        step_matrix.plot()

        log = get_log("step_matrix_plot")

        assert log["args"] == step_matrix_plot_params["expected_args"]
        assert log["performance_info"] == step_matrix_plot_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_step_sankey_fit_full(self, test_stream: Eventstream, step_sankey_fit_full_params: dict) -> None:
        step_sankey = StepSankey(test_stream)
        step_sankey.fit(**step_sankey_fit_full_params["args"])

        log = get_log("step_sankey_fit")

        assert log["args"] == step_sankey_fit_full_params["expected_args"]
        assert log["performance_info"] == step_sankey_fit_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_step_sankey_plot(
        self, test_stream: Eventstream, step_sankey_fit_full_params: dict, step_sankey_plot_full_params: dict
    ) -> None:
        step_sankey = StepSankey(test_stream)
        step_sankey.fit(**step_sankey_fit_full_params["args"])
        step_sankey.plot(**step_sankey_plot_full_params["args"])

        log = get_log("step_sankey_plot")

        assert log["args"] == step_sankey_plot_full_params["expected_args"]
        assert log["performance_info"] == step_sankey_plot_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_stattests_fit_full(self, test_stream: Eventstream, stattests_fit_full_params: dict) -> None:
        stattests = StatTests(test_stream)
        stattests.fit(**stattests_fit_full_params["args"])

        log = get_log("stattests_fit")

        assert log["args"] == stattests_fit_full_params["expected_args"]
        assert log["performance_info"] == stattests_fit_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_stattests_plot_full(
        self, test_stream: Eventstream, stattests_fit_full_params: dict, stattests_plot_params: dict
    ) -> None:
        stattests = StatTests(test_stream)
        stattests.fit(**stattests_fit_full_params["args"])
        stattests.plot(**stattests_plot_params["args"])

        log = get_log("stattests_plot")

        assert log["args"] == stattests_plot_params["expected_args"]
        assert log["performance_info"] == stattests_plot_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_transition_graph_helper_full(
        self, test_stream: Eventstream, transition_graph_plot_full_params: dict
    ) -> None:
        transition_graph = TransitionGraph(test_stream)
        transition_graph.plot(**transition_graph_plot_full_params["args"])

        log = get_log("transition_graph_plot")

        assert log["args"] == transition_graph_plot_full_params["expected_args"]
        assert log["performance_info"] == transition_graph_plot_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_clusters_features(self, test_stream_small: Eventstream, clusters_features_params: dict) -> None:
        clusters = Clusters(test_stream_small)
        clusters.extract_features(**clusters_features_params["args"])

        log = get_log("clusters_extract_features")

        assert log["args"] == clusters_features_params["expected_args"]
        assert log["performance_info"] == clusters_features_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_clusters_fit(
        self, test_stream_small: Eventstream, clusters_features_params: dict, clusters_fit_params: dict
    ) -> None:
        clusters = Clusters(test_stream_small)
        clusters.extract_features(**clusters_features_params["args"])
        clusters.fit(**clusters_fit_params["args"])

        log = get_log("clusters_fit")

        assert log["args"] == clusters_fit_params["expected_args"]
        assert log["performance_info"] == clusters_fit_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_clusters_fit_plot(
        self,
        test_stream_small: Eventstream,
        clusters_features_params: dict,
        clusters_fit_params: dict,
        clusters_plot_params: dict,
    ) -> None:
        clusters = Clusters(test_stream_small)
        clusters.extract_features(**clusters_features_params["args"])
        clusters.fit(**clusters_fit_params["args"])
        clusters.plot(**clusters_plot_params["args"])

        log = get_log("clusters_plot")

        assert log["args"] == clusters_plot_params["expected_args"]
        assert log["performance_info"] == clusters_plot_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_clusters_fit_projection(
        self,
        test_stream_small: Eventstream,
        clusters_features_params: dict,
        clusters_fit_params: dict,
        clusters_projection_params: dict,
    ) -> None:
        clusters = Clusters(test_stream_small)
        clusters.extract_features(**clusters_features_params["args"])
        clusters.fit(**clusters_fit_params["args"])
        clusters.projection(**clusters_projection_params["args"])

        log = get_log("clusters_projection")

        assert log["args"] == clusters_projection_params["expected_args"]
        assert log["performance_info"] == clusters_projection_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_clusters_fit_diff(
        self,
        test_stream_small: Eventstream,
        clusters_features_params: dict,
        clusters_fit_params: dict,
        clusters_diff_params: dict,
    ) -> None:
        clusters = Clusters(test_stream_small)
        clusters.extract_features(**clusters_features_params["args"])
        clusters.fit(**clusters_fit_params["args"])
        clusters.diff(**clusters_diff_params["args"])

        log = get_log("clusters_diff")

        assert log["args"] == clusters_diff_params["expected_args"]
        assert log["performance_info"] == clusters_diff_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_clusters_fit_filter(
        self,
        test_stream_small: Eventstream,
        clusters_features_params: dict,
        clusters_fit_params: dict,
        clusters_filter_params: dict,
    ) -> None:
        clusters = Clusters(test_stream_small)
        clusters.extract_features(**clusters_features_params["args"])
        clusters.fit(**clusters_fit_params["args"])
        clusters.filter_cluster(**clusters_filter_params["args"])

        log = get_log("clusters_filter_cluster")

        assert log["args"] == clusters_filter_params["expected_args"]
        assert log["performance_info"] == clusters_filter_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_pgraph_init(self, test_stream_small: Eventstream) -> None:
        PreprocessingGraph(test_stream_small)

        log = get_log("preprocessing_graph_init")

        assert log["args"] == {}
        assert log["performance_info"] == {}

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_sequences_fit(self, test_stream_small: Eventstream, sequences_fit_params: dict) -> None:
        sequences = Sequences(test_stream_small)
        sequences.fit(**sequences_fit_params["args"])

        log = get_log("sequences_fit")

        assert log["args"] == sequences_fit_params["expected_args"]
        assert log["performance_info"] == sequences_fit_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_sequences_plot(
        self, test_stream_small: Eventstream, sequences_fit_params: dict, sequences_plot_params: dict
    ) -> None:
        sequences = Sequences(test_stream_small)

        sequences.fit(**sequences_fit_params["args"])
        sequences.plot(**sequences_plot_params["args"])
        log = get_log("sequences_plot")

        assert log["args"] == sequences_plot_params["expected_args"]
        assert log["performance_info"] == sequences_plot_params["performance_info"]
