from __future__ import annotations

import os
import time

import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream
from retentioneering.utils.tracker_analytics_tools import get_inner_calls, process_data

from .fixtures.common import test_stream, test_stream_small
from .fixtures.helpers import *
from .fixtures.utils import set_local_tracker


def get_log(helper_name: str, performance_name: str | None = None) -> dict:
    if performance_name is None:
        performance_name = f"{helper_name}_helper"

    time.sleep(1)

    logs = process_data(pd.read_csv(os.environ["RETE_TRACKER_CSV_PATH"]), only_calls=False).sort_values(by="index")
    main_log = logs[logs["event_full_name"] == f"{helper_name}_helper"].iloc[0]

    inner_calls = get_inner_calls(logs, main_log["index"], index_col="index", parent_col="parent_index")

    performance_info = inner_calls[(inner_calls["event_full_name"] == f"{performance_name}")].iloc[0][
        "performance_before"
    ]

    return {
        "args": main_log["args"],
        "performance_info": performance_info,
    }


class TestToolHelpersTracking:
    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_funnel_helper_full(self, test_stream: Eventstream, funnel_full_params: dict) -> None:
        test_stream.funnel(**funnel_full_params["args"])

        log = get_log("funnel")

        assert log["args"] == funnel_full_params["expected_args"]
        assert log["performance_info"] == funnel_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_cohorts_helper_full(self, test_stream: Eventstream, cohorts_full_params: dict) -> None:
        test_stream.cohorts(**cohorts_full_params["args"])

        log = get_log("cohorts", performance_name="cohorts_fit")

        assert log["args"] == cohorts_full_params["expected_args"]
        assert log["performance_info"] == cohorts_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_step_matrix_helper_full(self, test_stream: Eventstream, step_matrix_full_params: dict) -> None:
        test_stream.step_matrix(**step_matrix_full_params["args"])

        log = get_log("step_matrix", performance_name="step_matrix_fit")

        assert log["args"] == step_matrix_full_params["expected_args"]
        assert log["performance_info"] == step_matrix_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_step_sankey_helper_full(self, test_stream: Eventstream, step_sankey_full_params: dict) -> None:
        test_stream.step_sankey(**step_sankey_full_params["args"])

        log = get_log("step_sankey", performance_name="step_sankey_fit")

        assert log["args"] == step_sankey_full_params["expected_args"]
        assert log["performance_info"] == step_sankey_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_transition_matrix_helper_full(
        self, test_stream: Eventstream, transition_matrix_full_params: dict
    ) -> None:
        test_stream.transition_matrix(**transition_matrix_full_params["args"])

        log = get_log("transition_matrix")

        assert log["args"] == transition_matrix_full_params["expected_args"]
        assert log["performance_info"] == transition_matrix_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_stattests_helper_full(self, test_stream: Eventstream, stattests_full_params: dict) -> None:
        test_stream.stattests(**stattests_full_params["args"])

        log = get_log("stattests")

        assert log["args"] == stattests_full_params["expected_args"]
        assert log["performance_info"] == stattests_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_describe_full(self, test_stream: Eventstream, describe_full_params: dict) -> None:
        test_stream.describe(**describe_full_params["args"])

        log = get_log("describe")

        assert log["args"] == describe_full_params["expected_args"]
        assert log["performance_info"] == describe_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_describe_events_full(self, test_stream: Eventstream, describe_events_full_params: dict) -> None:
        test_stream.describe_events(**describe_events_full_params["args"])

        log = get_log("describe_events")

        assert log["args"] == describe_events_full_params["expected_args"]
        assert log["performance_info"] == describe_events_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_timedelta_hist_full(self, test_stream: Eventstream, timedelta_hist_full_params: dict) -> None:
        test_stream.timedelta_hist(**timedelta_hist_full_params["args"])

        log = get_log("timedelta_hist")

        assert log["args"] == timedelta_hist_full_params["expected_args"]
        assert log["performance_info"] == timedelta_hist_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_user_lifetime_hist_full(
        self, test_stream: Eventstream, user_lifetime_hist_full_params: dict
    ) -> None:
        test_stream.user_lifetime_hist(**user_lifetime_hist_full_params["args"])

        log = get_log("user_lifetime_hist")

        assert log["args"] == user_lifetime_hist_full_params["expected_args"]
        assert log["performance_info"] == user_lifetime_hist_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_event_timestamp_hist_full(
        self, test_stream: Eventstream, event_timestamp_hist_full_params: dict
    ) -> None:
        test_stream.event_timestamp_hist(**event_timestamp_hist_full_params["args"])

        log = get_log("event_timestamp_hist")

        assert log["args"] == event_timestamp_hist_full_params["expected_args"]
        assert log["performance_info"] == event_timestamp_hist_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_transition_graph_helper_full(
        self, test_stream: Eventstream, transition_graph_full_params: dict
    ) -> None:
        test_stream.transition_graph(**transition_graph_full_params["args"])

        log = get_log("transition_graph", performance_name="transition_graph_plot")

        assert log["args"] == transition_graph_full_params["expected_args"]
        assert log["performance_info"] == transition_graph_full_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_clusters_helper_full(self, test_stream: Eventstream) -> None:
        test_stream.clusters

        log = get_log("clusters")

        assert log["args"] == {}
        assert log["performance_info"] == {}

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_pgraph_helper_full(self, test_stream: Eventstream, pgraph_params: dict) -> None:
        test_stream.preprocessing_graph(**pgraph_params["args"])

        log = get_log("preprocessing_graph")

        assert log["args"] == pgraph_params["expected_args"]
        assert log["performance_info"] == pgraph_params["performance_info"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_sequences_helper_full(self, test_stream_small: Eventstream, sequences_full_params: dict) -> None:
        test_stream_small.sequences(**sequences_full_params["args"])

        log = get_log("sequences", performance_name="sequences_plot")

        assert log["args"] == sequences_full_params["expected_args"]
        assert log["performance_info"] == sequences_full_params["performance_info"]
