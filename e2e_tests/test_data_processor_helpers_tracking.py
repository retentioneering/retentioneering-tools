from __future__ import annotations

import os
import time

import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream
from retentioneering.utils.tracker_analytics_tools import get_inner_calls, process_data

from .fixtures.common import test_stream
from .fixtures.helpers import *
from .fixtures.utils import set_local_tracker


def get_log(helper_name: str) -> dict:
    ignored_performance = ["eventstream_hist", "hash"]
    performance_name = "eventstream_init"

    time.sleep(1)

    logs = process_data(pd.read_csv(os.environ["RETE_TRACKER_CSV_PATH"]), only_calls=False).sort_values(by="index")
    main_log = logs[logs["event_full_name"] == f"{helper_name}_helper"].iloc[0]

    inner_calls = get_inner_calls(logs, main_log["index"], index_col="index", parent_col="parent_index")

    performance_logs = inner_calls[(inner_calls["event_full_name"] == f"{performance_name}")]

    performance_before: dict = performance_logs.iloc[0]["performance_before"]
    performance_after: dict = performance_logs.iloc[-1]["performance_after"]
    for item in ignored_performance:
        performance_before.pop(item, {})
        performance_after.pop(item, {})

    return {
        "args": main_log["args"],
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


class TestDataProcessorHelpersTracking:
    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_add_start_end_events_helper_full(
        self, test_stream: Eventstream, add_start_end_events_full_params: dict
    ) -> None:
        test_stream.add_start_end_events()
        log = get_log("add_start_end_events")

        assert log["args"] == add_start_end_events_full_params["expected_args"]
        assert log["performance_before"] == add_start_end_events_full_params["performance_before"]
        assert log["performance_after"] == add_start_end_events_full_params["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_add_negative_events_helper_full(
        self, test_stream: Eventstream, add_negative_events_full_params: dict
    ) -> None:
        test_stream.add_negative_events(**add_negative_events_full_params["args"])

        log = get_log("add_negative_events")

        assert log["args"] == add_negative_events_full_params["expected_args"]
        assert log["performance_before"] == add_negative_events_full_params["performance_before"]
        assert log["performance_after"] == add_negative_events_full_params["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_add_positive_events_helper_full(
        self, test_stream: Eventstream, add_positive_events_full_params: dict
    ) -> None:
        test_stream.add_positive_events(**add_positive_events_full_params["args"])

        log = get_log("add_positive_events")

        assert log["args"] == add_positive_events_full_params["expected_args"]
        assert log["performance_before"] == add_positive_events_full_params["performance_before"]
        assert log["performance_after"] == add_positive_events_full_params["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_split_sessions_helper_by_timeout(
        self, test_stream: Eventstream, split_sessions_by_timeout: dict
    ) -> None:
        test_stream.split_sessions(**split_sessions_by_timeout["args"])

        log = get_log("split_sessions")

        assert log["args"] == split_sessions_by_timeout["expected_args"]
        assert log["performance_before"] == split_sessions_by_timeout["performance_before"]
        assert log["performance_after"] == split_sessions_by_timeout["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_split_sessions_helper_by_events(
        self, test_stream: Eventstream, split_sessions_by_events: dict
    ) -> None:
        test_stream.split_sessions(**split_sessions_by_events["args"])

        log = get_log("split_sessions")

        assert log["args"] == split_sessions_by_events["expected_args"]
        assert log["performance_before"] == split_sessions_by_events["performance_before"]
        assert log["performance_after"] == split_sessions_by_events["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_split_sessions_helper_by_col(self, test_stream: Eventstream, split_sessions_by_col: dict) -> None:
        test_stream.split_sessions(**split_sessions_by_col["args"])

        log = get_log("split_sessions")

        assert log["args"] == split_sessions_by_col["expected_args"]
        assert log["performance_before"] == split_sessions_by_col["performance_before"]
        assert log["performance_after"] == split_sessions_by_col["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_label_new_users_helper_full(
        self, test_stream: Eventstream, label_new_users_full_params: dict
    ) -> None:
        test_stream.label_new_users(**label_new_users_full_params["args"])

        log = get_log("label_new_users")

        assert log["args"] == label_new_users_full_params["expected_args"]
        assert log["performance_before"] == label_new_users_full_params["performance_before"]
        assert log["performance_after"] == label_new_users_full_params["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_label_lost_users_helper_with_list(
        self, test_stream: Eventstream, label_lost_users_with_list: dict
    ) -> None:
        test_stream.label_lost_users(**label_lost_users_with_list["args"])

        log = get_log("label_lost_users")

        assert log["args"] == label_lost_users_with_list["expected_args"]
        assert log["performance_before"] == label_lost_users_with_list["performance_before"]
        assert log["performance_after"] == label_lost_users_with_list["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_label_lost_users_helper_with_time(
        self, test_stream: Eventstream, label_lost_users_with_time: dict
    ) -> None:
        test_stream.label_lost_users(**label_lost_users_with_time["args"])

        log = get_log("label_lost_users")

        assert log["args"] == label_lost_users_with_time["expected_args"]
        assert log["performance_before"] == label_lost_users_with_time["performance_before"]
        assert log["performance_after"] == label_lost_users_with_time["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_label_cropped_paths_helper_full(
        self, test_stream: Eventstream, label_cropped_paths_full_params: dict
    ) -> None:
        test_stream.label_cropped_paths(**label_cropped_paths_full_params["args"])

        log = get_log("label_cropped_paths")

        assert log["args"] == label_cropped_paths_full_params["expected_args"]
        assert log["performance_before"] == label_cropped_paths_full_params["performance_before"]
        assert log["performance_after"] == label_cropped_paths_full_params["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_filter_events_helper_full(
        self, test_stream: Eventstream, filter_events_full_params: dict
    ) -> None:
        test_stream.filter_events(**filter_events_full_params["args"])

        log = get_log("filter_events")

        assert log["args"] == filter_events_full_params["expected_args"]
        assert log["performance_before"] == filter_events_full_params["performance_before"]
        assert log["performance_after"] == filter_events_full_params["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_drop_paths_with_steps_helper_with_steps(
        self, test_stream: Eventstream, drop_paths_with_steps: dict
    ) -> None:
        test_stream.drop_paths(**drop_paths_with_steps["args"])

        log = get_log("drop_paths")

        assert log["args"] == drop_paths_with_steps["expected_args"]
        assert log["performance_before"] == drop_paths_with_steps["performance_before"]
        assert log["performance_after"] == drop_paths_with_steps["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_drop_paths_helper_with_time(self, test_stream: Eventstream, drop_paths_with_time: dict) -> None:
        test_stream.drop_paths(**drop_paths_with_time["args"])

        log = get_log("drop_paths")

        assert log["args"] == drop_paths_with_time["expected_args"]
        assert log["performance_before"] == drop_paths_with_time["performance_before"]
        assert log["performance_after"] == drop_paths_with_time["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_truncate_paths_helper_full(
        self, test_stream: Eventstream, truncate_paths_full_params: dict
    ) -> None:
        test_stream.truncate_paths(**truncate_paths_full_params["args"])

        log = get_log("truncate_paths")

        assert log["args"] == truncate_paths_full_params["expected_args"]
        assert log["performance_before"] == truncate_paths_full_params["performance_before"]
        assert log["performance_after"] == truncate_paths_full_params["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_group_events_helper_full(self, test_stream: Eventstream, group_events_full_params: dict) -> None:
        test_stream.group_events(**group_events_full_params["args"])

        log = get_log("group_events")

        assert log["args"] == group_events_full_params["expected_args"]
        assert log["performance_before"] == group_events_full_params["performance_before"]
        assert log["performance_after"] == group_events_full_params["performance_after"]

    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_collapse_loops_helper_full(
        self, test_stream: Eventstream, collapse_loops_full_params: dict
    ) -> None:
        test_stream.collapse_loops(**collapse_loops_full_params["args"])

        log = get_log("collapse_loops")

        assert log["args"] == collapse_loops_full_params["expected_args"]
        assert log["performance_before"] == collapse_loops_full_params["performance_before"]
        assert log["performance_after"] == collapse_loops_full_params["performance_after"]
