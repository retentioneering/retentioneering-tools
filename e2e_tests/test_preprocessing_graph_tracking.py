from __future__ import annotations

import os
import time

import pandas as pd
import pytest

from retentioneering.data_processors_lib import (
    AddStartEndEvents,
    AddStartEndEventsParams,
    CollapseLoops,
    CollapseLoopsParams,
    DropPaths,
    DropPathsParams,
    GroupEvents,
    GroupEventsParams,
    SplitSessions,
    SplitSessionsParams,
)
from retentioneering.eventstream import Eventstream
from retentioneering.preprocessing_graph import EventsNode, PreprocessingGraph
from retentioneering.utils.tracker_analytics_tools import get_inner_calls, process_data

from .fixtures.common import test_stream
from .fixtures.helpers import *
from .fixtures.tools import preprocessing_graph_combine_params
from .fixtures.utils import set_local_tracker


def get_logs() -> dict:
    ignored_performance = ["eventstream_hist", "hash"]
    performance_name = "eventstream_init"

    time.sleep(1)

    logs = process_data(pd.read_csv(os.environ["RETE_TRACKER_CSV_PATH"]), only_calls=False).sort_values(by="index")
    main_log = logs[(logs["event_full_name"] == f"preprocessing_graph_combine") & (logs["parent_index"] == 0)].iloc[0]

    inner_calls = get_inner_calls(logs, main_log["index"], index_col="index", parent_col="parent_index")

    arg_logs = inner_calls[inner_calls["event_name"] == "apply"]
    performance_logs = inner_calls[(inner_calls["event_full_name"] == f"{performance_name}")]

    performance_before: dict = performance_logs.iloc[0]["performance_before"]
    performance_after: dict = performance_logs.iloc[-1]["performance_after"]
    for item in ignored_performance:
        performance_before.pop(item, {})
        performance_after.pop(item, {})

    return {
        "arg_logs": arg_logs,
        "performance_before": performance_before,
        "performance_after": performance_after,
    }


class TestPreprocessingGraphTracking:
    @pytest.mark.usefixtures("set_local_tracker")
    def test_tracking_combine(
        self,
        test_stream: Eventstream,
        add_start_end_events_full_params: dict,
        split_sessions_by_timeout: dict,
        collapse_loops_full_params: dict,
        drop_paths_with_steps: dict,
        group_events_full_params: dict,
        preprocessing_graph_combine_params: dict,
    ) -> None:
        pgraph = PreprocessingGraph(test_stream)

        node1 = EventsNode(DropPaths(params=DropPathsParams(**drop_paths_with_steps["args"])))
        node2 = EventsNode(AddStartEndEvents(params=AddStartEndEventsParams()))
        node3 = EventsNode(SplitSessions(params=SplitSessionsParams(**split_sessions_by_timeout["args"])))
        node4 = EventsNode(CollapseLoops(params=CollapseLoopsParams(**collapse_loops_full_params["args"])))
        node5 = EventsNode(GroupEvents(params=GroupEventsParams(**group_events_full_params["args"])))

        pgraph.add_node(node=node1, parents=[pgraph.root])
        pgraph.add_node(node=node2, parents=[node1])
        pgraph.add_node(node=node3, parents=[node2])
        pgraph.add_node(node=node4, parents=[node3])
        pgraph.add_node(node=node5, parents=[node3])

        pgraph.combine(node=node4)

        params = [
            drop_paths_with_steps,
            add_start_end_events_full_params,
            split_sessions_by_timeout,
            collapse_loops_full_params,
        ]
        logs = get_logs()

        for i in range(len(params)):
            assert logs["arg_logs"].iloc[i]["args"] == params[i]["expected_args"]
        assert logs["performance_before"] == params[0]["performance_before"]
        assert logs["performance_after"] == preprocessing_graph_combine_params["performance_info"]
