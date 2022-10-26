from __future__ import annotations

import pandas as pd

from src.data_processors_lib.rete import CollapseLoops, CollapseLoopsParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema
from src.graph.p_graph import EventsNode, PGraph


class TestCollapseLoops:
    def test_collapse_loops_apply__suffix_count__agg_min(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            schema=EventstreamSchema(),
        )

        params = {"suffix": "count", "timestamp_aggregation_type": "min"}

        collapsed = CollapseLoops(params=CollapseLoopsParams(**params))
        result = collapsed.apply(source)
        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result_false_min_del = pd.DataFrame(
            [
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:02:00", False],
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:00", False],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:05", True],
            ],
            columns=correct_result_columns,
        )

        result_df_all = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df_all.compare(correct_result_false_min_del).shape == (0, 0)

    def test_collapse_loops_apply__suffix_count__agg_max(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            schema=EventstreamSchema(),
        )

        params = {"suffix": "count", "timestamp_aggregation_type": "max"}

        collapsed = CollapseLoops(params=CollapseLoopsParams(**params))
        result = collapsed.apply(source)

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result_false_max_del = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:05:00", False],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:05", False],
                [2, "event1", "raw", "2022-01-02 00:00:05", True],
            ],
            columns=correct_result_columns,
        )

        result_df_all = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df_all.compare(correct_result_false_max_del).shape == (0, 0)

    def test_collapse_loops_apply__suffix_loop__agg_mean(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            schema=EventstreamSchema(),
        )

        params = {"suffix": "loop", "timestamp_aggregation_type": "mean"}

        collapsed = CollapseLoops(params=CollapseLoopsParams(**params))
        result = collapsed.apply(source)

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result_true_mean_del = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1_loop", "group_alias", "2022-01-01 00:03:30", False],
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event1_loop", "group_alias", "2022-01-02 00:00:02.5", False],
                [2, "event1", "raw", "2022-01-02 00:00:05", True],
            ],
            columns=correct_result_columns,
        )

        result_df_all = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df_all.compare(correct_result_true_mean_del).shape == (0, 0)


class TestCollapseLoopsGraph:
    def test_collapse_loops_graph__suffix_count__agg_min(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            schema=EventstreamSchema(),
        )

        graph = PGraph(source_stream=source)

        params = {"suffix": "count", "timestamp_aggregation_type": "min"}

        collapsed = EventsNode(CollapseLoops(params=CollapseLoopsParams(**params)))

        graph.add_node(node=collapsed, parents=[graph.root])

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]

        res = graph.combine(node=collapsed).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_false_min = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:02:00"],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:00"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )

        assert res.compare(correct_result_false_min).shape == (0, 0)

    def test_collapse_loops_graph__suffix_count__agg_max(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            schema=EventstreamSchema(),
        )

        graph = PGraph(source_stream=source)

        params = {"suffix": "count", "timestamp_aggregation_type": "max"}

        collapsed = EventsNode(CollapseLoops(params=CollapseLoopsParams(**params)))

        graph.add_node(node=collapsed, parents=[graph.root])

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]

        res = graph.combine(node=collapsed).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_false_max = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:05:00"],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )

        assert res.compare(correct_result_false_max).shape == (0, 0)

    def test_collapse_loops_graph__suffix_loop__agg_mean(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            schema=EventstreamSchema(),
        )

        graph = PGraph(source_stream=source)

        params = {"suffix": "loop", "timestamp_aggregation_type": "mean"}

        collapsed = EventsNode(CollapseLoops(params=CollapseLoopsParams(**params)))

        graph.add_node(node=collapsed, parents=[graph.root])

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]

        res = graph.combine(node=collapsed).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_true_mean = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop", "group_alias", "2022-01-01 00:03:30"],
                [2, "event1_loop", "group_alias", "2022-01-02 00:00:02.5"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )

        assert res.compare(correct_result_true_mean).shape == (0, 0)

    def test_collapse_loops_graph__suffix_none__agg_mean(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            schema=EventstreamSchema(),
        )

        graph = PGraph(source_stream=source)

        params = {"suffix": None, "timestamp_aggregation_type": "mean"}

        collapsed = EventsNode(CollapseLoops(params=CollapseLoopsParams(**params)))

        graph.add_node(node=collapsed, parents=[graph.root])

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]

        res = graph.combine(node=collapsed).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_true_mean = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "group_alias", "2022-01-01 00:03:30"],
                [2, "event1", "group_alias", "2022-01-02 00:00:02.5"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )

        assert res.compare(correct_result_true_mean).shape == (0, 0)
