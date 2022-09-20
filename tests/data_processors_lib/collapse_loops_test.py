from __future__ import annotations

import pandas as pd

from src.data_processors_lib.rete import CollapseLoops, CollapseLoopsParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema
from src.graph.p_graph import PGraph, EventsNode


class TestCollapseLoops:
    def test_collapse_loops_apply_1(self):
        source_df = pd.DataFrame([
            [1, 'event1', '2022-01-01 00:01:00'],
            [1, 'event1', '2022-01-01 00:02:00'],
            [1, 'event2', '2022-01-01 00:01:02'],
            [1, 'event1', '2022-01-01 00:03:00'],
            [1, 'event1', '2022-01-01 00:04:00'],
            [1, 'event1', '2022-01-01 00:05:00'],
            [2, 'event1', '2022-01-02 00:00:00'],
            [2, 'event1', '2022-01-02 00:00:05'],
            [2, 'event2', '2022-01-02 00:00:05'],
        ], columns=['user_id', 'event', 'timestamp']
        )

        source = Eventstream(
                raw_data=source_df,
                raw_data_schema=RawDataSchema(
                    event_name="event",
                    event_timestamp="timestamp",
                    user_id="user_id"),
                schema=EventstreamSchema()
        )

        params = {
            'full_collapse': False,
            'agg': 'min'
        }

        collapsed = CollapseLoops(params=CollapseLoopsParams(**params))
        result = collapsed.apply(source)
        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        correct_result_false_min = pd.DataFrame([
            [1, 'event1_loop_4', 'group_alias', '2022-01-01 00:02:00'],
            [2, 'event1_loop_2', 'group_alias', '2022-01-02 00:00:00']
        ], columns=correct_result_columns
        )

        correct_result_false_min_del = pd.DataFrame([
            [1, 'event1_loop_4', 'group_alias', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'event1_loop_2', 'group_alias', '2022-01-02 00:00:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:05'],
        ], columns=correct_result_columns
        )

        result_df_all = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert (result_df.compare(correct_result_false_min).shape == (0, 0) and
                result_df_all.compare(correct_result_false_min_del).shape == (0, 0)
                )

    def test_collapse_loops_apply_2(self):
        source_df = pd.DataFrame([
            [1, 'event1', '2022-01-01 00:01:00'],
            [1, 'event1', '2022-01-01 00:02:00'],
            [1, 'event2', '2022-01-01 00:01:02'],
            [1, 'event1', '2022-01-01 00:03:00'],
            [1, 'event1', '2022-01-01 00:04:00'],
            [1, 'event1', '2022-01-01 00:05:00'],
            [2, 'event1', '2022-01-02 00:00:00'],
            [2, 'event1', '2022-01-02 00:00:05'],
            [2, 'event2', '2022-01-02 00:00:05'],
        ], columns=['user_id', 'event', 'timestamp']
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(
                event_name="event",
                event_timestamp="timestamp",
                user_id="user_id"),
            schema=EventstreamSchema()
        )

        params = {
            'full_collapse': False,
            'agg': 'max'
        }

        collapsed = CollapseLoops(params=CollapseLoopsParams(**params))
        result = collapsed.apply(source)

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        correct_result_false_max = pd.DataFrame([
            [1, 'event1_loop_4', 'group_alias', '2022-01-01 00:05:00'],
            [2, 'event1_loop_2', 'group_alias', '2022-01-02 00:00:05']
        ], columns=correct_result_columns
        )

        correct_result_false_max_del = pd.DataFrame([
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event1_loop_4', 'group_alias', '2022-01-01 00:05:00'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event1_loop_2', 'group_alias', '2022-01-02 00:00:05'],
            [2, 'event1', 'raw', '2022-01-02 00:00:05'],
        ], columns=correct_result_columns
        )

        result_df_all = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert (result_df.compare(correct_result_false_max).shape == (0, 0) and
                result_df_all.compare(correct_result_false_max_del).shape == (0, 0)
                )

    def test_collapse_loops_apply_3(self):
        source_df = pd.DataFrame([
            [1, 'event1', '2022-01-01 00:01:00'],
            [1, 'event1', '2022-01-01 00:02:00'],
            [1, 'event2', '2022-01-01 00:01:02'],
            [1, 'event1', '2022-01-01 00:03:00'],
            [1, 'event1', '2022-01-01 00:04:00'],
            [1, 'event1', '2022-01-01 00:05:00'],
            [2, 'event1', '2022-01-02 00:00:00'],
            [2, 'event1', '2022-01-02 00:00:05'],
            [2, 'event2', '2022-01-02 00:00:05'],
        ], columns=['user_id', 'event', 'timestamp']
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(
                event_name="event",
                event_timestamp="timestamp",
                user_id="user_id"),
            schema=EventstreamSchema()
        )

        params = {
            'full_collapse': True,
            'agg': 'mean'
        }

        collapsed = CollapseLoops(params=CollapseLoopsParams(**params))
        result = collapsed.apply(source)

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        correct_result_true_mean = pd.DataFrame([
            [1, 'event1_loop', 'group_alias', '2022-01-01 00:03:30'],
            [2, 'event1_loop', 'group_alias', '2022-01-02 00:00:02.5']
        ], columns=correct_result_columns
        )

        correct_result_true_mean_del = pd.DataFrame([
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1_loop', 'group_alias', '2022-01-01 00:03:30'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event1_loop', 'group_alias', '2022-01-02 00:00:02.5'],
            [2, 'event1', 'raw', '2022-01-02 00:00:05'],
        ], columns=correct_result_columns
        )

        result_df_all = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert (result_df.compare(correct_result_true_mean).shape == (0, 0) and
                result_df_all.compare(correct_result_true_mean_del).shape == (0, 0)
                )

    def test_collapse_loops_combine_1(self):
        source_df = pd.DataFrame([
            [1, 'event1', '2022-01-01 00:01:00'],
            [1, 'event1', '2022-01-01 00:02:00'],
            [1, 'event2', '2022-01-01 00:01:02'],
            [1, 'event1', '2022-01-01 00:03:00'],
            [1, 'event1', '2022-01-01 00:04:00'],
            [1, 'event1', '2022-01-01 00:05:00'],
            [2, 'event1', '2022-01-02 00:00:00'],
            [2, 'event1', '2022-01-02 00:00:05'],
            [2, 'event2', '2022-01-02 00:00:05'],
        ], columns=['user_id', 'event', 'timestamp']
        )

        source = Eventstream(
                raw_data=source_df,
                raw_data_schema=RawDataSchema(
                    event_name="event",
                    event_timestamp="timestamp",
                    user_id="user_id"),
                schema=EventstreamSchema()
        )

        graph = PGraph(source_stream=source)

        params = {
            'full_collapse': False,
            'agg': 'min'
        }

        collapsed = EventsNode(CollapseLoops(params=CollapseLoopsParams(**params)))

        graph.add_node(node=collapsed, parents=[graph.root])

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        res = graph.combine(node=collapsed).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_false_min = pd.DataFrame([
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1_loop_4', 'group_alias', '2022-01-01 00:02:00'],
            [2, 'event1_loop_2', 'group_alias', '2022-01-02 00:00:00'],
            [2, 'event2', 'raw', '2022-01-02 00:00:05']
        ], columns=correct_result_columns
        )

        assert res.compare(correct_result_false_min).shape == (0, 0)

    def test_collapse_loops_combine_2(self):
        source_df = pd.DataFrame([
            [1, 'event1', '2022-01-01 00:01:00'],
            [1, 'event1', '2022-01-01 00:02:00'],
            [1, 'event2', '2022-01-01 00:01:02'],
            [1, 'event1', '2022-01-01 00:03:00'],
            [1, 'event1', '2022-01-01 00:04:00'],
            [1, 'event1', '2022-01-01 00:05:00'],
            [2, 'event1', '2022-01-02 00:00:00'],
            [2, 'event1', '2022-01-02 00:00:05'],
            [2, 'event2', '2022-01-02 00:00:05'],
        ], columns=['user_id', 'event', 'timestamp']
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(
                event_name="event",
                event_timestamp="timestamp",
                user_id="user_id"),
            schema=EventstreamSchema()
        )

        graph = PGraph(source_stream=source)

        params = {
            'full_collapse': False,
            'agg': 'max'
        }

        collapsed = EventsNode(CollapseLoops(params=CollapseLoopsParams(**params)))

        graph.add_node(node=collapsed, parents=[graph.root])

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        res = graph.combine(node=collapsed).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_false_max = pd.DataFrame([
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1_loop_4', 'group_alias', '2022-01-01 00:05:00'],
            [2, 'event1_loop_2', 'group_alias', '2022-01-02 00:00:05'],
            [2, 'event2', 'raw', '2022-01-02 00:00:05']
        ], columns=correct_result_columns
        )

        assert res.compare(correct_result_false_max).shape == (0, 0)

    def test_collapse_loops_combine_3(self):
        source_df = pd.DataFrame([
            [1, 'event1', '2022-01-01 00:01:00'],
            [1, 'event1', '2022-01-01 00:02:00'],
            [1, 'event2', '2022-01-01 00:01:02'],
            [1, 'event1', '2022-01-01 00:03:00'],
            [1, 'event1', '2022-01-01 00:04:00'],
            [1, 'event1', '2022-01-01 00:05:00'],
            [2, 'event1', '2022-01-02 00:00:00'],
            [2, 'event1', '2022-01-02 00:00:05'],
            [2, 'event2', '2022-01-02 00:00:05'],
        ], columns=['user_id', 'event', 'timestamp']
        )

        source = Eventstream(
                raw_data=source_df,
                raw_data_schema=RawDataSchema(
                    event_name="event",
                    event_timestamp="timestamp",
                    user_id="user_id"),
                schema=EventstreamSchema()
        )

        graph = PGraph(source_stream=source)

        params = {
            'full_collapse': True,
            'agg': 'mean'
        }

        collapsed = EventsNode(CollapseLoops(params=CollapseLoopsParams(**params)))

        graph.add_node(node=collapsed, parents=[graph.root])

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        res = graph.combine(node=collapsed).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_true_mean = pd.DataFrame([
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1_loop', 'group_alias', '2022-01-01 00:03:30'],
            [2, 'event1_loop', 'group_alias', '2022-01-02 00:00:02.5'],
            [2, 'event2', 'raw', '2022-01-02 00:00:05']
        ], columns=correct_result_columns
        )

        assert res.compare(correct_result_true_mean).shape == (0, 0)
