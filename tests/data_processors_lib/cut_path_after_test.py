from __future__ import annotations

import pandas as pd

from src.data_processors_lib.rete import CutPathAfterEvent, CutPathAfterEventParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema
from src.graph.p_graph import PGraph, EventsNode


class TestCutPathAfter:
    def test_cut_path_after_apply__by_one_event(self):

        source_df = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00'],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30'],
            [1, 'event3', 'raw', '2022-01-01 00:03:30'],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event3', 'raw', '2022-01-01 00:04:30'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event3', 'raw', '2022-01-02 00:00:05'],
            [2, 'event2', 'raw', '2022-01-02 00:01:05'],
            [2, 'end', 'end', '2022-01-02 00:01:05'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event4', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']
        ], columns=['user_id', 'event', 'event_type', 'timestamp']
        )

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp', '_deleted']

        correct_result = pd.DataFrame([
            [1, 'event1', 'raw', '2022-01-01 00:04:00', True],
            [1, 'event3', 'raw', '2022-01-01 00:04:30', True],
            [1, 'event1', 'raw', '2022-01-01 00:05:00', True],
            [2, 'event2', 'raw', '2022-01-02 00:01:05', True],
            [2, 'end', 'end', '2022-01-02 00:01:05', True]
        ], columns=correct_result_columns
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event",
                event_timestamp="timestamp",
                user_id="user_id",
                event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = CutPathAfterEvent(params=CutPathAfterEventParams(cutoff_events=["event3"],
                                                                  cut_shift=0,
                                                                  min_cjm=0))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_cut_path_after_apply__by_two_events(self):

        source_df = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00'],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30'],
            [1, 'event3', 'raw', '2022-01-01 00:03:30'],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event3', 'raw', '2022-01-01 00:04:30'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event3', 'raw', '2022-01-02 00:00:05'],
            [2, 'event2', 'raw', '2022-01-02 00:01:05'],
            [2, 'end', 'end', '2022-01-02 00:01:05'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event4', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']
        ], columns=['user_id', 'event', 'event_type', 'timestamp']
        )

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp', '_deleted']

        correct_result = pd.DataFrame([
            [1, 'event1', 'raw', '2022-01-01 00:02:00', True],
            [1, 'event1', 'raw', '2022-01-01 00:03:00', True],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00', True],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30', True],
            [1, 'event3', 'raw', '2022-01-01 00:03:30', True],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30', True],
            [1, 'event1', 'raw', '2022-01-01 00:04:00', True],
            [1, 'event3', 'raw', '2022-01-01 00:04:30', True],
            [1, 'event1', 'raw', '2022-01-01 00:05:00', True],
            [2, 'event2', 'raw', '2022-01-02 00:01:05', True],
            [2, 'end', 'end', '2022-01-02 00:01:05', True]
        ], columns=correct_result_columns
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event",
                event_timestamp="timestamp",
                user_id="user_id",
                event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = CutPathAfterEvent(params=CutPathAfterEventParams(cutoff_events=["event3", "event2"],
                                                                  cut_shift=0,
                                                                  min_cjm=0))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_cut_path_after_apply__cut_shift(self):

        source_df = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00'],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30'],
            [1, 'event3', 'raw', '2022-01-01 00:03:30'],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event3', 'raw', '2022-01-01 00:04:30'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event3', 'raw', '2022-01-02 00:00:05'],
            [2, 'event2', 'raw', '2022-01-02 00:01:05'],
            [2, 'end', 'end', '2022-01-02 00:01:05'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event4', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']
        ], columns=['user_id', 'event', 'event_type', 'timestamp']
        )

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp', '_deleted']

        correct_result = pd.DataFrame([
            [1, 'event1', 'raw', '2022-01-01 00:03:00', True],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00', True],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30', True],
            [1, 'event3', 'raw', '2022-01-01 00:03:30', True],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30', True],
            [1, 'event1', 'raw', '2022-01-01 00:04:00', True],
            [1, 'event3', 'raw', '2022-01-01 00:04:30', True],
            [1, 'event1', 'raw', '2022-01-01 00:05:00', True],
            [2, 'event1', 'raw', '2022-01-02 00:00:00', True],
            [2, 'event3', 'raw', '2022-01-02 00:00:05', True],
            [2, 'event2', 'raw', '2022-01-02 00:01:05', True],
            [2, 'end', 'end', '2022-01-02 00:01:05', True]
        ], columns=correct_result_columns
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event",
                event_timestamp="timestamp",
                user_id="user_id",
                event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = CutPathAfterEvent(params=CutPathAfterEventParams(cutoff_events=["event3"],
                                                                  cut_shift=2,
                                                                  min_cjm=0))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_cut_path_after_apply__min_cjm_4(self):

        source_df = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00'],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30'],
            [1, 'event3', 'raw', '2022-01-01 00:03:30'],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event3', 'raw', '2022-01-01 00:04:30'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event3', 'raw', '2022-01-02 00:00:05'],
            [2, 'event2', 'raw', '2022-01-02 00:01:05'],
            [2, 'end', 'end', '2022-01-02 00:01:05'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event4', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']

        ], columns=['user_id', 'event', 'event_type', 'timestamp']
        )

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp', '_deleted']

        correct_result = pd.DataFrame([
            [1, 'event1', 'raw', '2022-01-01 00:04:00', True],
            [1, 'event3', 'raw', '2022-01-01 00:04:30', True],
            [1, 'event1', 'raw', '2022-01-01 00:05:00', True],
            [2, 'event1', 'raw', '2022-01-02 00:00:00', True],
            [2, 'event3', 'raw', '2022-01-02 00:00:05', True],
            [2, 'event2', 'raw', '2022-01-02 00:01:05', True],
            [2, 'end', 'end', '2022-01-02 00:01:05', True],
            [3, 'event1', 'raw', '2022-01-02 00:01:10', True],
            [3, 'event1', 'raw', '2022-01-02 00:02:05', True],
            [3, 'event4', 'raw', '2022-01-02 00:03:05', True],
            [3, 'end', 'end', '2022-01-02 00:03:05', True]
        ], columns=correct_result_columns
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event",
                event_timestamp="timestamp",
                user_id="user_id",
                event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = CutPathAfterEvent(params=CutPathAfterEventParams(cutoff_events=["event3"],
                                                                  cut_shift=0,
                                                                  min_cjm=4))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_cut_path_after_apply__cut_shift_1_min_cjm_3(self):

        source_df = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00'],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30'],
            [1, 'event3', 'raw', '2022-01-01 00:03:30'],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event3', 'raw', '2022-01-01 00:04:30'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event3', 'raw', '2022-01-02 00:00:05'],
            [2, 'event2', 'raw', '2022-01-02 00:01:05'],
            [2, 'end', 'end', '2022-01-02 00:01:05'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event4', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']
        ], columns=['user_id', 'event', 'event_type', 'timestamp']
        )

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp', '_deleted']

        correct_result = pd.DataFrame([
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30', True],
            [1, 'event3', 'raw', '2022-01-01 00:03:30', True],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30', True],
            [1, 'event1', 'raw', '2022-01-01 00:04:00', True],
            [1, 'event3', 'raw', '2022-01-01 00:04:30', True],
            [1, 'event1', 'raw', '2022-01-01 00:05:00', True],
            [2, 'event1', 'raw', '2022-01-02 00:00:00', True],
            [2, 'event3', 'raw', '2022-01-02 00:00:05', True],
            [2, 'event2', 'raw', '2022-01-02 00:01:05', True],
            [2, 'end', 'end', '2022-01-02 00:01:05', True]
        ], columns=correct_result_columns
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event",
                event_timestamp="timestamp",
                user_id="user_id",
                event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = CutPathAfterEvent(params=CutPathAfterEventParams(cutoff_events=["event3"],
                                                                  cut_shift=1,
                                                                  min_cjm=3))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)


class TestCutPathAfterGraph:
    def test_cut_path_after_graph__by_one_event(self):

        source_df = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00'],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30'],
            [1, 'event3', 'raw', '2022-01-01 00:03:30'],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event3', 'raw', '2022-01-01 00:04:30'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event3', 'raw', '2022-01-02 00:00:05'],
            [2, 'event2', 'raw', '2022-01-02 00:01:05'],
            [2, 'end', 'end', '2022-01-02 00:01:05'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event4', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']
        ], columns=['user_id', 'event', 'event_type', 'timestamp']
        )

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        correct_result = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00'],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30'],
            [1, 'event3', 'raw', '2022-01-01 00:03:30'],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event3', 'raw', '2022-01-02 00:00:05'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event4', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']
        ], columns=correct_result_columns
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event",
                event_timestamp="timestamp",
                user_id="user_id",
                event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = EventsNode(CutPathAfterEvent(params=CutPathAfterEventParams(cutoff_events=["event3"],
                                                                  cut_shift=0,
                                                                  min_cjm=0)))
        graph = PGraph(source_stream=source)
        graph.add_node(node=events, parents=[graph.root])

        result = graph.combine(node=events)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_cut_path_after_graph__by_two_events(self):

        source_df = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00'],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30'],
            [1, 'event3', 'raw', '2022-01-01 00:03:30'],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event3', 'raw', '2022-01-01 00:04:30'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event3', 'raw', '2022-01-02 00:00:05'],
            [2, 'event2', 'raw', '2022-01-02 00:01:05'],
            [2, 'end', 'end', '2022-01-02 00:01:05'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event4', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']
        ], columns=['user_id', 'event', 'event_type', 'timestamp']
        )

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        correct_result = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event3', 'raw', '2022-01-02 00:00:05'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event4', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']
        ], columns=correct_result_columns
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event",
                event_timestamp="timestamp",
                user_id="user_id",
                event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = EventsNode(CutPathAfterEvent(params=CutPathAfterEventParams(cutoff_events=["event3", "event2"],
                                                                  cut_shift=0,
                                                                  min_cjm=0)))
        graph = PGraph(source_stream=source)
        graph.add_node(node=events, parents=[graph.root])

        result = graph.combine(node=events)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_cut_path_after_graph__cut_shift_2(self):

        source_df = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00'],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30'],
            [1, 'event3', 'raw', '2022-01-01 00:03:30'],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event3', 'raw', '2022-01-01 00:04:30'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event3', 'raw', '2022-01-02 00:00:05'],
            [2, 'event2', 'raw', '2022-01-02 00:01:05'],
            [2, 'end', 'end', '2022-01-02 00:01:05'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event4', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']
        ], columns=['user_id', 'event', 'event_type', 'timestamp']
        )

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        correct_result = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event4', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']
        ], columns=correct_result_columns
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event",
                event_timestamp="timestamp",
                user_id="user_id",
                event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = EventsNode(CutPathAfterEvent(params=CutPathAfterEventParams(cutoff_events=["event3"],
                                                                  cut_shift=2,
                                                                  min_cjm=0)))
        graph = PGraph(source_stream=source)
        graph.add_node(node=events, parents=[graph.root])

        result = graph.combine(node=events)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_cut_path_after_graph__min_cjm_4(self):

        source_df = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00'],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30'],
            [1, 'event3', 'raw', '2022-01-01 00:03:30'],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event3', 'raw', '2022-01-01 00:04:30'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event3', 'raw', '2022-01-02 00:00:05'],
            [2, 'event2', 'raw', '2022-01-02 00:01:05'],
            [2, 'end', 'end', '2022-01-02 00:01:05'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event4', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']

        ], columns=['user_id', 'event', 'event_type', 'timestamp']
        )

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        correct_result = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00'],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30'],
            [1, 'event3', 'raw', '2022-01-01 00:03:30'],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30']
        ], columns=correct_result_columns
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event",
                event_timestamp="timestamp",
                user_id="user_id",
                event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = EventsNode(CutPathAfterEvent(params=CutPathAfterEventParams(cutoff_events=["event3"],
                                                                  cut_shift=0,
                                                                  min_cjm=4)))
        graph = PGraph(source_stream=source)
        graph.add_node(node=events, parents=[graph.root])

        result = graph.combine(node=events)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_cut_path_after_graph__cut_shift_1_min_cjm_3(self):

        source_df = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00'],
            [1, 'session_start', 'session_start', '2022-01-01 00:03:30'],
            [1, 'event3', 'raw', '2022-01-01 00:03:30'],
            [1, 'event3_synthetic', 'synthetic', '2022-01-01 00:03:30'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event3', 'raw', '2022-01-01 00:04:30'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event2', 'raw', '2022-01-02 00:00:05'],
            [2, 'event3', 'raw', '2022-01-02 00:01:05'],
            [2, 'end', 'end', '2022-01-02 00:01:05'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event2', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']

        ], columns=['user_id', 'event', 'event_type', 'timestamp']
        )

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        correct_result = pd.DataFrame([
            [1, 'start', 'start', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'synthetic', '2022-01-01 00:03:00'],
            [3, 'event1', 'raw', '2022-01-02 00:01:10'],
            [3, 'event1', 'raw', '2022-01-02 00:02:05'],
            [3, 'event2', 'raw', '2022-01-02 00:03:05'],
            [3, 'end', 'end', '2022-01-02 00:03:05']
        ], columns=correct_result_columns
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event",
                event_timestamp="timestamp",
                user_id="user_id",
                event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = EventsNode(CutPathAfterEvent(params=CutPathAfterEventParams(cutoff_events=["event3"],
                                                                  cut_shift=1,
                                                                  min_cjm=3)))
        graph = PGraph(source_stream=source)
        graph.add_node(node=events, parents=[graph.root])

        result = graph.combine(node=events)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)