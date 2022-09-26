from __future__ import annotations

import pandas as pd

from src.data_processors_lib.rete import NewResumeEvents, NewResumeParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema
from src.graph.p_graph import PGraph, EventsNode

class TestNewResume:
    def test_new_resume_apply__new_users_list_id(self):
        source_df = pd.DataFrame([
            [1, 'event1', '2022-01-01 00:01:00'],
            [1, 'event1', '2022-01-01 00:02:00'],
            [1, 'event2', '2022-01-01 00:01:02'],
            [1, 'event1', '2022-01-01 00:03:00'],
            [1, 'event1', '2022-01-01 00:04:00'],
            [1, 'event1', '2022-01-01 00:05:00'],
            [2, 'event1', '2022-01-02 00:00:00'],
            [2, 'event1', '2022-01-02 00:00:05'],
            [2, 'event2', '2022-01-02 00:01:05'],
        ], columns=['user_id', 'event', 'timestamp']
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        events = NewResumeEvents(params=NewResumeParams(new_users_list=[2]))

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        correct_result = pd.DataFrame([
            [1, 'resume', 'resume', '2022-01-01 00:01:00'],
            [2, 'new_user', 'new_user', '2022-01-02 00:00:00']
        ], columns=correct_result_columns
        )
        result = events.apply(source)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_new_resume_apply__new_users_list_all(self):
        source_df = pd.DataFrame([
            [1, 'event1', '2022-01-01 00:01:00'],
            [1, 'event1', '2022-01-01 00:02:00'],
            [1, 'event2', '2022-01-01 00:01:02'],
            [1, 'event1', '2022-01-01 00:03:00'],
            [1, 'event1', '2022-01-01 00:04:00'],
            [1, 'event1', '2022-01-01 00:05:00'],
            [2, 'event1', '2022-01-02 00:00:00'],
            [2, 'event1', '2022-01-02 00:00:05'],
            [2, 'event2', '2022-01-02 00:01:05'],
        ], columns=['user_id', 'event', 'timestamp']
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        events = NewResumeEvents(params=NewResumeParams(new_users_list='all'))

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        correct_result = pd.DataFrame([
            [1, 'new_user', 'new_user', '2022-01-01 00:01:00'],
            [2, 'new_user', 'new_user', '2022-01-02 00:00:00']
        ], columns=correct_result_columns
        )
        result = events.apply(source)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)


class TestNewResumeGraph:
    def test_new_resume_graph__new_users_list_id(self):
        source_df = pd.DataFrame([
            [1, 'event1', '2022-01-01 00:01:00'],
            [1, 'event2', '2022-01-01 00:01:02'],
            [1, 'event1', '2022-01-01 00:02:00'],
            [1, 'event1', '2022-01-01 00:03:00'],
            [1, 'event1', '2022-01-01 00:04:00'],
            [1, 'event1', '2022-01-01 00:05:00'],
            [2, 'event1', '2022-01-02 00:00:00'],
            [2, 'event1', '2022-01-02 00:00:05'],
            [2, 'event2', '2022-01-02 00:01:05'],
        ], columns=['user_id', 'event', 'timestamp']
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        graph = PGraph(source_stream=source)
        events = EventsNode(NewResumeEvents(params=NewResumeParams(new_users_list=[2])))
        graph.add_node(node=events, parents=[graph.root])
        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        correct_result = pd.DataFrame([
            [1, 'resume', 'resume', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'new_user', 'new_user', '2022-01-02 00:00:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:05'],
            [2, 'event2', 'raw', '2022-01-02 00:01:05'],
        ], columns=correct_result_columns
        )

        result = graph.combine(node=events)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_new_resume_graph__new_users_list_all(self):
        source_df = pd.DataFrame([
            [1, 'event1', '2022-01-01 00:01:00'],
            [1, 'event1', '2022-01-01 00:02:00'],
            [1, 'event2', '2022-01-01 00:01:02'],
            [1, 'event1', '2022-01-01 00:03:00'],
            [1, 'event1', '2022-01-01 00:04:00'],
            [1, 'event1', '2022-01-01 00:05:00'],
            [2, 'event1', '2022-01-02 00:00:00'],
            [2, 'event1', '2022-01-02 00:00:05'],
            [2, 'event2', '2022-01-02 00:01:05'],
        ], columns=['user_id', 'event', 'timestamp']
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        graph = PGraph(source_stream=source)
        events = EventsNode(NewResumeEvents(params=NewResumeParams(new_users_list='all')))
        graph.add_node(node=events, parents=[graph.root])
        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']

        correct_result = pd.DataFrame([
            [1, 'new_user', 'new_user', '2022-01-01 00:01:00'],
            [1, 'event1', 'raw', '2022-01-01 00:01:00'],
            [1, 'event2', 'raw', '2022-01-01 00:01:02'],
            [1, 'event1', 'raw', '2022-01-01 00:02:00'],
            [1, 'event1', 'raw', '2022-01-01 00:03:00'],
            [1, 'event1', 'raw', '2022-01-01 00:04:00'],
            [1, 'event1', 'raw', '2022-01-01 00:05:00'],
            [2, 'new_user', 'new_user', '2022-01-02 00:00:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:00'],
            [2, 'event1', 'raw', '2022-01-02 00:00:05'],
            [2, 'event2', 'raw', '2022-01-02 00:01:05'],
        ], columns=correct_result_columns
        )
        result = graph.combine(node=events)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)
