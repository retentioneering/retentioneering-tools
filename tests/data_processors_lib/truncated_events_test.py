from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from src.data_processors_lib.rete import TruncatedEvents, TruncatedEventsParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema
from src.graph.p_graph import EventsNode, PGraph


class TestTruncatedEvents:
    def test_truncated_events_apply__left_right(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:00:00"],
                [2, "event1", "2022-01-01 00:30:00"],
                [2, "event2", "2022-01-01 00:31:00"],
                [3, "event1", "2022-01-01 01:00:01"],
                [3, "event2", "2022-01-01 01:00:02"],
                [4, "event1", "2022-01-01 02:01:00"],
                [4, "event2", "2022-01-01 02:02:00"],
                [5, "event1", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]
        correct_result = pd.DataFrame(
            [
                [1, "truncated_left", "truncated_left", "2022-01-01 00:00:00"],
                [2, "truncated_left", "truncated_left", "2022-01-01 00:30:00"],
                [4, "truncated_right", "truncated_right", "2022-01-01 02:02:00"],
                [5, "truncated_right", "truncated_right", "2022-01-01 03:00:00"],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        params = TruncatedEventsParams(left_truncated_cutoff=(1, "h"), right_truncated_cutoff=(1, "h"))
        events = TruncatedEvents(params=params)
        result = events.apply(source)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncated_events_apply__left(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:00:00"],
                [2, "event1", "2022-01-01 00:30:00"],
                [2, "event2", "2022-01-01 00:31:00"],
                [3, "event1", "2022-01-01 01:00:01"],
                [3, "event2", "2022-01-01 01:00:02"],
                [4, "event1", "2022-01-01 02:01:00"],
                [4, "event2", "2022-01-01 02:02:00"],
                [5, "event1", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]
        correct_result = pd.DataFrame(
            [
                [1, "truncated_left", "truncated_left", "2022-01-01 00:00:00"],
                [2, "truncated_left", "truncated_left", "2022-01-01 00:30:00"],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        params = TruncatedEventsParams(left_truncated_cutoff=(1, "h"))
        events = TruncatedEvents(params=params)
        result = events.apply(source)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_truncated_events_apply__right(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:00:00"],
                [2, "event1", "2022-01-01 00:30:00"],
                [2, "event2", "2022-01-01 00:31:00"],
                [3, "event1", "2022-01-01 01:00:01"],
                [3, "event2", "2022-01-01 01:00:02"],
                [4, "event1", "2022-01-01 02:01:00"],
                [4, "event2", "2022-01-01 02:02:00"],
                [5, "event1", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]
        correct_result = pd.DataFrame(
            [
                [4, "truncated_right", "truncated_right", "2022-01-01 02:02:00"],
                [5, "truncated_right", "truncated_right", "2022-01-01 03:00:00"],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        params = TruncatedEventsParams(right_truncated_cutoff=(1, "h"))
        events = TruncatedEvents(params=params)
        result = events.apply(source)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_params_model__incorrect_datetime_unit(self):
        with pytest.raises(ValidationError):
            p = TruncatedEventsParams(left_truncated_cutoff=(1, "xxx"))


class TestTruncatedEventsGraph:
    def test_truncated_events_graph(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:00:00"],
                [2, "event1", "2022-01-01 00:30:00"],
                [2, "event2", "2022-01-01 00:31:00"],
                [3, "event1", "2022-01-01 01:00:01"],
                [3, "event2", "2022-01-01 01:00:02"],
                [4, "event1", "2022-01-01 02:01:00"],
                [4, "event2", "2022-01-01 02:02:00"],
                [5, "event1", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]
        correct_result = pd.DataFrame(
            [
                [1, "truncated_left", "truncated_left", "2022-01-01 00:00:00"],
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [2, "truncated_left", "truncated_left", "2022-01-01 00:30:00"],
                [2, "event1", "raw", "2022-01-01 00:30:00"],
                [2, "event2", "raw", "2022-01-01 00:31:00"],
                [3, "event1", "raw", "2022-01-01 01:00:01"],
                [3, "event2", "raw", "2022-01-01 01:00:02"],
                [4, "event1", "raw", "2022-01-01 02:01:00"],
                [4, "event2", "raw", "2022-01-01 02:02:00"],
                [4, "truncated_right", "truncated_right", "2022-01-01 02:02:00"],
                [5, "event1", "raw", "2022-01-01 03:00:00"],
                [5, "truncated_right", "truncated_right", "2022-01-01 03:00:00"],
            ],
            columns=correct_result_columns,
        )

        stream = Eventstream(
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        graph = PGraph(source_stream=stream)
        params = TruncatedEventsParams(left_truncated_cutoff=(1, "h"), right_truncated_cutoff=(1, "h"))
        truncated_events = EventsNode(TruncatedEvents(params=params))
        graph.add_node(node=truncated_events, parents=[graph.root])
        res = graph.combine(node=truncated_events).to_dataframe()[correct_result_columns]

        assert res.compare(correct_result).shape == (0, 0)


class TestTruncatedEventsHelper:
    def test_truncated_events_graph(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:00:00"],
                [2, "event1", "2022-01-01 00:30:00"],
                [2, "event2", "2022-01-01 00:31:00"],
                [3, "event1", "2022-01-01 01:00:01"],
                [3, "event2", "2022-01-01 01:00:02"],
                [4, "event1", "2022-01-01 02:01:00"],
                [4, "event2", "2022-01-01 02:02:00"],
                [5, "event1", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]
        correct_result = pd.DataFrame(
            [
                [1, "truncated_left", "truncated_left", "2022-01-01 00:00:00"],
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [2, "truncated_left", "truncated_left", "2022-01-01 00:30:00"],
                [2, "event1", "raw", "2022-01-01 00:30:00"],
                [2, "event2", "raw", "2022-01-01 00:31:00"],
                [3, "event1", "raw", "2022-01-01 01:00:01"],
                [3, "event2", "raw", "2022-01-01 01:00:02"],
                [4, "event1", "raw", "2022-01-01 02:01:00"],
                [4, "event2", "raw", "2022-01-01 02:02:00"],
                [4, "truncated_right", "truncated_right", "2022-01-01 02:02:00"],
                [5, "event1", "raw", "2022-01-01 03:00:00"],
                [5, "truncated_right", "truncated_right", "2022-01-01 03:00:00"],
            ],
            columns=correct_result_columns,
        )

        stream = Eventstream(
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        res = stream.truncated_events(left_truncated_cutoff=(1, "h"), right_truncated_cutoff=(1, "h")).to_dataframe()[
            correct_result_columns
        ]

        assert res.compare(correct_result).shape == (0, 0)
