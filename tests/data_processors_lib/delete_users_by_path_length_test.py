from __future__ import annotations

import pandas as pd

from src.data_processors_lib.rete import (
    DeleteUsersByPathLength,
    DeleteUsersByPathLengthParams,
)
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema
from src.graph.p_graph import EventsNode, PGraph


class TestDeleteUsersByPathLength:
    def test_delete_users_by_path_length_apply__by_event_4(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event3", "raw", "2022-01-02 00:00:05", True],
                [2, "event2", "raw", "2022-01-02 00:01:05", True],
                [2, "end", "end", "2022-01-02 00:01:05", True],
                [3, "event1", "raw", "2022-01-02 00:01:10", True],
                [3, "event1", "raw", "2022-01-02 00:02:05", True],
                [3, "event4", "raw", "2022-01-02 00:03:05", True],
                [3, "end", "end", "2022-01-02 00:03:05", True],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = DeleteUsersByPathLength(params=DeleteUsersByPathLengthParams(events_num=4))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_delete_users_by_path_length_apply__by_cutoff(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "_deleted"]

        correct_result = pd.DataFrame(
            [
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event3", "raw", "2022-01-02 00:00:05", True],
                [2, "event2", "raw", "2022-01-02 00:01:05", True],
                [2, "end", "end", "2022-01-02 00:01:05", True],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = DeleteUsersByPathLength(params=DeleteUsersByPathLengthParams(cutoff=(1.5, "m")))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)


class TestDeleteUsersByPathLengthGraph:
    def test_delete_users_by_path_length_graph__by_event_4(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]

        correct_result = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = EventsNode(DeleteUsersByPathLength(params=DeleteUsersByPathLengthParams(events_num=4)))
        graph = PGraph(source_stream=source)
        graph.add_node(node=events, parents=[graph.root])

        result = graph.combine(node=events)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_delete_users_by_path_length_graph__by_cutoff(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]

        correct_result = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        events = EventsNode(DeleteUsersByPathLength(params=DeleteUsersByPathLengthParams(cutoff=(1.5, "m"))))
        graph = PGraph(source_stream=source)
        graph.add_node(node=events, parents=[graph.root])

        result = graph.combine(node=events)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)


class TestDeleteUsersByPathLengthHelper:
    def test_delete_users_by_path_length_graph__by_event_4(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]

        correct_result = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        result = source.delete_users(events_num=4)
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_delete_users_by_path_length_graph__by_cutoff(self):
        source_df = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp"]

        correct_result = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "event3_synthetic", "synthetic", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=correct_result_columns,
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        result = source.delete_users(cutoff=(1.5, "m"))
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)
