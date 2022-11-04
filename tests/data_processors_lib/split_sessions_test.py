from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from src.data_processors_lib.rete import SplitSessions, SplitSessionsParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema
from src.graph.p_graph import EventsNode, PGraph


class TestSplitSessions:
    def test_split_session__data_processor(self):
        source_df = pd.DataFrame(
            [
                {"event_name": "pageview", "event_type": "raw", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {
                    "event_name": "cart_btn_click",
                    "event_type": "raw",
                    "event_timestamp": "2021-10-26 12:02",
                    "user_id": "1",
                },
                {"event_name": "pageview", "event_type": "raw", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {
                    "event_name": "plus_icon_click",
                    "event_type": "raw",
                    "event_timestamp": "2021-10-26 12:04",
                    "user_id": "1",
                },
            ]
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        events = SplitSessions(
            params=SplitSessionsParams(
                session_cutoff=(100, "s"),
                mark_truncated=True,
                session_col="session_id",
            )
        )

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)
        events_names: list[str] = result_df[result.schema.event_name].to_list()
        # @TODO: add correct test after example of needed work. Vladimir Makhanov
        assert True

    def test_params_model__incorrect_datetime_unit(self):
        with pytest.raises(ValidationError):
            p = SplitSessionsParams(session_cutoff=(1, "xxx"))


class TestSplitSessionsGraph:
    def test_split_sesssion(self):
        source_df = pd.DataFrame(
            [
                [111, "event1", "2022-01-01 00:00:00"],
                [111, "event2", "2022-01-01 00:01:00"],
                [111, "event3", "2022-01-01 00:33:00"],
                [111, "event4", "2022-01-01 00:34:00"],
                [222, "event1", "2022-01-01 00:30:00"],
                [222, "event2", "2022-01-01 00:31:00"],
                [222, "event3", "2022-01-01 01:01:00"],
                [333, "event1", "2022-01-01 01:00:00"],
                [333, "event2", "2022-01-01 01:01:00"],
                [333, "event3", "2022-01-01 01:32:00"],
                [333, "event4", "2022-01-01 01:33:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "session_id"]
        correct_result = pd.DataFrame(
            [
                [111, "session_start", "session_start", "2022-01-01 00:00:00", "111_1"],
                [111, "event1", "raw", "2022-01-01 00:00:00", "111_1"],
                [111, "event2", "raw", "2022-01-01 00:01:00", "111_1"],
                [111, "session_end", "session_end", "2022-01-01 00:01:00", "111_1"],
                [111, "session_start", "session_start", "2022-01-01 00:33:00", "111_2"],
                [111, "event3", "raw", "2022-01-01 00:33:00", "111_2"],
                [111, "event4", "raw", "2022-01-01 00:34:00", "111_2"],
                [111, "session_end", "session_end", "2022-01-01 00:34:00", "111_2"],
                [222, "session_start", "session_start", "2022-01-01 00:30:00", "222_1"],
                [222, "event1", "raw", "2022-01-01 00:30:00", "222_1"],
                [222, "event2", "raw", "2022-01-01 00:31:00", "222_1"],
                [222, "event3", "raw", "2022-01-01 01:01:00", "222_1"],
                [222, "session_end", "session_end", "2022-01-01 01:01:00", "222_1"],
                [333, "session_start", "session_start", "2022-01-01 01:00:00", "333_1"],
                [333, "event1", "raw", "2022-01-01 01:00:00", "333_1"],
                [333, "event2", "raw", "2022-01-01 01:01:00", "333_1"],
                [333, "session_end", "session_end", "2022-01-01 01:01:00", "333_1"],
                [333, "session_start", "session_start", "2022-01-01 01:32:00", "333_2"],
                [333, "event3", "raw", "2022-01-01 01:32:00", "333_2"],
                [333, "event4", "raw", "2022-01-01 01:33:00", "333_2"],
                [333, "session_end", "session_end", "2022-01-01 01:33:00", "333_2"],
            ],
            columns=correct_result_columns,
        )

        stream = Eventstream(
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        graph = PGraph(source_stream=stream)
        params = SplitSessionsParams(session_cutoff=(30, "m"), session_col="session_id")
        splitted_sessions = EventsNode(SplitSessions(params=params))
        graph.add_node(node=splitted_sessions, parents=[graph.root])
        res = (
            graph.combine(node=splitted_sessions)
            .to_dataframe()[correct_result_columns]
            .sort_values(["user_id", "event_timestamp"])
            .reset_index(drop=True)
        )

        assert res.compare(correct_result).shape == (0, 0)

    def test_split_sesssion__mark_truncated_true(self):
        source_df = pd.DataFrame(
            [
                [111, "event1", "2022-01-01 00:00:00"],
                [111, "event2", "2022-01-01 00:01:00"],
                [111, "event3", "2022-01-01 00:33:00"],
                [111, "event4", "2022-01-01 00:34:00"],
                [222, "event1", "2022-01-01 00:30:00"],
                [222, "event2", "2022-01-01 00:31:00"],
                [222, "event3", "2022-01-01 01:01:00"],
                [333, "event1", "2022-01-01 01:00:00"],
                [333, "event2", "2022-01-01 01:01:00"],
                [333, "event3", "2022-01-01 01:32:00"],
                [333, "event4", "2022-01-01 01:33:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "session_id"]
        correct_result = pd.DataFrame(
            [
                [111, "session_start", "session_start", "2022-01-01 00:00:00", "111_1"],
                [111, "session_start_truncated", "session_start_truncated", "2022-01-01 00:00:00", "111_1"],
                [111, "event1", "raw", "2022-01-01 00:00:00", "111_1"],
                [111, "event2", "raw", "2022-01-01 00:01:00", "111_1"],
                [111, "session_end", "session_end", "2022-01-01 00:01:00", "111_1"],
                [111, "session_start", "session_start", "2022-01-01 00:33:00", "111_2"],
                [111, "event3", "raw", "2022-01-01 00:33:00", "111_2"],
                [111, "event4", "raw", "2022-01-01 00:34:00", "111_2"],
                [111, "session_end", "session_end", "2022-01-01 00:34:00", "111_2"],
                [222, "session_start", "session_start", "2022-01-01 00:30:00", "222_1"],
                [222, "event1", "raw", "2022-01-01 00:30:00", "222_1"],
                [222, "event2", "raw", "2022-01-01 00:31:00", "222_1"],
                [222, "event3", "raw", "2022-01-01 01:01:00", "222_1"],
                [222, "session_end", "session_end", "2022-01-01 01:01:00", "222_1"],
                [333, "session_start", "session_start", "2022-01-01 01:00:00", "333_1"],
                [333, "event1", "raw", "2022-01-01 01:00:00", "333_1"],
                [333, "event2", "raw", "2022-01-01 01:01:00", "333_1"],
                [333, "session_end", "session_end", "2022-01-01 01:01:00", "333_1"],
                [333, "session_start", "session_start", "2022-01-01 01:32:00", "333_2"],
                [333, "event3", "raw", "2022-01-01 01:32:00", "333_2"],
                [333, "event4", "raw", "2022-01-01 01:33:00", "333_2"],
                [333, "session_end_truncated", "session_end_truncated", "2022-01-01 01:33:00", "333_2"],
                [333, "session_end", "session_end", "2022-01-01 01:33:00", "333_2"],
            ],
            columns=correct_result_columns,
        )

        stream = Eventstream(
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        graph = PGraph(source_stream=stream)
        params = SplitSessionsParams(session_cutoff=(30, "m"), session_col="session_id", mark_truncated=True)
        splitted_sessions = EventsNode(SplitSessions(params=params))
        graph.add_node(node=splitted_sessions, parents=[graph.root])
        res = (
            graph.combine(node=splitted_sessions)
            .to_dataframe()[correct_result_columns]
            .sort_values(["user_id", "event_timestamp"])
            .reset_index(drop=True)
        )

        assert res.compare(correct_result).shape == (0, 0)


class TestSplitSessionsHelper:
    def test_split_sesssion(self):
        source_df = pd.DataFrame(
            [
                [111, "event1", "2022-01-01 00:00:00"],
                [111, "event2", "2022-01-01 00:01:00"],
                [111, "event3", "2022-01-01 00:33:00"],
                [111, "event4", "2022-01-01 00:34:00"],
                [222, "event1", "2022-01-01 00:30:00"],
                [222, "event2", "2022-01-01 00:31:00"],
                [222, "event3", "2022-01-01 01:01:00"],
                [333, "event1", "2022-01-01 01:00:00"],
                [333, "event2", "2022-01-01 01:01:00"],
                [333, "event3", "2022-01-01 01:32:00"],
                [333, "event4", "2022-01-01 01:33:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "session_id"]
        correct_result = pd.DataFrame(
            [
                [111, "session_start", "session_start", "2022-01-01 00:00:00", "111_1"],
                [111, "event1", "raw", "2022-01-01 00:00:00", "111_1"],
                [111, "event2", "raw", "2022-01-01 00:01:00", "111_1"],
                [111, "session_end", "session_end", "2022-01-01 00:01:00", "111_1"],
                [111, "session_start", "session_start", "2022-01-01 00:33:00", "111_2"],
                [111, "event3", "raw", "2022-01-01 00:33:00", "111_2"],
                [111, "event4", "raw", "2022-01-01 00:34:00", "111_2"],
                [111, "session_end", "session_end", "2022-01-01 00:34:00", "111_2"],
                [222, "session_start", "session_start", "2022-01-01 00:30:00", "222_1"],
                [222, "event1", "raw", "2022-01-01 00:30:00", "222_1"],
                [222, "event2", "raw", "2022-01-01 00:31:00", "222_1"],
                [222, "event3", "raw", "2022-01-01 01:01:00", "222_1"],
                [222, "session_end", "session_end", "2022-01-01 01:01:00", "222_1"],
                [333, "session_start", "session_start", "2022-01-01 01:00:00", "333_1"],
                [333, "event1", "raw", "2022-01-01 01:00:00", "333_1"],
                [333, "event2", "raw", "2022-01-01 01:01:00", "333_1"],
                [333, "session_end", "session_end", "2022-01-01 01:01:00", "333_1"],
                [333, "session_start", "session_start", "2022-01-01 01:32:00", "333_2"],
                [333, "event3", "raw", "2022-01-01 01:32:00", "333_2"],
                [333, "event4", "raw", "2022-01-01 01:33:00", "333_2"],
                [333, "session_end", "session_end", "2022-01-01 01:33:00", "333_2"],
            ],
            columns=correct_result_columns,
        )

        stream = Eventstream(
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        res = (
            stream.split_sessions(session_cutoff=(30, "m"), session_col="session_id")
            .to_dataframe()[correct_result_columns]
            .sort_values(["user_id", "event_timestamp"])
            .reset_index(drop=True)
        )

        assert res.compare(correct_result).shape == (0, 0)

    def test_split_sesssion__mark_truncated_true(self):
        source_df = pd.DataFrame(
            [
                [111, "event1", "2022-01-01 00:00:00"],
                [111, "event2", "2022-01-01 00:01:00"],
                [111, "event3", "2022-01-01 00:33:00"],
                [111, "event4", "2022-01-01 00:34:00"],
                [222, "event1", "2022-01-01 00:30:00"],
                [222, "event2", "2022-01-01 00:31:00"],
                [222, "event3", "2022-01-01 01:01:00"],
                [333, "event1", "2022-01-01 01:00:00"],
                [333, "event2", "2022-01-01 01:01:00"],
                [333, "event3", "2022-01-01 01:32:00"],
                [333, "event4", "2022-01-01 01:33:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "session_id"]
        correct_result = pd.DataFrame(
            [
                [111, "session_start", "session_start", "2022-01-01 00:00:00", "111_1"],
                [111, "session_start_truncated", "session_start_truncated", "2022-01-01 00:00:00", "111_1"],
                [111, "event1", "raw", "2022-01-01 00:00:00", "111_1"],
                [111, "event2", "raw", "2022-01-01 00:01:00", "111_1"],
                [111, "session_end", "session_end", "2022-01-01 00:01:00", "111_1"],
                [111, "session_start", "session_start", "2022-01-01 00:33:00", "111_2"],
                [111, "event3", "raw", "2022-01-01 00:33:00", "111_2"],
                [111, "event4", "raw", "2022-01-01 00:34:00", "111_2"],
                [111, "session_end", "session_end", "2022-01-01 00:34:00", "111_2"],
                [222, "session_start", "session_start", "2022-01-01 00:30:00", "222_1"],
                [222, "event1", "raw", "2022-01-01 00:30:00", "222_1"],
                [222, "event2", "raw", "2022-01-01 00:31:00", "222_1"],
                [222, "event3", "raw", "2022-01-01 01:01:00", "222_1"],
                [222, "session_end", "session_end", "2022-01-01 01:01:00", "222_1"],
                [333, "session_start", "session_start", "2022-01-01 01:00:00", "333_1"],
                [333, "event1", "raw", "2022-01-01 01:00:00", "333_1"],
                [333, "event2", "raw", "2022-01-01 01:01:00", "333_1"],
                [333, "session_end", "session_end", "2022-01-01 01:01:00", "333_1"],
                [333, "session_start", "session_start", "2022-01-01 01:32:00", "333_2"],
                [333, "event3", "raw", "2022-01-01 01:32:00", "333_2"],
                [333, "event4", "raw", "2022-01-01 01:33:00", "333_2"],
                [333, "session_end_truncated", "session_end_truncated", "2022-01-01 01:33:00", "333_2"],
                [333, "session_end", "session_end", "2022-01-01 01:33:00", "333_2"],
            ],
            columns=correct_result_columns,
        )

        stream = Eventstream(
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        res = (
            stream.split_sessions(session_cutoff=(30, "m"), session_col="session_id", mark_truncated=True)
            .to_dataframe()[correct_result_columns]
            .sort_values(["user_id", "event_timestamp"])
            .reset_index(drop=True)
        )

        assert res.compare(correct_result).shape == (0, 0)

    def test_params_model__incorrect_datetime_unit(self):

        with pytest.raises(ValidationError):
            source_df = pd.DataFrame(
                [
                    [111, "event1", "2022-01-01 00:00:00"],
                    [111, "event2", "2022-01-01 00:01:00"],
                    [111, "event3", "2022-01-01 00:33:00"],
                    [111, "event4", "2022-01-01 00:34:00"],
                    [222, "event1", "2022-01-01 00:30:00"],
                    [222, "event2", "2022-01-01 00:31:00"],
                    [222, "event3", "2022-01-01 01:01:00"],
                    [333, "event1", "2022-01-01 01:00:00"],
                    [333, "event2", "2022-01-01 01:01:00"],
                    [333, "event3", "2022-01-01 01:32:00"],
                    [333, "event4", "2022-01-01 01:33:00"],
                ],
                columns=["user_id", "event", "timestamp"],
            )
            correct_result_columns = ["user_id", "event_name", "event_type", "event_timestamp", "session_id"]

            stream = Eventstream(
                raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
                raw_data=source_df,
                schema=EventstreamSchema(),
            )

            res = (
                stream.split_sessions(session_cutoff=(30, "xxx"), session_col="session_id", mark_truncated=True)
                .to_dataframe()[correct_result_columns]
                .sort_values(["user_id", "event_timestamp"])
                .reset_index(drop=True)
            )
