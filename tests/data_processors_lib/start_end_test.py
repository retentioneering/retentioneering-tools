from __future__ import annotations

import pandas as pd

from src.data_processors_lib.rete import StartEndEvents, StartEndEventsParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestStartEndEvents(ApplyTestBase):
    _Processor = StartEndEvents
    _source_df = pd.DataFrame(
        [
            [1, "pageview", "raw", "2021-10-26 12:00"],
            [1, "cart_btn_click", "raw", "2021-10-26 12:02"],
            [1, "plus_icon_click", "raw", "2021-10-26 12:04"],
        ],
        columns=["user_id", "event_name", "event_type", "event_timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event_name",
        event_type="event_type",
        event_timestamp="event_timestamp",
    )

    def test_start_end__apply(self):
        actual = self._apply(StartEndEventsParams())
        expected = pd.DataFrame(
            [
                [1, "start", "start", "2021-10-26 12:00:00"],
                [1, "end", "end", "2021-10-26 12:04:00"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestStartEndEventsGraph(GraphTestBase):
    _Processor = StartEndEvents
    _source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:00:00"],
            [1, "event2", "2022-01-01 00:00:01"],
            [1, "event3", "2022-01-01 00:00:02"],
            [2, "event4", "2022-01-02 00:00:00"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_start_end__graph(self):
        actual = self._apply(StartEndEventsParams())
        expected = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:00:00"],
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [1, "event2", "raw", "2022-01-01 00:00:01"],
                [1, "event3", "raw", "2022-01-01 00:00:02"],
                [1, "end", "end", "2022-01-01 00:00:02"],
                [2, "start", "start", "2022-01-02 00:00:00"],
                [2, "event4", "raw", "2022-01-02 00:00:00"],
                [2, "end", "end", "2022-01-02 00:00:00"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_start_end_events_helper(self) -> None:
        correct_result = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:00:00"],
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [1, "event2", "raw", "2022-01-01 00:00:01"],
                [1, "event3", "raw", "2022-01-01 00:00:02"],
                [1, "end", "end", "2022-01-01 00:00:02"],
                [2, "start", "start", "2022-01-02 00:00:00"],
                [2, "event4", "raw", "2022-01-02 00:00:00"],
                [2, "end", "end", "2022-01-02 00:00:00"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        stream = Eventstream(
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            raw_data=self._source_df,
            schema=EventstreamSchema(),
        )
        result = stream.add_start_end()
        result_df = result.to_dataframe()[correct_result.columns]
        assert result_df.compare(correct_result).shape == (0, 0)
