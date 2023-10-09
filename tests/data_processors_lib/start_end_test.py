from __future__ import annotations

import pandas as pd

from retentioneering.data_processors_lib import (
    AddStartEndEvents,
    AddStartEndEventsParams,
)
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestAddStartEndEvents(ApplyTestBase):
    _Processor = AddStartEndEvents
    _source_df = pd.DataFrame(
        [
            [1, "pageview", "raw", "2021-10-26 12:00"],
            [1, "cart_btn_click", "raw", "2021-10-26 12:02"],
            [1, "plus_icon_click", "raw", "2021-10-26 12:04"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_type="event_type",
        event_timestamp="timestamp",
    )

    def test_add_start_end_events__apply(self):
        actual = self._apply_dataprocessor(
            params=AddStartEndEventsParams(),
        )
        expected = pd.DataFrame(
            [
                [1, "path_start", "path_start", "2021-10-26 12:00:00"],
                [1, "pageview", "raw", "2021-10-26 12:00"],
                [1, "cart_btn_click", "raw", "2021-10-26 12:02"],
                [1, "plus_icon_click", "raw", "2021-10-26 12:04"],
                [1, "path_end", "path_end", "2021-10-26 12:04:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestAddStartEndEventsGraph(GraphTestBase):
    _Processor = AddStartEndEvents
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

    def test_add_start_end_events__graph(self):
        actual = self._apply(AddStartEndEventsParams())
        expected = pd.DataFrame(
            [
                [1, "path_start", "path_start", "2022-01-01 00:00:00"],
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [1, "event2", "raw", "2022-01-01 00:00:01"],
                [1, "event3", "raw", "2022-01-01 00:00:02"],
                [1, "path_end", "path_end", "2022-01-01 00:00:02"],
                [2, "path_start", "path_start", "2022-01-02 00:00:00"],
                [2, "event4", "raw", "2022-01-02 00:00:00"],
                [2, "path_end", "path_end", "2022-01-02 00:00:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestAddStartEndEventsHelper:
    def test_add_start_end_events_helper(self) -> None:
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:00:00"],
                [1, "event2", "2022-01-01 00:00:01"],
                [1, "event3", "2022-01-01 00:00:02"],
                [2, "event4", "2022-01-02 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = pd.DataFrame(
            [
                [1, "path_start", "path_start", "2022-01-01 00:00:00"],
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [1, "event2", "raw", "2022-01-01 00:00:01"],
                [1, "event3", "raw", "2022-01-01 00:00:02"],
                [1, "path_end", "path_end", "2022-01-01 00:00:02"],
                [2, "path_start", "path_start", "2022-01-02 00:00:00"],
                [2, "event4", "raw", "2022-01-02 00:00:00"],
                [2, "path_end", "path_end", "2022-01-02 00:00:00"],
            ],
            columns=correct_result_columns,
        )

        stream = Eventstream(source_df)

        result = stream.add_start_end_events()
        result_df = result.to_dataframe()[correct_result_columns]
        assert result_df.compare(correct_result).shape == (0, 0)
