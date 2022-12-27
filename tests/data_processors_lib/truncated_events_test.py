from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from src.data_processors_lib import TruncatedEvents, TruncatedEventsParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestTruncatedEvents(ApplyTestBase):
    _Processor = TruncatedEvents
    _source_df = pd.DataFrame(
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
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_truncated_events_apply__left_right(self):
        actual = self._apply(
            TruncatedEventsParams(
                left_truncated_cutoff=(1, "h"),
                right_truncated_cutoff=(1, "h"),
            )
        )
        expected = pd.DataFrame(
            [
                [1, "truncated_left", "truncated_left", "2022-01-01 00:00:00"],
                [2, "truncated_left", "truncated_left", "2022-01-01 00:30:00"],
                [4, "truncated_right", "truncated_right", "2022-01-01 02:02:00"],
                [5, "truncated_right", "truncated_right", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncated_events_apply__left(self):
        actual = self._apply(
            TruncatedEventsParams(
                left_truncated_cutoff=(1, "h"),
            )
        )
        expected = pd.DataFrame(
            [
                [1, "truncated_left", "truncated_left", "2022-01-01 00:00:00"],
                [2, "truncated_left", "truncated_left", "2022-01-01 00:30:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncated_events_apply__right(self):
        actual = self._apply(
            TruncatedEventsParams(
                right_truncated_cutoff=(1, "h"),
            )
        )
        expected = pd.DataFrame(
            [
                [4, "truncated_right", "truncated_right", "2022-01-01 02:02:00"],
                [5, "truncated_right", "truncated_right", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_params_model__incorrect_datetime_unit(self):
        with pytest.raises(ValidationError):
            p = TruncatedEventsParams(left_truncated_cutoff=(1, "xxx"))


class TestTruncatedEventsGraph(GraphTestBase):
    _Processor = TruncatedEvents
    _source_df = pd.DataFrame(
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
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_truncated_events_graph__left_right(self):
        actual = self._apply(
            TruncatedEventsParams(
                left_truncated_cutoff=(1, "h"),
                right_truncated_cutoff=(1, "h"),
            )
        )
        expected = pd.DataFrame(
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
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncated_events_graph__left(self):
        actual = self._apply(
            TruncatedEventsParams(
                left_truncated_cutoff=(1, "h"),
            )
        )
        expected = pd.DataFrame(
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
                [5, "event1", "raw", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncated_events_graph__right(self):
        actual = self._apply(
            TruncatedEventsParams(
                right_truncated_cutoff=(1, "h"),
            )
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:00:00"],
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
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


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

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
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

        stream = Eventstream(source_df)

        res = stream.truncated_events(left_truncated_cutoff=(1, "h"), right_truncated_cutoff=(1, "h")).to_dataframe()[
            correct_result_columns
        ]

        assert res.compare(correct_result).shape == (0, 0)
