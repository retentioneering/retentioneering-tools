from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from src.eventstream.schema import RawDataSchema
from src.data_processors_lib.rete import (
    TruncatedEvents,
    TruncatedEventsParams,
)
from tests.data_processors_lib.common import (
    apply_processor,
    apply_processor_with_graph,
)


class TestTruncatedEvents:
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

    def _apply(self, params: TruncatedEventsParams) -> pd.DataFrame:
        original, actual = apply_processor(
            TruncatedEvents(params),
            self._source_df,
            raw_data_schema=self._raw_data_schema,
        )
        return actual

    def test_truncated_events_apply__left_right(self):
        actual = self._apply(TruncatedEventsParams(
            left_truncated_cutoff=(1, "h"),
            right_truncated_cutoff=(1, "h"),
        ))
        expected = pd.DataFrame(
            [
                [1, "truncated_left", "truncated_left", "2022-01-01 00:00:00"],
                [2, "truncated_left", "truncated_left", "2022-01-01 00:30:00"],
                [4, "truncated_right", "truncated_right", "2022-01-01 02:02:00"],
                [5, "truncated_right", "truncated_right", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncated_events_apply__left(self):
        actual = self._apply(TruncatedEventsParams(
            left_truncated_cutoff=(1, "h"),
        ))
        expected = pd.DataFrame(
            [
                [1, "truncated_left", "truncated_left", "2022-01-01 00:00:00"],
                [2, "truncated_left", "truncated_left", "2022-01-01 00:30:00"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncated_events_apply__right(self):
        actual = self._apply(TruncatedEventsParams(
            right_truncated_cutoff=(1, "h"),
        ))
        expected = pd.DataFrame(
            [
                [4, "truncated_right", "truncated_right", "2022-01-01 02:02:00"],
                [5, "truncated_right", "truncated_right", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_params_model__incorrect_datetime_unit(self):
        with pytest.raises(ValidationError):
            p = TruncatedEventsParams(left_truncated_cutoff=(1, "xxx"))


class TestTruncatedEventsGraph:
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

    def _apply(self, params: TruncatedEventsParams) -> pd.DataFrame:
        original, actual = apply_processor_with_graph(
            TruncatedEvents(params),
            self._source_df,
            raw_data_schema=self._raw_data_schema,
        )
        return actual

    def test_truncated_events_graph__left_right(self):
        actual = self._apply(TruncatedEventsParams(
            left_truncated_cutoff=(1, "h"),
            right_truncated_cutoff=(1, "h"),
        ))
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
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncated_events_graph__left(self):
        actual = self._apply(TruncatedEventsParams(
            left_truncated_cutoff=(1, "h"),
        ))
        expected = pd.DataFrame(
            [
                [1,  "truncated_left",  "truncated_left", "2022-01-01 00:00:00"],
                [1,          "event1",             "raw", "2022-01-01 00:00:00"],
                [2,  "truncated_left",  "truncated_left", "2022-01-01 00:30:00"],
                [2,          "event1",             "raw", "2022-01-01 00:30:00"],
                [2,          "event2",             "raw", "2022-01-01 00:31:00"],
                [3,          "event1",             "raw", "2022-01-01 01:00:01"],
                [3,          "event2",             "raw", "2022-01-01 01:00:02"],
                [4,          "event1",             "raw", "2022-01-01 02:01:00"],
                [4,          "event2",             "raw", "2022-01-01 02:02:00"],
                [5,          "event1",             "raw", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_truncated_events_graph__right(self):
        actual = self._apply(TruncatedEventsParams(
            right_truncated_cutoff=(1, "h"),
        ))
        expected = pd.DataFrame(
            [
                [1,           "event1",              "raw", "2022-01-01 00:00:00"],
                [2,           "event1",              "raw", "2022-01-01 00:30:00"],
                [2,           "event2",              "raw", "2022-01-01 00:31:00"],
                [3,           "event1",              "raw", "2022-01-01 01:00:01"],
                [3,           "event2",              "raw", "2022-01-01 01:00:02"],
                [4,           "event1",              "raw", "2022-01-01 02:01:00"],
                [4,           "event2",              "raw", "2022-01-01 02:02:00"],
                [4,  "truncated_right",  "truncated_right", "2022-01-01 02:02:00"],
                [5,           "event1",              "raw", "2022-01-01 03:00:00"],
                [5,  "truncated_right",  "truncated_right", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        print(actual[expected.columns].to_string())
        assert actual[expected.columns].compare(expected).shape == (0, 0)
