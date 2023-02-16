from __future__ import annotations

import pandas as pd

from retentioneering.data_processors_lib import NegativeTarget, NegativeTargetParams
from retentioneering.eventstream.schema import RawDataSchema
from retentioneering.eventstream.types import EventstreamType
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase
from tests.data_processors_lib.fixtures.negative_target_corr import (
    apply__custom_func_corr,
    apply__one_event_corr,
    apply__two_events_corr,
    graph__custom_func_corr,
    graph__one_event_corr,
    graph__two_events_corr,
)
from tests.data_processors_lib.fixtures.negative_target_input import test_stream


class TestNegativeTarget(ApplyTestBase):
    _Processor = NegativeTarget
    _source_df = pd.DataFrame(
        [
            [1, "path_start", "path_start", "2022-01-01 00:01:00"],
            [1, "event1", "raw", "2022-01-01 00:01:00"],
            [1, "event2", "raw", "2022-01-01 00:01:02"],
            [1, "event1", "raw", "2022-01-01 00:02:00"],
            [1, "event1", "raw", "2022-01-01 00:03:00"],
            [1, "event1", "synthetic", "2022-01-01 00:03:00"],
            [1, "session_start", "session_start", "2022-01-01 00:03:30"],
            [1, "event3", "raw", "2022-01-01 00:03:30"],
            [1, "event1", "raw", "2022-01-01 00:04:00"],
            [1, "event3", "raw", "2022-01-01 00:04:30"],
            [1, "event1", "raw", "2022-01-01 00:05:00"],
            [2, "event1", "raw", "2022-01-02 00:00:00"],
            [2, "event3", "raw", "2022-01-02 00:00:05"],
            [2, "event2", "raw", "2022-01-02 00:01:05"],
            [2, "path_end", "path_end", "2022-01-02 00:01:05"],
            [3, "event1", "raw", "2022-01-02 00:01:10"],
            [3, "event1", "raw", "2022-01-02 00:02:05"],
            [3, "event4", "raw", "2022-01-02 00:03:05"],
            [3, "path_end", "path_end", "2022-01-02 00:03:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_type="event_type",
        event_timestamp="timestamp",
    )

    def test_negative_target_apply__one_event(self, apply__one_event_corr):
        actual = self._apply(
            NegativeTargetParams(
                negative_target_events=["event3"],
            )
        )
        expected = apply__one_event_corr
        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None

    def test_negative_target_apply__two_events(self, apply__two_events_corr):
        actual = self._apply(
            NegativeTargetParams(
                negative_target_events=["event3", "event2"],
            )
        )
        expected = apply__two_events_corr
        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None

    def test_negative_target_apply__custom_func(self, apply__custom_func_corr):
        def custom_func(eventstream: EventstreamType, events: list[str]) -> pd.DataFrame:
            user_col = eventstream.schema.user_id
            time_col = eventstream.schema.event_timestamp
            event_col = eventstream.schema.event_name
            df = eventstream.to_dataframe()
            events_index_1 = df[df[event_col].isin(events)].groupby(user_col)[time_col].idxmin()  # type: ignore
            events_index_2 = df[df[event_col].isin(events)].groupby(user_col)[time_col].idxmax()  # type: ignore
            events_index = pd.concat([events_index_1, events_index_2], axis=0)

            return df.loc[events_index]

        actual = self._apply(NegativeTargetParams(negative_target_events=["event1"], func=custom_func))
        expected = apply__custom_func_corr

        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None


class TestNegativeTargetGraph(GraphTestBase):
    _Processor = NegativeTarget
    _source_df = pd.DataFrame(
        [
            [1, "path_start", "path_start", "2022-01-01 00:01:00"],
            [1, "event1", "raw", "2022-01-01 00:01:00"],
            [1, "event2", "raw", "2022-01-01 00:01:02"],
            [1, "event1", "raw", "2022-01-01 00:02:00"],
            [1, "event1", "raw", "2022-01-01 00:03:00"],
            [1, "event1", "synthetic", "2022-01-01 00:03:00"],
            [1, "session_start", "session_start", "2022-01-01 00:03:30"],
            [1, "event3", "raw", "2022-01-01 00:03:30"],
            [1, "event1", "raw", "2022-01-01 00:04:00"],
            [1, "event3", "raw", "2022-01-01 00:04:30"],
            [1, "event1", "raw", "2022-01-01 00:05:00"],
            [2, "event1", "raw", "2022-01-02 00:00:00"],
            [2, "event3", "raw", "2022-01-02 00:00:05"],
            [2, "event2", "raw", "2022-01-02 00:01:05"],
            [2, "path_end", "path_end", "2022-01-02 00:01:05"],
            [3, "event1", "raw", "2022-01-02 00:01:10"],
            [3, "event1", "raw", "2022-01-02 00:02:05"],
            [3, "event4", "raw", "2022-01-02 00:03:05"],
            [3, "path_end", "path_end", "2022-01-02 00:03:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_type="event_type",
        event_timestamp="timestamp",
    )

    def test_negative_target_graph__one_event(self, graph__one_event_corr):
        actual = self._apply(
            NegativeTargetParams(
                negative_target_events=["event3"],
            )
        )
        expected = graph__one_event_corr
        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None

    def test_negative_target_graph__two_events(self, graph__two_events_corr):
        actual = self._apply(
            NegativeTargetParams(
                negative_target_events=["event3", "event2"],
            )
        )
        expected = graph__two_events_corr
        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None

    def test_negative_target_graph__custom_func(self, graph__custom_func_corr):
        def custom_func(eventstream: EventstreamType, events: list[str]) -> pd.DataFrame:
            user_col = eventstream.schema.user_id
            time_col = eventstream.schema.event_timestamp
            event_col = eventstream.schema.event_name
            df = eventstream.to_dataframe()
            events_index_1 = df[df[event_col].isin(events)].groupby(user_col)[time_col].idxmin()  # type: ignore
            events_index_2 = df[df[event_col].isin(events)].groupby(user_col)[time_col].idxmax()  # type: ignore
            events_index = pd.concat([events_index_1, events_index_2], axis=0)

            return df.loc[events_index]

        expected = graph__custom_func_corr
        actual = self._apply(NegativeTargetParams(negative_target_events=["event1"], func=custom_func))

        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None


class TestNegativeTargetHelper:
    def test_negative_target_graph__one_event(self, test_stream, graph__one_event_corr):
        source = test_stream
        expected = graph__one_event_corr
        actual = source.negative_target(negative_target_events=["event3"])
        actual = actual.to_dataframe()[expected.columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(actual, expected) is None

    def test_negative_target_graph__two_events(self, test_stream, graph__two_events_corr):
        source = test_stream
        expected = graph__two_events_corr

        actual = source.negative_target(negative_target_events=["event3", "event2"])
        actual = actual.to_dataframe()[expected.columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(actual, expected) is None

    def test_negative_target_graph__custom_func(self, test_stream, graph__custom_func_corr):
        def custom_func(eventstream: EventstreamType, events: list[str]) -> pd.DataFrame:
            user_col = eventstream.schema.user_id
            time_col = eventstream.schema.event_timestamp
            event_col = eventstream.schema.event_name
            df = eventstream.to_dataframe()
            events_index_1 = df[df[event_col].isin(events)].groupby(user_col)[time_col].idxmin()  # type: ignore
            events_index_2 = df[df[event_col].isin(events)].groupby(user_col)[time_col].idxmax()  # type: ignore
            events_index = pd.concat([events_index_1, events_index_2], axis=0)
            result = df.loc[events_index]
            return result

        source = test_stream
        expected = graph__custom_func_corr

        actual = source.negative_target(negative_target_events=["event1"], func=custom_func)
        actual = actual.to_dataframe()[expected.columns].reset_index(drop=True)

        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None
