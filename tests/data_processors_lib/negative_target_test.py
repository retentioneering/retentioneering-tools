from __future__ import annotations

import pandas as pd

from src.eventstream.schema import RawDataSchema
from src.data_processors_lib.rete import (
    NegativeTarget,
    NegativeTargetParams,
)
from tests.data_processors_lib.common import (
    apply_processor,
    apply_processor_with_graph,
)


class TestNegativeTarget:
    _source_df = pd.DataFrame(
        [
            [1, "start", "start", "2022-01-01 00:01:00"],
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
            [2, "end", "end", "2022-01-02 00:01:05"],
            [3, "event1", "raw", "2022-01-02 00:01:10"],
            [3, "event1", "raw", "2022-01-02 00:02:05"],
            [3, "event4", "raw", "2022-01-02 00:03:05"],
            [3, "end", "end", "2022-01-02 00:03:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_type="event_type",
        event_timestamp="timestamp",
    )

    def _apply(self, params: NegativeTargetParams) -> pd.DataFrame:
        original, actual = apply_processor(
            NegativeTarget(params),
            self._source_df,
            raw_data_schema=self._raw_data_schema,
        )
        return actual

    def test_negative_target_apply__1_event(self):
        actual = self._apply(NegativeTargetParams(
            negative_target_events=["event3"],
        ))
        expected = pd.DataFrame(
            [
                [1, "negative_target_event3", "negative_target", "2022-01-01 00:03:30"],
                [2, "negative_target_event3", "negative_target", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_negative_target_apply__2_events(self):
        actual = self._apply(NegativeTargetParams(
            negative_target_events=["event3", "event2"],
        ))
        expected = pd.DataFrame(
            [
                [1, "negative_target_event2", "negative_target", "2022-01-01 00:01:02"],
                [2, "negative_target_event3", "negative_target", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestNegativeTargetGraph:
    _source_df = pd.DataFrame(
        [
            [1, "start", "start", "2022-01-01 00:01:00"],
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
            [2, "end", "end", "2022-01-02 00:01:05"],
            [3, "event1", "raw", "2022-01-02 00:01:10"],
            [3, "event1", "raw", "2022-01-02 00:02:05"],
            [3, "event4", "raw", "2022-01-02 00:03:05"],
            [3, "end", "end", "2022-01-02 00:03:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_type="event_type",
        event_timestamp="timestamp",
    )

    def _apply(self, params: NegativeTargetParams) -> pd.DataFrame:
        original, actual = apply_processor_with_graph(
            NegativeTarget(params),
            self._source_df,
            raw_data_schema=self._raw_data_schema,
        )
        return actual

    def test_negative_target_graph__1_event(self):
        actual = self._apply(NegativeTargetParams(
            negative_target_events=["event3"],
        ))
        expected = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "synthetic", "2022-01-01 00:03:00"],
                [1, "session_start", "session_start", "2022-01-01 00:03:30"],
                [1, "event3", "raw", "2022-01-01 00:03:30"],
                [1, "negative_target_event3", "negative_target", "2022-01-01 00:03:30"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event3", "raw", "2022-01-01 00:04:30"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event3", "raw", "2022-01-02 00:00:05"],
                [2, "negative_target_event3", "negative_target", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_negative_target_graph__2_events(self):
        actual = self._apply(NegativeTargetParams(
            negative_target_events=["event3", "event2"],
        ))
        expected = pd.DataFrame(
            [
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "negative_target_event2", "negative_target", "2022-01-01 00:01:02"],
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
                [2, "negative_target_event3", "negative_target", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
                [2, "end", "end", "2022-01-02 00:01:05"],
                [3, "event1", "raw", "2022-01-02 00:01:10"],
                [3, "event1", "raw", "2022-01-02 00:02:05"],
                [3, "event4", "raw", "2022-01-02 00:03:05"],
                [3, "end", "end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)
