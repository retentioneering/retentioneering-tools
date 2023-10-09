from __future__ import annotations

import pandas as pd

from retentioneering.eventstream.types import EventstreamType
from tests.eventstream.tooling.fixtures.describe_events import test_stream
from tests.eventstream.tooling.fixtures.describe_events_corr import (
    basic_corr,
    session_corr,
    session_raw_corr,
)


class TestEventstreamDescribeEvents:
    def test_describe_events_eventstream__basic(self, test_stream: EventstreamType, basic_corr: pd.DataFrame):
        result = test_stream.describe_events()
        expected_df = basic_corr
        assert pd.testing.assert_frame_equal(result, expected_df) is None

    def test_describe_events_eventstream__session(self, test_stream: EventstreamType, session_corr: pd.DataFrame):
        result = test_stream.split_sessions(timeout=(10, "m")).describe_events()
        expected_df = session_corr
        assert pd.testing.assert_frame_equal(result, expected_df) is None

    def test_describe_events_eventstream__event_list(self, test_stream: EventstreamType, session_corr: pd.DataFrame):
        result = test_stream.split_sessions(timeout=(10, "m")).describe_events(event_list=["event3"])
        expected_df = session_corr
        expected_df = expected_df[expected_df.index == "event3"]
        assert pd.testing.assert_frame_equal(result, expected_df) is None

    def test_describe_events_eventstream__raw_events_only(
        self, test_stream: EventstreamType, session_raw_corr: pd.DataFrame
    ):
        result = test_stream.split_sessions(timeout=(10, "m")).describe_events(raw_events_only=True)
        expected_df = session_raw_corr
        assert pd.testing.assert_frame_equal(result, expected_df) is None
