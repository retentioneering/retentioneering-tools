from __future__ import annotations

import pandas as pd

from retentioneering.tooling._describe_events import _DescribeEvents
from tests.tooling.fixtures.describe_events import test_stream
from tests.tooling.fixtures.describe_events_corr import basic_corr, session_corr


class TestDescribeEvents:
    def test_describe_events__basic(self, test_stream, basic_corr):
        de = _DescribeEvents(test_stream)
        result = de._values()
        expected_df = basic_corr
        assert pd.testing.assert_frame_equal(result, expected_df) is None

    def test_describe_events__session(self, test_stream, session_corr):
        de = _DescribeEvents(test_stream.split_sessions(timeout=(10, "m")))
        result = de._values()
        expected_df = session_corr
        assert pd.testing.assert_frame_equal(result, expected_df) is None

    def test_describe_events__event_list(self, test_stream, session_corr):
        de = _DescribeEvents(test_stream.split_sessions(timeout=(10, "m")), event_list=["event3"])
        result = de._values()
        expected_df = session_corr
        expected_df = expected_df[expected_df.index == "event3"]
        assert pd.testing.assert_frame_equal(result, expected_df) is None
