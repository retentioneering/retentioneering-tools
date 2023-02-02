from __future__ import annotations

import pandas as pd

from tests.eventstream.tooling.fixtures.describe import test_stream
from tests.eventstream.tooling.fixtures.describe_corr import (
    basic_corr,
    session_corr,
    session_raw_corr,
)


class TestEventstreamDescribe:
    def test_describe_eventstream__basic(self, test_stream, basic_corr):
        result = test_stream.describe()
        expected_df = basic_corr
        assert pd.testing.assert_frame_equal(result, expected_df) is None

    def test_describe_eventstream__session(self, test_stream, session_corr):
        result = test_stream.split_sessions(session_cutoff=(10, "m")).describe()
        expected_df = session_corr
        assert pd.testing.assert_frame_equal(result, expected_df) is None

    def test_describe_eventstream__raw_events_only(self, test_stream, session_raw_corr):
        result = test_stream.split_sessions(session_cutoff=(10, "m")).describe(raw_events_only=True)
        expected_df = session_raw_corr
        assert pd.testing.assert_frame_equal(result, expected_df) is None
