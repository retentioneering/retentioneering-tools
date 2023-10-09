from __future__ import annotations

import pandas as pd

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling._describe import _Describe
from tests.tooling.fixtures.describe import test_stream
from tests.tooling.fixtures.describe_corr import basic_corr, session_corr


class TestDescribe:
    def test_describe__basic(self, test_stream: EventstreamType, basic_corr: pd.DataFrame):
        de = _Describe(test_stream)
        result = de._values()
        expected_df = basic_corr
        assert pd.testing.assert_frame_equal(result, expected_df) is None

    def test_describe__session(self, test_stream: EventstreamType, session_corr: pd.DataFrame):
        de = _Describe(test_stream.split_sessions(timeout=(10, "m")))
        result = de._values()
        expected_df = session_corr
        assert pd.testing.assert_frame_equal(result, expected_df) is None
