from __future__ import annotations

import numpy as np

from retentioneering.tooling.event_timestamp_hist.event_timestamp_hist import (
    EventTimestampHist,
)
from tests.tooling.fixtures.event_timestamp_hist import test_stream
from tests.tooling.fixtures.event_timestamp_hist_corr import (
    correct_basic,
    correct_lower_quantile,
    correct_raw_events_only,
    correct_upper_lower_quantile,
    correct_upper_quantile,
)


class TestEventTimestampHist:
    def test_event_timestamp_hist__default(self, test_stream, correct_basic):
        correct_result = correct_basic

        correct_bins = np.array(
            ["2023-01-01 00:00:00", "2023-01-01 16:00:01", "2023-01-02 08:00:01", "2023-01-03 00:00:02"],
            dtype="datetime64[ns]",
        )

        et = EventTimestampHist(test_stream, bins=3)
        result = et.values
        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1], correct_bins) is None, "bins"

    def test_event_timestamp_hist__lower_cutoff_quantile(self, test_stream, correct_lower_quantile):
        correct_result = correct_lower_quantile

        correct_bins = np.array(
            ["2023-01-03 00:00:00", "2023-01-03 00:00:01", "2023-01-03 00:00:02"], dtype="datetime64[ns]"
        )

        et = EventTimestampHist(test_stream, bins=2, lower_cutoff_quantile=0.5)
        result = et.values
        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1], correct_bins) is None, "bins"

    def test_event_timestamp_hist__upper_cutoff_quantile(self, test_stream, correct_upper_quantile):
        correct_result = correct_upper_quantile

        correct_bins = np.array(
            ["2023-01-01 00:00:00", "2023-01-01 12:00:00", "2023-01-02 00:00:01"], dtype="datetime64[ns]"
        )

        et = EventTimestampHist(test_stream, bins=2, upper_cutoff_quantile=0.5)
        result = et.values
        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1], correct_bins) is None, "bins"

    def test_event_timestamp_hist__upper_lower_cutoff_quantile(self, test_stream, correct_upper_lower_quantile):
        correct_result = correct_upper_lower_quantile

        correct_bins = np.array(
            ["2023-01-01 00:00:02", "2023-01-02 00:00:01", "2023-01-03 00:00:00"], dtype="datetime64[ns]"
        )

        et = EventTimestampHist(test_stream, bins=2, upper_cutoff_quantile=0.8, lower_cutoff_quantile=0.2)
        result = et.values
        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1], correct_bins) is None, "bins"

    def test_event_timestamp_hist__raw_events(self, test_stream, correct_raw_events_only):
        correct_result = correct_raw_events_only

        correct_bins = np.array(
            ["2023-01-01 00:00:00", "2023-01-01 16:00:01", "2023-01-02 08:00:01", "2023-01-03 00:00:02"],
            dtype="datetime64[ns]",
        )

        et = EventTimestampHist(test_stream, bins=3, raw_events_only=False)
        result = et.values
        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1], correct_bins) is None, "bins"
