from __future__ import annotations

import numpy as np

from tests.eventstream.tooling.fixtures.event_timestamp_hist import test_stream
from tests.eventstream.tooling.fixtures.event_timestamp_hist_corr import (
    correct_basic,
    correct_lower_quantile,
    correct_raw_events_only,
    correct_upper_lower_quantile,
    correct_upper_quantile,
)


class TestEventstreamEventTimestampHist:
    def test_event_timestamp_hist_eventstream__default(self, test_stream, correct_basic):
        correct_result = correct_basic

        correct_bins = np.array(
            ["2023-01-01 00:00:00", "2023-01-01 16:00:01", "2023-01-02 08:00:01", "2023-01-03 00:00:02"],
            dtype="datetime64[ns]",
        )
        result = test_stream.event_timestamp_hist(bins=3, show_plot=False).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1], correct_bins) is None, "bins"

    def test_event_timestamp_hist_eventstream__lower_cutoff_quantile(self, test_stream, correct_lower_quantile):
        correct_result = correct_lower_quantile

        correct_bins = np.array(
            ["2023-01-03 00:00:00", "2023-01-03 00:00:01", "2023-01-03 00:00:02"], dtype="datetime64[ns]"
        )
        result = test_stream.event_timestamp_hist(show_plot=False, bins=2, lower_cutoff_quantile=0.5).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1], correct_bins) is None, "bins"

    def test_event_timestamp_hist_eventstream__upper_cutoff_quantile(self, test_stream, correct_upper_quantile):
        correct_result = correct_upper_quantile

        correct_bins = np.array(
            ["2023-01-01 00:00:00", "2023-01-01 12:00:00", "2023-01-02 00:00:01"], dtype="datetime64[ns]"
        )
        result = test_stream.event_timestamp_hist(show_plot=False, bins=2, upper_cutoff_quantile=0.5).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1], correct_bins) is None, "bins"

    def test_event_timestamp_hist_eventstream__upper_lower_cutoff_quantile(
        self, test_stream, correct_upper_lower_quantile
    ):
        correct_result = correct_upper_lower_quantile

        correct_bins = np.array(
            ["2023-01-01 00:00:02", "2023-01-02 00:00:01", "2023-01-03 00:00:00"], dtype="datetime64[ns]"
        )
        result = test_stream.event_timestamp_hist(
            show_plot=False, bins=2, upper_cutoff_quantile=0.8, lower_cutoff_quantile=0.2
        ).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1], correct_bins) is None, "bins"

    def test_event_timestamp_hist_eventstream__raw_events(self, test_stream, correct_raw_events_only):
        correct_result = correct_raw_events_only

        correct_bins = np.array(
            ["2023-01-01 00:00:00", "2023-01-01 16:00:01", "2023-01-02 08:00:01", "2023-01-03 00:00:02"],
            dtype="datetime64[ns]",
        )

        result = test_stream.event_timestamp_hist(show_plot=False, bins=3, raw_events_only=False).values
        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1], correct_bins) is None, "bins"
