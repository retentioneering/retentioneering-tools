from __future__ import annotations

import numpy as np

from retentioneering.eventstream.types import EventstreamType
from tests.eventstream.tooling.fixtures.event_timestamp_hist import test_stream
from tests.eventstream.tooling.fixtures.event_timestamp_hist_corr import (
    correct_basic,
    correct_basic_bins,
    correct_lower_quantile,
    correct_lower_quantile_bins,
    correct_raw_events_only,
    correct_raw_events_only_bins,
    correct_upper_lower_quantile,
    correct_upper_lower_quantile_bins,
    correct_upper_quantile,
    correct_upper_quantile_bins,
)


class TestEventstreamEventTimestampHist:
    def test_event_timestamp_hist_eventstream__basic(
        self, test_stream: EventstreamType, correct_basic: np.array, correct_basic_bins: np.array
    ):
        correct_result = correct_basic
        result = test_stream.event_timestamp_hist(bins=3, show_plot=False).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "incorrect histogram values"
        assert np.testing.assert_array_equal(result[1], correct_basic_bins) is None, "incorrect histogram bins"

    def test_event_timestamp_hist_eventstream__lower_cutoff_quantile(
        self, test_stream: EventstreamType, correct_lower_quantile: np.array, correct_lower_quantile_bins: np.array
    ):
        correct_result = correct_lower_quantile

        result = test_stream.event_timestamp_hist(show_plot=False, bins=2, lower_cutoff_quantile=0.5).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "incorrect histogram values"
        assert np.testing.assert_array_equal(result[1], correct_lower_quantile_bins) is None, "incorrect histogram bins"

    def test_event_timestamp_hist_eventstream__upper_cutoff_quantile(
        self, test_stream: EventstreamType, correct_upper_quantile: np.array, correct_upper_quantile_bins: np.array
    ):
        correct_result = correct_upper_quantile

        result = test_stream.event_timestamp_hist(show_plot=False, bins=2, upper_cutoff_quantile=0.5).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "incorrect histogram values"
        assert np.testing.assert_array_equal(result[1], correct_upper_quantile_bins) is None, "incorrect histogram bins"

    def test_event_timestamp_hist_eventstream__upper_lower_cutoff_quantile(
        self,
        test_stream: EventstreamType,
        correct_upper_lower_quantile: np.array,
        correct_upper_lower_quantile_bins: np.array,
    ):
        correct_result = correct_upper_lower_quantile
        result = test_stream.event_timestamp_hist(
            show_plot=False, bins=2, upper_cutoff_quantile=0.8, lower_cutoff_quantile=0.2
        ).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result[1], correct_upper_lower_quantile_bins) is None
        ), "incorrect histogram bins"

    def test_event_timestamp_hist_eventstream__raw_events(
        self, test_stream: EventstreamType, correct_raw_events_only: np.array, correct_raw_events_only_bins: np.array
    ):
        correct_result = correct_raw_events_only

        result = test_stream.event_timestamp_hist(show_plot=False, bins=3, raw_events_only=False).values
        assert np.testing.assert_array_equal(result[0], correct_result) is None, "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result[1], correct_raw_events_only_bins) is None
        ), "incorrect histogram bins"
