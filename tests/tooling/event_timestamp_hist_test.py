from __future__ import annotations

import numpy as np

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.event_timestamp_hist.event_timestamp_hist import (
    EventTimestampHist,
)
from tests.tooling.fixtures.event_timestamp_hist import test_stream
from tests.tooling.fixtures.event_timestamp_hist_corr import (
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


class TestEventTimestampHist:
    def test_event_timestamp_hist__basic(
        self, test_stream: EventstreamType, correct_basic: np.array, correct_basic_bins: np.array
    ):
        correct_result = correct_basic

        et = EventTimestampHist(test_stream)
        et.fit(raw_events_only=True, bins=3)
        result_values, result_bins = et.values
        assert np.testing.assert_array_equal(result_values, correct_result) is None, "incorrect histogram values"
        assert np.testing.assert_array_equal(result_bins, correct_basic_bins) is None, "incorrect histogram bins"

    def test_event_timestamp_hist__lower_cutoff_quantile(
        self, test_stream: EventstreamType, correct_lower_quantile: np.array, correct_lower_quantile_bins: np.array
    ):
        correct_result = correct_lower_quantile

        et = EventTimestampHist(test_stream)
        et.fit(raw_events_only=True, bins=2, lower_cutoff_quantile=0.5)
        result_values, result_bins = et.values
        assert np.testing.assert_array_equal(result_values, correct_result) is None, "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins, correct_lower_quantile_bins) is None
        ), "incorrect histogram bins"

    def test_event_timestamp_hist__upper_cutoff_quantile(
        self, test_stream: EventstreamType, correct_upper_quantile: np.array, correct_upper_quantile_bins: np.array
    ):
        correct_result = correct_upper_quantile

        et = EventTimestampHist(test_stream)
        et.fit(raw_events_only=True, bins=2, upper_cutoff_quantile=0.5)
        result_values, result_bins = et.values
        assert np.testing.assert_array_equal(result_values, correct_result) is None, "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins, correct_upper_quantile_bins) is None
        ), "incorrect histogram bins"

    def test_event_timestamp_hist__upper_lower_cutoff_quantile(
        self,
        test_stream: EventstreamType,
        correct_upper_lower_quantile: np.array,
        correct_upper_lower_quantile_bins: np.array,
    ):
        correct_result = correct_upper_lower_quantile

        et = EventTimestampHist(test_stream)
        et.fit(raw_events_only=True, bins=2, upper_cutoff_quantile=0.8, lower_cutoff_quantile=0.2)
        result_values, result_bins = et.values
        assert np.testing.assert_array_equal(result_values, correct_result) is None, "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins, correct_upper_lower_quantile_bins) is None
        ), "incorrect histogram bins"

    def test_event_timestamp_hist__raw_events(
        self, test_stream: EventstreamType, correct_raw_events_only: np.array, correct_raw_events_only_bins: np.array
    ):
        correct_result = correct_raw_events_only

        et = EventTimestampHist(test_stream)
        et.fit(raw_events_only=False, bins=3)
        result_values, result_bins = et.values
        assert np.testing.assert_array_equal(result_values, correct_result) is None, "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins, correct_raw_events_only_bins) is None
        ), "incorrect histogram bins"
