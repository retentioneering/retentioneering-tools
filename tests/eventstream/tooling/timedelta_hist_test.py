from __future__ import annotations

import numpy as np

from retentioneering.eventstream.types import EventstreamType
from tests.eventstream.tooling.fixtures.timedelta_hist import (
    source_stream_for_log_scale,
    source_stream_sessions,
    source_stream_start_end_events,
    test_stream,
)

FLOAT_PRECISION_BINS = 1


class TestEventstreamTimedeltaHist:
    def test_timedelta_hist_eventstream__default(self, test_stream: EventstreamType):
        correct_result = np.array([1.0, 1.0, 86398.0, 1.0, 86399.0, 1.0, 1.0, 86340.0, 86400.0])
        correct_bins = np.array([1.0, 17280.8, 34560.6, 51840.4, 69120.2, 86400.0])
        result = test_stream.timedelta_hist(show_plot=False, bins=5).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_timedelta_hist_eventstream__event_pair(self, test_stream: EventstreamType):
        correct_result = np.array([1.0, 1.0, 86340.0])
        correct_bins = np.array([1.0, 17268.8, 34536.6, 51804.4, 69072.2, 86340.0])
        result = test_stream.timedelta_hist(event_pair=["A", "B"], show_plot=False, bins=5).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_timedelta_hist_eventstream__adjacent_event_pairs(self, test_stream: EventstreamType):
        correct_result = np.array([1.0, 1.0, 2.0, 86340.0])
        correct_bins = np.array([1.0, 17268.8, 34536.6, 51804.4, 69072.2, 86340.0])
        result = test_stream.timedelta_hist(
            event_pair=["A", "B"], show_plot=False, only_adjacent_event_pairs=False, bins=5
        ).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_timedelta_hist_eventstream__timedelta_unit(self, test_stream: EventstreamType):
        correct_result = np.array([0.0167, 0.0167, 1439.0])
        correct_bins = np.array([0.0, 287.8, 575.6, 863.4, 1151.2, 1439.0])
        result = test_stream.timedelta_hist(event_pair=["A", "B"], show_plot=False, timedelta_unit="m", bins=5).values

        assert np.testing.assert_array_equal(result[0].round(4), correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_timedelta_hist_eventstream__lower_quantile(self, test_stream: EventstreamType):
        correct_result = np.array([86398.0, 86399.0, 86340.0, 86400.0])
        correct_bins = np.array([86340.0, 86352.0, 86364.0, 86376.0, 86388.0, 86400.0])
        result = test_stream.timedelta_hist(
            show_plot=False, timedelta_unit="s", lower_cutoff_quantile=0.52, bins=5
        ).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_timedelta_hist_eventstream__upper_quantile(self, test_stream: EventstreamType):
        correct_result = np.array([1.0, 1.0, 1.0, 1.0, 1.0])
        correct_bins = np.array([0.5, 0.7, 0.9, 1.1, 1.3, 1.5])
        result = test_stream.timedelta_hist(
            show_plot=False, timedelta_unit="s", upper_cutoff_quantile=0.52, bins=5
        ).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_timedelta_hist_eventstream__lower_upper_quantile(self, test_stream: EventstreamType):
        correct_result = np.array([86398.0, 86340.0])
        correct_bins = np.array([86340.0, 86351.6, 86363.2, 86374.8, 86386.4, 86398.0])
        result = test_stream.timedelta_hist(
            show_plot=False, timedelta_unit="s", lower_cutoff_quantile=0.52, upper_cutoff_quantile=0.8, bins=5
        ).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_timedelta_hist_eventstream__log_scale_x(self, source_stream_for_log_scale: EventstreamType):
        correct_result = np.array([1.0, 1.0, 86398.0, 1.0, 86399.0, 1.0, 1.0, 86400.0, 86400.0, 0.1, 0.1, 0.1])
        correct_bins = np.array([0.1, 1.5, 23.7, 364.7, 5613.2, 86400.0])
        result = source_stream_for_log_scale.timedelta_hist(
            show_plot=False, timedelta_unit="s", log_scale_x=True, bins=5
        ).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_timedelta_hist_eventstream__agg(self, test_stream: EventstreamType):
        correct_result = np.array([24686.0, 86370.0])
        correct_bins = np.array([24686.0, 37022.8, 49359.6, 61696.4, 74033.2, 86370.0])
        result = test_stream.timedelta_hist(show_plot=False, timedelta_unit="s", aggregation="mean", bins=5).values

        assert np.testing.assert_array_equal(result[0].round(2), correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_timedelta_hist_eventstream__es_start_path_end(self, source_stream_start_end_events: EventstreamType):
        correct_result = np.array([172802.0, 172800.0])
        correct_bins = np.array([172800.0, 172800.4, 172800.8, 172801.2, 172801.6, 172802.0])
        result = source_stream_start_end_events.timedelta_hist(
            event_pair=("eventstream_start", "path_end"),
            show_plot=False,
            timedelta_unit="s",
            only_adjacent_event_pairs=False,
            bins=5,
        ).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_timedelta_hist_eventstream__path_start_es_end(self, source_stream_start_end_events: EventstreamType):
        correct_result = np.array([])
        correct_bins = np.array([])
        result = source_stream_start_end_events.timedelta_hist(
            event_pair=("eventstream_end", "path_start"),
            show_plot=False,
            timedelta_unit="s",
            only_adjacent_event_pairs=False,
            bins=5,
        ).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_timedelta_hist_eventstream__path_end_es_end(self, source_stream_start_end_events: EventstreamType):
        correct_result = np.array([0.0, 2.0])
        correct_bins = np.array([0.0, 0.4, 0.8, 1.2, 1.6, 2.0])
        result = source_stream_start_end_events.timedelta_hist(
            event_pair=("path_end", "eventstream_end"),
            show_plot=False,
            timedelta_unit="s",
            only_adjacent_event_pairs=False,
            bins=5,
        ).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_timedelta_hist_eventstream__path_start_path_end(self, source_stream_start_end_events: EventstreamType):
        correct_result = np.array([])
        correct_bins = np.array([])
        result = source_stream_start_end_events.timedelta_hist(
            event_pair=("path_start", "path_end"),
            show_plot=False,
            timedelta_unit="s",
            only_adjacent_event_pairs=True,
            bins=5,
        ).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_timedelta_hist_eventstream__sessions(self, source_stream_sessions: EventstreamType):
        correct_result = np.array([1.0, 1.0, 2.0])
        correct_bins = np.array([1.0, 1.2, 1.4, 1.6, 1.8, 2.0])
        result = source_stream_sessions.timedelta_hist(
            event_pair=("A", "B"),
            show_plot=False,
            timedelta_unit="s",
            weight_col="session_id",
            only_adjacent_event_pairs=False,
            bins=5,
        ).values

        assert np.testing.assert_array_equal(result[0], correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"
