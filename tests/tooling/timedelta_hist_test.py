from __future__ import annotations

import numpy as np

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.timedelta_hist import TimedeltaHist
from tests.tooling.fixtures.timedelta_hist import (
    source_stream_for_log_scale,
    source_stream_sessions,
    source_stream_start_end_events,
    test_stream,
)
from tests.tooling.fixtures.timedelta_hist_corr import (
    corr_adjacent_event_pairs,
    corr_adjacent_event_pairs_bins,
    corr_agg,
    corr_agg_bins,
    corr_default,
    corr_default_bins,
    corr_es_start_path_end,
    corr_es_start_path_end_bins,
    corr_event_pair,
    corr_event_pair_bins,
    corr_log_scale_x,
    corr_log_scale_x_bins,
    corr_lower_quantile,
    corr_lower_quantile_bins,
    corr_lower_upper_quantile,
    corr_lower_upper_quantile_bins,
    corr_path_end_es_end,
    corr_path_end_es_end_bins,
    corr_path_start_es_end,
    corr_path_start_es_end_bins,
    corr_path_start_path_end,
    corr_path_start_path_end_bins,
    corr_sessions,
    corr_sessions_bins,
    corr_timedelta_unit,
    corr_timedelta_unit_bins,
    corr_upper_quantile,
    corr_upper_quantile_bins,
)

FLOAT_PRECISION_BINS = 1


class TestTimedeltaHist:
    def test_timedelta_hist__default(
        self, test_stream: EventstreamType, corr_default: np.array, corr_default_bins: np.array
    ):
        th = TimedeltaHist(test_stream, bins=5)
        th._calculate()
        result_values, result_bins = th.values

        result_bins = result_bins.round(FLOAT_PRECISION_BINS)

        assert np.testing.assert_array_equal(result_values, corr_default) is None, "incorrect histogram values"
        assert np.testing.assert_array_equal(result_bins, corr_default_bins) is None, "incorrect histogram bins"

    def test_timedelta_hist__event_pair(
        self, test_stream: EventstreamType, corr_event_pair: np.array, corr_event_pair_bins: np.array
    ):
        th = TimedeltaHist(test_stream, event_pair=["A", "B"], bins=5)
        th._calculate()
        result_values, result_bins = th.values

        result_bins = result_bins.round(FLOAT_PRECISION_BINS)
        assert np.testing.assert_array_equal(result_values, corr_event_pair) is None, "incorrect histogram values"
        assert np.testing.assert_array_equal(result_bins, corr_event_pair_bins) is None, "incorrect histogram bins"

    def test_timedelta_hist__adjacent_event_pairs(
        self,
        test_stream: EventstreamType,
        corr_adjacent_event_pairs: np.array,
        corr_adjacent_event_pairs_bins: np.array,
    ):

        th = TimedeltaHist(test_stream, event_pair=["A", "B"], only_adjacent_event_pairs=False, bins=5)
        th._calculate()
        result_values, result_bins = th.values

        result_bins = result_bins.round(FLOAT_PRECISION_BINS)
        assert (
            np.testing.assert_array_equal(result_values, corr_adjacent_event_pairs) is None
        ), "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins, corr_adjacent_event_pairs_bins) is None
        ), "incorrect histogram bins"

    def test_timedelta_hist__timedelta_unit(
        self, test_stream: EventstreamType, corr_timedelta_unit: np.array, corr_timedelta_unit_bins: np.array
    ):

        th = TimedeltaHist(test_stream, event_pair=["A", "B"], timedelta_unit="m", bins=5)
        th._calculate()
        result_values, result_bins = th.values
        result_values = result_values.round(4)
        result_bins = result_bins.round(FLOAT_PRECISION_BINS)
        assert np.testing.assert_array_equal(result_values, corr_timedelta_unit) is None, "incorrect histogram values"
        assert np.testing.assert_array_equal(result_bins, corr_timedelta_unit_bins) is None, "incorrect histogram bins"

    def test_timedelta_hist__lower_quantile(
        self, test_stream: EventstreamType, corr_lower_quantile: np.array, corr_lower_quantile_bins: np.array
    ):

        th = TimedeltaHist(test_stream, timedelta_unit="s", lower_cutoff_quantile=0.52, bins=5)
        th._calculate()
        result_values, result_bins = th.values

        result_bins = result_bins.round(FLOAT_PRECISION_BINS)
        assert np.testing.assert_array_equal(result_values, corr_lower_quantile) is None, "incorrect histogram values"
        assert np.testing.assert_array_equal(result_bins, corr_lower_quantile_bins) is None, "incorrect histogram bins"

    def test_timedelta_hist__upper_quantile(
        self, test_stream: EventstreamType, corr_upper_quantile: np.array, corr_upper_quantile_bins: np.array
    ):

        th = TimedeltaHist(test_stream, timedelta_unit="s", upper_cutoff_quantile=0.52, bins=5)
        th._calculate()
        result_values, result_bins = th.values

        result_bins = result_bins.round(FLOAT_PRECISION_BINS)
        assert np.testing.assert_array_equal(result_values, corr_upper_quantile) is None, "incorrect histogram values"
        assert np.testing.assert_array_equal(result_bins, corr_upper_quantile_bins) is None, "incorrect histogram bins"

    def test_timedelta_hist__lower_upper_quantile(
        self,
        test_stream: EventstreamType,
        corr_lower_upper_quantile: np.array,
        corr_lower_upper_quantile_bins: np.array,
    ):

        th = TimedeltaHist(
            test_stream, timedelta_unit="s", lower_cutoff_quantile=0.52, upper_cutoff_quantile=0.8, bins=5
        )
        th._calculate()
        result_values, result_bins = th.values

        result_bins = result_bins.round(FLOAT_PRECISION_BINS)
        assert (
            np.testing.assert_array_equal(result_values, corr_lower_upper_quantile) is None
        ), "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins, corr_lower_upper_quantile_bins) is None
        ), "incorrect histogram bins"

    def test_timedelta_hist__log_scale_x(
        self, source_stream_for_log_scale: EventstreamType, corr_log_scale_x: np.array, corr_log_scale_x_bins: np.array
    ):

        th = TimedeltaHist(source_stream_for_log_scale, timedelta_unit="s", log_scale_x=True, bins=5)
        th._calculate()
        result_values, result_bins = th.values

        result_bins = result_bins.round(FLOAT_PRECISION_BINS)
        assert np.testing.assert_array_equal(result_values, corr_log_scale_x) is None, "incorrect histogram values"
        assert np.testing.assert_array_equal(result_bins, corr_log_scale_x_bins) is None, "incorrect histogram bins"

    def test_timedelta_hist__agg(self, test_stream: EventstreamType, corr_agg: np.array, corr_agg_bins: np.array):

        th = TimedeltaHist(test_stream, timedelta_unit="s", aggregation="mean", bins=5)
        th._calculate()
        result_values, result_bins = th.values

        result_bins = result_bins.round(FLOAT_PRECISION_BINS)
        assert np.testing.assert_array_equal(result_values.round(2), corr_agg) is None, "incorrect histogram values"
        assert np.testing.assert_array_equal(result_bins, corr_agg_bins) is None, "incorrect histogram bins"

    def test_timedelta_hist__es_start_path_end(
        self,
        source_stream_start_end_events: EventstreamType,
        corr_es_start_path_end: np.array,
        corr_es_start_path_end_bins: np.array,
    ):
        th = TimedeltaHist(
            source_stream_start_end_events,
            event_pair=["eventstream_start", "path_end"],
            timedelta_unit="s",
            only_adjacent_event_pairs=False,
            bins=5,
        )
        th._calculate()
        result_values, result_bins = th.values

        result_bins = result_bins.round(FLOAT_PRECISION_BINS)
        assert (
            np.testing.assert_array_equal(result_values, corr_es_start_path_end) is None
        ), "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins, corr_es_start_path_end_bins) is None
        ), "incorrect histogram bins"

    def test_timedelta_hist__path_start_es_end(
        self,
        source_stream_start_end_events: EventstreamType,
        corr_path_start_es_end: np.array,
        corr_path_start_es_end_bins: np.array,
    ):

        th = TimedeltaHist(
            source_stream_start_end_events,
            event_pair=["eventstream_end", "path_start"],
            timedelta_unit="s",
            only_adjacent_event_pairs=False,
            bins=5,
        )
        th._calculate()
        result_values, result_bins = th.values

        result_bins = result_bins.round(FLOAT_PRECISION_BINS)
        assert (
            np.testing.assert_array_equal(result_values, corr_path_start_es_end) is None
        ), "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins, corr_path_start_es_end_bins) is None
        ), "incorrect histogram bins"

    def test_timedelta_hist__path_end_es_end(
        self,
        source_stream_start_end_events: EventstreamType,
        corr_path_end_es_end: np.array,
        corr_path_end_es_end_bins: np.array,
    ):

        th = TimedeltaHist(
            source_stream_start_end_events,
            event_pair=["path_end", "eventstream_end"],
            timedelta_unit="s",
            only_adjacent_event_pairs=False,
            bins=5,
        )
        th._calculate()
        result_values, result_bins = th.values

        result_bins = result_bins.round(FLOAT_PRECISION_BINS)
        assert np.testing.assert_array_equal(result_values, corr_path_end_es_end) is None, "incorrect histogram values"
        assert np.testing.assert_array_equal(result_bins, corr_path_end_es_end_bins) is None, "incorrect histogram bins"

    def test_timedelta_hist__path_start_path_end(
        self,
        source_stream_start_end_events: EventstreamType,
        corr_path_start_path_end: np.array,
        corr_path_start_path_end_bins: np.array,
    ):

        th = TimedeltaHist(
            source_stream_start_end_events,
            event_pair=["path_start", "path_end"],
            timedelta_unit="s",
            only_adjacent_event_pairs=True,
            bins=5,
        )
        th._calculate()
        result_values, result_bins = th.values

        result_bins = result_bins.round(FLOAT_PRECISION_BINS)
        assert (
            np.testing.assert_array_equal(result_values, corr_path_start_path_end) is None
        ), "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins, corr_path_start_path_end_bins) is None
        ), "incorrect histogram bins"

    def test_timedelta_hist__sessions(
        self, source_stream_sessions: EventstreamType, corr_sessions: np.array, corr_sessions_bins: np.array
    ):

        th = TimedeltaHist(
            source_stream_sessions,
            event_pair=["A", "B"],
            timedelta_unit="s",
            weight_col="session_id",
            only_adjacent_event_pairs=False,
            bins=5,
        )
        th._calculate()
        result_values, result_bins = th.values

        result_bins = result_bins.round(FLOAT_PRECISION_BINS)
        assert np.testing.assert_array_equal(result_values, corr_sessions) is None, "incorrect histogram values"
        assert np.testing.assert_array_equal(result_bins, corr_sessions_bins) is None, "incorrect histogram bins"
