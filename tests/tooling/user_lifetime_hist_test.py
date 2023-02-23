from __future__ import annotations

import numpy as np

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.user_lifetime_hist.user_lifetime_hist import (
    UserLifetimeHist,
)
from tests.tooling.fixtures.user_lifetime_hist import test_stream
from tests.tooling.fixtures.user_lifetime_hist_corr import (
    correct_basic,
    correct_basic_bins,
    correct_log_scale,
    correct_log_scale_bins,
    correct_lower_cutoff_quantile,
    correct_lower_cutoff_quantile_bins,
    correct_timedelta_unit,
    correct_timedelta_unit_bins,
    correct_upper_cutoff_quantile,
    correct_upper_cutoff_quantile_bins,
    correct_upper_lower_cutoff_quantile,
    correct_upper_lower_cutoff_quantile_bins,
)

FLOAT_PRECISION_VALS = 2
FLOAT_PRECISION_BINS = 1


class TestUserLifetimeHist:
    def test_user_lifetime_hist__basic(
        self, test_stream: EventstreamType, correct_basic: np.array, correct_basic_bins: np.array
    ):

        ul = UserLifetimeHist(test_stream, bins=5)
        ul.fit()
        result_values, result_bins = ul.values

        assert (
            np.testing.assert_array_equal(result_values.round(FLOAT_PRECISION_VALS), correct_basic) is None
        ), "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins.round(FLOAT_PRECISION_BINS), correct_basic_bins) is None
        ), "incorrect histogram bins"

    def test_user_lifetime_hist__timedelta_unit(
        self, test_stream: EventstreamType, correct_timedelta_unit: np.array, correct_timedelta_unit_bins: np.array
    ):

        ul = UserLifetimeHist(test_stream, bins=5, timedelta_unit="h")
        ul.fit()
        result_values, result_bins = ul.values

        assert (
            np.testing.assert_array_equal(result_values.round(FLOAT_PRECISION_VALS), correct_timedelta_unit) is None
        ), "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins.round(FLOAT_PRECISION_BINS), correct_timedelta_unit_bins) is None
        ), "incorrect histogram bins"

    def test_user_lifetime_hist__log_scale(
        self, test_stream: EventstreamType, correct_log_scale: np.array, correct_log_scale_bins: np.array
    ):

        ul = UserLifetimeHist(test_stream, bins=5, timedelta_unit="h", log_scale=True)
        ul.fit()
        result_values, result_bins = ul.values

        assert (
            np.testing.assert_array_equal(result_values.round(FLOAT_PRECISION_VALS), correct_log_scale) is None
        ), "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins.round(FLOAT_PRECISION_BINS), correct_log_scale_bins) is None
        ), "incorrect histogram bins"

    def test_user_lifetime_hist__lower_cutoff_quantile(
        self,
        test_stream: EventstreamType,
        correct_lower_cutoff_quantile: np.array,
        correct_lower_cutoff_quantile_bins: np.array,
    ):

        ul = UserLifetimeHist(test_stream, bins=5, timedelta_unit="h", lower_cutoff_quantile=0.5)
        ul.fit()
        result_values, result_bins = ul.values

        assert (
            np.testing.assert_array_equal(result_values.round(FLOAT_PRECISION_VALS), correct_lower_cutoff_quantile)
            is None
        ), "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins.round(FLOAT_PRECISION_BINS), correct_lower_cutoff_quantile_bins)
            is None
        ), "incorrect histogram bins"

    def test_user_lifetime_hist__upper_cutoff_quantile(
        self,
        test_stream: EventstreamType,
        correct_upper_cutoff_quantile: np.array,
        correct_upper_cutoff_quantile_bins: np.array,
    ):

        ul = UserLifetimeHist(test_stream, bins=5, timedelta_unit="h", upper_cutoff_quantile=0.5)
        ul.fit()
        result_values, result_bins = ul.values

        assert (
            np.testing.assert_array_equal(result_values.round(FLOAT_PRECISION_VALS), correct_upper_cutoff_quantile)
            is None
        ), "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(result_bins.round(FLOAT_PRECISION_BINS), correct_upper_cutoff_quantile_bins)
            is None
        ), "incorrect histogram bins"

    def test_user_lifetime_hist__upper_lower_cutoff_quantile(
        self,
        test_stream: EventstreamType,
        correct_upper_lower_cutoff_quantile: np.array,
        correct_upper_lower_cutoff_quantile_bins: np.array,
    ):

        ul = UserLifetimeHist(
            test_stream, bins=5, timedelta_unit="h", upper_cutoff_quantile=0.5, lower_cutoff_quantile=0.5
        )
        ul.fit()
        result_values, result_bins = ul.values

        assert (
            np.testing.assert_array_equal(
                result_values.round(FLOAT_PRECISION_VALS), correct_upper_lower_cutoff_quantile
            )
            is None
        ), "incorrect histogram values"
        assert (
            np.testing.assert_array_equal(
                result_bins.round(FLOAT_PRECISION_BINS), correct_upper_lower_cutoff_quantile_bins
            )
            is None
        ), "incorrect histogram bins"
