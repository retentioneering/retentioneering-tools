from __future__ import annotations

import numpy as np

from tests.eventstream.tooling.fixtures.user_lifetime_hist import test_stream

FLOAT_PRECISION_VALS = 2
FLOAT_PRECISION_BINS = 1


class TestEventstreamUserLifetimeHist:
    def test_user_lifetime_hist_eventstream__default(self, test_stream):
        correct_result = np.array(
            [172802.0, 431940.0, 2505540.0, 863940.0, 1900740.0, 172740.0, 518340.0, 1641540.0, 1468740.0]
        )
        correct_bins = np.array([172740.0, 639300.0, 1105860.0, 1572420.0, 2038980.0, 2505540.0])
        result = test_stream.user_lifetime_hist(show_plot=False).values

        assert np.testing.assert_array_equal(result[0].round(FLOAT_PRECISION_VALS), correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_user_lifetime_hist_eventstream__timedelta_unit(self, test_stream):
        correct_result = np.array([48.0, 119.98, 695.98, 239.98, 527.98, 47.98, 143.98, 455.98, 407.98])
        correct_bins = np.array([48.0, 177.6, 307.2, 436.8, 566.4, 696.0])
        result = test_stream.user_lifetime_hist(show_plot=False, bins=5, timedelta_unit="h").values

        assert np.testing.assert_array_equal(result[0].round(FLOAT_PRECISION_VALS), correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_user_lifetime_hist_eventstream__log_scale(self, test_stream):
        correct_result = np.array([48.0, 119.98, 695.98, 239.98, 527.98, 47.98, 143.98, 455.98, 407.98])
        correct_bins = np.array([48.0, 81.9, 139.9, 238.8, 407.7, 696.0])
        result = test_stream.user_lifetime_hist(
            show_plot=False, bins=5, timedelta_unit="h", log_scale=(True, False)
        ).values

        assert np.testing.assert_array_equal(result[0].round(FLOAT_PRECISION_VALS), correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_user_lifetime_hist_eventstream__lower_cutoff_quantile(self, test_stream):
        correct_result = np.array([695.98, 239.98, 527.98, 455.98, 407.98])
        correct_bins = np.array([240.0, 331.2, 422.4, 513.6, 604.8, 696.0])
        result = test_stream.user_lifetime_hist(
            show_plot=False, bins=5, timedelta_unit="h", lower_cutoff_quantile=0.5
        ).values

        assert np.testing.assert_array_equal(result[0].round(FLOAT_PRECISION_VALS), correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_user_lifetime_hist_eventstream__upper_cutoff_quantile(self, test_stream):
        correct_result = np.array([48.0, 119.98, 239.98, 47.98, 143.98])
        correct_bins = np.array([48.0, 86.4, 124.8, 163.2, 201.6, 240.0])
        result = test_stream.user_lifetime_hist(
            show_plot=False, bins=5, timedelta_unit="h", upper_cutoff_quantile=0.5
        ).values

        assert np.testing.assert_array_equal(result[0].round(FLOAT_PRECISION_VALS), correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"

    def test_user_lifetime_hist_eventstream__upper_lower_cutoff_quantile(self, test_stream):
        correct_result = np.array([239.98])
        correct_bins = np.array([239.5, 239.7, 239.9, 240.1, 240.3, 240.5])
        result = test_stream.user_lifetime_hist(
            show_plot=False, bins=5, timedelta_unit="h", upper_cutoff_quantile=0.5, lower_cutoff_quantile=0.5
        ).values

        assert np.testing.assert_array_equal(result[0].round(FLOAT_PRECISION_VALS), correct_result) is None, "values"
        assert np.testing.assert_array_equal(result[1].round(FLOAT_PRECISION_BINS), correct_bins) is None, "bins"
