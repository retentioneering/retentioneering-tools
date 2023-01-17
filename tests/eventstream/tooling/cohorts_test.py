from __future__ import annotations

import pandas as pd
import pytest

from tests.eventstream.tooling.fixtures.cohorts import test_stream
from tests.eventstream.tooling.fixtures.cohorts_corr import refit_corr, simple_corr

FLOAT_PRECISION = 2


class TestEventstreamCohorts:
    def test_cohorts_eventstream__simple(self, test_stream, simple_corr):
        params = {"cohort_start_unit": "M", "cohort_period": (1, "M"), "average": False}

        correct_res_df = simple_corr.round(FLOAT_PRECISION)
        res = test_stream.cohorts(**params).values.fillna(-999).round(FLOAT_PRECISION)
        assert pd.testing.assert_frame_equal(res[correct_res_df.columns], correct_res_df) is None

    def test_cohorts_eventstream__refit(self, test_stream, simple_corr, refit_corr):
        params_1 = {"cohort_start_unit": "M", "cohort_period": (1, "M"), "average": False}
        params_2 = {"cohort_start_unit": "W", "cohort_period": (30, "D"), "average": False}
        correct_res_1 = simple_corr
        correct_res_2 = refit_corr

        res_1 = test_stream.cohorts(**params_1).values.fillna(-999).round(FLOAT_PRECISION)
        res_2 = test_stream.cohorts(**params_2).values.fillna(-999).round(FLOAT_PRECISION)

        assert pd.testing.assert_frame_equal(res_1[correct_res_1.columns], correct_res_1) is None, "First calculation"
        assert pd.testing.assert_frame_equal(res_2[correct_res_2.columns], correct_res_2) is None, "Refit"

    def test_cohorts_eventstream__fit_hash_check(self, test_stream):
        params = {"cohort_start_unit": "M", "cohort_period": (1, "M"), "average": False}

        cc = test_stream.cohorts(**params)
        hash1 = hash(cc)
        cc.values
        hash2 = hash(cc)

        assert hash1 == hash2
