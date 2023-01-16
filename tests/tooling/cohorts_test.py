from __future__ import annotations

import pandas as pd
import pytest

from retentioneering.tooling.cohorts import Cohorts
from tests.tooling.fixtures.cohorts_corr import (
    matrix_avg_corr,
    matrix_cut_corr,
    matrix_D1M_corr,
    matrix_D4W_corr,
    matrix_D30D_corr,
    matrix_M2M_corr,
    matrix_MM_corr,
    matrix_W4W_corr,
    matrix_W30D_corr,
)
from tests.tooling.fixtures.cohorts_input import test_stream


def run_test(stream, correct_res, **kwargs):
    c = Cohorts(eventstream=stream, **kwargs)
    c.fit()
    res = c.values.fillna(-999).round(2)
    correct_res = correct_res.round(2)
    return correct_res.compare(res).shape == (0, 0)


class TestCohorts:
    def test_cohorts__matrix_MM(self, test_stream, matrix_MM_corr):
        correct_res = matrix_MM_corr
        assert run_test(test_stream, correct_res, cohort_start_unit="M", cohort_period=(1, "M"), average=False)

    def test_cohorts__matrix_M2M(self, test_stream, matrix_M2M_corr):
        correct_res = matrix_M2M_corr
        assert run_test(test_stream, correct_res, cohort_start_unit="M", cohort_period=(2, "M"), average=False)

    def test_cohorts__matrix_D30D(self, test_stream, matrix_D30D_corr):
        correct_res = matrix_D30D_corr
        assert run_test(test_stream, correct_res, cohort_start_unit="D", cohort_period=(30, "D"), average=False)

    def test_cohorts__matrix_W4W(self, test_stream, matrix_W4W_corr):
        correct_res = matrix_W4W_corr
        assert run_test(test_stream, correct_res, cohort_start_unit="W", cohort_period=(4, "W"), average=False)

    def test_cohorts__matrix_D4W(self, test_stream, matrix_D4W_corr):
        correct_res = matrix_D4W_corr
        assert run_test(test_stream, correct_res, cohort_start_unit="D", cohort_period=(4, "W"), average=False)

    def test_cohorts__matrix_W30D(self, test_stream, matrix_W30D_corr):
        correct_res = matrix_W30D_corr
        assert run_test(test_stream, correct_res, cohort_start_unit="W", cohort_period=(30, "D"), average=False)

    def test_cohorts__matrix_D1M(self, test_stream, matrix_D1M_corr):
        correct_res = matrix_D1M_corr
        with pytest.raises(ValueError):
            p = run_test(test_stream, correct_res, cohort_start_unit="D", cohort_period=(1, "M"), average=False)

    def test_cohorts__matrix_avg(self, test_stream, matrix_avg_corr):
        correct_res = matrix_avg_corr
        assert run_test(
            test_stream,
            correct_res,
            cohort_start_unit="D",
            cohort_period=(21, "D"),
            average=True,
            cut_bottom=0,
            cut_right=0,
            cut_diagonal=0,
        )

    def test_cohorts__matrix_cut(self, test_stream, matrix_cut_corr):
        correct_res = matrix_cut_corr
        assert run_test(
            test_stream,
            correct_res,
            cohort_start_unit="D",
            cohort_period=(21, "D"),
            average=False,
            cut_bottom=3,
            cut_right=3,
            cut_diagonal=0,
        )
