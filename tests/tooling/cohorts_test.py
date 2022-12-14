from __future__ import annotations

import os

import pandas as pd
import pytest

from src.tooling.cohorts import Cohorts
from tests.tooling.fixtures.cohorts import test_stream


def run_test(stream, test_prefix, **kwargs):
    c = Cohorts(eventstream=stream, **kwargs)
    c.fit()
    res = c.values.fillna(-999).reset_index().round(2)

    current_dir = os.path.dirname(os.path.realpath(__file__))

    test_data_dir = os.path.join(current_dir, "../datasets/tooling/cohorts")
    correct_res_df = pd.read_csv(os.path.join(test_data_dir, f"{test_prefix}.csv"))
    correct_res_df.columns.name = "CohortPeriod"
    correct_res_df.drop(columns="CohortPeriod", inplace=True)
    correct_res_df.columns = ["CohortGroup"] + [int(i) for i in correct_res_df.columns[1:]]
    return correct_res_df.round(2).compare(res).shape == (0, 0)


class TestCohorts:
    def test_cohorts__matrix_MM(self, test_stream):
        assert run_test(test_stream, "01_matrix_MM", cohort_start_unit="M", cohort_period=(1, "M"), average=False)

    def test_cohorts__matrix_M2M(self, test_stream):
        assert run_test(test_stream, "02_matrix_M2M", cohort_start_unit="M", cohort_period=(2, "M"), average=False)

    def test_cohorts__matrix_D30D(self, test_stream):
        assert run_test(test_stream, "03_matrix_D30D", cohort_start_unit="D", cohort_period=(30, "D"), average=False)

    def test_cohorts__matrix_W4W(self, test_stream):
        assert run_test(test_stream, "04_matrix_W4W", cohort_start_unit="W", cohort_period=(4, "W"), average=False)

    def test_cohorts__matrix_D4W(self, test_stream):
        assert run_test(test_stream, "05_matrix_D4W", cohort_start_unit="D", cohort_period=(4, "W"), average=False)

    def test_cohorts__matrix_W30D(self, test_stream):
        assert run_test(test_stream, "06_matrix_W30D", cohort_start_unit="W", cohort_period=(30, "D"), average=False)

    def test_cohorts__matrix_D1M(self, test_stream):
        with pytest.raises(ValueError):
            p = run_test(test_stream, "04_matrix_W4W", cohort_start_unit="D", cohort_period=(1, "M"), average=False)
