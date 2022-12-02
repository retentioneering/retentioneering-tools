from __future__ import annotations

import os

import pandas as pd
import pytest

from tests.eventstream.tooling.fixtures.cohorts import test_stream

FLOAT_PRECISION = 2


def correct_res_test(test_prefix):

    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/eventstream/tooling/cohorts")
    correct_res_df = pd.read_csv(os.path.join(test_data_dir, f"{test_prefix}.csv"))
    correct_res_df.columns.name = "CohortPeriod"
    correct_res_df.drop(columns="CohortPeriod", inplace=True)
    correct_res_df.columns = ["CohortGroup"] + [int(i) for i in correct_res_df.columns[1:]]
    return correct_res_df.round(FLOAT_PRECISION)


class TestEventstreamCohorts:
    def test_cohorts_eventstream__simple(self, test_stream):
        params = {"cohort_start_unit": "M", "cohort_period": (1, "M"), "average": False}

        correct_res_df = correct_res_test("01_matrix_MM")
        res = test_stream.cohorts(**params).values.fillna(-999).reset_index().round(FLOAT_PRECISION)
        assert correct_res_df.round(FLOAT_PRECISION).compare(res).shape == (0, 0)

    def test_cohorts_eventstream__refit(self, test_stream):
        params_1 = {"cohort_start_unit": "M", "cohort_period": (1, "M"), "average": False}

        params_2 = {"cohort_start_unit": "W", "cohort_period": (30, "D"), "average": False}
        correct_res_1 = correct_res_test("01_matrix_MM")
        correct_res_2 = correct_res_test("06_matrix_W30D")

        res_1 = test_stream.cohorts(**params_1).values.fillna(-999).reset_index().round(FLOAT_PRECISION)
        res_2 = test_stream.cohorts(**params_2).values.fillna(-999).reset_index().round(FLOAT_PRECISION)
        calc_is_correct = correct_res_1.round(FLOAT_PRECISION).compare(res_1).shape == (0, 0)
        recalc_is_correct = correct_res_2.round(FLOAT_PRECISION).compare(res_2).shape == (0, 0)

        assert calc_is_correct and recalc_is_correct

    def test_cohorts_eventstream__fit_hash_check(self, test_stream):
        params = {"cohort_start_unit": "M", "cohort_period": (1, "M"), "average": False}

        cc = test_stream.cohorts(**params)
        hash1 = hash(cc)
        cc.values
        hash2 = hash(cc)

        assert hash1 == hash2
