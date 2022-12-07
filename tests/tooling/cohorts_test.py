from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from src.tooling.cohorts import Cohorts
from tests.tooling.fixtures.cohorts import test_stream


def run_test(stream, correct_res, **kwargs):
    c = Cohorts(eventstream=stream, **kwargs)
    c.fit()
    res = c.values.fillna(-999).round(2)
    correct_res = correct_res.round(2)
    return correct_res.compare(res).shape == (0, 0)


class TestCohorts:
    def test_cohorts__matrix_MM(self, test_stream):
        correct_res = pd.DataFrame(
            [
                [1.0, 1.0, 1.0, -999.0, 0.5],
                [1.0, 1.0, -999.0, 0.25, -999.0],
                [1.0, 0.67, 0.33, -999.00, -999.0],
                [1.0, 1.0, -999.0, -999.00, -999.0],
            ],
            index=pd.Index(["2021-12", "2022-01", "2022-02", "2022-03"], name="CohortGroup"),
            columns=pd.Index([0, 1, 2, 3, 4], dtype="int64", name="CohortPeriod"),
        )
        assert run_test(test_stream, correct_res, cohort_start_unit="M", cohort_period=(1, "M"), average=False)

    def test_cohorts__matrix_M2M(self, test_stream):
        correct_res = pd.DataFrame(
            [[1.0, 1.0, 0.33], [1.0, 0.5, -999.0]],
            index=pd.Index(["2021-12", "2022-02"], name="CohortGroup"),
            columns=pd.Index([0, 1, 2], dtype="int64", name="CohortPeriod"),
        )
        assert run_test(test_stream, correct_res, cohort_start_unit="M", cohort_period=(2, "M"), average=False)

    def test_cohorts__matrix_D30D(self, test_stream):
        correct_res = pd.DataFrame(
            [[1.0, 1.0, -999.0, 0.33], [1.0, 0.67, 0.33, -999.0], [1.0, 1.0, -999.0, -999.0]],
            index=pd.Index(["2021-12-28", "2022-01-27", "2022-02-26"], name="CohortGroup"),
            columns=pd.Index([0, 1, 2, 3], dtype="int64", name="CohortPeriod"),
        )
        assert run_test(test_stream, correct_res, cohort_start_unit="D", cohort_period=(30, "D"), average=False)

    def test_cohorts__matrix_W4W(self, test_stream):
        correct_res = pd.DataFrame(
            [[1.0, 1.0, -999.0, 0.33], [1.0, 0.6667, 0.33, -999.0], [1.0, 1.0, -999.0, -999.0]],
            index=pd.Index(["2021-12-27", "2022-01-24", "2022-02-21"], name="CohortGroup"),
            columns=pd.Index([0, 1, 2, 3], dtype="int64", name="CohortPeriod"),
        )
        assert run_test(test_stream, correct_res, cohort_start_unit="W", cohort_period=(4, "W"), average=False)

    def test_cohorts__matrix_D4W(self, test_stream):
        correct_res = pd.DataFrame(
            [[1.0, 1.0, -999.0, 0.3333], [1.0, 0.6667, 0.3333, -999.0], [1.0, 1.0, -999.0, -999.0]],
            index=pd.Index(["2021-12-28", "2022-01-25", "2022-02-22"], name="CohortGroup"),
            columns=pd.Index([0, 1, 2, 3], dtype="int64", name="CohortPeriod"),
        )
        assert run_test(test_stream, correct_res, cohort_start_unit="D", cohort_period=(4, "W"), average=False)

    def test_cohorts__matrix_W30D(self, test_stream):
        correct_res = pd.DataFrame(
            [[1.0, 1.0, -999.0, 0.3333], [1.0, 0.6667, 0.3333, -999.0], [1.0, 1.0, -999.0, -999.0]],
            index=pd.Index(["2021-12-27", "2022-01-26", "2022-02-25"], name="CohortGroup"),
            columns=pd.Index([0, 1, 2, 3], dtype="int64", name="CohortPeriod"),
        )
        assert run_test(test_stream, correct_res, cohort_start_unit="W", cohort_period=(30, "D"), average=False)

    def test_cohorts__matrix_D1M(self, test_stream):
        correct_res = pd.DataFrame(
            [[1.0, 1.0, -999.0, 0.33], [1.0, 0.6667, 0.33, -999.0], [1.0, 1.0, -999.0, -999.0]],
            index=pd.Index(["2021-12-27", "2022-01-24", "2022-02-21"], name="CohortGroup"),
            columns=pd.Index([0, 1, 2, 3], dtype="int64", name="CohortPeriod"),
        )
        with pytest.raises(ValueError):
            p = run_test(test_stream, correct_res, cohort_start_unit="D", cohort_period=(1, "M"), average=False)

    def test_cohorts__matrix_avg(self, test_stream):
        correct_res = pd.DataFrame(
            [
                [1.0, 0.83, 0.17, -999.0, 0.33],
                [1.0, -999, 0.5, 0.5, -999.0],
                [1.0, 1.0, -999, -999.00, -999.0],
                [1.0, 1.0, -999.0, -999.00, -999.0],
                [1.0, 0.94, 0.33, 0.5, 0.33],
            ],
            index=pd.Index(["2021-12-28", "2022-01-18", "2022-02-08", "2022-03-01", "Average"], name="CohortGroup"),
            columns=pd.Index([0, 1, 2, 3, 4], dtype="int64", name="CohortPeriod"),
        )
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

    def test_cohorts__matrix_cut(self, test_stream):
        correct_res = pd.DataFrame(
            [[1.0, 0.83]],
            index=pd.Index(["2021-12-28"], name="CohortGroup"),
            columns=pd.Index([0, 1], dtype="int64", name="CohortPeriod"),
        )
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
