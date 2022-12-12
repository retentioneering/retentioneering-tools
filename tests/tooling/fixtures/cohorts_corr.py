import pandas as pd
import pytest


@pytest.fixture
def MM_corr_corr():
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
    return correct_res


@pytest.fixture
def matrix_M2M_corr():
    correct_res = pd.DataFrame(
        [[1.0, 1.0, 0.33], [1.0, 0.5, -999.0]],
        index=pd.Index(["2021-12", "2022-02"], name="CohortGroup"),
        columns=pd.Index([0, 1, 2], dtype="int64", name="CohortPeriod"),
    )
    return correct_res


@pytest.fixture
def matrix_matrix_D30D_corr():
    correct_res = pd.DataFrame(
        [[1.0, 1.0, -999.0, 0.33], [1.0, 0.67, 0.33, -999.0], [1.0, 1.0, -999.0, -999.0]],
        index=pd.Index(["2021-12-28", "2022-01-27", "2022-02-26"], name="CohortGroup"),
        columns=pd.Index([0, 1, 2, 3], dtype="int64", name="CohortPeriod"),
    )
    return correct_res


@pytest.fixture
def matrix_matrix_W4W_corr():
    correct_res = pd.DataFrame(
        [[1.0, 1.0, -999.0, 0.33], [1.0, 0.6667, 0.33, -999.0], [1.0, 1.0, -999.0, -999.0]],
        index=pd.Index(["2021-12-27", "2022-01-24", "2022-02-21"], name="CohortGroup"),
        columns=pd.Index([0, 1, 2, 3], dtype="int64", name="CohortPeriod"),
    )
    return correct_res


@pytest.fixture
def matrix_matrix_D4W_corr():
    correct_res = pd.DataFrame(
        [[1.0, 1.0, -999.0, 0.3333], [1.0, 0.6667, 0.3333, -999.0], [1.0, 1.0, -999.0, -999.0]],
        index=pd.Index(["2021-12-28", "2022-01-25", "2022-02-22"], name="CohortGroup"),
        columns=pd.Index([0, 1, 2, 3], dtype="int64", name="CohortPeriod"),
    )
    return correct_res


@pytest.fixture
def matrix_matrix_W30D_corr():
    correct_res = pd.DataFrame(
        [[1.0, 1.0, -999.0, 0.3333], [1.0, 0.6667, 0.3333, -999.0], [1.0, 1.0, -999.0, -999.0]],
        index=pd.Index(["2021-12-27", "2022-01-26", "2022-02-25"], name="CohortGroup"),
        columns=pd.Index([0, 1, 2, 3], dtype="int64", name="CohortPeriod"),
    )
    return correct_res


@pytest.fixture
def matrix_matrix_D1M_corr():
    correct_res = pd.DataFrame(
        [[1.0, 1.0, -999.0, 0.33], [1.0, 0.6667, 0.33, -999.0], [1.0, 1.0, -999.0, -999.0]],
        index=pd.Index(["2021-12-27", "2022-01-24", "2022-02-21"], name="CohortGroup"),
        columns=pd.Index([0, 1, 2, 3], dtype="int64", name="CohortPeriod"),
    )
    return correct_res


@pytest.fixture
def matrix_matrix_matrix_avg_corr():
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
    return correct_res


@pytest.fixture
def matrix_matrix_matrix_cut_corr():
    correct_res = pd.DataFrame(
        [[1.0, 0.83]],
        index=pd.Index(["2021-12-28"], name="CohortGroup"),
        columns=pd.Index([0, 1], dtype="int64", name="CohortPeriod"),
    )
    return correct_res
