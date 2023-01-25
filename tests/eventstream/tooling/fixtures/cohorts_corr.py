import pandas as pd
import pytest


@pytest.fixture
def simple_corr():
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
def refit_corr():
    correct_res = pd.DataFrame(
        [[1.0, 1.0, -999.0, 0.33], [1.0, 0.67, 0.33, -999.0], [1.0, 1.0, -999.0, -999.0]],
        index=pd.Index(["2021-12-27", "2022-01-26", "2022-02-25"], name="CohortGroup"),
        columns=pd.Index([0, 1, 2, 3], dtype="int64", name="CohortPeriod"),
    )
    return correct_res
