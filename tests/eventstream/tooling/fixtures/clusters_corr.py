import pandas as pd
import pytest


@pytest.fixture
def count_corr():
    correct_columns = ["event1", "event2", "event3", "event4"]
    correct_columns = [c + "_count" for c in correct_columns]
    correct_features = pd.DataFrame(
        [
            [6, 1, 2, 0],
            [1, 2, 0, 0],
            [2, 0, 0, 1],
            [3, 0, 0, 0],
        ],
        columns=correct_columns,
        index=[1, 2, 3, 4],
    )
    return correct_features


@pytest.fixture
def time_corr():
    correct_columns = ["event1", "event2", "event3", "event4"]
    correct_columns = [c + "_time" for c in correct_columns]
    correct_features = pd.DataFrame(
        [
            [122.0, 58.0, 60.0, 0.0],
            [5.0, 60.0, 0.0, 0.0],
            [115.0, 0.0, 0.0, 0.0],
            [115.0, 0.0, 0.0, 0.0],
        ],
        columns=correct_columns,
        index=[1, 2, 3, 4],
    )
    return correct_features


@pytest.fixture
def cluster_mapping_corr():
    corr = {0: [1, 2, 4], 1: [3]}
    return corr
