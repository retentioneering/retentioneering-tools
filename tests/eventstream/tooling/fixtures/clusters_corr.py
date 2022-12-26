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


@pytest.fixture
def ngram_range_corr():
    correct_columns = [
        "event1 event1 event1",
        "event1 event1 event3",
        "event1 event1 event4",
        "event1 event2 event1",
        "event1 event2 event2",
        "event1 event3 event1",
        "event2 event1 event1",
        "event3 event1 event3",
    ]
    correct_columns = [c + "_count" for c in correct_columns]
    correct_features = pd.DataFrame(
        [
            [1, 1, 0, 1, 0, 2, 1, 1],
            [0, 0, 0, 0, 1, 0, 0, 0],
            [0, 0, 1, 0, 0, 0, 0, 0],
            [1, 0, 0, 0, 0, 0, 0, 0],
        ],
        columns=correct_columns,
        index=[1, 2, 3, 4],
    )
    return correct_features


@pytest.fixture
def set_clusters_corr():
    corr = {0: [2], 1: [0], 2: [3], 3: [1]}
    return corr


@pytest.fixture
def vector_corr():
    d = {1: 0, 2: 1, 3: 1, 4: 1}
    index = pd.Index([1, 2, 3, 4], name="user_id")
    corr = pd.Series(data=d, index=index)
    return corr
