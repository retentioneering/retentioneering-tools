import pandas as pd
import pytest


@pytest.fixture
def simple_corr():
    correct_res = pd.DataFrame(
        [
            [0.0, 3.0, 1.0, 0.0],
            [1.0, 0.0, 1.0, 0.0],
            [1.0, 0.0, 2.0, 3.0],
            [0.0, 0.0, 2.0, 0.0],
        ],
        index=pd.Index(["A", "B", "C", "D"]),
        columns=pd.Index(["A", "B", "C", "D"]),
    )
    return correct_res


@pytest.fixture
def full_corr():
    correct_res = pd.DataFrame(
        [
            [0.0, 0.214, 0.071, 0.0],
            [0.071, 0.0, 0.071, 0.0],
            [0.071, 0.0, 0.143, 0.214],
            [0.0, 0.0, 0.143, 0.0],
        ],
        index=pd.Index(["A", "B", "C", "D"]),
        columns=pd.Index(["A", "B", "C", "D"]),
    )
    return correct_res


@pytest.fixture
def node_corr():
    correct_res = pd.DataFrame(
        [
            [0.0, 0.75, 0.25, 0.0],
            [0.50, 0.0, 0.5, 0.0],
            [0.167, 0.0, 0.333, 0.5],
            [0.0, 0.0, 1.0, 0.0],
        ],
        index=pd.Index(["A", "B", "C", "D"]),
        columns=pd.Index(["A", "B", "C", "D"]),
    )
    return correct_res


@pytest.fixture
def session_simple_corr():
    correct_res = pd.DataFrame(
        [
            [0.0, 3, 1, 0.0],
            [0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 1, 1],
            [0.0, 0.0, 1.0, 0.0],
        ],
        index=pd.Index(["A", "B", "C", "D"]),
        columns=pd.Index(["A", "B", "C", "D"]),
    )
    return correct_res


@pytest.fixture
def session_full_corr():
    correct_res = pd.DataFrame(
        [
            [0.0, 0.5, 0.167, 0.0],
            [0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.167, 0],
            [0.0, 0.0, 0.167, 0.0],
        ],
        index=pd.Index(["A", "B", "C", "D"]),
        columns=pd.Index(["A", "B", "C", "D"]),
    )
    return correct_res


@pytest.fixture
def session_node_corr():
    correct_res = pd.DataFrame(
        [
            [0.0, 0.75, 0.25, 0.0],
            [0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.5, 0.5],
            [0.0, 0.0, 1.0, 0.0],
        ],
        index=pd.Index(["A", "B", "C", "D"]),
        columns=pd.Index(["A", "B", "C", "D"]),
    )
    return correct_res


@pytest.fixture
def user_simple_corr():
    correct_res = pd.DataFrame(
        [
            [0.0, 2, 1, 0.0],
            [1, 0.0, 1, 0.0],
            [1, 0.0, 1, 1],
            [0.0, 0.0, 1.0, 0.0],
        ],
        index=pd.Index(["A", "B", "C", "D"]),
        columns=pd.Index(["A", "B", "C", "D"]),
    )
    return correct_res


@pytest.fixture
def user_full_corr():
    correct_res = pd.DataFrame(
        [
            [0.0, 0.667, 0.333, 0.0],
            [0.333, 0.0, 0.333, 0.0],
            [0.333, 0.0, 0.333, 0.333],
            [0.0, 0.0, 0.333, 0.0],
        ],
        index=pd.Index(["A", "B", "C", "D"]),
        columns=pd.Index(["A", "B", "C", "D"]),
    )
    return correct_res


@pytest.fixture
def user_node_corr():
    correct_res = pd.DataFrame(
        [
            [0.0, 1.0, 0.5, 0.0],
            [0.5, 0.0, 0.5, 0.0],
            [0.333, 0.0, 0.333, 0.333],
            [0.0, 0.0, 1.0, 0.0],
        ],
        index=pd.Index(["A", "B", "C", "D"]),
        columns=pd.Index(["A", "B", "C", "D"]),
    )
    return correct_res
