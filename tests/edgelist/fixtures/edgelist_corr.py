import pandas as pd
import pytest


@pytest.fixture
def el_simple_corr():
    correct_res = pd.DataFrame(
        [
            ["A", "B", 3],
            ["A", "C", 1],
            ["B", "A", 1],
            ["B", "C", 1],
            ["C", "A", 1],
            ["C", "C", 2],
            ["C", "D", 3],
            ["D", "C", 2],
        ],
        columns=pd.Index(["event", "next_event", "event"]),
    )
    return correct_res


@pytest.fixture
def el_simple_node_corr():
    correct_res = pd.DataFrame(
        [
            ["A", "B", 0.75],
            ["A", "C", 0.25],
            ["B", "A", 0.5],
            ["B", "C", 0.5],
            ["C", "A", 0.167],
            ["C", "C", 0.333],
            ["C", "D", 0.5],
            ["D", "C", 1],
        ],
        columns=pd.Index(["event_col", "next_event_col", "event"]),
    )
    return correct_res


@pytest.fixture
def el_simple_full_corr():
    correct_res = pd.DataFrame(
        [
            ["A", "B", 0.2142],
            ["A", "C", 0.0714],
            ["B", "A", 0.0714],
            ["B", "C", 0.0714],
            ["C", "A", 0.0714],
            ["C", "C", 0.1428],
            ["C", "D", 0.2142],
            ["D", "C", 0.1428],
        ],
        columns=pd.Index(["event_col", "next_event_col", "event"]),
    )
    return correct_res


@pytest.fixture
def el_user__corr():
    correct_res = pd.DataFrame(
        [
            ["A", "B", 2],
            ["A", "C", 1],
            ["B", "A", 1],
            ["B", "C", 1],
            ["C", "A", 1],
            ["C", "C", 1],
            ["C", "D", 1],
            ["D", "C", 1],
        ],
        columns=pd.Index(["event", "next_event", "user_id"]),
    )

    return correct_res


@pytest.fixture
def el_user_node_corr():
    correct_res = pd.DataFrame(
        [
            ["A", "B", 1],
            ["A", "C", 0.5],
            ["B", "A", 0.5],
            ["B", "C", 0.5],
            ["C", "A", 0.333],
            ["C", "C", 0.333],
            ["C", "D", 0.333],
            ["D", "C", 1],
        ],
        columns=pd.Index(["event", "next_event", "user_id"]),
    )
    return correct_res


@pytest.fixture
def el_user_full_corr():
    correct_res = pd.DataFrame(
        [
            ["A", "B", 0.667],
            ["A", "C", 0.333],
            ["B", "A", 0.333],
            ["B", "C", 0.333],
            ["C", "A", 0.333],
            ["C", "C", 0.333],
            ["C", "D", 0.333],
            ["D", "C", 0.333],
        ],
        columns=pd.Index(["event", "next_event", "user_id"]),
    )
    return correct_res


@pytest.fixture
def el_session_corr():
    correct_res = pd.DataFrame(
        [
            ["A", "B", 3],
            ["A", "C", 1],
            ["B", "A", 0],
            ["B", "C", 0],
            ["C", "A", 0],
            ["C", "C", 1],
            ["C", "D", 1],
            ["D", "C", 1],
        ],
        columns=pd.Index(["event", "next_event", "session_id"]),
    )
    return correct_res


@pytest.fixture
def el_session_node_corr():
    correct_res = pd.DataFrame(
        [
            ["A", "B", 0.75],
            ["A", "C", 0.25],
            ["B", "A", 0],
            ["B", "C", 0],
            ["C", "A", 0],
            ["C", "C", 0.5],
            ["C", "D", 0.5],
            ["D", "C", 1],
        ],
        columns=pd.Index(["event", "next_event", "session_id"]),
    )
    return correct_res


@pytest.fixture
def el_session_full_corr():
    correct_res = pd.DataFrame(
        [
            ["A", "B", 0.5],
            ["A", "C", 0.167],
            ["B", "A", 0],
            ["B", "C", 0],
            ["C", "A", 0],
            ["C", "C", 0.167],
            ["C", "D", 0.167],
            ["D", "C", 0.167],
        ],
        columns=pd.Index(["event", "next_event", "session_id"]),
    )
    return correct_res
