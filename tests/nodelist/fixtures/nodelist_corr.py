import pandas as pd
import pytest


@pytest.fixture
def nl_simple_corr():
    correct_res = pd.DataFrame(
        [
            ["C", 7, True, False, None, None],
            ["A", 4, True, False, None, None],
            ["B", 3, True, False, None, None],
            ["D", 3, True, False, None, None],
        ],
        index=pd.Index([2, 0, 1, 3]),
        columns=pd.Index(["event", "nodelist_default_col", "active", "alias", "parent", "changed_name"]),
    )
    return correct_res


@pytest.fixture
def nl_user_corr():
    correct_res = pd.DataFrame(
        [
            ["C", 3, True, False, None, None],
            ["A", 2, True, False, None, None],
            ["B", 2, True, False, None, None],
            ["D", 1, True, False, None, None],
        ],
        index=pd.Index([2, 0, 1, 3]),
        columns=pd.Index(["event", "user_id", "active", "alias", "parent", "changed_name"]),
    )
    return correct_res


@pytest.fixture
def nl_session_corr():
    correct_res = pd.DataFrame(
        [
            ["C", 3, True, False, None, None],
            ["A", 4, True, False, None, None],
            ["B", 3, True, False, None, None],
            ["D", 1, True, False, None, None],
        ],
        index=pd.Index([2, 0, 1, 3]),
        columns=pd.Index(["event", "session_id", "active", "alias", "parent", "changed_name"]),
    )
    return correct_res
