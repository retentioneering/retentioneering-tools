from __future__ import annotations

import pandas as pd

from retentioneering.nodelist import Nodelist
from tests.edgelist.fixtures.edgelist_input import test_df
from tests.nodelist.fixtures.nodelist_corr import (
    nl_session_corr,
    nl_simple_corr,
    nl_user_corr,
)


class TestNodelist:
    def test_nodelist__user(self, test_df: pd.DataFrame, nl_user_corr: pd.DataFrame) -> None:
        nl = Nodelist("event", "timestamp", ["user_id"])
        result = nl.calculate_nodelist(test_df)
        correct = nl_user_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def test_nodelist__sessions(self, test_df: pd.DataFrame, nl_session_corr: pd.DataFrame) -> None:
        nl = Nodelist("event", "timestamp", ["session_id"])
        result = nl.calculate_nodelist(test_df)
        correct = nl_session_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    # всё ок, если nodelist_default_col не используется вместо custom_col
