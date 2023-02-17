from __future__ import annotations

import pandas as pd

from retentioneering.nodelist import Nodelist
from tests.nodelist.fixtures.nodelist_corr import session_corr, simple_corr, user_corr
from tests.nodelist.fixtures.nodelist_input import test_df


class TestNodelist:
    def test_nodelist__simple(self, test_df: pd.DataFrame, simple_corr: pd.DataFrame) -> bool:
        nl = Nodelist("event", "timestamp", "nodelist_default_col", None)
        result = nl.calculate_nodelist(test_df)
        correct = simple_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def test_nodelist__user(self, test_df: pd.DataFrame, user_corr: pd.DataFrame) -> bool:
        nl = Nodelist("event", "timestamp", "nodelist_default_col", ["user_id"])
        result = nl.calculate_nodelist(test_df)
        correct = user_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def test_nodelist__sessions(self, test_df: pd.DataFrame, session_corr: pd.DataFrame) -> bool:
        nl = Nodelist("event", "timestamp", "nodelist_default_col", ["session_id"])
        result = nl.calculate_nodelist(test_df)
        correct = session_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    # всё ок, если nodelist_default_col не используется вместо custom_col
