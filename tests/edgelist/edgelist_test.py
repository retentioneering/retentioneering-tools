from __future__ import annotations

import pandas as pd

from retentioneering.edgelist import Edgelist
from tests.edgelist.fixtures.edgelist_corr import (
    el_session_corr,
    el_session_full_corr,
    el_session_node_corr,
    el_simple_corr,
    el_simple_full_corr,
    el_simple_node_corr,
    el_user__corr,
    el_user_full_corr,
    el_user_node_corr,
)
from tests.edgelist.fixtures.edgelist_input import test_df
from tests.nodelist.fixtures.nodelist_corr import (
    nl_session_corr,
    nl_simple_corr,
    nl_user_corr,
)


class TestEdgelist:
    def test_edgelist__simple(
        self, test_df: pd.DataFrame, nl_simple_corr: pd.DataFrame, el_simple_corr: pd.DataFrame
    ) -> None:
        nl = nl_simple_corr
        correct = el_simple_corr
        el = Edgelist(
            event_col="event", time_col="timestamp", default_weight_col="event", index_col="user_id", nodelist=nl
        )
        result = el.calculate_edgelist(test_df)
        assert pd.testing.assert_frame_equal(result, correct) is None


class VerifyEdgelist:
    def test_edgelist__session_simple(
        self, test_df: pd.DataFrame, nl_session_corr: pd.DataFrame, el_session_corr: pd.DataFrame
    ) -> None:
        nl = nl_session_corr
        correct = el_session_corr
        el = Edgelist(
            event_col="event", time_col="timestamp", default_weight_col="session_id", index_col="user_id", nodelist=nl
        )
        result = el.calculate_edgelist(test_df)
        assert pd.testing.assert_frame_equal(result, correct) is None

    def test_edgelist__simple_node(
        self, test_df: pd.DataFrame, nl_simple_corr: pd.DataFrame, el_simple_node_corr: pd.DataFrame
    ) -> None:
        nl = nl_simple_corr
        correct = el_simple_node_corr
        el = Edgelist(
            event_col="event", time_col="timestamp", default_weight_col="event", index_col="user_id", nodelist=nl
        )
        result = el.calculate_edgelist(test_df, norm_type="node")
        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_edgelist__simple_full(
        self, test_df: pd.DataFrame, nl_simple_corr: pd.DataFrame, el_simple_full_corr: pd.DataFrame
    ) -> None:
        nl = nl_simple_corr
        correct = el_simple_full_corr
        el = Edgelist(
            event_col="event", time_col="timestamp", default_weight_col="event", index_col="user_id", nodelist=nl
        )
        result = el.calculate_edgelist(test_df, norm_type="full")
        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_edgelist__user_simple(
        self, test_df: pd.DataFrame, nl_user_corr: pd.DataFrame, el_user__corr: pd.DataFrame
    ) -> None:
        nl = nl_user_corr
        correct = el_user__corr
        el = Edgelist(
            event_col="event", time_col="timestamp", default_weight_col="user_id", index_col="user_id", nodelist=nl
        )
        result = el.calculate_edgelist(test_df)
        assert pd.testing.assert_frame_equal(result, correct) is None

    def test_edgelist__user_node(
        self, test_df: pd.DataFrame, nl_user_corr: pd.DataFrame, el_user_node_corr: pd.DataFrame
    ) -> None:
        nl = nl_user_corr
        correct = el_user_node_corr
        el = Edgelist(
            event_col="event", time_col="timestamp", default_weight_col="user_id", index_col="user_id", nodelist=nl
        )
        result = el.calculate_edgelist(test_df, norm_type="node")
        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_edgelist__user_full(
        self, test_df: pd.DataFrame, nl_user_corr: pd.DataFrame, el_user_full_corr: pd.DataFrame
    ) -> None:
        nl = nl_user_corr
        correct = el_user_full_corr
        el = Edgelist(
            event_col="event", time_col="timestamp", default_weight_col="user_id", index_col="user_id", nodelist=nl
        )
        result = el.calculate_edgelist(test_df, norm_type="full")
        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_edgelist__session_node(
        self, test_df: pd.DataFrame, nl_session_corr: pd.DataFrame, el_session_node_corr: pd.DataFrame
    ) -> None:
        nl = nl_session_corr
        correct = el_session_node_corr
        el = Edgelist(
            event_col="event", time_col="timestamp", default_weight_col="session_id", index_col="user_id", nodelist=nl
        )
        result = el.calculate_edgelist(test_df, norm_type="node")
        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_edgelist__session_full(
        self, test_df: pd.DataFrame, nl_session_corr: pd.DataFrame, el_session_full_corr: pd.DataFrame
    ) -> None:
        nl = nl_session_corr
        correct = el_session_full_corr
        el = Edgelist(
            event_col="event", time_col="timestamp", default_weight_col="session_id", index_col="user_id", nodelist=nl
        )
        result = el.calculate_edgelist(test_df, norm_type="full")
        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None
