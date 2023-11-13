from __future__ import annotations

import pandas as pd
import pytest

from retentioneering.edgelist import Edgelist
from retentioneering.eventstream import Eventstream, RawDataSchema
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
        stream = Eventstream(test_df, add_start_end_events=False)
        correct = el_simple_corr
        el = Edgelist(eventstream=stream)
        result = el.calculate_edgelist(weight_cols=["event_id"])
        assert pd.testing.assert_frame_equal(result, correct) is None


class TestVerifyEdgelist:
    def test_edgelist__session_simple(
        self, test_df: pd.DataFrame, nl_session_corr: pd.DataFrame, el_session_corr: pd.DataFrame
    ) -> None:
        raw_data_schema = RawDataSchema(
            user_id="user_id",
            event_name="event",
            event_timestamp="timestamp",
            custom_cols=[{"custom_col": "session_id", "raw_data_col": "session_id"}],
        )
        stream = Eventstream(test_df, raw_data_schema=raw_data_schema, add_start_end_events=False)
        nl = nl_session_corr
        correct = el_session_corr
        el = Edgelist(eventstream=stream)
        result = el.calculate_edgelist(weight_cols=["session_id"])
        assert pd.testing.assert_frame_equal(result, correct) is None

    def test_edgelist__simple_node(
        self, test_df: pd.DataFrame, nl_simple_corr: pd.DataFrame, el_simple_node_corr: pd.DataFrame
    ) -> None:
        stream = Eventstream(test_df, add_start_end_events=False)
        nl = nl_simple_corr
        correct = el_simple_node_corr
        el = Edgelist(eventstream=stream)
        result = el.calculate_edgelist(norm_type="node", weight_cols=["event_id"])
        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_edgelist__simple_full(
        self, test_df: pd.DataFrame, nl_simple_corr: pd.DataFrame, el_simple_full_corr: pd.DataFrame
    ) -> None:
        stream = Eventstream(test_df, add_start_end_events=False)
        nl = nl_simple_corr
        correct = el_simple_full_corr
        el = Edgelist(eventstream=stream)
        result = el.calculate_edgelist(norm_type="full", weight_cols=["event_id"])
        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_edgelist__user_simple(
        self, test_df: pd.DataFrame, nl_user_corr: pd.DataFrame, el_user__corr: pd.DataFrame
    ) -> None:
        stream = Eventstream(test_df, add_start_end_events=False)

        nl = nl_user_corr
        correct = el_user__corr
        el = Edgelist(eventstream=stream)
        result = el.calculate_edgelist(weight_cols=["user_id"])
        assert pd.testing.assert_frame_equal(result, correct) is None

    def test_edgelist__user_node(
        self, test_df: pd.DataFrame, nl_user_corr: pd.DataFrame, el_user_node_corr: pd.DataFrame
    ) -> None:
        nl = nl_user_corr
        correct = el_user_node_corr
        stream = Eventstream(test_df, add_start_end_events=False)
        el = Edgelist(eventstream=stream)
        result = el.calculate_edgelist(weight_cols=["user_id"], norm_type="node")
        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_edgelist__user_full(
        self, test_df: pd.DataFrame, nl_user_corr: pd.DataFrame, el_user_full_corr: pd.DataFrame
    ) -> None:
        stream = Eventstream(test_df, add_start_end_events=False)
        nl = nl_user_corr
        correct = el_user_full_corr
        el = Edgelist(eventstream=stream)
        result = el.calculate_edgelist(weight_cols=["user_id"], norm_type="full")
        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_edgelist__session_node(
        self, test_df: pd.DataFrame, nl_session_corr: pd.DataFrame, el_session_node_corr: pd.DataFrame
    ) -> None:
        nl = nl_session_corr
        correct = el_session_node_corr
        raw_data_schema = RawDataSchema(
            user_id="user_id",
            event_name="event",
            event_timestamp="timestamp",
            custom_cols=[{"custom_col": "session_id", "raw_data_col": "session_id"}],
        )
        stream = Eventstream(test_df, raw_data_schema=raw_data_schema, add_start_end_events=False)
        el = Edgelist(eventstream=stream)
        result = el.calculate_edgelist(weight_cols=["session_id"], norm_type="node")
        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_edgelist__session_full(
        self, test_df: pd.DataFrame, nl_session_corr: pd.DataFrame, el_session_full_corr: pd.DataFrame
    ) -> None:
        nl = nl_session_corr
        correct = el_session_full_corr
        raw_data_schema = RawDataSchema(
            user_id="user_id",
            event_name="event",
            event_timestamp="timestamp",
            custom_cols=[{"custom_col": "session_id", "raw_data_col": "session_id"}],
        )
        stream = Eventstream(test_df, raw_data_schema=raw_data_schema, add_start_end_events=False)
        el = Edgelist(eventstream=stream)
        result = el.calculate_edgelist(weight_cols=["session_id"], norm_type="full")
        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None
