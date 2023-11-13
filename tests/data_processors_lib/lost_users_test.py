from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from retentioneering.data_processors_lib import LabelLostUsers, LabelLostUsersParams
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestLabelLostUsers(ApplyTestBase):
    _Processor = LabelLostUsers
    _source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00"],
            [1, "event1", "2022-01-01 00:02:00"],
            [1, "event2", "2022-01-01 00:01:02"],
            [1, "event1", "2022-01-01 00:03:00"],
            [1, "event1", "2022-01-01 00:04:00"],
            [1, "event1", "2022-01-01 00:05:00"],
            [2, "event1", "2022-01-02 00:00:00"],
            [2, "event1", "2022-01-02 00:00:05"],
            [2, "event2", "2022-01-02 00:00:05"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_label_lost_users_apply__lost_users_list(self):
        actual = self._apply_dataprocessor(
            params=LabelLostUsersParams(
                lost_users_list=[2],
                timeout=None,
            )
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [1, "absent_user", "absent_user", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
                [2, "lost_user", "lost_user", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_label_lost_users_apply__timeout(self):
        actual = self._apply_dataprocessor(
            params=LabelLostUsersParams(
                timeout=(4, "h"),
                lost_users_list=None,
            )
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [1, "lost_user", "lost_user", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
                [2, "absent_user", "absent_user", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_params_model__incorrect_datetime_unit(self):
        with pytest.raises(ValidationError):
            p = LabelLostUsersParams(timeout=(1, "xxx"))


class TestLabelLostUsersGraph(GraphTestBase):
    _Processor = LabelLostUsers
    _source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00"],
            [1, "event1", "2022-01-01 00:02:00"],
            [1, "event2", "2022-01-01 00:01:02"],
            [1, "event1", "2022-01-01 00:03:00"],
            [1, "event1", "2022-01-01 00:04:00"],
            [1, "event1", "2022-01-01 00:05:00"],
            [2, "event1", "2022-01-02 00:00:00"],
            [2, "event1", "2022-01-02 00:00:05"],
            [2, "event2", "2022-01-02 00:00:05"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_label_lost_users_graph__lost_users_list(self):
        actual = self._apply(LabelLostUsersParams(lost_users_list=[2]))
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [1, "absent_user", "absent_user", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
                [2, "lost_user", "lost_user", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_label_lost_users_graph__timeout(self):
        actual = self._apply(LabelLostUsersParams(timeout=(4, "h")))
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [1, "lost_user", "lost_user", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
                [2, "absent_user", "absent_user", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestLabelLostUsersHelper:
    def test_label_lost_users_graph__lost_users_list(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        source = Eventstream(source_df, add_start_end_events=False)

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [1, "absent_user", "absent_user", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
                [2, "lost_user", "lost_user", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )

        result = source.label_lost_users(lost_users_list=[2])
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_label_lost_users_graph__timeout(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        source = Eventstream(source_df, add_start_end_events=False)

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [1, "lost_user", "lost_user", "2022-01-01 00:05:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
                [2, "absent_user", "absent_user", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )
        result = source.label_lost_users(timeout=(4, "h"))
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)
