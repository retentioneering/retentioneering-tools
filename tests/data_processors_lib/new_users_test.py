from __future__ import annotations

import pandas as pd

from retentioneering.data_processors_lib import LabelNewUsers, LabelNewUsersParams
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestNewUsers(ApplyTestBase):
    _Processor = LabelNewUsers
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
            [2, "event2", "2022-01-02 00:01:05"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_label_new_users__apply__new_users_list_id(self):
        actual = self._apply_dataprocessor(
            params=LabelNewUsersParams(
                new_users_list=[2],
            )
        )
        expected = pd.DataFrame(
            [
                [1, "existing_user", "existing_user", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "new_user", "new_user", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_label_new_users__apply__new_users_list_all(self):
        actual = self._apply_dataprocessor(
            params=LabelNewUsersParams(
                new_users_list="all",
            )
        )
        expected = pd.DataFrame(
            [
                [1, "new_user", "new_user", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "new_user", "new_user", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_label_new_users__apply__new_users_list_id_str(self):
        source_df = pd.DataFrame(
            [
                ["user111", "event1", "2022-01-01 00:01:00"],
                ["user111", "event1", "2022-01-01 00:02:00"],
                ["user111", "event2", "2022-01-01 00:01:02"],
                ["user111", "event1", "2022-01-01 00:03:00"],
                ["user111", "event1", "2022-01-01 00:04:00"],
                ["user111", "event1", "2022-01-01 00:05:00"],
                ["user222", "event1", "2022-01-02 00:00:00"],
                ["user222", "event1", "2022-01-02 00:00:05"],
                ["user222", "event2", "2022-01-02 00:01:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        actual = self._apply_dataprocessor(
            params=LabelNewUsersParams(
                new_users_list=["user222"],
            ),
            source_df=source_df,
        )
        expected = pd.DataFrame(
            [
                ["user111", "existing_user", "existing_user", "2022-01-01 00:01:00"],
                ["user111", "event1", "raw", "2022-01-01 00:01:00"],
                ["user111", "event2", "raw", "2022-01-01 00:01:02"],
                ["user111", "event1", "raw", "2022-01-01 00:02:00"],
                ["user111", "event1", "raw", "2022-01-01 00:03:00"],
                ["user111", "event1", "raw", "2022-01-01 00:04:00"],
                ["user111", "event1", "raw", "2022-01-01 00:05:00"],
                ["user222", "new_user", "new_user", "2022-01-02 00:00:00"],
                ["user222", "event1", "raw", "2022-01-02 00:00:00"],
                ["user222", "event1", "raw", "2022-01-02 00:00:05"],
                ["user222", "event2", "raw", "2022-01-02 00:01:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestLabelNewUsersGraph(GraphTestBase):
    _Processor = LabelNewUsers
    _source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00"],
            [1, "event2", "2022-01-01 00:01:02"],
            [1, "event1", "2022-01-01 00:02:00"],
            [1, "event1", "2022-01-01 00:03:00"],
            [1, "event1", "2022-01-01 00:04:00"],
            [1, "event1", "2022-01-01 00:05:00"],
            [2, "event1", "2022-01-02 00:00:00"],
            [2, "event1", "2022-01-02 00:00:05"],
            [2, "event2", "2022-01-02 00:01:05"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_label_new_users_graph__new_users_list_id(self):
        actual = self._apply(
            LabelNewUsersParams(
                new_users_list=[2],
            )
        )
        expected = pd.DataFrame(
            [
                [1, "existing_user", "existing_user", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "new_user", "new_user", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_label_new_users_graph__new_users_list_all(self):
        actual = self._apply(
            LabelNewUsersParams(
                new_users_list="all",
            )
        )
        expected = pd.DataFrame(
            [
                [1, "new_user", "new_user", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "new_user", "new_user", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_label_new_users__helper__new_users_list_id_str(self):
        source_df = pd.DataFrame(
            [
                ["user111", "event1", "2022-01-01 00:01:00"],
                ["user111", "event1", "2022-01-01 00:02:00"],
                ["user111", "event2", "2022-01-01 00:01:02"],
                ["user111", "event1", "2022-01-01 00:03:00"],
                ["user111", "event1", "2022-01-01 00:04:00"],
                ["user111", "event1", "2022-01-01 00:05:00"],
                ["user222", "event1", "2022-01-02 00:00:00"],
                ["user222", "event1", "2022-01-02 00:00:05"],
                ["user222", "event2", "2022-01-02 00:01:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        actual = self._apply(
            LabelNewUsersParams(
                new_users_list=["user222"],
            ),
            source_df=source_df,
        )
        expected = pd.DataFrame(
            [
                ["user111", "existing_user", "existing_user", "2022-01-01 00:01:00"],
                ["user111", "event1", "raw", "2022-01-01 00:01:00"],
                ["user111", "event2", "raw", "2022-01-01 00:01:02"],
                ["user111", "event1", "raw", "2022-01-01 00:02:00"],
                ["user111", "event1", "raw", "2022-01-01 00:03:00"],
                ["user111", "event1", "raw", "2022-01-01 00:04:00"],
                ["user111", "event1", "raw", "2022-01-01 00:05:00"],
                ["user222", "new_user", "new_user", "2022-01-02 00:00:00"],
                ["user222", "event1", "raw", "2022-01-02 00:00:00"],
                ["user222", "event1", "raw", "2022-01-02 00:00:05"],
                ["user222", "event2", "raw", "2022-01-02 00:01:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestLabelNewUsersHelper:
    def test_label_new_users_graph__new_users_list_id(self):
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
                [2, "event2", "2022-01-02 00:01:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        source = Eventstream(source_df)

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = pd.DataFrame(
            [
                [1, "existing_user", "existing_user", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "new_user", "new_user", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
            ],
            columns=correct_result_columns,
        )

        result = source.label_new_users(new_users_list=[2])
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_label_new_users_graph__new_users_list_all(self):
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
                [2, "event2", "2022-01-02 00:01:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        source = Eventstream(source_df)

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = pd.DataFrame(
            [
                [1, "new_user", "new_user", "2022-01-01 00:01:00"],
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "raw", "2022-01-01 00:02:00"],
                [1, "event1", "raw", "2022-01-01 00:03:00"],
                [1, "event1", "raw", "2022-01-01 00:04:00"],
                [1, "event1", "raw", "2022-01-01 00:05:00"],
                [2, "new_user", "new_user", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:00"],
                [2, "event1", "raw", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:01:05"],
            ],
            columns=correct_result_columns,
        )
        result = source.label_new_users(new_users_list="all")
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)

    def test_label_new_users__helper__new_users_list_id_str(self):
        source_df = pd.DataFrame(
            [
                ["user111", "event1", "2022-01-01 00:01:00"],
                ["user111", "event1", "2022-01-01 00:02:00"],
                ["user111", "event2", "2022-01-01 00:01:02"],
                ["user111", "event1", "2022-01-01 00:03:00"],
                ["user111", "event1", "2022-01-01 00:04:00"],
                ["user111", "event1", "2022-01-01 00:05:00"],
                ["user222", "event1", "2022-01-02 00:00:00"],
                ["user222", "event1", "2022-01-02 00:00:05"],
                ["user222", "event2", "2022-01-02 00:01:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        source = Eventstream(source_df)

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = pd.DataFrame(
            [
                ["user111", "existing_user", "existing_user", "2022-01-01 00:01:00"],
                ["user111", "event1", "raw", "2022-01-01 00:01:00"],
                ["user111", "event2", "raw", "2022-01-01 00:01:02"],
                ["user111", "event1", "raw", "2022-01-01 00:02:00"],
                ["user111", "event1", "raw", "2022-01-01 00:03:00"],
                ["user111", "event1", "raw", "2022-01-01 00:04:00"],
                ["user111", "event1", "raw", "2022-01-01 00:05:00"],
                ["user222", "new_user", "new_user", "2022-01-02 00:00:00"],
                ["user222", "event1", "raw", "2022-01-02 00:00:00"],
                ["user222", "event1", "raw", "2022-01-02 00:00:05"],
                ["user222", "event2", "raw", "2022-01-02 00:01:05"],
            ],
            columns=correct_result_columns,
        )
        result = source.label_new_users(new_users_list=["user222"])
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)
        assert result_df.compare(correct_result).shape == (0, 0)
