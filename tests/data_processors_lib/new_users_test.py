from __future__ import annotations

import pandas as pd

from src.data_processors_lib.rete import NewUsersEvents, NewUsersParams
from src.eventstream.schema import RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestNewUsers(ApplyTestBase):
    _Processor = NewUsersEvents
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

    def test_new_users__apply__new_users_list_id(self):
        actual = self._apply(
            NewUsersParams(
                new_users_list=[2],
            )
        )
        expected = pd.DataFrame(
            [
                [1, "existing_user", "existing_user", "2022-01-01 00:01:00"],
                [2, "new_user", "new_user", "2022-01-02 00:00:00"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_new_users__apply__new_users_list_all(self):
        actual = self._apply(
            NewUsersParams(
                new_users_list="all",
            )
        )
        expected = pd.DataFrame(
            [
                [1, "new_user", "new_user", "2022-01-01 00:01:00"],
                [2, "new_user", "new_user", "2022-01-02 00:00:00"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_new_users__apply__new_users_list_id_str(self):
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
            NewUsersParams(
                new_users_list=["user222"],
            ),
            source_df=source_df,
        )
        expected = pd.DataFrame(
            [
                ["user111", "existing_user", "existing_user", "2022-01-01 00:01:00"],
                ["user222", "new_user", "new_user", "2022-01-02 00:00:00"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestNewUsersGraph(GraphTestBase):
    _Processor = NewUsersEvents
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

    def test_new_users_graph__new_users_list_id(self):
        actual = self._apply(
            NewUsersParams(
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
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_new_users_graph__new_users_list_all(self):
        actual = self._apply(
            NewUsersParams(
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
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_new_users__helper__new_users_list_id_str(self):
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
            NewUsersParams(
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
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)
