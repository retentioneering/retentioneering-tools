from __future__ import annotations

import pytest
import pandas as pd

from pydantic import ValidationError
from src.eventstream.schema import RawDataSchema
from src.data_processors_lib.rete import (
    LostUsersEvents,
    LostUsersParams,
)
from tests.data_processors_lib.common import (
    ApplyTestBase,
    GraphTestBase,
)


class TestLostUsers(ApplyTestBase):
    _Processor = LostUsersEvents
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

    def test_lost_users_apply__lost_users_list(self):
        actual = self._apply(LostUsersParams(
            lost_users_list=[2],
            lost_cutoff=None,
        ))
        expected = pd.DataFrame(
            [
                [1, "absent_user", "absent_user", "2022-01-01 00:05:00"],
                [2, "lost_user", "lost_user", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_lost_users_apply__lost_cutoff(self):
        actual = self._apply(LostUsersParams(
            lost_users_list=None,
            lost_cutoff=(4, "h"),
        ))
        expected = pd.DataFrame(
            [
                [1, "lost_user", "lost_user", "2022-01-01 00:05:00"],
                [2, "absent_user", "absent_user", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_params_model__incorrect_datetime_unit(self):
        with pytest.raises(ValidationError):
            p = LostUsersParams(lost_cutoff=(1, "xxx"))


class TestLostUsersGraph(GraphTestBase):
    _Processor = LostUsersEvents
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

    def test_lost_users_graph__lost_users_list(self):
        actual = self._apply(LostUsersParams(
            lost_users_list=[2],
            lost_cutoff=None,
        ))
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
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_lost_users_graph__lost_cutoff(self):
        actual = self._apply(LostUsersParams(
            lost_users_list=None,
            lost_cutoff=(4, "h"),
        ))
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
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)
