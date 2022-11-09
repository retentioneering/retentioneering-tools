from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from src.data_processors_lib.rete import SplitSessions, SplitSessionsParams
from src.eventstream.schema import RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestSplitSessions(ApplyTestBase):
    _Processor = SplitSessions
    _source_df = pd.DataFrame(
        [
            [1, "pageview", "raw", "2021-10-26 12:00"],
            [1, "cart_btn_click", "raw", "2021-10-26 12:02"],
            [1, "pageview", "raw", "2021-10-26 12:03"],
            [1, "plus_icon_click", "raw", "2021-10-26 12:04"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_type="event_type",
        event_timestamp="timestamp",
    )

    def test_params_model__incorrect_datetime_unit(self):
        with pytest.raises(ValidationError):
            p = SplitSessionsParams(session_cutoff=(1, "xxx"))

    def test_split_session_apply_1(self):
        actual = self._apply(
            SplitSessionsParams(
                session_cutoff=(100, "s"),
                session_col="session_id",
            )
        )
        expected = pd.DataFrame(
            [
                [1, "session_start", "session_start", "2021-10-26 12:00:00"],
                [1, "pageview", "raw", "2021-10-26 12:00:00"],
                [1, "session_end", "session_end", "2021-10-26 12:00:00"],
                [1, "session_start", "session_start", "2021-10-26 12:02:00"],
                [1, "cart_btn_click", "raw", "2021-10-26 12:02:00"],
                [1, "pageview", "raw", "2021-10-26 12:03:00"],
                [1, "plus_icon_click", "raw", "2021-10-26 12:04:00"],
                [1, "session_end", "session_end", "2021-10-26 12:04:00"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_split_session_apply_2_mark_truncated_true(self) -> None:
        actual = self._apply(
            SplitSessionsParams(
                session_cutoff=(100, "s"),
                session_col="session_id",
                mark_truncated=True,
            )
        )
        expected = pd.DataFrame(
            [
                [1, "session_start", "session_start", "2021-10-26 12:00:00"],
                [1, "session_start_truncated", "session_start_truncated", "2021-10-26 12:00:00"],
                [1, "pageview", "raw", "2021-10-26 12:00:00"],
                [1, "session_end", "session_end", "2021-10-26 12:00:00"],
                [1, "session_start", "session_start", "2021-10-26 12:02:00"],
                [1, "cart_btn_click", "raw", "2021-10-26 12:02:00"],
                [1, "pageview", "raw", "2021-10-26 12:03:00"],
                [1, "plus_icon_click", "raw", "2021-10-26 12:04:00"],
                [1, "session_end_truncated", "session_end_truncated", "2021-10-26 12:04:00"],
                [1, "session_end", "session_end", "2021-10-26 12:04:00"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestSplitSessionsGraph(GraphTestBase):
    _Processor = SplitSessions
    _source_df = pd.DataFrame(
        [
            [111, "event1", "2022-01-01 00:00:00"],
            [111, "event2", "2022-01-01 00:01:00"],
            [111, "event3", "2022-01-01 00:33:00"],
            [111, "event4", "2022-01-01 00:34:00"],
            [222, "event1", "2022-01-01 00:30:00"],
            [222, "event2", "2022-01-01 00:31:00"],
            [222, "event3", "2022-01-01 01:01:00"],
            [333, "event1", "2022-01-01 01:00:00"],
            [333, "event2", "2022-01-01 01:01:00"],
            [333, "event3", "2022-01-01 01:32:00"],
            [333, "event4", "2022-01-01 01:33:00"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_split_sesssion_graph_1(self) -> None:
        actual = self._apply(
            SplitSessionsParams(
                session_cutoff=(30, "m"),
                session_col="session_id",
            )
        )
        expected = pd.DataFrame(
            [
                [111, "session_start", "session_start", "2022-01-01 00:00:00", "111_1"],
                [111, "event1", "raw", "2022-01-01 00:00:00", "111_1"],
                [111, "event2", "raw", "2022-01-01 00:01:00", "111_1"],
                [111, "session_end", "session_end", "2022-01-01 00:01:00", "111_1"],
                [222, "session_start", "session_start", "2022-01-01 00:30:00", "222_1"],
                [222, "event1", "raw", "2022-01-01 00:30:00", "222_1"],
                [222, "event2", "raw", "2022-01-01 00:31:00", "222_1"],
                [111, "session_start", "session_start", "2022-01-01 00:33:00", "111_2"],
                [111, "event3", "raw", "2022-01-01 00:33:00", "111_2"],
                [111, "event4", "raw", "2022-01-01 00:34:00", "111_2"],
                [111, "session_end", "session_end", "2022-01-01 00:34:00", "111_2"],
                [333, "session_start", "session_start", "2022-01-01 01:00:00", "333_1"],
                [333, "event1", "raw", "2022-01-01 01:00:00", "333_1"],
                [222, "event3", "raw", "2022-01-01 01:01:00", "222_1"],
                [333, "event2", "raw", "2022-01-01 01:01:00", "333_1"],
                [222, "session_end", "session_end", "2022-01-01 01:01:00", "222_1"],
                [333, "session_end", "session_end", "2022-01-01 01:01:00", "333_1"],
                [333, "session_start", "session_start", "2022-01-01 01:32:00", "333_2"],
                [333, "event3", "raw", "2022-01-01 01:32:00", "333_2"],
                [333, "event4", "raw", "2022-01-01 01:33:00", "333_2"],
                [333, "session_end", "session_end", "2022-01-01 01:33:00", "333_2"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp", "session_id"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_split_sesssion_graph_2_mark_truncated_true(self) -> None:
        actual = self._apply(
            SplitSessionsParams(
                session_cutoff=(30, "m"),
                session_col="session_id",
                mark_truncated=True,
            )
        )
        expected = pd.DataFrame(
            [
                [111, "session_start", "session_start", "2022-01-01 00:00:00", "111_1"],
                [111, "session_start_truncated", "session_start_truncated", "2022-01-01 00:00:00", "111_1"],
                [111, "event1", "raw", "2022-01-01 00:00:00", "111_1"],
                [111, "event2", "raw", "2022-01-01 00:01:00", "111_1"],
                [111, "session_end", "session_end", "2022-01-01 00:01:00", "111_1"],
                [222, "session_start", "session_start", "2022-01-01 00:30:00", "222_1"],
                [222, "event1", "raw", "2022-01-01 00:30:00", "222_1"],
                [222, "event2", "raw", "2022-01-01 00:31:00", "222_1"],
                [111, "session_start", "session_start", "2022-01-01 00:33:00", "111_2"],
                [111, "event3", "raw", "2022-01-01 00:33:00", "111_2"],
                [111, "event4", "raw", "2022-01-01 00:34:00", "111_2"],
                [111, "session_end", "session_end", "2022-01-01 00:34:00", "111_2"],
                [333, "session_start", "session_start", "2022-01-01 01:00:00", "333_1"],
                [333, "event1", "raw", "2022-01-01 01:00:00", "333_1"],
                [222, "event3", "raw", "2022-01-01 01:01:00", "222_1"],
                [333, "event2", "raw", "2022-01-01 01:01:00", "333_1"],
                [222, "session_end", "session_end", "2022-01-01 01:01:00", "222_1"],
                [333, "session_end", "session_end", "2022-01-01 01:01:00", "333_1"],
                [333, "session_start", "session_start", "2022-01-01 01:32:00", "333_2"],
                [333, "event3", "raw", "2022-01-01 01:32:00", "333_2"],
                [333, "event4", "raw", "2022-01-01 01:33:00", "333_2"],
                [333, "session_end_truncated", "session_end_truncated", "2022-01-01 01:33:00", "333_2"],
                [333, "session_end", "session_end", "2022-01-01 01:33:00", "333_2"],
            ],
            columns=["user_id", "event_name", "event_type", "event_timestamp", "session_id"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)
