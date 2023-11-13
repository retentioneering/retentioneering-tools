from __future__ import annotations

import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream, RawDataSchema


@pytest.fixture
def test_stream():
    source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00"],
            [1, "event2", "2022-01-01 00:01:02"],
            [1, "event1", "2022-01-01 00:02:00"],
            [1, "event1", "2022-01-01 00:03:00"],
            [1, "event1", "2022-01-01 00:03:01"],
            [1, "event3", "2022-01-01 00:03:30"],
            [1, "event1", "2022-01-01 00:04:00"],
            [1, "event3", "2022-01-01 00:04:30"],
            [1, "event1", "2022-01-01 00:05:50"],
            [2, "event1", "2022-01-02 00:00:00"],
            [2, "event2", "2022-01-02 00:00:05"],
            [2, "event2", "2022-01-02 00:01:09"],
            [3, "event1", "2022-01-02 00:01:10"],
            [3, "event1", "2022-01-02 00:02:05"],
            [3, "event3", "2022-01-02 00:04:30"],
            [3, "event4", "2022-01-02 00:05:05"],
            [4, "event1", "2022-01-02 00:06:10"],
            [4, "event1", "2022-01-02 00:07:05"],
            [4, "event1", "2022-01-02 00:08:05"],
            [4, "event2", "2022-01-02 00:09:05"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    stream = Eventstream(raw_data=source_df, add_start_end_events=False)
    return stream


@pytest.fixture
def test_stream_custom_col():
    source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00", "non_conv"],
            [1, "event2", "2022-01-01 00:01:02", "non_conv"],
            [1, "event1", "2022-01-01 00:02:00", "non_conv"],
            [1, "event1", "2022-01-01 00:03:00", "non_conv"],
            [1, "event1", "2022-01-01 00:03:01", "non_conv"],
            [1, "event3", "2022-01-01 00:03:30", "non_conv"],
            [1, "event1", "2022-01-01 00:04:00", "non_conv"],
            [1, "event3", "2022-01-01 00:04:30", "non_conv"],
            [1, "event1", "2022-01-01 00:05:50", "non_conv"],
            [2, "event1", "2022-01-02 00:00:00", "non_conv"],
            [2, "event2", "2022-01-02 00:00:05", "non_conv"],
            [2, "event2", "2022-01-02 00:01:09", "non_conv"],
            [3, "event1", "2022-01-02 00:01:10", "conv"],
            [3, "event1", "2022-01-02 00:02:05", "conv"],
            [3, "event3", "2022-01-02 00:04:30", "conv"],
            [3, "event4", "2022-01-02 00:05:05", "conv"],
            [4, "event1", "2022-01-02 00:06:10", "non_conv"],
            [4, "event1", "2022-01-02 00:07:05", "non_conv"],
            [4, "event1", "2022-01-02 00:08:05", "non_conv"],
            [4, "event2", "2022-01-02 00:09:05", "non_conv"],
        ],
        columns=["user_id", "event", "timestamp", "user_type"],
    )
    raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
        custom_cols=[{"custom_col": "user_type_col", "raw_data_col": "user_type"}],
    )
    stream = Eventstream(raw_data_schema=raw_data_schema, raw_data=source_df, add_start_end_events=False)
    return stream
