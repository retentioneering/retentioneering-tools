from __future__ import annotations

import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream, RawDataSchema


@pytest.fixture
def test_stream():
    source_df = pd.DataFrame(
        [
            [1, "path_start", "path_start", "2022-01-01 00:01:00"],
            [1, "event1", "raw", "2022-01-01 00:01:00"],
            [1, "event2", "raw", "2022-01-01 00:01:02"],
            [1, "event1", "raw", "2022-01-01 00:02:00"],
            [1, "event1", "raw", "2022-01-01 00:03:00"],
            [1, "event1", "synthetic", "2022-01-01 00:03:00"],
            [1, "session_start", "session_start", "2022-01-01 00:03:30"],
            [1, "event3", "raw", "2022-01-01 00:03:30"],
            [1, "event1", "raw", "2022-01-01 00:04:00"],
            [1, "event3", "raw", "2022-01-01 00:04:30"],
            [1, "event1", "raw", "2022-01-01 00:05:00"],
            [2, "event1", "raw", "2022-01-02 00:00:00"],
            [2, "event3", "raw", "2022-01-02 00:00:05"],
            [2, "event2", "raw", "2022-01-02 00:01:05"],
            [2, "path_end", "path_end", "2022-01-02 00:01:05"],
            [3, "event1", "raw", "2022-01-02 00:01:10"],
            [3, "event1", "raw", "2022-01-02 00:02:05"],
            [3, "event4", "raw", "2022-01-02 00:03:05"],
            [3, "path_end", "path_end", "2022-01-02 00:03:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_type="event_type",
        event_timestamp="timestamp",
    )
    return Eventstream(source_df, raw_data_schema=raw_data_schema, add_start_end_events=False)
