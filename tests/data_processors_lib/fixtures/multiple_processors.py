from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def multiple_processors_source():
    return pd.DataFrame(
        [
            [1, "event1", "raw", "2021-10-26 12:00:00"],
            [1, "event3", "raw", "2021-10-26 12:00:01"],
            [1, "event2", "raw", "2021-10-26 12:02:00"],
            [1, "event2", "raw", "2021-10-26 12:03:00"],
            [1, "event2", "raw", "2021-10-26 12:04:00"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )


@pytest.fixture
def split_session_and_group_corr():
    corr_result = pd.DataFrame(
        [
            [1, "session_start", "session_start", "2021-10-26 12:00:00", 0],
            [1, "event13", "group_alias", "2021-10-26 12:00:00", 0],
            [1, "event13", "group_alias", "2021-10-26 12:00:01", 1],
            [1, "session_end", "session_end", "2021-10-26 12:00:01", 1],
            [1, "session_start", "session_start", "2021-10-26 12:02:00", 2],
            [1, "event2", "raw", "2021-10-26 12:02:00", 2],
            [1, "event2", "raw", "2021-10-26 12:03:00", 3],
            [1, "event2", "raw", "2021-10-26 12:04:00", 4],
            [1, "session_end", "session_end", "2021-10-26 12:04:00", 4],
        ],
        columns=["user_id", "event", "event_type", "timestamp", "event_index"],
    )
    corr_result["timestamp"] = pd.to_datetime(corr_result["timestamp"])
    return corr_result
