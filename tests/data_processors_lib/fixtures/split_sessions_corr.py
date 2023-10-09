from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def basic_corr() -> pd.DataFrame:
    df = pd.DataFrame(
        [
            [111, "session_start", "session_start", "2022-01-01 00:00:00", "111_1"],
            [111, "event1", "raw", "2022-01-01 00:00:00", "111_1"],
            [111, "event2", "raw", "2022-01-01 00:01:00", "111_1"],
            [111, "session_end", "session_end", "2022-01-01 00:01:00", "111_1"],
            [111, "session_start", "session_start", "2022-01-01 00:33:00", "111_2"],
            [111, "event3", "raw", "2022-01-01 00:33:00", "111_2"],
            [111, "event4", "raw", "2022-01-01 00:34:00", "111_2"],
            [111, "session_end", "session_end", "2022-01-01 00:34:00", "111_2"],
            [222, "session_start", "session_start", "2022-01-01 00:30:00", "222_1"],
            [222, "event1", "raw", "2022-01-01 00:30:00", "222_1"],
            [222, "event2", "raw", "2022-01-01 00:31:00", "222_1"],
            [222, "event3", "raw", "2022-01-01 01:01:00", "222_1"],
            [222, "session_end", "session_end", "2022-01-01 01:01:00", "222_1"],
            [333, "session_start", "session_start", "2022-01-01 01:00:00", "333_1"],
            [333, "event1", "raw", "2022-01-01 01:00:00", "333_1"],
            [333, "event2", "raw", "2022-01-01 01:01:00", "333_1"],
            [333, "session_end", "session_end", "2022-01-01 01:01:00", "333_1"],
            [333, "session_start", "session_start", "2022-01-01 01:32:00", "333_2"],
            [333, "event3", "raw", "2022-01-01 01:32:00", "333_2"],
            [333, "event4", "raw", "2022-01-01 01:33:00", "333_2"],
            [333, "session_end", "session_end", "2022-01-01 01:33:00", "333_2"],
        ],
        columns=["user_id", "event", "event_type", "timestamp", "session_id"],
    )
    return df


@pytest.fixture
def mark_truncated_true_corr() -> pd.DataFrame:
    df = pd.DataFrame(
        [
            [111, "session_start", "session_start", "2022-01-01 00:00:00", "111_1"],
            [111, "session_start_cropped", "session_start_cropped", "2022-01-01 00:00:00", "111_1"],
            [111, "event1", "raw", "2022-01-01 00:00:00", "111_1"],
            [111, "event2", "raw", "2022-01-01 00:01:00", "111_1"],
            [111, "session_end", "session_end", "2022-01-01 00:01:00", "111_1"],
            [111, "session_start", "session_start", "2022-01-01 00:33:00", "111_2"],
            [111, "event3", "raw", "2022-01-01 00:33:00", "111_2"],
            [111, "event4", "raw", "2022-01-01 00:34:00", "111_2"],
            [111, "session_end", "session_end", "2022-01-01 00:34:00", "111_2"],
            [222, "session_start", "session_start", "2022-01-01 00:30:00", "222_1"],
            [222, "event1", "raw", "2022-01-01 00:30:00", "222_1"],
            [222, "event2", "raw", "2022-01-01 00:31:00", "222_1"],
            [222, "event3", "raw", "2022-01-01 01:01:00", "222_1"],
            [222, "session_end", "session_end", "2022-01-01 01:01:00", "222_1"],
            [333, "session_start", "session_start", "2022-01-01 01:00:00", "333_1"],
            [333, "event1", "raw", "2022-01-01 01:00:00", "333_1"],
            [333, "event2", "raw", "2022-01-01 01:01:00", "333_1"],
            [333, "session_end", "session_end", "2022-01-01 01:01:00", "333_1"],
            [333, "session_start", "session_start", "2022-01-01 01:32:00", "333_2"],
            [333, "event3", "raw", "2022-01-01 01:32:00", "333_2"],
            [333, "event4", "raw", "2022-01-01 01:33:00", "333_2"],
            [333, "session_end_cropped", "session_end_cropped", "2022-01-01 01:33:00", "333_2"],
            [333, "session_end", "session_end", "2022-01-01 01:33:00", "333_2"],
        ],
        columns=["user_id", "event", "event_type", "timestamp", "session_id"],
    )
    return df


@pytest.fixture
def one_delimiter_event_corr() -> pd.DataFrame:
    expected = pd.DataFrame(
        [
            [1, "session_start", "session_start", "2023-01-01 00:00:00"],
            [1, "A", "raw", "2023-01-01 00:00:01"],
            [1, "B", "raw", "2023-01-01 00:00:02"],
            [1, "session_end", "session_end", "2023-01-01 00:00:02"],
            [1, "session_start", "session_start", "2023-01-01 00:00:04"],
            [1, "A", "raw", "2023-01-01 00:00:04"],
            [1, "session_end", "session_end", "2023-01-01 00:00:04"],
            [2, "session_start", "session_start", "2023-01-01 00:00:00"],
            [2, "A", "raw", "2023-01-01 00:00:01"],
            [2, "session_end", "session_end", "2023-01-01 00:00:01"],
            [2, "session_start", "session_start", "2023-01-01 00:00:02"],
            [2, "session_end", "session_end", "2023-01-01 00:00:02"],
            [2, "session_start", "session_start", "2023-01-01 00:00:03"],
            [2, "session_end", "session_end", "2023-01-01 00:00:03"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    return expected


@pytest.fixture
def two_delimiter_events_corr() -> pd.DataFrame:
    expected = pd.DataFrame(
        [
            [1, "session_start", "session_start", "2023-01-01 00:00:00"],
            [1, "A", "raw", "2023-01-01 00:00:01"],
            [1, "B", "raw", "2023-01-01 00:00:02"],
            [1, "session_end", "session_end", "2023-01-01 00:00:02"],
            [1, "session_start", "session_start", "2023-01-01 00:00:04"],
            [1, "A", "raw", "2023-01-01 00:00:04"],
            [1, "session_end", "session_end", "2023-01-01 00:00:04"],
            [2, "session_start", "session_start", "2023-01-01 00:00:00"],
            [2, "A", "raw", "2023-01-01 00:00:01"],
            [2, "session_end", "session_end", "2023-01-01 00:00:01"],
            [2, "session_start", "session_start", "2023-01-01 00:00:02"],
            [2, "session_end", "session_end", "2023-01-01 00:00:02"],
            [2, "session_start", "session_start", "2023-01-01 00:00:03"],
            [2, "session_end", "session_end", "2023-01-01 00:00:03"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    return expected


@pytest.fixture
def delimiter_col_corr() -> pd.DataFrame:
    expected = pd.DataFrame(
        [
            [1, "session_start", "session_start", "2023-01-01 00:00:00", "1_1"],
            [1, "A", "raw", "2023-01-01 00:00:00", "1_1"],
            [1, "B", "raw", "2023-01-01 00:00:01", "1_1"],
            [1, "C", "raw", "2023-01-01 00:00:02", "1_1"],
            [1, "session_end", "session_end", "2023-01-01 00:00:02", "1_1"],
            [1, "session_start", "session_start", "2023-01-01 00:00:02", "1_2"],
            [1, "D", "raw", "2023-01-01 00:00:02", "1_2"],
            [1, "E", "raw", "2023-01-01 00:00:04", "1_2"],
            [1, "session_end", "session_end", "2023-01-01 00:00:04", "1_2"],
            [2, "session_start", "session_start", "2023-01-01 00:00:00", "2_1"],
            [2, "A", "raw", "2023-01-01 00:00:00", "2_1"],
            [2, "A", "raw", "2023-01-01 00:00:01", "2_1"],
            [2, "session_end", "session_end", "2023-01-01 00:00:01", "2_1"],
            [2, "session_start", "session_start", "2023-01-01 00:00:01", "2_2"],
            [2, "B", "raw", "2023-01-01 00:00:01", "2_2"],
            [2, "session_end", "session_end", "2023-01-01 00:00:01", "2_2"],
            [2, "session_start", "session_start", "2023-01-01 00:00:02", "2_3"],
            [2, "C", "raw", "2023-01-01 00:00:02", "2_3"],
            [2, "session_end", "session_end", "2023-01-01 00:00:02", "2_3"],
            [3, "session_start", "session_start", "2023-01-01 00:00:00", "3_1"],
            [3, "A", "raw", "2023-01-01 00:00:00", "3_1"],
            [3, "session_end", "session_end", "2023-01-01 00:00:00", "3_1"],
            [4, "session_start", "session_start", "2023-01-01 00:00:00", "4_1"],
            [4, "A", "raw", "2023-01-01 00:00:00", "4_1"],
            [4, "session_end", "session_end", "2023-01-01 00:00:00", "4_1"],
        ],
        columns=["user_id", "event", "event_type", "timestamp", "session_id"],
    )
    return expected
