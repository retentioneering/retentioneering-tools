from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def test_df_1() -> pd.DataFrame:
    df = pd.DataFrame(
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
    return df


@pytest.fixture
def test_df_2() -> pd.DataFrame:
    df = pd.DataFrame(
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
    return df


@pytest.fixture
def test_df_3() -> pd.DataFrame:
    df = pd.DataFrame(
        [
            [1, "custom_start", "2023-01-01 00:00:00"],
            [1, "A", "2023-01-01 00:00:01"],
            [1, "B", "2023-01-01 00:00:02"],
            [1, "custom_start", "2023-01-01 00:00:04"],
            [1, "A", "2023-01-01 00:00:04"],
            [2, "custom_start", "2023-01-01 00:00:00"],
            [2, "A", "2023-01-01 00:00:01"],
            [2, "custom_start", "2023-01-01 00:00:02"],
            [2, "custom_start", "2023-01-01 00:00:03"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    return df


@pytest.fixture
def test_df_4() -> pd.DataFrame:
    df = pd.DataFrame(
        [
            [1, "custom_start", "2023-01-01 00:00:00"],
            [1, "A", "2023-01-01 00:00:01"],
            [1, "B", "2023-01-01 00:00:02"],
            [1, "custom_end", "2023-01-01 00:00:02"],
            [1, "custom_start", "2023-01-01 00:00:04"],
            [1, "A", "2023-01-01 00:00:04"],
            [1, "custom_end", "2023-01-01 00:00:04"],
            [2, "custom_start", "2023-01-01 00:00:00"],
            [2, "A", "2023-01-01 00:00:01"],
            [2, "custom_end", "2023-01-01 00:00:01"],
            [2, "custom_start", "2023-01-01 00:00:02"],
            [2, "custom_end", "2023-01-01 00:00:02"],
            [2, "custom_start", "2023-01-01 00:00:03"],
            [2, "custom_end", "2023-01-01 00:00:03"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    return df


@pytest.fixture
def test_df_5() -> pd.DataFrame:
    df = pd.DataFrame(
        [
            [1, "A", "2023-01-01 00:00:00", "session_1"],
            [1, "B", "2023-01-01 00:00:01", "session_1"],
            [1, "C", "2023-01-01 00:00:02", "session_1"],
            [1, "D", "2023-01-01 00:00:02", "session_2"],
            [1, "E", "2023-01-01 00:00:04", "session_2"],
            [2, "A", "2023-01-01 00:00:00", "session_1"],
            [2, "A", "2023-01-01 00:00:01", "session_1"],
            [2, "B", "2023-01-01 00:00:01", "session_2"],
            [2, "C", "2023-01-01 00:00:02", "session_3"],
            [3, "A", "2023-01-01 00:00:00", "session_1"],
            [4, "A", "2023-01-01 00:00:00", "session_1"],
        ],
        columns=["user_id", "event", "timestamp", "session_id"],
    )
    return df
