from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def new_lost_corr():
    correct_result_columns = ["user_id", "event", "event_type", "timestamp", "event_index"]

    correct_result = pd.DataFrame(
        [
            [1, "new_user", "new_user", "2022-01-01 00:01:00", 0],
            [1, "event1", "raw", "2022-01-01 00:01:00", 1],
            [1, "event2", "raw", "2022-01-01 00:01:02", 2],
            [1, "event1", "raw", "2022-01-01 00:02:00", 3],
            [1, "event1", "raw", "2022-01-01 00:03:00", 4],
            [1, "event1", "raw", "2022-01-01 00:03:01", 5],
            [1, "event3", "raw", "2022-01-01 00:03:30", 6],
            [1, "event1", "raw", "2022-01-01 00:04:00", 7],
            [1, "event3", "raw", "2022-01-01 00:04:30", 8],
            [1, "absent_user", "absent_user", "2022-01-01 00:04:30", 9],
            [2, "existing_user", "existing_user", "2022-01-02 00:00:00", 12],
            [2, "event1", "raw", "2022-01-02 00:00:00", 13],
            [2, "event2", "raw", "2022-01-02 00:00:05", 14],
            [2, "event2", "raw", "2022-01-02 00:01:09", 15],
            [2, "absent_user", "absent_user", "2022-01-02 00:01:09", 16],
            [3, "existing_user", "existing_user", "2022-01-02 00:01:10", 17],
            [3, "event1", "raw", "2022-01-02 00:01:10", 18],
            [3, "event1", "raw", "2022-01-02 00:02:05", 19],
            [3, "event3", "raw", "2022-01-02 00:04:30", 20],
            [3, "absent_user", "absent_user", "2022-01-02 00:04:30", 21],
            [4, "existing_user", "existing_user", "2022-01-02 00:06:10", 24],
            [4, "event1", "raw", "2022-01-02 00:06:10", 25],
            [4, "event1", "raw", "2022-01-02 00:07:05", 26],
            [4, "event1", "raw", "2022-01-02 00:08:05", 27],
            [4, "event2", "raw", "2022-01-02 00:09:05", 28],
            [4, "absent_user", "absent_user", "2022-01-02 00:09:05", 29],
        ],
        columns=correct_result_columns,
    )
    correct_result["timestamp"] = pd.to_datetime(correct_result["timestamp"])
    return correct_result


@pytest.fixture
def split_start_end_corr():
    correct_result_columns = ["user_id", "event", "event_type", "timestamp", "event_index", "session_id"]

    correct_result = pd.DataFrame(
        [
            [1, "path_start", "path_start", "2022-01-01 00:01:00", 0, "1_1"],
            [1, "session_start", "session_start", "2022-01-01 00:01:00", 1, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:01:00", 2, "1_1"],
            [1, "event2", "raw", "2022-01-01 00:01:02", 4, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:02:00", 6, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:03:00", 8, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:03:01", 10, "1_1"],
            [1, "event3", "raw", "2022-01-01 00:03:30", 12, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:04:00", 14, "1_1"],
            [1, "event3", "raw", "2022-01-01 00:04:30", 16, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:05:50", 18, "1_1"],
            [1, "session_end", "session_end", "2022-01-01 00:05:50", 20, "1_1"],
            [1, "path_end", "path_end", "2022-01-01 00:05:50", 21, "1_1"],
            [2, "path_start", "path_start", "2022-01-02 00:00:00", 22, "2_1"],
            [2, "session_start", "session_start", "2022-01-02 00:00:00", 23, "2_1"],
            [2, "event1", "raw", "2022-01-02 00:00:00", 24, "2_1"],
            [2, "event2", "raw", "2022-01-02 00:00:05", 26, "2_1"],
            [2, "event2", "raw", "2022-01-02 00:01:09", 28, "2_1"],
            [2, "session_end", "session_end", "2022-01-02 00:01:09", 30, "2_1"],
            [2, "path_end", "path_end", "2022-01-02 00:01:09", 31, "2_1"],
            [3, "path_start", "path_start", "2022-01-02 00:01:10", 32, "3_1"],
            [3, "session_start", "session_start", "2022-01-02 00:01:10", 33, "3_1"],
            [3, "event1", "raw", "2022-01-02 00:01:10", 34, "3_1"],
            [3, "event1", "raw", "2022-01-02 00:02:05", 36, "3_1"],
            [3, "session_end", "session_end", "2022-01-02 00:02:05", 38, "3_1"],
            [3, "session_start", "session_start", "2022-01-02 00:04:30", 39, "3_2"],
            [3, "event3", "raw", "2022-01-02 00:04:30", 40, "3_2"],
            [3, "event4", "raw", "2022-01-02 00:05:05", 42, "3_2"],
            [3, "session_end", "session_end", "2022-01-02 00:05:05", 44, "3_2"],
            [3, "path_end", "path_end", "2022-01-02 00:05:05", 45, "3_2"],
            [4, "path_start", "path_start", "2022-01-02 00:06:10", 46, "4_1"],
            [4, "session_start", "session_start", "2022-01-02 00:06:10", 47, "4_1"],
            [4, "event1", "raw", "2022-01-02 00:06:10", 48, "4_1"],
            [4, "event1", "raw", "2022-01-02 00:07:05", 50, "4_1"],
            [4, "event1", "raw", "2022-01-02 00:08:05", 52, "4_1"],
            [4, "event2", "raw", "2022-01-02 00:09:05", 54, "4_1"],
            [4, "session_end", "session_end", "2022-01-02 00:09:05", 56, "4_1"],
            [4, "path_end", "path_end", "2022-01-02 00:09:05", 57, "4_1"],
        ],
        columns=correct_result_columns,
    )
    correct_result["timestamp"] = pd.to_datetime(correct_result["timestamp"])
    return correct_result


@pytest.fixture
def positive_target_delete_users_corr():
    correct_result_columns = ["user_id", "event", "event_type", "timestamp", "event_index"]

    correct_result = pd.DataFrame(
        [
            [1, "event1", "raw", "2022-01-01 00:01:00", 0],
            [1, "event2", "raw", "2022-01-01 00:01:02", 1],
            [1, "event1", "raw", "2022-01-01 00:02:00", 2],
            [1, "event1", "raw", "2022-01-01 00:03:00", 3],
            [1, "event1", "raw", "2022-01-01 00:03:01", 4],
            [1, "event3", "raw", "2022-01-01 00:03:30", 5],
            [1, "positive_target_event3", "positive_target", "2022-01-01 00:03:30", 6],
            [1, "event1", "raw", "2022-01-01 00:04:00", 7],
            [1, "event3", "raw", "2022-01-01 00:04:30", 8],
            [1, "event1", "raw", "2022-01-01 00:05:50", 9],
            [3, "event1", "raw", "2022-01-02 00:01:10", 16],
            [3, "event1", "raw", "2022-01-02 00:02:05", 17],
            [3, "event3", "raw", "2022-01-02 00:04:30", 18],
            [3, "positive_target_event3", "positive_target", "2022-01-02 00:04:30", 19],
            [3, "event4", "raw", "2022-01-02 00:05:05", 20],
            [4, "event1", "raw", "2022-01-02 00:06:10", 21],
            [4, "event1", "raw", "2022-01-02 00:07:05", 22],
            [4, "event1", "raw", "2022-01-02 00:08:05", 23],
            [4, "event2", "raw", "2022-01-02 00:09:05", 24],
        ],
        columns=correct_result_columns,
    )
    correct_result["timestamp"] = pd.to_datetime(correct_result["timestamp"])
    return correct_result


@pytest.fixture
def filter_events_negative_target_corr():
    correct_result_columns = ["user_id", "event", "event_type", "timestamp", "event_index"]
    correct_result = pd.DataFrame(
        [
            [1, "event1", "raw", "2022-01-01 00:01:00", 0],
            [1, "event2", "raw", "2022-01-01 00:01:02", 1],
            [1, "negative_target_event2", "negative_target", "2022-01-01 00:01:02", 2],
            [1, "event1", "raw", "2022-01-01 00:02:00", 3],
            [1, "event1", "raw", "2022-01-01 00:03:00", 4],
            [1, "event1", "raw", "2022-01-01 00:03:01", 5],
            [1, "event3", "raw", "2022-01-01 00:03:30", 6],
            [1, "event1", "raw", "2022-01-01 00:04:00", 7],
            [1, "event3", "raw", "2022-01-01 00:04:30", 8],
            [1, "event1", "raw", "2022-01-01 00:05:50", 9],
            [2, "event1", "raw", "2022-01-02 00:00:00", 10],
            [2, "event2", "raw", "2022-01-02 00:00:05", 11],
            [2, "negative_target_event2", "negative_target", "2022-01-02 00:00:05", 12],
            [2, "event2", "raw", "2022-01-02 00:01:09", 13],
        ],
        columns=correct_result_columns,
    )
    correct_result["timestamp"] = pd.to_datetime(correct_result["timestamp"])
    return correct_result


@pytest.fixture
def group_events_delete_users_corr():
    correct_result_columns = ["user_id", "event", "event_type", "timestamp", "event_index", "user_type_col"]

    correct_result = pd.DataFrame(
        [
            [1, "event1", "raw", "2022-01-01 00:01:00", 0, "non_conv"],
            [1, "event2", "raw", "2022-01-01 00:01:02", 1, "non_conv"],
            [1, "event1", "raw", "2022-01-01 00:02:00", 2, "non_conv"],
            [1, "event1", "raw", "2022-01-01 00:03:00", 3, "non_conv"],
            [1, "event1", "raw", "2022-01-01 00:03:01", 4, "non_conv"],
            [1, "last_event", "group_alias", "2022-01-01 00:03:30", 5, "non_conv"],
            [1, "event1", "raw", "2022-01-01 00:04:00", 7, "non_conv"],
            [1, "last_event", "group_alias", "2022-01-01 00:04:30", 8, "non_conv"],
            [1, "event1", "raw", "2022-01-01 00:05:50", 10, "non_conv"],
            [3, "event1", "raw", "2022-01-02 00:01:10", 17, "conv"],
            [3, "event1", "raw", "2022-01-02 00:02:05", 18, "conv"],
            [3, "last_event", "group_alias", "2022-01-02 00:04:30", 19, "conv"],
            [3, "last_event", "group_alias", "2022-01-02 00:05:05", 21, "conv"],
        ],
        columns=correct_result_columns,
    )
    correct_result["timestamp"] = pd.to_datetime(correct_result["timestamp"])
    return correct_result
