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
            [1, "lost_user", "lost_user", "2022-01-01 00:04:30", 9],
            [2, "existing_user", "existing_user", "2022-01-02 00:00:00", 11],
            [2, "event1", "raw", "2022-01-02 00:00:00", 12],
            [2, "event2", "raw", "2022-01-02 00:00:05", 13],
            [2, "event2", "raw", "2022-01-02 00:01:09", 14],
            [2, "lost_user", "lost_user", "2022-01-02 00:01:09", 15],
            [3, "existing_user", "existing_user", "2022-01-02 00:01:10", 16],
            [3, "event1", "raw", "2022-01-02 00:01:10", 17],
            [3, "event1", "raw", "2022-01-02 00:02:05", 18],
            [3, "event3", "raw", "2022-01-02 00:04:30", 19],
            [3, "absent_user", "absent_user", "2022-01-02 00:04:30", 20],
            [4, "existing_user", "existing_user", "2022-01-02 00:06:10", 22],
            [4, "event1", "raw", "2022-01-02 00:06:10", 23],
            [4, "event1", "raw", "2022-01-02 00:07:05", 24],
            [4, "event1", "raw", "2022-01-02 00:08:05", 25],
            [4, "event2", "raw", "2022-01-02 00:09:05", 26],
            [4, "absent_user", "absent_user", "2022-01-02 00:09:05", 27],
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
            [1, "event2", "raw", "2022-01-01 00:01:02", 3, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:02:00", 4, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:03:00", 5, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:03:01", 6, "1_1"],
            [1, "event3", "raw", "2022-01-01 00:03:30", 7, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:04:00", 8, "1_1"],
            [1, "event3", "raw", "2022-01-01 00:04:30", 9, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:05:50", 10, "1_1"],
            [1, "session_end", "session_end", "2022-01-01 00:05:50", 11, "1_1"],
            [1, "path_end", "path_end", "2022-01-01 00:05:50", 12, "1_1"],
            [2, "path_start", "path_start", "2022-01-02 00:00:00", 13, "2_1"],
            [2, "session_start", "session_start", "2022-01-02 00:00:00", 14, "2_1"],
            [2, "event1", "raw", "2022-01-02 00:00:00", 15, "2_1"],
            [2, "event2", "raw", "2022-01-02 00:00:05", 16, "2_1"],
            [2, "event2", "raw", "2022-01-02 00:01:09", 17, "2_1"],
            [2, "session_end", "session_end", "2022-01-02 00:01:09", 18, "2_1"],
            [2, "path_end", "path_end", "2022-01-02 00:01:09", 19, "2_1"],
            [3, "path_start", "path_start", "2022-01-02 00:01:10", 20, "3_1"],
            [3, "session_start", "session_start", "2022-01-02 00:01:10", 21, "3_1"],
            [3, "event1", "raw", "2022-01-02 00:01:10", 22, "3_1"],
            [3, "event1", "raw", "2022-01-02 00:02:05", 23, "3_1"],
            [3, "session_end", "session_end", "2022-01-02 00:02:05", 24, "3_1"],
            [3, "session_start", "session_start", "2022-01-02 00:04:30", 25, "3_2"],
            [3, "event3", "raw", "2022-01-02 00:04:30", 26, "3_2"],
            [3, "event4", "raw", "2022-01-02 00:05:05", 27, "3_2"],
            [3, "session_end", "session_end", "2022-01-02 00:05:05", 28, "3_2"],
            [3, "path_end", "path_end", "2022-01-02 00:05:05", 29, "3_2"],
            [4, "path_start", "path_start", "2022-01-02 00:06:10", 30, "4_1"],
            [4, "session_start", "session_start", "2022-01-02 00:06:10", 31, "4_1"],
            [4, "event1", "raw", "2022-01-02 00:06:10", 32, "4_1"],
            [4, "event1", "raw", "2022-01-02 00:07:05", 33, "4_1"],
            [4, "event1", "raw", "2022-01-02 00:08:05", 34, "4_1"],
            [4, "event2", "raw", "2022-01-02 00:09:05", 35, "4_1"],
            [4, "session_end", "session_end", "2022-01-02 00:09:05", 36, "4_1"],
            [4, "path_end", "path_end", "2022-01-02 00:09:05", 37, "4_1"],
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
            [3, "event1", "raw", "2022-01-02 00:01:10", 13],
            [3, "event1", "raw", "2022-01-02 00:02:05", 14],
            [3, "event3", "raw", "2022-01-02 00:04:30", 15],
            [3, "positive_target_event3", "positive_target", "2022-01-02 00:04:30", 16],
            [3, "event4", "raw", "2022-01-02 00:05:05", 17],
            [4, "event1", "raw", "2022-01-02 00:06:10", 18],
            [4, "event1", "raw", "2022-01-02 00:07:05", 19],
            [4, "event1", "raw", "2022-01-02 00:08:05", 20],
            [4, "event2", "raw", "2022-01-02 00:09:05", 21],
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
            [1, "event1", "raw", "2022-01-01 00:04:00", 6, "non_conv"],
            [1, "last_event", "group_alias", "2022-01-01 00:04:30", 7, "non_conv"],
            [1, "event1", "raw", "2022-01-01 00:05:50", 8, "non_conv"],
            [3, "event1", "raw", "2022-01-02 00:01:10", 12, "conv"],
            [3, "event1", "raw", "2022-01-02 00:02:05", 13, "conv"],
            [3, "last_event", "group_alias", "2022-01-02 00:04:30", 14, "conv"],
            [3, "last_event", "group_alias", "2022-01-02 00:05:05", 15, "conv"],
        ],
        columns=correct_result_columns,
    )
    correct_result["timestamp"] = pd.to_datetime(correct_result["timestamp"])
    return correct_result
