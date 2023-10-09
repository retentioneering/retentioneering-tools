from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def new_lost_corr():
    correct_result_columns = ["user_id", "event", "event_type", "timestamp", "event_index"]

    correct_result = pd.DataFrame(
        [
            [1, "new_user", "new_user", "2022-01-01 00:01:00", 0],
            [1, "event1", "raw", "2022-01-01 00:01:00", 0],
            [1, "event2", "raw", "2022-01-01 00:01:02", 1],
            [1, "event1", "raw", "2022-01-01 00:02:00", 2],
            [1, "event1", "raw", "2022-01-01 00:03:00", 3],
            [1, "event1", "raw", "2022-01-01 00:03:01", 4],
            [1, "event3", "raw", "2022-01-01 00:03:30", 5],
            [1, "event1", "raw", "2022-01-01 00:04:00", 6],
            [1, "event3", "raw", "2022-01-01 00:04:30", 7],
            [1, "lost_user", "lost_user", "2022-01-01 00:04:30", 7],
            [2, "existing_user", "existing_user", "2022-01-02 00:00:00", 9],
            [2, "event1", "raw", "2022-01-02 00:00:00", 9],
            [2, "event2", "raw", "2022-01-02 00:00:05", 10],
            [2, "event2", "raw", "2022-01-02 00:01:09", 11],
            [2, "lost_user", "lost_user", "2022-01-02 00:01:09", 11],
            [3, "existing_user", "existing_user", "2022-01-02 00:01:10", 12],
            [3, "event1", "raw", "2022-01-02 00:01:10", 12],
            [3, "event1", "raw", "2022-01-02 00:02:05", 13],
            [3, "event3", "raw", "2022-01-02 00:04:30", 14],
            [3, "absent_user", "absent_user", "2022-01-02 00:04:30", 14],
            [4, "existing_user", "existing_user", "2022-01-02 00:06:10", 16],
            [4, "event1", "raw", "2022-01-02 00:06:10", 16],
            [4, "event1", "raw", "2022-01-02 00:07:05", 17],
            [4, "event1", "raw", "2022-01-02 00:08:05", 18],
            [4, "event2", "raw", "2022-01-02 00:09:05", 19],
            [4, "absent_user", "absent_user", "2022-01-02 00:09:05", 19],
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
            [1, "session_start", "session_start", "2022-01-01 00:01:00", 0, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:01:00", 0, "1_1"],
            [1, "event2", "raw", "2022-01-01 00:01:02", 1, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:02:00", 2, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:03:00", 3, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:03:01", 4, "1_1"],
            [1, "event3", "raw", "2022-01-01 00:03:30", 5, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:04:00", 6, "1_1"],
            [1, "event3", "raw", "2022-01-01 00:04:30", 7, "1_1"],
            [1, "event1", "raw", "2022-01-01 00:05:50", 8, "1_1"],
            [1, "session_end", "session_end", "2022-01-01 00:05:50", 8, "1_1"],
            [1, "path_end", "path_end", "2022-01-01 00:05:50", 8, "1_1"],
            [2, "path_start", "path_start", "2022-01-02 00:00:00", 9, "2_1"],
            [2, "session_start", "session_start", "2022-01-02 00:00:00", 9, "2_1"],
            [2, "event1", "raw", "2022-01-02 00:00:00", 9, "2_1"],
            [2, "event2", "raw", "2022-01-02 00:00:05", 10, "2_1"],
            [2, "event2", "raw", "2022-01-02 00:01:09", 11, "2_1"],
            [2, "session_end", "session_end", "2022-01-02 00:01:09", 11, "2_1"],
            [2, "path_end", "path_end", "2022-01-02 00:01:09", 11, "2_1"],
            [3, "path_start", "path_start", "2022-01-02 00:01:10", 12, "3_1"],
            [3, "session_start", "session_start", "2022-01-02 00:01:10", 12, "3_1"],
            [3, "event1", "raw", "2022-01-02 00:01:10", 12, "3_1"],
            [3, "event1", "raw", "2022-01-02 00:02:05", 13, "3_1"],
            [3, "session_end", "session_end", "2022-01-02 00:02:05", 13, "3_1"],
            [3, "session_start", "session_start", "2022-01-02 00:04:30", 14, "3_2"],
            [3, "event3", "raw", "2022-01-02 00:04:30", 14, "3_2"],
            [3, "event4", "raw", "2022-01-02 00:05:05", 15, "3_2"],
            [3, "session_end", "session_end", "2022-01-02 00:05:05", 15, "3_2"],
            [3, "path_end", "path_end", "2022-01-02 00:05:05", 15, "3_2"],
            [4, "path_start", "path_start", "2022-01-02 00:06:10", 16, "4_1"],
            [4, "session_start", "session_start", "2022-01-02 00:06:10", 16, "4_1"],
            [4, "event1", "raw", "2022-01-02 00:06:10", 16, "4_1"],
            [4, "event1", "raw", "2022-01-02 00:07:05", 17, "4_1"],
            [4, "event1", "raw", "2022-01-02 00:08:05", 18, "4_1"],
            [4, "event2", "raw", "2022-01-02 00:09:05", 19, "4_1"],
            [4, "session_end", "session_end", "2022-01-02 00:09:05", 19, "4_1"],
            [4, "path_end", "path_end", "2022-01-02 00:09:05", 19, "4_1"],
        ],
        columns=correct_result_columns,
    )
    correct_result["timestamp"] = pd.to_datetime(correct_result["timestamp"])
    return correct_result


@pytest.fixture
def add_positive_events_drop_paths_corr():
    correct_result_columns = ["user_id", "event", "event_type", "timestamp", "event_index"]

    correct_result = pd.DataFrame(
        [
            [1, "event1", "raw", "2022-01-01 00:01:00", 0],
            [1, "event2", "raw", "2022-01-01 00:01:02", 1],
            [1, "event1", "raw", "2022-01-01 00:02:00", 2],
            [1, "event1", "raw", "2022-01-01 00:03:00", 3],
            [1, "event1", "raw", "2022-01-01 00:03:01", 4],
            [1, "event3", "raw", "2022-01-01 00:03:30", 5],
            [1, "positive_target_event3", "positive_target", "2022-01-01 00:03:30", 5],
            [1, "event1", "raw", "2022-01-01 00:04:00", 6],
            [1, "event3", "raw", "2022-01-01 00:04:30", 7],
            [1, "event1", "raw", "2022-01-01 00:05:50", 8],
            [3, "event1", "raw", "2022-01-02 00:01:10", 12],
            [3, "event1", "raw", "2022-01-02 00:02:05", 13],
            [3, "event3", "raw", "2022-01-02 00:04:30", 14],
            [3, "positive_target_event3", "positive_target", "2022-01-02 00:04:30", 14],
            [3, "event4", "raw", "2022-01-02 00:05:05", 15],
            [4, "event1", "raw", "2022-01-02 00:06:10", 16],
            [4, "event1", "raw", "2022-01-02 00:07:05", 17],
            [4, "event1", "raw", "2022-01-02 00:08:05", 18],
            [4, "event2", "raw", "2022-01-02 00:09:05", 19],
        ],
        columns=correct_result_columns,
    )
    correct_result["timestamp"] = pd.to_datetime(correct_result["timestamp"])
    return correct_result


@pytest.fixture
def filter_events_add_negative_events_corr():
    correct_result_columns = ["user_id", "event", "event_type", "timestamp", "event_index"]
    correct_result = pd.DataFrame(
        [
            [1, "event1", "raw", "2022-01-01 00:01:00", 0],
            [1, "event2", "raw", "2022-01-01 00:01:02", 1],
            [1, "negative_target_event2", "negative_target", "2022-01-01 00:01:02", 1],
            [1, "event1", "raw", "2022-01-01 00:02:00", 2],
            [1, "event1", "raw", "2022-01-01 00:03:00", 3],
            [1, "event1", "raw", "2022-01-01 00:03:01", 4],
            [1, "event3", "raw", "2022-01-01 00:03:30", 5],
            [1, "event1", "raw", "2022-01-01 00:04:00", 6],
            [1, "event3", "raw", "2022-01-01 00:04:30", 7],
            [1, "event1", "raw", "2022-01-01 00:05:50", 8],
            [2, "event1", "raw", "2022-01-02 00:00:00", 9],
            [2, "event2", "raw", "2022-01-02 00:00:05", 10],
            [2, "negative_target_event2", "negative_target", "2022-01-02 00:00:05", 10],
            [2, "event2", "raw", "2022-01-02 00:01:09", 11],
        ],
        columns=correct_result_columns,
    )
    correct_result["timestamp"] = pd.to_datetime(correct_result["timestamp"])
    return correct_result


@pytest.fixture
def group_events_drop_paths_corr():
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
