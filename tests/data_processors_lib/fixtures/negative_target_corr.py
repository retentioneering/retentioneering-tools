from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def apply__1_event_corr():
    corr = pd.DataFrame(
        [
            [1, "negative_target_event3", "negative_target", "2022-01-01 00:03:30"],
            [2, "negative_target_event3", "negative_target", "2022-01-02 00:00:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    corr["timestamp"] = pd.to_datetime(corr["timestamp"])

    return corr


@pytest.fixture
def apply__2_events_corr():
    corr = pd.DataFrame(
        [
            [1, "negative_target_event2", "negative_target", "2022-01-01 00:01:02"],
            [2, "negative_target_event3", "negative_target", "2022-01-02 00:00:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    corr["timestamp"] = pd.to_datetime(corr["timestamp"])

    return corr


@pytest.fixture
def apply__custom_func_corr():
    corr = pd.DataFrame(
        [
            [1, "negative_target_event1", "negative_target", "2022-01-01 00:01:00"],
            [1, "negative_target_event1", "negative_target", "2022-01-01 00:05:00"],
            [2, "negative_target_event1", "negative_target", "2022-01-02 00:00:00"],
            [2, "negative_target_event1", "negative_target", "2022-01-02 00:00:00"],
            [3, "negative_target_event1", "negative_target", "2022-01-02 00:01:10"],
            [3, "negative_target_event1", "negative_target", "2022-01-02 00:02:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    corr["timestamp"] = pd.to_datetime(corr["timestamp"])

    return corr


@pytest.fixture
def graph__1_event_corr():
    corr = pd.DataFrame(
        [
            [1, "path_start", "path_start", "2022-01-01 00:01:00"],
            [1, "event1", "raw", "2022-01-01 00:01:00"],
            [1, "event2", "raw", "2022-01-01 00:01:02"],
            [1, "event1", "raw", "2022-01-01 00:02:00"],
            [1, "event1", "raw", "2022-01-01 00:03:00"],
            [1, "event1", "synthetic", "2022-01-01 00:03:00"],
            [1, "session_start", "session_start", "2022-01-01 00:03:30"],
            [1, "event3", "raw", "2022-01-01 00:03:30"],
            [1, "negative_target_event3", "negative_target", "2022-01-01 00:03:30"],
            [1, "event1", "raw", "2022-01-01 00:04:00"],
            [1, "event3", "raw", "2022-01-01 00:04:30"],
            [1, "event1", "raw", "2022-01-01 00:05:00"],
            [2, "event1", "raw", "2022-01-02 00:00:00"],
            [2, "event3", "raw", "2022-01-02 00:00:05"],
            [2, "negative_target_event3", "negative_target", "2022-01-02 00:00:05"],
            [2, "event2", "raw", "2022-01-02 00:01:05"],
            [2, "path_end", "path_end", "2022-01-02 00:01:05"],
            [3, "event1", "raw", "2022-01-02 00:01:10"],
            [3, "event1", "raw", "2022-01-02 00:02:05"],
            [3, "event4", "raw", "2022-01-02 00:03:05"],
            [3, "path_end", "path_end", "2022-01-02 00:03:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    corr["timestamp"] = pd.to_datetime(corr["timestamp"])
    return corr


@pytest.fixture
def graph__2_events_corr():
    corr = pd.DataFrame(
        [
            [1, "path_start", "path_start", "2022-01-01 00:01:00"],
            [1, "event1", "raw", "2022-01-01 00:01:00"],
            [1, "event2", "raw", "2022-01-01 00:01:02"],
            [1, "negative_target_event2", "negative_target", "2022-01-01 00:01:02"],
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
            [2, "negative_target_event3", "negative_target", "2022-01-02 00:00:05"],
            [2, "event2", "raw", "2022-01-02 00:01:05"],
            [2, "path_end", "path_end", "2022-01-02 00:01:05"],
            [3, "event1", "raw", "2022-01-02 00:01:10"],
            [3, "event1", "raw", "2022-01-02 00:02:05"],
            [3, "event4", "raw", "2022-01-02 00:03:05"],
            [3, "path_end", "path_end", "2022-01-02 00:03:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )
    corr["timestamp"] = pd.to_datetime(corr["timestamp"])

    return corr


@pytest.fixture
def graph__custom_func_corr():
    corr = pd.DataFrame(
        [
            [1, "path_start", "path_start", "2022-01-01 00:01:00"],
            [1, "event1", "raw", "2022-01-01 00:01:00"],
            [1, "negative_target_event1", "negative_target", "2022-01-01 00:01:00"],
            [1, "event2", "raw", "2022-01-01 00:01:02"],
            [1, "event1", "raw", "2022-01-01 00:02:00"],
            [1, "event1", "raw", "2022-01-01 00:03:00"],
            [1, "event1", "synthetic", "2022-01-01 00:03:00"],
            [1, "session_start", "session_start", "2022-01-01 00:03:30"],
            [1, "event3", "raw", "2022-01-01 00:03:30"],
            [1, "event1", "raw", "2022-01-01 00:04:00"],
            [1, "event3", "raw", "2022-01-01 00:04:30"],
            [1, "event1", "raw", "2022-01-01 00:05:00"],
            [1, "negative_target_event1", "negative_target", "2022-01-01 00:05:00"],
            [2, "event1", "raw", "2022-01-02 00:00:00"],
            [2, "negative_target_event1", "negative_target", "2022-01-02 00:00:00"],
            [2, "negative_target_event1", "negative_target", "2022-01-02 00:00:00"],
            [2, "event3", "raw", "2022-01-02 00:00:05"],
            [2, "event2", "raw", "2022-01-02 00:01:05"],
            [2, "path_end", "path_end", "2022-01-02 00:01:05"],
            [3, "event1", "raw", "2022-01-02 00:01:10"],
            [3, "negative_target_event1", "negative_target", "2022-01-02 00:01:10"],
            [3, "event1", "raw", "2022-01-02 00:02:05"],
            [3, "negative_target_event1", "negative_target", "2022-01-02 00:02:05"],
            [3, "event4", "raw", "2022-01-02 00:03:05"],
            [3, "path_end", "path_end", "2022-01-02 00:03:05"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )

    corr["timestamp"] = pd.to_datetime(corr["timestamp"])

    return corr
