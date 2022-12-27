from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def simple_dataset_for_rename() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"event": "eventA", "timestamp": "2021-10-26 12:00:00", "user_id": "1"},
            {"event": "eventA", "timestamp": "2021-10-26 12:02:00", "user_id": "1"},
            {"event": "eventB", "timestamp": "2021-10-26 12:03:00", "user_id": "1"},
            {"event": "eventB", "timestamp": "2021-10-26 12:03:00", "user_id": "1"},
            {"event": "eventC", "timestamp": "2021-10-26 12:04:00", "user_id": "2"},
            {"event": "eventC", "timestamp": "2021-10-26 12:05:00", "user_id": "1"},
        ]
    )


@pytest.fixture
def simple_rules() -> list[dict[str, str]]:
    return [{"group_name": "some_group", "child_events": ["eventA", "eventB"]}]


@pytest.fixture
def simple_expected_results() -> pd.DataFrame:
    df = pd.DataFrame(
        [
            {"event": "some_group", "timestamp": "2021-10-26 12:00:00", "user_id": "1"},
            {"event": "some_group", "timestamp": "2021-10-26 12:02:00", "user_id": "1"},
            {"event": "some_group", "timestamp": "2021-10-26 12:03:00", "user_id": "1"},
            {"event": "some_group", "timestamp": "2021-10-26 12:03:00", "user_id": "1"},
            {"event": "eventC", "timestamp": "2021-10-26 12:04:00", "user_id": "2"},
            {"event": "eventC", "timestamp": "2021-10-26 12:05:00", "user_id": "1"},
        ]
    )
    df["timestamp"] = df["timestamp"].astype("datetime64[ns]")
    return df


@pytest.fixture
def complex_dataset_for_rename() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"event": "eventA", "timestamp": "2021-10-26 12:00:00", "user_id": "1"},
            {"event": "eventA", "timestamp": "2021-10-26 12:02:00", "user_id": "1"},
            {"event": "eventB", "timestamp": "2021-10-26 12:03:00", "user_id": "1"},
            {"event": "eventB", "timestamp": "2021-10-26 12:03:00", "user_id": "1"},
            {"event": "eventB", "timestamp": "2021-10-26 12:04:00", "user_id": "1"},
            {"event": "eventC", "timestamp": "2021-10-26 12:04:00", "user_id": "2"},
            {"event": "eventC", "timestamp": "2021-10-26 12:05:00", "user_id": "1"},
            {"event": "eventC", "timestamp": "2021-10-26 12:04:00", "user_id": "2"},
            {"event": "eventD", "timestamp": "2021-10-26 12:05:00", "user_id": "4"},
            {"event": "eventD", "timestamp": "2021-10-26 12:04:00", "user_id": "4"},
            {"event": "eventD", "timestamp": "2021-10-26 12:05:00", "user_id": "4"},
            {"event": "eventE", "timestamp": "2021-10-26 12:05:00", "user_id": "5"},
            {"event": "eventE", "timestamp": "2021-10-26 12:05:00", "user_id": "6"},
            {"event": "eventE", "timestamp": "2021-10-26 12:05:00", "user_id": "7"},
        ]
    )


@pytest.fixture
def complex_rules() -> list[dict[str, str]]:
    return [
        {
            "group_name": "group1",
            "child_events": ["eventA", "eventC"],
        },
        {
            "group_name": "group2",
            "child_events": ["eventB", "eventD"],
        },
    ]


@pytest.fixture
def complex_expected_results() -> pd.DataFrame:
    df = pd.DataFrame(
        [
            {"event": "group1", "timestamp": "2021-10-26 12:00:00", "user_id": "1"},
            {"event": "group1", "timestamp": "2021-10-26 12:02:00", "user_id": "1"},
            {"event": "group2", "timestamp": "2021-10-26 12:03:00", "user_id": "1"},
            {"event": "group2", "timestamp": "2021-10-26 12:03:00", "user_id": "1"},
            {"event": "group2", "timestamp": "2021-10-26 12:04:00", "user_id": "1"},
            {"event": "group1", "timestamp": "2021-10-26 12:04:00", "user_id": "2"},
            {"event": "group1", "timestamp": "2021-10-26 12:04:00", "user_id": "2"},
            {"event": "group2", "timestamp": "2021-10-26 12:04:00", "user_id": "4"},
            {"event": "group1", "timestamp": "2021-10-26 12:05:00", "user_id": "1"},
            {"event": "group2", "timestamp": "2021-10-26 12:05:00", "user_id": "4"},
            {"event": "group2", "timestamp": "2021-10-26 12:05:00", "user_id": "4"},
            {"event": "eventE", "timestamp": "2021-10-26 12:05:00", "user_id": "5"},
            {"event": "eventE", "timestamp": "2021-10-26 12:05:00", "user_id": "6"},
            {"event": "eventE", "timestamp": "2021-10-26 12:05:00", "user_id": "7"},
        ]
    )
    df["timestamp"] = df["timestamp"].astype("datetime64[ns]")
    return df
