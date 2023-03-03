import os

import numpy as np
import pandas as pd
import pytest


def read_test_data(filename: str) -> pd.Series:
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/tooling/event_timestamp_hist")
    filepath = os.path.join(test_data_dir, filename)
    source_df = pd.read_csv(filepath, header=None)
    return source_df[0].sort_values()


@pytest.fixture
def correct_basic() -> np.array:
    list_values = read_test_data("basic_values.csv")
    list_values = np.array(pd.to_datetime(list_values))
    return list_values


@pytest.fixture
def correct_basic_bins() -> np.array:
    correct_bins = np.array(
        ["2023-01-01 00:00:00", "2023-01-01 16:00:01", "2023-01-02 08:00:01", "2023-01-03 00:00:02"],
        dtype="datetime64[ns]",
    )
    return correct_bins


@pytest.fixture
def correct_lower_quantile() -> np.array:
    list_values = read_test_data("lower_quantile.csv")
    list_values = np.array(pd.to_datetime(list_values))
    return list_values


@pytest.fixture
def correct_lower_quantile_bins() -> np.array:
    correct_bins = np.array(
        ["2023-01-03 00:00:00", "2023-01-03 00:00:01", "2023-01-03 00:00:02"], dtype="datetime64[ns]"
    )
    return correct_bins


@pytest.fixture
def correct_upper_quantile() -> np.array:
    list_values = read_test_data("upper_quantile.csv")
    list_values = np.array(pd.to_datetime(list_values))
    return list_values


@pytest.fixture
def correct_upper_quantile_bins() -> np.array:
    correct_bins = np.array(
        ["2023-01-01 00:00:00", "2023-01-01 12:00:00", "2023-01-02 00:00:01"], dtype="datetime64[ns]"
    )
    return correct_bins


@pytest.fixture
def correct_upper_lower_quantile() -> np.array:
    list_values = read_test_data("lower_upper_quantile.csv")
    list_values = np.array(pd.to_datetime(list_values))
    return list_values


@pytest.fixture
def correct_upper_lower_quantile_bins() -> np.array:
    correct_bins = np.array(
        ["2023-01-01 00:00:02", "2023-01-02 00:00:01", "2023-01-03 00:00:00"], dtype="datetime64[ns]"
    )
    return correct_bins


@pytest.fixture
def correct_raw_events_only() -> np.array:
    list_values = read_test_data("raw_events_only.csv")
    list_values = np.array(pd.to_datetime(list_values))
    return list_values


@pytest.fixture
def correct_raw_events_only_bins() -> np.array:
    correct_bins = np.array(
        ["2023-01-01 00:00:00", "2023-01-01 16:00:01", "2023-01-02 08:00:01", "2023-01-03 00:00:02"],
        dtype="datetime64[ns]",
    )
    return correct_bins
