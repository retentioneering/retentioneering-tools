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
def correct_lower_quantile() -> np.array:
    list_values = read_test_data("lower_quantile.csv")
    list_values = np.array(pd.to_datetime(list_values))
    return list_values


@pytest.fixture
def correct_upper_quantile() -> np.array:
    list_values = read_test_data("upper_quantile.csv")
    list_values = np.array(pd.to_datetime(list_values))
    return list_values


@pytest.fixture
def correct_upper_lower_quantile() -> np.array:
    list_values = read_test_data("lower_upper_quantile.csv")
    list_values = np.array(pd.to_datetime(list_values))
    return list_values


@pytest.fixture
def correct_raw_events_only() -> np.array:
    list_values = read_test_data("raw_events_only.csv")
    list_values = np.array(pd.to_datetime(list_values))
    return list_values
