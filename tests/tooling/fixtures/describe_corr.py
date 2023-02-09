import os

import pandas as pd
import pytest


def read_corr_data(filename: str) -> pd.DataFrame:
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/tooling/describe")
    filepath = os.path.join(test_data_dir, filename)
    source_df = pd.read_csv(filepath, index_col=[0, 1], header=[0]).T

    for i in range(len(source_df.columns)):
        col = source_df.columns[i]
        if "time" in source_df.columns[i][0]:
            source_df[col] = pd.to_timedelta(source_df[col])
        elif "eventstream_length" in source_df.columns[i][0]:
            source_df[col] = pd.to_timedelta(source_df[col])
        elif "steps" in source_df.columns[i][0]:
            source_df[col] = pd.to_numeric(source_df[col])

    source_df[("overall", "eventstream_start")] = pd.to_datetime(source_df[("overall", "eventstream_start")])
    source_df[("overall", "eventstream_end")] = pd.to_datetime(source_df[("overall", "eventstream_end")])
    source_df[("overall", "eventstream_length")] = pd.to_timedelta(source_df[("overall", "eventstream_length")])
    source_df[("overall", "unique_users")] = pd.to_numeric(source_df[("overall", "unique_users")])
    source_df[("overall", "unique_events")] = pd.to_numeric(source_df[("overall", "unique_events")])

    return source_df


@pytest.fixture
def basic_corr() -> pd.DataFrame:
    correct_result = read_corr_data("describe_basic_corr.csv")

    return correct_result.T


@pytest.fixture
def session_corr() -> pd.DataFrame:
    correct_result = read_corr_data("describe_session_corr.csv")
    correct_result[("overall", "unique_sessions")] = pd.to_numeric(correct_result[("overall", "unique_sessions")])

    return correct_result.T
