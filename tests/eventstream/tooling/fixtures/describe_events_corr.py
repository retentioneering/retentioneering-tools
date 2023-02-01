import os

import pandas as pd
import pytest


def read_corr_data(filename: str) -> pd.DataFrame:
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../../datasets/eventstream/tooling/describe_events")
    filepath = os.path.join(test_data_dir, filename)
    source_df = pd.read_csv(filepath, index_col=0, header=[0, 1])
    for i in range(len(source_df.columns)):
        col = source_df.columns[i]
        if "time" in source_df.columns[i][0]:
            source_df[col] = pd.to_timedelta(source_df[col])
    source_df.index.names = ["event"]

    return source_df


@pytest.fixture
def basic_corr() -> pd.DataFrame:
    correct_result = read_corr_data("describe_events_basic_corr.csv")

    return correct_result


@pytest.fixture
def session_corr() -> pd.DataFrame:
    correct_result = read_corr_data("describe_events_session_corr.csv")

    return correct_result
