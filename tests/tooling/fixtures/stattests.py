import os

import pandas as pd
import pytest

from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema


def read_test_data(filename):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/tooling/stattests")
    filepath = os.path.join(test_data_dir, filename)
    source_df = pd.read_csv(filepath, index_col=0)
    return source_df


@pytest.fixture
def simple_data():
    source_df = read_test_data("01_simple_data.csv")
    source_stream = Eventstream(source_df)
    return source_stream


@pytest.fixture
def continuous_data():
    source_df = read_test_data("02_continuous_data.csv")
    raw_data_schema = RawDataSchema(
        event_name="event",
        event_timestamp="timestamp",
        user_id="user_id",
        custom_cols=[{"custom_col": "seconds", "raw_data_col": "seconds"}],
    )
    source = Eventstream(
        schema=EventstreamSchema(
            custom_cols=["seconds"], event_name="event", event_timestamp="timestamp", user_id="user_id"
        ),
        raw_data_schema=raw_data_schema,
        raw_data=source_df,
    )
    return source


@pytest.fixture
def non_equal_target_data():
    source_df = read_test_data("03_non_equal_target_data.csv")
    source_stream = Eventstream(source_df)
    return source_stream
