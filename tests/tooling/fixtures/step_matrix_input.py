import os

import pandas as pd
import pytest

from retentioneering import datasets
from retentioneering.eventstream import Eventstream, RawDataSchema
from retentioneering.eventstream.types import EventstreamType

FLOAT_PRECISION = 3


def read_test_data(filename) -> pd.DataFrame:
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/tooling/step_matrix")
    filepath = os.path.join(test_data_dir, filename)
    df = pd.read_csv(filepath, index_col=0).round(FLOAT_PRECISION)
    return df


@pytest.fixture
def stream_simple_shop() -> EventstreamType:
    stream = datasets.load_simple_shop(add_start_end_events=True)
    return stream


@pytest.fixture
def test_stream_end_path() -> EventstreamType:
    df = read_test_data("test_stream_end_path.csv")
    source_stream = Eventstream(df, add_start_end_events=False)
    return source_stream


@pytest.fixture
def test_stream() -> EventstreamType:
    df = read_test_data("test_stream.csv")
    source_stream = Eventstream(df, add_start_end_events=False)
    return source_stream


@pytest.fixture
def test_weight_col() -> EventstreamType:
    df = read_test_data("test_weight_col.csv")

    raw_data_schema = RawDataSchema(
        event_name="event",
        event_timestamp="timestamp",
        user_id="user_id",
        custom_cols=[{"custom_col": "session_id", "raw_data_col": "session_id"}],
    )
    source_stream = Eventstream(df, raw_data_schema, add_start_end_events=False)
    return source_stream
