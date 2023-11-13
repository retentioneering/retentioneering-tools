import os

import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream
from retentioneering.eventstream.types import EventstreamType


def read_test_data(filename: str) -> pd.DataFrame:
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/tooling/timedelta_hist")
    filepath = os.path.join(test_data_dir, filename)
    source_df = pd.read_csv(filepath)
    return source_df


@pytest.fixture
def test_stream() -> EventstreamType:
    source_df = read_test_data("input.csv")
    source_stream = Eventstream(source_df, add_start_end_events=False)
    return source_stream


@pytest.fixture
def source_stream_for_log_scale() -> EventstreamType:
    source_df = read_test_data("input_for_log_scale.csv")
    source_stream = Eventstream(source_df, add_start_end_events=False)
    return source_stream


@pytest.fixture
def source_stream_add_start_end_events() -> EventstreamType:
    source_df = read_test_data("input.csv")
    source_stream = Eventstream(source_df)
    return source_stream


@pytest.fixture
def source_stream_sessions() -> EventstreamType:
    source_df = read_test_data("input.csv")
    source_stream = Eventstream(source_df).split_sessions(timeout=(1, "s"))
    return source_stream
