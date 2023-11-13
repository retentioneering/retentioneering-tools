import os

import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream
from retentioneering.eventstream.types import EventstreamType


def read_test_data(filename: str) -> pd.DataFrame:
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../../datasets/tooling/describe_events")
    filepath = os.path.join(test_data_dir, filename)
    source_df = pd.read_csv(filepath)
    return source_df


@pytest.fixture
def test_stream() -> EventstreamType:
    source_df = read_test_data("input1.csv")
    source_stream = Eventstream(source_df, add_start_end_events=False)
    return source_stream
