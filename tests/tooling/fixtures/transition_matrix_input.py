import os

import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream, RawDataSchema


@pytest.fixture
def test_stream():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/transition_graph")
    filepath = os.path.join(test_data_dir, "input.csv")

    raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
        custom_cols=[{"custom_col": "session_id", "raw_data_col": "session_id"}],
    )
    stream = Eventstream(raw_data=pd.read_csv(filepath), raw_data_schema=raw_data_schema)

    return stream
