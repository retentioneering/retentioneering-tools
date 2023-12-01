import os

import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream, RawDataSchema


@pytest.fixture
def test_stream():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../../datasets/eventstream/tooling/sequences")
    filepath = os.path.join(test_data_dir, "test_stream_input.csv")

    df = pd.read_csv(filepath)
    raw_data_schema = RawDataSchema(
        event_name="event",
        event_timestamp="timestamp",
        user_id="user_id",
        custom_cols=[{"custom_col": "session_id", "raw_data_col": "session_id"}],
    )
    source_stream = Eventstream(df, raw_data_schema, add_start_end_events=False)
    return source_stream
