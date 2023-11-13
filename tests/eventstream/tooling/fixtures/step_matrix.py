import os

import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream, EventstreamSchema, RawDataSchema


@pytest.fixture
def test_stream():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../../datasets/eventstream/tooling/step_matrix")
    filepath = os.path.join(test_data_dir, "input_1.csv")

    source_stream = Eventstream(
        raw_data=pd.read_csv(filepath),
        raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
        schema=EventstreamSchema(),
        add_start_end_events=False,
    )

    return source_stream
