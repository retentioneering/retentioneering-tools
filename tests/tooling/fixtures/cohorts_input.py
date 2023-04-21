import os

import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from retentioneering.eventstream.types import EventstreamType


@pytest.fixture
def test_stream() -> EventstreamType:
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/tooling/cohorts")
    filepath = os.path.join(test_data_dir, "input_1.csv")

    stream = Eventstream(
        raw_data=pd.read_csv(filepath),
        raw_data_schema=RawDataSchema(
            event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
        ),
        schema=EventstreamSchema(),
    )

    return stream
