import pandas as pd
import pytest

from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema


@pytest.fixture
def test_stream():
    source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00"],
            [1, "event2", "2022-01-01 00:01:02"],
            [1, "event1", "2022-01-01 00:02:00"],
            [1, "event1", "2022-01-01 00:03:00"],
            [1, "event1", "2022-01-01 00:03:00"],
            [1, "event3", "2022-01-01 00:03:30"],
            [1, "event1", "2022-01-01 00:04:00"],
            [1, "event3", "2022-01-01 00:04:30"],
            [1, "event1", "2022-01-01 00:05:00"],
            [2, "event1", "2022-01-02 00:00:00"],
            [2, "event2", "2022-01-02 00:00:05"],
            [2, "event2", "2022-01-02 00:01:05"],
            [3, "event1", "2022-01-02 00:01:10"],
            [3, "event1", "2022-01-02 00:02:05"],
            [3, "event4", "2022-01-02 00:03:05"],
            [4, "event1", "2022-01-02 00:01:10"],
            [4, "event1", "2022-01-02 00:02:05"],
            [4, "event1", "2022-01-02 00:03:05"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    stream = Eventstream(source_df)
    return stream
