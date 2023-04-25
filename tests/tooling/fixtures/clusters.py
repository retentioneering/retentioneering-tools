import pandas as pd
import pytest

from retentioneering import datasets
from retentioneering.eventstream import Eventstream
from retentioneering.eventstream.types import EventstreamType


@pytest.fixture
def stream_simple_shop() -> EventstreamType:
    test_stream = datasets.load_simple_shop()
    return test_stream


@pytest.fixture
def test_stream() -> EventstreamType:
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


@pytest.fixture
def custom_X() -> pd.DataFrame:
    columns = ["event1", "event2", "event3", "event4"]
    index = pd.Index([1, 2, 3, 4], name="user_id")
    vector = pd.DataFrame(
        [
            [0.0, 1.0, 2.0, 7.0],
            [1.0, 4.0, 4.0, 0.0],
            [2.0, 1.0, 6.0, 1.0],
            [3.0, 2.0, 9.0, 0.0],
        ],
        columns=columns,
        index=index,
    )
    return vector


@pytest.fixture
def features_tfidf_input() -> pd.DataFrame:
    correct_columns = ["event1", "event2", "event3", "event4"]

    correct_columns = [c + "_tfidf" for c in correct_columns]
    index = pd.Index([1, 2, 3, 4], name="user_id")
    features = pd.DataFrame(
        [
            [0.824387, 0.207584, 0.526588, 0.000000],
            [0.314186, 0.949361, 0.000000, 0.000000],
            [0.722056, 0.000000, 0.000000, 0.691835],
            [1.000000, 0.000000, 0.000000, 0.000000],
        ],
        columns=correct_columns,
        index=index,
    )
    return features
