from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream, RawDataSchema
from retentioneering.eventstream.types import RawDataCustomColSchema


@pytest.fixture
def test_stream() -> Eventstream:
    import retentioneering.datasets as datasets

    rng = np.random.default_rng(seed=42)
    simple_shop: pd.DataFrame = datasets.load_simple_shop(as_dataframe=True, add_start_end_events=False)  # type: ignore

    simple_shop["session_id"] = rng.binomial(1, 1 / 20, size=simple_shop.shape[0])
    simple_shop["session_id"] = simple_shop.groupby("user_id")["session_id"].cumsum()
    simple_shop["session_id"] = simple_shop["user_id"].apply(str) + "_" + simple_shop["session_id"].apply(str)

    raw_data_schema: dict[str, str | list[RawDataCustomColSchema]] = {
        "custom_cols": [{"raw_data_col": "session_id", "custom_col": "session_id"}]
    }
    stream = Eventstream(simple_shop, raw_data_schema=raw_data_schema, add_start_end_events=False)

    return stream


@pytest.fixture
def groups(test_stream: Eventstream) -> tuple[list, list]:
    users = test_stream.to_dataframe()["user_id"].unique()
    index_separator = int(users.shape[0] / 2)
    return list(users[:index_separator]), list(users[index_separator:])


@pytest.fixture
def test_stream_small() -> Eventstream:
    source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00", "1_1"],
            [1, "event2", "2022-01-01 00:01:02", "1_1"],
            [1, "event1", "2022-01-01 00:02:00", "1_1"],
            [1, "event1", "2022-01-01 00:03:00", "1_1"],
            [1, "event1", "2022-01-01 00:03:00", "1_2"],
            [1, "event3", "2022-01-01 00:03:30", "1_2"],
            [1, "event1", "2022-01-01 00:04:00", "1_2"],
            [1, "event3", "2022-01-01 00:04:30", "1_2"],
            [1, "event1", "2022-01-01 00:05:00", "1_2"],
            [2, "event1", "2022-01-02 00:00:00", "2_1"],
            [2, "event2", "2022-01-02 00:00:05", "2_1"],
            [2, "event2", "2022-01-02 00:01:05", "2_2"],
            [3, "event1", "2022-01-02 00:01:10", "3_1"],
            [3, "event1", "2022-01-02 00:02:05", "3_1"],
            [3, "event4", "2022-01-02 00:03:05", "3_1"],
            [4, "event1", "2022-01-02 00:01:10", "4_1"],
            [4, "event1", "2022-01-02 00:02:05", "4_1"],
            [4, "event1", "2022-01-02 00:03:05", "4_1"],
        ],
        columns=["user_id", "event", "timestamp", "session_id"],
    )
    raw_data_schema = RawDataSchema(
        event_name="event",
        event_timestamp="timestamp",
        user_id="user_id",
        custom_cols=[{"custom_col": "session_id", "raw_data_col": "session_id"}],
    )

    stream = Eventstream(source_df, raw_data_schema=raw_data_schema, add_start_end_events=False)
    return stream


@pytest.fixture
def groups_small_session_id(test_stream_small: Eventstream) -> tuple[list, list]:
    sessions = test_stream_small.to_dataframe()["session_id"].unique()
    index_separator = int(sessions.shape[0] / 2)
    return list(sessions[:index_separator]), list(sessions[index_separator:])


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
