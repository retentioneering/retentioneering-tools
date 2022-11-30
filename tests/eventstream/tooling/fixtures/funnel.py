import pandas as pd
import pytest

from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema


@pytest.fixture
def test_stream():
    source_df = pd.DataFrame(
        [
            # открытая, закрытая, закрытая+
            [1, "start", "start", "2022-01-01 00:01:00"],
            [1, "catalog", "raw", "2022-01-01 00:01:00"],
            [1, "product1", "raw", "2022-01-01 00:02:00"],
            [1, "product2", "raw", "2022-01-01 00:03:00"],
            [1, "cart", "raw", "2022-01-01 00:07:00"],
            [1, "payment_done", "raw", "2022-01-01 00:08:00"],
            # открытая
            [2, "start", "start", "2022-02-01 00:01:00"],
            [2, "product1", "raw", "2022-02-01 00:01:00"],
            [2, "product2", "raw", "2022-02-01 00:02:00"],
            [2, "cart", "raw", "2022-02-01 00:07:00"],
            [2, "payment_done", "raw", "2022-02-01 00:08:00"],
            # открытая, закрытая - кроме продукта, закрытая+ только каталог
            [3, "start", "start", "2022-01-01 00:01:00"],
            [3, "product1", "raw", "2022-01-01 00:01:00"],
            [3, "product2", "raw", "2022-01-01 00:02:00"],
            [3, "catalog", "raw", "2022-01-01 00:03:00"],
            [3, "catalog", "raw", "2022-01-01 00:04:00"],
            [3, "product18", "raw", "2022-01-01 00:06:30"],
            [3, "cart", "raw", "2022-01-01 00:07:00"],
            [3, "payment_done", "raw", "2022-01-01 00:08:00"],
            # открытая, закрытая, закрытая+
            [4, "start", "start", "2022-01-01 00:01:00"],
            [4, "catalog", "raw", "2022-01-01 00:01:00"],
            [4, "product1", "raw", "2022-01-01 00:02:00"],
            [4, "product2", "raw", "2022-01-01 00:03:00"],
            [4, "catalog", "raw", "2022-01-01 00:04:00"],
            [4, "product1", "raw", "2022-01-01 00:06:00"],
            [4, "product18", "raw", "2022-01-01 00:06:30"],
            [4, "cart", "raw", "2022-01-01 00:07:00"],
            [4, "payment_done", "raw", "2022-01-01 00:08:00"],
            # открытая
            [5, "start", "start", "2022-01-01 00:01:00"],
            [5, "product1", "raw", "2022-01-01 00:02:00"],
            [5, "product2", "raw", "2022-01-01 00:03:00"],
            [5, "product1", "raw", "2022-01-01 00:06:00"],
            [5, "product18", "raw", "2022-01-01 00:06:30"],
            [5, "cart", "raw", "2022-01-01 00:07:00"],
            [5, "payment_done", "raw", "2022-01-01 00:08:00"],
            # открытая, закрытая - кроме продукта, закрытая+ только каталог
            [6, "start", "start", "2022-01-01 00:01:00"],
            [6, "product1", "raw", "2022-01-01 00:01:00"],
            [6, "product2", "raw", "2022-01-01 00:02:00"],
            [6, "catalog", "raw", "2022-01-01 00:03:00"],
            [6, "catalog", "raw", "2022-01-01 00:04:00"],
            [6, "product18", "raw", "2022-01-01 00:06:30"],
            [6, "cart", "raw", "2022-01-01 00:07:00"],
            [6, "payment_done", "raw", "2022-01-01 00:08:00"],
            #
            [7, "start", "start", "2022-01-01 00:01:00"],
            [7, "catalog", "raw", "2022-01-01 00:01:00"],
            [7, "product1", "raw", "2022-01-01 00:02:00"],
            [7, "product2", "raw", "2022-01-01 00:03:00"],
            [7, "catalog", "raw", "2022-01-01 00:04:00"],
            [7, "product1", "raw", "2022-01-01 00:06:00"],
            [7, "product18", "raw", "2022-01-01 00:06:30"],
            [7, "payment_done", "raw", "2022-01-01 00:07:00"],
            [7, "cart", "raw", "2022-01-01 00:08:00"],
            #
            [8, "start", "start", "2022-01-01 00:01:00"],
            [8, "catalog", "raw", "2022-01-01 00:01:00"],
            [8, "product1", "raw", "2022-01-01 00:02:00"],
            [8, "product2", "raw", "2022-01-01 00:03:00"],
            [8, "catalog", "raw", "2022-01-01 00:04:00"],
            [8, "product1", "raw", "2022-01-01 00:06:00"],
            [8, "product18", "raw", "2022-01-01 00:06:30"],
            [8, "payment_done", "raw", "2022-01-01 00:07:00"],
            [8, "cart", "raw", "2022-01-01 00:08:00"],
        ],
        columns=["user_id", "event", "event_type", "timestamp"],
    )

    source_stream = Eventstream(
        raw_data=source_df,
        raw_data_schema=RawDataSchema(
            event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
        ),
        schema=EventstreamSchema(),
    )

    return source_stream
