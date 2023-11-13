from __future__ import annotations

import pandas as pd
import pytest

from retentioneering.data_processors_lib import Pipe, PipeParams
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import EventstreamSchema, RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestPipeDataprocessor(ApplyTestBase):
    _Processor = Pipe
    _source_df = pd.DataFrame(
        [
            [1, "pageview", "2021-10-26 12:00"],
            [1, "cart_btn_click", "2021-10-26 12:02"],
            [1, "pageview", "2021-10-26 12:03"],
            [2, "plus_icon_click", "2021-10-26 12:04"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_pipe(self) -> None:
        def mapper(df: pd.DataFrame, schema: EventstreamSchema):
            df[schema.event_name] = "new_event"
            return df

        actual = self._apply_dataprocessor(
            params=PipeParams(
                func=mapper,
            ),
        )
        expected = pd.DataFrame(
            [
                [1, "new_event", "raw", "2021-10-26 12:00"],
                [1, "new_event", "raw", "2021-10-26 12:02"],
                [1, "new_event", "raw", "2021-10-26 12:03"],
                [2, "new_event", "raw", "2021-10-26 12:04"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)
