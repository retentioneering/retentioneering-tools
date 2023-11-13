from __future__ import annotations

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import RawDataSchema
from tests.data_processors_lib.fixtures.multiple_processors import (
    multiple_processors_source,
    split_session_and_group_corr,
)


class TestMultipleProcessors:
    def test_split_sessions_and_groups(self, multiple_processors_source, split_session_and_group_corr):
        def group(df, schema):
            return df[schema.event_name].isin(["event1", "event3"])

        columns = ["user_id", "event", "event_type", "timestamp", "event_index"]
        source_df = multiple_processors_source

        split_and_group = Eventstream(source_df, add_start_end_events=False).split_sessions(timeout=(100, "s")).group_events(event_name="event13", func=group).to_dataframe()  # type: ignore
        group_and_split = Eventstream(source_df, add_start_end_events=False).group_events(event_name="event13", func=group).split_sessions(timeout=(100, "s")).to_dataframe()  # type: ignore

        assert pd.testing.assert_frame_equal(split_and_group[columns], split_session_and_group_corr[columns]) is None
        assert pd.testing.assert_frame_equal(group_and_split[columns], split_session_and_group_corr[columns]) is None
