import pandas as pd

from src.data_processors_lib.rete import MergeParams, MergeProcessor
from src.eventstream import Eventstream, RawDataSchema


class TestMerge:
    rules = [{"group_name": "some_group", "child_events": ["eventA", "eventB"]}]

    def test__merge__dataprocessor(self) -> None:
        source_df = pd.DataFrame(
            [
                {"event_name": "eventA", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "eventA", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "eventB", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "eventB", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "eventC", "event_timestamp": "2021-10-26 12:04", "user_id": "2"},
                {"event_name": "eventC", "event_timestamp": "2021-10-26 12:05", "user_id": "1"},
            ]
        )

        expected = pd.DataFrame(
            [
                {"event_name": "some_group", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "some_group", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "some_group", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "some_group", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
            ]
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
        )

        params = MergeParams(rules=self.rules)
        processor = MergeProcessor(params=params)
        actual = processor.apply(eventstream=source).to_dataframe()
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test__merge__helper(self):
        source_df = pd.DataFrame(
            [
                {"event_name": "eventA", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "eventA", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "eventB", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "eventB", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "eventC", "event_timestamp": "2021-10-26 12:04", "user_id": "2"},
                {"event_name": "eventC", "event_timestamp": "2021-10-26 12:05", "user_id": "1"},
            ]
        )

        expected = pd.DataFrame(
            [
                {"event_name": "some_group", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "some_group", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "some_group", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "some_group", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "eventC", "event_timestamp": "2021-10-26 12:04", "user_id": "2"},
                {"event_name": "eventC", "event_timestamp": "2021-10-26 12:05", "user_id": "1"},
            ]
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
        )

        actual = source.merge(rules=self.rules)
        result_df = actual.to_dataframe()[expected.columns].reset_index(drop=True)
        assert result_df.compare(expected).shape == (0, 0)
