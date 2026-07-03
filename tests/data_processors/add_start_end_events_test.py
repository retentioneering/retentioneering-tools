import pandas as pd
from retentioneering.eventstream.eventstream import Eventstream

class TestAddStartEndEvents:
    def test__simple(self) -> None:
        df = pd.DataFrame([
            ["user_1", "A", "2020-01-01 00:00:00", "US"],
            ["user_1", "B", "2020-01-01 00:01:00", "US"],
            ["user_1", "C", "2020-01-01 00:02:00", "US"],
            ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ["user_3", "B", "2020-01-01 00:00:00", "UK"],
            ["user_3", "B", "2020-01-01 00:01:00", "UK"],
            ["user_3", "B", "2020-01-01 00:02:00", "UK"],
            ["user_3", "B", "2020-01-01 00:03:00", "UK"],
        ], columns=["user_id", "event", "timestamp", "country"])
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.add_start_end_events()

        expected_columns = ["user_id", "event", "event_type", "timestamp", "country"]
        expected = pd.DataFrame([
            ["user_1", "path_start", "path_start", "2020-01-01 00:00:00", "US"],
            ["user_1", "A", "raw", "2020-01-01 00:00:00", "US"],
            ["user_1", "B", "raw", "2020-01-01 00:01:00", "US"],
            ["user_1", "C", "raw", "2020-01-01 00:02:00", "US"],
            ["user_1", "path_end", "path_end", "2020-01-01 00:02:00", "US"],
            ["user_2", "path_start", "path_start", "2020-01-01 00:00:00", "US"],
            ["user_2", "A", "raw", "2020-01-01 00:00:00", "US"],
            ["user_2", "path_end", "path_end", "2020-01-01 00:00:00", "US"],
            ["user_3", "path_start", "path_start", "2020-01-01 00:00:00", "UK"],
            ["user_3", "B", "raw", "2020-01-01 00:00:00", "UK"],
            ["user_3", "B", "raw", "2020-01-01 00:01:00", "UK"],
            ["user_3", "B", "raw", "2020-01-01 00:02:00", "UK"],
            ["user_3", "B", "raw", "2020-01-01 00:03:00", "UK"],
            ["user_3", "path_end", "path_end", "2020-01-01 00:03:00", "UK"],
        ], columns=expected_columns)
        expected_schema = {"custom_cols": ["country"], "event_type": "event_type"}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__existing_start_end(self) -> None:
        df = pd.DataFrame([
            ["user_1", "path_start", "path_start", "2020-01-01 00:00:00", "US"],
            ["user_1", "A", "raw", "2020-01-01 00:00:00", "US"],
            ["user_1", "B", "raw", "2020-01-01 00:01:00", "US"],
            ["user_1", "C", "raw", "2020-01-01 00:02:00", "US"],
            ["user_2", "path_start", "path_start", "2020-01-01 00:00:00", "US"],
            ["user_2", "A", "raw", "2020-01-01 00:00:00", "US"],
            ["user_2", "path_end", "path_end", "2020-01-01 00:00:00", "US"],
            ["user_3", "B", "raw", "2020-01-01 00:00:00", "UK"],
            ["user_3", "B", "raw", "2020-01-01 00:01:00", "UK"],
            ["user_3", "B", "raw", "2020-01-01 00:02:00", "UK"],
            ["user_3", "B", "raw", "2020-01-01 00:03:00", "UK"],
            ["user_3", "path_end", "path_end", "2020-01-01 00:03:00", "UK"],
        ], columns=["user_id", "event", "event_type", "timestamp", "country"])
        schema = {"custom_cols": ["country"], "event_type": "event_type"}
        stream = Eventstream(df, schema)

        res = stream.add_start_end_events()

        expected_columns = ["user_id", "event", "event_type", "timestamp", "country"]
        expected = pd.DataFrame([
            ["user_1", "path_start", "path_start", "2020-01-01 00:00:00", "US"],
            ["user_1", "A", "raw", "2020-01-01 00:00:00", "US"],
            ["user_1", "B", "raw", "2020-01-01 00:01:00", "US"],
            ["user_1", "C", "raw", "2020-01-01 00:02:00", "US"],
            ["user_1", "path_end", "path_end", "2020-01-01 00:02:00", "US"],
            ["user_2", "path_start", "path_start", "2020-01-01 00:00:00", "US"],
            ["user_2", "A", "raw", "2020-01-01 00:00:00", "US"],
            ["user_2", "path_end", "path_end", "2020-01-01 00:00:00", "US"],
            ["user_3", "path_start", "path_start", "2020-01-01 00:00:00", "UK"],
            ["user_3", "B", "raw", "2020-01-01 00:00:00", "UK"],
            ["user_3", "B", "raw", "2020-01-01 00:01:00", "UK"],
            ["user_3", "B", "raw", "2020-01-01 00:02:00", "UK"],
            ["user_3", "B", "raw", "2020-01-01 00:03:00", "UK"],
            ["user_3", "path_end", "path_end", "2020-01-01 00:03:00", "UK"],
        ], columns=expected_columns)
        expected_schema = {"custom_cols": ["country"], "event_type": "event_type"}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)
