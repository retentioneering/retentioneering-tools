import pandas as pd
from retentioneering.eventstream.eventstream import Eventstream

def get_df():
    df = pd.DataFrame([
        ["user_1", "A", "2020-01-01 00:00:00", "US"],
        ["user_1", "B", "2020-01-01 00:01:00", "US"],
        ["user_1", "C", "2020-01-01 00:02:00", "US"],
        ["user_2", "A", "2020-01-01 00:00:00", "US"],
        ["user_2", "B", "2020-01-01 00:01:00", "US"],
        ["user_3", "A", "2020-01-01 00:00:00", "UK"],
        ["user_4", "A", "2020-01-01 00:00:00", "UK"],
        ["user_4", "B", "2020-01-01 00:01:00", "UK"],
    ], columns=["user_id", "event", "timestamp", "country"])
    return df

class TestSamplePaths:
    def test__sample_size_float(self) -> None:
        df = get_df()
        schema = {"segment_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.sample_paths(sample_size=0.5, random_state=42)

        expected = pd.DataFrame([
            ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ["user_2", "B", "2020-01-01 00:01:00", "US"],
            ["user_4", "A", "2020-01-01 00:00:00", "UK"],
            ["user_4", "B", "2020-01-01 00:01:00", "UK"],
        ], columns=["user_id", "event", "timestamp", "country"])
        expected = Eventstream(expected, schema)

        assert res.equals(expected)

    def test__sample_size_int(self) -> None:
        df = get_df()
        schema = {"segment_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.sample_paths(sample_size=3, random_state=42)

        expected = pd.DataFrame([
            ["user_1", "A", "2020-01-01 00:00:00", "US"],
            ["user_1", "B", "2020-01-01 00:01:00", "US"],
            ["user_1", "C", "2020-01-01 00:02:00", "US"],
            ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ["user_2", "B", "2020-01-01 00:01:00", "US"],
            ["user_3", "A", "2020-01-01 00:00:00", "UK"],
        ], columns=["user_id", "event", "timestamp", "country"])
        expected = Eventstream(expected, schema)

        assert res.equals(expected)

    def test__sample_size_1_float(self) -> None:
        df = get_df()
        schema = {"segment_cols": ["country"]}
        stream = Eventstream(df, schema)
        res = stream.sample_paths(sample_size=1., random_state=42)

        expected = df.copy()
        expected = Eventstream(expected, schema)

        assert res.equals(expected)

    def test__sample_size_1_int(self) -> None:
        df = get_df()
        schema = {"segment_cols": ["country"]}
        stream = Eventstream(df, schema)
        res = stream.sample_paths(sample_size=1, random_state=42)

        expected = pd.DataFrame([
            ["user_1", "A", "2020-01-01 00:00:00", "US"],
            ["user_1", "B", "2020-01-01 00:01:00", "US"],
            ["user_1", "C", "2020-01-01 00:02:00", "US"],
        ], columns=["user_id", "event", "timestamp", "country"])
        expected = Eventstream(expected, schema)

        assert res.equals(expected)

    def test__sample_size_too_large(self) -> None:
        df = get_df()
        schema = {"segment_cols": ["country"]}
        stream = Eventstream(df, schema)
        res = stream.sample_paths(sample_size=10, random_state=42)

        expected = df.copy()
        expected = Eventstream(expected, schema)

        assert res.equals(expected)
