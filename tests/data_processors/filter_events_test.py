import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream

def get_df():
    df = pd.DataFrame([
        ["user_1", "A", "2020-01-01 00:00:00", "US"],
        ["user_1", "B", "2020-01-02 00:00:00", "US"],
        ["user_1", "C", "2020-01-03 00:00:00", "US"],
        ["user_2", "A", "2020-01-01 00:00:00", "US"],
        ["user_3", "B", "2020-01-01 00:00:00", "UK"],
        ["user_3", "B", "2020-01-02 00:01:00", "UK"],
    ], columns=["user_id", "event", "timestamp", "country"])
    return df

class TestFilterEvents:
    def test__values_events(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.filter_events(by_column={"column": "event", "values": ["A", "C"]})

        expected_columns = ["user_id", "event", "timestamp", "country"]
        expected = pd.DataFrame([
            ["user_1", "A", "2020-01-01 00:00:00", "US"],
            ["user_1", "C", "2020-01-03 00:00:00", "US"],
            ["user_2", "A", "2020-01-01 00:00:00", "US"],
        ], columns=expected_columns)
        expected_schema = {"custom_cols": ["country"]}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__values_country(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.filter_events(by_column={"column": "country", "values": ["US"]})

        expected_columns = ["user_id", "event", "timestamp", "country"]
        expected = pd.DataFrame([
            ["user_1", "A", "2020-01-01 00:00:00", "US"],
            ["user_1", "B", "2020-01-02 00:00:00", "US"],
            ["user_1", "C", "2020-01-03 00:00:00", "US"],
            ["user_2", "A", "2020-01-01 00:00:00", "US"],
        ], columns=expected_columns)
        expected_schema = {"custom_cols": ["country"]}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__values_exclude(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.filter_events(by_column={"column": "event", "values": ["B"], "exclude": True})

        expected_columns = ["user_id", "event", "timestamp", "country"]
        expected = pd.DataFrame([
            ["user_1", "A", "2020-01-01 00:00:00", "US"],
            ["user_1", "C", "2020-01-03 00:00:00", "US"],
            ["user_2", "A", "2020-01-01 00:00:00", "US"],
        ], columns=expected_columns)
        expected_schema = {"custom_cols": ["country"]}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__values_missing_keys_raises(self) -> None:
        stream = Eventstream(get_df(), {"custom_cols": ["country"]})

        with pytest.raises(Exception):
            stream.filter_events(by_column={"event": ["A"]})

    def test__func_events(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.filter_events(func=lambda _df: _df["event"] != "B")

        expected_columns = ["user_id", "event", "timestamp", "country"]
        expected = pd.DataFrame([
            ["user_1", "A", "2020-01-01 00:00:00", "US"],
            ["user_1", "C", "2020-01-03 00:00:00", "US"],
            ["user_2", "A", "2020-01-01 00:00:00", "US"],
        ], columns=expected_columns)
        expected_schema = {"custom_cols": ["country"]}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__func_timestamp(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.filter_events(func=lambda _df: _df["timestamp"] < "2020-01-03")

        expected_columns = ["user_id", "event", "timestamp", "country"]
        expected = pd.DataFrame([
            ["user_1", "A", "2020-01-01 00:00:00", "US"],
            ["user_1", "B", "2020-01-02 00:00:00", "US"],
            ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ["user_3", "B", "2020-01-01 00:00:00", "UK"],
            ["user_3", "B", "2020-01-02 00:01:00", "UK"],
        ], columns=expected_columns)
        expected_schema = {"custom_cols": ["country"]}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__sql(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)
        query = "select * from eventstream where timestamp < '2020-01-03'"
        res = stream.filter_events(sql=query)

        expected_columns = ["user_id", "event", "timestamp", "country"]
        expected = pd.DataFrame([
            ["user_1", "A", "2020-01-01 00:00:00", "US"],
            ["user_1", "B", "2020-01-02 00:00:00", "US"],
            ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ["user_3", "B", "2020-01-01 00:00:00", "UK"],
            ["user_3", "B", "2020-01-02 00:01:00", "UK"],
        ], columns=expected_columns)
        expected_schema = {"custom_cols": ["country"]}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__sql_wrong_columns(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)
        query = "select *, 42 as wrong_column  from eventstream where timestamp < '2020-01-03'"

        with pytest.raises(Exception):
            stream.filter_events(sql=query)

    def test__synthetic(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream\
            .add_start_end_events()\
            .filter_events(func=lambda _df: ~_df["event"].isin(["path_end", "B"]))

        expected_columns = ["user_id", "event", "timestamp", "country"]
        expected = pd.DataFrame([
            ["user_1", "path_start", "2020-01-01 00:00:00", "US"],
            ["user_1", "A", "2020-01-01 00:00:00", "US"],
            ["user_1", "C", "2020-01-03 00:00:00", "US"],
            ["user_2", "path_start", "2020-01-01 00:00:00", "US"],
            ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ["user_3", "path_start", "2020-01-01 00:00:00", "UK"],
        ], columns=expected_columns)
        expected_schema = {"custom_cols": ["country"]}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__sql_big_df(self) -> None:
        df = pd.DataFrame({"user_id":
            [111]*100000 + [222]*100000 + [333]*100000 + [444]*100000 + [555]*100000,
        })
        df["event"] = "A"
        df["timestamp"] = "2020-01-01 00:00:00"
        stream = Eventstream(df)

        query = """
        select * from eventstream
        where (user_id % 2) = 0
        """
        res = stream.filter_events(sql=query)

        expected_df = df.copy()
        expected_df = expected_df[expected_df["user_id"] % 2 == 0]
        expected = Eventstream(expected_df)

        assert res.equals(expected)
