import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream


def get_df():
    df = pd.DataFrame(
        [
            ["user_1", "A", "2020-01-01 00:00:00", "US"],
            ["user_1", "B", "2020-01-02 00:00:00", "US"],
            ["user_1", "C", "2020-01-03 00:00:00", "US"],
            ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ["user_3", "B", "2020-01-01 00:00:00", "UK"],
            ["user_3", "B", "2020-01-02 00:01:00", "UK"],
        ],
        columns=["user_id", "event", "timestamp", "country"],
    )
    return df


class TestFilterEvents:
    def test__add_segment_values(self) -> None:
        df = get_df()
        schema = {"segment_cols": ["country"]}
        stream = Eventstream(df, schema)

        values = [["user_id", "in", "('user_1', 'user_3')", "female"], ["male"]]

        res = stream.add_segment(name="sex", rules=values)

        expected_df = df.copy()
        expected_df["sex"] = ["female", "female", "female", "male", "female", "female"]
        expected_schema = {"segment_cols": ["country", "sex"]}
        expected = Eventstream(expected_df, expected_schema)

        assert res.equals(expected)

    def test__add_segment_wrong_name(self) -> None:
        df = get_df()
        schema = {"segment_cols": ["country"]}
        stream = Eventstream(df, schema)

        values = ["female", "female", "female", "male", "female", "female"]

        with pytest.raises(Exception):
            stream.add_segment(name="user_id", rules=values)

        with pytest.raises(Exception):
            stream.add_segment(name="country", rules=values)

    def test__add_segment_sql(self) -> None:
        df = get_df()
        schema = {"segment_cols": ["country"]}
        stream = Eventstream(df, schema)

        query = """
        select
            case
                when user_id = 'user_1' then 'seg_1'
                when user_id = 'user_2' then 'seg_2'
                when user_id = 'user_3' then 'seg_3'
                else 'seg_0'
            end
        from eventstream
        """
        segment_name = "my_segment"

        res = stream.add_segment(name=segment_name, sql=query)

        expected_df = df.copy()
        expected_df[segment_name] = [
            "seg_1",
            "seg_1",
            "seg_1",
            "seg_2",
            "seg_3",
            "seg_3",
        ]
        expected_schema = {"segment_cols": ["country", segment_name]}
        expected = Eventstream(expected_df, expected_schema)

        assert res.equals(expected)

    def test__add_segment_wrong_sql_too_few_rows(self) -> None:
        df = get_df()
        schema = {"segment_cols": ["country"]}
        stream = Eventstream(df, schema)

        query = """
        select
            case
                when user_id = 'user_1' then 'seg_1'
                when user_id = 'user_2' then 'seg_2'
                when user_id = 'user_3' then 'seg_3'
                else 'seg_0'
            end
        from eventstream
        where timestamp < '2020-01-03'
        """
        segment_name = "my_segment"

        with pytest.raises(Exception):
            stream.add_segment(name=segment_name, sql=query)

    def test__add_segment_wrong_sql_multiple_columns(self) -> None:
        df = get_df()
        schema = {"segment_cols": ["country"]}
        stream = Eventstream(df, schema)

        query = """
        select
            *,
            case
                when user_id = 'user_1' then 'seg_1'
                when user_id = 'user_2' then 'seg_2'
                when user_id = 'user_3' then 'seg_3'
                else 'seg_0'
            end as segment
        from eventstream
        """
        segment_name = "my_segment"

        with pytest.raises(Exception):
            stream.add_segment(name=segment_name, sql=query)

    def test__add_segment_sql_big_df(self) -> None:
        df = pd.DataFrame(
            {
                "user_id": [111] * 100000
                + [222] * 100000
                + [333] * 100000
                + [444] * 100000
                + [555] * 100000,
            }
        )
        df["event"] = "A"
        df["timestamp"] = "2020-01-01 00:00:00"
        stream = Eventstream(df)

        query = """
        select
            cast((user_id % 2) as varchar) as segment
        from eventstream
        """
        segment_name = "my_segment"
        res = stream.add_segment(name=segment_name, sql=query)

        expected_df = df.copy()
        expected_df[segment_name] = (
            ["1"] * 100000
            + ["0"] * 100000
            + ["1"] * 100000
            + ["0"] * 100000
            + ["1"] * 100000
        )
        expected_schema = {"segment_cols": [segment_name]}
        expected = Eventstream(expected_df, expected_schema)

        assert res.equals(expected)

        segment_name_2 = segment_name + "_2"
        res2 = res.add_segment(name=segment_name_2, sql=query)
        assert all(res2.df[segment_name] == res2.df[segment_name_2])

    def test__drop_segment(self):
        df = get_df()
        schema = {"segment_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.drop_segment("country")

        expected_df = df.drop(columns="country")
        expected_schema = {"segment_cols": []}
        expected = Eventstream(expected_df, expected_schema)

        assert res.equals(expected)

    def test__drop_segment_wrong_column(self):
        df = get_df()
        schema = {"segment_cols": ["country"]}
        stream = Eventstream(df, schema)

        with pytest.raises(Exception):
            stream.drop_segment("event")

    def test__get_segment_values(self):
        df = get_df()
        df["sex"] = ["female", "female", "female", "male", "female", "female"]
        schema = {"segment_cols": ["country", "sex"]}
        stream = Eventstream(df, schema)
        res = stream.get_segment_values()

        expected = {"country": ["UK", "US"], "sex": ["female", "male"]}
        assert res == expected


class TestSplitTwo:
    def test___split_two_outer_literal(self) -> None:
        """Test _split_two with <REST> literal for complement selection."""
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "seg_1"],
                ["user_2", "B", "2020-01-02 00:00:00", "seg_1"],
                ["user_3", "C", "2020-01-01 00:00:00", "seg_2"],
                ["user_4", "D", "2020-01-01 00:00:00", "seg_3"],
                ["user_5", "E", "2020-01-02 00:00:00", "seg_3"],
            ],
            columns=["user_id", "event", "timestamp", "my_segment"],
        )
        schema = {"segment_cols": ["my_segment"]}
        stream = Eventstream(df, schema)

        # Split by segment: seg_1 vs all others (seg_2, seg_3)
        stream1, stream2 = stream._split_two(["my_segment", "seg_1", "<REST>"])

        # stream1 should contain only seg_1 rows
        assert set(stream1.df["user_id"].tolist()) == {"user_1", "user_2"}
        assert set(stream1.df["my_segment"].unique()) == {"seg_1"}

        # stream2 should contain seg_2 and seg_3 rows (complement of seg_1)
        assert set(stream2.df["user_id"].tolist()) == {"user_3", "user_4", "user_5"}
        assert set(stream2.df["my_segment"].unique()) == {"seg_2", "seg_3"}
