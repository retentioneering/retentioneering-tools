import numpy as np
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
    def test__keep_events(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.filter_events(keep={"event": ["A", "C"]})

        expected_columns = ["user_id", "event", "timestamp", "country"]
        expected = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "US"],
                ["user_1", "C", "2020-01-03 00:00:00", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ],
            columns=expected_columns,
        )
        expected_schema = {"custom_cols": ["country"]}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__keep_country(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.filter_events(keep={"country": ["US"]})

        expected_columns = ["user_id", "event", "timestamp", "country"]
        expected = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "US"],
                ["user_1", "B", "2020-01-02 00:00:00", "US"],
                ["user_1", "C", "2020-01-03 00:00:00", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ],
            columns=expected_columns,
        )
        expected_schema = {"custom_cols": ["country"]}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__drop_events(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.filter_events(drop={"event": ["B"]})

        expected_columns = ["user_id", "event", "timestamp", "country"]
        expected = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "US"],
                ["user_1", "C", "2020-01-03 00:00:00", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ],
            columns=expected_columns,
        )
        expected_schema = {"custom_cols": ["country"]}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__keep_multiple_columns_and(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        # AND semantics: event in [A, B] AND country in [US]
        res = stream.filter_events(keep={"event": ["A", "B"], "country": ["US"]})

        expected = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "US"],
                ["user_1", "B", "2020-01-02 00:00:00", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ],
            columns=["user_id", "event", "timestamp", "country"],
        )
        expected = Eventstream(expected, {"custom_cols": ["country"]})

        assert res.equals(expected)

    def test__drop_multiple_columns_or(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        # OR semantics: remove rows where event is B or country is UK
        res = stream.filter_events(drop={"event": ["B"], "country": ["UK"]})

        expected = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "US"],
                ["user_1", "C", "2020-01-03 00:00:00", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ],
            columns=["user_id", "event", "timestamp", "country"],
        )
        expected = Eventstream(expected, {"custom_cols": ["country"]})

        assert res.equals(expected)

    def test__keep_unknown_event_raises(self) -> None:
        stream = Eventstream(get_df(), {"custom_cols": ["country"]})

        with pytest.raises(Exception):
            stream.filter_events(keep={"event": ["xxxxxxxx"]})

    def test__drop_unknown_event_raises(self) -> None:
        stream = Eventstream(get_df(), {"custom_cols": ["country"]})

        with pytest.raises(Exception):
            stream.filter_events(drop={"event": ["xxxxxxxx"]})

    def test__keep_unknown_path_id_raises(self) -> None:
        stream = Eventstream(get_df(), {"custom_cols": ["country"]})

        with pytest.raises(Exception):
            stream.filter_events(keep={"user_id": ["user_xxxxx"]})

    def test__scalar_values_raises(self) -> None:
        stream = Eventstream(get_df(), {"custom_cols": ["country"]})

        with pytest.raises(Exception):
            stream.filter_events(keep={"event": "A"})

    def test__keep_and_drop_together_raises(self) -> None:
        stream = Eventstream(get_df(), {"custom_cols": ["country"]})

        with pytest.raises(Exception):
            stream.filter_events(keep={"event": ["A"]}, drop={"event": ["B"]})

    def test__empty_values_list_raises(self) -> None:
        stream = Eventstream(get_df(), {"custom_cols": ["country"]})

        with pytest.raises(Exception):
            stream.filter_events(keep={"event": []})

        with pytest.raises(Exception):
            stream.filter_events(drop={"event": []})

    def test__func_events(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.filter_events(func=lambda _df: _df["event"] != "B")

        expected_columns = ["user_id", "event", "timestamp", "country"]
        expected = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "US"],
                ["user_1", "C", "2020-01-03 00:00:00", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ],
            columns=expected_columns,
        )
        expected_schema = {"custom_cols": ["country"]}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__func_timestamp(self) -> None:
        df = get_df()
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.filter_events(func=lambda _df: _df["timestamp"] < "2020-01-03")

        expected_columns = ["user_id", "event", "timestamp", "country"]
        expected = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "US"],
                ["user_1", "B", "2020-01-02 00:00:00", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "US"],
                ["user_3", "B", "2020-01-01 00:00:00", "UK"],
                ["user_3", "B", "2020-01-02 00:01:00", "UK"],
            ],
            columns=expected_columns,
        )
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
        expected = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "US"],
                ["user_1", "B", "2020-01-02 00:00:00", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "US"],
                ["user_3", "B", "2020-01-01 00:00:00", "UK"],
                ["user_3", "B", "2020-01-02 00:01:00", "UK"],
            ],
            columns=expected_columns,
        )
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

        res = stream.add_start_end_events().filter_events(
            func=lambda _df: ~_df["event"].isin(["path_end", "B"])
        )

        expected_columns = ["user_id", "event", "timestamp", "country"]
        expected = pd.DataFrame(
            [
                ["user_1", "path_start", "2020-01-01 00:00:00", "US"],
                ["user_1", "A", "2020-01-01 00:00:00", "US"],
                ["user_1", "C", "2020-01-03 00:00:00", "US"],
                ["user_2", "path_start", "2020-01-01 00:00:00", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "US"],
                ["user_3", "path_start", "2020-01-01 00:00:00", "UK"],
            ],
            columns=expected_columns,
        )
        expected_schema = {"custom_cols": ["country"]}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__sql_big_df(self) -> None:
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
        select * from eventstream
        where (user_id % 2) = 0
        """
        res = stream.filter_events(sql=query)

        expected_df = df.copy()
        expected_df = expected_df[expected_df["user_id"] % 2 == 0]
        expected = Eventstream(expected_df)

        assert res.equals(expected)


class TestFilterEventsValueTables:
    """`keep` and `drop` send their values as a small table, not as SQL text."""

    @staticmethod
    def _stream(n_paths: int) -> Eventstream:
        df = pd.DataFrame(
            {
                "user_id": [f"user_{i}" for i in range(n_paths)],
                "event": ["A"] * n_paths,
                "timestamp": ["2020-01-01 00:00:00"] * n_paths,
            }
        )
        return Eventstream(df)

    @staticmethod
    def _spy_on_engine(monkeypatch) -> list:
        """Keeps the SQL text of every query the engine is asked to run."""
        from retentioneering import engine

        seen: list = []
        real_run = engine.run

        def spy(sql, /, **tables):
            seen.append(sql)
            return real_run(sql, **tables)

        monkeypatch.setattr(engine, "run", spy)
        return seen

    def test__query_text_does_not_grow_with_value_count(self, monkeypatch) -> None:
        """This is the point of the change. The query stays the same size,
        however many values you filter on. Before, it grew with every value."""
        lengths = []
        for n_paths in (10, 2000):
            stream = self._stream(n_paths)
            keep_ids = [f"user_{i}" for i in range(n_paths)]
            seen = self._spy_on_engine(monkeypatch)
            stream.filter_events(keep={"user_id": keep_ids})
            filter_queries = [q for q in seen if "_filter_values_0" in q]
            assert len(filter_queries) == 1
            lengths.append(len(filter_queries[0]))
            # no id may show up in the query text itself
            assert f"user_{n_paths - 1}" not in filter_queries[0]

        assert lengths[0] == lengths[1]

    def test__one_value_table_per_column(self, monkeypatch) -> None:
        df = get_df()
        stream = Eventstream(df, {"custom_cols": ["country"]})
        seen = self._spy_on_engine(monkeypatch)

        res = stream.filter_events(keep={"event": ["B"], "country": ["UK"]})

        query = next(q for q in seen if "_filter_values_0" in q)
        assert "_filter_values_1" in query
        assert res.to_dataframe()["user_id"].tolist() == ["user_3", "user_3"]

    @staticmethod
    def _stream_with_a_gap() -> Eventstream:
        """Six rows where the first one has no country."""
        df = get_df()
        df.loc[0, "country"] = np.nan
        return Eventstream(df, {"custom_cols": ["country"]})

    def test__keep_a_missing_value_selects_those_rows(self) -> None:
        """`[np.nan]` means the rows where this column has no value."""
        stream = self._stream_with_a_gap()

        res = stream.filter_events(keep={"country": [np.nan]}).to_dataframe()

        assert len(res) == 1
        assert res["country"].isna().all()

    def test__drop_a_missing_value_removes_those_rows(self) -> None:
        stream = self._stream_with_a_gap()

        res = stream.filter_events(drop={"country": [np.nan]}).to_dataframe()

        assert len(res) == 5
        assert not res["country"].isna().any()

    def test__drop_a_real_value_keeps_the_rows_with_no_value(self) -> None:
        """A row with no value never matched the list, so `drop` has to keep it.
        In SQL `NULL in ('US')` gives back NULL, and without `coalesce` the
        `not (...)` around it would throw that row away too."""
        stream = self._stream_with_a_gap()

        res = stream.filter_events(drop={"country": ["US"]}).to_dataframe()

        assert res["country"].isna().sum() == 1
        assert set(res["country"].dropna()) == {"UK"}

    def test__keep_a_real_value_together_with_a_missing_one(self) -> None:
        stream = self._stream_with_a_gap()

        res = stream.filter_events(keep={"country": ["UK", np.nan]}).to_dataframe()

        assert len(res) == 3
        assert res["country"].isna().sum() == 1
        assert set(res["country"].dropna()) == {"UK"}

    def test__keep_and_drop_stay_opposites(self) -> None:
        """Whatever the list holds, the two sides must add up to every row."""
        stream = self._stream_with_a_gap()
        total = len(stream.to_dataframe())

        for values in ([np.nan], ["US"], ["UK"], ["US", np.nan], ["US", "UK"]):
            kept = stream.filter_events(keep={"country": values}).to_dataframe()
            dropped = stream.filter_events(drop={"country": values}).to_dataframe()
            assert len(kept) + len(dropped) == total, values

    def test__value_table_wins_over_a_name_left_in_the_catalog(self) -> None:
        """A caller can leave a table behind with `sql=`, and it stays there for
        the life of the process (see `engine.run`). The table we register for
        this call must still win."""
        from retentioneering import engine

        root = engine._root()
        root.execute("CREATE TABLE _filter_values_0 AS SELECT 'user_2' AS v")
        try:
            df = get_df()
            stream = Eventstream(df, {"custom_cols": ["country"]})

            res = stream.filter_events(keep={"event": ["A"]}).to_dataframe()

            assert res["event"].tolist() == ["A", "A"]
            assert res["user_id"].tolist() == ["user_1", "user_2"]
        finally:
            root.execute("DROP TABLE IF EXISTS _filter_values_0")

    def test__values_needing_escaping_round_trip(self) -> None:
        """We no longer put quotes around values by hand. Characters that used
        to need escaping must still work."""
        awkward = ["it's", 'say "hi"', "a,b;c", "naïve ünïcode", "x" * 300]
        df = pd.DataFrame(
            {
                "user_id": [f"user_{i}" for i in range(len(awkward))],
                "event": ["A"] * len(awkward),
                "timestamp": ["2020-01-01 00:00:00"] * len(awkward),
                "label": awkward,
            }
        )
        stream = Eventstream(df, {"custom_cols": ["label"]})

        res = stream.filter_events(keep={"label": awkward[:2]}).to_dataframe()

        assert sorted(res["label"].tolist()) == sorted(awkward[:2])

    def test__mixed_type_column_is_matched_as_text(self) -> None:
        """A column can hold values of different python types. DuckDB reads such
        a column as text, so the number 1 and the string "1" become the same
        value, and so do True and "True". This test writes that behaviour down,
        because pandas `isin` would not give the same answer."""
        df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_2", "user_3", "user_4"],
                "event": ["A", "B", "C", "D"],
                "timestamp": ["2020-01-01 00:00:00"] * 4,
                "tag": pd.Series([1, "True", 3.5, True], dtype=object),
            }
        )
        stream = Eventstream(df, {"custom_cols": ["tag"]})

        res = stream.filter_events(keep={"tag": [1, "True"]}).to_dataframe()

        # 1 matches the number 1. "True" matches both the string "True" and the
        # boolean True, because written as text the two are the same.
        assert res["tag"].tolist() == ["1", "True", "True"]

    def test__integer_path_ids(self) -> None:
        df = pd.DataFrame(
            {
                "user_id": [1, 2, 3],
                "event": ["A", "B", "C"],
                "timestamp": ["2020-01-01 00:00:00"] * 3,
            }
        )
        stream = Eventstream(df)

        res = stream.filter_events(keep={"user_id": [1, 3]}).to_dataframe()

        assert res["user_id"].tolist() == [1, 3]
