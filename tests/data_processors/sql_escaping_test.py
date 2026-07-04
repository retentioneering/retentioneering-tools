"""
End-to-end tests for SQL literal escaping: user-supplied strings containing
single quotes (apostrophes) must not break DuckDB queries or allow the quote
to escape the SQL string literal.
"""

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream


def make_stream(rows):
    df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp"])
    return Eventstream(df)


class TestFilterEventsEscaping:
    def test__values_with_apostrophe_include(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_page", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
                ["user_2", "B", "2020-01-01 00:00:00"],
            ]
        )

        res = stream.filter_events(
            by_column={"column": "event", "values": ["o'brien_page"]}
        )

        assert list(res.df["event"].astype(str)) == ["o'brien_page"]

    def test__values_with_apostrophe_exclude(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_page", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
                ["user_2", "B", "2020-01-01 00:00:00"],
            ]
        )

        res = stream.filter_events(
            by_column={"column": "event", "values": ["o'brien_page"], "exclude": True}
        )

        assert list(res.df["event"].astype(str)) == ["A", "B"]

    def test__values_with_int_path_ids(self) -> None:
        df = pd.DataFrame(
            [
                [1, "A", "2020-01-01 00:00:00"],
                [2, "B", "2020-01-01 00:00:00"],
                [3, "C", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)

        res = stream.filter_events(by_column={"column": "user_id", "values": [1, 3]})

        assert sorted(res.df["user_id"].tolist()) == [1, 3]


class TestTruncatePathsEscaping:
    def test__boundaries_with_apostrophe(self) -> None:
        stream = make_stream(
            [
                ["user_1", "noise", "2020-01-01 00:00:00"],
                ["user_1", "cart's_start", "2020-01-01 00:01:00"],
                ["user_1", "A", "2020-01-01 00:02:00"],
                ["user_1", "cart's_end", "2020-01-01 00:03:00"],
                ["user_1", "noise", "2020-01-01 00:04:00"],
            ]
        )

        res = stream.truncate_paths(left="cart's_start", right="cart's_end")

        assert list(res.df["event"].astype(str)) == ["cart's_start", "A", "cart's_end"]

    def test__same_boundary_with_apostrophe(self) -> None:
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "o'brien_page", "2020-01-01 00:01:00"],
                ["user_1", "B", "2020-01-01 00:02:00"],
            ]
        )

        res = stream.truncate_paths(left="o'brien_page", right="o'brien_page")

        assert list(res.df["event"].astype(str)) == ["o'brien_page"]


class TestAddSegmentEscaping:
    def test__values_with_apostrophe_in_value_and_labels(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_page", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
                ["user_2", "B", "2020-01-01 00:00:00"],
            ]
        )

        values = [
            ["event", "=", "o'brien_page", "o'brien's segment"],
            ["other's segment"],
        ]
        res = stream.add_segment(name="seg", values=values)

        assert list(res.df["seg"].astype(str)) == [
            "o'brien's segment",
            "other's segment",
            "other's segment",
        ]


class TestSplitSessionsEscaping:
    def test__separator_with_apostrophe(self) -> None:
        stream = make_stream(
            [
                ["user_1", "day's_start", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
                ["user_1", "B", "2020-01-01 00:02:00"],
            ]
        )

        res = stream.split_sessions(separator="day's_start")
        df = res.df

        assert "day's_start" not in df["event"].values
        assert list(df["event"].astype(str)) == ["A", "B"]
        assert list(df["session_index"]) == [1, 1]


class TestMatchesPatternEscaping:
    def test__pattern_with_apostrophe_no_matches(self) -> None:
        stream = make_stream(
            [
                ["user_1", "promo_view", "2020-01-01 00:00:00"],
                ["user_1", "purchase", "2020-01-01 00:01:00"],
            ]
        )

        metrics = stream.get_metrics(
            [{"metric": "matches", "metric_args": {"pattern": "o'brien->purchase"}}]
        )

        assert not metrics[metrics.columns[0]].any()

    def test__pattern_with_apostrophe_matches(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_page", "2020-01-01 00:00:00"],
                ["user_1", "purchase", "2020-01-01 00:01:00"],
                ["user_2", "promo_view", "2020-01-01 00:00:00"],
            ]
        )

        metrics = stream.get_metrics(
            [
                {
                    "metric": "matches",
                    "metric_args": {"pattern": "o'brien_page->purchase"},
                }
            ]
        )

        col = metrics.columns[0]
        assert bool(metrics.loc["user_1", col])
        assert not bool(metrics.loc["user_2", col])
