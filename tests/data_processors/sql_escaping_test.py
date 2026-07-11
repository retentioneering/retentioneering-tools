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

        res = stream.filter_events(keep={"event": ["o'brien_page"]})

        assert list(res.df["event"].astype(str)) == ["o'brien_page"]

    def test__values_with_apostrophe_exclude(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_page", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
                ["user_2", "B", "2020-01-01 00:00:00"],
            ]
        )

        res = stream.filter_events(drop={"event": ["o'brien_page"]})

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

        res = stream.filter_events(keep={"user_id": [1, 3]})

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

        res = stream.truncate_paths(start_event="cart's_start", end_event="cart's_end")

        assert list(res.df["event"].astype(str)) == ["cart's_start", "A", "cart's_end"]

    def test__same_boundary_with_apostrophe(self) -> None:
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "o'brien_page", "2020-01-01 00:01:00"],
                ["user_1", "B", "2020-01-01 00:02:00"],
            ]
        )

        res = stream.truncate_paths(
            start_event="o'brien_page", end_event="o'brien_page"
        )

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
        res = stream.add_segment(name="seg", rules=values)

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


class TestGetMetricsEscaping:
    def test__event_count_with_apostrophe(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_page", "2020-01-01 00:00:00"],
                ["user_1", "o'brien_page", "2020-01-01 00:01:00"],
                ["user_1", "A", "2020-01-01 00:02:00"],
            ]
        )

        metrics = stream.get_metrics(
            [{"metric": "event_count", "metric_args": {"events": "o'brien_page"}}]
        )

        assert metrics.loc["user_1", metrics.columns[0]] == 2

    def test__has_event_with_apostrophe(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_page", "2020-01-01 00:00:00"],
                ["user_2", "A", "2020-01-01 00:00:00"],
            ]
        )

        metrics = stream.get_metrics(
            [{"metric": "has_event", "metric_args": {"events": "o'brien_page"}}]
        )

        col = metrics.columns[0]
        assert bool(metrics.loc["user_1", col])
        assert not bool(metrics.loc["user_2", col])

    def test__time_between_with_apostrophe_events(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_start", "2020-01-01 00:00:00"],
                ["user_1", "o'brien_end", "2020-01-01 00:01:40"],
            ]
        )

        metrics = stream.get_metrics(
            [
                {
                    "metric": "time_between",
                    "metric_args": {
                        "start_event": "o'brien_start",
                        "end_event": "o'brien_end",
                    },
                }
            ]
        )

        assert metrics.loc["user_1", metrics.columns[0]] == 100.0

    def test__active_days_with_apostrophe_events(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_page", "2020-01-01 00:00:00"],
                ["user_1", "o'brien_page", "2020-01-02 00:00:00"],
                ["user_1", "A", "2020-01-03 00:00:00"],
            ]
        )

        metrics = stream.get_metrics(
            [
                {
                    "metric": "active_days",
                    "metric_args": {"active_events": "o'brien_page"},
                }
            ]
        )

        assert metrics.loc["user_1", "active_days"] == 2


class TestCollapseEventsMetricEscaping:
    def test__has_event_case_with_apostrophe(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_page", "2020-01-01 00:00:00"],
                ["user_1", "sep", "2020-01-01 00:01:00"],
            ]
        )

        res = stream.collapse_events(
            event_groups=[
                {
                    "separator": "sep",
                    "cases": [
                        {
                            "condition": {
                                "op": ">",
                                "metric": "has_event",
                                "value": 0,
                                "metric_args": {"events": "o'brien_page"},
                            },
                            "name": "obrien_session",
                        }
                    ],
                    "name": "other_session",
                }
            ]
        )

        assert "obrien_session" in list(res.df["event"].astype(str))

    def test__event_count_case_with_apostrophe(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_page", "2020-01-01 00:00:00"],
                ["user_1", "o'brien_page", "2020-01-01 00:01:00"],
                ["user_1", "sep", "2020-01-01 00:02:00"],
            ]
        )

        res = stream.collapse_events(
            event_groups=[
                {
                    "separator": "sep",
                    "cases": [
                        {
                            "condition": {
                                "op": ">",
                                "metric": "event_count",
                                "value": 1,
                                "metric_args": {"events": "o'brien_page"},
                            },
                            "name": "active_session",
                        }
                    ],
                    "name": "quiet_session",
                }
            ]
        )

        assert list(res.df["event"].astype(str)) == ["active_session"]

    def test__time_between_case_with_apostrophe(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_start", "2020-01-01 00:00:00"],
                ["user_1", "o'brien_end", "2020-01-01 00:01:40"],
                ["user_1", "sep", "2020-01-01 00:02:00"],
            ]
        )

        res = stream.collapse_events(
            event_groups=[
                {
                    "separator": "sep",
                    "cases": [
                        {
                            "condition": {
                                "op": ">=",
                                "metric": "time_between",
                                "value": 100,
                                "metric_args": {
                                    "start_event": "o'brien_start",
                                    "end_event": "o'brien_end",
                                },
                            },
                            "name": "slow_session",
                        }
                    ],
                    "name": "fast_session",
                }
            ]
        )

        assert list(res.df["event"].astype(str)) == ["slow_session"]

    def test__consecutive_list_with_apostrophe(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_page", "2020-01-01 00:00:00"],
                ["user_1", "o'brien_page", "2020-01-01 00:01:00"],
                ["user_1", "A", "2020-01-01 00:02:00"],
            ]
        )

        res = stream.collapse_events(consecutive=["o'brien_page"])

        assert list(res.df["event"].astype(str)) == ["o'brien_page", "A"]


class TestAddEventsChurnEscaping:
    def test__churn_active_events_with_apostrophe(self) -> None:
        stream = make_stream(
            [
                ["user_1", "o'brien_page", "2020-01-01 00:00:00"],
                ["user_1", "o'brien_page", "2020-03-01 00:00:00"],
            ]
        )

        res = stream.add_events(
            name="churned",
            churn={"inactivity_days": 30, "active_events": ["o'brien_page"]},
        )

        assert "churned" in list(res.df["event"].astype(str))


class TestMatchesPatternEscaping:
    def test__pattern_with_apostrophe_no_matches(self) -> None:
        stream = make_stream(
            [
                ["user_1", "promo_view", "2020-01-01 00:00:00"],
                ["user_1", "purchase", "2020-01-01 00:01:00"],
            ]
        )

        metrics = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "o'brien->purchase"},
                }
            ]
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
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "o'brien_page->purchase"},
                }
            ]
        )

        col = metrics.columns[0]
        assert bool(metrics.loc["user_1", col])
        assert not bool(metrics.loc["user_2", col])
