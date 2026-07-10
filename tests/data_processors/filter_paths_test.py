import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import (
    PreprocessingConfigError,
    EmptyEventstreamError,
    InvalidMetricConfigError,
)


def build_stream():
    df = pd.DataFrame(
        [
            ["user_1", "promo_view", "2020-01-01 00:00:00", "US"],
            ["user_1", "purchase", "2020-01-01 00:10:00", "US"],
            ["user_1", "logout", "2020-01-01 00:20:00", "US"],
            ["user_2", "promo_view", "2020-01-01 00:00:00", "US"],
            ["user_2", "purchase", "2020-01-01 00:05:00", "US"],
            ["user_2", "purchase", "2020-01-01 00:15:00", "US"],
            ["user_3", "promo_view", "2020-01-01 00:00:00", "UK"],
            ["user_3", "purchase", "2020-01-01 00:05:00", "UK"],
            ["user_3", "cancellation", "2020-01-01 00:07:00", "UK"],
        ],
        columns=["user_id", "event", "timestamp", "country"],
    )
    schema = {"segment_cols": ["country"]}
    return Eventstream(df, schema)


class TestFilterPathsAST:
    def test__condition_filters_expected_paths(self) -> None:
        stream = build_stream()

        condition = {
            "op": "and",
            "args": [
                {
                    "op": ">",
                    "metric": "event_count",
                    "value": 1,
                    "metric_args": {"events": "purchase"},
                },
                {
                    "op": "=",
                    "metric": "has_event",
                    "value": True,
                    "metric_args": {"events": "promo_view"},
                },
                {
                    "op": "not",
                    "args": [
                        {
                            "op": "or",
                            "args": [
                                {
                                    "op": "=",
                                    "metric": "has_event",
                                    "value": True,
                                    "metric_args": {"events": "logout"},
                                },
                                {
                                    "op": "=",
                                    "metric": "has_event",
                                    "value": True,
                                    "metric_args": {"events": "cancellation"},
                                },
                            ],
                        }
                    ],
                },
            ],
        }

        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_2"]})
        assert res.equals(expected)

    def test__condition_with_unknown_metric_raises(self) -> None:
        stream = build_stream()
        condition = {"op": "=", "metric": "unknown_metric", "value": 1}
        with pytest.raises((PreprocessingConfigError, InvalidMetricConfigError)):
            _ = stream.filter_paths(condition=condition)

    def test__condition_with_typo_event_raises_instead_of_matching_all(self) -> None:
        """A typoed event name must fail loudly, not silently build an
        all-zero has_event column that makes `== False` match every path."""
        stream = build_stream()
        condition = {
            "op": "=",
            "metric": "has_event",
            "value": False,
            "metric_args": {"events": "purchse"},  # typo of "purchase"
        }
        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            _ = stream.filter_paths(condition=condition)

    def test__condition_with_typo_event_in_event_count_raises(self) -> None:
        stream = build_stream()
        condition = {
            "op": ">",
            "metric": "event_count",
            "value": 0,
            "metric_args": {"events": "purchse"},  # typo of "purchase"
        }
        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            _ = stream.filter_paths(condition=condition)

    def test__condition_no_matches_raises_empty_result_error(self) -> None:
        stream = build_stream()
        condition = {
            "op": ">",
            "metric": "event_count",
            "value": 10,
            "metric_args": {"events": "purchase"},
        }
        with pytest.raises(EmptyEventstreamError):
            _ = stream.filter_paths(condition=condition)

    def test__condition_in_numeric(self) -> None:
        stream = build_stream()
        condition = {
            "op": "in",
            "metric": "event_count",
            "value": [2],
            "metric_args": {"events": "purchase"},
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_2"]})
        assert res.equals(expected)

    def test__condition_in_boolean_has_flag(self) -> None:
        stream = build_stream()
        condition = {
            "op": "in",
            "metric": "has_event",
            "value": [False],
            "metric_args": {"events": "logout"},
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_2", "user_3"]})
        assert res.equals(expected)

    def test__pattern_simple_adjacent(self) -> None:
        stream = build_stream()
        condition = {
            "op": "=",
            "metric": "matches_pattern",
            "value": True,
            "metric_args": {"pattern": "purchase->cancellation"},
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_3"]})
        assert res.equals(expected)

    def test__pattern_with_path_start_end_logout(self) -> None:
        stream = build_stream()
        condition = {
            "op": "=",
            "metric": "matches_pattern",
            "value": True,
            "metric_args": {"pattern": "path_start->.*->logout->path_end"},
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_1"]})
        assert res.equals(expected)

    def test__pattern_with_custom_path_col(self) -> None:
        df = pd.DataFrame(
            [
                ["user_1", "sess_1", "promo_view", "2020-01-01 00:00:00"],
                ["user_1", "sess_1", "purchase", "2020-01-01 00:10:00"],
                ["user_1", "sess_1", "logout", "2020-01-01 00:20:00"],
                ["user_2", "sess_2", "promo_view", "2020-01-01 00:00:00"],
                ["user_2", "sess_2", "purchase", "2020-01-01 00:05:00"],
                ["user_2", "sess_2", "purchase", "2020-01-01 00:15:00"],
            ],
            columns=["user_id", "session_id", "event", "timestamp"],
        )
        schema = {
            "path_cols": ["user_id", "session_id"],
            "event_cols": ["event"],
            "timestamp_col": "timestamp",
        }
        stream = Eventstream(df, schema)

        condition = {
            "op": "=",
            "metric": "matches_pattern",
            "value": True,
            "metric_args": {"pattern": "promo_view->purchase"},
        }
        res = stream.filter_paths(condition=condition, path_col="session_id")

        expected = stream.filter_events(keep={"session_id": ["sess_1", "sess_2"]})
        assert res.equals(expected)

    def test__ast_with_custom_path_col(self) -> None:
        df = pd.DataFrame(
            [
                ["user_1", "sess_1", "promo_view", "2020-01-01 00:00:00"],
                ["user_1", "sess_1", "purchase", "2020-01-01 00:10:00"],
                ["user_1", "sess_1", "logout", "2020-01-01 00:20:00"],
                ["user_2", "sess_2", "promo_view", "2020-01-01 00:00:00"],
                ["user_2", "sess_2", "purchase", "2020-01-01 00:05:00"],
                ["user_2", "sess_2", "purchase", "2020-01-01 00:15:00"],
            ],
            columns=["user_id", "session_id", "event", "timestamp"],
        )
        schema = {
            "path_cols": ["user_id", "session_id"],
            "event_cols": ["event"],
            "timestamp_col": "timestamp",
        }
        stream = Eventstream(df, schema)

        condition = {
            "op": ">",
            "metric": "event_count",
            "value": 1,
            "metric_args": {"events": "purchase"},
        }
        res = stream.filter_paths(condition=condition, path_col="session_id")

        expected = stream.filter_events(keep={"session_id": ["sess_2"]})
        assert res.equals(expected)

    def test__condition_has_with_list_of_events_all_present(self) -> None:
        """Test has metric with list of events - checking if ALL are present"""
        stream = build_stream()

        condition = {
            "op": "=",
            "metric": "has_event",
            "value": True,
            "metric_args": {"events": ["promo_view", "purchase"]},
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(
            keep={"user_id": ["user_1", "user_2", "user_3"]}
        )
        assert res.equals(expected)

    def test__condition_has_with_list_of_events_all_present_subset(self) -> None:
        """Test has metric with list of events - only some users have all"""
        stream = build_stream()

        condition = {
            "op": "=",
            "metric": "has_event",
            "value": True,
            "metric_args": {"events": ["promo_view", "logout"]},
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_1"]})
        assert res.equals(expected)

    def test__condition_has_with_list_of_events_at_least_one_absent(self) -> None:
        """Test has metric with list of events - checking if at least one is absent"""
        stream = build_stream()

        condition = {
            "op": "=",
            "metric": "has_event",
            "value": False,
            "metric_args": {"events": ["logout", "cancellation"]},
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(
            keep={"user_id": ["user_1", "user_2", "user_3"]}
        )
        assert res.equals(expected)

    def test__condition_has_with_list_combined_logic(self) -> None:
        """Test complex condition using has metric with list"""
        stream = build_stream()

        condition = {
            "op": "and",
            "args": [
                {
                    "op": ">",
                    "metric": "event_count",
                    "value": 0,
                    "metric_args": {"events": "purchase"},
                },
                {
                    "op": "=",
                    "metric": "has_event",
                    "value": True,
                    "metric_args": {"events": ["promo_view", "purchase"]},
                },
            ],
        }

        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(
            keep={"user_id": ["user_1", "user_2", "user_3"]}
        )
        assert res.equals(expected)

    def test__matches_combined_with_metrics(self) -> None:
        """Test combining matches with other metrics"""
        stream = build_stream()

        condition = {
            "op": "and",
            "args": [
                {
                    "op": "=",
                    "metric": "matches_pattern",
                    "value": True,
                    "metric_args": {"pattern": "promo_view->.*->purchase"},
                },
                {
                    "op": ">",
                    "metric": "event_count",
                    "value": 1,
                    "metric_args": {"events": "purchase"},
                },
            ],
        }

        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_2"]})
        assert res.equals(expected)

    def test__in_segment_any_mode_scalar_value(self) -> None:
        """in_segment with mode=any and a scalar segment_value keeps only matching paths"""
        stream = build_stream()
        condition = {
            "op": "=",
            "metric": "in_segment",
            "metric_args": {
                "segment_name": "country",
                "segment_value": "US",
                "mode": "any",
            },
            "value": 1,
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_1", "user_2"]})
        assert res.equals(expected)

    def test__in_segment_all_mode_scalar_value(self) -> None:
        """in_segment with mode=all keeps paths where segment_value is the only value"""
        stream = build_stream()
        condition = {
            "op": "=",
            "metric": "in_segment",
            "metric_args": {
                "segment_name": "country",
                "segment_value": "UK",
                "mode": "all",
            },
            "value": 1,
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_3"]})
        assert res.equals(expected)

    def test__in_segment_event_share_mode(self) -> None:
        """in_segment with mode=event_share keeps paths where segment_value covers >= threshold"""
        stream = build_stream()
        condition = {
            "op": "=",
            "metric": "in_segment",
            "metric_args": {
                "segment_name": "country",
                "segment_value": "US",
                "mode": "event_share",
                "threshold": 0.5,
            },
            "value": 1,
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_1", "user_2"]})
        assert res.equals(expected)

    def test__in_segment_combined_with_other_metrics(self) -> None:
        """in_segment can be combined with other metrics in AND/OR expressions"""
        stream = build_stream()
        condition = {
            "op": "and",
            "args": [
                {
                    "op": "=",
                    "metric": "in_segment",
                    "metric_args": {
                        "segment_name": "country",
                        "segment_value": "US",
                        "mode": "any",
                    },
                    "value": 1,
                },
                {
                    "op": ">",
                    "metric": "event_count",
                    "value": 1,
                    "metric_args": {"events": "purchase"},
                },
            ],
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_2"]})
        assert res.equals(expected)

    def test__event_count_no_events_arg_counts_all_events(self) -> None:
        """Omitting 'events' (or passing None/[]) means 'all events' for event_count -
        the per-path total across every event in the stream, i.e. the sum of every
        per-event column MetricBuilder builds for the wildcard."""
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_2", "A", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)

        condition = {"op": ">", "metric": "event_count", "value": 1}
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_1"]})
        assert res.equals(expected)

    def test__event_count_bare_metric_no_metric_args_key_at_all(self) -> None:
        """Same wildcard behavior when 'metric_args' is omitted entirely (not just its
        'events' key) - this is the exact shape from the reported bug repro."""
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_2", "A", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)

        res = stream.filter_paths({"metric": "event_count", "op": ">", "value": 1})

        expected = stream.filter_events(keep={"user_id": ["user_1"]})
        assert res.equals(expected)

    def test__event_count_with_list_of_events_sums_counts(self) -> None:
        """event_count with an explicit list of events sums the per-event counts
        instead of silently comparing only the first event's column."""
        stream = build_stream()

        condition = {
            "op": ">",
            "metric": "event_count",
            "value": 1,
            "metric_args": {"events": ["purchase", "cancellation"]},
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_2", "user_3"]})
        assert res.equals(expected)

    def test__has_event_no_events_arg_true_requires_all_events_present(self) -> None:
        """Omitting 'events' for has_event means 'all events' - like the explicit-list
        case, '= True' requires every event in the stream to be present on the path."""
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_2", "A", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)

        condition = {"op": "=", "metric": "has_event", "value": True}
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_1"]})
        assert res.equals(expected)

    def test__active_days_no_active_events_arg_counts_all_days(self) -> None:
        """Omitting 'active_events' for active_days means 'all events' contribute to
        the day count - this already worked (active_days is always a single aggregate
        column), but is covered here alongside its event_count/has_event siblings."""
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-02 00:00:00"],
                ["user_2", "A", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)

        condition = {"op": ">", "metric": "active_days", "value": 1}
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_1"]})
        assert res.equals(expected)

    def test__in_segment_none_segment_value_raises(self) -> None:
        """in_segment with segment_value=None cannot be used in condition"""
        stream = build_stream()
        condition = {
            "op": "=",
            "metric": "in_segment",
            "metric_args": {
                "segment_name": "country",
                "segment_value": None,
                "mode": "any",
            },
            "value": 1,
        }
        with pytest.raises(PreprocessingConfigError):
            _ = stream.filter_paths(condition=condition)


class TestFilterPathsConditionSugar:
    def _stream(self):
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_1", "purchase", "2020-01-01 00:02:00"],
                ["user_2", "A", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        return Eventstream(df)

    def test__double_equals_alias(self):
        stream = self._stream()
        res = stream.filter_paths(
            {
                "op": "==",
                "metric": "has_event",
                "value": True,
                "metric_args": {"events": "purchase"},
            }
        )
        assert set(res.df["user_id"].astype(str)) == {"user_1"}

    def test__top_level_list_means_and(self):
        stream = self._stream()
        res = stream.filter_paths(
            [
                {"op": ">", "metric": "length", "value": 1},
                {
                    "op": "=",
                    "metric": "has_event",
                    "value": True,
                    "metric_args": {"events": "purchase"},
                },
            ]
        )
        assert set(res.df["user_id"].astype(str)) == {"user_1"}
