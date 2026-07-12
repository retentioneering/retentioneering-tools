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
                    "metric_args": {"event": "purchase"},
                },
                {
                    "op": "=",
                    "metric": "has_event",
                    "value": True,
                    "metric_args": {"event": "promo_view"},
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
                                    "metric_args": {"event": "logout"},
                                },
                                {
                                    "op": "=",
                                    "metric": "has_event",
                                    "value": True,
                                    "metric_args": {"event": "cancellation"},
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

    def test__condition_has_event_missing_event_key_raises(self) -> None:
        """has_event/event_count now require a single 'event' key - the old
        'events' key (string or list) is no longer accepted."""
        stream = build_stream()
        condition = {
            "op": "=",
            "metric": "has_event",
            "value": True,
            "metric_args": {"events": "purchase"},
        }
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
            "metric_args": {"event": "purchse"},  # typo of "purchase"
        }
        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            _ = stream.filter_paths(condition=condition)

    def test__condition_with_typo_event_in_event_count_raises(self) -> None:
        stream = build_stream()
        condition = {
            "op": ">",
            "metric": "event_count",
            "value": 0,
            "metric_args": {"event": "purchse"},  # typo of "purchase"
        }
        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            _ = stream.filter_paths(condition=condition)

    def test__condition_no_matches_raises_empty_result_error(self) -> None:
        stream = build_stream()
        condition = {
            "op": ">",
            "metric": "event_count",
            "value": 10,
            "metric_args": {"event": "purchase"},
        }
        with pytest.raises(EmptyEventstreamError):
            _ = stream.filter_paths(condition=condition)

    def test__condition_in_numeric(self) -> None:
        stream = build_stream()
        condition = {
            "op": "in",
            "metric": "event_count",
            "value": [2],
            "metric_args": {"event": "purchase"},
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
            "metric_args": {"event": "logout"},
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
            "metric_args": {"event": "purchase"},
        }
        res = stream.filter_paths(condition=condition, path_col="session_id")

        expected = stream.filter_events(keep={"session_id": ["sess_2"]})
        assert res.equals(expected)

    def test__has_all_events_all_present(self) -> None:
        """has_all_events (AND semantics) - every user has both events"""
        stream = build_stream()

        condition = {
            "op": "=",
            "metric": "has_all_events",
            "value": True,
            "metric_args": {"events": ["promo_view", "purchase"]},
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(
            keep={"user_id": ["user_1", "user_2", "user_3"]}
        )
        assert res.equals(expected)

    def test__has_all_events_subset(self) -> None:
        """has_all_events (AND semantics) - only some users have all"""
        stream = build_stream()

        condition = {
            "op": "=",
            "metric": "has_all_events",
            "value": True,
            "metric_args": {"events": ["promo_view", "logout"]},
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_1"]})
        assert res.equals(expected)

    def test__has_all_events_false_means_not_all_present(self) -> None:
        """has_all_events == False is 'at least one of the listed events is
        absent' - no user has BOTH logout and cancellation, so all three are
        kept (this mirrors the pre-redesign has_event+list '=False' behavior,
        now expressed through the single-valued has_all_events metric)."""
        stream = build_stream()

        condition = {
            "op": "=",
            "metric": "has_all_events",
            "value": False,
            "metric_args": {"events": ["logout", "cancellation"]},
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(
            keep={"user_id": ["user_1", "user_2", "user_3"]}
        )
        assert res.equals(expected)

    def test__has_any_event_or_semantics(self) -> None:
        """has_any_event (genuine OR-of-presence) - user_1 has logout, user_3
        has cancellation, user_2 has neither."""
        stream = build_stream()

        condition = {
            "op": "=",
            "metric": "has_any_event",
            "value": True,
            "metric_args": {"events": ["logout", "cancellation"]},
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_1", "user_3"]})
        assert res.equals(expected)

    def test__has_all_events_combined_with_other_metric(self) -> None:
        """Test complex condition using has_all_events combined with event_count"""
        stream = build_stream()

        condition = {
            "op": "and",
            "args": [
                {
                    "op": ">",
                    "metric": "event_count",
                    "value": 0,
                    "metric_args": {"event": "purchase"},
                },
                {
                    "op": "=",
                    "metric": "has_all_events",
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

    def test__has_all_events_missing_events_raises(self) -> None:
        stream = build_stream()
        condition = {
            "op": "=",
            "metric": "has_all_events",
            "value": True,
            "metric_args": {},
        }
        with pytest.raises((PreprocessingConfigError, InvalidMetricConfigError)):
            _ = stream.filter_paths(condition=condition)

    def test__has_all_events_typo_raises(self) -> None:
        stream = build_stream()
        condition = {
            "op": "=",
            "metric": "has_all_events",
            "value": True,
            "metric_args": {"events": ["promo_view", "purchse"]},  # typo
        }
        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            _ = stream.filter_paths(condition=condition)

    @pytest.mark.parametrize("metric", ["has_event_bulk", "event_count_bulk"])
    def test__bulk_metric_forbidden_in_comparison_condition(self, metric: str) -> None:
        stream = build_stream()
        condition = {
            "op": "=",
            "metric": metric,
            "value": True,
            "metric_args": {"events": ["promo_view", "purchase"]},
        }
        with pytest.raises(PreprocessingConfigError):
            _ = stream.filter_paths(condition=condition)

    @pytest.mark.parametrize("metric", ["has_event_bulk", "event_count_bulk"])
    def test__bulk_metric_forbidden_in_in_condition(self, metric: str) -> None:
        stream = build_stream()
        condition = {
            "op": "in",
            "metric": metric,
            "value": [1],
            "metric_args": {"events": ["promo_view", "purchase"]},
        }
        with pytest.raises(PreprocessingConfigError):
            _ = stream.filter_paths(condition=condition)

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
                    "metric_args": {"event": "purchase"},
                },
            ],
        }

        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_2"]})
        assert res.equals(expected)

    def test__matches_pattern_does_not_match_prefix_suffix_collision(self) -> None:
        """Regression: a pattern like 'results->basket' must not match inside
        an unrelated event name that merely ends in 'results' followed by a
        real '->basket' - matching is on whole tokens, not substrings."""
        df = pd.DataFrame(
            [
                ["user_1", "user_search", "2020-01-01 00:00:00"],
                ["user_1", "basket", "2020-01-01 00:01:00"],
                ["user_2", "results", "2020-01-01 00:00:00"],
                ["user_2", "basket", "2020-01-01 00:01:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)

        condition = {
            "op": "=",
            "metric": "matches_pattern",
            "value": True,
            "metric_args": {"pattern": "results->basket"},
        }
        res = stream.filter_paths(condition=condition)

        # user_1's "user_search" ends in a substring shared with "results" but
        # is a different, whole event token - only user_2 has a real "results"
        # event immediately followed by "basket".
        expected = stream.filter_events(keep={"user_id": ["user_2"]})
        assert res.equals(expected)

    def test__matches_pattern_escapes_regex_metacharacters(self) -> None:
        """Regression: an event literally named 'item(1)' must be matched as a
        literal, not interpreted as a regex capture group; a '.' in an event
        name must not act as a wildcard over unrelated events."""
        df = pd.DataFrame(
            [
                ["user_1", "item(1)", "2020-01-01 00:00:00"],
                ["user_1", "checkout", "2020-01-01 00:01:00"],
                ["user_2", "a.c", "2020-01-01 00:00:00"],
                ["user_3", "abcX", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)

        condition = {
            "op": "=",
            "metric": "matches_pattern",
            "value": True,
            "metric_args": {"pattern": "item(1)->checkout"},
        }
        res = stream.filter_paths(condition=condition)
        expected = stream.filter_events(keep={"user_id": ["user_1"]})
        assert res.equals(expected)

    def test__matches_pattern_dot_is_not_a_wildcard_over_events(self) -> None:
        df = pd.DataFrame(
            [
                ["user_1", "a.c", "2020-01-01 00:00:00"],
                ["user_2", "abcX", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)

        condition = {
            "op": "=",
            "metric": "matches_pattern",
            "value": True,
            "metric_args": {"pattern": "a.c"},
        }
        res = stream.filter_paths(condition=condition)
        # only user_1's literal "a.c" event should match, not user_2's "abcX"
        expected = stream.filter_events(keep={"user_id": ["user_1"]})
        assert res.equals(expected)

    def test__matches_pattern_typo_raises(self) -> None:
        stream = build_stream()
        condition = {
            "op": "=",
            "metric": "matches_pattern",
            "value": True,
            "metric_args": {"pattern": "promo_view->purchse"},  # typo
        }
        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            _ = stream.filter_paths(condition=condition)

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
                    "metric_args": {"event": "purchase"},
                },
            ],
        }
        res = stream.filter_paths(condition=condition)

        expected = stream.filter_events(keep={"user_id": ["user_2"]})
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
                "metric_args": {"event": "purchase"},
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
                    "metric_args": {"event": "purchase"},
                },
            ]
        )
        assert set(res.df["user_id"].astype(str)) == {"user_1"}
