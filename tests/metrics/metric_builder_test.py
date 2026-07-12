import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidMetricConfigError


def build_stream():
    df = pd.DataFrame(
        [
            ["user_1", "promo_view", "2020-01-01 00:00:00"],
            ["user_1", "purchase", "2020-01-01 00:10:00"],
            ["user_1", "logout", "2020-01-01 00:20:00"],
            ["user_2", "promo_view", "2020-01-01 00:00:00"],
            ["user_2", "purchase", "2020-01-01 00:05:00"],
            ["user_2", "purchase", "2020-01-01 00:15:00"],
            ["user_3", "promo_view", "2020-01-01 00:00:00"],
            ["user_3", "purchase", "2020-01-01 00:05:00"],
            ["user_3", "cancellation", "2020-01-01 00:07:00"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    return Eventstream(df)


class TestHasEventEventCountSingle:
    def test__has_event_requires_event_key(self) -> None:
        stream = build_stream()
        with pytest.raises(InvalidMetricConfigError):
            stream.get_metrics([{"metric": "has_event", "metric_args": {}}])

    def test__event_count_requires_event_key(self) -> None:
        stream = build_stream()
        with pytest.raises(InvalidMetricConfigError):
            stream.get_metrics([{"metric": "event_count", "metric_args": {}}])

    def test__has_event_rejects_list(self) -> None:
        """The old 'events' key (even as a list) is no longer accepted."""
        stream = build_stream()
        with pytest.raises(InvalidMetricConfigError):
            stream.get_metrics(
                [
                    {
                        "metric": "has_event",
                        "metric_args": {"events": ["purchase", "logout"]},
                    }
                ]
            )

    def test__has_event_typo_raises(self) -> None:
        stream = build_stream()
        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            stream.get_metrics(
                [{"metric": "has_event", "metric_args": {"event": "purchse"}}]
            )

    def test__event_count_single_event(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [{"metric": "event_count", "metric_args": {"event": "purchase"}}]
        )
        assert result.loc["user_1", "event_count_purchase"] == 1
        assert result.loc["user_2", "event_count_purchase"] == 2
        assert result.loc["user_3", "event_count_purchase"] == 1

    def test__has_event_single_event(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [{"metric": "has_event", "metric_args": {"event": "logout"}}]
        )
        assert result.loc["user_1", "has_event_logout"] == 1
        assert result.loc["user_2", "has_event_logout"] == 0
        assert result.loc["user_3", "has_event_logout"] == 0


class TestBulkMetrics:
    def test__has_event_bulk_explicit_list(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "has_event_bulk",
                    "metric_args": {"events": ["logout", "cancellation"]},
                }
            ]
        )
        assert set(result.columns) == {
            "has_event_bulk_logout",
            "has_event_bulk_cancellation",
        }
        assert result.loc["user_1", "has_event_bulk_logout"] == 1
        assert result.loc["user_1", "has_event_bulk_cancellation"] == 0
        assert result.loc["user_3", "has_event_bulk_cancellation"] == 1

    def test__event_count_bulk_explicit_list(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "event_count_bulk",
                    "metric_args": {"events": ["promo_view", "purchase"]},
                }
            ]
        )
        assert result.loc["user_2", "event_count_bulk_purchase"] == 2
        assert result.loc["user_2", "event_count_bulk_promo_view"] == 1

    def test__bulk_wildcard_omitted_events_means_all(self) -> None:
        stream = build_stream()
        result = stream.get_metrics([{"metric": "event_count_bulk"}])
        expected_cols = {
            f"event_count_bulk_{e}"
            for e in ["promo_view", "purchase", "logout", "cancellation"]
        }
        assert expected_cols == set(result.columns)

    def test__bulk_explicit_empty_list_rejected(self) -> None:
        """An explicit [] is not a valid spelling of the wildcard - only
        omitting 'events' (or passing None) means 'all events'."""
        stream = build_stream()
        with pytest.raises(InvalidMetricConfigError):
            stream.get_metrics(
                [{"metric": "has_event_bulk", "metric_args": {"events": []}}]
            )
        with pytest.raises(InvalidMetricConfigError):
            stream.get_metrics(
                [{"metric": "event_count_bulk", "metric_args": {"events": []}}]
            )

    def test__bulk_typo_raises(self) -> None:
        stream = build_stream()
        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            stream.get_metrics(
                [
                    {
                        "metric": "has_event_bulk",
                        "metric_args": {"events": ["purchse", "logout"]},
                    }
                ]
            )


class TestHasAllAndAnyEvents:
    def test__has_all_events_truth_table(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "has_all_events",
                    "metric_args": {"events": ["logout", "cancellation"]},
                }
            ]
        )
        col = "has_all_events_logout_and_cancellation"
        assert result.loc["user_1", col] == 0  # has logout only
        assert result.loc["user_2", col] == 0  # has neither
        assert result.loc["user_3", col] == 0  # has cancellation only

    def test__has_all_events_true_when_all_present(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "has_all_events",
                    "metric_args": {"events": ["promo_view", "purchase"]},
                }
            ]
        )
        col = "has_all_events_promo_view_and_purchase"
        assert result.loc["user_1", col] == 1
        assert result.loc["user_2", col] == 1
        assert result.loc["user_3", col] == 1

    def test__has_any_event_or_semantics(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "has_any_event",
                    "metric_args": {"events": ["logout", "cancellation"]},
                }
            ]
        )
        col = "has_any_event_logout_or_cancellation"
        assert result.loc["user_1", col] == 1  # has logout
        assert result.loc["user_2", col] == 0  # has neither
        assert result.loc["user_3", col] == 1  # has cancellation

    def test__has_all_events_requires_nonempty_events(self) -> None:
        stream = build_stream()
        with pytest.raises(InvalidMetricConfigError):
            stream.get_metrics([{"metric": "has_all_events", "metric_args": {}}])
        with pytest.raises(InvalidMetricConfigError):
            stream.get_metrics(
                [{"metric": "has_all_events", "metric_args": {"events": []}}]
            )

    def test__has_any_event_typo_raises(self) -> None:
        stream = build_stream()
        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            stream.get_metrics(
                [
                    {
                        "metric": "has_any_event",
                        "metric_args": {"events": ["purchse", "logout"]},
                    }
                ]
            )


class TestMatchesPatternTokenMatching:
    def test__no_substring_collision_on_prefix(self) -> None:
        """A pattern like 'search' (a single event token) must not match as a
        substring inside an unrelated, longer event name like
        'view_search_results'."""
        df = pd.DataFrame(
            [
                ["user_1", "view_search_results", "2020-01-01 00:00:00"],
                ["user_2", "search", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)
        result = stream.get_metrics(
            [{"metric": "matches_pattern", "metric_args": {"pattern": "search"}}]
        )
        col = "matches_pattern_search"
        assert result.loc["user_1", col] == 0
        assert result.loc["user_2", col] == 1

    def test__no_substring_collision_on_suffix_adjacency(self) -> None:
        """A two-token pattern 'results->basket' must not match when the
        preceding event merely ends in 'results' as part of a longer name
        (e.g. 'view_search_results') immediately followed by 'basket'."""
        df = pd.DataFrame(
            [
                ["user_1", "view_search_results", "2020-01-01 00:00:00"],
                ["user_1", "basket", "2020-01-01 00:01:00"],
                ["user_2", "results", "2020-01-01 00:00:00"],
                ["user_2", "basket", "2020-01-01 00:01:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)
        result = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "results->basket"},
                }
            ]
        )
        col = "matches_pattern_results->basket"
        assert result.loc["user_1", col] == 0
        assert result.loc["user_2", col] == 1

    def test__regex_metachar_event_name_matched_literally(self) -> None:
        df = pd.DataFrame(
            [
                ["user_1", "item(1)", "2020-01-01 00:00:00"],
                ["user_1", "checkout", "2020-01-01 00:01:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)
        result = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "item(1)->checkout"},
                }
            ]
        )
        assert result.loc["user_1", "matches_pattern_item(1)->checkout"] == 1

    def test__dot_in_event_name_is_not_a_wildcard(self) -> None:
        df = pd.DataFrame(
            [
                ["user_1", "a.c", "2020-01-01 00:00:00"],
                ["user_2", "abcX", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)
        result = stream.get_metrics(
            [{"metric": "matches_pattern", "metric_args": {"pattern": "a.c"}}]
        )
        col = "matches_pattern_a.c"
        assert result.loc["user_1", col] == 1
        assert result.loc["user_2", col] == 0

    def test__gap_wildcard_still_matches_through_and_when_adjacent(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "promo_view->.*->purchase"},
                }
            ]
        )
        col = "matches_pattern_promo_view->.*->purchase"
        # all three users have promo_view eventually followed by purchase
        assert result.loc["user_1", col] == 1
        assert result.loc["user_2", col] == 1
        assert result.loc["user_3", col] == 1

    def test__matches_pattern_typo_raises(self) -> None:
        stream = build_stream()
        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            stream.get_metrics(
                [
                    {
                        "metric": "matches_pattern",
                        "metric_args": {"pattern": "promo_view->purchse"},
                    }
                ]
            )

    def test__matches_pattern_wildcard_token_not_validated_as_event(self) -> None:
        """'.*' itself must not be checked against available_events."""
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "path_start->.*->purchase"},
                }
            ]
        )
        assert result is not None
