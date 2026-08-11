import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import (
    InvalidMetricConfigError,
    PreprocessingConfigError,
)


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


class TestMatchesPatternEventClasses:
    """`matches_pattern` is the degenerate case of anchoring — "did it match at
    all" — so an event class has to behave here exactly as it does for
    positions, including having its member names checked."""

    def test__alternation_matches_either_member(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "promo_view->[logout|cancellation]"},
                }
            ]
        )
        col = "matches_pattern_promo_view->[logout|cancellation]"
        # user_1 ends promo_view->purchase->logout, so the pair is not adjacent
        assert result.loc["user_1", col] == 0
        assert result.loc["user_2", col] == 0
        assert result.loc["user_3", col] == 0

    def test__alternation_after_a_gap_matches_either_member(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "promo_view->.*->[logout|cancellation]"},
                }
            ]
        )
        col = "matches_pattern_promo_view->.*->[logout|cancellation]"
        assert result.loc["user_1", col] == 1
        assert result.loc["user_2", col] == 0
        assert result.loc["user_3", col] == 1

    def test__negation_matches_anything_else(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "purchase->[^purchase]"},
                }
            ]
        )
        col = "matches_pattern_purchase->[^purchase]"
        assert result.loc["user_1", col] == 1  # purchase -> logout
        assert result.loc["user_2", col] == 0  # purchase -> purchase, then ends
        assert result.loc["user_3", col] == 1  # purchase -> cancellation

    def test__negation_does_not_match_the_end_of_a_path(self) -> None:
        """user_1's last event is `logout`; the synthetic path_end row sitting
        after it must not satisfy `[^purchase]` and turn the pattern true."""
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "logout->[^purchase]"},
                }
            ]
        )
        assert result["matches_pattern_logout->[^purchase]"].tolist() == [0, 0, 0]

    def test__equals_renaming_the_members_together(self) -> None:
        stream = build_stream()
        merged = stream.rename_events({"logout": "END", "cancellation": "END"})

        by_class = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "promo_view->.*->[logout|cancellation]"},
                }
            ]
        )
        by_rename = merged.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "promo_view->.*->END"},
                }
            ]
        )
        assert (
            by_class["matches_pattern_promo_view->.*->[logout|cancellation]"].tolist()
            == by_rename["matches_pattern_promo_view->.*->END"].tolist()
        )

    def test__typo_inside_a_class_raises(self) -> None:
        """The dangerous typo: it would widen the position to always-true
        rather than emptying the result."""
        stream = build_stream()
        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            stream.get_metrics(
                [
                    {
                        "metric": "matches_pattern",
                        "metric_args": {"pattern": "promo_view->[^purchse]"},
                    }
                ]
            )

    def test__valid_class_is_not_mistaken_for_a_missing_event(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "[promo_view|purchase]"},
                }
            ]
        )
        assert result["matches_pattern_[promo_view|purchase]"].tolist() == [1, 1, 1]

    def test__unsupported_syntax_is_reported_as_a_config_error(self) -> None:
        stream = build_stream()
        with pytest.raises(InvalidMetricConfigError, match="not a sequence"):
            stream.get_metrics(
                [
                    {
                        "metric": "matches_pattern",
                        "metric_args": {"pattern": "[^promo_view->purchase]"},
                    }
                ]
            )


class TestMatchesPatternRestrictedGaps:
    """`A->[^X]*->B` — "reached B from A without passing through X" — is the
    analytically valuable form: "bought without contacting support"."""

    def test__excludes_paths_that_passed_through_the_event(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "promo_view->[^purchase]*->logout"},
                }
            ]
        )
        col = "matches_pattern_promo_view->[^purchase]*->logout"
        # user_1 is promo_view -> purchase -> logout, so the run is not clean.
        assert result.loc["user_1", col] == 0

    def test__admits_a_clean_run(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "promo_view->[^logout]*->purchase"},
                }
            ]
        )
        col = "matches_pattern_promo_view->[^logout]*->purchase"
        assert result[col].tolist() == [1, 1, 1]

    def test__positive_gap_admits_only_the_listed_events(self) -> None:
        stream = build_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "matches_pattern",
                    "metric_args": {"pattern": "promo_view->[purchase]*->logout"},
                }
            ]
        )
        col = "matches_pattern_promo_view->[purchase]*->logout"
        assert result.loc["user_1", col] == 1  # only a purchase lies between

    def test__typo_inside_a_gap_raises(self) -> None:
        stream = build_stream()
        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            stream.get_metrics(
                [
                    {
                        "metric": "matches_pattern",
                        "metric_args": {"pattern": "promo_view->[^purchse]*->logout"},
                    }
                ]
            )

    def test__unbounded_gap_is_rejected(self) -> None:
        stream = build_stream()
        with pytest.raises(InvalidMetricConfigError, match="nothing on its outer side"):
            stream.get_metrics(
                [
                    {
                        "metric": "matches_pattern",
                        "metric_args": {"pattern": "[^purchase]*->logout"},
                    }
                ]
            )


def build_segmented_stream():
    """Two segment columns over three paths:

    - segment: user_1 -> s1 only, user_2 -> s1 then s2, user_3 -> s2 only
    - channel: user_1 -> mobile only, user_2 -> desktop then mobile,
      user_3 -> desktop only
    """
    df = pd.DataFrame(
        [
            ["user_1", "promo_view", "s1", "mobile", "2020-01-01 00:00:00"],
            ["user_1", "purchase", "s1", "mobile", "2020-01-01 00:01:00"],
            ["user_2", "promo_view", "s1", "desktop", "2020-01-01 00:00:00"],
            ["user_2", "purchase", "s2", "mobile", "2020-01-01 00:01:00"],
            ["user_3", "promo_view", "s2", "desktop", "2020-01-01 00:00:00"],
        ],
        columns=["user_id", "event", "segment", "channel", "timestamp"],
    )
    return Eventstream(
        df, {"event_cols": ["event"], "segment_cols": ["segment", "channel"]}
    )


class TestInSegmentBulk:
    def test__explicit_segment_and_levels(self) -> None:
        stream = build_segmented_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "in_segment_bulk",
                    "metric_args": {
                        "segment_name": "channel",
                        "segment_levels": ["mobile"],
                    },
                }
            ]
        )
        assert set(result.columns) == {"in_segment_bulk_channel_mobile_any"}
        assert result.loc["user_1", "in_segment_bulk_channel_mobile_any"] == 1
        assert result.loc["user_2", "in_segment_bulk_channel_mobile_any"] == 1
        assert result.loc["user_3", "in_segment_bulk_channel_mobile_any"] == 0

    def test__omitted_levels_mean_every_level_of_the_segment(self) -> None:
        stream = build_segmented_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "in_segment_bulk",
                    "metric_args": {"segment_name": "channel"},
                }
            ]
        )
        assert set(result.columns) == {
            "in_segment_bulk_channel_mobile_any",
            "in_segment_bulk_channel_desktop_any",
        }
        assert result.loc["user_2", "in_segment_bulk_channel_desktop_any"] == 1
        assert result.loc["user_1", "in_segment_bulk_channel_desktop_any"] == 0

    def test__omitted_segment_means_every_level_of_every_segment(self) -> None:
        stream = build_segmented_stream()
        result = stream.get_metrics([{"metric": "in_segment_bulk"}])
        assert set(result.columns) == {
            "in_segment_bulk_segment_s1_any",
            "in_segment_bulk_segment_s2_any",
            "in_segment_bulk_channel_mobile_any",
            "in_segment_bulk_channel_desktop_any",
        }
        assert result.loc["user_2", "in_segment_bulk_segment_s1_any"] == 1
        assert result.loc["user_2", "in_segment_bulk_segment_s2_any"] == 1
        assert result.loc["user_3", "in_segment_bulk_segment_s1_any"] == 0

    def test__all_mode_needs_the_level_to_be_the_only_one(self) -> None:
        stream = build_segmented_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "in_segment_bulk",
                    "metric_args": {"segment_name": "segment", "mode": "all"},
                }
            ]
        )
        # user_2 visits both levels, so neither is "the only one" for that path
        assert result.loc["user_1", "in_segment_bulk_segment_s1_all"] == 1
        assert result.loc["user_2", "in_segment_bulk_segment_s1_all"] == 0
        assert result.loc["user_2", "in_segment_bulk_segment_s2_all"] == 0
        assert result.loc["user_3", "in_segment_bulk_segment_s2_all"] == 1

    def test__event_share_mode_uses_the_threshold(self) -> None:
        stream = build_segmented_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "in_segment_bulk",
                    "metric_args": {
                        "segment_name": "segment",
                        "mode": "event_share",
                        "threshold": 0.75,
                    },
                }
            ]
        )
        # user_2 is 50/50 across s1/s2, below the 75% threshold on both
        assert result.loc["user_1", "in_segment_bulk_segment_s1_event_share"] == 1
        assert result.loc["user_2", "in_segment_bulk_segment_s1_event_share"] == 0
        assert result.loc["user_2", "in_segment_bulk_segment_s2_event_share"] == 0

    def test__event_share_mode_requires_threshold(self) -> None:
        stream = build_segmented_stream()
        with pytest.raises(InvalidMetricConfigError, match="threshold"):
            stream.get_metrics(
                [
                    {
                        "metric": "in_segment_bulk",
                        "metric_args": {
                            "segment_name": "segment",
                            "mode": "event_share",
                        },
                    }
                ]
            )

    def test__explicit_empty_levels_rejected(self) -> None:
        """As with event_count_bulk's 'events', [] is not a spelling of the
        wildcard - only omitting 'segment_levels' means 'all levels'."""
        stream = build_segmented_stream()
        with pytest.raises(InvalidMetricConfigError, match="empty list"):
            stream.get_metrics(
                [
                    {
                        "metric": "in_segment_bulk",
                        "metric_args": {
                            "segment_name": "segment",
                            "segment_levels": [],
                        },
                    }
                ]
            )

    def test__levels_without_segment_name_rejected(self) -> None:
        stream = build_segmented_stream()
        with pytest.raises(InvalidMetricConfigError, match="requires 'segment_name'"):
            stream.get_metrics(
                [
                    {
                        "metric": "in_segment_bulk",
                        "metric_args": {"segment_levels": ["s1"]},
                    }
                ]
            )

    def test__singular_segment_level_key_rejected(self) -> None:
        """Silently ignoring the in_segment spelling would widen the metric to
        every level instead of the one asked for."""
        stream = build_segmented_stream()
        with pytest.raises(InvalidMetricConfigError, match="segment_levels"):
            stream.get_metrics(
                [
                    {
                        "metric": "in_segment_bulk",
                        "metric_args": {
                            "segment_name": "segment",
                            "segment_level": "s1",
                        },
                    }
                ]
            )

    def test__unknown_segment_raises(self) -> None:
        stream = build_segmented_stream()
        with pytest.raises(InvalidMetricConfigError, match="chanel"):
            stream.get_metrics(
                [
                    {
                        "metric": "in_segment_bulk",
                        "metric_args": {"segment_name": "chanel"},
                    }
                ]
            )

    def test__unknown_level_raises(self) -> None:
        stream = build_segmented_stream()
        with pytest.raises(InvalidMetricConfigError, match="tablet"):
            stream.get_metrics(
                [
                    {
                        "metric": "in_segment_bulk",
                        "metric_args": {
                            "segment_name": "channel",
                            "segment_levels": ["tablet"],
                        },
                    }
                ]
            )

    def test__wildcard_segment_requires_a_segment_column(self) -> None:
        stream = build_stream()  # no segment columns at all
        with pytest.raises(InvalidMetricConfigError, match="at least one segment"):
            stream.get_metrics([{"metric": "in_segment_bulk"}])

    def test__cannot_be_used_in_a_filter_paths_condition(self) -> None:
        stream = build_segmented_stream()
        with pytest.raises(PreprocessingConfigError, match="in_segment_bulk"):
            stream.filter_paths(
                {
                    "op": "=",
                    "metric": "in_segment_bulk",
                    "metric_args": {"segment_name": "segment"},
                    "value": True,
                }
            )

    def test__paths_with_no_segment_value_are_skipped_not_crashed(self) -> None:
        """A segment column may have paths with no assigned level. NaN is not a
        level: SQL equality can never match it, and it used to be interpolated
        into the query as a bare `nan` identifier, crashing the whole build."""
        df = pd.DataFrame(
            [
                ["user_1", "promo_view", "s1", "2020-01-01 00:00:00"],
                ["user_2", "promo_view", None, "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )
        stream = Eventstream(df, {"event_cols": ["event"], "segment_cols": ["segment"]})

        bulk = stream.get_metrics([{"metric": "in_segment_bulk"}])
        assert bulk.columns.tolist() == ["in_segment_bulk_segment_s1_any"]
        assert bulk.loc["user_2", "in_segment_bulk_segment_s1_any"] == 0

        single = stream.get_metrics(
            [{"metric": "in_segment", "metric_args": {"segment_name": "segment"}}]
        )
        assert single.columns.tolist() == ["in_segment_segment_s1_any"]


class TestInSegmentUnknownArgs:
    """An unread metric_args key used to be ignored. For the key naming the
    level that is not harmless: the level then reads as "not given", which
    means "every level", so the metric silently fans out into one column per
    level instead of failing."""

    def test__pre_5_0_segment_value_key_rejected(self) -> None:
        stream = build_segmented_stream()
        with pytest.raises(InvalidMetricConfigError, match="not 'segment_value'"):
            stream.get_metrics(
                [
                    {
                        "metric": "in_segment",
                        "metric_args": {
                            "segment_name": "segment",
                            "segment_value": "s1",
                        },
                    }
                ]
            )

    def test__pre_5_0_segment_value_key_rejected_for_bulk(self) -> None:
        stream = build_segmented_stream()
        with pytest.raises(InvalidMetricConfigError, match="not 'segment_value'"):
            stream.get_metrics(
                [
                    {
                        "metric": "in_segment_bulk",
                        "metric_args": {
                            "segment_name": "segment",
                            "segment_value": "s1",
                        },
                    }
                ]
            )

    def test__plural_segment_levels_key_rejected(self) -> None:
        """Mirror of in_segment_bulk rejecting the singular spelling."""
        stream = build_segmented_stream()
        with pytest.raises(InvalidMetricConfigError, match="not 'segment_levels'"):
            stream.get_metrics(
                [
                    {
                        "metric": "in_segment",
                        "metric_args": {
                            "segment_name": "segment",
                            "segment_levels": ["s1"],
                        },
                    }
                ]
            )

    def test__misspelled_key_rejected(self) -> None:
        stream = build_segmented_stream()
        with pytest.raises(InvalidMetricConfigError, match="unknown metric_args key"):
            stream.get_metrics(
                [
                    {
                        "metric": "in_segment",
                        "metric_args": {
                            "segment_name": "segment",
                            "segment_level": "s1",
                            "Mode": "all",
                        },
                    }
                ]
            )

    def test__rejected_in_a_filter_paths_condition_too(self) -> None:
        """The condition path derives column names on its own; without the same
        check it reported the level as merely 'unknown until runtime'."""
        stream = build_segmented_stream()
        with pytest.raises(InvalidMetricConfigError, match="not 'segment_value'"):
            stream.filter_paths(
                {
                    "op": "=",
                    "metric": "in_segment",
                    "metric_args": {"segment_name": "segment", "segment_value": "s1"},
                    "value": True,
                }
            )

    def test__every_documented_key_still_accepted(self) -> None:
        stream = build_segmented_stream()
        result = stream.get_metrics(
            [
                {
                    "metric": "in_segment",
                    "metric_args": {
                        "segment_name": "segment",
                        "segment_level": "s1",
                        "mode": "event_share",
                        "threshold": 0.5,
                    },
                },
                {
                    "metric": "in_segment_bulk",
                    "metric_args": {
                        "segment_name": "channel",
                        "segment_levels": ["mobile"],
                        "mode": "event_share",
                        "threshold": 0.5,
                    },
                },
            ]
        )
        assert result.columns.tolist() == [
            "in_segment_segment_s1_event_share",
            "in_segment_bulk_channel_mobile_event_share",
        ]
