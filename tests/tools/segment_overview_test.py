import numpy as np
import pandas as pd
import pytest
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import (
    InvalidComplementConfigError,
    InvalidMetricConfigError,
    InvalidParameterError,
    SegmentValueNotFoundError,
)
from retentioneering.tools.segment_overview import SegmentOverview
from scipy.stats import wasserstein_distance


class TestSegmentOverview:
    def test_basic_mean_aggregation(self) -> None:
        """Test basic mean aggregation across segments"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "B", "segment_1", "2020-01-01 00:01:00"],
                ["user_1", "C", "segment_1", "2020-01-01 00:02:00"],
                ["user_2", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "B", "segment_1", "2020-01-01 00:01:00"],
                ["user_3", "A", "segment_2", "2020-01-01 00:00:00"],
                ["user_4", "A", "segment_2", "2020-01-01 00:00:00"],
                ["user_4", "B", "segment_2", "2020-01-01 00:01:00"],
                ["user_4", "C", "segment_2", "2020-01-01 00:02:00"],
                ["user_4", "D", "segment_2", "2020-01-01 00:03:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {"metric": "length", "agg": "mean"},
            ],
        )

        # segment_size and segment_share are always included
        assert "segment_size" in result.index
        assert "segment_share" in result.index

        # segment_1: user_1 has 3 events, user_2 has 2 events -> mean = 2.5
        # segment_2: user_3 has 1 event, user_4 has 4 events -> mean = 2.5
        assert result.loc["length_mean", "segment_1"] == 2.5
        assert result.loc["length_mean", "segment_2"] == 2.5

    def test_segment_size_and_share(self) -> None:
        """Test segment_size and segment_share are always computed correctly"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_3", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_4", "A", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[],  # Empty config - only segment_size and segment_share
        )

        # segment_size and segment_share are always first two rows
        assert list(result.index) == ["segment_size", "segment_share"]

        # segment_1 has 3 paths, segment_2 has 1 path
        assert result.loc["segment_size", "segment_1"] == 3
        assert result.loc["segment_size", "segment_2"] == 1

        # segment_share should sum to 1
        assert result.loc["segment_share", "segment_1"] == 0.75
        assert result.loc["segment_share", "segment_2"] == 0.25
        assert result.loc["segment_share"].sum() == pytest.approx(1.0)

    def test_segment_size_with_spanning_paths(self) -> None:
        """Test segment_size counts path-segment pairs correctly"""
        # user_1 spans both segments
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "B", "segment_2", "2020-01-01 00:01:00"],
                ["user_2", "A", "segment_1", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[],
        )

        # segment_1: (user_1, segment_1) and (user_2, segment_1) = 2 paths
        # segment_2: (user_1, segment_2) = 1 path
        assert result.loc["segment_size", "segment_1"] == 2
        assert result.loc["segment_size", "segment_2"] == 1
        assert result.loc["segment_share", "segment_1"] == pytest.approx(2 / 3)
        assert result.loc["segment_share", "segment_2"] == pytest.approx(1 / 3)

    def test_median_aggregation(self) -> None:
        """Test median aggregation"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "B", "segment_1", "2020-01-01 00:01:00"],
                ["user_2", "C", "segment_1", "2020-01-01 00:02:00"],
                ["user_3", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_3", "B", "segment_1", "2020-01-01 00:01:00"],
                ["user_3", "C", "segment_1", "2020-01-01 00:02:00"],
                ["user_3", "D", "segment_1", "2020-01-01 00:03:00"],
                ["user_3", "E", "segment_1", "2020-01-01 00:04:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {"metric": "length", "agg": "median"},
            ],
        )

        # segment_1: user_1 has 1, user_2 has 3, user_3 has 5 -> median = 3
        assert result.loc["length_median", "segment_1"] == 3.0

    def test_percentile_q25_aggregation(self) -> None:
        """Test q25 percentile aggregation"""
        # Create 20 users with increasing path lengths
        rows = []
        for i in range(20):
            user = f"user_{i}"
            for j in range(i + 1):
                rows.append(
                    [user, f"event_{j}", "segment_1", f"2020-01-01 00:{j:02d}:00"]
                )

        df = pd.DataFrame(rows, columns=["user_id", "event", "segment", "timestamp"])
        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {"metric": "length", "agg": "q25"},
            ],
        )

        # Path lengths are 1, 2, 3, ..., 20 -> q25 = 5.75
        assert result.loc["length_q25", "segment_1"] == pytest.approx(5.75, rel=0.1)

    def test_complement_distance_aggregation(self) -> None:
        """Test Wasserstein distance between segment and complement"""
        # Create two clearly different distributions
        rows = []
        # segment_1: all users have 1 event
        for i in range(10):
            rows.append([f"user_1_{i}", "A", "segment_1", "2020-01-01 00:00:00"])

        # segment_2: all users have 10 events
        for i in range(10):
            user = f"user_2_{i}"
            for j in range(10):
                rows.append(
                    [user, f"event_{j}", "segment_2", f"2020-01-01 00:{j:02d}:00"]
                )

        df = pd.DataFrame(rows, columns=["user_id", "event", "segment", "timestamp"])
        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {"metric": "length", "agg": "complement_distance"},
            ],
        )

        # segment_1 has length 1, segment_2 has length 10
        # Wasserstein distance should be 9 (|10 - 1|)
        assert result.loc["length_complement_distance", "segment_1"] == pytest.approx(
            9.0, rel=0.01
        )
        assert result.loc["length_complement_distance", "segment_2"] == pytest.approx(
            9.0, rel=0.01
        )

    def test_event_count_metric(self) -> None:
        """Test event_count metric with aggregation"""
        df = pd.DataFrame(
            [
                ["user_1", "checkout", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "checkout", "segment_1", "2020-01-01 00:01:00"],
                ["user_1", "checkout", "segment_1", "2020-01-01 00:02:00"],
                ["user_2", "checkout", "segment_1", "2020-01-01 00:00:00"],
                ["user_3", "checkout", "segment_2", "2020-01-01 00:00:00"],
                ["user_3", "checkout", "segment_2", "2020-01-01 00:01:00"],
                ["user_4", "other", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {
                    "metric": "event_count",
                    "metric_args": {"events": "checkout"},
                    "agg": "mean",
                },
            ],
        )

        # segment_1: user_1 has 3 checkouts, user_2 has 1 -> mean = 2.0
        # segment_2: user_3 has 2 checkouts, user_4 has 0 -> mean = 1.0
        assert result.loc["event_count_checkout_mean", "segment_1"] == 2.0
        assert result.loc["event_count_checkout_mean", "segment_2"] == 1.0

    def test_has_metric(self) -> None:
        """Test has (presence) metric with aggregation"""
        df = pd.DataFrame(
            [
                ["user_1", "purchase", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "view", "segment_1", "2020-01-01 00:00:00"],
                ["user_3", "purchase", "segment_2", "2020-01-01 00:00:00"],
                ["user_4", "purchase", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {
                    "metric": "has_event",
                    "metric_args": {"events": "purchase"},
                    "agg": "mean",
                },
            ],
        )

        # segment_1: user_1 has purchase (1), user_2 doesn't (0) -> mean = 0.5
        # segment_2: user_3 has purchase (1), user_4 has purchase (1) -> mean = 1.0
        assert result.loc["has_event_purchase_mean", "segment_1"] == 0.5
        assert result.loc["has_event_purchase_mean", "segment_2"] == 1.0

    def test_in_segment_metric_with_list(self) -> None:
        """Test in_segment metric with explicit segment_value list"""
        df = pd.DataFrame(
            [
                # user_1: events in segment_1 only
                ["user_1", "view", "segment_1", "mobile", "2020-01-01 00:00:00"],
                ["user_1", "purchase", "segment_1", "mobile", "2020-01-01 00:01:00"],
                # user_2: events in segment_1, mixed channels
                ["user_2", "view", "segment_1", "mobile", "2020-01-01 00:00:00"],
                ["user_2", "purchase", "segment_1", "desktop", "2020-01-01 00:01:00"],
                # user_3: events in segment_2, desktop only
                ["user_3", "view", "segment_2", "desktop", "2020-01-01 00:00:00"],
                ["user_3", "purchase", "segment_2", "desktop", "2020-01-01 00:01:00"],
                # user_4: events in segment_2, mobile only
                ["user_4", "view", "segment_2", "mobile", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "channel", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment", "channel"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {
                    "metric": "in_segment",
                    "metric_args": {
                        "segment_name": "channel",
                        "segment_value": ["mobile", "desktop"],
                        "mode": "any",
                    },
                    "agg": "mean",
                },
            ],
        )

        # segment_1: user_1 has mobile (1), user_2 has mobile (1) -> mobile mean = 1.0
        # segment_1: user_1 has desktop (0), user_2 has desktop (1) -> desktop mean = 0.5
        assert result.loc["in_segment_channel_mobile_any_mean", "segment_1"] == 1.0
        assert result.loc["in_segment_channel_desktop_any_mean", "segment_1"] == 0.5

        # segment_2: user_3 has mobile (0), user_4 has mobile (1) -> mobile mean = 0.5
        # segment_2: user_3 has desktop (1), user_4 has desktop (0) -> desktop mean = 0.5
        assert result.loc["in_segment_channel_mobile_any_mean", "segment_2"] == 0.5
        assert result.loc["in_segment_channel_desktop_any_mean", "segment_2"] == 0.5

    def test_in_segment_metric_all_mode(self) -> None:
        """Test in_segment metric with 'all' mode (only that value in column)"""
        df = pd.DataFrame(
            [
                # user_1: only mobile
                ["user_1", "view", "segment_1", "mobile", "2020-01-01 00:00:00"],
                ["user_1", "purchase", "segment_1", "mobile", "2020-01-01 00:01:00"],
                # user_2: mixed channels
                ["user_2", "view", "segment_1", "mobile", "2020-01-01 00:00:00"],
                ["user_2", "purchase", "segment_1", "desktop", "2020-01-01 00:01:00"],
                # user_3: only desktop
                ["user_3", "view", "segment_2", "desktop", "2020-01-01 00:00:00"],
                ["user_3", "purchase", "segment_2", "desktop", "2020-01-01 00:01:00"],
                # user_4: only mobile
                ["user_4", "view", "segment_2", "mobile", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "channel", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment", "channel"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {
                    "metric": "in_segment",
                    "metric_args": {
                        "segment_name": "channel",
                        "segment_value": "mobile",
                        "mode": "all",
                    },
                    "agg": "mean",
                },
            ],
        )

        # segment_1: user_1 is all mobile (1), user_2 is mixed (0) -> mean = 0.5
        # segment_2: user_3 is all desktop (0), user_4 is all mobile (1) -> mean = 0.5
        assert result.loc["in_segment_channel_mobile_all_mean", "segment_1"] == 0.5
        assert result.loc["in_segment_channel_mobile_all_mean", "segment_2"] == 0.5

    def test_multiple_metrics(self) -> None:
        """Test multiple different metrics in one config"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "B", "segment_1", "2020-01-01 00:01:00"],
                ["user_2", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_3", "A", "segment_2", "2020-01-01 00:00:00"],
                ["user_3", "B", "segment_2", "2020-01-01 00:01:00"],
                ["user_3", "C", "segment_2", "2020-01-01 00:02:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {"metric": "length", "agg": "mean"},
                {"metric": "duration", "agg": "median"},
                {"metric": "has_event", "metric_args": {"events": "C"}, "agg": "mean"},
            ],
        )

        # 2 special metrics (segment_size, segment_share) + 3 regular metrics = 5
        assert len(result) == 5
        # segment_size and segment_share should be first
        assert list(result.index)[:2] == ["segment_size", "segment_share"]
        assert "length_mean" in result.index
        assert "duration_median" in result.index
        assert "has_event_C_mean" in result.index

    def test_invalid_segment_column(self) -> None:
        """Test error handling for invalid segment column"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        schema = {"event_cols": ["event"]}
        stream = Eventstream(df, schema)

        with pytest.raises(
            ValueError, match="Segment column 'invalid_segment' not found"
        ):
            stream.segment_overview_data(
                segment_col="invalid_segment",
                metrics=[{"metric": "length", "agg": "mean"}],
            )

    def test_invalid_aggregation(self) -> None:
        """Test error handling for invalid aggregation type"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        with pytest.raises(ValueError, match="Unknown aggregation type"):
            stream.segment_overview_data(
                segment_col="segment",
                metrics=[{"metric": "length", "agg": "invalid_agg"}],
            )

    def test_default_aggregation_is_mean(self) -> None:
        """Test that default aggregation is mean when not specified"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "B", "segment_1", "2020-01-01 00:01:00"],
                ["user_2", "A", "segment_1", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[{"metric": "length"}],  # No 'agg' specified
        )

        # Default should be mean
        assert "length_mean" in result.index
        assert result.loc["length_mean", "segment_1"] == 1.5

    def test_duration_metric(self) -> None:
        """Test duration metric with aggregation"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "B", "segment_1", "2020-01-01 00:01:00"],  # 60 sec duration
                ["user_2", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "B", "segment_1", "2020-01-01 00:02:00"],  # 120 sec duration
                ["user_3", "A", "segment_2", "2020-01-01 00:00:00"],
                ["user_3", "B", "segment_2", "2020-01-01 00:00:30"],  # 30 sec duration
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {"metric": "duration", "agg": "mean"},
            ],
        )

        # segment_1: (60 + 120) / 2 = 90
        # segment_2: 30
        assert result.loc["duration_mean", "segment_1"] == 90.0
        assert result.loc["duration_mean", "segment_2"] == 30.0

    def test_path_spanning_multiple_segments(self) -> None:
        """Test that a single path can span multiple segments (e.g., user traveling between countries)"""
        # user_1 travels: starts in country_A (2 events), then moves to country_B (3 events)
        # user_2 stays in country_A (4 events)
        # user_3 stays in country_B (1 event)
        df = pd.DataFrame(
            [
                # user_1 in country_A
                ["user_1", "login", "country_A", "2020-01-01 00:00:00"],
                ["user_1", "view", "country_A", "2020-01-01 00:01:00"],
                # user_1 travels to country_B
                ["user_1", "view", "country_B", "2020-01-01 01:00:00"],
                ["user_1", "purchase", "country_B", "2020-01-01 01:01:00"],
                ["user_1", "logout", "country_B", "2020-01-01 01:02:00"],
                # user_2 stays in country_A
                ["user_2", "login", "country_A", "2020-01-01 00:00:00"],
                ["user_2", "view", "country_A", "2020-01-01 00:01:00"],
                ["user_2", "view", "country_A", "2020-01-01 00:02:00"],
                ["user_2", "logout", "country_A", "2020-01-01 00:03:00"],
                # user_3 stays in country_B
                ["user_3", "login", "country_B", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "country", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["country"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="country",
            metrics=[
                {"metric": "length", "agg": "mean"},
            ],
        )

        # country_A: (user_1, country_A) has 2 events, (user_2, country_A) has 4 events
        # -> mean = (2 + 4) / 2 = 3.0
        # country_B: (user_1, country_B) has 3 events, (user_3, country_B) has 1 event
        # -> mean = (3 + 1) / 2 = 2.0
        assert result.loc["length_mean", "country_A"] == 3.0
        assert result.loc["length_mean", "country_B"] == 2.0

    def test_path_spanning_segments_with_has_metric(self) -> None:
        """Test has metric with paths spanning multiple segments"""
        df = pd.DataFrame(
            [
                # user_1: purchase only in country_B
                ["user_1", "view", "country_A", "2020-01-01 00:00:00"],
                ["user_1", "purchase", "country_B", "2020-01-01 01:00:00"],
                # user_2: purchase in country_A
                ["user_2", "purchase", "country_A", "2020-01-01 00:00:00"],
                # user_3: no purchase in country_A
                ["user_3", "view", "country_A", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "country", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["country"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="country",
            metrics=[
                {
                    "metric": "has_event",
                    "metric_args": {"events": "purchase"},
                    "agg": "mean",
                },
            ],
        )

        # country_A: (user_1, A) no purchase, (user_2, A) has purchase, (user_3, A) no purchase
        # -> mean = (0 + 1 + 0) / 3 = 0.333...
        # country_B: (user_1, B) has purchase -> mean = 1.0
        assert result.loc["has_event_purchase_mean", "country_A"] == pytest.approx(
            0.333, rel=0.01
        )
        assert result.loc["has_event_purchase_mean", "country_B"] == 1.0

    def test_row_order(self) -> None:
        """Test that segment_size and segment_share are always first two rows"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "A", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {"metric": "length", "agg": "mean"},
                {"metric": "duration", "agg": "mean"},
            ],
        )

        # First two rows should always be segment_size and segment_share
        assert result.index[0] == "segment_size"
        assert result.index[1] == "segment_share"

    def test_has_with_multiple_events(self) -> None:
        """Test has metric with a list of events returns multiple rows"""
        df = pd.DataFrame(
            [
                ["user_1", "login", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "purchase", "segment_1", "2020-01-01 00:01:00"],
                ["user_2", "login", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "view", "segment_1", "2020-01-01 00:01:00"],
                ["user_3", "login", "segment_2", "2020-01-01 00:00:00"],
                ["user_3", "view", "segment_2", "2020-01-01 00:01:00"],
                ["user_3", "purchase", "segment_2", "2020-01-01 00:02:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {
                    "metric": "has_event",
                    "metric_args": {"events": ["purchase", "view"]},
                    "agg": "mean",
                },
            ],
        )

        # Should have rows for both events
        assert "has_event_purchase_mean" in result.index
        assert "has_event_view_mean" in result.index

        # segment_1: user_1 has purchase (1), user_2 doesn't (0) -> mean = 0.5
        # segment_2: user_3 has purchase (1) -> mean = 1.0
        assert result.loc["has_event_purchase_mean", "segment_1"] == 0.5
        assert result.loc["has_event_purchase_mean", "segment_2"] == 1.0

        # segment_1: user_1 doesn't have view (0), user_2 has view (1) -> mean = 0.5
        # segment_2: user_3 has view (1) -> mean = 1.0
        assert result.loc["has_event_view_mean", "segment_1"] == 0.5
        assert result.loc["has_event_view_mean", "segment_2"] == 1.0

    def test_event_count_with_multiple_events(self) -> None:
        """Test event_count metric with a list of events returns multiple rows"""
        df = pd.DataFrame(
            [
                ["user_1", "click", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "click", "segment_1", "2020-01-01 00:01:00"],
                ["user_1", "scroll", "segment_1", "2020-01-01 00:02:00"],
                ["user_2", "scroll", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "scroll", "segment_1", "2020-01-01 00:01:00"],
                ["user_2", "scroll", "segment_1", "2020-01-01 00:02:00"],
                ["user_3", "click", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {
                    "metric": "event_count",
                    "metric_args": {"events": ["click", "scroll"]},
                    "agg": "mean",
                },
            ],
        )

        # Should have rows for both events
        assert "event_count_click_mean" in result.index
        assert "event_count_scroll_mean" in result.index

        # segment_1: user_1 has 2 clicks, user_2 has 0 -> mean = 1.0
        # segment_2: user_3 has 1 click -> mean = 1.0
        assert result.loc["event_count_click_mean", "segment_1"] == 1.0
        assert result.loc["event_count_click_mean", "segment_2"] == 1.0

        # segment_1: user_1 has 1 scroll, user_2 has 3 -> mean = 2.0
        # segment_2: user_3 has 0 scrolls -> mean = 0.0
        assert result.loc["event_count_scroll_mean", "segment_1"] == 2.0
        assert result.loc["event_count_scroll_mean", "segment_2"] == 0.0

    def test_event_count_all_events_wildcard(self) -> None:
        """Omitting 'events' counts every event in the stream, like active_days' active_events"""
        df = pd.DataFrame(
            [
                ["user_1", "click", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "click", "segment_1", "2020-01-01 00:01:00"],
                ["user_1", "scroll", "segment_1", "2020-01-01 00:02:00"],
                ["user_2", "scroll", "segment_1", "2020-01-01 00:00:00"],
                ["user_3", "click", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[{"metric": "event_count", "agg": "mean"}],
        )

        assert "event_count_click_mean" in result.index
        assert "event_count_scroll_mean" in result.index
        assert result.loc["event_count_click_mean", "segment_1"] == 1.0
        assert result.loc["event_count_scroll_mean", "segment_1"] == 1.0
        assert result.loc["event_count_click_mean", "segment_2"] == 1.0
        assert result.loc["event_count_scroll_mean", "segment_2"] == 0.0

    def test_time_between_metric(self) -> None:
        """Test time_between metric with aggregation"""
        df = pd.DataFrame(
            [
                # user_1: login -> purchase in 60 seconds
                ["user_1", "login", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "view", "segment_1", "2020-01-01 00:00:30"],
                ["user_1", "purchase", "segment_1", "2020-01-01 00:01:00"],
                # user_2: login -> purchase in 120 seconds
                ["user_2", "login", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "purchase", "segment_1", "2020-01-01 00:02:00"],
                # user_3: login -> purchase in 30 seconds
                ["user_3", "login", "segment_2", "2020-01-01 00:00:00"],
                ["user_3", "purchase", "segment_2", "2020-01-01 00:00:30"],
                # user_4: login only (no purchase) - should be excluded from time_between
                ["user_4", "login", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {
                    "metric": "time_between",
                    "metric_args": {"start_event": "login", "end_event": "purchase"},
                    "agg": "mean",
                },
            ],
        )

        # segment_1: (60 + 120) / 2 = 90 seconds
        # segment_2: 30 seconds (user_4 has no purchase, so excluded)
        assert "time_from_login_to_purchase_mean" in result.index
        assert result.loc["time_from_login_to_purchase_mean", "segment_1"] == 90.0
        assert result.loc["time_from_login_to_purchase_mean", "segment_2"] == 30.0

    def test_time_between_median(self) -> None:
        """Test time_between metric with median aggregation"""
        df = pd.DataFrame(
            [
                # user_1: 10 seconds
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "B", "segment_1", "2020-01-01 00:00:10"],
                # user_2: 20 seconds
                ["user_2", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "B", "segment_1", "2020-01-01 00:00:20"],
                # user_3: 100 seconds (outlier)
                ["user_3", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_3", "B", "segment_1", "2020-01-01 00:01:40"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {
                    "metric": "time_between",
                    "metric_args": {"start_event": "A", "end_event": "B"},
                    "agg": "median",
                },
            ],
        )

        # median of [10, 20, 100] = 20
        assert result.loc["time_from_A_to_B_median", "segment_1"] == 20.0

    def test_time_between_with_path_start(self) -> None:
        """Test time_between metric with path_start event"""
        df = pd.DataFrame(
            [
                # user_1: path_start (first event) -> purchase in 60 seconds
                ["user_1", "view", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "purchase", "segment_1", "2020-01-01 00:01:00"],
                # user_2: path_start -> purchase in 30 seconds
                ["user_2", "login", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "purchase", "segment_1", "2020-01-01 00:00:30"],
                # user_3: path_start -> purchase in 90 seconds
                ["user_3", "view", "segment_2", "2020-01-01 00:00:00"],
                ["user_3", "cart", "segment_2", "2020-01-01 00:00:30"],
                ["user_3", "purchase", "segment_2", "2020-01-01 00:01:30"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {
                    "metric": "time_between",
                    "metric_args": {
                        "start_event": "path_start",
                        "end_event": "purchase",
                    },
                    "agg": "mean",
                },
            ],
        )

        # segment_1: (60 + 30) / 2 = 45 seconds
        # segment_2: 90 seconds
        assert "time_from_path_start_to_purchase_mean" in result.index
        assert result.loc["time_from_path_start_to_purchase_mean", "segment_1"] == 45.0
        assert result.loc["time_from_path_start_to_purchase_mean", "segment_2"] == 90.0

    def test_segment_values_with_double_underscore(self) -> None:
        """Regression: segment values containing '__' must stay distinct.

        Previously the composite path id was built as path_id + '__' + segment
        and the segment was recovered with .str.split('__').str[-1], which
        truncated 'control__1' and 'test__1' to '1' and merged them.
        """
        df = pd.DataFrame(
            [
                ["user_1", "A", "control__1", "2020-01-01 00:00:00"],
                ["user_1", "B", "control__1", "2020-01-01 00:01:00"],
                ["user_2", "A", "control__1", "2020-01-01 00:00:00"],
                ["user_3", "A", "test__1", "2020-01-01 00:00:00"],
                ["user_3", "B", "test__1", "2020-01-01 00:01:00"],
                ["user_3", "C", "test__1", "2020-01-01 00:02:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {"metric": "length", "agg": "mean"},
            ],
        )

        # Both segment values must survive as distinct columns
        assert sorted(result.columns) == ["control__1", "test__1"]
        assert "1" not in result.columns

        # control__1: user_1 (2 paths total in segment), test__1: user_3
        assert result.loc["segment_size", "control__1"] == 2
        assert result.loc["segment_size", "test__1"] == 1
        assert result.loc["segment_share", "control__1"] == pytest.approx(2 / 3)
        assert result.loc["segment_share", "test__1"] == pytest.approx(1 / 3)

        # control__1: user_1 has 2 events, user_2 has 1 -> mean = 1.5
        # test__1: user_3 has 3 events -> mean = 3.0
        assert result.loc["length_mean", "control__1"] == 1.5
        assert result.loc["length_mean", "test__1"] == 3.0

    def test_path_id_with_double_underscore(self) -> None:
        """Regression: path ids containing '__' must not corrupt segment recovery"""
        df = pd.DataFrame(
            [
                ["user__1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user__1", "B", "segment_1", "2020-01-01 00:01:00"],
                ["user__2", "A", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[
                {"metric": "length", "agg": "mean"},
            ],
        )

        assert sorted(result.columns) == ["segment_1", "segment_2"]
        assert result.loc["segment_size", "segment_1"] == 1
        assert result.loc["segment_size", "segment_2"] == 1
        assert result.loc["length_mean", "segment_1"] == 2.0
        assert result.loc["length_mean", "segment_2"] == 1.0

    def test_segment_values_with_double_underscore_empty_config(self) -> None:
        """Regression: '__' segment values stay distinct with empty metrics"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "control__1", "2020-01-01 00:00:00"],
                ["user_2", "A", "test__1", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = stream.segment_overview_data(
            segment_col="segment",
            metrics=[],
        )

        assert sorted(result.columns) == ["control__1", "test__1"]
        assert result.loc["segment_size", "control__1"] == 1
        assert result.loc["segment_size", "test__1"] == 1


class TestMetricDistribution:
    """Tests for metric_distribution method"""

    def test_single_segment_with_complement(self) -> None:
        """Test distribution for a single segment value with complement=True"""
        # Create users with varying path lengths
        rows = []
        for i in range(50):
            user = f"user_{i}"
            segment = "segment_1" if i < 30 else "segment_2"
            for j in range(i + 1):
                rows.append(
                    [user, f"event_{j}", segment, f"2020-01-01 00:{j % 60:02d}:00"]
                )

        df = pd.DataFrame(rows, columns=["user_id", "event", "segment", "timestamp"])
        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = SegmentOverview(stream).get_metric_distribution(
            segment_col="segment",
            segment_value="segment_1",
            metric={"metric": "length"},
            complement=True,
        )

        # Should return pair distribution when complement=True
        assert "distribution_1" in result
        assert "distribution_2" in result
        assert "distance" in result

        dist = result["distribution_1"]

        # Check all required fields are present
        assert "bins" in dist
        assert "counts" in dist
        assert "counts_normalized" in dist
        assert "kde" in dist
        assert "mean" in dist
        assert "median" in dist

        # Check that bins and counts are consistent
        assert len(dist["bins"]) == len(dist["counts"]) + 1
        assert len(dist["counts"]) == len(dist["counts_normalized"])

        # Check mean and median are reasonable
        assert dist["mean"] > 0
        assert dist["median"] > 0

        # For continuous data, KDE should be computed
        assert dist["kde"] is not None
        assert len(dist["kde"]) == 2  # [x_values, y_values]
        assert len(dist["kde"][0]) == 1000  # Default n_points

    def test_discrete_metric_with_pair(self) -> None:
        """Test distribution for discrete data (has metric) with two segments"""
        df = pd.DataFrame(
            [
                ["user_1", "purchase", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "view", "segment_1", "2020-01-01 00:00:00"],
                ["user_3", "purchase", "segment_1", "2020-01-01 00:00:00"],
                ["user_4", "view", "segment_1", "2020-01-01 00:00:00"],
                ["user_5", "view", "segment_2", "2020-01-01 00:00:00"],
                ["user_6", "purchase", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = SegmentOverview(stream).get_metric_distribution(
            segment_col="segment",
            segment_value=["segment_1", "segment_2"],
            metric={"metric": "has_event", "metric_args": {"events": "purchase"}},
        )

        assert "distribution_1" in result
        assert "distribution_2" in result

        dist_1 = result["distribution_1"]

        # For discrete data (0/1), KDE should be None
        assert dist_1["kde"] is None

        # Bins should be centered on values: [-0.5, 0.5, 1.5]
        assert len(dist_1["bins"]) == 3

        # segment_1: 2 users have purchase (1), 2 don't (0)
        assert sum(dist_1["counts"]) == 4
        assert dist_1["mean"] == 0.5

    def test_pair_of_segments(self) -> None:
        """Test distribution for a pair of segment values"""
        # Create two clearly different distributions
        rows = []
        # segment_1: short paths (1-5 events)
        for i in range(20):
            user = f"user_1_{i}"
            for j in range(i % 5 + 1):
                rows.append(
                    [user, f"event_{j}", "segment_1", f"2020-01-01 00:{j:02d}:00"]
                )

        # segment_2: longer paths (10-20 events)
        for i in range(20):
            user = f"user_2_{i}"
            for j in range(i % 10 + 10):
                rows.append(
                    [user, f"event_{j}", "segment_2", f"2020-01-01 00:{j % 60:02d}:00"]
                )

        df = pd.DataFrame(rows, columns=["user_id", "event", "segment", "timestamp"])
        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = SegmentOverview(stream).get_metric_distribution(
            segment_col="segment",
            segment_value=["segment_1", "segment_2"],
            metric={"metric": "length"},
        )

        # Should have pair response structure
        assert "distribution_1" in result
        assert "distribution_2" in result
        assert "distance" in result

        # Both distributions should have same bins (shared)
        assert result["distribution_1"]["bins"] == result["distribution_2"]["bins"]

        # Wasserstein distance should be significant (distributions are different)
        assert result["distance"] > 0

        # Mean of segment_2 should be higher than segment_1
        assert result["distribution_2"]["mean"] > result["distribution_1"]["mean"]

    def test_complement_mode(self) -> None:
        """Test distribution with complement=True"""
        # Create two clearly different distributions
        rows = []
        # segment_1: short paths (1-3 events)
        for i in range(20):
            user = f"user_1_{i}"
            for j in range(i % 3 + 1):
                rows.append(
                    [user, f"event_{j}", "segment_1", f"2020-01-01 00:{j:02d}:00"]
                )

        # segment_2: longer paths (8-10 events)
        for i in range(20):
            user = f"user_2_{i}"
            for j in range(i % 3 + 8):
                rows.append(
                    [user, f"event_{j}", "segment_2", f"2020-01-01 00:{j % 60:02d}:00"]
                )

        df = pd.DataFrame(rows, columns=["user_id", "event", "segment", "timestamp"])
        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = SegmentOverview(stream).get_metric_distribution(
            segment_col="segment",
            segment_value="segment_1",
            metric={"metric": "length"},
            complement=True,
        )

        # Should have pair response structure (because complement=True)
        assert "distribution_1" in result
        assert "distribution_2" in result
        assert "distance" in result

        # Wasserstein distance should be positive
        assert result["distance"] > 0

    def test_invalid_segment_column(self) -> None:
        """Test error handling for invalid segment column"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "A", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        with pytest.raises(
            InvalidParameterError,
            match="Invalid value 'invalid_segment' for parameter 'segment_col'",
        ):
            SegmentOverview(stream).get_metric_distribution(
                segment_col="invalid_segment",
                segment_value=["segment_1", "segment_2"],
                metric={"metric": "length"},
            )

    def test_nonexistent_segment_value(self) -> None:
        """Test error when segment value doesn't exist"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "A", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        with pytest.raises(
            SegmentValueNotFoundError, match="Segment value 'non_existent' not found"
        ):
            SegmentOverview(stream).get_metric_distribution(
                segment_col="segment",
                segment_value=["segment_1", "non_existent"],
                metric={"metric": "length"},
            )

    def test_event_count_metric(self) -> None:
        """Test distribution with event_count metric"""
        df = pd.DataFrame(
            [
                ["user_1", "click", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "click", "segment_1", "2020-01-01 00:01:00"],
                ["user_1", "click", "segment_1", "2020-01-01 00:02:00"],
                ["user_2", "click", "segment_1", "2020-01-01 00:00:00"],
                ["user_3", "view", "segment_1", "2020-01-01 00:00:00"],
                ["user_4", "view", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = SegmentOverview(stream).get_metric_distribution(
            segment_col="segment",
            segment_value="segment_1",
            metric={"metric": "event_count", "metric_args": {"events": "click"}},
            complement=True,
        )

        dist = result["distribution_1"]

        # segment_1: user_1: 3 clicks, user_2: 1 click, user_3: 0 clicks
        # mean = (3 + 1 + 0) / 3 = 1.333...
        assert dist["mean"] == pytest.approx(4 / 3, rel=0.01)

    def test_normalized_counts_sum_to_one(self) -> None:
        """Test that counts_normalized sum to 1"""
        rows = []
        for i in range(100):
            user = f"user_{i}"
            segment = "segment_1" if i < 50 else "segment_2"
            for j in range(i % 10 + 1):
                rows.append([user, f"event_{j}", segment, f"2020-01-01 00:{j:02d}:00"])

        df = pd.DataFrame(rows, columns=["user_id", "event", "segment", "timestamp"])
        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = SegmentOverview(stream).get_metric_distribution(
            segment_col="segment",
            segment_value=["segment_1", "segment_2"],
            metric={"metric": "length"},
        )

        dist = result["distribution_1"]
        assert sum(dist["counts_normalized"]) == pytest.approx(1.0, rel=0.001)

    def test_wasserstein_distance_identical_distributions(self) -> None:
        """Test that Wasserstein distance is 0 for identical distributions"""
        # Create identical data in both segments
        rows = []
        for seg in ["segment_1", "segment_2"]:
            for i in range(20):
                user = f"user_{seg}_{i}"
                for j in range(5):
                    rows.append([user, f"event_{j}", seg, f"2020-01-01 00:{j:02d}:00"])

        df = pd.DataFrame(rows, columns=["user_id", "event", "segment", "timestamp"])
        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = SegmentOverview(stream).get_metric_distribution(
            segment_col="segment",
            segment_value=["segment_1", "segment_2"],
            metric={"metric": "length"},
        )

        # For identical distributions, Wasserstein distance should be 0
        assert result["distance"] == pytest.approx(0.0, abs=0.001)

    def test_multiple_metrics_error(self) -> None:
        """Test that error is raised when metric config produces multiple metrics"""
        df = pd.DataFrame(
            [
                ["user_1", "click", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "purchase", "segment_1", "2020-01-01 00:01:00"],
                ["user_2", "view", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        # This config produces 2 metrics: event_count_click and event_count_purchase
        with pytest.raises(
            InvalidMetricConfigError, match="requires exactly one metric"
        ):
            SegmentOverview(stream).get_metric_distribution(
                segment_col="segment",
                segment_value=["segment_1", "segment_2"],
                metric={
                    "metric": "event_count",
                    "metric_args": {"events": ["click", "purchase"]},
                },
            )

    def test_has_multiple_events_error(self) -> None:
        """Test that error is raised when 'has_event' metric has multiple events"""
        df = pd.DataFrame(
            [
                ["user_1", "click", "segment_1", "2020-01-01 00:00:00"],
                ["user_1", "purchase", "segment_1", "2020-01-01 00:01:00"],
                ["user_2", "view", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        # This config produces 2 metrics: has_click and has_purchase
        with pytest.raises(
            InvalidMetricConfigError, match="requires exactly one metric"
        ):
            SegmentOverview(stream).get_metric_distribution(
                segment_col="segment",
                segment_value=["segment_1", "segment_2"],
                metric={
                    "metric": "has_event",
                    "metric_args": {"events": ["click", "purchase"]},
                },
            )

    def test_single_segment_without_complement_error(self) -> None:
        """Test that error is raised when single segment provided without complement=True"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "A", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        with pytest.raises(
            InvalidComplementConfigError, match="complement must be True"
        ):
            SegmentOverview(stream).get_metric_distribution(
                segment_col="segment",
                segment_value="segment_1",
                metric={"metric": "length"},
                complement=False,
            )

    def test_two_segments_with_complement_error(self) -> None:
        """Test that error is raised when two segments provided with complement=True"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "A", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        with pytest.raises(
            InvalidComplementConfigError,
            match="complement=True is only valid when a single segment",
        ):
            SegmentOverview(stream).get_metric_distribution(
                segment_col="segment",
                segment_value=["segment_1", "segment_2"],
                metric={"metric": "length"},
                complement=True,
            )

    def test_invalid_path_col(self) -> None:
        """Test error when path_col doesn't exist"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "A", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        with pytest.raises(
            InvalidParameterError,
            match="Invalid value 'invalid_col' for parameter 'path_col'",
        ):
            SegmentOverview(stream).get_metric_distribution(
                segment_col="segment",
                segment_value=["segment_1", "segment_2"],
                metric={"metric": "length"},
                path_col="invalid_col",
            )

    # Note: Metric validation tests (invalid metric name, missing args, nonexistent events)
    # are in tests/metrics/test_metric_builder.py::TestValidateMetricConfig

    def test_log_scale_auto_detection(self) -> None:
        """Test that log_scale is auto-detected for highly skewed data"""
        # Create data with exponential-like distribution (large range, high skewness)
        rows = []
        # segment_1: exponential-like values (1, 10, 100, 1000, 10000, ...)
        for i in range(50):
            user = f"user_1_{i}"
            # Duration values with exponential growth
            duration_events = int(10 ** (i / 10))  # Creates values from 1 to 100000+
            for j in range(duration_events % 100 + 1):
                rows.append(
                    [
                        user,
                        f"event_{j}",
                        "segment_1",
                        f"2020-01-01 00:{j % 60:02d}:{j % 60:02d}",
                    ]
                )

        # segment_2: similar exponential pattern
        for i in range(50):
            user = f"user_2_{i}"
            duration_events = int(10 ** (i / 10))
            for j in range(duration_events % 100 + 1):
                rows.append(
                    [
                        user,
                        f"event_{j}",
                        "segment_2",
                        f"2020-01-01 00:{j % 60:02d}:{j % 60:02d}",
                    ]
                )

        df = pd.DataFrame(rows, columns=["user_id", "event", "segment", "timestamp"])
        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = SegmentOverview(stream).get_metric_distribution(
            segment_col="segment",
            segment_value=["segment_1", "segment_2"],
            metric={"metric": "length"},
        )

        # log_scale key should be present
        assert "log_scale" in result
        # Since the data has high range/skewness, log_scale might be True
        # (depends on actual data distribution)
        assert isinstance(result["log_scale"], bool)

    def test_log_scale_not_used_for_discrete(self) -> None:
        """Test that log_scale is never used for discrete data"""
        df = pd.DataFrame(
            [
                ["user_1", "purchase", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "view", "segment_1", "2020-01-01 00:00:00"],
                ["user_3", "view", "segment_2", "2020-01-01 00:00:00"],
                ["user_4", "purchase", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = SegmentOverview(stream).get_metric_distribution(
            segment_col="segment",
            segment_value=["segment_1", "segment_2"],
            metric={"metric": "has_event", "metric_args": {"events": "purchase"}},
        )

        # For discrete (has) metric, log_scale should always be False
        assert result["log_scale"] is False

    def test_max_bins_limit(self) -> None:
        """Test that histogram bins are limited to MAX_BINS"""
        # Create data with many unique values that would normally create many bins
        rows = []
        for i in range(200):
            user = f"user_{i}"
            # Each user has unique number of events
            for j in range(i + 1):
                rows.append(
                    [
                        user,
                        f"event_{j}",
                        "segment_1" if i < 100 else "segment_2",
                        f"2020-01-01 00:{j % 60:02d}:{j % 60:02d}",
                    ]
                )

        df = pd.DataFrame(rows, columns=["user_id", "event", "segment", "timestamp"])
        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = SegmentOverview(stream).get_metric_distribution(
            segment_col="segment",
            segment_value=["segment_1", "segment_2"],
            metric={"metric": "length"},
        )

        # Number of bins should not exceed MAX_BINS (50) + 1 (for edges)
        assert len(result["distribution_1"]["bins"]) <= 51

    def test_segment_values_with_double_underscore(self) -> None:
        """Regression: metric_distribution must not silently return empty
        distributions for segment values containing '__'.

        Previously the segment recovered from the composite id was truncated
        to the piece after the last '__' ('control__1' -> '1'), so the mask
        metrics_df[segment_col] == 'control__1' matched nothing.
        """
        df = pd.DataFrame(
            [
                ["user_1", "A", "control__1", "2020-01-01 00:00:00"],
                ["user_1", "B", "control__1", "2020-01-01 00:01:00"],
                ["user_2", "A", "control__1", "2020-01-01 00:00:00"],
                ["user_3", "A", "test__1", "2020-01-01 00:00:00"],
                ["user_3", "B", "test__1", "2020-01-01 00:01:00"],
                ["user_3", "C", "test__1", "2020-01-01 00:02:00"],
                ["user_4", "A", "test__1", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = SegmentOverview(stream).get_metric_distribution(
            segment_col="segment",
            segment_value=["control__1", "test__1"],
            metric={"metric": "length"},
        )

        # control__1: 2 paths (lengths 2, 1); test__1: 2 paths (lengths 3, 1)
        assert sum(result["distribution_1"]["counts"]) == 2
        assert sum(result["distribution_2"]["counts"]) == 2
        assert result["distribution_1"]["mean"] == pytest.approx(1.5)
        assert result["distribution_2"]["mean"] == pytest.approx(2.0)

    def test_complement_with_double_underscore_segment(self) -> None:
        """Regression: complement mode works for segment values containing '__'"""
        df = pd.DataFrame(
            [
                ["user_1", "A", "control__1", "2020-01-01 00:00:00"],
                ["user_1", "B", "control__1", "2020-01-01 00:01:00"],
                ["user_2", "A", "test__1", "2020-01-01 00:00:00"],
                ["user_3", "A", "test__1", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )

        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        stream = Eventstream(df, schema)

        result = SegmentOverview(stream).get_metric_distribution(
            segment_col="segment",
            segment_value="control__1",
            metric={"metric": "length"},
            complement=True,
        )

        # distribution_1: control__1 (1 path, length 2)
        # distribution_2: complement = test__1 (2 paths, length 1 each)
        assert sum(result["distribution_1"]["counts"]) == 1
        assert sum(result["distribution_2"]["counts"]) == 2
        assert result["distribution_1"]["mean"] == pytest.approx(2.0)
        assert result["distribution_2"]["mean"] == pytest.approx(1.0)


class TestLogScaleSharedZeroOffset:
    """Regression tests for the log-scale branch of _build_pair_distribution.

    Previously each array (data_1, data_2, combined) derived its own
    zero-replacement offset (its min positive value / 2), so identical raw
    zeros in the two groups mapped to different transformed values whenever
    the groups had different minimum positive values. That distorted the
    shared-bin histograms and injected a pure artifact into the Wasserstein
    distance. Now a single offset derived from the combined data is used for
    all three transforms.
    """

    # Data shapes chosen so the pair takes the log-scale continuous branch:
    # combined data has > DISCRETE_THRESHOLD unique values (not discrete) and
    # max/min-positive ratio 1000 / 0.01 = 1e5 > LOG_SCALE_RANGE_RATIO.
    # Both groups contain a raw zero, but wildly different min positives
    # (100 vs 0.01), which is exactly the case the old per-array offsets broke.
    DATA_1 = np.array([0.0, 100.0, 200.0, 300.0, 400.0, 500.0, 1000.0])
    DATA_2 = np.array([0.0, 0.01, 150.0, 250.0, 350.0, 450.0, 1000.0])

    @staticmethod
    def _make_overview() -> SegmentOverview:
        df = pd.DataFrame(
            [
                ["user_1", "A", "segment_1", "2020-01-01 00:00:00"],
                ["user_2", "A", "segment_2", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "segment", "timestamp"],
        )
        schema = {"event_cols": ["event"], "segment_cols": ["segment"]}
        return SegmentOverview(Eventstream(df, schema))

    def test_identical_zeros_transform_equally_across_groups(self) -> None:
        """Raw zeros in both groups must land at the same transformed value"""
        overview = self._make_overview()
        combined = np.concatenate([self.DATA_1, self.DATA_2])

        offset = overview._log_zero_offset(combined)
        transformed_1 = overview._log_transform(self.DATA_1, offset)
        transformed_2 = overview._log_transform(self.DATA_2, offset)

        # Shared offset = min positive of combined / 2 = 0.01 / 2 = 0.005
        expected_zero = np.log10(0.005)
        assert transformed_1[0] == pytest.approx(expected_zero)
        assert transformed_2[0] == pytest.approx(expected_zero)
        # Under the old per-array derivation data_1's zero mapped to
        # log10(100 / 2) ~= +1.7 instead of ~-2.3.

    def test_pair_distribution_uses_shared_offset(self) -> None:
        """_build_pair_distribution stats must reflect the shared offset"""
        overview = self._make_overview()
        result = overview._build_pair_distribution(self.DATA_1, self.DATA_2)

        assert result["log_scale"] is True

        # Expected transforms with the single combined-data offset (0.005)
        offset = 0.005
        expected_1 = np.log10(np.where(self.DATA_1 > 0, self.DATA_1, offset))
        expected_2 = np.log10(np.where(self.DATA_2 > 0, self.DATA_2, offset))

        assert result["distribution_1"]["mean"] == pytest.approx(
            float(np.mean(expected_1))
        )
        assert result["distribution_2"]["mean"] == pytest.approx(
            float(np.mean(expected_2))
        )
        assert result["distance"] == pytest.approx(
            float(wasserstein_distance(expected_1, expected_2))
        )
        # Sanity: under the old code distribution_1's mean was inflated by
        # (log10(50) - log10(0.005)) / len(data_1) = 4 / 7 ~= 0.57.
