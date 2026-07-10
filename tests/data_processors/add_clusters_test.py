import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import (
    InvalidMetricConfigError,
    PreprocessingConfigError,
)


def get_df():
    """Create test dataframe with multiple users and varying path characteristics"""
    df = pd.DataFrame(
        [
            # User 1: short path, no purchase
            ["user_1", "login", "2020-01-01 00:00:00"],
            ["user_1", "view", "2020-01-01 00:01:00"],
            # User 2: short path, no purchase
            ["user_2", "login", "2020-01-01 00:00:00"],
            ["user_2", "logout", "2020-01-01 00:02:00"],
            # User 3: medium path with purchase
            ["user_3", "login", "2020-01-01 00:00:00"],
            ["user_3", "view", "2020-01-01 00:01:00"],
            ["user_3", "purchase", "2020-01-01 00:05:00"],
            # User 4: long path with purchase
            ["user_4", "login", "2020-01-01 00:00:00"],
            ["user_4", "view", "2020-01-01 00:01:00"],
            ["user_4", "view", "2020-01-01 00:02:00"],
            ["user_4", "view", "2020-01-01 00:03:00"],
            ["user_4", "purchase", "2020-01-01 00:10:00"],
            # User 5: long path with purchase
            ["user_5", "login", "2020-01-01 00:00:00"],
            ["user_5", "view", "2020-01-01 00:01:00"],
            ["user_5", "view", "2020-01-01 00:02:00"],
            ["user_5", "purchase", "2020-01-01 00:08:00"],
            # User 6: short path, no purchase
            ["user_6", "login", "2020-01-01 00:00:00"],
            ["user_6", "view", "2020-01-01 00:01:00"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    return df


class TestAddClusters:
    def test_kmeans_basic(self) -> None:
        """Test basic k-means clustering"""
        df = get_df()
        stream = Eventstream(df)

        features = [
            {"metric": "length"},
            {"metric": "duration"},
        ]

        result = stream.add_clusters(
            name="cluster",
            features=features,
            method="kmeans",
            n_clusters=2,
            scaler="minmax",
        )

        # Check segment was added
        assert "cluster" in result.schema.segment_cols
        assert "cluster" in result.df.columns

        # Check all rows have cluster labels
        assert result.df["cluster"].notna().all()

        # Check cluster labels format
        unique_clusters = result.df["cluster"].unique()
        assert len(unique_clusters) <= 2
        for cluster in unique_clusters:
            assert cluster.startswith("cluster_")

    def test_kmeans_with_has_metric(self) -> None:
        """Test k-means with has metric feature"""
        df = get_df()
        stream = Eventstream(df)

        features = [
            {"metric": "length"},
            {"metric": "has_event", "metric_args": {"events": "purchase"}},
        ]

        result = stream.add_clusters(
            name="buyer_cluster",
            features=features,
            method="kmeans",
            n_clusters=2,
            scaler="minmax",
        )

        assert "buyer_cluster" in result.schema.segment_cols
        assert result.df["buyer_cluster"].notna().all()

    def test_kmeans_with_event_count(self) -> None:
        """Test k-means with event_count metric feature"""
        df = get_df()
        stream = Eventstream(df)

        features = [
            {"metric": "event_count", "metric_args": {"events": "view"}},
            {"metric": "duration"},
        ]

        result = stream.add_clusters(
            name="view_cluster",
            features=features,
            method="kmeans",
            n_clusters=2,
            scaler="standard",
        )

        assert "view_cluster" in result.schema.segment_cols

    def test_kmeans_no_scaler(self) -> None:
        """Test k-means without feature scaling"""
        df = get_df()
        stream = Eventstream(df)

        features = [{"metric": "length"}]

        result = stream.add_clusters(
            name="cluster",
            features=features,
            method="kmeans",
            n_clusters=2,
            scaler=None,
        )

        assert "cluster" in result.schema.segment_cols

    def test_kmeans_missing_n_clusters(self) -> None:
        """Test that kmeans raises error without n_clusters"""
        df = get_df()
        stream = Eventstream(df)

        features = [{"metric": "length"}]

        with pytest.raises(PreprocessingConfigError, match="n_clusters is required"):
            stream.add_clusters(
                name="cluster",
                features=features,
                method="kmeans",
                scaler="minmax",
            )

    def test_hdbscan_basic(self) -> None:
        """Test basic HDBSCAN clustering"""
        df = get_df()
        stream = Eventstream(df)

        features = [
            {"metric": "length"},
            {"metric": "duration"},
        ]

        result = stream.add_clusters(
            name="cluster",
            features=features,
            method="hdbscan",
            min_cluster_size=2,
            scaler="minmax",
        )

        assert "cluster" in result.schema.segment_cols
        assert result.df["cluster"].notna().all()

        # HDBSCAN can produce "noise" labels
        unique_clusters = result.df["cluster"].unique()
        for cluster in unique_clusters:
            assert cluster.startswith("cluster_") or cluster == "noise"

    def test_hdbscan_with_epsilon(self) -> None:
        """Test HDBSCAN with cluster_selection_epsilon"""
        df = get_df()
        stream = Eventstream(df)

        features = [{"metric": "length"}, {"metric": "duration"}]

        result = stream.add_clusters(
            name="cluster",
            features=features,
            method="hdbscan",
            min_cluster_size=2,
            cluster_selection_epsilon=0.5,
            scaler="standard",
        )

        assert "cluster" in result.schema.segment_cols

    def test_duplicate_segment_name(self) -> None:
        """Test that adding cluster with existing segment name raises error"""
        df = get_df()
        df["existing_segment"] = "value"
        schema = {"segment_cols": ["existing_segment"]}
        stream = Eventstream(df, schema)

        features = [{"metric": "length"}]

        with pytest.raises(PreprocessingConfigError, match="already exists"):
            stream.add_clusters(
                name="existing_segment",
                features=features,
                method="kmeans",
                n_clusters=2,
            )

    def test_reserved_column_name(self) -> None:
        """Test that using reserved column name raises error"""
        df = get_df()
        stream = Eventstream(df)

        features = [{"metric": "length"}]

        with pytest.raises(PreprocessingConfigError, match="already reserved"):
            stream.add_clusters(
                name="user_id", features=features, method="kmeans", n_clusters=2
            )

    def test_unknown_method(self) -> None:
        """Test that unknown clustering method raises error"""
        df = get_df()
        stream = Eventstream(df)

        features = [{"metric": "length"}]

        with pytest.raises(PreprocessingConfigError, match="Unknown clustering method"):
            stream.add_clusters(
                name="cluster",
                features=features,
                method="unknown_method",
                n_clusters=2,
            )

    def test_cluster_labels_mapped_to_all_events(self) -> None:
        """Test that cluster labels are properly mapped to all events in path"""
        df = get_df()
        stream = Eventstream(df)

        features = [{"metric": "length"}]

        result = stream.add_clusters(
            name="cluster",
            features=features,
            method="kmeans",
            n_clusters=2,
            scaler="minmax",
        )

        # All events from the same user should have the same cluster label
        for user_id in df["user_id"].unique():
            user_clusters = result.df[result.df["user_id"] == user_id][
                "cluster"
            ].unique()
            assert len(user_clusters) == 1, f"User {user_id} has multiple clusters"

    def test_multiple_features(self) -> None:
        """Test clustering with multiple diverse features"""
        df = get_df()
        stream = Eventstream(df)

        features = [
            {"metric": "length"},
            {"metric": "duration"},
            {"metric": "has_event", "metric_args": {"events": "purchase"}},
            {"metric": "has_event", "metric_args": {"events": "view"}},
            {"metric": "event_count", "metric_args": {"events": "view"}},
        ]

        result = stream.add_clusters(
            name="multi_cluster",
            features=features,
            method="kmeans",
            n_clusters=3,
            scaler="minmax",
        )

        assert "multi_cluster" in result.schema.segment_cols
        # With 3 clusters requested, we might get up to 3 unique clusters
        unique_clusters = result.df["multi_cluster"].unique()
        assert len(unique_clusters) <= 3

    def test_scaler_std(self) -> None:
        """Test standard scaler"""
        df = get_df()
        stream = Eventstream(df)

        features = [{"metric": "length"}, {"metric": "duration"}]

        result = stream.add_clusters(
            name="cluster",
            features=features,
            method="kmeans",
            n_clusters=2,
            scaler="standard",
        )

        assert "cluster" in result.schema.segment_cols

    def test_nmf_kmeans(self) -> None:
        """Test NMF dimensionality reduction before k-means clustering"""
        df = get_df()
        stream = Eventstream(df)

        features = [
            {"metric": "length"},
            {"metric": "duration"},
            {"metric": "has_event", "metric_args": {"events": "purchase"}},
            {"metric": "event_count", "metric_args": {"events": "view"}},
        ]

        result = stream.add_clusters(
            name="nmf_cluster",
            features=features,
            method="kmeans",
            n_clusters=2,
            scaler="minmax",
            nmf_components=2,
        )

        assert "nmf_cluster" in result.schema.segment_cols
        assert result.df["nmf_cluster"].notna().all()

        unique_clusters = result.df["nmf_cluster"].unique()
        assert len(unique_clusters) <= 2
        for cluster in unique_clusters:
            assert cluster.startswith("cluster_")

        # All events from the same user should have the same cluster label
        for user_id in df["user_id"].unique():
            user_clusters = result.df[result.df["user_id"] == user_id][
                "nmf_cluster"
            ].unique()
            assert len(user_clusters) == 1

    def test_nmf_hdbscan(self) -> None:
        """Test NMF dimensionality reduction before HDBSCAN clustering"""
        df = get_df()
        stream = Eventstream(df)

        features = [
            {"metric": "length"},
            {"metric": "duration"},
            {"metric": "has_event", "metric_args": {"events": "purchase"}},
        ]

        result = stream.add_clusters(
            name="nmf_hdb_cluster",
            features=features,
            method="hdbscan",
            min_cluster_size=2,
            scaler="minmax",
            nmf_components=2,
        )

        assert "nmf_hdb_cluster" in result.schema.segment_cols
        assert result.df["nmf_hdb_cluster"].notna().all()

        unique_clusters = result.df["nmf_hdb_cluster"].unique()
        for cluster in unique_clusters:
            assert cluster.startswith("cluster_") or cluster == "noise"

    def test_typo_event_in_feature_raises(self) -> None:
        """A typoed event name in a feature must fail loudly instead of
        silently feeding an all-zero column into the clustering model."""
        df = get_df()
        stream = Eventstream(df)

        features = [
            {"metric": "length"},
            {"metric": "has_event", "metric_args": {"events": "purchse"}},  # typo
        ]

        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            stream.add_clusters(
                name="cluster",
                features=features,
                method="kmeans",
                n_clusters=2,
                scaler="minmax",
            )

    def test_custom_path_col(self) -> None:
        """Test clustering with custom path_col"""
        df = pd.DataFrame(
            [
                ["session_1", "login", "2020-01-01 00:00:00"],
                ["session_1", "view", "2020-01-01 00:01:00"],
                ["session_2", "login", "2020-01-01 00:00:00"],
                ["session_2", "purchase", "2020-01-01 00:05:00"],
                ["session_3", "login", "2020-01-01 00:00:00"],
                ["session_3", "view", "2020-01-01 00:01:00"],
            ],
            columns=["session_id", "event", "timestamp"],
        )

        schema = {"path_cols": ["session_id"]}
        stream = Eventstream(df, schema)

        features = [{"metric": "length"}]

        result = stream.add_clusters(
            name="cluster",
            features=features,
            method="kmeans",
            n_clusters=2,
            path_col="session_id",
        )

        assert "cluster" in result.schema.segment_cols
        # Each session should have exactly one cluster label
        for session_id in df["session_id"].unique():
            session_clusters = result.df[result.df["session_id"] == session_id][
                "cluster"
            ].unique()
            assert len(session_clusters) == 1
