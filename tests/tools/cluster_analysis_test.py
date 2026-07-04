import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream


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


class TestClusterAnalysis:
    def test_kmeans_basic(self) -> None:
        """Test basic kmeans clustering with overview"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
            ],
            method="kmeans",
            n_clusters=2,
            scaler="minmax",
            metrics_config=[
                {"metric": "length", "agg": "mean"},
            ],
        )

        assert "overview_df" in result
        overview_df = result["overview_df"]
        assert "segment_size" in overview_df.index
        assert "segment_share" in overview_df.index
        assert "length_mean" in overview_df.index
        assert len(overview_df.columns) <= 2

    def test_kmeans_silhouette(self) -> None:
        """Test silhouette mode returns scores for each k"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
            ],
            method="kmeans",
            n_clusters=[2, 3, 4, 5],
            scaler="minmax",
        )

        assert "silhouette" in result
        sil = result["silhouette"]
        assert len(sil["params"]) == 4
        assert len(sil["silhouette"]) == 4
        for p in sil["params"]:
            assert "n_clusters" in p
        # All silhouette scores should be valid floats between -1 and 1
        for score in sil["silhouette"]:
            assert score is not None
            assert -1 <= score <= 1
        # Best clustering overview is returned
        assert "overview_df" in result
        assert "segment_size" in result["overview_df"].index

    def test_hdbscan_basic(self) -> None:
        """Test hdbscan clustering with overview"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
            ],
            method="hdbscan",
            min_cluster_size=2,
            scaler="minmax",
            metrics_config=[
                {"metric": "length", "agg": "mean"},
            ],
        )

        assert "overview_df" in result
        overview_df = result["overview_df"]
        assert "segment_size" in overview_df.index

    def test_hdbscan_silhouette(self) -> None:
        """Test HDBSCAN parameter search with silhouette"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
            ],
            method="hdbscan",
            min_cluster_size=[2, 3, 4],
            scaler="minmax",
        )

        assert "silhouette" in result
        sil = result["silhouette"]
        assert len(sil["params"]) == 3
        assert len(sil["silhouette"]) == 3
        for p in sil["params"]:
            assert "min_cluster_size" in p
            assert "cluster_selection_epsilon" in p
        # Best clustering overview is returned
        assert "overview_df" in result

    @pytest.mark.skip(
        reason="HDBSCAN cluster_selection_epsilon grid triggers sklearn/numpy scalar bug"
    )
    def test_hdbscan_silhouette_grid(self) -> None:
        """Test HDBSCAN parameter search with both ranges (grid search)"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
            ],
            method="hdbscan",
            min_cluster_size=[2, 3],
            cluster_selection_epsilon=[0.0, 0.25, 0.5],
            scaler="minmax",
        )

        assert "silhouette" in result
        sil = result["silhouette"]
        # 2 mcs * 3 eps = 6 combinations
        assert len(sil["params"]) == 6
        assert len(sil["silhouette"]) == 6

    def test_nmf_kmeans(self) -> None:
        """Test NMF + kmeans returns overview and H matrix"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
                {"metric": "has", "metric_args": {"events": "purchase"}},
                {"metric": "event_count", "metric_args": {"events": "view"}},
            ],
            method="kmeans",
            n_clusters=2,
            scaler="minmax",
            nmf_k=2,
            metrics_config=[
                {"metric": "length", "agg": "mean"},
            ],
        )

        assert "overview_df" in result
        nmf = result["nmf"]
        assert nmf is not None
        # H matrix shape: (nmf_k, n_features) = (2, 4)
        assert len(nmf["H_matrix"]) == 2
        assert len(nmf["H_matrix"][0]) == 4
        assert len(nmf["features"]) == 4
        # W_cluster_means: dict {cluster_name: [w1, w2, ...]}
        assert "W_cluster_means" in nmf
        assert len(nmf["W_cluster_means"]) == 2  # 2 clusters
        for name, means in nmf["W_cluster_means"].items():
            assert name.startswith("cluster_")
            assert len(means) == 2  # nmf_k=2 components

    def test_nmf_silhouette(self) -> None:
        """Test NMF + silhouette mode returns silhouette data but no H matrix"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
                {"metric": "has", "metric_args": {"events": "purchase"}},
            ],
            method="kmeans",
            n_clusters=[2, 3, 4],
            scaler="minmax",
            nmf_k=2,
        )

        assert "silhouette" in result
        assert result["silhouette"]["params"] == [
            {"n_clusters": 2},
            {"n_clusters": 3},
            {"n_clusters": 4},
        ]
        # NMF data returned for the best clustering
        assert result["nmf"] is not None
        assert len(result["nmf"]["H_matrix"]) == 2  # nmf_k=2
        assert "W_cluster_means" in result["nmf"]
        assert "overview_df" in result

    def test_nmf_k_search(self) -> None:
        """Test nmf_k as list triggers search over nmf components"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
                {"metric": "has", "metric_args": {"events": "purchase"}},
            ],
            method="kmeans",
            n_clusters=2,
            scaler="minmax",
            nmf_k=[2, 3],
        )

        assert "silhouette" in result
        sil = result["silhouette"]
        assert len(sil["params"]) == 2
        assert sil["params"] == [
            {"n_clusters": 2, "nmf_k": 2},
            {"n_clusters": 2, "nmf_k": 3},
        ]

    def test_nmf_k_grid_search(self) -> None:
        """Test nmf_k list x n_clusters list grid search"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
                {"metric": "has", "metric_args": {"events": "purchase"}},
            ],
            method="kmeans",
            n_clusters=[2, 3],
            scaler="minmax",
            nmf_k=[2, 3],
        )

        assert "silhouette" in result
        sil = result["silhouette"]
        # 2 nmf_k * 2 n_clusters = 4
        assert len(sil["params"]) == 4
        assert sil["params"] == [
            {"n_clusters": 2, "nmf_k": 2},
            {"n_clusters": 3, "nmf_k": 2},
            {"n_clusters": 2, "nmf_k": 3},
            {"n_clusters": 3, "nmf_k": 3},
        ]

    def test_kmeans_missing_n_clusters(self) -> None:
        """Test kmeans without n_clusters raises a clean ValueError (normal mode)"""
        df = get_df()
        stream = Eventstream(df)

        with pytest.raises(
            ValueError, match="n_clusters is required for kmeans method"
        ):
            stream.cluster_analysis_data(
                features=[
                    {"metric": "length"},
                    {"metric": "duration"},
                ],
                method="kmeans",
                nmf_k=2,
            )

    def test_kmeans_missing_n_clusters_nmf_k_search(self) -> None:
        """Test nmf_k-only search with kmeans and no n_clusters raises a clean ValueError"""
        df = get_df()
        stream = Eventstream(df)

        with pytest.raises(
            ValueError, match="n_clusters is required for kmeans method"
        ):
            stream.cluster_analysis_data(
                features=[
                    {"metric": "length"},
                    {"metric": "duration"},
                ],
                method="kmeans",
                nmf_k=[2, 3],
            )

    def test_overview_empty_metrics_config(self) -> None:
        """Test that overview works with empty metrics_config (only segment_size/share)"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[{"metric": "length"}],
            method="kmeans",
            n_clusters=2,
            scaler="minmax",
            metrics_config=[],
        )

        overview_df = result["overview_df"]
        assert "segment_size" in overview_df.index
        assert "segment_share" in overview_df.index
