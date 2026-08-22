import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidMetricConfigError


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


def get_large_df():
    """12 users with varying path lengths - enough distinct paths for the
    default method_args={"n_clusters": "3-8"} grid search, which needs at least 8 samples."""
    rows = []
    for i in range(12):
        uid = f"user_{i}"
        rows.append([uid, "login", "2020-01-01 00:00:00"])
        for j in range(i % 4):
            rows.append([uid, "view", f"2020-01-01 00:0{j + 1}:00"])
        if i % 3 == 0:
            rows.append([uid, "purchase", "2020-01-01 00:09:00"])
    return pd.DataFrame(rows, columns=["user_id", "event", "timestamp"])


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
            method_args={"n_clusters": 2},
            scaler="minmax",
            overview_metrics=[
                {"metric": "length", "agg": "mean"},
            ],
        )

        assert "overview_df" in result
        overview_df = result["overview_df"]
        assert "segment_size" in overview_df.index
        assert "segment_share" in overview_df.index
        assert "length_mean" in overview_df.index
        assert len(overview_df.columns) <= 2

        # best_params reports the exact (fixed) config used, already shaped as
        # add_clusters keyword arguments
        assert result["best_params"] == {
            "method": "kmeans",
            "method_args": {"n_clusters": 2},
            "scaler": "minmax",
        }

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
            method_args={"n_clusters": [2, 3, 4, 5]},
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

        # best_params reports the winning n_clusters from the grid search,
        # so callers can reproduce the exact same clustering via add_clusters.
        assert "best_params" in result
        assert result["best_params"]["method_args"]["n_clusters"] in [2, 3, 4, 5]
        best_idx = sil["params"].index(result["best_params"]["method_args"])
        assert sil["silhouette"][best_idx] == max(sil["silhouette"])

    def test_kmeans_n_clusters_range_string(self) -> None:
        """n_clusters accepts the "lo-hi" range string documented on
        cluster_analysis/cluster_analysis_data, not just a list of ints."""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
            ],
            method="kmeans",
            method_args={"n_clusters": "2-5"},
            scaler="minmax",
        )

        assert "silhouette" in result
        assert [p["n_clusters"] for p in result["silhouette"]["params"]] == [
            2,
            3,
            4,
            5,
        ]
        assert result["best_params"]["method_args"]["n_clusters"] in [2, 3, 4, 5]

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
            method_args={"min_cluster_size": 2},
            scaler="minmax",
            overview_metrics=[
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
            method_args={"min_cluster_size": [2, 3, 4]},
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
            method_args={
                "min_cluster_size": [2, 3],
                "cluster_selection_epsilon": [0.0, 0.25, 0.5],
            },
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
                {"metric": "has_event", "metric_args": {"event": "purchase"}},
                {"metric": "event_count", "metric_args": {"event": "view"}},
            ],
            method="kmeans",
            method_args={"n_clusters": 2},
            scaler="minmax",
            nmf_components=2,
            overview_metrics=[
                {"metric": "length", "agg": "mean"},
            ],
        )

        assert "overview_df" in result
        nmf = result["nmf"]
        assert nmf is not None
        # H matrix shape: (nmf_components, n_features) = (2, 4)
        assert len(nmf["H_matrix"]) == 2
        assert len(nmf["H_matrix"][0]) == 4
        assert len(nmf["features"]) == 4
        # W_cluster_means: dict {cluster_name: [w1, w2, ...]}
        assert "W_cluster_means" in nmf
        assert len(nmf["W_cluster_means"]) == 2  # 2 clusters
        for name, means in nmf["W_cluster_means"].items():
            assert name.startswith("cluster_")
            assert len(means) == 2  # nmf_components=2 components

    def test_nmf_silhouette(self) -> None:
        """Test NMF + silhouette mode returns silhouette data but no H matrix"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
                {"metric": "has_event", "metric_args": {"event": "purchase"}},
            ],
            method="kmeans",
            method_args={"n_clusters": [2, 3, 4]},
            scaler="minmax",
            nmf_components=2,
        )

        assert "silhouette" in result
        assert result["silhouette"]["params"] == [
            {"n_clusters": 2},
            {"n_clusters": 3},
            {"n_clusters": 4},
        ]
        # NMF data returned for the best clustering
        assert result["nmf"] is not None
        assert len(result["nmf"]["H_matrix"]) == 2  # nmf_components=2
        assert "W_cluster_means" in result["nmf"]
        assert "overview_df" in result

    def test_nmf_k_search(self) -> None:
        """Test nmf_components as list triggers search over nmf components"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
                {"metric": "has_event", "metric_args": {"event": "purchase"}},
            ],
            method="kmeans",
            method_args={"n_clusters": 2},
            scaler="minmax",
            nmf_components=[2, 3],
        )

        assert "silhouette" in result
        sil = result["silhouette"]
        assert len(sil["params"]) == 2
        assert sil["params"] == [
            {"n_clusters": 2, "nmf_components": 2},
            {"n_clusters": 2, "nmf_components": 3},
        ]

    def test_nmf_k_grid_search(self) -> None:
        """Test nmf_components list x n_clusters list grid search"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
                {"metric": "has_event", "metric_args": {"event": "purchase"}},
            ],
            method="kmeans",
            method_args={"n_clusters": [2, 3]},
            scaler="minmax",
            nmf_components=[2, 3],
        )

        assert "silhouette" in result
        sil = result["silhouette"]
        # 2 nmf_components * 2 n_clusters = 4
        assert len(sil["params"]) == 4
        assert sil["params"] == [
            {"n_clusters": 2, "nmf_components": 2},
            {"n_clusters": 3, "nmf_components": 2},
            {"n_clusters": 2, "nmf_components": 3},
            {"n_clusters": 3, "nmf_components": 3},
        ]

    def test_kmeans_missing_n_clusters_defaults_to_range(self) -> None:
        """Omitting n_clusters for kmeans defaults to the documented "3-8"
        range search instead of raising (regression test for issue #89)."""
        df = get_large_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
            ],
            method="kmeans",
            nmf_components=2,
        )

        assert "silhouette" in result
        assert [p["n_clusters"] for p in result["silhouette"]["params"]] == [
            3,
            4,
            5,
            6,
            7,
            8,
        ]
        assert result["best_params"]["method_args"]["n_clusters"] in range(3, 9)

    def test_kmeans_missing_n_clusters_nmf_k_search_defaults_to_range(self) -> None:
        """The same "3-8" default applies when nmf_components is itself a
        grid-search list, combining into the full nmf x n_clusters grid."""
        df = get_large_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[
                {"metric": "length"},
                {"metric": "duration"},
            ],
            method="kmeans",
            nmf_components=[2, 3],
        )

        assert "silhouette" in result
        params = result["silhouette"]["params"]
        assert {p["n_clusters"] for p in params} == set(range(3, 9))
        assert {p["nmf_components"] for p in params} == {2, 3}
        assert len(params) == 6 * 2

    def test_overview_empty_overview_metrics(self) -> None:
        """Test that overview works with empty overview_metrics (only segment_size/share)"""
        df = get_df()
        stream = Eventstream(df)

        result = stream.cluster_analysis_data(
            features=[{"metric": "length"}],
            method="kmeans",
            method_args={"n_clusters": 2},
            scaler="minmax",
            overview_metrics=[],
        )

        overview_df = result["overview_df"]
        assert "segment_size" in overview_df.index
        assert "segment_share" in overview_df.index

    def test_typo_event_in_feature_raises(self) -> None:
        """A typoed event name in a feature must fail loudly instead of
        silently changing the clustering model with an all-zero column."""
        df = get_df()
        stream = Eventstream(df)

        with pytest.raises(InvalidMetricConfigError, match="purchse"):
            stream.cluster_analysis_data(
                features=[
                    {"metric": "length"},
                    {"metric": "has_event", "metric_args": {"event": "purchse"}},
                ],
                method="kmeans",
                method_args={"n_clusters": 2},
                scaler="minmax",
            )


class TestGridSelection:
    """`select=` interprets a chosen grid point instead of the silhouette
    winner, while keeping the whole grid — the point being that a near-tie on
    score is not a tie on interpretability, and picking the runner-up should not
    cost you the chart that shows what the trade was."""

    FEATURES = [
        {"metric": "length"},
        {"metric": "event_count", "metric_args": {"event": "view"}},
    ]

    @pytest.fixture()
    def stream(self):
        return Eventstream(get_large_df())

    def test__default_interprets_the_silhouette_winner(self, stream):
        res = stream.cluster_analysis_data(
            features=self.FEATURES, method_args={"n_clusters": [2, 3, 4]}
        )
        sil = res["silhouette"]
        assert sil["selected_index"] is None
        assert res["best_params"]["method_args"] == sil["params"][sil["best_index"]]

    def test__select_interprets_the_named_point(self, stream):
        res = stream.cluster_analysis_data(
            features=self.FEATURES,
            method_args={"n_clusters": [2, 3, 4]},
            select={"n_clusters": 4},
        )
        assert res["best_params"]["method_args"] == {"n_clusters": 4}
        assert res["cluster_labels"].nunique() == 4

    def test__select_keeps_the_whole_grid_and_marks_the_winner(self, stream):
        plain = stream.cluster_analysis_data(
            features=self.FEATURES, method_args={"n_clusters": [2, 3, 4]}
        )
        picked = stream.cluster_analysis_data(
            features=self.FEATURES,
            method_args={"n_clusters": [2, 3, 4]},
            select={"n_clusters": 4},
        )
        assert picked["silhouette"]["params"] == plain["silhouette"]["params"]
        assert picked["silhouette"]["silhouette"] == plain["silhouette"]["silhouette"]
        # The winner is still reported, so the UI can show what was traded away.
        assert picked["silhouette"]["best_index"] == plain["silhouette"]["best_index"]
        assert picked["silhouette"]["selected_index"] == 2

    def test__selecting_the_winner_is_the_same_as_not_selecting(self, stream):
        plain = stream.cluster_analysis_data(
            features=self.FEATURES, method_args={"n_clusters": [2, 3, 4]}
        )
        winner = plain["silhouette"]["params"][plain["silhouette"]["best_index"]]
        picked = stream.cluster_analysis_data(
            features=self.FEATURES, method_args={"n_clusters": [2, 3, 4]}, select=winner
        )
        assert picked["best_params"] == plain["best_params"]
        pd.testing.assert_series_equal(
            picked["cluster_labels"], plain["cluster_labels"]
        )

    def test__best_params_reproduces_the_selection_via_add_clusters(self, stream):
        """best_params is documented as 'pass straight to add_clusters' — with a
        selection in play that has to mean the selected point, or you would save
        a different clustering than the one you read."""
        res = stream.cluster_analysis_data(
            features=self.FEATURES,
            method_args={"n_clusters": [2, 3, 4]},
            select={"n_clusters": 4},
        )
        saved = stream.add_clusters(
            name="c", features=self.FEATURES, **res["best_params"]
        )
        assert len(saved.get_segment_levels()["c"]) == 4

    def test__best_params_round_trips_an_hdbscan_run(self, stream):
        """The round trip has to carry `method` too — without it `add_clusters`
        would silently fall back to its kmeans default."""
        res = stream.cluster_analysis_data(
            features=self.FEATURES,
            method="hdbscan",
            method_args={"min_cluster_size": 2},
        )
        assert res["best_params"]["method"] == "hdbscan"
        saved = stream.add_clusters(
            name="c", features=self.FEATURES, **res["best_params"]
        )
        assert "c" in saved.schema.segment_cols

    def test__best_params_keeps_a_fixed_nmf_step(self, stream):
        """`nmf_components` only becomes a grid coordinate when it is itself
        searched, so a fixed value used to vanish from `best_params` — splatting
        it into `add_clusters` then rebuilt a pipeline without the NMF step."""
        res = stream.cluster_analysis_data(
            features=self.FEATURES,
            method_args={"n_clusters": [2, 3, 4]},
            nmf_components=2,
        )
        assert res["best_params"]["nmf_components"] == 2
        # Still a coordinate-free axis: the grid varies only n_clusters.
        assert all("nmf_components" not in p for p in res["silhouette"]["params"])

        saved = stream.add_clusters(
            name="c", features=self.FEATURES, **res["best_params"]
        )
        assert "c" in saved.schema.segment_cols

    def test__method_args_of_the_other_method_raise(self, stream):
        with pytest.raises(ValueError, match="'kmeans' method"):
            stream.cluster_analysis_data(
                features=self.FEATURES,
                method="hdbscan",
                method_args={"n_clusters": 3},
            )

    def test__unknown_grid_point_raises_and_lists_the_grid(self, stream):
        from retentioneering.exceptions import GridPointNotFoundError

        with pytest.raises(GridPointNotFoundError) as exc:
            stream.cluster_analysis_data(
                features=self.FEATURES,
                method_args={"n_clusters": [2, 3, 4]},
                select={"n_clusters": 9},
            )
        assert "n_clusters" in exc.value.message and "9" in exc.value.message

    def test__select_without_a_grid_is_rejected(self, stream):
        with pytest.raises(ValueError, match="only applies to a grid search"):
            stream.cluster_analysis_data(
                features=self.FEATURES,
                method_args={"n_clusters": 3},
                select={"n_clusters": 3},
            )

    def test__partial_select_is_fine_when_it_picks_out_one_point(self, stream):
        """A grid over two parameters is still addressable by the one you care
        about, as long as only one point matches."""
        res = stream.cluster_analysis_data(
            features=self.FEATURES,
            method_args={"n_clusters": [2, 3]},
            nmf_components=[2],
            select={"n_clusters": 3},
        )
        assert res["best_params"]["method_args"]["n_clusters"] == 3

    def test__partial_select_matching_several_points_is_rejected(self, stream):
        """Silently interpreting whichever match came first would answer a
        question the caller never asked — the two candidates can differ
        arbitrarily, since they were fitted on different NMF projections."""
        from retentioneering.exceptions import AmbiguousGridPointError

        with pytest.raises(AmbiguousGridPointError) as exc:
            stream.cluster_analysis_data(
                features=self.FEATURES,
                method_args={"n_clusters": [2, 3]},
                nmf_components=[2, 3],
                select={"n_clusters": 3},
            )
        assert len(exc.value.matches) == 2
        assert "nmf_components" in exc.value.message
