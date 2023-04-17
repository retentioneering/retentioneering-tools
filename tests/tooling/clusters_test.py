from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from retentioneering.eventstream import Eventstream, EventstreamSchema
from retentioneering.tooling.clusters import Clusters
from tests.tooling.fixtures.clusters import custom_X, stream_simple_shop, test_stream
from tests.tooling.fixtures.clusters_corr import (
    X_corr,
    cluster_mapping_corr,
    count_corr,
    gmm_corr,
    kmeans_corr,
    markov_corr,
    ngram_range_corr,
    set_clusters_corr,
    time_corr,
    time_fraction_corr,
)


class TestClusters:
    def test_clusters__simple_shop_no_exceptions(self, stream_simple_shop):
        c = Clusters(eventstream=stream_simple_shop)
        try:
            c.fit(method="kmeans", n_clusters=4, feature_type="tfidf", ngram_range=(1, 1))
        except Exception as e:
            pytest.fail("Runtime error in Clusters.fit. " + str(e))

        try:
            c.diff(cluster_id1=0)
        except Exception as e:
            pytest.fail("Runtime error in Clusters.diff. " + str(e))

        try:
            c.diff(cluster_id1=0, cluster_id2=1)
        except Exception as e:
            pytest.fail("Runtime error in Clusters.diff. " + str(e))

        try:
            c.plot()
        except Exception as e:
            pytest.fail("Runtime error in Clusters.plot. " + str(e))

        try:
            c.filter_cluster(cluster_id=0)
        except Exception as e:
            pytest.fail("Runtime error in Clusters.filter_cluster. " + str(e))

        try:
            c.extract_features(feature_type="count", ngram_range=(1, 2))
        except Exception as e:
            pytest.fail("Runtime error in Clusters.projection. " + str(e))

        try:
            c.projection()
        except Exception as e:
            pytest.fail("Runtime error in Clusters.projection. " + str(e))

        try:
            users = stream_simple_shop.to_dataframe()["user_id"].unique()
            clusters = np.random.choice([0, 1, 2, 3], size=len(users))
            user_clusters = pd.Series(clusters, index=users)
            c.set_clusters(user_clusters)
        except Exception as e:
            pytest.fail("Runtime error in Clusters.set_clusters. " + str(e))

    def test_clusters_vectorization__markov(self, test_stream, markov_corr):
        correct_features = markov_corr
        c = Clusters(eventstream=test_stream)
        features = c.extract_features(feature_type="markov")
        assert pd.testing.assert_frame_equal(features[correct_features.columns], correct_features) is None

    def test_clusters_vectorization__count(self, test_stream, count_corr):
        correct_features = count_corr
        c = Clusters(eventstream=test_stream)
        features = c.extract_features(feature_type="count", ngram_range=(1, 1))
        assert pd.testing.assert_frame_equal(features[correct_features.columns], correct_features) is None

    def test_clusters_vectorization__time(self, test_stream, time_corr):
        correct_features = time_corr
        c = Clusters(eventstream=test_stream)
        features = c.extract_features(feature_type="time", ngram_range=(1, 1))
        assert pd.testing.assert_frame_equal(features[correct_features.columns], correct_features) is None

    def test_clusters_vectorization__time_fraction(self, test_stream, time_fraction_corr):
        correct_features = time_fraction_corr
        c = Clusters(eventstream=test_stream)
        features = round(c.extract_features(feature_type="time_fraction", ngram_range=(1, 1)), 3)
        assert pd.testing.assert_frame_equal(features[correct_features.columns], correct_features) is None

    def test_clusters_method__kmeans_(self, test_stream, kmeans_corr):
        correct_result = kmeans_corr
        c = Clusters(eventstream=test_stream)
        c.fit(method="kmeans", n_clusters=2, feature_type="tfidf", ngram_range=(1, 1))
        result = c.user_clusters
        assert pd.testing.assert_series_equal(result, correct_result, check_dtype=False) is None

    def test_clusters_method__gmm(self, test_stream, gmm_corr):
        correct_result = gmm_corr
        c = Clusters(eventstream=test_stream)
        c.fit(method="gmm", n_clusters=2, feature_type="tfidf", ngram_range=(1, 1))
        result = c.user_clusters
        assert pd.testing.assert_series_equal(result, correct_result, check_dtype=True) is None

    def test_clusters__cluster_mapping(self, test_stream, cluster_mapping_corr):
        correct_result = cluster_mapping_corr
        c = Clusters(eventstream=test_stream)
        c.fit(method="gmm", n_clusters=2, feature_type="tfidf", ngram_range=(1, 1))
        result = c.cluster_mapping
        assert result == correct_result

    def test_clusters__ngram_range(self, test_stream, ngram_range_corr):
        correct_features = ngram_range_corr
        c = Clusters(eventstream=test_stream)
        features = c.extract_features(feature_type="count", ngram_range=(3, 3))
        assert pd.testing.assert_frame_equal(features[correct_features.columns], correct_features) is None

    def test_clusters__set_clusters(self, test_stream, set_clusters_corr):
        correct_result = set_clusters_corr
        c = Clusters(eventstream=test_stream)
        user_clusters = pd.Series([1, 3, 0, 2])
        c.set_clusters(user_clusters)
        result = c.cluster_mapping
        assert result == correct_result

    def test_clusters__X(self, test_stream, custom_X, X_corr):
        correct_result = X_corr
        c = Clusters(eventstream=test_stream)
        c.fit(method="kmeans", n_clusters=2, X=custom_X)
        result = c.user_clusters
        assert pd.testing.assert_series_equal(result, correct_result, check_dtype=False) is None
