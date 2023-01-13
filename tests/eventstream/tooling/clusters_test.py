from __future__ import annotations

import pandas as pd
import pytest

from tests.eventstream.tooling.fixtures.clusters import test_stream
from tests.eventstream.tooling.fixtures.clusters_corr import (
    cluster_mapping_corr,
    count_corr,
    time_corr,
)


class TestEventstreamClusters:
    def test_clusters_eventstream__simple_features(self, test_stream, count_corr):
        correct_features = count_corr
        features = test_stream.clusters.extract_features(feature_type="count", ngram_range=(1, 1))
        assert features.compare(correct_features).shape == (0, 0)

    def test_clusters__cluster_mapping(self, test_stream, cluster_mapping_corr):
        correct_result = cluster_mapping_corr
        c = test_stream.clusters
        c.fit(method="gmm", n_clusters=2, feature_type="tfidf", ngram_range=(1, 1))
        result = c.cluster_mapping
        assert result == correct_result

    def test_clusters_eventstream__refit(self, test_stream, count_corr, time_corr):
        params_1 = {"feature_type": "count", "ngram_range": (1, 1)}

        params_2 = {"feature_type": "time", "ngram_range": (1, 1)}

        correct_res_1 = count_corr
        correct_res_2 = time_corr

        res_1 = test_stream.clusters.extract_features(**params_1)
        res_2 = test_stream.clusters.extract_features(**params_2)

        assert correct_res_1.compare(res_1).shape == (0, 0), "First calculation"
        assert correct_res_2.compare(res_2).shape == (0, 0), "Refit"

    def test_clusters_eventstream__fit_hash_check(self, test_stream):
        params = {"feature_type": "count", "ngram_range": (1, 1)}

        c = test_stream.clusters
        c.fit(method="gmm", n_clusters=2, feature_type="tfidf", ngram_range=(1, 1))
        hash1 = hash(c)
        c.filter_cluster
        hash2 = hash(c)

        assert hash1 == hash2
