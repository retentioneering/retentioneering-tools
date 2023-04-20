from __future__ import annotations

import pandas as pd
import pytest

from retentioneering.eventstream.types import EventstreamType
from tests.eventstream.tooling.fixtures.clusters import (
    features_tfidf_input,
    test_stream,
)
from tests.eventstream.tooling.fixtures.clusters_corr import (
    cluster_mapping_corr,
    count_corr,
    time_corr,
)


class TestEventstreamClusters:
    def test_clusters_eventstream__simple_features(
        self, test_stream: EventstreamType, count_corr: pd.DataFrame
    ) -> None:
        correct_features = count_corr
        features = test_stream.clusters.extract_features(feature_type="count", ngram_range=(1, 1))
        assert pd.testing.assert_frame_equal(features[correct_features.columns], correct_features) is None

    def test_clusters__cluster_mapping(
        self, test_stream: EventstreamType, features_tfidf_input: pd.DataFrame, cluster_mapping_corr: dict
    ) -> None:
        correct_result = cluster_mapping_corr
        c = test_stream.clusters
        c.fit(method="gmm", n_clusters=2, X=features_tfidf_input)
        result = c.cluster_mapping
        assert result == correct_result

    def test_clusters_eventstream__refit(
        self, test_stream: EventstreamType, count_corr: pd.DataFrame, time_corr: pd.DataFrame
    ) -> None:
        params_1 = {"feature_type": "count", "ngram_range": (1, 1)}

        params_2 = {"feature_type": "time", "ngram_range": (1, 1)}

        correct_res_1 = count_corr
        correct_res_2 = time_corr

        res_1 = test_stream.clusters.extract_features(**params_1)
        res_2 = test_stream.clusters.extract_features(**params_2)

        assert pd.testing.assert_frame_equal(res_1[correct_res_1.columns], correct_res_1) is None, "First calculation"
        assert pd.testing.assert_frame_equal(res_2[correct_res_2.columns], correct_res_2) is None, "Refit"
