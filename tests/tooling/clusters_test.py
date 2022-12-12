from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src import datasets
from src.tooling.clusters import Clusters


@pytest.fixture
def stream_simple_shop():
    test_stream = datasets.load_simple_shop()
    return test_stream


class TestClusters:
    def test_clusters__simple_shop_basic(self, stream_simple_shop):
        c = Clusters(eventstream=stream_simple_shop)
        try:
            c.fit(method="kmeans", n_clusters=4, feature_type="tfidf", ngram_range=(1, 1))
        except Exception as e:
            pytest.fail("Runtime error in Clusters.fit.")

        try:
            c.event_dist(cluster_id1=0)
        except Exception as e:
            pytest.fail("Runtime error in Clusters.event_dist.")

        try:
            c.event_dist(cluster_id1=0, cluster_id2=1)
        except Exception as e:
            pytest.fail("Runtime error in Clusters.event_dist.")

        try:
            c.plot()
        except Exception as e:
            pytest.fail("Runtime error in Clusters.plot.")

        try:
            c.filter_cluster(cluster_id=0)
        except Exception as e:
            pytest.fail("Runtime error in Clusters.filter_cluster.")

        try:
            c.extract_features(feature_type="count", ngram_range=(1, 2))
        except Exception as e:
            pytest.fail("Runtime error in Clusters.projection.")

        try:
            c.projection()
        except Exception as e:
            pytest.fail("Runtime error in Clusters.projection.")

        try:
            users = stream_simple_shop.to_dataframe()["user_id"].unique()
            clusters = np.random.choice([0, 1, 2, 3], size=len(users))
            user_clusters = pd.Series(clusters, index=users)
            c.set_clusters(user_clusters)
        except Exception as e:
            pytest.fail("Runtime error in Clusters.set_clusters.")
