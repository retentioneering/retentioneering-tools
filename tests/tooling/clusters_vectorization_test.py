from __future__ import annotations

import pandas as pd

from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from src.tooling.clusters import Clusters


class TestClustersVectorization:
    def test_clusters_vectorization__markov(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event3", "2022-01-01 00:03:30"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event3", "2022-01-01 00:04:30"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event2", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:01:05"],
                [3, "event1", "2022-01-02 00:01:10"],
                [3, "event1", "2022-01-02 00:02:05"],
                [3, "event4", "2022-01-02 00:03:05"],
                [4, "event1", "2022-01-02 00:01:10"],
                [4, "event1", "2022-01-02 00:02:05"],
                [4, "event1", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        stream = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            schema=EventstreamSchema(),
        )

        correct_columns = [
            "event1~event1",
            "event1~event2",
            "event1~event3",
            "event1~event4",
            "event2~event1",
            "event2~event2",
            "event3~event1",
        ]
        correct_columns = [c + "_markov" for c in correct_columns]
        correct_features = pd.DataFrame(
            [
                [0.4, 0.2, 0.4, 0.0, 1.0, 0.0, 1.0],
                [0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0],
                [0.5, 0.0, 0.0, 0.5, 0.0, 0.0, 0.0],
                [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ],
            columns=correct_columns,
            index=[1, 2, 3, 4],
        )

        c = Clusters(eventstream=stream)
        features = c.extract_features(feature_type="markov")
        assert features.compare(correct_features).shape == (0, 0)
