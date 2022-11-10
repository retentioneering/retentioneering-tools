from __future__ import annotations

import os

import pandas as pd

from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from src.tooling.sankey import Sankey


class TestSankey:
    def test_sankey__simple(self):
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
                [1, "path_end", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event3", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:01:05"],
                [2, "path_end", "2022-01-02 00:01:05"],
                [3, "event1", "2022-01-02 00:01:10"],
                [3, "event3", "2022-01-02 00:02:05"],
                [3, "event4", "2022-01-02 00:03:05"],
                [3, "path_end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        stream = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            schema=EventstreamSchema(),
        )
        s = Sankey(eventstream=stream, max_steps=5)
        _, res_nodes, res_edges, res_data = s._get_plot_data()

        test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)))
        correct_data = pd.read_csv(os.path.join(test_dir, "data/01_simple_test_data.csv"))
        correct_nodes = pd.read_csv(os.path.join(test_dir, "data/01_simple_test_nodes.csv"))
        correct_edges = pd.read_csv(os.path.join(test_dir, "data/01_simple_test_edges.csv"))

        res_nodes = res_nodes.drop("color", axis=1)
        correct_nodes = correct_nodes.drop("color", axis=1)

        assert res_data.compare(correct_data).shape == (0, 0)
        assert res_nodes.compare(correct_nodes).shape == (0, 0)
        assert res_edges.compare(correct_edges).shape == (0, 0)
