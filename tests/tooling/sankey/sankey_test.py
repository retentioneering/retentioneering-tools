from __future__ import annotations

import os

import pandas as pd
import pytest

from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from src.tooling.sankey import Sankey


@pytest.fixture
def stream():
    source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00"],
            [1, "event1", "2022-01-01 00:01:01"],
            [1, "event1", "2022-01-01 00:01:02"],
            [1, "event1", "2022-01-01 00:01:03"],
            [1, "event1", "2022-01-01 00:01:04"],
            [1, "event1", "2022-01-01 00:01:05"],
            [1, "path_end", "2022-01-01 00:01:05"],
            [2, "event1", "2022-01-01 00:01:00"],
            [2, "event2", "2022-01-01 00:01:01"],
            [2, "event1", "2022-01-01 00:01:02"],
            [2, "event2", "2022-01-01 00:01:03"],
            [2, "event1", "2022-01-01 00:01:04"],
            [2, "event2", "2022-01-01 00:01:05"],
            [2, "path_end", "2022-01-01 00:01:05"],
            [3, "event1", "2022-01-01 00:01:00"],
            [3, "event2", "2022-01-01 00:01:01"],
            [3, "event3", "2022-01-01 00:01:02"],
            [3, "event3", "2022-01-01 00:01:03"],
            [3, "event3", "2022-01-01 00:01:04"],
            [3, "event3", "2022-01-01 00:01:05"],
            [3, "path_end", "2022-01-01 00:01:05"],
            [4, "event1", "2022-01-01 00:01:00"],
            [4, "event3", "2022-01-01 00:01:01"],
            [4, "event3", "2022-01-01 00:01:03"],
            [4, "path_end", "2022-01-01 00:01:03"],
            [5, "event1", "2022-01-01 00:01:00"],
            [5, "event3", "2022-01-01 00:01:01"],
            [5, "event4", "2022-01-01 00:01:03"],
            [5, "event5", "2022-01-01 00:01:04"],
            [5, "path_end", "2022-01-01 00:01:04"],
        ],
        columns=["user_id", "event", "timestamp"],
    )

    stream = Eventstream(
        raw_data=source_df,
        raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
        schema=EventstreamSchema(),
    )

    return stream


def run_test(stream, test_prefix, **kwargs):
    s = Sankey(eventstream=stream, **kwargs)
    _, res_nodes, res_edges, _ = s._get_plot_data()

    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)))
    correct_nodes = pd.read_csv(os.path.join(test_dir, f"data/{test_prefix}_nodes.csv"))
    correct_edges = pd.read_csv(os.path.join(test_dir, f"data/{test_prefix}_edges.csv"))

    res_nodes = res_nodes.drop("color", axis=1)
    correct_nodes = correct_nodes.drop("color", axis=1)
    test_is_correct = res_nodes.compare(correct_nodes).shape == (0, 0) and res_edges.compare(correct_edges).shape == (
        0,
        0,
    )

    return test_is_correct


class TestSankey:
    def test_sankey__simple(self, stream):
        assert run_test(stream, "01_basic")

    def test_sankey__threshold_float(self, stream):
        assert run_test(stream, "02_threshold_float", max_steps=6, thresh=0.25)

    def test_sankey__threshold_int(self, stream):
        assert run_test(stream, "03_threshold_int", max_steps=6, thresh=1)

    def test_sankey__target(self, stream):
        assert run_test(stream, "04_target", max_steps=6, thresh=0.25, target=["event4"])
