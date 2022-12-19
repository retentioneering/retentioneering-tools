from __future__ import annotations

import os

import pandas as pd

from src.tooling.step_sankey import StepSankey
from tests.tooling.fixtures.step_sankey import test_stream


def run_test(stream, test_prefix, **kwargs):
    s = StepSankey(eventstream=stream, **kwargs)
    s.fit()
    res_nodes, res_edges = s.values

    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../datasets/tooling/sankey")
    correct_nodes = pd.read_csv(os.path.join(test_data_dir, f"{test_prefix}_nodes.csv"))
    correct_edges = pd.read_csv(os.path.join(test_data_dir, f"{test_prefix}_edges.csv"))

    res_nodes = res_nodes.drop("color", axis=1)
    correct_nodes = correct_nodes.drop("color", axis=1)

    nodes_are_correct = res_nodes.compare(correct_nodes).shape == (0, 0)
    edges_are_correct = res_edges.compare(correct_edges).shape == (0, 0)

    return nodes_are_correct and edges_are_correct


class TestSankey:
    def test_sankey__simple(self, test_stream):
        assert run_test(test_stream, "01_basic")

    def test_sankey__threshold_float(self, test_stream):
        assert run_test(test_stream, "02_threshold_float", max_steps=6, thresh=0.25)

    def test_sankey__threshold_int(self, test_stream):
        assert run_test(test_stream, "03_threshold_int", max_steps=6, thresh=1)

    def test_sankey__target(self, test_stream):
        assert run_test(test_stream, "04_target", max_steps=6, thresh=0.25, target=["event4"])
