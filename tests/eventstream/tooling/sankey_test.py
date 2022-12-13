from __future__ import annotations

import os

import pandas as pd
import pytest

from tests.eventstream.tooling.fixtures.sankey import test_stream


def correct_res_test(test_prefix):

    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/eventstream/tooling/sankey")
    correct_nodes = pd.read_csv(os.path.join(test_data_dir, f"{test_prefix}_nodes.csv"))
    correct_edges = pd.read_csv(os.path.join(test_data_dir, f"{test_prefix}_edges.csv"))
    correct_nodes = correct_nodes.drop("color", axis=1)

    return correct_nodes, correct_edges


class TestEventstreamSankey:
    def test_sankey_eventstream__simple(self, test_stream):
        params = {"max_steps": 6, "thresh": 0.25}
        res_nodes, res_edges = test_stream.step_sankey(**params).values
        res_nodes = res_nodes.drop("color", axis=1)

        correct_nodes, correct_edges = correct_res_test("02_threshold_float")

        assert res_nodes.compare(correct_nodes).shape == (0, 0), "Nodes calculation"
        assert res_edges.compare(correct_edges).shape == (0, 0), "Edges calculation"

    def test_sankey_eventstream__refit(self, test_stream):
        params_1 = {"max_steps": 6, "thresh": 0.25}

        params_2 = {"max_steps": 6, "thresh": 0.25, "target": ["event4"]}
        res_nodes_1, res_edges_1 = test_stream.step_sankey(**params_1).values
        res_nodes_1 = res_nodes_1.drop("color", axis=1)
        res_nodes_2, res_edges_2 = test_stream.step_sankey(**params_2).values
        res_nodes_2 = res_nodes_2.drop("color", axis=1)

        correct_nodes_1, correct_edges_1 = correct_res_test("02_threshold_float")
        correct_nodes_2, correct_edges_2 = correct_res_test("04_target")

        assert res_nodes_1.compare(correct_nodes_1).shape == (0, 0), "First nodes calculation"
        assert res_edges_1.compare(correct_edges_1).shape == (0, 0), "First edges calculation"

        assert res_nodes_2.compare(correct_nodes_2).shape == (0, 0), "Nodes calculation after refit"
        assert res_edges_2.compare(correct_edges_2).shape == (0, 0), "Edges calculation after refit"

    def test_sankey_eventstream__fit_hash_check(self, test_stream):
        params = {}

        cc = test_stream.step_sankey(**params)
        hash1 = hash(cc)
        cc.values
        hash2 = hash(cc)

        assert hash1 == hash2
