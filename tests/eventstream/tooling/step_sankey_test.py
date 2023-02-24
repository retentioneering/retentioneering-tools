from __future__ import annotations

import os

import pandas as pd

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
        res_nodes, res_edges = test_stream.step_sankey(**params, show_plot=False).values
        res_nodes = res_nodes.drop("color", axis=1)

        correct_nodes, correct_edges = correct_res_test("02_threshold_float")

        correct_edges["time_to_next_sum"] = pd.to_timedelta(correct_edges["time_to_next_sum"])

        assert (
            pd.testing.assert_frame_equal(res_nodes[correct_nodes.columns], correct_nodes) is None
        ), "Nodes calculation"
        assert (
            pd.testing.assert_frame_equal(res_edges[correct_edges.columns], correct_edges) is None
        ), "Edges calculation"

    def test_sankey_eventstream__refit(self, test_stream):
        params_1 = {"max_steps": 6, "thresh": 0.25}

        params_2 = {"max_steps": 6, "thresh": 0.25, "target": ["event4"]}
        res_nodes_1, res_edges_1 = test_stream.step_sankey(**params_1, show_plot=False).values
        res_nodes_1 = res_nodes_1.drop("color", axis=1)
        res_nodes_2, res_edges_2 = test_stream.step_sankey(**params_2, show_plot=False).values
        res_nodes_2 = res_nodes_2.drop("color", axis=1)

        correct_nodes_1, correct_edges_1 = correct_res_test("02_threshold_float")
        correct_nodes_2, correct_edges_2 = correct_res_test("04_target")
        correct_edges_1["time_to_next_sum"] = pd.to_timedelta(correct_edges_1["time_to_next_sum"])
        correct_edges_2["time_to_next_sum"] = pd.to_timedelta(correct_edges_2["time_to_next_sum"])

        assert (
            pd.testing.assert_frame_equal(res_nodes_1[correct_nodes_1.columns], correct_nodes_1) is None
        ), "First nodes calculation"
        assert (
            pd.testing.assert_frame_equal(res_edges_1[correct_edges_1.columns], correct_edges_1) is None
        ), "First edges calculation"

        assert (
            pd.testing.assert_frame_equal(res_nodes_2[correct_nodes_2.columns], correct_nodes_2) is None
        ), "Nodes calculation after refit"
        assert (
            pd.testing.assert_frame_equal(res_edges_2[correct_edges_2.columns], correct_edges_2) is None
        ), "Edges calculation after refit"
