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
