from __future__ import annotations

import pandas as pd
import pytest

from retentioneering.transition_graph import TransitionGraph
from tests.transition_graph.fixtures.transition_corr import (
    full_corr,
    node_corr,
    session_full_corr,
    session_node_corr,
    session_simple_corr,
    simple_corr,
    user_full_corr,
    user_node_corr,
    user_simple_corr,
)
from tests.transition_graph.fixtures.transition_input import test_stream


class TestTransitionGraph:
    def test_transition_graph__simple(self, test_stream, simple_corr):

        tg = TransitionGraph(eventstream=test_stream, graph_settings={})
        result = tg.get_adjacency(weights=None, norm_type=None)
        correct = simple_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def test_transition_graph__norm_full(self, test_stream, full_corr):

        tg = TransitionGraph(eventstream=test_stream, graph_settings={})
        result = tg.get_adjacency(weights=None, norm_type="full").round(3)
        correct = full_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def test_transition_graph__norm_node(self, test_stream, node_corr):

        tg = TransitionGraph(eventstream=test_stream, graph_settings={})
        result = tg.get_adjacency(weights=None, norm_type="node").round(3)
        correct = node_corr

        assert pd.testing.assert_frame_equal(result, correct) is None


class VerifyTransitionGraph:
    def verify_transition_graph__session_simple(self, test_stream, session_simple_corr):

        tg = TransitionGraph(eventstream=test_stream, graph_settings={})
        result = tg.get_adjacency(weights={"edges": "session_id", "nodes": "session_id"}, norm_type=None)
        correct = session_simple_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def verify_transition_graph__session_full(self, test_stream, session_full_corr):

        tg = TransitionGraph(eventstream=test_stream, graph_settings={})
        result = tg.get_adjacency(weights={"edges": "session_id", "nodes": "session_id"}, norm_type="full").round(3)
        correct = session_full_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def verify_transition_graph__session_node(self, test_stream, session_node_corr):

        tg = TransitionGraph(eventstream=test_stream, graph_settings={})
        result = tg.get_adjacency(weights={"edges": "session_id", "nodes": "session_id"}, norm_type="node").round(3)
        correct = session_node_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def verify_transition_graph__user_simple(self, test_stream, user_simple_corr):

        tg = TransitionGraph(eventstream=test_stream, graph_settings={})
        result = tg.get_adjacency(weights={"edges": "user_id", "nodes": "user_id"}, norm_type=None)
        correct = user_simple_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def verify_transition_graph__users_full(self, test_stream, user_full_corr):

        tg = TransitionGraph(eventstream=test_stream, graph_settings={})
        result = tg.get_adjacency(weights={"edges": "user_id", "nodes": "user_id"}, norm_type="full").round(3)
        correct = user_full_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def verify_transition_graph__users_node(self, test_stream, user_node_corr):

        tg = TransitionGraph(eventstream=test_stream, graph_settings={})
        result = tg.get_adjacency(weights={"edges": "user_id", "nodes": "user_id"}, norm_type="node").round(3)
        correct = user_node_corr

        assert pd.testing.assert_frame_equal(result, correct) is None
