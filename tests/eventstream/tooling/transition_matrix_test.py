from __future__ import annotations

import pandas as pd
import pytest

from retentioneering.eventstream.types import EventstreamType
from tests.eventstream.tooling.fixtures.transition_matrix_corr import (
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
from tests.eventstream.tooling.fixtures.transition_matrix_input import test_stream


class TestEventstreamTransitionMatrix:
    def test_eventstream_transition_matrix__simple(
        self, test_stream: EventstreamType, simple_corr: pd.DataFrame
    ) -> None:
        result = test_stream.transition_matrix(weight_col=test_stream.schema.event_id)
        correct = simple_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def test_eventstream_transition_matrix__norm_full(
        self, test_stream: EventstreamType, full_corr: pd.DataFrame
    ) -> None:
        result = test_stream.transition_matrix(norm_type="full", weight_col=test_stream.schema.event_id)
        correct = full_corr

        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_eventstream_transition_matrix__norm_node(
        self, test_stream: EventstreamType, node_corr: pd.DataFrame
    ) -> None:
        result = test_stream.transition_matrix(norm_type="node", weight_col=test_stream.schema.event_id)
        correct = node_corr

        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_eventstream_transition_matrix__session_simple(
        self, test_stream: EventstreamType, session_simple_corr: pd.DataFrame
    ) -> None:
        result = test_stream.transition_matrix(weight_col="session_id")
        correct = session_simple_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def test_eventstream_transition_matrix__session_full(
        self, test_stream: EventstreamType, session_full_corr: pd.DataFrame
    ) -> None:
        result = test_stream.transition_matrix(weight_col="session_id", norm_type="full")
        correct = session_full_corr

        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_eventstream_transition_matrix__session_node(
        self, test_stream: EventstreamType, session_node_corr: pd.DataFrame
    ) -> None:
        result = test_stream.transition_matrix(weight_col="session_id", norm_type="node")
        correct = session_node_corr

        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_eventstream_transition_matrix__user_simple(
        self, test_stream: EventstreamType, user_simple_corr: pd.DataFrame
    ) -> None:
        result = test_stream.transition_matrix(weight_col="user_id", norm_type=None)
        correct = user_simple_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def test_eventstream_transition_matrix__users_full(
        self, test_stream: EventstreamType, user_full_corr: pd.DataFrame
    ) -> None:
        result = test_stream.transition_matrix(weight_col="user_id", norm_type="full")
        correct = user_full_corr

        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def test_eventstream_transition_matrix__users_node(
        self, test_stream: EventstreamType, user_node_corr: pd.DataFrame
    ) -> None:
        result = test_stream.transition_matrix(weight_col="user_id", norm_type="node")
        correct = user_node_corr

        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None
