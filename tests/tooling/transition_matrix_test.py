from __future__ import annotations

import pandas as pd

from retentioneering.tooling.transition_matrix import TransitionMatrix
from tests.tooling.fixtures.transition_matrix_corr import (
    full_corr,
    node_corr,
    simple_corr,
)
from tests.tooling.fixtures.transition_matrix_input import test_stream


class VerifyTransitionMatrix:
    def test_transition_matrix__simple(self, test_stream: EventstreamType, simple_corr: pd.DataFrame) -> None:

        tm = TransitionMatrix(eventstream=test_stream)
        result = tm.values()  # weights and events are not required parameters
        correct = simple_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def verify_transition_matrix__norm_full(self, test_stream: EventstreamType, full_corr: pd.DataFrame) -> None:

        tm = TransitionMatrix(eventstream=test_stream)
        result = tm.values(norm_type="full")
        correct = full_corr

        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def verify_transition_matrix__norm_node(self, test_stream: EventstreamType, node_corr: pd.DataFrame) -> None:

        tm = TransitionMatrix(eventstream=test_stream)
        result = tm.values(norm_type="node")
        correct = node_corr

        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def verify_transition_matrix__session_simple(
        self, test_stream: EventstreamType, session_simple_corr: pd.DataFrame
    ) -> None:

        tm = TransitionMatrix(eventstream=test_stream)
        result = tm.values(weights="session_id")
        correct = session_simple_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def verify_transition_matrix__session_full(
        self, test_stream: EventstreamType, session_full_corr: pd.DataFrame
    ) -> bool:

        tm = TransitionMatrix(eventstream=test_stream)
        result = tm.values(weights="session_id", norm_type="full")
        correct = session_full_corr

        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def verify_transition_matrix__session_node(
        self, test_stream: EventstreamType, session_node_corr: pd.DataFrame
    ) -> bool:

        tm = TransitionMatrix(eventstream=test_stream)
        result = tm.values(weights="session_id", norm_type="node")
        correct = session_node_corr

        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def verify_transition_matrix__user_simple(
        self, test_stream: EventstreamType, user_simple_corr: pd.DataFrame
    ) -> bool:

        tm = TransitionMatrix(eventstream=test_stream)
        result = tm.values(weights="user_id", norm_type=None)
        correct = user_simple_corr

        assert pd.testing.assert_frame_equal(result, correct) is None

    def verify_transition_matrix__users_full(self, test_stream: EventstreamType, user_full_corr: pd.DataFrame) -> bool:

        tm = TransitionMatrix(eventstream=test_stream)
        result = tm.values(weights="user_id", norm_type="full")
        correct = user_full_corr

        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None

    def verify_transition_matrix__users_node(self, test_stream: EventstreamType, user_node_corr: pd.DataFrame) -> bool:

        tm = TransitionMatrix(eventstream=test_stream)
        result = tm.values(weights="user_id", norm_type="node")
        correct = user_node_corr

        assert pd.testing.assert_frame_equal(result, correct, atol=0.001) is None
