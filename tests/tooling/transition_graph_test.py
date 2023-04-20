from __future__ import annotations

import pandas as pd
import pytest

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.transition_graph import TransitionGraph
from tests.tooling.fixtures.transition_graph_input import test_stream


class TestTransitionGraph:
    def test_transition_graph__simple(self, test_stream: EventstreamType) -> None:
        tg = TransitionGraph(eventstream=test_stream)
        tg.plot()

        assert tg is not None
        assert tg.nodes_weight_col == "event_id"
        assert tg.edges_weight_col == "event_id"
        assert tg.targets == {"negative": None, "positive": None, "source": None}
        assert tg.edges_norm_type is None
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_transition_graph__edges_norm_type(self, test_stream: EventstreamType) -> None:
        tg = TransitionGraph(eventstream=test_stream)
        tg.plot(edges_norm_type="full")

        assert tg is not None
        assert tg.nodes_weight_col == "event_id"
        assert tg.edges_weight_col == "event_id"
        assert tg.targets == {"negative": None, "positive": None, "source": None}
        assert tg.edges_norm_type == "full"
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_transition_graph__targets(self, test_stream: EventstreamType) -> None:
        tg = TransitionGraph(eventstream=test_stream)
        tg.plot(targets={"negative": "A", "positive": "B", "source": "C"})

        assert tg is not None
        assert tg.nodes_weight_col == "event_id"
        assert tg.edges_weight_col == "event_id"
        assert tg.targets == {"negative": "A", "positive": "B", "source": "C"}
        assert tg.edges_norm_type is None
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_transition_graph__nodes_weight_col(self, test_stream: EventstreamType) -> None:
        tg = TransitionGraph(eventstream=test_stream)
        tg.plot(nodes_weight_col="user_id")

        assert tg is not None
        assert tg.nodes_weight_col == "user_id"
        assert tg.edges_weight_col == "event_id"
        assert tg.targets == {"negative": None, "positive": None, "source": None}
        assert tg.edges_norm_type is None
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_transition_graph__edges_weight_col(self, test_stream: EventstreamType) -> None:
        tg = TransitionGraph(eventstream=test_stream)
        tg.plot(edges_weight_col="user_id")

        assert tg is not None
        assert tg.nodes_weight_col == "event_id"
        assert tg.edges_weight_col == "user_id"
        assert tg.targets == {"negative": None, "positive": None, "source": None}
        assert tg.edges_norm_type is None
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_transition_graph__percents(self, test_stream: EventstreamType) -> None:
        tg = TransitionGraph(eventstream=test_stream)
        with pytest.raises(ValueError, match="If show_percents=True, edges_norm_type should be 'full' or 'node'!"):
            tg.plot(show_percents=True)

    def test_transition_graph__edges_threshold(self, test_stream: EventstreamType) -> None:
        tg = TransitionGraph(eventstream=test_stream)
        tg.plot(edges_threshold={"event_id": 3})

        assert tg.edges_thresholds == {"event_id": 3}

    def test_transition_graph__nodes_threshold(self, test_stream: EventstreamType) -> None:
        tg = TransitionGraph(eventstream=test_stream)
        tg.plot(nodes_threshold={"user_id": 2})

        assert tg.nodes_thresholds == {"user_id": 2}
