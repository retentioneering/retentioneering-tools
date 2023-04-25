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
