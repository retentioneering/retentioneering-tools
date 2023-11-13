from __future__ import annotations

import pandas as pd
import pytest

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.transition_graph import TransitionGraph
from tests.eventstream.tooling.fixtures.transition_graph_input import test_stream


class TestEventstreamTransitionGraph:
    def test_eventstream_transition_graph__simple(self, test_stream: EventstreamType) -> None:
        tg = test_stream.transition_graph()

        assert tg is not None
        assert tg.nodes_weight_col == test_stream.schema.user_id
        assert tg.edges_weight_col == test_stream.schema.user_id
        assert tg.targets == {"negative": None, "positive": None, "source": None}
        assert tg.edges_norm_type is None
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_eventstream_transition_graph__all_args(self, test_stream: EventstreamType) -> None:
        tg = test_stream.transition_graph(
            targets={"negative": "A", "positive": "B", "source": "C"},
            edges_norm_type="node",
            nodes_weight_col="user_id",
            edges_weight_col="session_id",
            edges_threshold={"event_id": 0.5},
            nodes_threshold={"user_id": 0.5},
            show_percents=True,
            show_weights=False,
            show_nodes_names=True,
            show_all_edges_for_targets=False,
            show_nodes_without_links=True,
        )

        assert tg is not None

    def test_eventstream_transition_graph__edges_norm_type(self, test_stream: EventstreamType) -> None:
        tg = test_stream.transition_graph(edges_norm_type="full")

        assert tg is not None
        assert tg.nodes_weight_col == test_stream.schema.user_id
        assert tg.edges_weight_col == test_stream.schema.user_id
        assert tg.targets == {"negative": None, "positive": None, "source": None}
        assert tg.edges_norm_type == "full"
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_eventstream_transition_graph__targets(self, test_stream: EventstreamType) -> None:
        tg = test_stream.transition_graph(targets={"negative": "A", "positive": "B", "source": "C"})

        assert tg is not None
        assert tg.nodes_weight_col == test_stream.schema.user_id
        assert tg.edges_weight_col == test_stream.schema.user_id
        assert tg.targets == {"negative": "A", "positive": "B", "source": "C"}
        assert tg.edges_norm_type is None
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_eventstream_transition_graph__nodes_weight_col(self, test_stream: EventstreamType) -> None:
        tg = test_stream.transition_graph(nodes_weight_col="user_id")

        assert tg is not None
        assert tg.nodes_weight_col == test_stream.schema.user_id
        assert tg.edges_weight_col == test_stream.schema.user_id
        assert tg.targets == {"negative": None, "positive": None, "source": None}
        assert tg.edges_norm_type is None
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_eventstream_transition_graph__edges_weight_col(self, test_stream: EventstreamType) -> None:
        tg = test_stream.transition_graph(edges_weight_col="user_id")

        assert tg is not None
        assert tg.nodes_weight_col == test_stream.schema.user_id
        assert tg.edges_weight_col == test_stream.schema.user_id
        assert tg.targets == {"negative": None, "positive": None, "source": None}
        assert tg.edges_norm_type is None
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_eventstream_transition_graph__percents(self, test_stream: EventstreamType) -> None:
        with pytest.raises(ValueError, match="If show_percents=True, edges_norm_type should be 'full' or 'node'!"):
            test_stream.transition_graph(show_percents=True)

    def test_eventstream_transition_graph__edges_threshold(self, test_stream: EventstreamType) -> None:
        tg = test_stream.transition_graph(edges_threshold={"event_id": 3})

        assert tg.edges_thresholds == {"event_id": 3}
        assert tg is not None
        assert tg.nodes_weight_col == test_stream.schema.user_id
        assert tg.edges_weight_col == test_stream.schema.user_id
        assert tg.targets == {"negative": None, "positive": None, "source": None}
        assert tg.edges_norm_type is None
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_eventstream_transition_graph__nodes_threshold(self, test_stream: EventstreamType) -> None:
        tg = test_stream.transition_graph(nodes_threshold={"user_id": 2})

        assert tg.nodes_thresholds == {"user_id": 2}
        assert tg is not None
        assert tg.nodes_weight_col == test_stream.schema.user_id
        assert tg.edges_weight_col == test_stream.schema.user_id
        assert tg.targets == {"negative": None, "positive": None, "source": None}
        assert tg.edges_norm_type is None
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_eventstream_transition_graph__nodes_weight_col_session(self, test_stream: EventstreamType) -> None:
        tg = test_stream.transition_graph(nodes_weight_col="session_id")

        assert tg is not None
        assert tg.nodes_weight_col == "session_id"
        # @TODO: "session_id" needs its own variable in eventstream.schema
        assert tg.edges_weight_col == test_stream.schema.user_id
        assert tg.targets == {"negative": None, "positive": None, "source": None}
        assert tg.edges_norm_type is None
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_eventstream_transition_graph__edges_weight_col_session(self, test_stream: EventstreamType) -> None:
        tg = test_stream.transition_graph(edges_weight_col="session_id")

        assert tg is not None
        assert tg.nodes_weight_col == test_stream.schema.user_id
        assert tg.edges_weight_col == "session_id"
        # @TODO: "session_id" needs its own variable in eventstream.schema
        assert tg.targets == {"negative": None, "positive": None, "source": None}
        assert tg.edges_norm_type is None
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]

    def test_eventstream_transition_graph__settings(self, test_stream: EventstreamType) -> None:
        tg = test_stream.transition_graph(
            show_percents=True,
            edges_norm_type="full",
            show_weights=False,
            show_nodes_names=True,
            show_all_edges_for_targets=False,
            show_nodes_without_links=True,
        )

        assert tg is not None
        assert tg.nodes_weight_col == test_stream.schema.user_id
        assert tg.edges_weight_col == test_stream.schema.user_id
        assert tg.targets == {"negative": None, "positive": None, "source": None}
        assert tg.edges_norm_type == "full"
        assert tg.weight_cols == ["event_id", "user_id", "session_id"]
