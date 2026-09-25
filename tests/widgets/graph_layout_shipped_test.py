"""The transition graph ships its semantic layout with the initial state."""

import json

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream


def _stream() -> Eventstream:
    df = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 2, 2, 2, 2, 3, 3],
            "event": ["A", "B", "C", "A", "B", "B", "C", "A", "C"],
            "timestamp": pd.date_range("2024-01-01", periods=9, freq="1min"),
        }
    )
    return Eventstream(df)


class TestGraphLayoutShipped:
    def test__layout_is_computed_at_construction(self):
        # JS reads this trait on mount instead of calling the graph_layout
        # compute, which under "Run all" waits behind every queued cell.
        widget = _stream().transition_graph()
        shipped = json.loads(widget.graph_layout)

        assert "error" not in shipped
        positions = shipped["result"]
        assert {"A", "B", "C"} <= set(positions)
        for pos in positions.values():
            assert set(pos) >= {"x", "y"}

    def test__shipped_layout_matches_the_compute_tool(self):
        widget = _stream().transition_graph()

        assert json.loads(widget.graph_layout) == widget.dispatch_compute(
            "graph_layout", {}
        )

    def test__layout_is_not_persisted(self):
        assert "graph_layout" not in _stream().transition_graph()._persist_names
