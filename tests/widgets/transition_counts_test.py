"""Transition counts in the graph payload (ego view's proba_in/proba_out)."""

import json

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream


def _stream() -> Eventstream:
    # user 1: A B C, user 2: A B B C, user 3: A C
    df = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 2, 2, 2, 2, 3, 3],
            "event": ["A", "B", "C", "A", "B", "B", "C", "A", "C"],
            "timestamp": pd.date_range("2024-01-01", periods=9, freq="1min"),
        }
    )
    return Eventstream(df)


class TestTransitionCountsPayload:
    def test__counts_ride_along_with_any_edge_weight(self):
        widget = _stream().transition_graph(edge_weight="proba_out")
        result = json.loads(widget.result)

        counts = result["counts"]
        assert counts["A"] == {"B": 2, "C": 1}
        assert counts["B"] == {"B": 1, "C": 2}
        # sparse: zero rows/cells are absent
        assert "A" not in counts.get("C", {})

    def test__counts_match_count_weight_matrix(self):
        w_count = _stream().transition_graph(edge_weight="count")
        w_proba = _stream().transition_graph(edge_weight="proba_out")

        assert (
            json.loads(w_count.result)["counts"] == json.loads(w_proba.result)["counts"]
        )

    def test__no_counts_in_diff_mode(self):
        stream = _stream().add_segment(
            "platform", func=lambda df: df["user_id"] % 2 == 0
        )
        widget = stream.transition_graph(diff=["platform", "True", "False"])

        assert "counts" not in json.loads(widget.result)
