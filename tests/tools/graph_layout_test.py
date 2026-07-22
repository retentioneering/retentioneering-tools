import pandas as pd
import pytest
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools.graph_layout import GraphLayout


def _linear_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "user_id": [1, 1, 1, 2, 2, 2, 2, 3, 3],
            "event": ["A", "B", "C", "A", "B", "B", "C", "A", "C"],
            "timestamp": pd.date_range("2024-01-01", periods=9, freq="1min"),
        }
    )


class TestGraphLayout:
    def test__covers_all_events(self):
        layout = GraphLayout(Eventstream(_linear_df())).fit()

        # Trajectories include the path boundaries, so the anchors get
        # positions too (the graph always renders them).
        assert set(layout) == {"path_start", "path_end", "A", "B", "C"}
        for pos in layout.values():
            assert pd.notna(pos["x"]) and pd.notna(pos["y"])

    def test__deterministic(self):
        df = _linear_df()
        base = GraphLayout(Eventstream(df)).fit()

        # Fixed seed + single worker + process-independent hashfxn
        for _ in range(3):
            assert GraphLayout(Eventstream(df)).fit() == base

    def test__positions_within_canvas_box(self):
        layout = GraphLayout(Eventstream(_linear_df())).fit()

        for event, pos in layout.items():
            if event in ("path_start", "path_end"):
                continue
            assert -500 <= pos["x"] <= 500
            assert -500 <= pos["y"] <= 500

    def test__anchors_pinned(self):
        layout = GraphLayout(Eventstream(_linear_df())).fit()

        interior = {
            e: p for e, p in layout.items() if e not in ("path_start", "path_end")
        }
        # far left / far right of the content
        assert layout["path_start"]["x"] < min(p["x"] for p in interior.values())
        assert layout["path_end"]["x"] > max(p["x"] for p in interior.values())
        # both at the vertical center of the content
        ys = [p["y"] for p in interior.values()]
        mid_y = (min(ys) + max(ys)) / 2
        assert layout["path_start"]["y"] == mid_y
        assert layout["path_end"]["y"] == mid_y

    def test__random_walks_mode(self):
        layout = GraphLayout(Eventstream(_linear_df())).fit(
            use_original_trajectories=False, sample_size=50, walk_length=10
        )

        assert {"A", "B", "C"} <= set(layout)

    def test__disconnected_components(self):
        df = pd.DataFrame(
            {
                "user_id": [1, 1, 2, 2, 3, 3],
                "event": ["A", "B", "A", "B", "X", "Y"],
                "timestamp": pd.date_range("2024-01-01", periods=6, freq="1min"),
            }
        )
        layout = GraphLayout(Eventstream(df)).fit()

        assert set(layout) == {"path_start", "path_end", "A", "B", "X", "Y"}

    def test__single_event(self):
        df = pd.DataFrame(
            {
                "user_id": [1],
                "event": ["A"],
                "timestamp": [pd.Timestamp("2024-01-01")],
            }
        )
        layout = GraphLayout(Eventstream(df)).fit()

        assert set(layout) == {"path_start", "path_end", "A"}

    def test__invalid_path_col(self):
        with pytest.raises(InvalidParameterError):
            GraphLayout(Eventstream(_linear_df())).fit(path_col="no_such_col")

    def test__n_clusters_affects_layout(self):
        # Regression test: n_clusters used to be computed into `clusters` and
        # then silently ignored by _generate_layout, so it had zero effect on
        # the output regardless of value.
        df = pd.DataFrame(
            {
                "user_id": [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4],
                "event": ["A", "B", "C", "D"] * 4,
                "timestamp": pd.date_range("2024-01-01", periods=16, freq="1min"),
            }
        )
        layout_one_cluster = GraphLayout(Eventstream(df)).fit(n_clusters=1)
        layout_many_clusters = GraphLayout(Eventstream(df)).fit(n_clusters=4)

        assert layout_one_cluster != layout_many_clusters
