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

        # Closed-form embedding + seeded sampling and jitter
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

    def test__related_events_share_a_region(self):
        # Two flows that never meet, each walked in a varying order: their
        # events co-occur only within the flow, so the flows get different
        # top-level cells. (Paths are long enough for the flow, not the
        # path_start/path_end shared by every path, to dominate the context.)
        flows = [["A", "B", "C", "D"], ["W", "X", "Y", "Z"]]
        users, events = [], []
        for user in range(12):
            flow = flows[user % 2]
            path = [flow[(user + step * (1 + user % 3)) % 4] for step in range(10)]
            users += [user] * len(path)
            events += path
        df = pd.DataFrame(
            {
                "user_id": users,
                "event": events,
                "timestamp": pd.date_range(
                    "2024-01-01", periods=len(events), freq="1min"
                ),
            }
        )
        layout = GraphLayout(Eventstream(df)).fit(n_clusters=2)

        # Two top-level clusters stack as a 2x1 grid: the cell is the sign of y.
        cells = [{layout[e]["y"] > 0 for e in flow} for flow in flows]
        assert all(len(cell) == 1 for cell in cells)
        assert cells[0] != cells[1]

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
