import warnings

import pandas as pd
import pytest
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError


class TestTransitionMatrix:
    def test__count(self, fx_read_csv):
        df = fx_read_csv("tools/transition_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        res = stream.transition_graph_data(edge_weight="count")

        expected = pd.DataFrame(
            [
                [0, 2, 1, 0, 0],
                [0, 0, 4, 0, 0],
                [0, 1, 4, 3, 2],
                [0, 1, 1, 1, 1],
                [0, 0, 0, 0, 0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"

        pd.testing.assert_frame_equal(res, expected)

    def test__transition_rate(self, fx_read_csv):
        df = fx_read_csv("tools/transition_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        res = stream.transition_graph_data(edge_weight="share_of_total")

        expected = pd.DataFrame(
            [
                [0, 2, 1, 0, 0],
                [0, 0, 4, 0, 0],
                [0, 1, 4, 3, 2],
                [0, 1, 1, 1, 1],
                [0, 0, 0, 0, 0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"
        expected = expected / 21

        pd.testing.assert_frame_equal(res, expected)

    def test__per_path(self, fx_read_csv):
        df = fx_read_csv("tools/transition_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        res = stream.transition_graph_data(edge_weight="avg_per_path")

        expected = pd.DataFrame(
            [
                [0, 2, 1, 0, 0],
                [0, 0, 4, 0, 0],
                [0, 1, 4, 3, 2],
                [0, 1, 1, 1, 1],
                [0, 0, 0, 0, 0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"
        expected = expected / 3

        pd.testing.assert_frame_equal(res, expected)

    def test__proba_out(self, fx_read_csv):
        df = fx_read_csv("tools/transition_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        res = stream.transition_graph_data(edge_weight="proba_out")

        expected = pd.DataFrame(
            [
                [0.0, 2 / 3, 1 / 3, 0.0, 0.0],
                [0.0, 0, 4 / 4, 0.0, 0.0],
                [0.0, 1 / 10, 4 / 10, 3 / 10, 2 / 10],
                [0.0, 1 / 4, 1 / 4, 1 / 4, 1 / 4],
                [0.0, 0.0, 0.0, 0.0, 0.0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"

        pd.testing.assert_frame_equal(res, expected)

    def test__proba_in(self, fx_read_csv):
        df = fx_read_csv("tools/transition_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        res = stream.transition_graph_data(edge_weight="proba_in")

        expected = pd.DataFrame(
            [
                [0.0, 2 / 4, 1 / 10, 0.0, 0.0],
                [0.0, 0, 4 / 10, 0.0, 0.0],
                [0.0, 1 / 4, 4 / 10, 3 / 4, 2 / 3],
                [0.0, 1 / 4, 1 / 10, 1 / 4, 1 / 3],
                [0.0, 0.0, 0.0, 0.0, 0.0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"

        pd.testing.assert_frame_equal(res, expected)

    def test__path_col_count(self, fx_read_csv):
        df = fx_read_csv("tools/transition_matrix_input.csv", sep="\t")
        stream = Eventstream(df, {"path_cols": ["user_id", "session_id"]})
        res = stream.transition_graph_data(edge_weight="count", path_col="session_id")

        expected = pd.DataFrame(
            [
                [0, 2, 2, 0, 0],
                [0, 0, 4, 0, 0],
                [0, 1, 4, 3, 2],
                [0, 1, 0, 1, 2],
                [0, 0, 0, 0, 0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"

        pd.testing.assert_frame_equal(res, expected)

    def test__path_col_transition_rate(self, fx_read_csv):
        df = fx_read_csv("tools/transition_matrix_input.csv", sep="\t")
        stream = Eventstream(df, {"path_cols": ["user_id", "session_id"]})
        res = stream.transition_graph_data(
            edge_weight="share_of_total", path_col="session_id"
        )

        expected = pd.DataFrame(
            [
                [0, 2, 2, 0, 0],
                [0, 0, 4, 0, 0],
                [0, 1, 4, 3, 2],
                [0, 1, 0, 1, 2],
                [0, 0, 0, 0, 0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"
        expected = expected / 22

        pd.testing.assert_frame_equal(res, expected)

    def test__path_col_per_path(self, fx_read_csv):
        df = fx_read_csv("tools/transition_matrix_input.csv", sep="\t")
        stream = Eventstream(df, {"path_cols": ["user_id", "session_id"]})
        res = stream.transition_graph_data(
            edge_weight="avg_per_path", path_col="session_id"
        )

        expected = pd.DataFrame(
            [
                [0, 2, 2, 0, 0],
                [0, 0, 4, 0, 0],
                [0, 1, 4, 3, 2],
                [0, 1, 0, 1, 2],
                [0, 0, 0, 0, 0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"
        expected = expected / 4

        pd.testing.assert_frame_equal(res, expected)

    def test__path_col_proba_out(self, fx_read_csv):
        df = fx_read_csv("tools/transition_matrix_input.csv", sep="\t")
        stream = Eventstream(df, {"path_cols": ["user_id", "session_id"]})
        res = stream.transition_graph_data(
            edge_weight="proba_out", path_col="session_id"
        )

        expected = pd.DataFrame(
            [
                [0.0, 2 / 4, 2 / 4, 0.0, 0.0],
                [0.0, 0, 4 / 4, 0.0, 0.0],
                [0.0, 1 / 10, 4 / 10, 3 / 10, 2 / 10],
                [0.0, 1 / 4, 0.0, 1 / 4, 2 / 4],
                [0.0, 0.0, 0.0, 0.0, 0.0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"

        pd.testing.assert_frame_equal(res, expected)

    def test__path_col_proba_in(self, fx_read_csv):
        df = fx_read_csv("tools/transition_matrix_input.csv", sep="\t")
        stream = Eventstream(df, {"path_cols": ["user_id", "session_id"]})
        res = stream.transition_graph_data(
            edge_weight="proba_in", path_col="session_id"
        )

        expected = pd.DataFrame(
            [
                [0.0, 2 / 4, 2 / 10, 0.0, 0.0],
                [0.0, 0, 4 / 10, 0.0, 0.0],
                [0.0, 1 / 4, 4 / 10, 3 / 4, 2 / 4],
                [0.0, 1 / 4, 0.0, 1 / 4, 2 / 4],
                [0.0, 0.0, 0.0, 0.0, 0.0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"

        pd.testing.assert_frame_equal(res, expected)

    def test__diff_segments(self, fx_read_csv):
        df = fx_read_csv("tools/transition_matrix_input.csv", sep="\t")
        stream = Eventstream(
            df, {"path_cols": ["user_id", "session_id"], "segment_cols": ["country"]}
        )
        res, _, _ = stream.transition_graph_data(
            edge_weight="count", path_col="session_id", diff=("country", "US", "UK")
        )

        expected = pd.DataFrame(
            [
                [0, 0, 2, 0, 0],
                [0, 0, 0, 0, 0],
                [0, -1, 4, 1, 2],
                [0, 1, 0, -1, 0],
                [0, 0, 0, 0, 0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"

        pd.testing.assert_frame_equal(res, expected)

    def test__diff_user_lists(self, fx_read_csv):
        df = fx_read_csv("tools/transition_matrix_input.csv", sep="\t")
        stream = Eventstream(df, {"path_cols": ["user_id", "session_id"]})
        res, _, _ = stream.transition_graph_data(
            edge_weight="count", diff=(["user_1"], ["user_2", "user_3"])
        )

        expected = pd.DataFrame(
            [
                [0, 0, -1, 0, 0],
                [0, 0, 0, 0, 0],
                [0, -1, 0, 1, 0],
                [0, 1, 1, -1, -1],
                [0, 0, 0, 0, 0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"

        pd.testing.assert_frame_equal(res, expected)

    def test__diff_rest_pools_all_other_segment_values(self) -> None:
        """diff value2="<REST>" must pool every other segment value, not just one."""
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "session_1", "US"],
                ["user_1", "B", "2020-01-01 00:01:00", "session_1", "US"],
                ["user_1", "C", "2020-01-01 00:02:00", "session_1", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "session_2", "UK"],
                ["user_2", "B", "2020-01-01 00:01:00", "session_2", "UK"],
                ["user_3", "A", "2020-01-01 00:00:00", "session_3", "DE"],
                ["user_3", "C", "2020-01-01 00:01:00", "session_3", "DE"],
            ],
            columns=["user_id", "event", "timestamp", "session_id", "country"],
        )
        stream = Eventstream(
            df, {"path_cols": ["session_id"], "segment_cols": ["country"]}
        )
        diff, g1, g2 = stream.transition_graph_data(
            edge_weight="count", path_col="session_id", diff=("country", "US", "<REST>")
        )

        # group1 = US only (session_1)
        expected_g1 = pd.DataFrame(
            [
                [0, 1, 0, 0, 0],
                [0, 0, 1, 0, 0],
                [0, 0, 0, 1, 0],
                [0, 0, 0, 0, 1],
                [0, 0, 0, 0, 0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected_g1.index.name = "event"
        expected_g1.columns.name = "next_event"

        # group2 = <REST> = UK + DE pooled (session_2, session_3)
        expected_g2 = pd.DataFrame(
            [
                [0, 2, 0, 0, 0],
                [0, 0, 1, 1, 0],
                [0, 0, 0, 0, 1],
                [0, 0, 0, 0, 1],
                [0, 0, 0, 0, 0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected_g2.index.name = "event"
        expected_g2.columns.name = "next_event"

        pd.testing.assert_frame_equal(g1, expected_g1)
        pd.testing.assert_frame_equal(g2, expected_g2)
        pd.testing.assert_frame_equal(diff, expected_g1 - expected_g2)

    def test__unique_paths(self, fx_read_csv):
        df = fx_read_csv("tools/transition_matrix_input.csv", sep="\t")
        stream = Eventstream(df, {"path_cols": ["user_id", "session_id"]})
        res = stream.transition_graph_data(
            edge_weight="unique_paths", path_col="session_id"
        )

        expected = pd.DataFrame(
            [
                [0, 2, 2, 0, 0],
                [0, 0, 2, 0, 0],
                [0, 1, 2, 2, 2],
                [0, 1, 0, 1, 2],
                [0, 0, 0, 0, 0],
            ],
            columns=["path_start", "A", "B", "C", "path_end"],
            index=["path_start", "A", "B", "C", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"

        pd.testing.assert_frame_equal(res, expected)

    def test__path_col_override_finer_grain_preserves_chronological_order(self):
        # path_cols must be coarsest-first (validated at Eventstream
        # construction): user_id then session_id. Overriding to session_id (a
        # valid, finer declared path_col) must still reflect real event order.
        df = pd.DataFrame(
            [
                ["U1", "S1", "A", "2024-01-01 10:00:00"],
                ["U1", "S1", "B", "2024-01-01 10:01:00"],
                ["U1", "S2", "C", "2024-01-01 10:02:00"],
                ["U1", "S2", "D", "2024-01-01 10:03:00"],
            ],
            columns=["user_id", "session_id", "event", "timestamp"],
        )
        stream = Eventstream(df, {"path_cols": ["user_id", "session_id"]})
        res = stream.transition_graph_data(edge_weight="count", path_col="session_id")

        # session S1: path_start -> A -> B -> path_end
        # session S2: path_start -> C -> D -> path_end
        expected = pd.DataFrame(
            [
                [0, 1, 0, 1, 0, 0],
                [0, 0, 1, 0, 0, 0],
                [0, 0, 0, 0, 0, 1],
                [0, 0, 0, 0, 1, 0],
                [0, 0, 0, 0, 0, 1],
                [0, 0, 0, 0, 0, 0],
            ],
            columns=["path_start", "A", "B", "C", "D", "path_end"],
            index=["path_start", "A", "B", "C", "D", "path_end"],
        )
        expected.index.name = "event"
        expected.columns.name = "next_event"

        pd.testing.assert_frame_equal(res, expected)

    def test__path_col_override_rejects_undeclared_column(self):
        df = pd.DataFrame(
            [
                ["U1", "A", "2024-01-01 10:00:00"],
                ["U1", "B", "2024-01-01 10:01:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df, {"path_cols": ["user_id"]})
        with pytest.raises(InvalidParameterError):
            stream.transition_graph_data(edge_weight="count", path_col="not_a_path_col")

    def test__time_median(self):
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "session_1", "US"],
                ["user_1", "B", "2020-01-01 00:01:00", "session_1", "US"],
                ["user_1", "A", "2020-01-01 00:02:00", "session_2", "US"],
                ["user_1", "B", "2020-01-01 00:05:00", "session_2", "US"],
                ["user_1", "A", "2020-01-01 00:08:00", "session_2", "US"],
                ["user_1", "B", "2020-01-01 00:16:00", "session_2", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "session_3", "US"],
                ["user_2", "B", "2020-01-01 00:01:00", "session_3", "US"],
                ["user_2", "A", "2020-01-01 00:02:00", "session_3", "US"],
                ["user_2", "B", "2020-01-01 00:03:00", "session_4", "US"],
                ["user_2", "A", "2020-01-01 00:05:00", "session_4", "US"],
                ["user_2", "B", "2020-01-01 00:07:00", "session_4", "US"],
                ["user_3", "B", "2020-01-01 00:00:00", "session_5", "UK"],
                ["user_3", "A", "2020-01-01 00:03:00", "session_5", "UK"],
                ["user_3", "B", "2020-01-01 00:06:00", "session_5", "UK"],
                ["user_4", "B", "2020-01-01 00:09:00", "session_5", "UK"],
                ["user_4", "A", "2020-01-01 00:12:00", "session_5", "UK"],
                ["user_4", "B", "2020-01-01 00:15:00", "session_5", "UK"],
            ],
            columns=["user_id", "event", "timestamp", "session_id", "country"],
        )

        stream = Eventstream(df)
        res = stream.transition_graph_data(edge_weight="time_median")

        expected = pd.DataFrame(
            [
                [None, 0, 0, None],
                [None, None, 150, None],
                [None, 150, None, 0],
                [None, None, None, None],
            ]
        )

        with warnings.catch_warnings():
            warnings.simplefilter(action="ignore", category=FutureWarning)
            expected = pd.to_timedelta(
                expected.stack(future_stack=True), unit="s"
            ).unstack()

        expected.index = pd.Index(["path_start", "A", "B", "path_end"], name="event")
        expected.columns = pd.Index(
            ["path_start", "A", "B", "path_end"], name="next_event"
        )

        pd.testing.assert_frame_equal(res, expected)

    def test__time_median_nat_cells_are_none_after_df_to_list(self):
        """_df_to_list must convert pd.NaT to None, not raise on float(NaT)."""
        from retentioneering.widgets.transition_graph import _df_to_list

        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:02:00"],
                ["user_2", "A", "2020-01-01 00:00:00"],
                ["user_2", "B", "2020-01-01 00:03:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)
        tm = stream.transition_graph_data(edge_weight="time_median")

        rows = _df_to_list(tm)

        events = tm.index.tolist()
        a_idx = events.index("A")
        b_idx = events.index("B")

        # A→B: median of 120 s and 180 s = 150 s
        assert rows[a_idx][b_idx] == pytest.approx(150.0)
        # NaT cells (no such transition) must be None, not NaN or an exception
        for row in rows:
            for v in row:
                assert v is None or isinstance(v, float)
                if isinstance(v, float):
                    assert v == v, "NaN must not appear in output"

    def test__time_q95(self):
        shifts = pd.Series([0] + [1] * 101).cumsum().cumsum()
        timestamps = pd.to_datetime("2020-01-01 00:00:00") + pd.to_timedelta(
            shifts, unit="s"
        )
        df = pd.DataFrame({"timestamp": timestamps})
        df["event"] = "A"
        df["user_id"] = "user_1"

        stream = Eventstream(df)
        res = stream.transition_graph_data(edge_weight="time_q95")

        expected = pd.DataFrame([[None, 0, None], [None, 96, 0], [None, None, None]])

        with warnings.catch_warnings():
            warnings.simplefilter(action="ignore", category=FutureWarning)
            expected = pd.to_timedelta(
                expected.stack(future_stack=True), unit="s"
            ).unstack()

        expected.index = pd.Index(["path_start", "A", "path_end"], name="event")
        expected.columns = pd.Index(["path_start", "A", "path_end"], name="next_event")

        pd.testing.assert_frame_equal(res, expected)
