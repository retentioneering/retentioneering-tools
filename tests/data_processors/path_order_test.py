"""One path order for processors and tools (ADR-0018).

Markers from `add_events` share their source event's `index`, and raw events
can share a timestamp. These tests pin what that order then means: every
processor and every tool reads a path by `(index, subindex)`, no two rows of a
path share a position, and supplied positions are checked.
"""

import itertools

import pandas as pd
import pytest

from retentioneering.data_processors.add_events import AddEvents
from retentioneering.data_processors.collapse_events import CollapseEvents
from retentioneering.data_processors.split_sessions import SplitSessions
from retentioneering.data_processors.to_daily_states import ToDailyStates
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError, SchemaConfigError
from tests.path_order import assert_path_order

RANK = EventTypes().get_order()
T0 = "2026-01-01 00:00"


def _stream(rows, **schema) -> Eventstream:
    return Eventstream(
        pd.DataFrame(rows, columns=["user_id", "event", "timestamp"]), schema or None
    )


def _path(stream: Eventstream, path: str = "u") -> list:
    """The events of one path in path order."""
    s = stream.schema
    d = stream.df[stream.df[s.path_col] == path]
    return d.sort_values([s.index, s.subindex], kind="stable")[s.event_col].tolist()


def _orders(df: pd.DataFrame):
    """Every row order of a small frame."""
    for order in itertools.permutations(range(len(df))):
        yield df.iloc[list(order)].reset_index(drop=True)


def _one_result(stream: Eventstream, processor) -> pd.DataFrame:
    """Apply `processor` to every row order of the stream; all must agree."""
    results = []
    for frame in _orders(stream.df):
        out, schema = processor.apply(frame, stream.schema)
        assert_path_order(out, schema)
        results.append(out.reset_index(drop=True))
    for other in results[1:]:
        pd.testing.assert_frame_equal(other, results[0])
    return results[0]


@pytest.fixture
def marked_ties() -> Eventstream:
    """R1: two `A` at one timestamp, each with a marker `M` in front of it."""
    return _stream([("u", "A", T0), ("u", "A", T0)]).add_events(
        name="M", source_event="A"
    )


class TestMarkersAtTiedTimestamps:
    """R1: processors and tools read the same sequence."""

    def test_the_stream_reads_marker_source_marker_source(self, marked_ties):
        assert _path(marked_ties) == ["M", "A", "M", "A"]

    def test_pattern_transitions_and_collapse_agree(self, marked_ties):
        pattern = "M->A->M->A"
        metrics = marked_ties.get_metrics(
            [{"metric": "matches_pattern", "metric_args": {"pattern": pattern}}]
        )
        assert bool(metrics[f"matches_pattern_{pattern}"].iloc[0])

        counts = marked_ties.transition_graph_data(edge_weight="count")
        assert counts.loc["M", "A"] == 2
        assert counts.loc["A", "M"] == 1
        assert counts.loc["A", "A"] == 0

        # The two A are not adjacent, so there is no loop to collapse.
        collapsed = _one_result(marked_ties, CollapseEvents(loops=True))
        assert collapsed["event"].astype(str).tolist() == ["M", "A", "M", "A"]

    def test_collapsing_markers_keeps_them_next_to_their_sources(self, marked_ties):
        out = _one_result(
            marked_ties,
            CollapseEvents(event_groups=["M", "A"], name="visit"),
        )
        assert out["event"].astype(str).tolist() == ["visit"]

    def test_a_run_of_a_column_holds_each_marker_with_its_source(self):
        # Each marker copies its source row's `page`, so in path order the runs
        # are (M A) on p1, then (M A) on p2.
        df = pd.DataFrame(
            [("u", "A", T0, "p1"), ("u", "A", T0, "p2")],
            columns=["user_id", "event", "timestamp", "page"],
        )
        stream = Eventstream(df, {"custom_cols": ["page"]}).add_events(
            name="M", source_event="A"
        )
        out = _one_result(stream, CollapseEvents(group_col="page"))
        assert out["event"].astype(str).tolist() == ["p1", "p2"]
        assert out["index"].tolist() == [1, 2]

    def test_a_separator_is_lifted_to_the_front_of_its_timestamp(self, marked_ties):
        # The documented exception: the separator rule puts both M before both
        # A, so the two A, stored at one instant, fall in one session.
        out = _one_result(marked_ties, SplitSessions(separator="M"))
        assert out["event"].astype(str).tolist() == ["A", "A"]
        assert out["session_index"].tolist() == [1, 1]


class TestRepeatedMarkers:
    """R2: markers added at one event each get a position of their own."""

    @pytest.mark.parametrize("names", [["M1", "M2"], ["M1", "M2", "M3"]])
    def test_markers_stand_in_the_order_they_were_added(self, names):
        stream = _stream([("u", "A", T0), ("u", "B", T0)])
        for name in names:
            stream = stream.add_events(name=name, source_event="A")
        assert _path(stream) == [*names, "A", "B"]
        assert stream.df[stream.df["event"] == "A"]["subindex"].tolist() == [
            RANK["raw"]
        ]
        markers = stream.df[stream.df["event"].isin(names)]
        assert sorted(markers["subindex"]) == [
            RANK["synthetic"] + k for k in range(len(names))
        ]
        assert_path_order(stream.df, stream.schema)

    def test_a_new_marker_gets_the_same_position_in_every_row_order(self):
        stream = (
            _stream([("u", "A", T0), ("u", "A", T0)])
            .add_events(name="M1", source_event="A")
            .add_events(name="M2", source_event="A")
        )
        out = _one_result(stream, AddEvents(name="M3", source_event="A"))
        assert out["event"].astype(str).tolist() == [
            "M1", "M2", "M3", "A", "M1", "M2", "M3", "A",
        ]  # fmt: skip

    def test_churn_markers_follow_their_event_in_call_order(self):
        stream = _stream([("u", "A", T0), ("u", "B", "2026-03-01 00:00")])
        for name in ["gone", "gone_again"]:
            stream = stream.add_events(name=name, churn={"inactivity_days": 30})
        assert _path(stream) == ["A", "gone", "gone_again", "B"]
        churn = stream.df[stream.df["event_type"] == "churn"]
        assert sorted(churn["subindex"]) == [RANK["churn"], RANK["churn"] + 1]

    def test_a_group_of_repeated_markers_is_the_same_in_every_row_order(self):
        stream = (
            _stream([("u", "A", T0), ("u", "B", T0)])
            .add_events(name="M1", source_event="A")
            .add_events(name="M2", source_event="A")
        )
        out = _one_result(stream, CollapseEvents(event_groups=["M2", "A"], name="G"))
        assert out["event"].astype(str).tolist() == ["M1", "G", "B"]

    def test_the_recipe_replays_the_same_positions(self):
        df = pd.DataFrame(
            [("u", "A", T0), ("u", "A", T0), ("v", "A", T0)],
            columns=["user_id", "event", "timestamp"],
        )
        stream = (
            Eventstream(df)
            .add_events(name="M1", source_event="A")
            .add_events(name="M2", source_event="A")
            .add_events(name="gone", churn={"inactivity_days": 1})
        )
        rebuilt = Eventstream.from_recipe(df, stream.recipe())
        assert rebuilt.fingerprint == stream.fingerprint
        pd.testing.assert_frame_equal(rebuilt.df, stream.df)


class TestTiedTimestampsInOtherProcessors:
    def test_churn_follows_the_last_tied_event_of_the_path(self):
        stream = _stream(
            [("u", "A", T0), ("u", "B", T0), ("v", "C", "2026-03-01 00:00")]
        )
        out = _one_result(stream, AddEvents(name="gone", churn={"inactivity_days": 30}))
        u = out[out["user_id"] == "u"]
        assert u["event"].astype(str).tolist() == ["A", "B", "gone"]
        assert u["index"].tolist() == [1, 2, 2]

    def test_a_days_first_and_last_values_follow_the_path(self):
        df = pd.DataFrame(
            [("u", "A", T0, "x"), ("u", "B", T0, "y"), ("u", "C", T0, "z")],
            columns=["user_id", "event", "timestamp", "page"],
        )
        stream = Eventstream(df, {"custom_cols": ["page"]})
        first = _one_result(stream, ToDailyStates(agg={"page": "first"}))
        last = _one_result(stream, ToDailyStates(agg={"page": "last"}))
        assert first["page"].tolist() == ["x"]
        assert last["page"].tolist() == ["z"]
        assert first["subindex"].tolist() == [RANK["collapsed"]]


class TestSuppliedPositions:
    """D3: a frame that brings its own positions must form a path order."""

    @staticmethod
    def _frame(**cols) -> pd.DataFrame:
        df = pd.DataFrame(
            {
                "user_id": ["u", "u", "u"],
                "event": ["A", "B", "C"],
                "timestamp": pd.to_datetime([T0, T0, "2026-01-01 00:01"]),
            }
        )
        for name, values in cols.items():
            df[name] = values
        return df

    def test_the_stream_round_trips_through_to_dataframe(self, marked_ties):
        again = Eventstream(marked_ties.to_dataframe())
        pd.testing.assert_frame_equal(again.df, marked_ties.df)

    def test_supplied_positions_order_tied_rows(self):
        stream = Eventstream(self._frame(index=[2, 1, 3]))
        assert _path(stream) == ["B", "A", "C"]
        assert stream.df["event"].astype(str).tolist() == ["B", "A", "C"]

    @pytest.mark.parametrize(
        "cols, message",
        [
            ({"index": [1, None, 3]}, "empty values"),
            ({"index": [1.0, 2.5, 3.0]}, "whole numbers"),
            ({"index": ["1", "2", "3"]}, "must hold integers"),
            ({"index": [1, 1, 2]}, "share a position"),
            ({"index": [1, 3, 2]}, "earlier timestamp"),
            ({"subindex": [200, None, 200]}, "empty values"),
            ({"event_type": ["raw", "raw", "page_view"]}, "not event types"),
        ],
    )
    def test_bad_positions_raise(self, cols, message):
        with pytest.raises(SchemaConfigError, match=message):
            Eventstream(self._frame(**cols))

    def test_a_reset_index_column_in_time_order_is_a_valid_position(self):
        stream = Eventstream(self._frame().reset_index())
        assert stream.df["index"].tolist() == [0, 1, 2]

    def test_ranks_saved_on_the_old_scale_are_moved_to_the_new_one(self):
        old = self._frame(
            event_type=["synthetic", "raw", "churn"],
            subindex=[1, 2, 3],
            index=[1, 1, 2],
        )
        stream = Eventstream(old)
        assert stream.df["subindex"].tolist() == [
            RANK["synthetic"],
            RANK["raw"],
            RANK["churn"],
        ]

    def test_sql_markers_must_be_rows_of_the_stream(self):
        stream = _stream([("u", "A", T0), ("u", "B", "2026-01-01 00:05")])
        moved = (
            "SELECT * REPLACE (timestamp + INTERVAL 1 MINUTE AS timestamp) "
            "FROM eventstream WHERE event = 'A'"
        )
        with pytest.raises(PreprocessingConfigError, match="must be an event"):
            stream.add_events(name="M", sql=moved)
        unplaced = "SELECT * REPLACE (NULL AS index) FROM eventstream"
        with pytest.raises(PreprocessingConfigError, match="needs the 'index'"):
            stream.add_events(name="M", sql=unplaced)

        marked = stream.add_events(
            name="M", sql="SELECT * FROM eventstream WHERE event = 'B'"
        )
        assert _path(marked) == ["A", "M", "B"]


def test_untied_streams_read_the_same_in_both_orders():
    """Without ties, (timestamp, subindex) and (index, subindex) agree, so
    results on such streams do not change."""
    df = pd.DataFrame(
        [
            ("u", "A", "2026-01-01 00:00"),
            ("u", "B", "2026-01-01 00:01"),
            ("u", "A", "2026-01-01 00:02"),
            ("v", "B", "2026-01-01 00:00"),
            ("v", "B", "2026-01-01 00:03"),
        ],
        columns=["user_id", "event", "timestamp"],
    )
    stream = Eventstream(df).add_events(name="M", source_event="A")
    s = stream.schema
    by_time = stream.df.sort_values(
        [s.path_col, s.timestamp_col, s.subindex], kind="stable"
    ).reset_index(drop=True)
    by_position = stream.df.sort_values(
        [s.path_col, s.index, s.subindex], kind="stable"
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(by_time, by_position)
