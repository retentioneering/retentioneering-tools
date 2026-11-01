"""
The event order contract (ADR-0017): events within a path are ordered by a
unique `(index, subindex)`, the same way everywhere, whatever the physical row
order and however many events share a timestamp.
"""

import numpy as np
import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError, SchemaConfigError

SCHEMA = {
    "path_cols": ["user_id"],
    "event_col": "event",
    "timestamp_col": "timestamp",
}
T0 = pd.Timestamp("2026-01-01")


def events(stream, path=None):
    df = stream.df
    if path is not None:
        df = df[df["user_id"] == path]
    return list(df["event"].astype(str))


def tied_stream(n_paths, pattern=("a", "b", "c")):
    """Every path is `pattern` at one timestamp, then `end` an hour later."""
    rows = []
    for p in range(n_paths):
        t = T0 + pd.Timedelta(seconds=p)
        rows += [[f"u{p}", e, t] for e in pattern]
        rows.append([f"u{p}", "end", t + pd.Timedelta(hours=1)])
    return Eventstream(
        pd.DataFrame(rows, columns=["user_id", "event", "timestamp"]), SCHEMA
    )


def shuffled(stream, seed):
    """The same eventstream with its rows in a random physical order."""
    df = stream.df.sample(frac=1, random_state=seed).reset_index(drop=True)
    return Eventstream(df, stream._schema, preprocess=False)


def layout(stream):
    """Rows in the eventstream's own order, as plain comparable values."""
    df = stream.df.copy()
    for col in df.columns:
        if isinstance(df[col].dtype, pd.CategoricalDtype):
            df[col] = df[col].astype(str)
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Uniqueness of (path, index, subindex)
# ---------------------------------------------------------------------------


class TestUniqueness:
    def test__repeated_user_index_raises(self):
        df = pd.DataFrame(
            {
                "user_id": ["u1", "u1", "u2"],
                "event": ["a", "b", "a"],
                "timestamp": [T0] * 3,
                "index": [1, 1, 1],
            }
        )
        with pytest.raises(SchemaConfigError, match="unique position"):
            Eventstream(df, SCHEMA)

    def test__repeated_user_index_and_subindex_raises(self):
        df = pd.DataFrame(
            {
                "user_id": ["u1", "u1"],
                "event": ["a", "b"],
                "timestamp": [T0] * 2,
                "index": [1, 1],
                "subindex": [1.5, 1.5],
            }
        )
        with pytest.raises(SchemaConfigError, match="unique position"):
            Eventstream(df, SCHEMA)

    def test__shared_index_with_distinct_subindex_is_fine(self):
        df = pd.DataFrame(
            {
                "user_id": ["u1", "u1"],
                "event": ["a", "b"],
                "timestamp": [T0] * 2,
                "index": [1, 1],
                "subindex": [2, 1],
            }
        )
        stream = Eventstream(df, SCHEMA)
        assert events(stream) == ["b", "a"]
        assert stream.df["subindex"].dtype == "float64"

    def test__missing_index_raises(self):
        df = pd.DataFrame(
            {
                "user_id": ["u1", "u1"],
                "event": ["a", "b"],
                "timestamp": [T0] * 2,
                "index": [1, None],
            }
        )
        with pytest.raises(SchemaConfigError, match="missing values"):
            Eventstream(df, SCHEMA)

    def test__derived_stream_with_duplicated_rows_raises(self):
        stream = tied_stream(2)
        with pytest.raises(SchemaConfigError, match="unique position"):
            stream.filter_events(
                sql="SELECT * FROM eventstream UNION ALL "
                "SELECT * FROM eventstream WHERE event = 'a'"
            )

    def test__tied_raw_events_keep_input_order(self):
        df = pd.DataFrame(
            {
                "user_id": ["u1"] * 3,
                "event": ["c", "a", "b"],
                "timestamp": [T0] * 3,
            }
        )
        stream = Eventstream(df, SCHEMA)
        assert events(stream) == ["c", "a", "b"]
        assert stream.df["index"].tolist() == [1, 2, 3]


# ---------------------------------------------------------------------------
# Markers sharing a position
# ---------------------------------------------------------------------------


class TestMarkerSubindex:
    def test__markers_from_one_source_keep_the_order_they_were_added(self):
        stream = (
            tied_stream(3)
            .add_events("x", source_event="a")
            .add_events("y", source_event="a")
            .add_events("x", source_event="a")
        )
        for p in ["u0", "u1", "u2"]:
            assert events(stream, p) == ["x", "y", "x", "a", "b", "c", "end"]
        markers = stream.df[stream.df["event_type"] == "synthetic"]
        assert sorted(set(markers["subindex"].round(6))) == [1.0, 1.5, 0.666667 + 1]

    def test__subindex_values_follow_r_plus_n_over_n_plus_one(self):
        stream = tied_stream(1)
        for _ in range(5):
            stream = stream.add_events("x", source_event="a")
        sub = stream.df[stream.df["event"] == "x"]["subindex"].tolist()
        assert sub == pytest.approx([1 + n / (n + 1) for n in range(5)])
        assert all(s < 2 for s in sub)

    def test__new_marker_goes_after_the_largest_remaining_one(self):
        stream = (
            tied_stream(1)
            .add_events("x", source_event="a")
            .add_events("y", source_event="a")
            .filter_events(drop={"event": ["x"]})
            .add_events("z", source_event="a")
        )
        assert events(stream) == ["y", "z", "a", "b", "c", "end"]

    def test__one_call_marking_several_rows_at_one_position(self):
        stream = (
            tied_stream(1)
            .add_events("x", source_event="a")
            .add_events("m", source_event=["x", "a"])
        )
        assert events(stream) == ["x", "m", "m", "a", "b", "c", "end"]

    def test__churn_markers_stack_after_their_source(self):
        df = pd.DataFrame(
            {
                "user_id": ["u1", "u1", "u1"],
                "event": ["a", "b", "c"],
                "timestamp": [T0, T0, T0 + pd.Timedelta(days=10)],
            }
        )
        stream = (
            Eventstream(df, SCHEMA)
            .add_events("gone", churn={"inactivity_days": 1})
            .add_events("gone2", churn={"inactivity_days": 1})
        )
        # the gap follows `b`; `a` shares its timestamp but not its position
        assert events(stream) == ["a", "b", "gone", "gone2", "c"]
        sub = stream.df[stream.df["event_type"] == "churn"]["subindex"].tolist()
        assert sub == [3.0, 3.5]

    def test__sql_rows_must_carry_an_existing_index(self):
        stream = tied_stream(1)
        with pytest.raises(PreprocessingConfigError, match="matches no event"):
            stream.add_events(
                "x",
                sql="SELECT * REPLACE (index + 100 AS index) "
                "FROM eventstream WHERE event = 'a'",
            )

    def test__sql_rows_take_their_source_position_whatever_the_timestamp(self):
        stream = tied_stream(1).add_events(
            "deadline",
            sql="SELECT * REPLACE (timestamp + INTERVAL 1 DAY AS timestamp) "
            "FROM eventstream WHERE event = 'b'",
        )
        assert events(stream) == ["a", "deadline", "b", "c", "end"]

    def test__repeated_runs_on_a_large_stream_agree(self):
        """
        x, y, x before every `a`, all at one timestamp. Before ADR-0017 the two
        x tied with y in (index, subindex), so each reader saw them in whatever
        order the rows happened to arrive.
        """
        stream = (
            tied_stream(20_000)
            .add_events("x", source_event="a")
            .add_events("y", source_event="a")
            .add_events("x", source_event="a")
        )
        # x, y, x, a, b, c, end: no two neighbours are equal
        collapsed = [len(stream.collapse_events(loops=True).df) for _ in range(3)]
        assert collapsed == [len(stream.df)] * 3

        graphs = [stream.transition_graph_data("unique_paths") for _ in range(3)]
        for g in graphs[1:]:
            pd.testing.assert_frame_equal(g, graphs[0])
        assert graphs[0].loc["x", "y"] == 20_000
        assert graphs[0].loc["y", "x"] == 20_000
        assert graphs[0].loc["x", "a"] == 20_000

        steps = [stream.step_matrix_data(max_steps=6) for _ in range(3)]
        for s in steps[1:]:
            pd.testing.assert_frame_equal(s, steps[0])
        assert steps[0].loc["y", 2] == 1.0


# ---------------------------------------------------------------------------
# Readers that used to order by timestamp
# ---------------------------------------------------------------------------


class TestTiedTimestamps:
    def test__split_sessions_separator_follows_index_order(self):
        """
        a, b, a at one timestamp: the first `a` precedes the separator, so it
        belongs to no session. Ordered by timestamp, `b` was put first and both
        `a` landed in session 1.
        """
        df = pd.DataFrame(
            {
                "user_id": ["u1"] * 4,
                "event": ["a", "b", "a", "c"],
                "timestamp": [T0] * 3 + [T0 + pd.Timedelta(hours=1)],
            }
        )
        res = Eventstream(df, SCHEMA).split_sessions(separator="b")
        out = res.df
        assert list(out["event"].astype(str)) == ["a", "a", "c"]
        assert out["session_index"].isna().tolist() == [True, False, False]
        assert out["session_index"].dropna().tolist() == [1, 1]

    def test__split_sessions_matches_distinct_timestamps(self):
        def run(timestamps):
            df = pd.DataFrame(
                {
                    "user_id": ["u1"] * 4,
                    "event": ["a", "b", "a", "c"],
                    "timestamp": timestamps,
                }
            )
            res = Eventstream(df, SCHEMA).split_sessions(separator="b")
            return res.df["session_index"].fillna(0).tolist()

        tied = run([T0] * 3 + [T0 + pd.Timedelta(hours=1)])
        distinct = run([T0 + pd.Timedelta(minutes=m) for m in range(4)])
        assert tied == distinct == [0, 1, 1]

    def test__collapse_boundary_takes_the_group_first_position(self):
        df = pd.DataFrame(
            {
                "user_id": ["u1"] * 4,
                "event": ["a", "s", "b", "a"],
                "timestamp": [T0] * 4,
            }
        )
        res = Eventstream(df, SCHEMA).collapse_events(
            event_groups=["s", "b"], name="grp"
        )
        assert events(res) == ["a", "grp", "a"]
        assert res.df["index"].tolist() == [1, 2, 4]

    def test__collapse_group_col_index_and_subindex_come_from_one_row(self):
        df = pd.DataFrame(
            {
                "user_id": ["u1"] * 3,
                "event": ["a", "b", "c"],
                "timestamp": [T0] * 3,
                "screen": ["s1", "s1", "s2"],
            }
        )
        stream = Eventstream(df, {**SCHEMA, "custom_cols": ["screen"]})
        stream = stream.add_events("m", source_event="b")
        res = stream.collapse_events(group_col="screen")
        out = res.df
        assert out["index"].tolist() == [1, 3]
        assert out["subindex"].tolist() == [2.0, 2.0]


# ---------------------------------------------------------------------------
# Chains: physical order of intermediate streams must not matter
# ---------------------------------------------------------------------------


def _chain_steps():
    return [
        lambda s: s.add_events("x", source_event="b").add_events("y", source_event="b"),
        lambda s: s.truncate_paths(start_anchor="a", end_anchor="end"),
        lambda s: s.split_sessions(separator="x"),
        lambda s: s.collapse_events(loops=True),
    ]


class TestChainStability:
    def test__chain_is_identical_across_runs_and_physical_orders(self):
        base = tied_stream(500, pattern=("a", "b", "a", "a", "b"))

        reference = base
        for step in _chain_steps():
            reference = step(reference)
        expected = layout(reference)

        for seed in range(3):
            stream = shuffled(base, seed)
            for step in _chain_steps():
                stream = shuffled(step(stream), seed + 100)
            pd.testing.assert_frame_equal(layout(stream), expected)

    def test__physical_order_is_canonical(self):
        stream = shuffled(tied_stream(50).add_events("x", source_event="b"), 7)
        df = stream.df
        key = df[["user_id", "index", "subindex"]]
        assert key.equals(
            key.sort_values(["user_id", "index", "subindex"]).reset_index(drop=True)
        )
        assert not key.duplicated().any()
        assert np.all(np.diff(df.index.to_numpy()) == 1)


# ---------------------------------------------------------------------------
# Where the order is checked
# ---------------------------------------------------------------------------


class _Reversed:
    """A deliberately broken processor: returns every path backwards."""

    def __init__(self, may_break_order):
        self.may_break_order = may_break_order

    def apply(self, df, schema):
        return df.iloc[::-1].reset_index(drop=True), schema


class TestOrderChecks:
    def test__union_all_duplicates_raise(self):
        stream = tied_stream(3)
        with pytest.raises(SchemaConfigError, match="unique position"):
            stream.filter_events(
                sql="select * from eventstream union all select * from eventstream"
            )

    @pytest.mark.parametrize(
        "call",
        [
            lambda s: s.rename_events({"a": "z"}),
            lambda s: s.edit_events(delete=["b"]),
            lambda s: s.filter_events(keep={"event": ["a", "c"]}),
            lambda s: s.filter_events(func=lambda df: df["event"] != "b"),
            lambda s: s.sample_paths(n=2, random_state=1),
            lambda s: s.truncate_paths(start_anchor="b", end_anchor="end"),
            lambda s: s.split_sessions(separator="b"),
        ],
    )
    def test__opted_out_processors_are_not_checked(self, call, monkeypatch):
        from retentioneering.eventstream import ordering

        stream = tied_stream(5)
        monkeypatch.setattr(ordering, "STRICT", False)

        def boom(*args, **kwargs):
            raise AssertionError("order checked for an opted-out processor")

        monkeypatch.setattr(ordering, "is_laid_out", boom)
        monkeypatch.setattr(ordering, "sort_and_validate", boom)
        call(stream)

    def test__checked_processors_are_checked(self, monkeypatch):
        from retentioneering.eventstream import ordering

        stream = tied_stream(5)
        monkeypatch.setattr(ordering, "STRICT", False)
        calls = []
        real = ordering.is_laid_out
        monkeypatch.setattr(
            ordering,
            "is_laid_out",
            lambda df, schema: calls.append(1) or real(df, schema),
        )
        stream.add_events("x", source_event="a")
        stream.collapse_events(loops=True)
        stream.filter_events(sql="select * from eventstream")
        assert len(calls) == 3

    def test__broken_processor_is_resorted_in_production(self, monkeypatch):
        from retentioneering.eventstream import ordering

        monkeypatch.setattr(ordering, "STRICT", False)
        res = tied_stream(3)._derive(_Reversed(may_break_order=True))
        assert events(res, "u0") == ["a", "b", "c", "end"]

    @pytest.mark.parametrize("may_break_order", [True, False])
    def test__test_suite_net_catches_a_broken_processor(self, may_break_order):
        from retentioneering.eventstream.ordering import EventOrderError

        with pytest.raises(EventOrderError):
            tied_stream(3)._derive(_Reversed(may_break_order))

    def test__net_accepts_paths_in_any_relative_order(self):
        class _PathsBackwards:
            may_break_order = False

            def apply(self, df, schema):
                order = df["user_id"].astype(str).rank(method="dense", ascending=False)
                return (
                    df.assign(_o=order)
                    .sort_values("_o", kind="stable")
                    .drop(columns="_o")
                    .reset_index(drop=True),
                    schema,
                )

        res = tied_stream(3)._derive(_PathsBackwards())
        assert list(dict.fromkeys(res.df["user_id"])) == ["u2", "u1", "u0"]

    def test__user_frame_without_preprocess_is_checked_and_sorted(self):
        base = tied_stream(3).df
        res = Eventstream(
            base.iloc[::-1].reset_index(drop=True), SCHEMA, preprocess=False
        )
        assert events(res, "u0") == ["a", "b", "c", "end"]
