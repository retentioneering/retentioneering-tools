"""Ordering/dtype regressions for categorical marker concatenation (ADR-0017)."""

import importlib

import numpy as np
import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream

add_events = importlib.import_module("retentioneering.data_processors.add_events")


def reference_combine(df, new, schema):
    """The pre-optimization operation, independent of the insertion algorithm."""
    result = pd.concat([df, new]).sort_values(schema.order_cols, kind="stable")
    result[schema.event_col] = result[schema.event_col].astype("category")
    return result.reset_index(drop=True)


def assert_same_rows(left, right):
    pd.testing.assert_frame_equal(left, right, check_categorical=False)


def stream_fixture(path_dtype, index_dtype="int64"):
    frame = pd.DataFrame(
        {
            "actor": np.repeat([20, 3, 100], 6),
            "action": pd.Categorical(["a", "b", "a", "c", "b", "d"] * 3),
            "time": [pd.Timestamp("2026-01-01")] * 18,
            "position": np.tile([1, 4, 8, 10, 11, 14], 3),
            "detail": pd.array([None, 2, 3, 4, 5, 6] * 3, dtype="Int64"),
        }
    )
    if path_dtype == "category":
        frame["actor"] = pd.Categorical(frame["actor"], categories=[100, 20, 3, 999])
    else:
        frame["actor"] = frame["actor"].astype(path_dtype)
    if index_dtype == "uint64":
        frame["position"] = frame["position"].astype("uint64") + np.uint64(2**63)
    elif index_dtype == "string":
        frame["position"] = frame["position"].map(lambda v: f"p{v:02}")
    else:
        frame["position"] = frame["position"].astype(index_dtype)
    return Eventstream(
        frame,
        {
            "path_cols": ["actor"],
            "event_col": "action",
            "timestamp_col": "time",
            "index": "position",
            "subindex": "minor",
        },
    )


@pytest.mark.parametrize("path_dtype", ["int64", "string", "category"])
@pytest.mark.parametrize(
    "mode,index_dtype",
    [
        (mode, dtype)
        for mode in ["source", "sql", "anchor", "churn"]
        for dtype in ["int64", "uint64", "float64", "string"]
        if mode != "anchor" or dtype != "string"
    ],
)
def test_matches_full_sort_with_ties_repeated_markers_and_custom_schema(
    monkeypatch, path_dtype, index_dtype, mode
):
    stream = stream_fixture(path_dtype, index_dtype)
    if mode == "source":
        call = {"source_event": ["a", "b"]}
    elif mode == "sql":
        # Duplicate source rows; reverse SQL order; timestamps are attributes.
        call = {
            "sql": "SELECT * REPLACE (time + INTERVAL 1 DAY AS time) FROM "
            "(SELECT * FROM eventstream WHERE action = 'a' UNION ALL "
            "SELECT * FROM eventstream WHERE action = 'a') ORDER BY position DESC"
        }
    elif mode == "anchor":
        call = {"anchor": "a"}
    else:
        frame = stream.df.copy()
        frame.loc[frame["action"] == "d", "time"] += pd.Timedelta(days=10)
        stream = Eventstream(frame, stream._schema, preprocess=False)
        call = {"churn": {"inactivity_days": 1}}
    before = stream.df.copy(deep=True)
    actual = stream
    for name in ["x", "y", "x"]:
        actual = actual.add_events(name, **call)
    pd.testing.assert_frame_equal(stream.df, before)
    with monkeypatch.context() as m:
        m.setattr(add_events, "_combine_events", reference_combine)
        expected = stream
        for name in ["x", "y", "x"]:
            expected = expected.add_events(name, **call)
    assert_same_rows(actual.df, expected.df)
    assert not actual.df[["actor", "position", "minor"]].duplicated().any()


@pytest.mark.parametrize("name", ["new", "a", "unused"])
@pytest.mark.parametrize("ordered", [False, True])
def test_categories_preserved_at_concat_without_mutating_source(
    monkeypatch, name, ordered
):
    stream = stream_fixture("int64")
    stream.df["action"] = stream.df["action"].cat.add_categories(["unused"])
    stream.df["action"] = stream.df["action"].cat.reorder_categories(
        ["d", "c", "b", "a", "unused"], ordered=ordered
    )
    before = stream.df.copy(deep=True)
    concat = pd.concat
    seen = []

    def checked_concat(frames, *args, **kwargs):
        if len(frames) == 2 and all("action" in frame for frame in frames):
            for frame in frames:
                assert isinstance(frame["action"].dtype, pd.CategoricalDtype)
            assert frames[0]["action"].dtype == frames[1]["action"].dtype
            seen.append(True)
        return concat(frames, *args, **kwargs)

    monkeypatch.setattr(pd, "concat", checked_concat)
    result = stream.add_events(name, source_event="a")
    assert seen
    assert result.df["action"].cat.ordered == ordered
    assert set(before["action"].cat.categories) <= set(
        result.df["action"].cat.categories
    )
    pd.testing.assert_frame_equal(stream.df, before)


def test_category_code_width_expands_without_string_roundtrip():
    stream = stream_fixture("int64")
    stream.df["action"] = stream.df["action"].cat.add_categories(
        [f"unused{i}" for i in range(122)]
    )
    assert stream.df["action"].cat.codes.dtype == "int8"
    result = stream.add_events("new", source_event="a")
    assert result.df["action"].cat.codes.dtype == "int16"
    assert (result.df["action"] == "new").sum() == 6
    assert stream.df["action"].cat.codes.dtype == "int8"


def test_noncanonical_path_blocks_and_nonrange_dataframe_index(monkeypatch):
    stream = stream_fixture("int64")
    frame = pd.concat([stream.df[stream.df["actor"] == p] for p in [100, 3, 20]])
    frame.index = pd.Index(["duplicate-label"] * len(frame))
    # This layout is valid under ADR-0017, as produced by sample_paths.
    stream = Eventstream(frame, stream._schema, preprocess=False, _order="trusted")
    actual = stream.add_events("x", source_event="a")
    with monkeypatch.context() as m:
        m.setattr(add_events, "_combine_events", reference_combine)
        expected = stream.add_events("x", source_event="a")
    assert_same_rows(actual.df, expected.df)


@pytest.mark.parametrize("fraction", [0, 0.01, 0.1, 0.5, 1])
def test_randomized_addition_matches_reference(monkeypatch, fraction):
    rng = np.random.default_rng(229)
    frame = pd.DataFrame(
        {
            "user_id": np.repeat(np.arange(17), 50),
            "event": np.where(rng.random(850) < fraction, "a", "b"),
            "timestamp": pd.Timestamp("2026-01-01"),
        }
    )
    frame["event"] = pd.Categorical(frame["event"], categories=["a", "b"])
    stream = Eventstream(frame)
    actual = stream.add_events("x", source_event="a")
    if fraction:
        actual = actual.add_events("y", source_event="x")
    with monkeypatch.context() as m:
        m.setattr(add_events, "_combine_events", reference_combine)
        expected = stream.add_events("x", source_event="a")
        if fraction:
            expected = expected.add_events("y", source_event="x")
    assert_same_rows(actual.df, expected.df)
