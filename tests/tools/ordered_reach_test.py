"""Independent positional scanner, separate from the SQL reach implementation."""

import numpy as np
import pandas as pd
import pytest

from retentioneering import Eventstream, engine
from retentioneering.paths.ordered_reach import ordered_reach, ordered_reach_counts


def fixture():
    rows = []
    for user, sessions in {
        "u1": [["a", "b", "b", "c"]],
        "u2": [["b", "a"], ["b", "c"]],
        "u3": [["c", "a", "b's"]],
    }.items():
        for j, events in enumerate(sessions):
            for k, event in enumerate(events):
                rows.append(
                    (
                        user,
                        f"{user}_{j}",
                        event,
                        pd.Timestamp("2026-01-01")
                        + pd.Timedelta(hours=j, seconds=k // 2),
                    )
                )
    stream = Eventstream(
        pd.DataFrame(rows, columns=["user_id", "session_id", "event", "timestamp"]),
        schema={"path_cols": ["user_id", "session_id"]},
    )
    return stream.add_start_end_events(path_col="session_id")


def scan(stream, steps, path_col):
    ordered = stream.df.sort_values(
        [stream.schema.index, stream.schema.subindex], kind="stable"
    )
    result = {}
    for path, rows in ordered.groupby(path_col):
        cursor = 0
        for event in rows[stream.schema.event_col]:
            if cursor < len(steps) and event == steps[cursor]:
                cursor += 1
        result[path] = [int(j < cursor) for j in range(len(steps))]
    return result


@pytest.mark.parametrize("path", ["user_id", "session_id"])
@pytest.mark.parametrize(
    "steps",
    [
        ["a", "b", "c"],
        ["b", "b"],
        ["c", "a"],
        ["a", "b's"],
        ["path_start", "a", "path_end"],
    ],
)
def test_shared_reach_and_legacy_funnel_against_independent_scanner(path, steps):
    stream = fixture()
    expected = scan(stream, steps, path)
    reach = ordered_reach(stream, steps, path_col=path)
    for key, flags in expected.items():
        np.testing.assert_array_equal(reach.loc[key], flags)
    counts = ordered_reach_counts(stream, steps, path_col=path)
    np.testing.assert_array_equal(counts, np.sum(list(expected.values()), axis=0))
    native = stream.funnel_data(steps=steps, path_col=path)
    assert [r["unique_paths"] for r in native["steps"]] == list(
        np.sum(list(expected.values()), axis=0)
    )


def test_known_absent_step_has_zero_reach():
    stream = fixture()
    reach = ordered_reach(stream, ["a", "never"], event_catalog={"event": ["never"]})
    assert reach.reach_1.sum() > 0
    assert reach.reach_2.sum() == 0


def test_funnel_diff_keeps_zero_step_for_one_group():
    stream = fixture()
    result = stream.funnel_data(steps=["a", "b's"], diff=(["u1"], ["u3"]))
    assert result["steps"][-1]["funnel1_unique_paths"] == 0
    assert result["steps"][-1]["funnel2_unique_paths"] == 1


@pytest.mark.parametrize("extra", [None, ""])
def test_unrelated_null_or_empty_event_does_not_break_legacy_diff(extra):
    frame = pd.DataFrame(
        {
            "user_id": ["a", "a", "a", "b"],
            "event": ["open", "buy", extra, "open"],
            "timestamp": pd.date_range("2026-01-01", periods=4, freq="s"),
        }
    )
    stream = Eventstream(frame)
    result = stream.funnel_data(steps=["open", "buy"], diff=(["a"], ["b"]))
    assert result["steps"][-1]["funnel1_unique_paths"] == 1
    assert result["steps"][-1]["funnel2_unique_paths"] == 0


@pytest.mark.parametrize("n_paths", [1, 100, 10_000])
def test_point_funnel_materializes_only_aggregate_counts(monkeypatch, n_paths):
    """The client output size depends on steps, not on the path population."""
    steps = ["open", "cart", "buy"]
    stream = Eventstream(
        pd.DataFrame(
            {
                "user_id": np.repeat(np.arange(n_paths), len(steps)),
                "event": steps * n_paths,
                "timestamp": pd.Timestamp("2026-01-01"),
            }
        )
    )
    outputs = []
    original = engine.run

    def record_output(sql, **tables):
        result = original(sql, **tables)
        outputs.append(result.shape)
        return result

    monkeypatch.setattr(engine, "run", record_output)
    result = stream.funnel_data(steps=steps)
    assert [row["unique_paths"] for row in result["steps"]] == [n_paths] * len(steps)
    assert outputs == [(1, len(steps))]


def test_aggregate_projection_keeps_zero_counts_for_absent_or_empty_input():
    stream = fixture()
    steps = ["a", "never"]
    catalog = {"event": ["never"]}
    counts = ordered_reach_counts(stream, steps, event_catalog=catalog)
    expected = ordered_reach(stream, steps, event_catalog=catalog).sum()
    np.testing.assert_array_equal(counts, expected)
    assert counts.reach_1 > 0
    assert counts.reach_2 == 0
    empty = ordered_reach_counts(
        stream, steps, frame=stream.df.iloc[:0], event_catalog=catalog
    )
    np.testing.assert_array_equal(empty, [0, 0])


def test_diff_funnel_materializes_only_each_groups_aggregate_counts(monkeypatch):
    stream = fixture()
    outputs = []
    original = engine.run

    def record_reach_output(sql, **tables):
        result = original(sql, **tables)
        if "step_1 AS" in sql:
            outputs.append(result.shape)
        return result

    monkeypatch.setattr(engine, "run", record_reach_output)
    result = stream.funnel_data(steps=["a", "b's"], diff=(["u1"], ["u3"]))
    assert result["steps"][-1]["funnel1_unique_paths"] == 0
    assert result["steps"][-1]["funnel2_unique_paths"] == 1
    assert outputs == [(1, 2), (1, 2)]
