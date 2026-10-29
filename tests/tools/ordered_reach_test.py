"""Independent positional scanner, separate from the SQL reach implementation."""

import numpy as np
import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.paths.ordered_reach import ordered_reach


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
