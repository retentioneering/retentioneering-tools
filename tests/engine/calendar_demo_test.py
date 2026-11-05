"""Replay complete demo paths and retain calendar semantics through CSV export."""

import datetime as dt

import pandas as pd
import pytest

from scripts.generate_calendar_cases import (
    CalendarCase,
    expected_calendar,
    export_case,
    generate_cases,
    load_case,
    validate_case,
)
from tests.engine.calendar_offset_test import session_timezone  # noqa: F401


@pytest.fixture(scope="module")
def cases():
    return generate_cases(n_users=3)


@pytest.mark.usefixtures("session_timezone")
@pytest.mark.parametrize("case_index", range(10))
def test_demo_calendar_and_export(cases, case_index, tmp_path):
    case = cases[case_index]
    before = case.frame.copy(deep=True)
    result = validate_case(case)
    restored = load_case(export_case(case, tmp_path))
    assert validate_case(restored) == result
    ts = case.schema["timestamp_col"]
    pd.testing.assert_series_equal(restored.frame[ts], case.frame[ts])
    assert str(restored.frame[ts].dt.tz) == str(case.frame[ts].dt.tz)
    assert restored.schema == case.schema
    assert restored.provenance == case.provenance
    pd.testing.assert_frame_equal(case.frame, before)


def test_replay_preserves_complete_paths_and_intervals(cases):
    original = cases[0]
    ts, path = original.schema["timestamp_col"], original.schema["path_cols"][0]
    elapsed = original.frame.groupby(path, observed=True)[ts].diff().dt.as_unit("ns")
    for case in cases[1:]:
        pd.testing.assert_frame_equal(
            case.frame.drop(columns=ts), original.frame.drop(columns=ts)
        )
        pd.testing.assert_series_equal(
            case.frame.groupby(path, observed=True)[ts].diff().dt.as_unit("ns"),
            elapsed,
        )
    repeated = generate_cases(n_users=3)
    for first, second in zip(cases, repeated):
        pd.testing.assert_frame_equal(first.frame, second.frame)
        assert first.provenance == second.provenance


def test_oracle_covers_lifecycle_thresholds():
    start = dt.date(2024, 1, 1)
    frame = pd.DataFrame(
        {
            "user": ["u"] * 4,
            "ts": pd.to_datetime(
                [start + dt.timedelta(days=d) for d in [0, 7, 37, 69]]
            ),
        }
    )
    case = CalendarCase(
        "thresholds", frame, {"path_cols": ["user"], "timestamp_col": "ts"}, {}
    )
    states = expected_calendar(case).set_index("day")["state"]
    for day, state in {
        0: "new",
        7: "current",
        14: "at_risk_wau",
        15: "at_risk_mau",
        37: "reactivated",
        67: "at_risk_mau",
        68: "dormant",
        69: "resurrected",
    }.items():
        assert states[start + dt.timedelta(days=day)] == state
