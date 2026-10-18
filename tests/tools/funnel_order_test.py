"""Funnels and funnel segments must use the same full event position."""

import pandas as pd
import pytest

from retentioneering import Eventstream


@pytest.mark.parametrize(
    "steps, counts, level",
    [
        (["marker", "purchase"], [1, 1], "purchase"),
        (["purchase", "marker"], [1, 0], "purchase"),
        (["marker", "marker"], [1, 0], "marker"),
        (["purchase", "purchase"], [1, 0], "purchase"),
    ],
)
def test_marker_funnel_requires_strict_full_position(steps, counts, level):
    stream = Eventstream(
        pd.DataFrame(
            [("u1", "purchase", "2026-01-01")],
            columns=["user_id", "event", "timestamp"],
        )
    ).add_events(name="marker", source_event="purchase")
    before = stream.df.copy(deep=True)
    result = stream.funnel_data(steps)
    assert [step["unique_paths"] for step in result["steps"]] == counts
    assert [step["conversion_rate"] for step in result["steps"]] == counts
    segmented = stream.add_segment(name="progress", funnel_events=steps)
    assert segmented.df["progress"].tolist() == [level, level]
    pd.testing.assert_frame_equal(stream.df, before)


def test_funnel_segment_credits_marker_source_pair():
    # Separate regression: a failure of funnel_data must not hide this API's bug.
    stream = Eventstream(
        pd.DataFrame(
            [("u1", "purchase", "2026-01-01")],
            columns=["user_id", "event", "timestamp"],
        )
    ).add_events(name="marker", source_event="purchase")
    result = stream.add_segment(name="progress", funnel_events=["marker", "purchase"])
    assert result.df["progress"].tolist() == ["purchase", "purchase"]


def test_repeated_marker_pairs_advance_to_the_next_index():
    stream = Eventstream(
        pd.DataFrame(
            [("u1", "purchase", "2026-01-01")] * 2,
            columns=["user_id", "event", "timestamp"],
        )
    ).add_events(name="marker", source_event="purchase")
    # Timestamp ties and physical row order must not replace (index, subindex).
    steps = ["marker", "purchase", "marker", "purchase", "marker"]
    result = stream.funnel_data(steps)
    assert [step["unique_paths"] for step in result["steps"]] == [1, 1, 1, 1, 0]
    result = stream.add_segment(name="progress", funnel_events=steps)
    assert result.df["progress"].tolist() == ["purchase"] * 4


def test_funnel_counts_boundaries_sharing_the_raw_event_index():
    stream = Eventstream(
        pd.DataFrame(
            [("u1", "purchase", "2026-01-01")],
            columns=["user_id", "event", "timestamp"],
        )
    ).add_start_end_events()
    steps = ["path_start", "purchase", "path_end"]
    result = stream.funnel_data(steps)
    assert [step["unique_paths"] for step in result["steps"]] == [1, 1, 1]
    result = stream.add_segment(name="progress", funnel_events=steps)
    assert result.df["progress"].tolist() == ["path_end"] * 3


def test_marker_funnel_custom_columns_session_grain_and_diff():
    stream = Eventstream(
        pd.DataFrame(
            [
                ("u1", "s1", "purchase", "2026-01-01 00:00:00"),
                ("u1", "s2", "browse", "2026-01-01 01:00:00"),
                ("u2", "s3", "purchase", "2026-01-01 00:00:00"),
            ],
            columns=["user_id", "session id", 'event "name', "timestamp"],
        ),
        {
            "path_cols": ["user_id", "session id"],
            "event_col": 'event "name',
            "index": "event index",
            "subindex": 'event "subindex',
        },
    ).add_events(name="marker", source_event="purchase")
    steps = ["marker", "purchase"]
    result = stream.funnel_data(steps, path_col="session id")
    assert [step["unique_paths"] for step in result["steps"]] == [2, 2]
    assert result["steps"][1]["conversion_rate"] == pytest.approx(2 / 3)
    assert result["steps"][1]["step_conversion_rate"] == 1
    diff = stream.funnel_data(steps, path_col="session id", diff=(["s1", "s2"], ["s3"]))
    assert diff["steps"][1]["funnel1_conversion_rate"] == 0.5
    assert diff["steps"][1]["funnel2_conversion_rate"] == 1
    segmented = stream.add_segment(
        name="progress", funnel_events=steps, path_col="session id"
    )
    levels = segmented.df.groupby("session id", observed=True)["progress"].first()
    assert levels.to_dict() == {
        "s1": "purchase",
        "s2": "out_of_funnel",
        "s3": "purchase",
    }
