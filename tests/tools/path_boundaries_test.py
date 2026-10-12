"""Tools place path boundaries themselves and refuse baked-in ones."""

import warnings

import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.exceptions import PathBoundariesPresentError


def make_stream():
    return Eventstream(
        pd.DataFrame(
            [
                ("u1", "s1", "A"),
                ("u1", "s1", "B"),
                ("u1", "s2", "C"),
                ("u2", "s3", "A"),
            ],
            columns=["user_id", "session_id", "event"],
        ).assign(timestamp=pd.date_range("2026-01-01", periods=4, freq="h")),
        {"path_cols": ["user_id", "session_id"]},
    )


def bounded_stream(path_col=None):
    return make_stream()._build_start_end_events(path_col)


def test_add_start_end_events_is_deprecated():
    with pytest.warns(FutureWarning, match="add_start_end_events") as record:
        make_stream().add_start_end_events()
    # Points at the caller, not at the library's decorators.
    assert record[0].filename == __file__


TOOL_CALLS = {
    "transition_graph_data": lambda s: s.transition_graph_data(),
    "transition_graph_data_session": lambda s: s.transition_graph_data(
        path_col="session_id"
    ),
    "transition_graph_data_diff": lambda s: s.transition_graph_data(
        diff=(["u1"], ["u2"])
    ),
    "step_matrix_data": lambda s: s.step_matrix_data(max_steps=3),
    "step_matrix_data_session": lambda s: s.step_matrix_data(
        max_steps=3, path_col="session_id"
    ),
    "step_matrix_data_diff": lambda s: s.step_matrix_data(
        max_steps=3, diff=(["u1"], ["u2"])
    ),
    "step_matrix_data_anchor": lambda s: s.step_matrix_data(max_steps=3, anchor="B"),
    "step_matrix_data_pattern": lambda s: s.step_matrix_data(
        max_steps=3, path_pattern="A->.*->B"
    ),
    "step_sankey_data": lambda s: s.step_sankey_data(max_steps=3),
    "time_between_metric": lambda s: s.get_metrics(
        [
            {
                "metric": "time_between",
                "metric_args": {"start_event": "path_start", "end_event": "B"},
            }
        ]
    ),
    "transition_graph_widget": lambda s: s.transition_graph(),
    "step_matrix_widget": lambda s: s.step_matrix(max_steps=3),
}


@pytest.mark.parametrize("path_col", [None, "session_id"])
@pytest.mark.parametrize("call", TOOL_CALLS.values(), ids=TOOL_CALLS.keys())
def test_tools_refuse_baked_in_boundaries(call, path_col):
    with pytest.raises(PathBoundariesPresentError, match="add_start_end_events"):
        call(bounded_stream(path_col))


def test_renamed_boundaries_are_still_refused():
    stream = bounded_stream().rename_events({"path_start": "opening"})
    with pytest.raises(PathBoundariesPresentError):
        stream.step_matrix_data(max_steps=3)


@pytest.mark.parametrize("call", TOOL_CALLS.values(), ids=TOOL_CALLS.keys())
def test_tools_work_without_baked_in_boundaries(call):
    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        call(make_stream())


def test_dropping_the_boundaries_makes_the_stream_usable_again():
    stream = bounded_stream().filter_events(
        func=lambda df: ~df["event_type"].isin(["path_start", "path_end"])
    )
    pd.testing.assert_frame_equal(
        stream.step_matrix_data(max_steps=3),
        make_stream().step_matrix_data(max_steps=3),
    )
