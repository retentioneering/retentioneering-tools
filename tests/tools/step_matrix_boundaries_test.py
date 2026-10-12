import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.exceptions import EmptyEventstreamError


def example_stream():
    return Eventstream(
        pd.DataFrame(
            [("u1", "A"), ("u1", "B"), ("u2", "A")],
            columns=["user_id", "event"],
        ).assign(timestamp=pd.date_range("2026-01-01", periods=3, freq="s"))
    )


@pytest.mark.parametrize("method", ["step_matrix_data", "step_sankey_data"])
@pytest.mark.parametrize("boundaries", ["both", "start", "end"])
def test_regular_matrix_ignores_explicit_boundaries(method, boundaries):
    stream = example_stream().add_start_end_events()
    if boundaries != "both":
        remove = "path_end" if boundaries == "start" else "path_start"
        stream = stream.filter_events(drop={"event_type": [remove]})
    before = stream.df.copy(deep=True)
    actual = getattr(stream, method)(max_steps=4)
    expected = pd.DataFrame(
        [
            [1.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.5, 0.0, 0.0],
            [0.0, 0.0, 0.5, 1.0, 1.0],
        ],
        index=pd.Index(["path_start", "A", "B", "path_end"], name="event"),
        columns=pd.Index(range(5), name="step"),
    )
    pd.testing.assert_frame_equal(
        actual, expected, check_index_type=False, check_categorical=False
    )
    pd.testing.assert_frame_equal(stream.df, before)


def test_regular_matrix_probability_mass_with_explicit_boundaries():
    matrix = example_stream().add_start_end_events().step_matrix_data(max_steps=4)
    assert matrix.sum().tolist() == pytest.approx([1.0] * 5)
    assert (matrix.to_numpy() >= 0).all()


@pytest.mark.parametrize("keep_real_events", [True, False])
def test_boundary_only_paths_remain_in_denominator(keep_real_events):
    stream = example_stream().add_start_end_events()
    stream = stream.filter_events(
        func=lambda df: (df.event_type != "raw")
        | (keep_real_events & (df.user_id == "u1"))
    )
    matrix = stream.step_matrix_data(max_steps=3)
    assert matrix.loc["path_start"].tolist() == [1.0, 0.0, 0.0, 0.0]
    if keep_real_events:
        assert matrix.loc["A", 1] == 0.5
        assert matrix.loc["B", 2] == 0.5
        assert matrix.loc["path_end"].tolist() == [0.0, 0.5, 0.5, 1.0]
    else:
        assert matrix.loc["path_end"].tolist() == [0.0, 1.0, 1.0, 1.0]
    assert matrix.sum().tolist() == pytest.approx([1.0] * 4)


def test_regular_diff_keeps_each_group_normalized():
    stream = example_stream()
    kwargs = {"max_steps": 4, "diff": (["u1"], ["u2"])}
    expected = stream.step_matrix_data(**kwargs)
    actual = stream.add_start_end_events().step_matrix_data(**kwargs)
    for result, reference in zip(actual, expected):
        pd.testing.assert_frame_equal(
            result, reference, check_index_type=False, check_categorical=False
        )
    difference, group1, group2 = actual
    assert group1.sum().tolist() == pytest.approx([1.0] * 5)
    assert group2.sum().tolist() == pytest.approx([1.0] * 5)
    assert difference.sum().tolist() == pytest.approx([0.0] * 5)


@pytest.mark.parametrize("path_col", ["user_id", "session id"])
def test_boundaries_at_finer_grain_and_custom_schema(path_col):
    stream = Eventstream(
        pd.DataFrame(
            [("u1", "s1", "A"), ("u1", "s2", "B"), ("u2", "s3", "A")],
            columns=["user_id", "session id", "event name"],
        ).assign(timestamp=pd.date_range("2026-01-01", periods=3, freq="s")),
        {
            "path_cols": ["user_id", "session id"],
            "event_col": "event name",
            "event_type": 'event "type',
        },
    )
    expected = stream.step_matrix_data(max_steps=3, path_col=path_col)
    actual = stream.add_start_end_events(path_col="session id").step_matrix_data(
        max_steps=3, path_col=path_col
    )
    pd.testing.assert_frame_equal(
        actual, expected, check_index_type=False, check_categorical=False
    )


def test_only_boundary_types_are_removed_not_other_synthetic_events():
    stream = example_stream().add_events(name="marker", source_event="A")
    expected = stream.step_matrix_data(max_steps=4)
    bounded = stream.add_start_end_events().rename_events(
        {"path_start": "opening", "path_end": "closing"}
    )
    actual = bounded.step_matrix_data(max_steps=4)
    pd.testing.assert_frame_equal(
        actual, expected, check_index_type=False, check_categorical=False
    )
    assert actual.loc["marker", 1] == 1.0
    assert "opening" not in actual.index
    assert "closing" not in actual.index


@pytest.mark.parametrize("method", ["step_matrix_data", "step_sankey_data"])
@pytest.mark.parametrize("all_boundaries", [False, True])
def test_diff_with_boundary_only_group(method, all_boundaries):
    stream = example_stream().add_start_end_events()
    stream = stream.filter_events(
        func=lambda df: (df.event_type != "raw")
        | ((not all_boundaries) & (df.user_id == "u1"))
    )
    difference, group1, group2 = getattr(stream, method)(
        max_steps=3, diff=(["u1"], ["u2"])
    )
    assert group2.loc["path_end"].tolist() == [0.0, 1.0, 1.0, 1.0]
    if all_boundaries:
        assert (difference.to_numpy() == 0).all()
    else:
        assert group1.loc["A", 1] == 1.0
        assert difference.loc["path_end", 1] == -1.0
    assert group1.sum().tolist() == pytest.approx([1.0] * 4)
    assert group2.sum().tolist() == pytest.approx([1.0] * 4)
    assert difference.sum().tolist() == pytest.approx([0.0] * 4)


@pytest.mark.parametrize("method", ["step_matrix_data", "step_sankey_data"])
def test_no_paths_still_raises(method):
    stream = example_stream().filter_events(func=lambda df: df.user_id == "absent")
    with pytest.raises(EmptyEventstreamError):
        getattr(stream, method)(max_steps=3)


@pytest.mark.parametrize(
    "kwargs", [{"anchor": "path_start"}, {"path_pattern": "path_start"}]
)
def test_boundary_only_anchor_and_pattern_keep_empty_check(kwargs):
    stream = (
        example_stream()
        .add_start_end_events()
        .filter_events(func=lambda df: df.event_type != "raw")
    )
    with pytest.raises(EmptyEventstreamError):
        stream.step_matrix_data(max_steps=3, **kwargs)


@pytest.mark.parametrize("method", ["step_matrix", "step_sankey"])
@pytest.mark.parametrize("diff", [None, (["u1"], ["u2"])])
def test_boundary_only_widgets_construct(method, diff):
    stream = (
        example_stream()
        .add_start_end_events()
        .filter_events(func=lambda df: df.event_type != "raw")
    )
    widget = getattr(stream, method)(max_steps=3, diff=diff)
    assert widget is not None
