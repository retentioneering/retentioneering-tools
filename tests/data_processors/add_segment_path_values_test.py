"""`add_segment(name, path_values=...)`: one value per path, broadcast to its events."""

import json

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError, PreprocessingConfigError
from retentioneering.mcp.server import _apply_preprocessors


def _stream(paths, sessions=False) -> Eventstream:
    rows = []
    for pid, events in paths.items():
        base = pd.Timestamp("2023-01-01 10:00")
        for i, event in enumerate(events):
            row = {
                "user_id": pid,
                "event": event,
                "timestamp": base + pd.Timedelta(hours=i),
            }
            if sessions:
                row["session_id"] = f"{pid}_s{i // 2}"
            rows.append(row)
    schema = {"path_cols": ["user_id", "session_id"]} if sessions else None
    return Eventstream(pd.DataFrame(rows), schema)


@pytest.fixture
def stream() -> Eventstream:
    return _stream({1: ["a", "b"], 2: ["a"], 3: ["b", "c", "d"]})


def _by_path(stream, name):
    df = stream.df.assign(**{name: stream.df[name].astype(object)})
    return df.groupby("user_id")[name].agg(list).to_dict()


def test_series_is_broadcast(stream) -> None:
    out = stream.add_segment("arm", path_values=pd.Series({1: "t", 2: "c", 3: "t"}))
    assert "arm" in out.schema.segment_cols
    assert isinstance(out.df["arm"].dtype, pd.CategoricalDtype)
    assert _by_path(out, "arm") == {1: ["t", "t"], 2: ["c"], 3: ["t", "t", "t"]}


def test_missing_paths_and_extra_ids(stream) -> None:
    out = stream.add_segment("arm", path_values={1: "t", 99: "c"})
    assert out.df.loc[out.df.user_id == 2, "arm"].isna().all()
    assert out.get_segment_levels()["arm"] == ["t", "<MISSING>"]
    assert out.get_path_segments("arm").to_dict() == {
        1: "t",
        2: "<MISSING>",
        3: "<MISSING>",
    }
    # <MISSING> works as a diff level, like for any other segment
    s1, s2 = out._split_two(("arm", "<MISSING>", "t"))
    assert sorted(s1.df.user_id.unique()) == [2, 3]


def test_categorical_keeps_category_order(stream) -> None:
    values = pd.Series(
        pd.Categorical(["low", "high", "low"], categories=["low", "mid", "high"]),
        index=[1, 2, 3],
    )
    out = stream.add_segment("tier", path_values=values)
    assert out.df["tier"].cat.categories.tolist() == ["low", "mid", "high"]


def test_json_dict_with_string_keys(stream) -> None:
    values = json.loads(json.dumps({1: "t", 2: "c", 3: "t"}))
    assert list(values) == ["1", "2", "3"]
    out = stream.add_segment("arm", path_values=values)
    assert out.get_path_segments("arm").to_dict() == {1: "t", 2: "c", 3: "t"}


def test_non_unique_index_raises(stream) -> None:
    with pytest.raises(InvalidParameterError, match="repeated path id"):
        stream.add_segment("arm", path_values=pd.Series(["t", "c"], index=[1, 1]))


def test_wrong_type_raises(stream) -> None:
    with pytest.raises(InvalidParameterError):
        stream.add_segment("arm", path_values=["t", "c"])


def test_exclusive_with_other_modes(stream) -> None:
    with pytest.raises(PreprocessingConfigError, match="At most one"):
        stream.add_segment("arm", path_values={1: "t"}, time_range=("2023", "2024"))


def test_session_grain() -> None:
    stream = _stream({1: ["a", "b", "c"], 2: ["a", "b"]}, sessions=True)
    out = stream.add_segment(
        "variant",
        path_values={"1_s0": "x", "1_s1": "y", "2_s0": "x"},
        path_col="session_id",
    )
    assert out.get_path_segments("variant").to_dict() == {1: "<MULTI>", 2: "x"}
    per_session = out.get_path_segments("variant", path_col="session_id")
    assert per_session.to_dict() == {"1_s0": "x", "1_s1": "y", "2_s0": "x"}
    with pytest.raises(PreprocessingConfigError, match="path_col"):
        stream.add_segment("v", path_values={1: "x"}, path_col="event")


def test_round_trip_through_get_path_segments(stream) -> None:
    truncated = stream.filter_events(keep={"event": ["a"]}).add_segment(
        "first", path_values={1: "p", 2: "q"}
    )
    labels = truncated.get_path_segments("first")
    out = stream.add_segment("first", path_values=labels)
    assert out.get_path_segments("first").to_dict() == {1: "p", 2: "q", 3: "<MISSING>"}


def test_recipe_and_mcp_step(stream) -> None:
    out = stream.add_segment("arm", path_values={1: "t", 2: "c", 3: "t"})
    assert out.recipe()[-1]["path_values"] == {1: "t", 2: "c", 3: "t"}
    step = json.loads(
        json.dumps(
            {"type": "add_segment", "name": "arm", "path_values": {1: "t", 2: "c"}}
        )
    )
    result = _apply_preprocessors(stream, [step])
    assert result.get_path_segments("arm").to_dict() == {1: "t", 2: "c", 3: "<MISSING>"}


def test_ids_that_match_no_path_raise(stream) -> None:
    # a mapping keyed by another column would leave every path <MISSING>
    with pytest.raises(InvalidParameterError, match="only 0 of 2 path_values ids"):
        stream.add_segment("arm", path_values={"x1": "t", "x2": "c"})


def test_user_ids_at_the_session_grain_raise() -> None:
    sessions = _stream({"u1": ["a", "b", "c"], "u2": ["a"]}, sessions=True)
    users = pd.Series({"u1": "t", "u2": "c"})
    with pytest.raises(InvalidParameterError, match="'session_id'"):
        sessions.add_segment("arm", path_values=users, path_col="session_id")


def test_mostly_unmatched_ids_warn_with_the_counts(stream) -> None:
    values = {1: "t", 2: "c", 97: "t", 98: "c", 99: "t"}
    with pytest.warns(UserWarning, match="only 2 of 5 path_values ids") as record:
        out = stream.add_segment("arm", path_values=values)
    assert {w.filename for w in record} == {__file__}
    assert out.get_path_segments("arm").to_dict() == {1: "t", 2: "c", 3: "<MISSING>"}
