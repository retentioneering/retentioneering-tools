"""`metrics/composite.py`: derived streams whose paths are (path, key) pairs."""

import numpy as np
import pandas as pd
import pytest

from retentioneering.datasets import load_ecom
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.metrics.composite import COMPOSITE_COL, composite_path_stream


@pytest.fixture
def stream() -> Eventstream:
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u1", "u1", "u2", "u2", "u3"],
            "session_id": ["s1", "s1", "s2", "s3", "s3", "s4"],
            "event": ["a", "b", "a", "a", "c", "b"],
            "timestamp": pd.date_range("2024-01-01", periods=6, freq="D"),
            "week": ["w1", "w1", "w2", "w1", None, "w2"],
            "amount": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        }
    )
    return Eventstream(df, {"path_cols": ["user_id", "session_id"]})


def test_pairs_and_back_map(stream) -> None:
    derived, back = composite_path_stream(stream, "week")
    assert derived.schema.path_cols == [COMPOSITE_COL]
    assert derived.schema.path_col == COMPOSITE_COL
    # (u1,w1), (u1,w2), (u2,w1), (u2,None), (u3,w2) in order of first appearance
    assert derived.df[COMPOSITE_COL].tolist() == [0, 0, 1, 2, 3, 4]
    assert back.index.name == COMPOSITE_COL
    assert back["user_id"].tolist() == ["u1", "u1", "u2", "u2", "u3"]
    assert back["key"].tolist() == ["w1", "w2", "w1", None, "w2"]
    assert len(derived.df) == len(stream.df)


def test_metrics_on_the_derived_stream(stream) -> None:
    derived, back = composite_path_stream(stream, stream.df["week"].to_numpy())
    lengths = derived.get_metrics([{"metric": "length"}])
    by_key = lengths.join(back).set_index(["user_id", "key"])["length"]
    assert by_key[("u1", "w1")] == 2 and by_key[("u1", "w2")] == 1


def test_custom_columns_survive(stream) -> None:
    derived, _ = composite_path_stream(stream, "week")
    assert "amount" in derived.schema.custom_cols
    assert derived.schema.segment_cols == stream.schema.segment_cols


def test_finer_grain(stream) -> None:
    derived, back = composite_path_stream(stream, "week", path_col="session_id")
    assert list(back.columns) == ["session_id", "key"]
    assert back["session_id"].tolist() == ["s1", "s2", "s3", "s3", "s4"]


def test_validation(stream) -> None:
    with pytest.raises(InvalidParameterError):
        composite_path_stream(stream, "nope")
    with pytest.raises(InvalidParameterError):
        composite_path_stream(stream, np.array(["w1"]))
    with pytest.raises(InvalidParameterError):
        composite_path_stream(stream, "week", path_col="event")


def test_source_is_untouched(stream) -> None:
    before = stream.df.copy()
    composite_path_stream(stream, "week")
    pd.testing.assert_frame_equal(stream.df, before)


def test_segment_overview_agrees_with_composite() -> None:
    stream = load_ecom()
    overview = stream.segment_overview_data(
        "platform", metrics=[{"metric": "length", "agg": "mean"}]
    )
    derived, back = composite_path_stream(stream, "platform")
    lengths = derived.get_metrics([{"metric": "length"}]).join(back)
    expected = lengths.groupby("key")["length"].mean()
    table = overview if isinstance(overview, pd.DataFrame) else overview["table"]
    row = table.loc[[i for i in table.index if "length" in str(i)][0]]
    for level, value in expected.items():
        assert row[level] == pytest.approx(value)
