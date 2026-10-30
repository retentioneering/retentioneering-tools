"""`Eventstream.get_path_segments`: one segment value per path."""

import pandas as pd
import pytest

from retentioneering.datasets import load_ecom
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError


@pytest.fixture
def stream() -> Eventstream:
    df = pd.DataFrame(
        {
            "user_id": ["b", "b", "a", "a", "c", "d", "d"],
            "event": ["x"] * 7,
            "timestamp": pd.date_range("2024-01-01", periods=7, freq="h"),
            "arm": ["t", "t", "c", "c", None, "t", None],
        }
    )
    return Eventstream(df, {"segment_cols": ["arm"]})


def test_values(stream) -> None:
    labels = stream.get_path_segments("arm")
    assert labels.to_dict() == {"a": "c", "b": "t", "c": "<MISSING>", "d": "<MULTI>"}
    assert list(labels.index) == ["a", "b", "c", "d"]
    assert labels.name == "arm" and labels.index.name == "user_id"


def test_validation(stream) -> None:
    with pytest.raises(InvalidParameterError, match="segment_col"):
        stream.get_path_segments("event")
    with pytest.raises(InvalidParameterError, match="path_col"):
        stream.get_path_segments("arm", path_col="event")


def test_ecom_platform_by_grain() -> None:
    stream = load_ecom()
    by_user = stream.get_path_segments("platform")
    assert len(by_user) == stream.df.user_id.nunique()
    assert (by_user == "<MULTI>").sum() == 180
    by_session = stream.get_path_segments("platform", path_col="session_id")
    assert (by_session == "<MULTI>").sum() == 0
    assert len(by_session) == stream.df.session_id.nunique()
