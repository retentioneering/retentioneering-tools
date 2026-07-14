import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError


SCHEMA = {
    "path_cols": ["user_id"],
    "event_cols": ["event"],
    "timestamp_col": "timestamp",
}


def make_stream(rows):
    df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp"])
    return Eventstream(df, SCHEMA)


def sessions(stream):
    """Return (event, session_index, session_id) for each row."""
    df = stream.df
    return list(zip(df["event"].astype(str), df["session_index"], df["session_id"]))


# ---------------------------------------------------------------------------
# Separator mode
# ---------------------------------------------------------------------------


class TestSplitSessionsSeparator:
    def test_separator_basic(self):
        """Separator event starts a new session and is removed from output."""
        stream = make_stream(
            [
                ["user_1", "sep", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
                ["user_1", "B", "2020-01-01 00:02:00"],
            ]
        )
        res = stream.split_sessions(separator="sep")
        df = res.df

        assert "sep" not in df["event"].values
        assert list(df["event"].astype(str)) == ["A", "B"]
        assert list(df["session_index"]) == [1, 1]
        assert list(df["session_id"].astype(str)) == ["user_1_1", "user_1_1"]

    def test_separator_multiple_sessions(self):
        """Each separator starts a new session; session_index increments correctly."""
        stream = make_stream(
            [
                ["user_1", "sep", "2020-01-01 00:00:00"],
                ["user_1", "event_1", "2020-01-01 00:01:00"],
                ["user_1", "event_2", "2020-01-01 00:02:00"],
                ["user_1", "sep", "2020-01-01 00:03:00"],
                ["user_1", "event_3", "2020-01-01 00:04:00"],
            ]
        )
        res = stream.split_sessions(separator="sep")
        df = res.df

        assert list(df["event"].astype(str)) == ["event_1", "event_2", "event_3"]
        assert list(df["session_index"]) == [1, 1, 2]
        assert list(df["session_id"].astype(str)) == [
            "user_1_1",
            "user_1_1",
            "user_1_2",
        ]

    def test_separator_same_timestamp_as_regular_event(self):
        """Separator at same timestamp as a regular event: separator starts the new session."""
        stream = make_stream(
            [
                ["user_1", "sep", "2020-01-01 00:00:00"],
                ["user_1", "event_1", "2020-01-01 00:00:00"],
                ["user_1", "event_2", "2020-01-01 00:01:00"],
                ["user_1", "sep", "2020-01-01 00:02:00"],
                ["user_1", "event_3", "2020-01-01 00:02:00"],  # same ts as second sep
            ]
        )
        res = stream.split_sessions(separator="sep")
        df = res.df

        assert list(df["event"].astype(str)) == ["event_1", "event_2", "event_3"]
        assert list(df["session_index"]) == [1, 1, 2]

    def test_separator_last_segment_has_session_index(self):
        """Events after the last separator (no following separator) get a valid session_index."""
        stream = make_stream(
            [
                ["user_1", "sep", "2020-01-01 00:00:00"],
                ["user_1", "event_1", "2020-01-01 00:01:00"],
                ["user_1", "sep", "2020-01-01 00:02:00"],
                ["user_1", "event_2", "2020-01-01 00:03:00"],
                ["user_1", "event_3", "2020-01-01 00:04:00"],
            ]
        )
        res = stream.split_sessions(separator="sep")
        df = res.df

        assert list(df["session_index"]) == [1, 2, 2]

    def test_separator_events_before_first_separator_have_no_session(self):
        """Events before the first separator are kept but have session_index = None."""
        stream = make_stream(
            [
                ["user_1", "event_1", "2020-01-01 00:00:00"],
                ["user_1", "sep", "2020-01-01 00:01:00"],
                ["user_1", "event_2", "2020-01-01 00:02:00"],
            ]
        )
        res = stream.split_sessions(separator="sep")
        df = res.df

        assert list(df["event"].astype(str)) == ["event_1", "event_2"]
        assert pd.isna(df.loc[df["event"] == "event_1", "session_index"].iloc[0])
        assert df.loc[df["event"] == "event_2", "session_index"].iloc[0] == 1

    def test_separator_multiple_users_independent(self):
        """Sessions are counted independently per user."""
        stream = make_stream(
            [
                ["user_1", "sep", "2020-01-01 00:00:00"],
                ["user_1", "event_1", "2020-01-01 00:00:00"],
                ["user_1", "event_2", "2020-01-01 00:01:00"],
                ["user_1", "sep", "2020-01-01 00:02:00"],
                ["user_1", "event_3", "2020-01-01 00:02:00"],
                ["user_2", "sep", "2020-01-01 00:00:00"],
                ["user_2", "event_1", "2020-01-01 00:00:00"],
                ["user_2", "event_2", "2020-01-01 00:01:00"],
                ["user_2", "event_3", "2020-01-01 00:02:00"],
            ]
        )
        res = stream.split_sessions(separator="sep")
        df = res.df

        u1 = df[df["user_id"] == "user_1"]["session_index"].tolist()
        u2 = df[df["user_id"] == "user_2"]["session_index"].tolist()
        assert u1 == [1, 1, 2]
        assert u2 == [1, 1, 1]


# ---------------------------------------------------------------------------
# Start / End mode
# ---------------------------------------------------------------------------


class TestSplitSessionsStartEnd:
    def test_start_end_basic(self):
        """Events between start/end are in session; start and end events are deleted."""
        stream = make_stream(
            [
                ["user_1", "start", "2020-01-01 00:00:00"],
                ["user_1", "event_1", "2020-01-01 00:01:00"],
                ["user_1", "event_2", "2020-01-01 00:02:00"],
                ["user_1", "end", "2020-01-01 00:03:00"],
            ]
        )
        res = stream.split_sessions(start_event="start", end_event="end")
        df = res.df

        assert "start" not in df["event"].values
        assert "end" not in df["event"].values
        assert list(df["event"].astype(str)) == ["event_1", "event_2"]
        assert list(df["session_index"]) == [1, 1]

    def test_start_end_multiple_sessions(self):
        """Second start/end pair receives session_index=2."""
        stream = make_stream(
            [
                ["user_1", "start", "2020-01-01 00:00:00"],
                ["user_1", "event_1", "2020-01-01 00:00:00"],
                ["user_1", "event_2", "2020-01-01 00:01:00"],
                ["user_1", "end", "2020-01-01 00:01:00"],
                ["user_1", "start", "2020-01-01 00:02:00"],
                ["user_1", "event_3", "2020-01-01 00:02:00"],
                ["user_1", "end", "2020-01-01 00:02:00"],
            ]
        )
        res = stream.split_sessions(start_event="start", end_event="end")
        df = res.df

        assert list(df["event"].astype(str)) == ["event_1", "event_2", "event_3"]
        assert list(df["session_index"]) == [1, 1, 2]
        assert list(df["session_id"].astype(str)) == [
            "user_1_1",
            "user_1_1",
            "user_1_2",
        ]

    def test_start_end_multiple_users(self):
        """Sessions counted independently per user; user without second session gets only 1."""
        stream = make_stream(
            [
                ["user_1", "start", "2020-01-01 00:00:00"],
                ["user_1", "event_1", "2020-01-01 00:00:00"],
                ["user_1", "event_2", "2020-01-01 00:01:00"],
                ["user_1", "end", "2020-01-01 00:01:00"],
                ["user_1", "start", "2020-01-01 00:02:00"],
                ["user_1", "event_3", "2020-01-01 00:02:00"],
                ["user_1", "end", "2020-01-01 00:02:00"],
                ["user_2", "start", "2020-01-01 00:00:00"],
                ["user_2", "event_1", "2020-01-01 00:00:00"],
                ["user_2", "event_2", "2020-01-01 00:01:00"],
                ["user_2", "event_3", "2020-01-01 00:02:00"],
                ["user_2", "end", "2020-01-01 00:02:00"],
            ]
        )
        res = stream.split_sessions(start_event="start", end_event="end")
        df = res.df

        u1 = df[df["user_id"] == "user_1"]["session_index"].tolist()
        u2 = df[df["user_id"] == "user_2"]["session_index"].tolist()
        assert u1 == [1, 1, 2]
        assert u2 == [1, 1, 1]


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class TestSplitSessionsSchema:
    def test_session_col_in_path_cols(self):
        stream = make_stream(
            [
                ["user_1", "sep", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
            ]
        )
        res = stream.split_sessions(separator="sep")
        assert "session_id" in res.schema.path_cols

    def test_session_index_col_in_custom_cols(self):
        stream = make_stream(
            [
                ["user_1", "sep", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
            ]
        )
        res = stream.split_sessions(separator="sep")
        assert "session_index" in res.schema.custom_cols

    def test_custom_col_names(self):
        stream = make_stream(
            [
                ["user_1", "sep", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
            ]
        )
        res = stream.split_sessions(
            separator="sep", session_col="sid", session_index_col="snum"
        )
        assert "sid" in res.schema.path_cols
        assert "snum" in res.schema.custom_cols
        assert "sid" in res.df.columns
        assert "snum" in res.df.columns


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestSplitSessionsValidation:
    def test_raises_no_boundary_params(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.split_sessions()

    def test_raises_start_without_end(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.split_sessions(start_event="start")

    def test_raises_end_without_start(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.split_sessions(end_event="end")

    def test_raises_multiple_boundary_modes(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.split_sessions(separator="sep", start_event="start", end_event="end")

    def test_raises_session_col_already_exists(self):
        df = pd.DataFrame(
            [["user_1", "A", "2020-01-01", "existing"]],
            columns=["user_id", "event", "timestamp", "session_id"],
        )
        schema = {**SCHEMA, "custom_cols": ["session_id"]}
        stream = Eventstream(df, schema)
        with pytest.raises(PreprocessingConfigError):
            stream.split_sessions(separator="sep")


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------


class TestSplitSessionsTimeout:
    def test_timeout_duration_string(self):
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:00:30"],
                ["user_1", "C", "2020-01-01 01:00:00"],
            ]
        )
        res = stream.split_sessions(timeout="1m")
        sessions = res.df["session_id"].dropna().unique().tolist()
        assert len(sessions) == 2

    def test_timeout_timedelta(self):
        import pandas as _pd

        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 01:00:00"],
            ]
        )
        res = stream.split_sessions(timeout=_pd.Timedelta(minutes=30))
        sessions = res.df["session_id"].dropna().unique().tolist()
        assert len(sessions) == 2

    def test_timeout_bare_number_raises(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.split_sessions(timeout=1800)

    def test_timeout_string_without_unit_raises(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.split_sessions(timeout="1800")
