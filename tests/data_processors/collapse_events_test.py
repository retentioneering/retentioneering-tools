import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import PreprocessingConfigError


SCHEMA = {
    "path_cols": ["user_id"],
    "event_cols": ["event"],
    "timestamp_col": "timestamp",
}

COLLAPSED = EventTypes().COLLAPSED_EVENT.type


def make_stream(rows):
    df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp"])
    return Eventstream(df, SCHEMA)


def events(stream):
    return list(stream.df["event"].astype(str))


def event_types(stream):
    return list(stream.df[stream.schema.event_type].astype(str))


# ---------------------------------------------------------------------------
# Repetitive mode
# ---------------------------------------------------------------------------


class TestCollapseEventsRepetitive:
    def test_repetitive_collapse(self):
        df = pd.DataFrame(
            [
                ["u1", "A", "2023-01-01 00:00:00"],
                ["u1", "A", "2023-01-01 00:01:00"],
                ["u1", "B", "2023-01-01 00:02:00"],
                ["u1", "B", "2023-01-01 00:03:00"],
                ["u1", "B", "2023-01-01 00:04:00"],
                ["u1", "C", "2023-01-01 00:05:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)
        res = stream.collapse_events(consecutive=True)

        expected = Eventstream(
            pd.DataFrame(
                [
                    ["u1", "A", "2023-01-01 00:00:00"],
                    ["u1", "B", "2023-01-01 00:02:00"],
                    ["u1", "C", "2023-01-01 00:05:00"],
                ],
                columns=["user_id", "event", "timestamp"],
            )
        )
        assert res.equals(expected)

    def test_repetitive_with_event_list(self):
        """Only specified events are collapsed; others remain as-is."""
        df = pd.DataFrame(
            [
                ["u1", "A", "2023-01-01 00:00:00"],
                ["u1", "A", "2023-01-01 00:01:00"],
                ["u1", "A", "2023-01-01 00:02:00"],
                ["u1", "B", "2023-01-01 00:03:00"],
                ["u1", "B", "2023-01-01 00:04:00"],
                ["u1", "C", "2023-01-01 00:05:00"],
                ["u1", "C", "2023-01-01 00:06:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)
        res = stream.collapse_events(consecutive=["A", "B"])

        expected = Eventstream(
            pd.DataFrame(
                [
                    ["u1", "A", "2023-01-01 00:00:00"],
                    ["u1", "B", "2023-01-01 00:03:00"],
                    ["u1", "C", "2023-01-01 00:05:00"],
                    ["u1", "C", "2023-01-01 00:06:00"],
                ],
                columns=["user_id", "event", "timestamp"],
            )
        )
        assert res.equals(expected)

    def test_repetitive_with_single_event_in_list(self):
        """Repetitive list with one event collapses only that event."""
        df = pd.DataFrame(
            [
                ["u1", "A", "2023-01-01 00:00:00"],
                ["u1", "A", "2023-01-01 00:01:00"],
                ["u1", "B", "2023-01-01 00:02:00"],
                ["u1", "B", "2023-01-01 00:03:00"],
                ["u1", "C", "2023-01-01 00:04:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)
        res = stream.collapse_events(consecutive=["A"])

        expected = Eventstream(
            pd.DataFrame(
                [
                    ["u1", "A", "2023-01-01 00:00:00"],
                    ["u1", "B", "2023-01-01 00:02:00"],
                    ["u1", "B", "2023-01-01 00:03:00"],
                    ["u1", "C", "2023-01-01 00:04:00"],
                ],
                columns=["user_id", "event", "timestamp"],
            )
        )
        assert res.equals(expected)

    def test_distinct_events_with_tied_timestamps_not_merged(self):
        """Regression: distinct consecutive events sharing the exact same timestamp
        must survive as separate rows. DuckDB's default RANGE window frame treated
        rows tied on (timestamp, subindex) as peers, so the running SUM gave them
        an identical group id and they were silently merged into one 'collapsed' row.
        """
        df = pd.DataFrame(
            [
                ["u1", "A", "2020-01-01 00:00:00"],
                ["u1", "B", "2020-01-01 00:00:00"],  # same timestamp, different event
                ["u1", "C", "2020-01-01 00:01:00"],
                ["u1", "C", "2020-01-01 00:02:00"],  # genuine repetition
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)
        res = stream.collapse_events(consecutive=True)

        assert events(res) == ["A", "B", "C"]
        # A and B are not collapsed rows; only the C-run is.
        assert event_types(res).count(COLLAPSED) == 1
        assert event_types(res)[-1] == COLLAPSED

    def test_path_id_override_and_agg(self):
        df = pd.DataFrame(
            [
                ["user_1", "sess_1", "A", "2023-01-01 00:00:00", 1],
                ["user_1", "sess_1", "A", "2023-01-01 00:01:00", 3],
                ["user_1", "sess_2", "B", "2023-01-01 00:02:00", 5],
                ["user_1", "sess_2", "B", "2023-01-01 00:03:00", 2],
            ],
            columns=["user_id", "session_id", "event", "timestamp", "score"],
        )
        schema = {"path_cols": ["user_id", "session_id"], "custom_cols": ["score"]}
        stream = Eventstream(df, schema)
        res = stream.collapse_events(
            consecutive=True, agg={"score": "max"}, path_col="session_id"
        )

        expected = Eventstream(
            pd.DataFrame(
                [
                    ["user_1", "sess_1", "A", "2023-01-01 00:00:00", 3],
                    ["user_1", "sess_2", "B", "2023-01-01 00:02:00", 5],
                ],
                columns=["user_id", "session_id", "event", "timestamp", "score"],
            ),
            schema,
        )
        assert res.equals(expected)


# ---------------------------------------------------------------------------
# Event groups — all classes below use the event_groups parameter
# which was NOT ported to the library (it depends on FilterPaths).
# ---------------------------------------------------------------------------

# Not ported
# class TestCollapseEventsGroupsEvents: ...
# class TestCollapseEventsGroupsSeparator: ...
# class TestCollapseEventsGroupsStartEnd: ...
# class TestCollapseEventsGroupsTimeout: ...
# class TestCollapseEventsGroupsCases: ...
# class TestCollapseEventsMultipleGroups: ...
# class TestCollapseEventsAgg (event_groups variant): ...


# ---------------------------------------------------------------------------
# group_col mode
# ---------------------------------------------------------------------------


class TestCollapseEventsFromCol:
    def test_basic_col_collapse(self):
        """Consecutive runs of equal column value are collapsed into one event named after that value."""
        df = pd.DataFrame(
            [
                ["user_1", "A", "session_type_1", "2020-01-01 00:00:00"],
                ["user_1", "B", "session_type_1", "2020-01-01 00:01:00"],
                ["user_1", "C", "session_type_2", "2020-01-01 00:02:00"],
                ["user_1", "D", "session_type_2", "2020-01-01 00:03:00"],
            ],
            columns=["user_id", "event", "session_type", "timestamp"],
        )
        schema = {**SCHEMA, "custom_cols": ["session_type"]}
        stream = Eventstream(df, schema)

        res = stream.collapse_events(group_col="session_type")

        assert events(res) == ["session_type_1", "session_type_2"]

    def test_col_collapse_multiple_users(self):
        """Column-based collapse is independent per user."""
        df = pd.DataFrame(
            [
                ["user_1", "A", "x", "2020-01-01 00:00:00"],
                ["user_1", "B", "x", "2020-01-01 00:01:00"],
                ["user_1", "C", "y", "2020-01-01 00:02:00"],
                ["user_2", "A", "x", "2020-01-01 00:00:00"],
                ["user_2", "B", "x", "2020-01-01 00:01:00"],
            ],
            columns=["user_id", "event", "col", "timestamp"],
        )
        schema = {**SCHEMA, "custom_cols": ["col"]}
        stream = Eventstream(df, schema)

        res = stream.collapse_events(group_col="col")
        df_res = res.df

        u1 = list(df_res[df_res["user_id"] == "user_1"]["event"].astype(str))
        u2 = list(df_res[df_res["user_id"] == "user_2"]["event"].astype(str))
        assert u1 == ["x", "y"]
        assert u2 == ["x"]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestCollapseEventsValidation:
    def test_raises_no_mode(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events()

    def test_raises_multiple_modes(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(
                consecutive=True, event_groups=[{"events": ["A"], "name": "s"}]
            )

    def test_raises_group_col_not_found(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(group_col="nonexistent_col")

    def test_raises_group_col_same_as_event_col(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(group_col="event")

    def test_raises_session_id_col_without_session_type_col(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(session_id_col="session_id")

    def test_raises_session_id_col_not_found(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(
                session_id_col="nonexistent", session_type_col="also_nonexistent"
            )

    def test_raises_session_type_col_not_found(self):
        df = pd.DataFrame(
            [
                ["user_1", "A", 1, "2020-01-01"],
            ],
            columns=["user_id", "event", "session_id", "timestamp"],
        )
        schema = {**SCHEMA, "custom_cols": ["session_id"]}
        stream = Eventstream(df, schema)
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(
                session_id_col="session_id", session_type_col="nonexistent"
            )

    def test_raises_empty_event_groups(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(event_groups=[])

    def test_raises_no_boundary_mode(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(event_groups=[{"name": "session"}])

    def test_raises_multiple_boundary_modes(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(
                event_groups=[
                    {
                        "events": ["A"],
                        "separator": "sep",
                        "name": "session",
                    }
                ]
            )

    def test_raises_start_without_end(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(
                event_groups=[{"start_event": "start", "name": "session"}]
            )

    def test_raises_end_without_start(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(
                event_groups=[{"end_event": "end", "name": "session"}]
            )

    def test_raises_no_default_and_no_cases(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(event_groups=[{"events": ["A"]}])


# ---------------------------------------------------------------------------
# Session type mode
# ---------------------------------------------------------------------------


class TestCollapseEventsBySessionType:
    def _make_stream(self, rows):
        df = pd.DataFrame(
            rows,
            columns=["user_id", "event", "session_id", "session_type", "timestamp"],
        )
        schema = {**SCHEMA, "custom_cols": ["session_id", "session_type"]}
        return Eventstream(df, schema)

    def test_basic_collapse(self):
        """Each session collapses into one row with session_type as the event name."""
        stream = self._make_stream(
            [
                ["user_1", "A", 1, "browse", "2020-01-01 00:00:00"],
                ["user_1", "B", 1, "browse", "2020-01-01 00:01:00"],
                ["user_1", "C", 2, "purchase", "2020-01-01 00:02:00"],
                ["user_1", "D", 2, "purchase", "2020-01-01 00:03:00"],
            ]
        )
        res = stream.collapse_events(
            session_id_col="session_id", session_type_col="session_type"
        )

        assert events(res) == ["browse", "purchase"]

    def test_event_type_is_collapsed(self):
        """Collapsed rows get the collapsed event_type."""
        stream = self._make_stream(
            [
                ["user_1", "A", 1, "browse", "2020-01-01 00:00:00"],
                ["user_1", "B", 1, "browse", "2020-01-01 00:01:00"],
            ]
        )
        res = stream.collapse_events(
            session_id_col="session_id", session_type_col="session_type"
        )

        assert all(res.df[res.schema.event_type] == COLLAPSED)

    def test_earliest_timestamp_kept(self):
        """The collapsed row uses the earliest timestamp within the session."""
        stream = self._make_stream(
            [
                ["user_1", "A", 1, "browse", "2020-01-01 00:05:00"],
                ["user_1", "B", 1, "browse", "2020-01-01 00:10:00"],
                ["user_1", "C", 1, "browse", "2020-01-01 00:15:00"],
            ]
        )
        res = stream.collapse_events(
            session_id_col="session_id", session_type_col="session_type"
        )

        ts = pd.to_datetime(res.df["timestamp"].iloc[0])
        assert ts == pd.Timestamp("2020-01-01 00:05:00")

    def test_multiple_users(self):
        """Sessions are collapsed independently per user."""
        stream = self._make_stream(
            [
                ["user_1", "A", 1, "browse", "2020-01-01 00:00:00"],
                ["user_1", "B", 2, "purchase", "2020-01-01 00:01:00"],
                ["user_2", "A", 3, "browse", "2020-01-01 00:00:00"],
                ["user_2", "B", 3, "browse", "2020-01-01 00:01:00"],
            ]
        )
        res = stream.collapse_events(
            session_id_col="session_id", session_type_col="session_type"
        )
        df = res.df

        u1 = list(df[df["user_id"] == "user_1"]["event"].astype(str))
        u2 = list(df[df["user_id"] == "user_2"]["event"].astype(str))
        assert sorted(u1) == ["browse", "purchase"]
        assert u2 == ["browse"]

    def test_single_event_per_session(self):
        """Sessions with a single event also collapse correctly."""
        stream = self._make_stream(
            [
                ["user_1", "A", 1, "browse", "2020-01-01 00:00:00"],
                ["user_1", "B", 2, "purchase", "2020-01-01 00:01:00"],
            ]
        )
        res = stream.collapse_events(
            session_id_col="session_id", session_type_col="session_type"
        )

        assert sorted(events(res)) == ["browse", "purchase"]

    def test_agg_max(self):
        """Custom agg is applied to extra columns."""
        df = pd.DataFrame(
            [
                ["user_1", "A", 1, "browse", "2020-01-01 00:00:00", 10],
                ["user_1", "B", 1, "browse", "2020-01-01 00:01:00", 30],
                ["user_1", "C", 2, "purchase", "2020-01-01 00:02:00", 5],
            ],
            columns=[
                "user_id",
                "event",
                "session_id",
                "session_type",
                "timestamp",
                "score",
            ],
        )
        schema = {**SCHEMA, "custom_cols": ["session_id", "session_type", "score"]}
        stream = Eventstream(df, schema)

        res = stream.collapse_events(
            session_id_col="session_id",
            session_type_col="session_type",
            agg={"score": "max"},
        )
        df_res = res.df
        browse_row = df_res[df_res["event"] == "browse"]
        assert int(browse_row["score"].iloc[0]) == 30

    def test_agg_first_is_default(self):
        """Without explicit agg, 'first' (earliest timestamp) is the default."""
        df = pd.DataFrame(
            [
                ["user_1", "A", 1, "browse", "2020-01-01 00:00:00", 10],
                ["user_1", "B", 1, "browse", "2020-01-01 00:01:00", 20],
            ],
            columns=[
                "user_id",
                "event",
                "session_id",
                "session_type",
                "timestamp",
                "score",
            ],
        )
        schema = {**SCHEMA, "custom_cols": ["session_id", "session_type", "score"]}
        stream = Eventstream(df, schema)

        res = stream.collapse_events(
            session_id_col="session_id", session_type_col="session_type"
        )
        df_res = res.df
        assert int(df_res["score"].iloc[0]) == 10


# ---------------------------------------------------------------------------
# Event groups — events mode
# ---------------------------------------------------------------------------


class TestCollapseEventsGroupsEvents:
    def test_basic_events_collapse(self):
        """Events in the session group are collapsed into a single row with the default name."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
                ["user_1", "B", "2020-01-01 00:02:00"],
                ["user_1", "C", "2020-01-01 00:03:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[{"events": ["A", "B"], "name": "session"}]
        )

        assert events(res) == ["session", "C"]

    def test_events_mode_collapsed_event_type(self):
        """Collapsed rows get the collapsed event_type."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_1", "C", "2020-01-01 00:02:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[{"events": ["A", "B"], "name": "session"}]
        )
        df = res.df

        collapsed_rows = df[df["event"] == "session"]
        assert all(collapsed_rows[res.schema.event_type] == COLLAPSED)

    def test_events_mode_uncollapsed_rows_preserved(self):
        """Rows outside the session group are kept as-is."""
        stream = make_stream(
            [
                ["user_1", "X", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
                ["user_1", "Y", "2020-01-01 00:02:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[{"events": ["A"], "name": "session"}]
        )

        assert "X" in events(res)
        assert "Y" in events(res)

    def test_events_mode_multiple_sessions(self):
        """Two disjoint session groups in the same path each produce one collapsed row."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
                ["user_1", "C", "2020-01-01 00:02:00"],
                ["user_1", "A", "2020-01-01 00:03:00"],
                ["user_1", "A", "2020-01-01 00:04:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[{"events": ["A"], "name": "session"}]
        )
        df = res.df

        assert list(df["event"].astype(str)).count("session") == 2
        assert "C" in list(df["event"].astype(str))

    def test_events_mode_earliest_timestamp_kept(self):
        """The collapsed row uses the earliest timestamp in the session."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:05:00"],
                ["user_1", "A", "2020-01-01 00:10:00"],
                ["user_1", "B", "2020-01-01 00:15:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[{"events": ["A", "B"], "name": "session"}]
        )
        df = res.df

        ts = pd.to_datetime(df.loc[df["event"] == "session", "timestamp"].iloc[0])
        assert ts == pd.Timestamp("2020-01-01 00:05:00")

    def test_events_mode_multiple_users(self):
        """Each user's sessions are counted independently."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_2", "A", "2020-01-01 00:00:00"],
                ["user_2", "C", "2020-01-01 00:01:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[{"events": ["A", "B"], "name": "session"}]
        )
        df = res.df

        u1_events = list(df[df["user_id"] == "user_1"]["event"].astype(str))
        u2_events = list(df[df["user_id"] == "user_2"]["event"].astype(str))
        assert "session" in u1_events
        assert "C" in u2_events


# ---------------------------------------------------------------------------
# Event groups — cases (conditional naming)
# ---------------------------------------------------------------------------


class TestCollapseEventsGroupsCases:
    def test_cases_has_metric(self):
        """Cases with 'has_event' metric assign correct name when event is present."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "purchase", "2020-01-01 00:01:00"],
                ["user_1", "B", "2020-01-01 00:02:00"],
                ["user_1", "sep", "2020-01-01 00:03:00"],
                ["user_1", "C", "2020-01-01 00:04:00"],
                ["user_1", "D", "2020-01-01 00:05:00"],
                ["user_1", "sep", "2020-01-01 00:06:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[
                {
                    "separator": "sep",
                    "cases": [
                        {
                            "condition": {
                                "op": ">",
                                "metric": "has_event",
                                "value": 0,
                                "metric_args": {"event": "purchase"},
                            },
                            "name": "purchase_session",
                        }
                    ],
                    "name": "no_purchase_session",
                }
            ]
        )

        result_events = events(res)
        assert "purchase_session" in result_events
        assert "no_purchase_session" in result_events

    def test_cases_event_count_metric(self):
        """Cases with 'event_count' metric (documented 'event' key) name sessions by threshold."""
        stream = make_stream(
            [
                # session 1: two 'click' events -> exceeds threshold
                ["user_1", "click", "2020-01-01 00:00:00"],
                ["user_1", "click", "2020-01-01 00:01:00"],
                ["user_1", "sep", "2020-01-01 00:02:00"],
                # session 2: one 'click' event -> below threshold
                ["user_1", "click", "2020-01-01 00:03:00"],
                ["user_1", "A", "2020-01-01 00:04:00"],
                ["user_1", "sep", "2020-01-01 00:05:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[
                {
                    "separator": "sep",
                    "cases": [
                        {
                            "condition": {
                                "op": ">",
                                "metric": "event_count",
                                "value": 1,
                                "metric_args": {"event": "click"},
                            },
                            "name": "active_session",
                        }
                    ],
                    "name": "quiet_session",
                }
            ]
        )

        assert events(res) == ["active_session", "quiet_session"]

    def test_cases_default_when_no_match(self):
        """When no case condition matches, the default name is used."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_1", "sep", "2020-01-01 00:02:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[
                {
                    "separator": "sep",
                    "cases": [
                        {
                            "condition": {
                                "op": ">",
                                "metric": "has_event",
                                "value": 0,
                                "metric_args": {"event": "purchase"},
                            },
                            "name": "purchase_session",
                        }
                    ],
                    "name": "other_session",
                }
            ]
        )

        assert "other_session" in events(res)
        assert "purchase_session" not in events(res)

    def test_cases_has_all_events_metric(self):
        """Cases with 'has_all_events' (AND semantics) name sessions containing
        every listed event."""
        stream = make_stream(
            [
                # session 1: has both A and B -> matches
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_1", "sep", "2020-01-01 00:02:00"],
                # session 2: only A -> doesn't match
                ["user_1", "A", "2020-01-01 00:03:00"],
                ["user_1", "sep", "2020-01-01 00:04:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[
                {
                    "separator": "sep",
                    "cases": [
                        {
                            "condition": {
                                "op": "=",
                                "metric": "has_all_events",
                                "value": True,
                                "metric_args": {"events": ["A", "B"]},
                            },
                            "name": "both_session",
                        }
                    ],
                    "name": "partial_session",
                }
            ]
        )

        assert events(res) == ["both_session", "partial_session"]

    def test_cases_has_any_event_metric(self):
        """Cases with 'has_any_event' (OR semantics) name sessions containing
        at least one of the listed events."""
        stream = make_stream(
            [
                # session 1: has A -> matches
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "sep", "2020-01-01 00:01:00"],
                # session 2: has neither A nor B -> doesn't match
                ["user_1", "C", "2020-01-01 00:02:00"],
                ["user_1", "sep", "2020-01-01 00:03:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[
                {
                    "separator": "sep",
                    "cases": [
                        {
                            "condition": {
                                "op": "=",
                                "metric": "has_any_event",
                                "value": True,
                                "metric_args": {"events": ["A", "B"]},
                            },
                            "name": "matched_session",
                        }
                    ],
                    "name": "unmatched_session",
                }
            ]
        )

        assert events(res) == ["matched_session", "unmatched_session"]

    def test_cases_bulk_metric_forbidden_in_condition(self):
        """has_event_bulk/event_count_bulk cannot appear in case conditions -
        they produce multiple columns, not a single comparable value."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "sep", "2020-01-01 00:01:00"],
            ]
        )
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(
                event_groups=[
                    {
                        "separator": "sep",
                        "cases": [
                            {
                                "condition": {
                                    "op": "=",
                                    "metric": "has_event_bulk",
                                    "value": True,
                                    "metric_args": {"events": ["A", "B"]},
                                },
                                "name": "matched_session",
                            }
                        ],
                        "name": "unmatched_session",
                    }
                ]
            )


# ---------------------------------------------------------------------------
# Event groups — separator mode
# ---------------------------------------------------------------------------


class TestCollapseEventsGroupsSeparator:
    def test_separator_basic(self):
        """Events up to (and including) the separator are collapsed."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_1", "sep", "2020-01-01 00:02:00"],
                ["user_1", "C", "2020-01-01 00:03:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[{"separator": "sep", "name": "session"}]
        )
        df = res.df

        assert "session" in list(df["event"].astype(str))
        assert "C" in list(df["event"].astype(str))

    def test_separator_multiple_sessions(self):
        """Multiple separator-delimited groups each collapse into one row."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "sep", "2020-01-01 00:01:00"],
                ["user_1", "B", "2020-01-01 00:02:00"],
                ["user_1", "sep", "2020-01-01 00:03:00"],
                ["user_1", "C", "2020-01-01 00:04:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[{"separator": "sep", "name": "session"}]
        )

        assert events(res).count("session") == 2
        assert "C" in events(res)


# ---------------------------------------------------------------------------
# Event groups — start / end mode
# ---------------------------------------------------------------------------


class TestCollapseEventsGroupsStartEnd:
    def test_start_end_basic(self):
        """Events between start and end (inclusive) are collapsed into one row."""
        stream = make_stream(
            [
                ["user_1", "start", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
                ["user_1", "B", "2020-01-01 00:02:00"],
                ["user_1", "end", "2020-01-01 00:03:00"],
                ["user_1", "C", "2020-01-01 00:04:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[
                {"start_event": "start", "end_event": "end", "name": "session"}
            ]
        )
        df = res.df

        assert "session" in list(df["event"].astype(str))
        assert "C" in list(df["event"].astype(str))

    def test_start_end_multiple_sessions(self):
        """Two start/end pairs produce two collapsed rows."""
        stream = make_stream(
            [
                ["user_1", "start", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
                ["user_1", "end", "2020-01-01 00:02:00"],
                ["user_1", "start", "2020-01-01 00:03:00"],
                ["user_1", "B", "2020-01-01 00:04:00"],
                ["user_1", "end", "2020-01-01 00:05:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[
                {"start_event": "start", "end_event": "end", "name": "session"}
            ]
        )

        assert events(res).count("session") == 2


# ---------------------------------------------------------------------------
# Event groups — timeout mode
# ---------------------------------------------------------------------------


class TestCollapseEventsGroupsTimeout:
    def test_timeout_splits_events_session(self):
        """A timeout add-on to events mode splits a session when the gap exceeds the timeout."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],  # within 60s
                ["user_1", "A", "2020-01-01 01:00:00"],  # > 60s gap — new session
                ["user_1", "B", "2020-01-01 01:01:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[{"events": ["A", "B"], "timeout": "60s", "name": "session"}]
        )

        assert events(res).count("session") == 2

    def test_timeout_no_split_when_within_window(self):
        """No timeout split when all events are within the window."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:00:30"],
                ["user_1", "A", "2020-01-01 00:00:59"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[{"events": ["A", "B"], "timeout": "60s", "name": "session"}]
        )

        assert events(res) == ["session"]

    def test_timeout_multiple_users(self):
        """Timeout is applied independently per user."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 01:00:00"],  # gap > 60s — second session
                ["user_2", "A", "2020-01-01 00:00:00"],
                ["user_2", "A", "2020-01-01 00:00:30"],  # gap < 60s — same session
            ]
        )
        res = stream.collapse_events(
            event_groups=[{"events": ["A"], "timeout": "60s", "name": "session"}]
        )
        df = res.df

        u1_sessions = list(df[df["user_id"] == "user_1"]["event"].astype(str))
        u2_sessions = list(df[df["user_id"] == "user_2"]["event"].astype(str))
        assert u1_sessions.count("session") == 2
        assert u2_sessions.count("session") == 1


# ---------------------------------------------------------------------------
# Event groups — multiple groups applied sequentially
# ---------------------------------------------------------------------------


class TestCollapseEventsMultipleGroups:
    def test_two_groups_applied_sequentially(self):
        """Two groups are applied one after the other."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
                ["user_1", "B", "2020-01-01 00:02:00"],
                ["user_1", "B", "2020-01-01 00:03:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=[
                {"events": ["A"], "name": "session_a"},
                {"events": ["B"], "name": "session_b"},
            ]
        )

        result_events = events(res)
        assert "session_a" in result_events
        assert "session_b" in result_events
        assert "A" not in result_events
        assert "B" not in result_events


# ---------------------------------------------------------------------------
# Agg parameter (event_groups variant)
# ---------------------------------------------------------------------------


class TestCollapseEventsAgg:
    def test_agg_last_for_custom_col(self):
        """The 'last' aggregation picks the value from the latest event in the session."""
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", 10],
                ["user_1", "A", "2020-01-01 00:01:00", 20],
                ["user_1", "C", "2020-01-01 00:02:00", 30],
            ],
            columns=["user_id", "event", "timestamp", "score"],
        )
        schema = {**SCHEMA, "custom_cols": ["score"]}
        stream = Eventstream(df, schema)

        res = stream.collapse_events(
            event_groups=[{"events": ["A"], "name": "session"}],
            agg={"score": "last"},
        )
        df_res = res.df
        session_row = df_res[df_res["event"] == "session"]
        assert int(session_row["score"].iloc[0]) == 20

    def test_agg_first_is_default(self):
        """Without explicit agg, 'first' is used (earliest timestamp value)."""
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", 10],
                ["user_1", "B", "2020-01-01 00:01:00", 20],
                ["user_1", "sep", "2020-01-01 00:02:00", 30],
            ],
            columns=["user_id", "event", "timestamp", "score"],
        )
        schema = {**SCHEMA, "custom_cols": ["score"]}
        stream = Eventstream(df, schema)

        res = stream.collapse_events(
            event_groups=[{"separator": "sep", "name": "session"}],
        )
        df_res = res.df
        session_row = df_res[df_res["event"] == "session"]
        assert int(session_row["score"].iloc[0]) == 10


class TestEventGroupsTimeoutValidation:
    def test_bare_number_timeout_in_group_raises(self):
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:02:00"],
            ]
        )
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(
                event_groups=[{"events": ["A", "B"], "timeout": 60, "name": "session"}]
            )
