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
        res = stream.collapse_events(loops=True)

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
        res = stream.collapse_events(loops=["A", "B"])

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
        res = stream.collapse_events(loops=["A"])

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
        res = stream.collapse_events(loops=True)

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
            loops=True, agg={"score": "max"}, path_col="session_id"
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
# Boundary modes — event_groups / bounds
# which was NOT ported to the library (it depends on FilterPaths).
# ---------------------------------------------------------------------------

# Not ported
# class TestCollapseEventsGroupsEvents: ...
# class TestCollapseEventsGroupsStartEnd: ...
# class TestCollapseEventsGroupsTimeout: ...
# class TestCollapseEventsGroupsCases: ...
# class TestCollapseEventsMultipleGroups: ...
# class TestCollapseEventsAgg (boundary modes): ...


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
            stream.collapse_events(loops=True, event_groups=["A"], name="s")

    def test_raises_runs_col_not_found(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(group_col="nonexistent_col")

    def test_raises_runs_col_same_as_event_col(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(group_col="event")

    def test_raises_run_mode_with_boundary_mode(self):
        """Adjacency modes and windows are different ways to chunk, not
        composable ones."""
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError, match="cannot be combined"):
            stream.collapse_events(loops=True, event_groups=["A"], name="s")

    def test_raises_timeout_is_not_a_mode(self):
        """Breaking on inactivity belongs to split_sessions; collapse_events
        collapses the session column it writes."""
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(TypeError):
            stream.collapse_events(event_groups=["A"], name="s", timeout="30m")

    def test_raises_unknown_name_dict_key(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError, match="exactly one key, 'col'"):
            stream.collapse_events(event_groups=["A"], name={"column": "kind"})

    def test_raises_bad_name_type(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError, match="'name' must be"):
            stream.collapse_events(event_groups=["A"], name=42)

    def test_raises_name_list_without_cases(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError, match="pass it directly"):
            stream.collapse_events(event_groups=["A"], name=["just_a_fallback"])

    def test_raises_malformed_case(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError, match="'condition' and"):
            stream.collapse_events(event_groups=["A"], name=[{"name": "no_condition"}])

    def test_raises_name_col_not_found(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(
                group_col="nonexistent", name={"col": "also_nonexistent"}
            )

    def test_raises_name_col_missing_from_stream(self):
        df = pd.DataFrame(
            [
                ["user_1", "A", 1, "2020-01-01"],
            ],
            columns=["user_id", "event", "session_id", "timestamp"],
        )
        schema = {**SCHEMA, "custom_cols": ["session_id"]}
        stream = Eventstream(df, schema)
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(group_col="session_id", name={"col": "nonexistent"})

    def test_raises_empty_events(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError, match="exactly one mode"):
            stream.collapse_events(event_groups=[], name="session")

    def test_raises_no_boundary_mode(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(name="session")

    def test_raises_multiple_boundary_modes(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError, match="at most one boundary"):
            stream.collapse_events(
                event_groups=["A"],
                bounds={"start_event": "s", "end_event": "e"},
                name="session",
            )

    def test_raises_start_without_end(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(bounds={"start_event": "start"}, name="session")

    def test_raises_end_without_start(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(bounds={"end_event": "end"}, name="session")

    def test_raises_no_default_and_no_cases(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(event_groups=["A"])

    def test_raises_group_name_with_path_delimiter(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError, match="add->cart"):
            stream.collapse_events(event_groups=["A"], name="add->cart")

    def test_raises_case_name_with_path_delimiter(self):
        stream = make_stream([["user_1", "A", "2020-01-01"]])
        with pytest.raises(PreprocessingConfigError, match="add->cart"):
            stream.collapse_events(
                event_groups=["A"],
                name=[
                    {
                        "name": "add->cart",
                        "condition": {
                            "op": ">",
                            "metric": "has_event",
                            "value": 0,
                            "metric_args": {"event": "A"},
                        },
                    },
                    "session",
                ],
            )


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
            group_col="session_id", name={"col": "session_type"}
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
            group_col="session_id", name={"col": "session_type"}
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
            group_col="session_id", name={"col": "session_type"}
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
            group_col="session_id", name={"col": "session_type"}
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
            group_col="session_id", name={"col": "session_type"}
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
            group_col="session_id", name={"col": "session_type"}, agg={"score": "max"}
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
            group_col="session_id", name={"col": "session_type"}
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
        res = stream.collapse_events(event_groups=["A", "B"], name="session")

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
        res = stream.collapse_events(event_groups=["A", "B"], name="session")
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
        res = stream.collapse_events(event_groups=["A"], name="session")

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
        res = stream.collapse_events(event_groups=["A"], name="session")
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
        res = stream.collapse_events(event_groups=["A", "B"], name="session")
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
        res = stream.collapse_events(event_groups=["A", "B"], name="session")
        df = res.df

        u1_events = list(df[df["user_id"] == "user_1"]["event"].astype(str))
        u2_events = list(df[df["user_id"] == "user_2"]["event"].astype(str))
        assert "session" in u1_events
        assert "C" in u2_events


# ---------------------------------------------------------------------------
# Event groups — cases (conditional naming)
# ---------------------------------------------------------------------------


class TestCollapseEventsGroupsCases:
    """Conditional naming. The chunking here is `bounds`, but nothing in `cases`
    depends on that — the conditions are aggregates over whatever group the mode
    produced (see TestCollapseEventsNaming for the same cases on other modes)."""

    BOUNDS = {"start_event": "start", "end_event": "end"}

    @staticmethod
    def _windows(*groups):
        """Two-marker windows laid end to end, one minute apart."""
        rows, t = [], 0
        for g in groups:
            for event in ["start", *g, "end"]:
                rows.append(["user_1", event, f"2020-01-01 00:{t:02d}:00"])
                t += 1
        return make_stream(rows)

    def test_cases_has_metric(self):
        """Cases with 'has_event' name a window by what happened inside it."""
        stream = self._windows(["A", "purchase", "B"], ["C", "D"])
        res = stream.collapse_events(
            bounds=self.BOUNDS,
            name=[
                {
                    "condition": {
                        "op": ">",
                        "metric": "has_event",
                        "value": 0,
                        "metric_args": {"event": "purchase"},
                    },
                    "name": "purchase_session",
                },
                "no_purchase_session",
            ],
        )
        assert events(res) == ["purchase_session", "no_purchase_session"]

    def test_cases_event_count_metric(self):
        """'event_count' (documented 'event' key) names windows by threshold."""
        stream = self._windows(["click", "click"], ["click", "A"])
        res = stream.collapse_events(
            bounds=self.BOUNDS,
            name=[
                {
                    "condition": {
                        "op": ">",
                        "metric": "event_count",
                        "value": 1,
                        "metric_args": {"event": "click"},
                    },
                    "name": "active_session",
                },
                "quiet_session",
            ],
        )
        assert events(res) == ["active_session", "quiet_session"]

    def test_cases_default_when_no_match(self):
        """When no case matches, the fallback is used."""
        stream = self._windows(["A", "B"])
        res = stream.collapse_events(
            bounds=self.BOUNDS,
            name=[
                {
                    "condition": {
                        "op": "=",
                        "metric": "has_event",
                        "value": True,
                        "metric_args": {"event": "purchase"},
                    },
                    "name": "purchase_session",
                },
                "plain_session",
            ],
        )
        assert events(res) == ["plain_session"]

    def test_cases_has_all_events_metric(self):
        stream = self._windows(["A", "B"], ["A", "C"])
        res = stream.collapse_events(
            bounds=self.BOUNDS,
            name=[
                {
                    "condition": {
                        "op": "=",
                        "metric": "has_all_events",
                        "value": True,
                        "metric_args": {"events": ["A", "B"]},
                    },
                    "name": "both",
                },
                "not_both",
            ],
        )
        assert events(res) == ["both", "not_both"]

    def test_cases_has_any_event_metric(self):
        stream = self._windows(["A", "X"], ["C", "D"])
        res = stream.collapse_events(
            bounds=self.BOUNDS,
            name=[
                {
                    "condition": {
                        "op": "=",
                        "metric": "has_any_event",
                        "value": True,
                        "metric_args": {"events": ["A", "B"]},
                    },
                    "name": "any_of_them",
                },
                "none_of_them",
            ],
        )
        assert events(res) == ["any_of_them", "none_of_them"]

    def test_cases_bulk_metric_forbidden_in_condition(self):
        """A bulk metric produces several columns and cannot be compared."""
        stream = self._windows(["A", "B"])
        with pytest.raises(PreprocessingConfigError):
            stream.collapse_events(
                bounds=self.BOUNDS,
                name=[
                    {
                        "condition": {
                            "op": "=",
                            "metric": "has_event_bulk",
                            "value": True,
                            "metric_args": {"events": ["A", "B"]},
                        },
                        "name": "matched_session",
                    },
                    "unmatched_session",
                ],
            )


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
            bounds={"start_event": "start", "end_event": "end"}, name="session"
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
            bounds={"start_event": "start", "end_event": "end"}, name="session"
        )

        assert events(res).count("session") == 2


# ---------------------------------------------------------------------------
# Chained collapses
# ---------------------------------------------------------------------------


class TestCollapseEventsChained:
    def test_two_collapses_applied_sequentially(self):
        """Two collapses chain, which is what the old `event_groups` list did
        internally anyway — it looped over the groups and re-ran the query."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
                ["user_1", "B", "2020-01-01 00:02:00"],
                ["user_1", "B", "2020-01-01 00:03:00"],
            ]
        )
        res = stream.collapse_events(
            event_groups=["A"], name="session_a"
        ).collapse_events(event_groups=["B"], name="session_b")

        result_events = events(res)
        assert "session_a" in result_events
        assert "session_b" in result_events
        assert "A" not in result_events
        assert "B" not in result_events


# ---------------------------------------------------------------------------
# Agg parameter (boundary modes)
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
            event_groups=["A"], name="session", agg={"score": "last"}
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
                ["user_1", "C", "2020-01-01 00:02:00", 30],
            ],
            columns=["user_id", "event", "timestamp", "score"],
        )
        schema = {**SCHEMA, "custom_cols": ["score"]}
        stream = Eventstream(df, schema)

        res = stream.collapse_events(event_groups=["A", "B", "C"], name="session")
        df_res = res.df
        session_row = df_res[df_res["event"] == "session"]
        assert int(session_row["score"].iloc[0]) == 10


# ---------------------------------------------------------------------------
# Naming is orthogonal to the mode
# ---------------------------------------------------------------------------


def _session_stream():
    """One path, three runs of session_id — s1 comes back after s2."""
    df = pd.DataFrame(
        [
            ["user_1", "home", "2020-01-01 00:00:00", "s1", "browse"],
            ["user_1", "catalog", "2020-01-01 00:01:00", "s1", "browse"],
            ["user_1", "cart", "2020-01-01 00:02:00", "s2", "buy"],
            ["user_1", "purchase", "2020-01-01 00:03:00", "s2", "buy"],
            ["user_1", "home", "2020-01-01 00:04:00", "s1", "browse"],
        ],
        columns=["user_id", "event", "timestamp", "session_id", "session_kind"],
    )
    schema = {**SCHEMA, "custom_cols": ["session_id", "session_kind"]}
    return Eventstream(df, schema)


class TestCollapseEventsNaming:
    def test_runs_defaults_to_the_column_value(self):
        assert events(_session_stream().collapse_events(group_col="session_id")) == [
            "s1",
            "s2",
            "s1",
        ]

    def test_runs_are_runs_not_values(self):
        """A value that comes back later is a second event, not one event
        stretched across the gap — which is what grouping by the value would do,
        producing a row whose timestamp span swallows the session in between."""
        res = _session_stream().collapse_events(group_col="session_id")
        assert len(res.to_dataframe()) == 3

    def test_name_from_another_column(self):
        res = _session_stream().collapse_events(
            group_col="session_id", name={"col": "session_kind"}
        )
        assert events(res) == ["browse", "buy", "browse"]

    def test_cases_name_a_run(self):
        """`cases` used to be reachable only through `event_groups`; a group is a
        group, so it now applies to any mode."""
        res = _session_stream().collapse_events(
            group_col="session_id",
            name=[
                {
                    "condition": {
                        "op": "=",
                        "metric": "has_event",
                        "value": True,
                        "metric_args": {"event": "purchase"},
                    },
                    "name": "buying_session",
                },
                "browsing_session",
            ],
        )
        assert events(res) == ["browsing_session", "buying_session", "browsing_session"]

    def test_cases_name_a_loop(self):
        stream = make_stream(
            [
                ["user_1", "search", "2020-01-01 00:00:00"],
                ["user_1", "search", "2020-01-01 00:01:00"],
                ["user_1", "search", "2020-01-01 00:02:00"],
                ["user_1", "purchase", "2020-01-01 00:03:00"],
            ]
        )
        res = stream.collapse_events(
            loops=True,
            name=[
                {
                    "condition": {"op": ">", "metric": "length", "value": 2},
                    "name": "search_spree",
                },
            ],
        )
        # No fallback given, so an unmatched run keeps its own event name.
        assert events(res) == ["search_spree", "purchase"]

    def test_literal_name_on_a_run_mode(self):
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "A", "2020-01-01 00:01:00"],
            ]
        )
        assert events(stream.collapse_events(loops=True, name="merged")) == ["merged"]


class TestCollapseEventsInactivity:
    def test_timeout_bursts_via_split_sessions(self):
        """Inactivity is not a mode of this processor: `split_sessions` writes
        the boundary into a column and `group_col` collapses it, which is the
        documented replacement for the old per-group `timeout` key."""
        stream = make_stream(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_1", "A", "2020-01-01 02:00:00"],  # > 30m gap — second burst
                ["user_1", "B", "2020-01-01 02:01:00"],
            ]
        )
        res = stream.split_sessions(timeout="30m").collapse_events(
            group_col="session_id", name="burst"
        )
        assert events(res) == ["burst", "burst"]
