import numpy as np
import pandas as pd
from retentioneering.eventstream.eventstream import Eventstream


class TestAddStartEndEvents:
    def test__simple(self) -> None:
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "US"],
                ["user_1", "B", "2020-01-01 00:01:00", "US"],
                ["user_1", "C", "2020-01-01 00:02:00", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "US"],
                ["user_3", "B", "2020-01-01 00:00:00", "UK"],
                ["user_3", "B", "2020-01-01 00:01:00", "UK"],
                ["user_3", "B", "2020-01-01 00:02:00", "UK"],
                ["user_3", "B", "2020-01-01 00:03:00", "UK"],
            ],
            columns=["user_id", "event", "timestamp", "country"],
        )
        schema = {"custom_cols": ["country"]}
        stream = Eventstream(df, schema)

        res = stream.add_start_end_events()

        expected_columns = ["user_id", "event", "event_type", "timestamp", "country"]
        expected = pd.DataFrame(
            [
                ["user_1", "path_start", "path_start", "2020-01-01 00:00:00", "US"],
                ["user_1", "A", "raw", "2020-01-01 00:00:00", "US"],
                ["user_1", "B", "raw", "2020-01-01 00:01:00", "US"],
                ["user_1", "C", "raw", "2020-01-01 00:02:00", "US"],
                ["user_1", "path_end", "path_end", "2020-01-01 00:02:00", "US"],
                ["user_2", "path_start", "path_start", "2020-01-01 00:00:00", "US"],
                ["user_2", "A", "raw", "2020-01-01 00:00:00", "US"],
                ["user_2", "path_end", "path_end", "2020-01-01 00:00:00", "US"],
                ["user_3", "path_start", "path_start", "2020-01-01 00:00:00", "UK"],
                ["user_3", "B", "raw", "2020-01-01 00:00:00", "UK"],
                ["user_3", "B", "raw", "2020-01-01 00:01:00", "UK"],
                ["user_3", "B", "raw", "2020-01-01 00:02:00", "UK"],
                ["user_3", "B", "raw", "2020-01-01 00:03:00", "UK"],
                ["user_3", "path_end", "path_end", "2020-01-01 00:03:00", "UK"],
            ],
            columns=expected_columns,
        )
        expected_schema = {"custom_cols": ["country"], "event_type": "event_type"}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__nullable_extra_columns_take_boundary_rows(self) -> None:
        # Regression test: groupby().first()/.last() take the first/last
        # NON-NULL value per column independently, so a NaN in an extra column
        # on the boundary event used to be backfilled from a *different* event,
        # producing a chimera synthetic row. path_start/path_end must copy the
        # whole boundary row instead.
        df = pd.DataFrame(
            [
                # user_1: first event has NaN utm_source, a later one has a value
                ["user_1", "A", "2020-01-01 00:00:00", np.nan, np.nan],
                ["user_1", "B", "2020-01-01 00:01:00", "google", "camp_1"],
                # last event has NaN again -> path_end must carry NaN too
                ["user_1", "C", "2020-01-01 00:02:00", np.nan, np.nan],
                # user_2: fully non-null path, values must pass through as-is
                ["user_2", "A", "2020-01-01 00:00:00", "direct", "camp_2"],
                ["user_2", "B", "2020-01-01 00:01:00", "direct", "camp_2"],
            ],
            columns=["user_id", "event", "timestamp", "utm_source", "campaign"],
        )
        schema = {"segment_cols": ["utm_source"], "custom_cols": ["campaign"]}
        stream = Eventstream(df, schema)

        res = stream.add_start_end_events()
        res_df = res.df

        # 5 raw events + (path_start + path_end) per user
        assert len(res_df) == 9
        assert (res_df["event_type"] == "path_start").sum() == 2
        assert (res_df["event_type"] == "path_end").sum() == 2

        u1_start = res_df[
            (res_df["user_id"] == "user_1") & (res_df["event"] == "path_start")
        ].iloc[0]
        u1_end = res_df[
            (res_df["user_id"] == "user_1") & (res_df["event"] == "path_end")
        ].iloc[0]

        # path_start carries the first ROW's values: NaN, first timestamp
        assert pd.isna(u1_start["utm_source"])
        assert pd.isna(u1_start["campaign"])
        assert u1_start["timestamp"] == pd.Timestamp("2020-01-01 00:00:00")
        assert u1_start["event_type"] == "path_start"
        assert u1_start["subindex"] == 0

        # path_end carries the last ROW's values: NaN, last timestamp
        assert pd.isna(u1_end["utm_source"])
        assert pd.isna(u1_end["campaign"])
        assert u1_end["timestamp"] == pd.Timestamp("2020-01-01 00:02:00")
        assert u1_end["event_type"] == "path_end"
        assert u1_end["subindex"] == 3

        # non-null path keeps its values on the synthetic rows
        u2_start = res_df[
            (res_df["user_id"] == "user_2") & (res_df["event"] == "path_start")
        ].iloc[0]
        u2_end = res_df[
            (res_df["user_id"] == "user_2") & (res_df["event"] == "path_end")
        ].iloc[0]
        assert u2_start["utm_source"] == "direct"
        assert u2_start["campaign"] == "camp_2"
        assert u2_end["utm_source"] == "direct"
        assert u2_end["campaign"] == "camp_2"

        # raw events are intact and in order, synthetic rows wrap each path
        u1 = res_df[res_df["user_id"] == "user_1"]
        assert list(u1["event"]) == ["path_start", "A", "B", "C", "path_end"]
        assert list(u1["event_type"]) == ["path_start", "raw", "raw", "raw", "path_end"]
        assert list(u1["subindex"]) == [0, 1, 1, 1, 3]

    def test__existing_start_end(self) -> None:
        df = pd.DataFrame(
            [
                ["user_1", "path_start", "path_start", "2020-01-01 00:00:00", "US"],
                ["user_1", "A", "raw", "2020-01-01 00:00:00", "US"],
                ["user_1", "B", "raw", "2020-01-01 00:01:00", "US"],
                ["user_1", "C", "raw", "2020-01-01 00:02:00", "US"],
                ["user_2", "path_start", "path_start", "2020-01-01 00:00:00", "US"],
                ["user_2", "A", "raw", "2020-01-01 00:00:00", "US"],
                ["user_2", "path_end", "path_end", "2020-01-01 00:00:00", "US"],
                ["user_3", "B", "raw", "2020-01-01 00:00:00", "UK"],
                ["user_3", "B", "raw", "2020-01-01 00:01:00", "UK"],
                ["user_3", "B", "raw", "2020-01-01 00:02:00", "UK"],
                ["user_3", "B", "raw", "2020-01-01 00:03:00", "UK"],
                ["user_3", "path_end", "path_end", "2020-01-01 00:03:00", "UK"],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "country"],
        )
        schema = {"custom_cols": ["country"], "event_type": "event_type"}
        stream = Eventstream(df, schema)

        res = stream.add_start_end_events()

        expected_columns = ["user_id", "event", "event_type", "timestamp", "country"]
        expected = pd.DataFrame(
            [
                ["user_1", "path_start", "path_start", "2020-01-01 00:00:00", "US"],
                ["user_1", "A", "raw", "2020-01-01 00:00:00", "US"],
                ["user_1", "B", "raw", "2020-01-01 00:01:00", "US"],
                ["user_1", "C", "raw", "2020-01-01 00:02:00", "US"],
                ["user_1", "path_end", "path_end", "2020-01-01 00:02:00", "US"],
                ["user_2", "path_start", "path_start", "2020-01-01 00:00:00", "US"],
                ["user_2", "A", "raw", "2020-01-01 00:00:00", "US"],
                ["user_2", "path_end", "path_end", "2020-01-01 00:00:00", "US"],
                ["user_3", "path_start", "path_start", "2020-01-01 00:00:00", "UK"],
                ["user_3", "B", "raw", "2020-01-01 00:00:00", "UK"],
                ["user_3", "B", "raw", "2020-01-01 00:01:00", "UK"],
                ["user_3", "B", "raw", "2020-01-01 00:02:00", "UK"],
                ["user_3", "B", "raw", "2020-01-01 00:03:00", "UK"],
                ["user_3", "path_end", "path_end", "2020-01-01 00:03:00", "UK"],
            ],
            columns=expected_columns,
        )
        expected_schema = {"custom_cols": ["country"], "event_type": "event_type"}
        expected = Eventstream(expected, expected_schema)

        assert res.equals(expected)

    def test__grain_switch_does_not_strand_stale_markers(self) -> None:
        """Regression test: calling add_start_end_events at a finer grain
        (session_id) and then again at a coarser grain (user_id) must not
        leave the session-level path_start/path_end markers stranded in the
        middle of the user-level path -- only one path_start and one
        path_end per user should remain, at the user's true boundaries."""
        df = pd.DataFrame(
            [
                ["U1", "S1", "A", "2020-01-01 00:00:00"],
                ["U1", "S1", "B", "2020-01-01 00:01:00"],
                ["U1", "S2", "C", "2020-01-01 01:00:00"],
                ["U1", "S2", "D", "2020-01-01 01:01:00"],
            ],
            columns=["user_id", "session_id", "event", "timestamp"],
        )
        stream = Eventstream(df, {"path_cols": ["user_id", "session_id"]})

        res = stream.add_start_end_events(path_col="session_id").add_start_end_events(
            path_col="user_id"
        )
        res_df = res.df

        assert (res_df["event_type"] == "path_start").sum() == 1
        assert (res_df["event_type"] == "path_end").sum() == 1
        assert list(res_df["event"]) == ["path_start", "A", "B", "C", "D", "path_end"]
