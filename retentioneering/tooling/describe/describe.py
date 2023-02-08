from __future__ import annotations

import pandas as pd

from retentioneering.eventstream.types import EventstreamType


class Describe:
    def __init__(
        self, eventstream: EventstreamType, session_col: str = "session_id", raw_events_only: bool = False
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.session_col = session_col
        self.type_col = self.__eventstream.schema.event_type
        self.raw_events_only = raw_events_only

    def _describe(self) -> pd.DataFrame:
        user_col, event_col, time_col, type_col, session_col = (
            self.user_col,
            self.event_col,
            self.time_col,
            self.type_col,
            self.session_col,
        )

        df = self.__eventstream.to_dataframe(copy=True)
        has_sessions = session_col in df.columns

        if self.raw_events_only:
            df = df[df[type_col].isin(["raw"])]

        max_time = df[time_col].max()
        min_time = df[time_col].min()

        values_all_users = [
            df[user_col].nunique(),
            df[event_col].nunique(),
            min_time.round("s"),
            max_time.round("s"),
            (max_time - min_time).round("s"),
        ]

        user_agg = df.groupby(user_col).agg({time_col: ["min", "max"], event_col: ["count"]}).reset_index()
        time_diff_user = user_agg[(time_col, "max")] - user_agg[(time_col, "min")]
        mean_time_user = time_diff_user.mean().round("s")  # type: ignore
        median_time_user = time_diff_user.median().round("s")  # type: ignore
        std_time_user = time_diff_user.std().round("s")  # type: ignore
        min_length_time_user = time_diff_user.min().round("s")
        max_length_time_user = time_diff_user.max().round("s")

        event_count_user = user_agg[(event_col, "count")]
        mean_user = round(event_count_user.mean(), 2)  # type: ignore
        median_user = event_count_user.median()
        std_user = round(event_count_user.std(), 2)  # type: ignore
        min_length_user = event_count_user.min()
        max_length_user = event_count_user.max()

        values_time_events = [
            mean_time_user,
            std_time_user,
            median_time_user,
            min_length_time_user,
            max_length_time_user,
            mean_user,
            std_user,
            median_user,
            min_length_user,
            max_length_user,
        ]

        all_iterables = [
            ["all_users"],
            ["unique_users", "unique_events", "eventstream_start", "eventstream_end", "eventstream_length"],
        ]

        time_events_iterables = [["time_per_user", "events_per_user"], ["mean", "std", "median", "min", "max"]]

        if has_sessions:
            time_events_iterables[0] += ["time_per_session", "events_per_session"]
            all_iterables[1].insert(2, "unique_sessions")

            values_all_users.insert(2, df[session_col].nunique())  # type: ignore

            session_agg = df.groupby(session_col).agg({time_col: ["min", "max"], event_col: ["count"]}).reset_index()
            time_diff_session = session_agg[(time_col, "max")] - session_agg[(time_col, "min")]
            mean_time_session = time_diff_session.mean().round("s")  # type: ignore
            median_time_session = time_diff_session.median().round("s")  # type: ignore
            std_time_session = time_diff_session.std().round("s")  # type: ignore
            min_length_time_session = time_diff_session.min().round("s")
            max_length_time_session = time_diff_session.max().round("s")

            if session_agg is None:
                raise ValueError("session_agg is None")
            event_count_session = session_agg[(event_col, "count")]
            mean_session = round(event_count_session.mean(), 2)  # type: ignore
            median_session = event_count_session.median()
            std_session = round(event_count_session.std(), 2)  # type: ignore
            min_length_session = event_count_session.min()
            max_length_session = event_count_session.max()

            values_time_events += [
                mean_time_session,
                std_time_session,
                median_time_session,
                min_length_time_session,
                max_length_time_session,
                mean_session,
                std_session,
                median_session,
                min_length_session,
                max_length_session,
            ]

        out_columns = ["value"]
        index_names = ["category", "metric"]

        all_users_index = pd.MultiIndex.from_product(all_iterables, names=index_names)
        time_events_index = pd.MultiIndex.from_product(time_events_iterables, names=index_names)

        df_all_users = pd.DataFrame(data=values_all_users, index=all_users_index, columns=out_columns)
        df_time_events = pd.DataFrame(data=values_time_events, index=time_events_index, columns=out_columns)

        res = pd.concat([df_all_users, df_time_events])

        return res
