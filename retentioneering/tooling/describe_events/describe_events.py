from __future__ import annotations

import pandas as pd

from retentioneering.eventstream.types import EventstreamType


class DescribeEvents:
    def __init__(
        self,
        eventstream: EventstreamType,
        session_col: str = "session_id",
        raw_events_only: bool = False,
        event_list: list[str] | None = None,
    ) -> None:

        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.session_col = session_col
        self.type_col = self.__eventstream.schema.event_type
        self.event_list = event_list
        self.raw_events_only = raw_events_only

    def _describe(self) -> pd.DataFrame:
        user_col, event_col, time_col, type_col, session_col, event_list = (
            self.user_col,
            self.event_col,
            self.time_col,
            self.type_col,
            self.session_col,
            self.event_list,
        )

        df = self.__eventstream.to_dataframe(copy=True)
        has_sessions = session_col in df.columns

        if self.raw_events_only:
            df = df[df[type_col].isin(["raw"])]

        total_events_base = df.shape[0]
        unique_users_base = df[user_col].nunique()

        df["__event_trajectory_idx"] = df.groupby(user_col).cumcount()
        df["__event_trajectory_timedelta"] = df[time_col] - df.groupby(user_col)[time_col].transform("first")

        if has_sessions:
            total_sessions_base = df[session_col].nunique()  # type: ignore
            df["__event_session_idx"] = df.groupby(session_col).cumcount()
            df["__event_session_timedelta"] = df[time_col] - df.groupby(session_col)[time_col].transform("first")

        if event_list:
            df = df[df[event_col].isin(event_list)]

        basic_info = df.groupby("event").agg(
            number_of_occurrences=("event_id", "count"), unique_users=("user_id", "nunique")
        )

        basic_info["number_of_occurrences_shared"] = (basic_info["number_of_occurrences"] / total_events_base).round(2)
        basic_info["unique_users_shared"] = (basic_info["unique_users"] / unique_users_base).round(2)

        basic_info.columns = pd.MultiIndex.from_product([["basic_statistics"], basic_info.columns])
        stats_order = ["mean", "std", "median", "min", "max"]
        first_time = "time_to_FO_user_wise"
        first_event = "steps_to_FO_user_wise"

        df_agg_event = (
            df.groupby([event_col, user_col])
            .agg(
                time_to_FO_user_wise=("__event_trajectory_timedelta", "first"),
                steps_to_FO_user_wise=("__event_trajectory_idx", "first"),
            )
            .reset_index()
        )

        df_agg_event[first_time] = df_agg_event[first_time].dt.total_seconds()

        df_agg_event = df_agg_event.groupby([event_col])[[first_time, first_event]].agg(stats_order)  # type: ignore
        df_agg_event[(first_event, "mean")] = df_agg_event[(first_event, "mean")].round(2)
        df_agg_event[(first_event, "std")] = df_agg_event[(first_event, "std")].round(2)

        for stat in stats_order:
            mult_ind = (first_time, stat)
            df_agg_event[mult_ind] = pd.to_timedelta(df_agg_event[mult_ind], unit="s").round("s")  # type: ignore

        if has_sessions:
            first_time = "time_to_FO_session_wise"
            first_event = "steps_to_FO_session_wise"
            unique_sess = ("basic_statistics", "unique_sessions")
            share_sess = ("basic_statistics", "unique_sessions_shared")

            basic_info[unique_sess] = df.groupby("event")[session_col].agg("nunique")  # type: ignore
            basic_info[share_sess] = (basic_info[unique_sess] / total_sessions_base).round(2)  # type: ignore

            df_agg_sess = (
                df.groupby([event_col, session_col])
                .agg(
                    time_to_FO_session_wise=("__event_session_timedelta", "first"),
                    steps_to_FO_session_wise=("__event_session_idx", "first"),
                )
                .reset_index()
            )

            df_agg_sess[first_time] = df_agg_sess[first_time].dt.total_seconds()

            df_agg_sess = df_agg_sess.groupby([event_col])[[first_time, first_event]].agg(stats_order)  # type: ignore
            df_agg_sess[(first_event, "mean")] = df_agg_sess[(first_event, "mean")].round(2)
            df_agg_sess[(first_event, "std")] = df_agg_sess[(first_event, "std")].round(2)

            for stat in stats_order:
                mult_ind = (first_time, stat)
                df_agg_sess[mult_ind] = pd.to_timedelta(df_agg_sess[mult_ind], unit="s").round("s")  # type: ignore

            df_agg_event = df_agg_event.merge(df_agg_sess, left_index=True, right_index=True)

        res = basic_info.merge(df_agg_event, left_index=True, right_index=True)
        if has_sessions:
            res.insert(2, unique_sess, res.pop(unique_sess))  # type: ignore

        return res
